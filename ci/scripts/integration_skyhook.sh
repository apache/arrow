#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script spawns a single-node Ceph cluster, creates a CephFS mount,
# generates a Parquet dataset, and runs the SkyhookDM integration tests.
# Taken from https://github.com/ceph/go-ceph/blob/master/micro-osd.sh

set -e
set -x
set -u

if [ "${ARROW_SKYHOOK:-OFF}" != "ON" ]; then
  exit 0
fi

ARROW_BUILD_DIR=${1}/cpp
DIR=/tmp/integration_skyhook

# set environment variables
pkill ceph || true
rm -rf ${DIR}/*
LOG_DIR=${DIR}/log
MON_DATA=${DIR}/mon
MDS_DATA=${DIR}/mds
MOUNTPT=${MDS_DATA}/mnt
OSD_DATA=${DIR}/osd
mkdir -p ${LOG_DIR} ${MON_DATA} ${OSD_DATA} ${MDS_DATA} ${MOUNTPT}
MDS_NAME="Z"
MON_NAME="a"
MGR_NAME="x"
MIRROR_ID="m"

# cluster wide parameters
cat >> ${DIR}/ceph.conf <<EOF
[global]
fsid = $(uuidgen)
osd crush chooseleaf type = 0
run dir = ${DIR}/run
auth cluster required = none
auth service required = none
auth client required = none
osd pool default size = 1
mon host = ${HOSTNAME}
[mds.${MDS_NAME}]
host = ${HOSTNAME}
[mon.${MON_NAME}]
log file = ${LOG_DIR}/mon.log
chdir = ""
mon cluster log file = ${LOG_DIR}/mon-cluster.log
mon data = ${MON_DATA}
mon data avail crit = 0
mon addr = ${HOSTNAME}
mon allow pool delete = true
[osd.0]
log file = ${LOG_DIR}/osd.log
chdir = ""
osd data = ${OSD_DATA}
osd journal = ${OSD_DATA}.journal
osd journal size = 100
osd objectstore = memstore
osd class load list = *
osd class default list = *
EOF

export CEPH_CONF=${DIR}/ceph.conf
cp $CEPH_CONF /etc/ceph/ceph.conf

# start an osd
ceph-mon --id ${MON_NAME} --mkfs --keyring /dev/null
touch ${MON_DATA}/keyring
ceph-mon --id ${MON_NAME}

# start an osd
OSD_ID=$(ceph osd create)
ceph osd crush add osd.${OSD_ID} 1 root=default
ceph-osd --id ${OSD_ID} --mkjournal --mkfs
ceph-osd --id ${OSD_ID} || ceph-osd --id ${OSD_ID} || ceph-osd --id ${OSD_ID}

# start an mds for cephfs
ceph auth get-or-create mds.${MDS_NAME} mon 'profile mds' mgr 'profile mds' mds 'allow *' osd 'allow *' > ${MDS_DATA}/keyring
ceph osd pool create cephfs_data 8
ceph osd pool create cephfs_metadata 8
ceph fs new cephfs cephfs_metadata cephfs_data
ceph fs ls
ceph-mds -i ${MDS_NAME}
ceph status
while [[ ! $(ceph mds stat | grep "up:active") ]]; do sleep 1; done

# start a manager
ceph-mgr --id ${MGR_NAME}

# test the setup
ceph --version
ceph status

apt update
apt install -y python3-pip

pushd ${ARROW_BUILD_DIR}
    # create the rados-classes, if not there already
    mkdir -p /usr/lib/x86_64-linux-gnu/rados-classes/
    cp debug/libcls_skyhook* /usr/lib/x86_64-linux-gnu/rados-classes/

    # mount a ceph filesystem to /mnt/cephfs in the user-space using ceph-fuse
    mkdir -p /mnt/cephfs
    ceph-fuse /mnt/cephfs
    sleep 5

    # download an example dataset and copy into the mounted dir
    pip3 install pyarrow pandas
    python3 /arrow/ci/scripts/generate_dataset.py
    cp -r nyc /mnt/cephfs/
    sleep 10

    # run the tests
    SKYHOOK_CLS_TEST=debug/skyhook-cls-test
    if [ -f "$SKYHOOK_CLS_TEST" ]; then
        debug/skyhook-cls-test
    fi

    SKYHOOK_PROTOCOL_TEST=debug/skyhook-protocol-test
    if [ -f "$SKYHOOK_PROTOCOL_TEST" ]; then
        debug/skyhook-protocol-test
    fi
popd
