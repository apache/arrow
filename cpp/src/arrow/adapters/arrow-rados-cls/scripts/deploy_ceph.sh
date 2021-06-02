#!/bin/bash
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
set -e

if [[ $# -lt 4 ]] ; then
    echo "usage: ./deploy_ceph.sh [mon hosts] [osd hosts] [mds hosts] [mgr hosts]"
    echo " "
    echo "for example: ./deploy_ceph.sh node1,node2,node3 node4,node5,node6 node1 node1"
    exit 1
fi

MON=$1
OSD=$2
MDS=$3
MGR=$4

IFS=',' read -ra MON_LIST <<< "$MON"; unset IFS
IFS=',' read -ra OSD_LIST <<< "$OSD"; unset IFS
IFS=',' read -ra MDS_LIST <<< "$MDS"; unset IFS
IFS=',' read -ra MGR_LIST <<< "$MGR"; unset IFS

MON_LIST=${MON_LIST[@]}
OSD_LIST=${OSD_LIST[@]}
MDS_LIST=${MDS_LIST[@]}
MGR_LIST=${MGR_LIST[@]}

cat > ~/.ssh/config << EOF
Host *
    StrictHostKeyChecking no
EOF

echo "[1] installing common packages"
apt update
apt install -y python3-venv python3-pip ceph-fuse ceph-common

echo "[2] installing ceph-deploy"
git clone https://github.com/ceph/ceph-deploy /tmp/ceph-deploy
pip3 install --upgrade /tmp/ceph-deploy

mkdir /tmp/deployment
cd /tmp/deployment/

echo "[3] initializng Ceph config"
ceph-deploy new $MON_LIST

echo "[4] installing Ceph packages on all the hosts"
ceph-deploy install --release octopus $MON_LIST $OSD_LIST $MDS_LIST $MGR_LIST

echo "[5] deploying MONs"
ceph-deploy mon create-initial
ceph-deploy admin $MON_LIST

echo "[6] deploying MGRs"
ceph-deploy mgr create $MGR_LIST

cat >> ceph.conf << EOF
mon allow pool delete = true
osd class load list = *
osd op threads = 16
EOF

ceph-deploy --overwrite-conf config push $OSD_LIST

cp ceph.conf /etc/ceph/ceph.conf
cp ceph.client.admin.keyring  /etc/ceph/ceph.client.admin.keyring
ceph -s

echo "[7] deploying OSDs"
for node in ${OSD_LIST}; do
    scp /tmp/deployment/ceph.bootstrap-osd.keyring $node:/etc/ceph/ceph.keyring
    scp /tmp/deployment/ceph.bootstrap-osd.keyring $node:/var/lib/ceph/bootstrap-osd/ceph.keyring
    ceph-deploy osd create --data /dev/nvme0n1p4 $node
done

echo "[8] deploying MDSs"
ceph-deploy mds create $MDS_LIST

echo "[9] creating pools for deploying CephFS"
ceph osd pool create cephfs_data 128
ceph osd pool create cephfs_metadata 16
ceph osd pool set cephfs_data pg_autoscale_mode off

echo "[9] deploying CephFS"
ceph fs new cephfs cephfs_metadata cephfs_data
mkdir -p /mnt/cephfs

echo "[10] mounting CephFS at /mnt/cephfs"
sleep 5
ceph-fuse /mnt/cephfs

echo "Done."
ceph -s
