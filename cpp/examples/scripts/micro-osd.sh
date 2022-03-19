#
#    Copyright (C) 2013,2014 Loic Dachary <loic@dachary.org>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
set -e
set -x
set -u

DIR=${1}

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

# mount the ceph filesystem
mkdir -p /mnt/cephfs
ceph-fuse /mnt/cephfs
sleep 5
