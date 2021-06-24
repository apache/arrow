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
set -eu

if [[ $# -lt 6 ]] ; then
    echo "usage: ./deploy_ceph.sh [mon hosts] [osd hosts] [mds hosts] [mgr hosts] [blkdevice] [pool size]"
    echo " "
    echo "for example: ./deploy_ceph.sh node1,node2,node3 node4,node5,node6 node1 node1 /dev/sdb 3"
    exit 1
fi

# in default mode (without any arguments), deploy a single OSD Ceph cluster 
MON=${1:-node1}
OSD=${2:-node1}
MDS=${3:-node1}
MGR=${4:-node1}
BLKDEV=${5:-/dev/nvme0n1p4}
POOL_SIZE=${6:-1}

# split the comma separated nodes into a list
IFS=',' read -ra MON_LIST <<< "$MON"; unset IFS
IFS=',' read -ra OSD_LIST <<< "$OSD"; unset IFS
IFS=',' read -ra MDS_LIST <<< "$MDS"; unset IFS
IFS=',' read -ra MGR_LIST <<< "$MGR"; unset IFS
MON_LIST=${MON_LIST[@]}
OSD_LIST=${OSD_LIST[@]}
MDS_LIST=${MDS_LIST[@]}
MGR_LIST=${MGR_LIST[@]}

# disable host key checking
cat > ~/.ssh/config << EOF
Host *
    StrictHostKeyChecking no
EOF

# delete CephFS application along with any mountpoint
function delete_cephfs {
    echo "deleting cephfs"
    fusermount -uz /mnt/cephfs || true
    rm -rf /mnt/cephfs
    ceph fs fail cephfs || true
    ceph fs rm cephfs --yes-i-really-mean-it || true
}

# delete the trailing pools
function delete_pools {
    echo "deleting pools"
    ceph osd pool delete cephfs_data cephfs_data --yes-i-really-really-mean-it || true
    ceph osd pool delete cephfs_metadata cephfs_metadata --yes-i-really-really-mean-it || true
    ceph osd pool delete device_health_metrics device_health_metrics --yes-i-really-really-mean-it || true
}

# delete trailing OSD daemons
function delete_osds {
    # kill the daemon and format the LVM partition
    for node in ${OSD_LIST}; do
        ssh $node pkill ceph-osd || true
        # try to zap the block devices.
        ssh $node ceph-volume lvm zap $BLKDEV --destroy || true
        ssh $node rm -rf /etc/ceph/*
    done

    # remove all the OSDs
    NUM_OSDS=${#OSD_LIST[@]}
    for ((i=0; i<$NUM_OSDS; i++)); do 
        # mark an OSD down and remove it
        ceph osd down osd.${i} || true
        ceph osd out osd.${i} || true
        ceph osd rm osd.${i} || true

        # remove entry from crush map
        ceph osd crush rm osd.${i} || true

        # remove auth entry
        ceph auth del osd.${i} || true
    done
}

# kill the MONs, MGRs and MDSs.
function delete_mon_mgr_mds {
    for node in ${MON_LIST}; do
        ssh $node rm -rf /etc/ceph/*
        ssh $node pkill ceph-mon || true
    done
    for node in ${MGR_LIST}; do
        ssh $node rm -rf /etc/ceph/*
        ssh $node pkill ceph-mgr || true
    done
    for node in ${MDS_LIST}; do
        ssh $node rm -rf /etc/ceph/*
        ssh $node pkill ceph-mds || true
    done
}

echo "[0] cleaning up a previous Ceph installation"
# clean the cluster
delete_cephfs
delete_pools
delete_osds
delete_mon_mgr_mds

# clean the old workspace
rm -rf /tmp/deployment
rm -rf /tmp/ceph-deploy
rm -rf /etc/ceph/*

echo "[1] installing common packages"
apt update
apt install -y python3-venv python3-pip ceph-fuse ceph-common attr

echo "[2] installing ceph-deploy"
git clone https://github.com/ceph/ceph-deploy /tmp/ceph-deploy
pip3 install --upgrade /tmp/ceph-deploy

mkdir /tmp/deployment
cd /tmp/deployment

echo "[3] initializng Ceph config"
ceph-deploy new $MON_LIST

echo "[4] installing Ceph packages on all the hosts"
ceph-deploy install --release octopus $MON_LIST $OSD_LIST $MDS_LIST $MGR_LIST

echo "[5] deploying MONs"
ceph-deploy mon create-initial
ceph-deploy admin $MON_LIST

echo "[6] deploying MGRs"
ceph-deploy mgr create $MGR_LIST

# update the Ceph config to allow pool deletion and to recognize object class libs.
cat >> ceph.conf << EOF
mon allow pool delete = true
osd class load list = *
osd op threads = 16
EOF

# deploy the updated Ceph config and restat the MONs for the config to take effect
ceph-deploy --overwrite-conf config push $MON_LIST $OSD_LIST $MDS_LIST $MGR_LIST
for node in ${MON_LIST}; do
    ssh $node systemctl restart ceph-mon.target
done

# copy the config to the default location on the admin node
cp ceph.conf /etc/ceph/ceph.conf
cp ceph.client.admin.keyring  /etc/ceph/ceph.client.admin.keyring

# pause and let user's can take a quick look if everything is fine before deploying OSDs
ceph -s
sleep 5

echo "[7] deploying OSDs"
for node in ${OSD_LIST}; do
    scp /tmp/deployment/ceph.bootstrap-osd.keyring $node:/etc/ceph/ceph.keyring
    scp /tmp/deployment/ceph.bootstrap-osd.keyring $node:/var/lib/ceph/bootstrap-osd/ceph.keyring
    ceph-deploy osd create --data $BLKDEV $node
done

echo "[8] deploying MDSs"
ceph-deploy mds create $MDS_LIST

echo "[9] creating pools for deploying CephFS"
ceph osd pool create cephfs_data 128
ceph osd pool create cephfs_metadata 16

# turn off pg autoscale
ceph osd pool set cephfs_data pg_autoscale_mode off

# set the pool sizes based on commandline arguments
ceph osd pool set cephfs_data size $POOL_SIZE
ceph osd pool set cephfs_metadata size $POOL_SIZE
ceph osd pool set device_health_metrics size $POOL_SIZE

echo "[9] deploying CephFS"
ceph fs new cephfs cephfs_metadata cephfs_data
mkdir -p /mnt/cephfs

echo "[10] mounting CephFS at /mnt/cephfs"
sleep 5
ceph-fuse /mnt/cephfs

echo "Ceph deployed successfully !"
ceph -s
