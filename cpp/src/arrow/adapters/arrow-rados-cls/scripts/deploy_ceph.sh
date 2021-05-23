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

set -ex

cat > ~/.ssh/config << EOF
Host *
    StrictHostKeyChecking no
EOF

apt update
apt install -y python3-venv python3-pip ceph-fuse ceph-common

git clone https://github.com/ceph/ceph-deploy /tmp/ceph-deploy
pip3 install --upgrade /tmp/ceph-deploy

mkdir /tmp/deployment
cd /tmp/deployment/

ceph-deploy new node{1..3}

ceph-deploy install --release octopus node{1..3}

ceph-deploy mon create-initial

ceph-deploy admin node{1..3}

ceph-deploy mgr create node1

cat >> ceph.conf << EOF
mon allow pool delete = true
osd class load list = *
osd op threads = 8
EOF

ceph-deploy --overwrite-conf config push node{1..3}

cp ceph.conf /etc/ceph/ceph.conf
cp ceph.client.admin.keyring  /etc/ceph/ceph.client.admin.keyring
ceph -s

for i in {1..3}; do
    scp /tmp/deployment/ceph.bootstrap-osd.keyring node${i}:/etc/ceph/ceph.keyring
    scp /tmp/deployment/ceph.bootstrap-osd.keyring node${i}:/var/lib/ceph/bootstrap-osd/ceph.keyring
    ceph-deploy osd create --data /dev/nvme0n1p4 node${i}
done

ceph-deploy mds create node1
ceph osd pool create cephfs_data 16
ceph osd pool create cephfs_metadata 16
ceph osd pool create device_health_metrics 16

ceph fs new cephfs cephfs_metadata cephfs_data
mkdir -p /mnt/cephfs

sleep 5
ceph-fuse /mnt/cephfs
