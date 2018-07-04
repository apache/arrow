#!/usr/bin/env bash

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

 # Initialise all at once
declare -A UNITSMAP=( [kB]=$((10**3)) [mB]=$((10**6)) [gB]=$((10**9)) )

sudo sysctl -w vm.nr_hugepages=2048

HUGEPAGEVALUE=$(awk '/Hugepagesize/ { print $2 }' /proc/meminfo)
HUGEPAGEUNIT=$(awk '/Hugepagesize/ { print $3 }' /proc/meminfo)

echo $HUGEPAGEVALUE" "$HUGEPAGEUNIT
# Compute the current value of hugepagesize
HUGEPAGESIZE=$(($HUGEPAGEVALUE*${UNITSMAP[$HUGEPAGEUNIT]} / ${UNITSMAP[kB]}))

TARGETOBJECTSIZE=${UNITSMAP[gB]}

NBPAGETORESERVE=$(($TARGETOBJECTSIZE/ $HUGEPAGESIZE))

cat /proc/meminfo | head -n2

echo "current_hugepagesize="$HUGEPAGESIZE
echo "nb_reserved_pages="$NBPAGETORESERVE

sudo mkdir -p /mnt/hugepages
sudo mount -t hugetlbfs -o uid=`id -u` -o gid=`id -g` none /mnt/hugepages
sudo bash -c "echo `id -g` > /proc/sys/vm/hugetlb_shm_group"
sudo bash -c "echo "$NBPAGETORESERVE" > /proc/sys/vm/nr_hugepages"
