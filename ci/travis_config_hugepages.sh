#!/usr/bin/env bash

 # Initialise all at once
declare -A UNITSMAP=( [kB]=$((10**3)) [mB]=$((10**6)) [gB]=$((10**9)) )

sudo sysctl -w vm.nr_hugepages=2048

HUGEPAGEVALUE=$(awk '/Hugepagesize/ { print $2 }' /proc/meminfo)
HUGEPAGEUNIT=$(awk '/Hugepagesize/ { print $3 }' /proc/meminfo)

# Compute the current value of hugepagesize
HUGEPAGESIZE=$(($HUGEPAGEVALUE*${UNITSMAP[$HUGEPAGEUNIT]}))

TARGETOBJECTSIZE=${UNITSMAP[gB]}

NBPAGETORESERVE=$(($TARGETOBJECTSIZE/ $HUGEPAGESIZE))

sudo mkdir -p /mnt/hugepages
sudo mount -t hugetlbfs -o uid=`id -u` -o gid=`id -g` none /mnt/hugepages
sudo bash -c "echo `id -g` > /proc/sys/vm/hugetlb_shm_group"
sudo bash -c "echo "$NBPAGETORESERVE" > /proc/sys/vm/nr_hugepages"
