#!/bin/bash
set -e

echo "source /opt/rh/devtoolset-8/enable" >> /etc/profile
source /etc/profile

if [ -d "ceph/" ]; then
  echo "Using ceph/ as folder as source folder"
  CEPH_SRC_DIR="ceph"
else
  echo "No CEPH_SRC_DIR variable defined, using current directory"
  CEPH_SRC_DIR="./"
fi

cd "$CEPH_SRC_DIR"

if [ "$CMAKE_CLEAN" == "true" ] || [ "$CMAKE_CLEAN" == "1" ]; then
  rm -rf ./build
fi

mkdir -p ./build

cd build/

if [ -z "$(ls -A ./)" ] || [ "$CMAKE_RECONFIGURE" == "true" ] || [ "$CMAKE_RECONFIGURE" == "1" ]; then
  cmake3 $CMAKE_FLAGS ..
fi

if [ -z "$BUILD_THREADS" ] ; then
  BUILD_THREADS=`grep processor /proc/cpuinfo | wc -l`
fi

make -j$BUILD_THREADS
