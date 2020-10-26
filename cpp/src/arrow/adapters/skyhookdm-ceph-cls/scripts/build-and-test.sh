#!/bin/bash

mkdir -p build/
cd build/
cmake ..
make VERBOSE=1
cd ..

cp build/lib/libcls_arrow.so* /usr/lib64/rados-classes/
scripts/micro-osd.sh test-cluster /etc/ceph
build/bin/cls_arrow_test
