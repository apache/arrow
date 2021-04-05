#!/bin/bash

# slightly based on 
# https://github.com/apache/arrow/issues/6270
# https://issues.apache.org/jira/browse/ARROW-10495
# https://github.com/apache/arrow/issues/1514
# https://embeddeddevelop.blogspot.com/2019/01/clang-tidy-cmake-on-ubuntu-1804.html
# https://apt.llvm.org/
# https://arrow.apache.org/docs/r/#developing


if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root!"
   exit 1
fi

# the basics
apt-get -y update
apt-get -y install cmake build-essential

# faster builds
apt-get -y install ninja-build

# s3 dependencies
apt-get -y install libcurl4-openssl-dev

