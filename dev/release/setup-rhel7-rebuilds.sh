#!/bin/bash
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


# A script to install dependencies required for release
# verification Red Hat Enterprise Linux 7 rebuilds in particular
# on CentOS 7

set -exu

yum -y update
yum -y groupinstall "Development Tools"
yum -y install centos-release-scl curl
yum -y install epel-release
yum -y install \
  cmake3 \
  devtoolset-11-* \
  git \
  gobject-introspection-devel \
  java-11-openjdk-devel \
  libcurl-devel \
  libicu-devel \
  libtool \
  llvm13 \
  llvm13-devel \
  maven \
  ncurses-devel \
  ninja-build \
  perl-core \
  rh-python38 \
  rh-python38-scldevel \
  rh-ruby30* \
  sqlite-devel \
  tar \
  vala-devel \
  wget \
  which \
  zlib-devel

chmod +x get-openssl.sh
scl enable rh-python38 devtoolset-11 rh-ruby30 ./get-openssl.sh

echo "use "
echo ">scl enable rh-python38 devtoolset-11 rh-ruby30 bash"
echo "to get the updated Python, GCC compilers and Ruby within your shell environment"
