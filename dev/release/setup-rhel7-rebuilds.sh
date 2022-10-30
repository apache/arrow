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
releasever="$(rpm --query --file --queryformat '%{VERSION}\n' /etc/system-release)";
basearch="$(rpm --query --queryformat '%{ARCH}\n' "kernel-$(uname --kernel-release)")";
cd /etc/pki/rpm-gpg
curl -O http://springdale.princeton.edu/data/springdale/$releasever/$basearch/os/RPM-GPG-KEY-springdale
cd /etc/yum.repos.d
cat << EOF > Springdale-SCL.repo
[Springdale-SCL]
name=Springdale - SCL
baseurl=http://springdale.princeton.edu/data/springdale/SCL/$releasever/$basearch
gpgcheck=1
enabled=1
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-springdale
EOF
cd $HOME
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
  llvm-toolset-13.0-* \
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

alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 \
  --slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
  --slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
  --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 \
  --family cmake
scl enable rh-python38 devtoolset-11 llvm-toolset-13.0 rh-ruby30 bash
python -m pip install -U pip
wget https://www.openssl.org/source/openssl-3.0.5.tar.gz
tar -xf openssl-3.0.5.tar.gz
cd openssl-3.0.5
./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl
make
make install
# The command below is needed when building Arrow, so that the 
# CentOS7 default Openssl version 1.0 is not used, but the newer
# version of Openssl that as just been installed is used by CMake
echo 'export OPENSSL_ROOT_DIR=/usr/local/openssl' >> ~/.bash_profile
source  ~/.bash_profile
