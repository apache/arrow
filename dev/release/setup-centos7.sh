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
# verification Red Hat Enterprise Linux 7 clones in particular
# on CentOS 7

set -exu

yum -y update
yum -y groupinstall "Development Tools"
yum install -y centos-release-scl curl
cd /etc/pki/rpm-gpg
curl -O http://springdale.princeton.edu/data/springdale/7/x86_64/os/RPM-GPG-KEY-springdale
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
  sqlite-devel \
  tar \
  vala-devel \
  wget \
  which \
  zlib-devel
  
source scl_source enable devtoolset-11
source scl_source enable llvm-toolset-13.0
git clone https://github.com/sstephenson/rbenv.git ~/.rbenv
echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bash_profile
echo 'eval "$(rbenv init -)"' >> ~/.bash_profile
source  ~/.bash_profile
rbenv install 3.1.2
rbenv global 3.1.2

alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 \
--family cmake
scl enable rh-python36 bash
python -m pip install -U pip
wget https://www.openssl.org/source/openssl-3.0.5.tar.gz
tar -xf openssl-3.0.5.tar.gz
cd openssl-3.0.5
./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl
make
make install
# The command below is needed when building Arrow with Gandiva, so that
# the CentOS7 default Openssl version 1.0 is not used, but the newer
# version of Openssl that as just been installed is used by CMake
echo 'export OPENSSL_ROOT_DIR=/usr/local/openssl' >> ~/.bash_profile
source  ~/.bash_profile
