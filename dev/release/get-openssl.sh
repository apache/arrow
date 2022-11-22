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


# A script to install OpenSSL from source required for release
# verification Red Hat Enterprise Linux 7 rebuilds in particular
# on CentOS 7

set -exu

wget https://www.openssl.org/source/openssl-3.0.7.tar.gz
tar -xf openssl-3.0.7.tar.gz
cd openssl-3.0.7
./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl
make
make install
# The command below is needed when building Arrow, so that the 
# CentOS7 default Openssl version 1.0 is not used, but the newer
# version of Openssl that is installed by this script is used by
# CMake
echo 'export OPENSSL_ROOT_DIR=/usr/local/openssl' >> ~/.bash_profile
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/openssl/lib64' >> ~/.bash_profile
echo 'export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/openssl/lib64' >> ~/.bash_profile
echo "use "
echo "$source  ~/.bash_profile"
echo "to set paths for installed version of OpenSSL"
