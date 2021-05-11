#!/bin/bash -ex
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

wget http://ftp.gnu.org/gnu/wget/wget-1.19.4.tar.gz
yum -y remove wget
tar -xzvf wget-1.19.4.tar.gz
pushd wget-1.19.4
./configure --with-ssl=openssl --with-libssl-prefix=/usr/lib64/openssl --prefix=/usr
make
make install
popd

rm -rf wget-1.19.4.tar.gz wget-1.19.4