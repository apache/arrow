#!/bin/bash -ex
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

wget --no-check-certificate https://www.openssl.org/source/openssl-1.0.2k.tar.gz -O openssl-1.0.2k.tar.gz
tar xf openssl-1.0.2k.tar.gz
pushd openssl-1.0.2k
./config -fpic shared --prefix=/usr
make -j5
make install
popd
rm -rf openssl-1.0.2k.tar.gz openssl-1.0.2k
