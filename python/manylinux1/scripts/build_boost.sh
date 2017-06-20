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


wget --no-check-certificate http://downloads.sourceforge.net/project/boost/boost/1.60.0/boost_1_60_0.tar.gz -O /boost_1_60_0.tar.gz
tar xf boost_1_60_0.tar.gz
pushd /boost_1_60_0
./bootstrap.sh
./bjam cxxflags=-fPIC cflags=-fPIC --prefix=/usr --with-filesystem --with-date_time --with-system --with-regex install
popd
rm -rf boost_1_60_0.tar.gz boost_1_60_0
