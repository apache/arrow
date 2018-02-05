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

BOOST_VERSION=1.65.1
BOOST_VERSION_UNDERSCORE=${BOOST_VERSION//\./_}

wget --no-check-certificate https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORE}.tar.gz -O /boost_${BOOST_VERSION_UNDERSCORE}.tar.gz
tar xf boost_${BOOST_VERSION_UNDERSCORE}.tar.gz
pushd /boost_${BOOST_VERSION_UNDERSCORE}
./bootstrap.sh
./bjam cxxflags=-fPIC cflags=-fPIC variant=release link=static --prefix=/usr --with-filesystem --with-date_time --with-system --with-regex install
popd
rm -rf boost_${BOOST_VERSION_UNDERSCORE}.tar.gz boost_${BOOST_VERSION_UNDERSCORE}
# Boost always install header-only parts but they also take up quite some space.
# We don't need them in array, so don't persist them in the docker layer.
# phoenix 18.1 MiB
rm -r /usr/include/boost/phoenix
# fusion 16.7 MiB
rm -r /usr/include/boost/fusion
# spirit 8.2 MiB
rm -r /usr/include/boost/spirit
# geometry 6.0 MiB
rm -r /usr/include/boost/geometry
