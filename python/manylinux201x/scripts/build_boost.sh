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

BOOST_VERSION=1.73.0
BOOST_VERSION_UNDERSCORE=${BOOST_VERSION//\./_}
NCORES=$(($(grep -c ^processor /proc/cpuinfo) + 1))

BASE_NAME=boost_${BOOST_VERSION_UNDERSCORE}
ARCHIVE_NAME=boost_${BOOST_VERSION_UNDERSCORE}.tar.bz2

# Bintray is much faster but can fail because of limitations
curl -sL https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORE}.tar.bz2 -o /${ARCHIVE_NAME} \
    || curl -sL https://sourceforge.net/projects/boost/files/boost/${BOOST_VERSION}/boost_${BOOST_VERSION_UNDERSCORE}.tar.bz2 -o /${ARCHIVE_NAME}

tar xf ${ARCHIVE_NAME}
mkdir /arrow_boost
pushd /${BASE_NAME}
./bootstrap.sh
./b2 -j${NCORES} tools/bcp
./dist/bin/bcp --namespace=arrow_boost --namespace-alias filesystem date_time system regex build predef algorithm locale format variant multiprecision/cpp_int /arrow_boost
popd

pushd /arrow_boost
ls -l
./bootstrap.sh
./b2 -j${NCORES} dll-path="'\$ORIGIN/'" cxxflags='-std=c++11 -fPIC' cflags=-fPIC linkflags="-std=c++11" variant=release link=shared --prefix=/arrow_boost_dist --with-filesystem --with-date_time --with-system --with-regex install
popd

rm -rf ${ARCHIVE_NAME} ${BASE_NAME} arrow_boost
# Boost always install header-only parts but they also take up quite some space.
# We don't need them in array, so don't persist them in the docker layer.
# fusion 16.7 MiB
rm -r /arrow_boost_dist/include/boost/fusion
# spirit 8.2 MiB
rm -r /arrow_boost_dist/include/boost/spirit
