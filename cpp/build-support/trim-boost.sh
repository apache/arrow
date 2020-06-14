#!/bin/bash
#
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
#

# This script is used to make the subset of boost that we actually use,
# so that we don't have to download the whole big boost project when we build
# boost from source.
#
# After running this script, run upload-boost.sh to put the bundle on bintray

set -eu

# if version is not defined by the caller, set a default.
: ${BOOST_VERSION:=1.71.0}
: ${BOOST_FILE:=boost_${BOOST_VERSION//./_}}
: ${BOOST_URL:=https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/${BOOST_FILE}.tar.gz}

# Arrow tests require these
BOOST_LIBS="system.hpp filesystem.hpp"
# Add these to be able to build those
BOOST_LIBS="$BOOST_LIBS config build boost_install headers log predef"
# Parquet needs this (if using gcc < 4.9)
BOOST_LIBS="$BOOST_LIBS regex.hpp"
# Gandiva needs these
BOOST_LIBS="$BOOST_LIBS multiprecision/cpp_int.hpp"
# These are for Thrift when Thrift_SOURCE=BUNDLED
BOOST_LIBS="$BOOST_LIBS algorithm/string.hpp locale.hpp noncopyable.hpp numeric/conversion/cast.hpp scope_exit.hpp typeof/incr_registration_group.hpp scoped_array.hpp shared_array.hpp tokenizer.hpp version.hpp"

if [ ! -d ${BOOST_FILE} ]; then
  curl -L "${BOOST_URL}" > ${BOOST_FILE}.tar.gz
  tar -xzf ${BOOST_FILE}.tar.gz
fi

pushd ${BOOST_FILE}

if [ ! -f "dist/bin/bcp" ]; then
  ./bootstrap.sh
  ./b2 tools/bcp
fi
mkdir -p ${BOOST_FILE}
./dist/bin/bcp ${BOOST_LIBS} ${BOOST_FILE}

tar -czf ${BOOST_FILE}.tar.gz ${BOOST_FILE}/
# Resulting tarball is in ${BOOST_FILE}/${BOOST_FILE}.tar.gz

popd
