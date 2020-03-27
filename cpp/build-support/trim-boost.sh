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

set -eu

BOOST_VERSION=1.71.0
# These are the ones Arrow uses directly
BOOST_LIBS="system.hpp filesystem.hpp regex.hpp"
# Add these to be able to build those
BOOST_LIBS="$BOOST_LIBS config build boost_install headers"
# Maybe log is only needed for debug build? (predef is needed for log)
BOOST_LIBS="$BOOST_LIBS log predef"
# Gandiva needs this
BOOST_LIBS="$BOOST_LIBS optional.hpp"
# These are for Thrift
BOOST_LIBS="$BOOST_LIBS algorithm/string.hpp locale.hpp noncopyable.hpp numeric/conversion/cast.hpp scope_exit.hpp scoped_array.hpp shared_array.hpp tokenizer.hpp version.hpp"

BOOST_VERSION_=`echo $BOOST_VERSION | sed -e 's/\./_/g'`
BOOST_FILE="boost_${BOOST_VERSION_}"

curl -L https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/${BOOST_FILE}.tar.gz > ${BOOST_FILE}.tar.gz
tar -xzf ${BOOST_FILE}.tar.gz
cd ${BOOST_FILE}

./bootstrap.sh
./b2 tools/bcp
mkdir ${BOOST_FILE}
./dist/bin/bcp ${BOOST_LIBS} ${BOOST_FILE}

# Resulting tarball is in ${BOOST_FILE}/${BOOST_FILE}.tar.gz
tar -czf ${BOOST_FILE}.tar.gz ${BOOST_FILE}/
