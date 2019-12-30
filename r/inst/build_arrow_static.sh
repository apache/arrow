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

# Quit on failure
# set -e

# Print commands for debugging
set -x

# By default, this script assumes it's in the top-level dir of the apache/arrow
# git repository. Set any of the following env vars to customize where to read
# and write from
: ${ARROW_HOME:="$(pwd)"}                       # Only used in default SOURCE/BUILD dirs
: ${SOURCE_DIR:="${ARROW_HOME}/cpp"}            # Where the C++ source is
: ${BUILD_DIR:="${ARROW_HOME}/r/libarrow/dist"} # Where cmake should build
: ${DEST_DIR:="$BUILD_DIR"}                     # Where the resulting /lib and /include should be
: ${CMAKE:="$(which cmake)"}

# Make sure SOURCE and DEST dirs are absolute and exist
SOURCE_DIR="$(cd "${SOURCE_DIR}" && pwd)"
DEST_DIR="$(mkdir -p "${DEST_DIR}" && cd "${DEST_DIR}" && pwd)"

if [ "$CMAKE_GENERATOR" = "" ]; then
  # Look for ninja, prefer it
  ninja --version >/dev/null 2>&1
  if ninja --version >/dev/null 2>&1; then
    CMAKE_GENERATOR="Ninja"
  fi
fi

if [ "$FLEX_ROOT" != "" ]; then
  EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} -DFLEX_ROOT=${FLEX_ROOT}"
fi

mkdir -p "${BUILD_DIR}"
pushd "${BUILD_DIR}"
${CMAKE} -DCMAKE_BUILD_TYPE=Release \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DCMAKE_INSTALL_PREFIX=${DEST_DIR} \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_BOOST_USE_SHARED=OFF \
    -DARROW_JEMALLOC=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_DATASET=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_BROTLI=ON \
    -DOPENSSL_USE_STATIC_LIBS=ON \
    ${EXTRA_CMAKE_FLAGS} \
    -G ${CMAKE_GENERATOR:-"Unix Makefiles"} \
    ${SOURCE_DIR}
${CMAKE} --build . --target install

# if [ $? -ne 0 ]; then
#   # FOR TEST DEBUGGING
#   cp -r ./* /home/docker
# fi

# Copy the bundled static libs from the build to the install dir
find . -regex .*/.*/lib/.*\\.a\$ | xargs -I{} cp -u {} ${DEST_DIR}/lib
popd

# TODO: put the build step in its own script, not needed for R package source build
# pushd ${DEST_DIR}
# zip -r libarrow.zip lib/*.a include/
# popd
