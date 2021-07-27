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
set -e

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

# Make some env vars case-insensitive
LIBARROW_MINIMAL=`echo $LIBARROW_MINIMAL | tr '[:upper:]' '[:lower:]'`

if [ "$LIBARROW_MINIMAL" = "false" ]; then
  ARROW_DEFAULT_PARAM="ON"
else
  ARROW_DEFAULT_PARAM="OFF"
fi

mkdir -p "${BUILD_DIR}"
pushd "${BUILD_DIR}"
${CMAKE} -DARROW_BOOST_USE_SHARED=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_SHARED=OFF \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=${ARROW_DATASET:-ON} \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DARROW_FILESYSTEM=ON \
    -DARROW_JEMALLOC=${ARROW_JEMALLOC:-$ARROW_DEFAULT_PARAM} \
    -DARROW_MIMALLOC=${ARROW_MIMALLOC:-ON} \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=${ARROW_PARQUET:-ON} \
    -DARROW_S3=${ARROW_S3:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_BROTLI=${ARROW_WITH_BROTLI:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_BZ2=${ARROW_WITH_BZ2:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_LZ4=${ARROW_WITH_LZ4:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_RE2=${ARROW_WITH_RE2:-ON} \
    -DARROW_WITH_SNAPPY=${ARROW_WITH_SNAPPY:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_UTF8PROC=${ARROW_WITH_UTF8PROC:-ON} \
    -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB:-$ARROW_DEFAULT_PARAM} \
    -DARROW_WITH_ZSTD=${ARROW_WITH_ZSTD:-$ARROW_DEFAULT_PARAM} \
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release} \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=${DEST_DIR} \
    -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON \
    -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD:-ON} \
    ${EXTRA_CMAKE_FLAGS} \
    -G ${CMAKE_GENERATOR:-"Unix Makefiles"} \
    ${SOURCE_DIR}
${CMAKE} --build . --target install
popd
