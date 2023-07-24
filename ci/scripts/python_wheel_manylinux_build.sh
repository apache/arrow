#!/usr/bin/env bash
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

set -ex

function check_arrow_visibility {
    nm --demangle --dynamic /tmp/arrow-dist/lib/libarrow.so > nm_arrow.log

    # Filter out Arrow symbols and see if anything remains.
    # '_init' and '_fini' symbols may or not be present, we don't care.
    # (note we must ignore the grep exit status when no match is found)
    grep ' T ' nm_arrow.log | grep -v -E '(arrow|\b_init\b|\b_fini\b)' | cat - > visible_symbols.log

    if [[ -f visible_symbols.log && `cat visible_symbols.log | wc -l` -eq 0 ]]; then
        return 0
    else
        echo "== Unexpected symbols exported by libarrow.so =="
        cat visible_symbols.log
        echo "================================================"

        exit 1
    fi
}

echo "=== (${PYTHON_VERSION}) Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf /tmp/arrow-build
rm -rf /arrow/python/dist
rm -rf /arrow/python/build
rm -rf /arrow/python/repaired_wheels
rm -rf /arrow/python/pyarrow/*.so
rm -rf /arrow/python/pyarrow/*.so.*

echo "=== (${PYTHON_VERSION}) Building Arrow C++ libraries ==="
: ${ARROW_ACERO:=ON}
: ${ARROW_DATASET:=ON}
: ${ARROW_FLIGHT:=ON}
: ${ARROW_GANDIVA:=OFF}
: ${ARROW_GCS:=ON}
: ${ARROW_HDFS:=ON}
: ${ARROW_JEMALLOC:=ON}
: ${ARROW_MIMALLOC:=ON}
: ${ARROW_ORC:=ON}
: ${ARROW_PARQUET:=ON}
: ${PARQUET_REQUIRE_ENCRYPTION:=ON}
: ${ARROW_SUBSTRAIT:=ON}
: ${ARROW_S3:=ON}
: ${ARROW_TENSORFLOW:=ON}
: ${ARROW_WITH_BROTLI:=ON}
: ${ARROW_WITH_BZ2:=ON}
: ${ARROW_WITH_LZ4:=ON}
: ${ARROW_WITH_SNAPPY:=ON}
: ${ARROW_WITH_ZLIB:=ON}
: ${ARROW_WITH_ZSTD:=ON}
: ${CMAKE_BUILD_TYPE:=release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${CMAKE_GENERATOR:=Ninja}
: ${VCPKG_ROOT:=/opt/vcpkg}
: ${VCPKG_FEATURE_FLAGS:=-manifests}
: ${VCPKG_TARGET_TRIPLET:=${VCPKG_DEFAULT_TRIPLET:-x64-linux-static-${CMAKE_BUILD_TYPE}}}

if [[ "$(uname -m)" == arm* ]] || [[ "$(uname -m)" == aarch* ]]; then
    # Build jemalloc --with-lg-page=16 in order to make the wheel work on both
    # 4k and 64k page arm64 systems. For more context see
    # https://github.com/apache/arrow/issues/10929
    export ARROW_EXTRA_CMAKE_FLAGS="-DARROW_JEMALLOC_LG_PAGE=16"
fi

mkdir /tmp/arrow-build
pushd /tmp/arrow-build

# ARROW-17501: We can remove -DAWSSDK_SOURCE=BUNDLED once
# https://github.com/aws/aws-sdk-cpp/issues/1809 is fixed and vcpkg
# ships the fix.
cmake \
    -DARROW_ACERO=${ARROW_ACERO} \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=${ARROW_DATASET} \
    -DARROW_DEPENDENCY_SOURCE="VCPKG" \
    -DARROW_DEPENDENCY_USE_SHARED=OFF \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=${ARROW_FLIGHT} \
    -DARROW_GANDIVA=${ARROW_GANDIVA} \
    -DARROW_GCS=${ARROW_GCS} \
    -DARROW_HDFS=${ARROW_HDFS} \
    -DARROW_JEMALLOC=${ARROW_JEMALLOC} \
    -DARROW_JSON=ON \
    -DARROW_MIMALLOC=${ARROW_MIMALLOC} \
    -DARROW_ORC=${ARROW_ORC} \
    -DARROW_PACKAGE_KIND="python-wheel-manylinux${MANYLINUX_VERSION}" \
    -DARROW_PARQUET=${ARROW_PARQUET} \
    -DARROW_RPATH_ORIGIN=ON \
    -DARROW_S3=${ARROW_S3} \
    -DARROW_SUBSTRAIT=${ARROW_SUBSTRAIT} \
    -DARROW_TENSORFLOW=${ARROW_TENSORFLOW} \
    -DARROW_USE_CCACHE=ON \
    -DARROW_WITH_BROTLI=${ARROW_WITH_BROTLI} \
    -DARROW_WITH_BZ2=${ARROW_WITH_BZ2} \
    -DARROW_WITH_LZ4=${ARROW_WITH_LZ4} \
    -DARROW_WITH_SNAPPY=${ARROW_WITH_SNAPPY} \
    -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB} \
    -DARROW_WITH_ZSTD=${ARROW_WITH_ZSTD} \
    -DAWSSDK_SOURCE=BUNDLED \
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=/tmp/arrow-dist \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
    -DORC_PROTOBUF_EXECUTABLE=${VCPKG_ROOT}/installed/${VCPKG_TARGET_TRIPLET}/tools/protobuf/protoc \
    -DORC_SOURCE=BUNDLED \
    -DPARQUET_REQUIRE_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION} \
    -DVCPKG_MANIFEST_MODE=OFF \
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET} \
    ${ARROW_EXTRA_CMAKE_FLAGS} \
    -G ${CMAKE_GENERATOR} \
    /arrow/cpp
cmake --build . --target install
popd

# Check that we don't expose any unwanted symbols
check_arrow_visibility

echo "=== (${PYTHON_VERSION}) Building wheel ==="
export PYARROW_BUILD_TYPE=${CMAKE_BUILD_TYPE}
export PYARROW_BUNDLE_ARROW_CPP=1
export PYARROW_CMAKE_GENERATOR=${CMAKE_GENERATOR}
export PYARROW_INSTALL_TESTS=1
export PYARROW_WITH_ACERO=${ARROW_ACERO}
export PYARROW_WITH_DATASET=${ARROW_DATASET}
export PYARROW_WITH_FLIGHT=${ARROW_FLIGHT}
export PYARROW_WITH_GANDIVA=${ARROW_GANDIVA}
export PYARROW_WITH_GCS=${ARROW_GCS}
export PYARROW_WITH_HDFS=${ARROW_HDFS}
export PYARROW_WITH_ORC=${ARROW_ORC}
export PYARROW_WITH_PARQUET=${ARROW_PARQUET}
export PYARROW_WITH_PARQUET_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION}
export PYARROW_WITH_SUBSTRAIT=${ARROW_SUBSTRAIT}
export PYARROW_WITH_S3=${ARROW_S3}
export ARROW_HOME=/tmp/arrow-dist
# PyArrow build configuration
export CMAKE_PREFIX_PATH=/tmp/arrow-dist

pushd /arrow/python
python setup.py bdist_wheel

echo "=== (${PYTHON_VERSION}) Tag the wheel with manylinux${MANYLINUX_VERSION} ==="
auditwheel repair -L . dist/pyarrow-*.whl -w repaired_wheels
popd
