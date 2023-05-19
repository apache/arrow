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

source_dir=${1}/cpp
build_dir=${2}/cpp

: ${ARROW_USE_CCACHE:=OFF}
: ${BUILD_DOCS_CPP:=OFF}

if [ -x "$(command -v git)" ]; then
  git config --global --add safe.directory ${1}
fi

# TODO(kszucs): consider to move these to CMake
if [ ! -z "${CONDA_PREFIX}" ]; then
  echo -e "===\n=== Conda environment for build\n==="
  conda list

  export CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_AR=${AR} -DCMAKE_RANLIB=${RANLIB}"
  export ARROW_GANDIVA_PC_CXX_FLAGS=$(echo | ${CXX} -E -Wp,-v -xc++ - 2>&1 | grep '^ ' | awk '{print "-isystem;" substr($1, 1)}' | tr '\n' ';')
elif [ -x "$(command -v xcrun)" ]; then
  export ARROW_GANDIVA_PC_CXX_FLAGS="-isysroot;$(xcrun --show-sdk-path)"
fi

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
    echo -e "===\n=== ccache statistics before build\n==="
    ccache -sv 2>/dev/null || ccache -s
fi

if [ "${ARROW_USE_TSAN}" == "ON" ] && [ ! -x "${ASAN_SYMBOLIZER_PATH}" ]; then
    echo -e "Invalid value for \$ASAN_SYMBOLIZER_PATH: ${ASAN_SYMBOLIZER_PATH}"
    exit 1
fi

case "$(uname)" in
  Linux)
    n_jobs=$(nproc)
    ;;
  Darwin)
    n_jobs=$(sysctl -n hw.ncpu)
    ;;
  MINGW*)
    n_jobs=${NUMBER_OF_PROCESSORS:-1}
    ;;
  *)
    n_jobs=${NPROC:-1}
    ;;
esac

mkdir -p ${build_dir}
pushd ${build_dir}

cmake \
  -Dabsl_SOURCE=${absl_SOURCE:-} \
  -DARROW_ACERO=${ARROW_ACERO:-ON} \
  -DARROW_BOOST_USE_SHARED=${ARROW_BOOST_USE_SHARED:-ON} \
  -DARROW_BUILD_BENCHMARKS_REFERENCE=${ARROW_BUILD_BENCHMARKS:-OFF} \
  -DARROW_BUILD_BENCHMARKS=${ARROW_BUILD_BENCHMARKS:-OFF} \
  -DARROW_BUILD_EXAMPLES=${ARROW_BUILD_EXAMPLES:-OFF} \
  -DARROW_BUILD_INTEGRATION=${ARROW_BUILD_INTEGRATION:-OFF} \
  -DARROW_BUILD_SHARED=${ARROW_BUILD_SHARED:-ON} \
  -DARROW_BUILD_STATIC=${ARROW_BUILD_STATIC:-ON} \
  -DARROW_BUILD_TESTS=${ARROW_BUILD_TESTS:-OFF} \
  -DARROW_BUILD_UTILITIES=${ARROW_BUILD_UTILITIES:-ON} \
  -DARROW_COMPUTE=${ARROW_COMPUTE:-ON} \
  -DARROW_CSV=${ARROW_CSV:-ON} \
  -DARROW_CUDA=${ARROW_CUDA:-OFF} \
  -DARROW_CXXFLAGS=${ARROW_CXXFLAGS:-} \
  -DARROW_DATASET=${ARROW_DATASET:-ON} \
  -DARROW_DEPENDENCY_SOURCE=${ARROW_DEPENDENCY_SOURCE:-AUTO} \
  -DARROW_ENABLE_TIMING_TESTS=${ARROW_ENABLE_TIMING_TESTS:-ON} \
  -DARROW_EXTRA_ERROR_CONTEXT=${ARROW_EXTRA_ERROR_CONTEXT:-OFF} \
  -DARROW_FILESYSTEM=${ARROW_FILESYSTEM:-ON} \
  -DARROW_FLIGHT=${ARROW_FLIGHT:-OFF} \
  -DARROW_FLIGHT_SQL=${ARROW_FLIGHT_SQL:-OFF} \
  -DARROW_FUZZING=${ARROW_FUZZING:-OFF} \
  -DARROW_GANDIVA_PC_CXX_FLAGS=${ARROW_GANDIVA_PC_CXX_FLAGS:-} \
  -DARROW_GANDIVA=${ARROW_GANDIVA:-OFF} \
  -DARROW_GCS=${ARROW_GCS:-OFF} \
  -DARROW_HDFS=${ARROW_HDFS:-ON} \
  -DARROW_INSTALL_NAME_RPATH=${ARROW_INSTALL_NAME_RPATH:-ON} \
  -DARROW_JEMALLOC=${ARROW_JEMALLOC:-ON} \
  -DARROW_JSON=${ARROW_JSON:-ON} \
  -DARROW_LARGE_MEMORY_TESTS=${ARROW_LARGE_MEMORY_TESTS:-OFF} \
  -DARROW_MIMALLOC=${ARROW_MIMALLOC:-OFF} \
  -DARROW_NO_DEPRECATED_API=${ARROW_NO_DEPRECATED_API:-OFF} \
  -DARROW_ORC=${ARROW_ORC:-OFF} \
  -DARROW_PARQUET=${ARROW_PARQUET:-OFF} \
  -DARROW_RUNTIME_SIMD_LEVEL=${ARROW_RUNTIME_SIMD_LEVEL:-MAX} \
  -DARROW_S3=${ARROW_S3:-OFF} \
  -DARROW_SKYHOOK=${ARROW_SKYHOOK:-OFF} \
  -DARROW_SUBSTRAIT=${ARROW_SUBSTRAIT:-ON} \
  -DARROW_TEST_LINKAGE=${ARROW_TEST_LINKAGE:-shared} \
  -DARROW_TEST_MEMCHECK=${ARROW_TEST_MEMCHECK:-OFF} \
  -DARROW_USE_ASAN=${ARROW_USE_ASAN:-OFF} \
  -DARROW_USE_CCACHE=${ARROW_USE_CCACHE:-ON} \
  -DARROW_USE_GLOG=${ARROW_USE_GLOG:-OFF} \
  -DARROW_USE_LD_GOLD=${ARROW_USE_LD_GOLD:-OFF} \
  -DARROW_USE_PRECOMPILED_HEADERS=${ARROW_USE_PRECOMPILED_HEADERS:-OFF} \
  -DARROW_USE_STATIC_CRT=${ARROW_USE_STATIC_CRT:-OFF} \
  -DARROW_USE_TSAN=${ARROW_USE_TSAN:-OFF} \
  -DARROW_USE_UBSAN=${ARROW_USE_UBSAN:-OFF} \
  -DARROW_VERBOSE_THIRDPARTY_BUILD=${ARROW_VERBOSE_THIRDPARTY_BUILD:-OFF} \
  -DARROW_WITH_BROTLI=${ARROW_WITH_BROTLI:-OFF} \
  -DARROW_WITH_BZ2=${ARROW_WITH_BZ2:-OFF} \
  -DARROW_WITH_LZ4=${ARROW_WITH_LZ4:-OFF} \
  -DARROW_WITH_OPENTELEMETRY=${ARROW_WITH_OPENTELEMETRY:-OFF} \
  -DARROW_WITH_MUSL=${ARROW_WITH_MUSL:-OFF} \
  -DARROW_WITH_SNAPPY=${ARROW_WITH_SNAPPY:-OFF} \
  -DARROW_WITH_UCX=${ARROW_WITH_UCX:-OFF} \
  -DARROW_WITH_UTF8PROC=${ARROW_WITH_UTF8PROC:-ON} \
  -DARROW_WITH_ZLIB=${ARROW_WITH_ZLIB:-OFF} \
  -DARROW_WITH_ZSTD=${ARROW_WITH_ZSTD:-OFF} \
  -DAWSSDK_SOURCE=${AWSSDK_SOURCE:-} \
  -Dbenchmark_SOURCE=${benchmark_SOURCE:-} \
  -DBOOST_SOURCE=${BOOST_SOURCE:-} \
  -DBrotli_SOURCE=${Brotli_SOURCE:-} \
  -DBUILD_WARNING_LEVEL=${BUILD_WARNING_LEVEL:-CHECKIN} \
  -Dc-ares_SOURCE=${cares_SOURCE:-} \
  -DCMAKE_BUILD_TYPE=${ARROW_BUILD_TYPE:-debug} \
  -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE:-OFF} \
  -DCMAKE_C_FLAGS="${CFLAGS:-}" \
  -DCMAKE_CXX_FLAGS="${CXXFLAGS:-}" \
  -DCMAKE_CXX_STANDARD="${CMAKE_CXX_STANDARD:-17}" \
  -DCMAKE_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR:-lib} \
  -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX:-${ARROW_HOME}} \
  -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD:-OFF} \
  -Dgflags_SOURCE=${gflags_SOURCE:-} \
  -Dgoogle_cloud_cpp_storage_SOURCE=${google_cloud_cpp_storage_SOURCE:-} \
  -DgRPC_SOURCE=${gRPC_SOURCE:-} \
  -DGTest_SOURCE=${GTest_SOURCE:-} \
  -Dlz4_SOURCE=${lz4_SOURCE:-} \
  -DORC_SOURCE=${ORC_SOURCE:-} \
  -DPARQUET_BUILD_EXAMPLES=${PARQUET_BUILD_EXAMPLES:-OFF} \
  -DPARQUET_BUILD_EXECUTABLES=${PARQUET_BUILD_EXECUTABLES:-OFF} \
  -DPARQUET_REQUIRE_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION:-ON} \
  -DProtobuf_SOURCE=${Protobuf_SOURCE:-} \
  -DRapidJSON_SOURCE=${RapidJSON_SOURCE:-} \
  -Dre2_SOURCE=${re2_SOURCE:-} \
  -DSnappy_SOURCE=${Snappy_SOURCE:-} \
  -DThrift_SOURCE=${Thrift_SOURCE:-} \
  -Dutf8proc_SOURCE=${utf8proc_SOURCE:-} \
  -Dzstd_SOURCE=${zstd_SOURCE:-} \
  -Dxsimd_SOURCE=${xsimd_SOURCE:-} \
  -G "${CMAKE_GENERATOR:-Ninja}" \
  ${CMAKE_ARGS} \
  ${source_dir}

export CMAKE_BUILD_PARALLEL_LEVEL=${CMAKE_BUILD_PARALLEL_LEVEL:-$[${n_jobs} + 1]}
time cmake --build . --target install

popd

if [ -x "$(command -v ldconfig)" ]; then
  ldconfig ${ARROW_HOME}/${CMAKE_INSTALL_LIBDIR:-lib}
fi

if [ "${ARROW_USE_CCACHE}" == "ON" ]; then
    echo -e "===\n=== ccache statistics after build\n==="
    ccache -sv 2>/dev/null || ccache -s
fi

if command -v sccache &> /dev/null; then
  echo "=== sccache stats after the build ==="
  sccache --show-stats
fi

if [ "${BUILD_DOCS_CPP}" == "ON" ]; then
  pushd ${source_dir}/apidoc
  doxygen
  popd
fi
