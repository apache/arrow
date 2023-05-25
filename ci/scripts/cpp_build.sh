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
CONFIG_FILE=arm-be.config
pushd /opt/crosstool-ng
cp "/arrow/${CONFIG_FILE}" .config
/opt/crosstool-ng/bin/ct-ng build
popd

mkdir -p ${build_dir}
pushd ${build_dir}

cmake \
  -DARROW_FLIGHT=ON \
  -DARROW_GCS=OFF \
  -DARROW_MIMALLOC=OFF \
  -DARROW_ORC=OFF \
  -DARROW_PARQUET=OFF \
  -DARROW_S3=OFF \
  -DARROW_SUBSTRAIT=OFF \
  -DCMAKE_BUILD_PARALLEL_LEVEL=2 \
  -DCMAKE_TOOLCHAIN_FILE= ../\
  -DCMAKE_UNITY_BUILD=ON \
  -DPARQUET_BUILD_EXAMPLES=OFF \
  -DPARQUET_BUILD_EXECUTABLES=OFF \
  -DProtobuf_SOURCE=BUNDLED \
  -DgRPC_SOURCE=BUNDLED \
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
