#!/usr/bin/env bash

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

arrow_dir=${1}
arrow_install_dir=${2}
build_dir=${3}/java_jni
# The directory where the final binaries will be stored when scripts finish
dist_dir=${4}

prefix_dir="${build_dir}/java-jni"

echo "=== Clear output directories and leftovers ==="
# Clear output directories and leftovers
rm -rf ${build_dir}

echo "=== Building Arrow Java C Data Interface native library ==="
mkdir -p "${build_dir}"
pushd "${build_dir}"

case "$(uname)" in
  Linux)
    n_jobs=$(nproc)
    ;;
  Darwin)
    n_jobs=$(sysctl -n hw.ncpu)
    ;;
  *)
    n_jobs=${NPROC:-1}
    ;;
esac

: ${ARROW_JAVA_BUILD_TESTS:=${ARROW_BUILD_TESTS:-OFF}}
: ${CMAKE_BUILD_TYPE:=release}
cmake \
  -DARROW_JAVA_JNI_ENABLE_DATASET=${ARROW_DATASET:-OFF} \
  -DARROW_JAVA_JNI_ENABLE_GANDIVA=${ARROW_GANDIVA:-OFF} \
  -DARROW_JAVA_JNI_ENABLE_ORC=${ARROW_ORC:-OFF} \
  -DBUILD_TESTING=${ARROW_JAVA_BUILD_TESTS} \
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
  -DCMAKE_PREFIX_PATH=${arrow_install_dir} \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DCMAKE_INSTALL_PREFIX=${prefix_dir} \
  -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD:-OFF} \
  -DProtobuf_USE_STATIC_LIBS=ON \
  -GNinja \
  ${JAVA_JNI_CMAKE_ARGS:-} \
  ${arrow_dir}/java
export CMAKE_BUILD_PARALLEL_LEVEL=${n_jobs}
cmake --build . --config ${CMAKE_BUILD_TYPE}
if [ "${ARROW_JAVA_BUILD_TESTS}" = "ON" ]; then
  ctest \
    --output-on-failure \
    --parallel ${n_jobs} \
    --timeout 300
fi
cmake --build . --config ${CMAKE_BUILD_TYPE} --target install
popd

mkdir -p ${dist_dir}
# For Windows. *.dll are installed into bin/ on Windows.
if [ -d "${prefix_dir}/bin" ]; then
  mv ${prefix_dir}/bin/* ${dist_dir}/
else
  mv ${prefix_dir}/lib/* ${dist_dir}/
fi
