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

arrow_dir=${1}
build_dir=${2}

source_dir=${arrow_dir}/python
python_build_dir=${build_dir}/python

: ${BUILD_DOCS_PYTHON:=OFF}

if [ -x "$(command -v git)" ]; then
  git config --global --add safe.directory ${arrow_dir}
fi

if [ -n "${ARROW_PYTHON_VENV:-}" ]; then
  . "${ARROW_PYTHON_VENV}/bin/activate"
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

if [ ! -z "${CONDA_PREFIX}" ]; then
  echo -e "===\n=== Conda environment for build\n==="
  conda list
fi

PYARROW_WITH_ACERO=$(case "$ARROW_ACERO" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                     esac)
PYARROW_WITH_AZURE=$(case "$ARROW_AZURE" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                     esac)
PYARROW_WITH_CUDA=$(case "$ARROW_CUDA" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                    esac)
PYARROW_WITH_DATASET=$(case "$ARROW_DATASET" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "enabled" ;;
                       esac)
PYARROW_WITH_FLIGHT=$(case "$ARROW_FLIGHT" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                      esac)
PYARROW_WITH_GANDIVA=$(case "$ARROW_GANDIVA" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                       esac)
PYARROW_WITH_GCS=$(case "$ARROW_GCS" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                   esac)
PYARROW_WITH_HDFS=$(case "$ARROW_HDFS" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "enabled" ;;
                    esac)
PYARROW_WITH_ORC=$(case "$ARROW_ORC" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                   esac)
PYARROW_WITH_PARQUET=$(case "$ARROW_PARQUET" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                       esac)
PYARROW_WITH_PARQUET_ENCRYPTION=$(case "$PARQUET_REQUIRE_ENCRYPTION" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "enabled" ;;
                                  esac)
PYARROW_WITH_S3=$(case "$ARROW_S3" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                  esac)
PYARROW_WITH_SUBSTRAIT=$(case "$ARROW_SUBSTRAIT" in
                         ON) echo "enabled" ;;
                         OFF) echo "disabled" ;;
                         *) echo "auto" ;;
                    esac)

: ${CMAKE_PREFIX_PATH:=${ARROW_HOME}}
export CMAKE_PREFIX_PATH
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}

# https://github.com/apache/arrow/issues/41429
# TODO: We want to out-of-source build. This is a workaround. We copy
# all needed files to the build directory from the source directory
# and build in the build directory.
rm -rf ${python_build_dir}
cp -aL ${source_dir} ${python_build_dir}
pushd ${python_build_dir}
# - Cannot call setup.py as it may install in the wrong directory
#   on Debian/Ubuntu (ARROW-15243).
# - Cannot use build isolation as we want to use specific dependency versions
#   (e.g. Numpy, Pandas) on some CI jobs.

# The conda compilers package may mess with C{XX}_FLAGS in a way that interferes
# with the compiler
OLD_CFLAGS=$CFLAGS
OLD_CPPFLAGS=$CPPFLAGS
OLD_CXXFLAGS=$CXXFLAGS
export CFLAGS=
export CPPFLAGS=
export CXXFLAGS=

BUILD_TYPE=${CMAKE_BUILD_TYPE:-debug}
BUILD_TYPE=${BUILD_TYPE,,}  # Meson requires lowercase values

${PYTHON:-python} -m pip install --no-deps --no-build-isolation -vv . \
                  -Csetup-args="-Dbuildtype=${BUILD_TYPE}" \
                  -Csetup-args="-Dacero=${PYARROW_WITH_ACERO}" \
                  -Csetup-args="-Dazure=${PYARROW_WITH_AZURE}" \
                  -Csetup-args="-Dcuda=${PYARROW_WITH_CUDA}" \
                  -Csetup-args="-Ddataset=${PYARROW_WITH_DATASET}" \
                  -Csetup-args="-Dflight=${PYARROW_WITH_FLIGHT}" \
                  -Csetup-args="-Dgandiva=${PYARROW_WITH_GANDIVA}" \
                  -Csetup-args="-Dgcs=${PYARROW_WITH_GCS}" \
                  -Csetup-args="-Dhdfs=${PYARROW_WITH_HDFS}" \
                  -Csetup-args="-Dorc=${PYARROW_WITH_ORC}" \
                  -Csetup-args="-Dparquet=${PYARROW_WITH_PARQUET}" \
                  -Csetup-args="-Dparquet_encryption=${PYARROW_WITH_PARQUET_ENCRYPTION}" \
                  -Csetup-args="-Ds3=${PYARROW_WITH_S3}" \
                  -Csetup-args="-Dsubstrait=${PYARROW_WITH_SUBSTRAIT}" \
                  -Ccompile-args="-v" \
                  -Csetup-args="--pkg-config-path=${ARROW_HOME}/lib/pkgconfig"

export CFLAGS=$OLD_CFLAGS
export CPPFLAGS=$OLD_CPPFLAGS
export CXXFLAGS=$OLD_CXXFLAGS
popd

if [ "${BUILD_DOCS_PYTHON}" == "ON" ]; then
  # https://github.com/apache/arrow/issues/41429
  # TODO: We want to out-of-source build. This is a workaround.
  #
  # Copy docs/source because the "autosummary_generate = True"
  # configuration generates files to docs/source/python/generated/.
  rm -rf ${python_build_dir}/docs/source
  mkdir -p ${python_build_dir}/docs
  cp -a ${arrow_dir}/docs/source ${python_build_dir}/docs/
  rm -rf ${python_build_dir}/format
  cp -a ${arrow_dir}/format ${python_build_dir}/
  rm -rf ${python_build_dir}/cpp/examples
  mkdir -p ${python_build_dir}/cpp
  cp -a ${arrow_dir}/cpp/examples ${python_build_dir}/cpp/
  rm -rf ${python_build_dir}/ci
  cp -a ${arrow_dir}/ci/ ${python_build_dir}/
  ncpus=$(python -c "import os; print(os.cpu_count())")
  export ARROW_CPP_DOXYGEN_XML=${build_dir}/cpp/apidoc/xml
  pushd ${build_dir}
  sphinx-build \
    -b html \
    ${python_build_dir}/docs/source \
    ${build_dir}/docs
  popd
fi
