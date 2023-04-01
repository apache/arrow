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

export PYARROW_CMAKE_GENERATOR=${CMAKE_GENERATOR:-Ninja}
export PYARROW_BUILD_TYPE=${CMAKE_BUILD_TYPE:-debug}

export PYARROW_WITH_ACERO=${ARROW_ACERO:-ON}
export PYARROW_WITH_CUDA=${ARROW_CUDA:-OFF}
export PYARROW_WITH_DATASET=${ARROW_DATASET:-ON}
export PYARROW_WITH_FLIGHT=${ARROW_FLIGHT:-OFF}
export PYARROW_WITH_GANDIVA=${ARROW_GANDIVA:-OFF}
export PYARROW_WITH_GCS=${ARROW_GCS:-OFF}
export PYARROW_WITH_HDFS=${ARROW_HDFS:-ON}
export PYARROW_WITH_ORC=${ARROW_ORC:-OFF}
export PYARROW_WITH_PARQUET=${ARROW_PARQUET:-OFF}
export PYARROW_WITH_PARQUET_ENCRYPTION=${PARQUET_REQUIRE_ENCRYPTION:-ON}
export PYARROW_WITH_S3=${ARROW_S3:-OFF}
export PYARROW_WITH_SUBSTRAIT=${ARROW_SUBSTRAIT:-OFF}

export PYARROW_PARALLEL=${n_jobs}

export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}

pushd ${source_dir}
# - Cannot call setup.py as it may install in the wrong directory
#   on Debian/Ubuntu (ARROW-15243).
# - Cannot use build isolation as we want to use specific dependency versions
#   (e.g. Numpy, Pandas) on some CI jobs.
${PYTHON:-python} -m pip install --no-deps --no-build-isolation -vv .
# Remove build artifacts from source directory
find build/ -user root -delete
popd

if [ "${BUILD_DOCS_PYTHON}" == "ON" ]; then
  ncpus=$(python -c "import os; print(os.cpu_count())")
  sphinx-build -b html -j ${ncpus} ${arrow_dir}/docs/source ${build_dir}/docs
fi
