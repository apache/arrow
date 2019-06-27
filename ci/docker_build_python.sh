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

set -e

source_dir=${1:-/arrow/python}
build_dir=${2:-/build/python}

export PYARROW_CXXFLAGS=$CXXFLAGS
export PYARROW_CMAKE_GENERATOR=Ninja
export PYARROW_BUILD_TYPE=${PYARROW_BUILD_TYPE:-debug}

# Feature flags
export SETUPTOOLS_SCM_VERSION_WRITE_TO_PREFIX=$build_dir
export PYARROW_WITH_ORC=${PYARROW_WITH_ORC:-1}
export PYARROW_WITH_PARQUET=${PYARROW_WITH_PARQUET:-1}
export PYARROW_WITH_PLASMA=${PYARROW_WITH_PLASMA:-1}

# Build pyarrow
pushd ${source_dir}
  # hacky again, setuptools_scm writes _generated_version.py before pyarrow
  # directory is created by setuptools
  rm -rf ${build_dir}
  mkdir -p $build_dir/pyarrow

  relative_build_dir=$(realpath --relative-to=. $build_dir)

  # this is a nightmare, but prevents mutating the source directory
  # which is bind mounted as readonly
  python setup.py build_ext --build-temp $relative_build_dir \
                            --build-lib $relative_build_dir \
                  build_py --build-lib $relative_build_dir \
                  egg_info --egg-base $relative_build_dir \
                  install_lib --build-dir $relative_build_dir \
                  install --single-version-externally-managed \
                          --record $relative_build_dir/record.txt
popd
