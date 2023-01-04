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
test_dir=${1}/python/build/dist

export ARROW_SOURCE_DIR=${arrow_dir}
export ARROW_TEST_DATA=${arrow_dir}/testing/data
export PARQUET_TEST_DATA=${arrow_dir}/cpp/submodules/parquet-testing/data
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}
export DYLD_LIBRARY_PATH=${ARROW_HOME}/lib:${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}
export ARROW_GDB_SCRIPT=${arrow_dir}/cpp/gdb_arrow.py

# Enable some checks inside Python itself
export PYTHONDEVMODE=1

# Enable memory debug checks.
export ARROW_DEBUG_MEMORY_POOL=trap

# By default, force-test all optional components
: ${PYARROW_TEST_CUDA:=${ARROW_CUDA:-ON}}
: ${PYARROW_TEST_DATASET:=${ARROW_DATASET:-ON}}
: ${PYARROW_TEST_FLIGHT:=${ARROW_FLIGHT:-ON}}
: ${PYARROW_TEST_GANDIVA:=${ARROW_GANDIVA:-ON}}
: ${PYARROW_TEST_GCS:=${ARROW_GCS:-ON}}
: ${PYARROW_TEST_HDFS:=${ARROW_HDFS:-ON}}
: ${PYARROW_TEST_ORC:=${ARROW_ORC:-ON}}
: ${PYARROW_TEST_PARQUET:=${ARROW_PARQUET:-ON}}
: ${PYARROW_TEST_S3:=${ARROW_S3:-ON}}

export PYARROW_TEST_CUDA
export PYARROW_TEST_DATASET
export PYARROW_TEST_FLIGHT
export PYARROW_TEST_GANDIVA
export PYARROW_TEST_GCS
export PYARROW_TEST_HDFS
export PYARROW_TEST_ORC
export PYARROW_TEST_PARQUET
export PYARROW_TEST_S3

# Testing PyArrow
pytest -r s ${PYTEST_ARGS} --pyargs pyarrow
