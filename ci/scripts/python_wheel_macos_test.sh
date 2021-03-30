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

source_dir=${1}

: ${ARROW_S3:=ON}

export PYARROW_TEST_CYTHON=OFF
export PYARROW_TEST_DATASET=ON
export PYARROW_TEST_GANDIVA=OFF
export PYARROW_TEST_HDFS=ON
export PYARROW_TEST_ORC=ON
export PYARROW_TEST_PANDAS=ON
export PYARROW_TEST_PARQUET=ON
export PYARROW_TEST_PLASMA=ON
export PYARROW_TEST_S3=${ARROW_S3}
export PYARROW_TEST_TENSORFLOW=ON
export PYARROW_TEST_FLIGHT=ON

export ARROW_TEST_DATA=${source_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data

# Install the built wheels
pip install ${source_dir}/python/dist/*.whl

# Test that the modules are importable
python -c "
import pyarrow
import pyarrow._hdfs
import pyarrow.csv
import pyarrow.dataset
import pyarrow.flight
import pyarrow.fs
import pyarrow.json
import pyarrow.orc
import pyarrow.parquet
import pyarrow.plasma
"

if [ "${PYARROW_TEST_S3}" == "ON" ]; then
  python -c "import pyarrow._s3fs"
fi

# Install testing dependencies
pip install -r ${source_dir}/python/requirements-wheel-test.txt

# Execute unittest
pytest -r s --pyargs pyarrow
