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

set -e
set -x
set -o pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <arrow-src-dir>"
  exit 1
fi

source_dir=${1}

: ${ARROW_FLIGHT:=ON}
: ${ARROW_S3:=ON}
: ${CHECK_IMPORTS:=ON}
: ${CHECK_UNITTESTS:=ON}
: ${INSTALL_PYARROW:=ON}

export PYARROW_TEST_CYTHON=OFF
export PYARROW_TEST_DATASET=ON
export PYARROW_TEST_FLIGHT=${ARROW_FLIGHT}
export PYARROW_TEST_GANDIVA=OFF
export PYARROW_TEST_HDFS=ON
export PYARROW_TEST_ORC=ON
export PYARROW_TEST_PANDAS=ON
export PYARROW_TEST_PARQUET=ON
export PYARROW_TEST_PLASMA=ON
export PYARROW_TEST_S3=${ARROW_S3}
export PYARROW_TEST_TENSORFLOW=ON

export ARROW_TEST_DATA=${source_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/submodules/parquet-testing/data

if [ "${INSTALL_PYARROW}" == "ON" ]; then
  # Install the built wheels
  pip install ${source_dir}/python/repaired_wheels/*.whl
fi

if [ "${CHECK_IMPORTS}" == "ON" ]; then
  # Test that the modules are importable
  python -c "
import pyarrow
import pyarrow._hdfs
import pyarrow.csv
import pyarrow.dataset
import pyarrow.fs
import pyarrow.json
import pyarrow.orc
import pyarrow.parquet
import pyarrow.plasma
"
  if [ "${PYARROW_TEST_S3}" == "ON" ]; then
    python -c "import pyarrow._s3fs"
  fi
  if [ "${PYARROW_TEST_FLIGHT}" == "ON" ]; then
    python -c "import pyarrow.flight"
  fi
fi

if [ "${CHECK_UNITTESTS}" == "ON" ]; then
  # Install testing dependencies
  pip install -U -r ${source_dir}/python/requirements-wheel-test.txt
  # Execute unittest, test dependencies must be installed
  python -c 'import pyarrow; pyarrow.create_library_symlinks()'
  pytest -r s --pyargs pyarrow
fi
