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

case $# in
  1) KIND="$1"
     case $KIND in
       imports|unittests) ;;
       *) echo "Invalid argument: '${KIND}', valid options are 'imports', 'unittests'"
          exit 1
          ;;
     esac
     ;;
  *) echo "Usage: $0 imports|unittests"
     exit 1
     ;;
esac

export PYARROW_TEST_CYTHON=OFF
export PYARROW_TEST_DATASET=ON
export PYARROW_TEST_GANDIVA=OFF
export PYARROW_TEST_HDFS=ON
export PYARROW_TEST_ORC=ON
export PYARROW_TEST_PANDAS=ON
export PYARROW_TEST_PARQUET=ON
export PYARROW_TEST_PLASMA=ON
export PYARROW_TEST_S3=ON
export PYARROW_TEST_TENSORFLOW=ON
export PYARROW_TEST_FLIGHT=ON

export ARROW_TEST_DATA=/arrow/testing/data
export PARQUET_TEST_DATA=/arrow/submodules/parquet-testing/data

# Install the built wheels
pip install /arrow/python/repaired_wheels/*.whl

if [ "${KIND}" == "imports" ]; then
  # Test that the modules are importable
  python -c "
import pyarrow
import pyarrow._hdfs
import pyarrow._s3fs
import pyarrow.csv
import pyarrow.dataset
import pyarrow.flight
import pyarrow.fs
import pyarrow.json
import pyarrow.orc
import pyarrow.parquet
import pyarrow.plasma"
elif [ "${KIND}" == "unittests" ]; then
  # Execute unittest, test dependencies must be installed
  pytest -r s --pyargs pyarrow
fi
