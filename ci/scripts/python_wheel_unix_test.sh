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

: "${ARROW_AZURE:=ON}"
: "${ARROW_FLIGHT:=ON}"
: "${ARROW_GCS:=ON}"
: "${CHECK_IMPORTS:=ON}"
: "${ARROW_S3:=ON}"
: "${ARROW_SUBSTRAIT:=ON}"
: "${CHECK_WHEEL_CONTENT:=ON}"
: "${CHECK_UNITTESTS:=ON}"
: "${INSTALL_PYARROW:=ON}"

export PYARROW_TEST_ACERO=ON
export PYARROW_TEST_AZURE=${ARROW_AZURE}
export PYARROW_TEST_CYTHON=OFF
export PYARROW_TEST_DATASET=ON
export PYARROW_TEST_FLIGHT=${ARROW_FLIGHT}
export PYARROW_TEST_GANDIVA=OFF
export PYARROW_TEST_GCS=${ARROW_GCS}
export PYARROW_TEST_HDFS=ON
export PYARROW_TEST_ORC=ON
export PYARROW_TEST_PANDAS=ON
export PYARROW_TEST_PARQUET=ON
export PYARROW_TEST_PARQUET_ENCRYPTION=ON
export PYARROW_TEST_SUBSTRAIT=${ARROW_SUBSTRAIT}
export PYARROW_TEST_S3=${ARROW_S3}
export PYARROW_TEST_TENSORFLOW=ON

export ARROW_TEST_DATA=${source_dir}/testing/data
export PARQUET_TEST_DATA=${source_dir}/cpp/submodules/parquet-testing/data

if [ "${INSTALL_PYARROW}" == "ON" ]; then
  # TODO: We probably want to cover this case in isolation on it's own internal unit test.
  # Install the core wheel alone first: S3 ships in the separate pyarrow-s3
  # wheel, so check that S3 fails with an actionable message without it.
  python -m pip install "${source_dir}"/python/repaired_wheels/pyarrow-*.whl
  if [ "${ARROW_S3}" == "ON" ]; then
    python -c "
import pyarrow.fs as fs
try:
    fs.S3FileSystem
except ImportError as e:
    assert 'pyarrow-s3' in str(e), e
else:
    raise AssertionError('S3FileSystem available without pyarrow-s3')
try:
    fs.FileSystem.from_uri('s3://bucket/key?region=us-east-1')
except ValueError as e:
    assert 'pyarrow-s3' in str(e), e
else:
    raise AssertionError('from_uri resolved s3:// without pyarrow-s3')
"
    python -m pip install "${source_dir}"/python/repaired_wheels/pyarrow_s3-*.whl
    # With pyarrow-s3 installed, from_uri must work even if pyarrow.fs was
    # never imported (it loads pyarrow-s3 on demand).
    python -c "from pyarrow._fs import FileSystem; FileSystem.from_uri('s3://bucket/key?region=us-east-1')"
  fi
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
"
  if [ "${PYARROW_TEST_GCS}" == "ON" ]; then
    python -c "import pyarrow._gcsfs"
  fi
  if [ "${PYARROW_TEST_S3}" == "ON" ]; then
    # S3 ships in the separate pyarrow-s3 wheel; Load via pyarrow.fs.
    python -c "import pyarrow.fs; pyarrow.fs.S3FileSystem"
  fi
  if [ "${PYARROW_TEST_FLIGHT}" == "ON" ]; then
    python -c "import pyarrow.flight"
  fi
  if [ "${PYARROW_TEST_SUBSTRAIT}" == "ON" ]; then
    python -c "import pyarrow.substrait"
  fi
fi

if [ "${CHECK_VERSION}" == "ON" ]; then
  pyarrow_version=$(python -c "import pyarrow; print(pyarrow.__version__)")
  [ "${pyarrow_version}" = "${ARROW_VERSION}" ]
  arrow_cpp_version=$(python -c "import pyarrow; print(pyarrow.cpp_build_info.version)")
  [ "${arrow_cpp_version}" = "${ARROW_VERSION}" ]
fi

if [ "${CHECK_WHEEL_CONTENT}" == "ON" ]; then
  python "${source_dir}/ci/scripts/python_wheel_validate_contents.py" \
    --path "${source_dir}/python/repaired_wheels"
fi

if [ "${CHECK_UNITTESTS}" == "ON" ]; then
  # Install testing dependencies
  python -m pip install -U -r "${source_dir}/python/requirements-wheel-test.txt"

  # Execute unittest, test dependencies must be installed
  python -c 'import pyarrow; pyarrow.create_library_symlinks()'
  python -m pytest -r s --pyargs pyarrow
fi
