#!/bin/bash

set -ex

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
import pyarrow.plasma
"

# Install testing dependencies
pip install -r /arrow/python/requirements-wheel-test.txt

# Execute unittest
pytest -r s --pyargs pyarrow
