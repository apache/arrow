#!/bin/bash

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

export ARROW_TEST_DATA=/arrow/testing/data
export PYARROW_TEST_CYTHON=OFF

python --version
# Install built wheel
pip install -q /arrow/python/$WHEEL_DIR/dist/*.whl
# Install test dependencies
pip install -q -r /arrow/python/requirements-wheel-test.txt
# Run pyarrow tests
pytest -rs --pyargs pyarrow

if [[ "$1" == "--remove-system-libs" ]]; then
  # Run import tests after removing the bundled dependencies from the system
  # (be careful not to remove libz.so, required for some Python stdlib modules)
  echo "Removing the following libraries to fail loudly if they are bundled incorrectly:"
  ldconfig -p | grep "lib\(bz2\|lz4\|z[^.]\|boost\)" | awk -F'> ' '{print $2}' | sort | xargs rm -v -f
fi

# Test import and optional dependencies
python -c "
import pyarrow
import pyarrow.csv
import pyarrow.dataset
import pyarrow.flight
import pyarrow.fs
import pyarrow._hdfs
import pyarrow.json
import pyarrow.parquet
import pyarrow.plasma
import pyarrow._s3fs
"
