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

echo "INSTALL SUBSTRAIT CONSUMER TEST SUITE";

git clone https://github.com/substrait-io/consumer-testing.git
# pip install duckdb \
#             filelock \
#             ibis-framework \
#             ibis-substrait \
#             protobuf \
#             pytest \
#             pytest-xdist \
#             substrait-validator
echo "PYARROW PRE INSTALLED"
echo $(pip list | grep pyarrow)
pip install -r consumer-testing/requirements.txt --ignore-installed
echo "PYARROW POST INSTALLED"
echo $(pip list | grep pyarrow)
# TODO: write a better installation and testing script

python -c "import pyarrow.substrait"
python -c "from tests.consumers import AceroConsumer"

cd consumer-testing && \
    pytest tests/integration/test_acero_tpch.py

#dask=$1

#if [ "${dask}" = "upstream_devel" ]; then
#  pip install https://github.com/dask/dask/archive/main.tar.gz#egg=dask[dataframe]
#elif [ "${dask}" = "latest" ]; then
#  pip install dask[dataframe]
#else
#  pip install dask[dataframe]==${dask}
#fi

# additional dependencies needed for dask's s3 tests
#pip install moto[server] flask requests
