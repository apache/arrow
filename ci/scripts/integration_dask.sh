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

# check that optional pyarrow modules are available
# because pytest would just skip the dask tests
python -c "import pyarrow.orc"
python -c "import pyarrow.parquet"

# check that dask.dataframe is correctly installed
python -c "import dask.dataframe"

# TODO(kszucs): the following tests are also uses pyarrow
# pytest -sv --pyargs dask.bytes.tests.test_hdfs
# pytest -sv --pyargs dask.bytes.tests.test_local

pytest -v --pyargs dask.dataframe.tests.test_dataframe
pytest -v --pyargs dask.dataframe.io.tests.test_orc
pytest -v --pyargs dask.dataframe.io.tests.test_parquet
# this file contains parquet tests that use S3 filesystem
pytest -v --pyargs dask.bytes.tests.test_s3
