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
# pytest -sv --pyargs dask.bytes.tests.test_s3
# pytest -sv --pyargs dask.bytes.tests.test_hdfs
# pytest -sv --pyargs dask.bytes.tests.test_local

# skip failing pickle test, see https://github.com/dask/dask/issues/6374
pytest -v --pyargs dask.dataframe.tests.test_dataframe -k "not test_dataframe_picklable"
pytest -v --pyargs dask.dataframe.io.tests.test_orc
# skip failing parquet tests, see https://github.com/dask/dask/issues/6243
# test_illegal_column_name can be removed once next dask release is out
# (https://github.com/dask/dask/pull/6378)
pytest -v --pyargs dask.dataframe.io.tests.test_parquet \
  -k "not test_to_parquet_pyarrow_w_inconsistent_schema_by_partition_fails_by_default and not test_timeseries_nulls_in_schema and not test_illegal_column_name"
