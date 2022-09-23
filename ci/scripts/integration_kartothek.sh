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
# because pytest would just skip the pyarrow tests
python -c "import pyarrow.parquet"

# check that kartothek is correctly installed
python -c "import kartothek"

pushd /kartothek
# See ARROW-12314, test_load_dataframes_columns_raises_missing skipped because of changed error message
# See ARROW-16262 and https://github.com/JDASoftwareGroup/kartothek/issues/515
pytest -n0 --ignore tests/cli/test_query.py -k "not test_load_dataframes_columns_raises_missing \
              and not dates_as_object and not test_date_as_object \
              and not test_predicate_pushdown and not test_predicate_evaluation_date"
