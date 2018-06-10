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

import os

import fastparquet
import pyarrow as pa
import pyarrow.parquet as pq
import pandas.util.testing as tm


def hdfs_test_client(driver='libhdfs'):
    host = os.environ.get('ARROW_HDFS_TEST_HOST', 'localhost')
    user = os.environ['ARROW_HDFS_TEST_USER']
    try:
        port = int(os.environ.get('ARROW_HDFS_TEST_PORT', 20500))
    except ValueError:
        raise ValueError('Env variable ARROW_HDFS_TEST_PORT was not '
                         'an integer')

    return pa.HdfsClient(host, port, user, driver=driver)


def test_fastparquet_read_with_hdfs():
    fs = hdfs_test_client()

    df = tm.makeDataFrame()
    table = pa.Table.from_pandas(df)

    path = '/tmp/testing.parquet'
    with fs.open(path, 'wb') as f:
        pq.write_table(table, f)

    parquet_file = fastparquet.ParquetFile(path, open_with=fs.open)

    result = parquet_file.to_pandas()
    tm.assert_frame_equal(result, df)
