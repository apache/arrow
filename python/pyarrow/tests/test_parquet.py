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

from pyarrow.compat import unittest
import pyarrow as arrow
import pyarrow.parquet

A = arrow

from shutil import rmtree
from tempfile import mkdtemp

import os.path


class TestParquetIO(unittest.TestCase):

  def setUp(self):
    self.temp_directory = mkdtemp()

  def tearDown(self):
    rmtree(self.temp_directory)

  def test_single_int64_column(self):
    filename = os.path.join(self.temp_directory, 'single_int64_column.parquet')
    data = [A.from_pylist(range(5))]
    table = A.Table.from_arrays(('a', 'b'), data, 'table_name')
    A.parquet.write_table(table, filename)
    table_read = pyarrow.parquet.read_table(filename)
    for col_written, col_read in zip(table.itercolumns(), table_read.itercolumns()):
        assert col_written.name == col_read.name
        assert col_read.data.num_chunks == 1
        data_written = col_written.data.chunk(0)
        data_read = col_read.data.chunk(0)
        assert data_written == data_read
