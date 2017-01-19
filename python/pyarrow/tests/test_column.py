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

A = arrow

import pandas as pd


class TestColumn(unittest.TestCase):

    def test_basics(self):
        data = [
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        table = A.Table.from_arrays(('a'), data, 'table_name')
        column = table.column(0)
        assert column.name == 'a'
        assert column.length() == 5
        assert len(column) == 5
        assert column.shape == (5,)
        assert column.to_pylist() == [-10, -5, 0, 5, 10]

    def test_pandas(self):
        data = [
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        table = A.Table.from_arrays(('a'), data, 'table_name')
        column = table.column(0)
        series = column.to_pandas()
        assert series.name == 'a'
        assert series.shape == (5,)
        assert series.iloc[0] == -10
