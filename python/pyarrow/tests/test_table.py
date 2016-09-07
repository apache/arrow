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
import pyarrow as A


class TestRowBatch(unittest.TestCase):

    def test_basics(self):
        data = [
            A.from_pylist(range(5)),
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        num_rows = 5

        descr = A.schema([A.field('c0', data[0].type),
                          A.field('c1', data[1].type)])

        batch = A.RowBatch(descr, num_rows, data)

        assert len(batch) == num_rows
        assert batch.num_rows == num_rows
        assert batch.num_columns == len(data)


class TestTable(unittest.TestCase):

    def test_basics(self):
        data = [
            A.from_pylist(range(5)),
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        table = A.Table.from_arrays(('a', 'b'), data, 'table_name')
        assert table.name == 'table_name'
        assert len(table) == 5
        assert table.num_rows == 5
        assert table.num_columns == 2
        assert table.shape == (5, 2)

        for col in table.itercolumns():
            for chunk in col.data.iterchunks():
                assert chunk is not None

    def test_pandas(self):
        data = [
            A.from_pylist(range(5)),
            A.from_pylist([-10, -5, 0, 5, 10])
        ]
        table = A.Table.from_arrays(('a', 'b'), data, 'table_name')

        # TODO: Use this part once from_pandas is implemented
        # data = {'a': range(5), 'b': [-10, -5, 0, 5, 10]}
        # df = pd.DataFrame(data)
        # A.Table.from_pandas(df)

        df = table.to_pandas()
        assert set(df.columns) == set(('a', 'b'))
        assert df.shape == (5, 2)
        assert df.ix[0, 'b'] == -10
