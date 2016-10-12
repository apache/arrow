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

import numpy as np

from pandas.util.testing import assert_frame_equal
import pandas as pd

import pyarrow as pa


def test_recordbatch_basics():
    data = [
        pa.from_pylist(range(5)),
        pa.from_pylist([-10, -5, 0, 5, 10])
    ]

    batch = pa.RecordBatch.from_arrays(['c0', 'c1'], data)

    assert len(batch) == 5
    assert batch.num_rows == 5
    assert batch.num_columns == len(data)


def test_recordbatch_from_to_pandas():
    data = pd.DataFrame({
        'c1': np.array([1, 2, 3, 4, 5], dtype='int64'),
        'c2': np.array([1, 2, 3, 4, 5], dtype='uint32'),
        'c2': np.random.randn(5),
        'c3': ['foo', 'bar', None, 'baz', 'qux'],
        'c4': [False, True, False, True, False]
    })

    batch = pa.RecordBatch.from_pandas(data)
    result = batch.to_pandas()
    assert_frame_equal(data, result)


def test_table_basics():
    data = [
        pa.from_pylist(range(5)),
        pa.from_pylist([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(('a', 'b'), data, 'table_name')
    assert table.name == 'table_name'
    assert len(table) == 5
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert table.shape == (5, 2)

    for col in table.itercolumns():
        for chunk in col.data.iterchunks():
            assert chunk is not None


def test_table_pandas():
    data = [
        pa.from_pylist(range(5)),
        pa.from_pylist([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(('a', 'b'), data, 'table_name')

    # TODO: Use this part once from_pandas is implemented
    # data = {'a': range(5), 'b': [-10, -5, 0, 5, 10]}
    # df = pd.DataFrame(data)
    # pa.Table.from_pandas(df)

    df = table.to_pandas()
    assert set(df.columns) == set(('a', 'b'))
    assert df.shape == (5, 2)
    assert df.loc[0, 'b'] == -10
