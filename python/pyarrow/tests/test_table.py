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

from collections import OrderedDict
import numpy as np
from pandas.util.testing import assert_frame_equal
import pandas as pd
import pytest

from pyarrow.compat import unittest
import pyarrow as pa


class TestColumn(unittest.TestCase):

    def test_basics(self):
        data = [
            pa.array([-10, -5, 0, 5, 10])
        ]
        table = pa.Table.from_arrays(data, names=['a'])
        column = table.column(0)
        assert column.name == 'a'
        assert column.length() == 5
        assert len(column) == 5
        assert column.shape == (5,)
        assert column.to_pylist() == [-10, -5, 0, 5, 10]

    def test_from_array(self):
        arr = pa.array([0, 1, 2, 3, 4])

        col1 = pa.Column.from_array('foo', arr)
        col2 = pa.Column.from_array(pa.field('foo', arr.type), arr)

        assert col1.equals(col2)

    def test_pandas(self):
        data = [
            pa.array([-10, -5, 0, 5, 10])
        ]
        table = pa.Table.from_arrays(data, names=['a'])
        column = table.column(0)
        series = column.to_pandas()
        assert series.name == 'a'
        assert series.shape == (5,)
        assert series.iloc[0] == -10


def test_recordbatch_basics():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]

    batch = pa.RecordBatch.from_arrays(data, ['c0', 'c1'])

    assert len(batch) == 5
    assert batch.num_rows == 5
    assert batch.num_columns == len(data)
    assert batch.to_pydict() == OrderedDict([
        ('c0', [0, 1, 2, 3, 4]),
        ('c1', [-10, -5, 0, 5, 10])
    ])


def test_recordbatch_slice():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    names = ['c0', 'c1']

    batch = pa.RecordBatch.from_arrays(data, names)

    sliced = batch.slice(2)

    assert sliced.num_rows == 3

    expected = pa.RecordBatch.from_arrays(
        [x.slice(2) for x in data], names)
    assert sliced.equals(expected)

    sliced2 = batch.slice(2, 2)
    expected2 = pa.RecordBatch.from_arrays(
        [x.slice(2, 2) for x in data], names)
    assert sliced2.equals(expected2)

    # 0 offset
    assert batch.slice(0).equals(batch)

    # Slice past end of array
    assert len(batch.slice(len(batch))) == 0

    with pytest.raises(IndexError):
        batch.slice(-1)


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


def test_recordbatchlist_to_pandas():
    data1 = pd.DataFrame({
        'c1': np.array([1, 1, 2], dtype='uint32'),
        'c2': np.array([1.0, 2.0, 3.0], dtype='float64'),
        'c3': [True, None, False],
        'c4': ['foo', 'bar', None]
    })

    data2 = pd.DataFrame({
        'c1': np.array([3, 5], dtype='uint32'),
        'c2': np.array([4.0, 5.0], dtype='float64'),
        'c3': [True, True],
        'c4': ['baz', 'qux']
    })

    batch1 = pa.RecordBatch.from_pandas(data1)
    batch2 = pa.RecordBatch.from_pandas(data2)

    table = pa.Table.from_batches([batch1, batch2])
    result = table.to_pandas()
    data = pd.concat([data1, data2], ignore_index=True)
    assert_frame_equal(data, result)


def test_recordbatchlist_schema_equals():
    data1 = pd.DataFrame({'c1': np.array([1], dtype='uint32')})
    data2 = pd.DataFrame({'c1': np.array([4.0, 5.0], dtype='float64')})

    batch1 = pa.RecordBatch.from_pandas(data1)
    batch2 = pa.RecordBatch.from_pandas(data2)

    with pytest.raises(pa.ArrowException):
        pa.Table.from_batches([batch1, batch2])


def test_table_basics():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b'))
    assert len(table) == 5
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert table.shape == (5, 2)
    assert table.to_pydict() == OrderedDict([
        ('a', [0, 1, 2, 3, 4]),
        ('b', [-10, -5, 0, 5, 10])
    ])

    for col in table.itercolumns():
        for chunk in col.data.iterchunks():
            assert chunk is not None


def test_table_add_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    col = pa.Column.from_array('d', data[1])
    t2 = table.add_column(3, col)
    t3 = table.append_column(col)

    expected = pa.Table.from_arrays(data + [data[1]],
                                    names=('a', 'b', 'c', 'd'))
    assert t2.equals(expected)
    assert t3.equals(expected)

    t4 = table.add_column(0, col)
    expected = pa.Table.from_arrays([data[1]] + data,
                                    names=('d', 'a', 'b', 'c'))
    assert t4.equals(expected)


def test_table_remove_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    t2 = table.remove_column(0)
    expected = pa.Table.from_arrays(data[1:], names=('b', 'c'))
    assert t2.equals(expected)


def test_concat_tables():
    data = [
        list(range(5)),
        [-10., -5., 0., 5., 10.]
    ]
    data2 = [
        list(range(5, 10)),
        [1., 2., 3., 4., 5.]
    ]

    t1 = pa.Table.from_arrays([pa.array(x) for x in data],
                              names=('a', 'b'))
    t2 = pa.Table.from_arrays([pa.array(x) for x in data2],
                              names=('a', 'b'))

    result = pa.concat_tables([t1, t2])
    assert len(result) == 10

    expected = pa.Table.from_arrays([pa.array(x + y)
                                     for x, y in zip(data, data2)],
                                    names=('a', 'b'))

    assert result.equals(expected)


def test_table_pandas():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b'))

    # TODO: Use this part once from_pandas is implemented
    # data = {'a': range(5), 'b': [-10, -5, 0, 5, 10]}
    # df = pd.DataFrame(data)
    # pa.Table.from_pandas(df)

    df = table.to_pandas()
    assert set(df.columns) == set(('a', 'b'))
    assert df.shape == (5, 2)
    assert df.loc[0, 'b'] == -10


def test_table_negative_indexing():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array([1.0, 2.0, 3.0]),
        pa.array(['ab', 'bc', 'cd']),
    ]
    table = pa.Table.from_arrays(data, names=tuple('abcd'))

    assert table[-1].equals(table[3])
    assert table[-2].equals(table[2])
    assert table[-3].equals(table[1])
    assert table[-4].equals(table[0])

    with pytest.raises(IndexError):
        table[-5]

    with pytest.raises(IndexError):
        table[4]
