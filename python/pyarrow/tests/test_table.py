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

from collections import OrderedDict, Iterable
import pickle

import numpy as np
from pandas.util.testing import assert_frame_equal
import pandas as pd
import pytest

import pyarrow as pa


def test_chunked_array_basics():
    data = pa.chunked_array([], type=pa.string())
    assert data.to_pylist() == []

    with pytest.raises(ValueError):
        pa.chunked_array([])


def test_chunked_array_str():
    data = [
        pa.array([1, 2, 3]),
        pa.array([4, 5, 6])
    ]
    data = pa.chunked_array(data)
    assert str(data) == """[
  [
    1,
    2,
    3
  ],
  [
    4,
    5,
    6
  ]
]"""


def test_chunked_array_getitem():
    data = [
        pa.array([1, 2, 3]),
        pa.array([4, 5, 6])
    ]
    data = pa.chunked_array(data)
    assert data[1].as_py() == 2
    assert data[-1].as_py() == 6
    assert data[-6].as_py() == 1
    with pytest.raises(IndexError):
        data[6]
    with pytest.raises(IndexError):
        data[-7]

    data_slice = data[2:4]
    assert data_slice.to_pylist() == [3, 4]

    data_slice = data[4:-1]
    assert data_slice.to_pylist() == [5]

    data_slice = data[99:99]
    assert data_slice.type == data.type
    assert data_slice.to_pylist() == []


def test_chunked_array_iter():
    data = [
        pa.array([0]),
        pa.array([1, 2, 3]),
        pa.array([4, 5, 6]),
        pa.array([7, 8, 9])
    ]
    arr = pa.chunked_array(data)

    for i, j in zip(range(10), arr):
        assert i == j

    assert isinstance(arr, Iterable)


def test_chunked_array_equals():
    def eq(xarrs, yarrs):
        if isinstance(xarrs, pa.ChunkedArray):
            x = xarrs
        else:
            x = pa.chunked_array(xarrs)
        if isinstance(yarrs, pa.ChunkedArray):
            y = yarrs
        else:
            y = pa.chunked_array(yarrs)
        assert x.equals(y)
        assert y.equals(x)

    def ne(xarrs, yarrs):
        if isinstance(xarrs, pa.ChunkedArray):
            x = xarrs
        else:
            x = pa.chunked_array(xarrs)
        if isinstance(yarrs, pa.ChunkedArray):
            y = yarrs
        else:
            y = pa.chunked_array(yarrs)
        assert not x.equals(y)
        assert not y.equals(x)

    eq(pa.chunked_array([], type=pa.int32()),
       pa.chunked_array([], type=pa.int32()))
    ne(pa.chunked_array([], type=pa.int32()),
       pa.chunked_array([], type=pa.int64()))

    a = pa.array([0, 2], type=pa.int32())
    b = pa.array([0, 2], type=pa.int64())
    c = pa.array([0, 3], type=pa.int32())
    d = pa.array([0, 2, 0, 3], type=pa.int32())

    eq([a], [a])
    ne([a], [b])
    eq([a, c], [a, c])
    eq([a, c], [d])
    ne([c, a], [a, c])


@pytest.mark.parametrize(
    ('data', 'typ'),
    [
        ([True, False, True, True], pa.bool_()),
        ([1, 2, 4, 6], pa.int64()),
        ([1.0, 2.5, None], pa.float64()),
        (['a', None, 'b'], pa.string()),
        ([], pa.list_(pa.uint8())),
        ([[1, 2], [3]], pa.list_(pa.int64())),
        ([['a'], None, ['b', 'c']], pa.list_(pa.string())),
        ([(1, 'a'), (2, 'c'), None],
            pa.struct([pa.field('a', pa.int64()), pa.field('b', pa.string())]))
    ]
)
def test_chunked_array_pickle(data, typ):
    arrays = []
    while data:
        arrays.append(pa.array(data[:2], type=typ))
        data = data[2:]
    array = pa.chunked_array(arrays, type=typ)
    result = pickle.loads(pickle.dumps(array))
    assert result.equals(array)


def test_chunked_array_to_pandas():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])
    chunked_arr = table.column(0).data
    assert isinstance(chunked_arr, pa.ChunkedArray)
    array = chunked_arr.to_pandas()
    assert array.shape == (5,)
    assert array[0] == -10


def test_chunked_array_asarray():
    data = [
        pa.array([0]),
        pa.array([1, 2, 3])
    ]
    chunked_arr = pa.chunked_array(data)

    np_arr = np.asarray(chunked_arr)
    assert np_arr.tolist() == [0, 1, 2, 3]
    assert np_arr.dtype == np.dtype('int64')

    # An optional type can be specified when calling np.asarray
    np_arr = np.asarray(chunked_arr, dtype='str')
    assert np_arr.tolist() == ['0', '1', '2', '3']

    # Types are modified when there are nulls
    data = [
        pa.array([1, None]),
        pa.array([1, 2, 3])
    ]
    chunked_arr = pa.chunked_array(data)

    np_arr = np.asarray(chunked_arr)
    elements = np_arr.tolist()
    assert elements[0] == 1.
    assert np.isnan(elements[1])
    assert elements[2:] == [1., 2., 3.]
    assert np_arr.dtype == np.dtype('float64')


def test_column_basics():
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


def test_column_factory_function():
    # ARROW-1575
    arr = pa.array([0, 1, 2, 3, 4])
    arr2 = pa.array([5, 6, 7, 8])

    col1 = pa.Column.from_array('foo', arr)
    col2 = pa.Column.from_array(pa.field('foo', arr.type), arr)

    assert col1.equals(col2)

    col3 = pa.column('foo', [arr, arr2])
    chunked_arr = pa.chunked_array([arr, arr2])
    col4 = pa.column('foo', chunked_arr)
    assert col3.equals(col4)

    col5 = pa.column('foo', arr.to_pandas())
    assert col5.equals(pa.column('foo', arr))

    # Type mismatch
    with pytest.raises(ValueError):
        pa.Column.from_array(pa.field('foo', pa.string()), arr)


def test_column_pickle():
    arr = pa.chunked_array([[1, 2], [5, 6, 7]], type=pa.int16())
    field = pa.field("ints", pa.int16()).add_metadata({b"foo": b"bar"})
    col = pa.column(field, arr)

    result = pickle.loads(pickle.dumps(col))
    assert result.equals(col)
    assert result.data.num_chunks == 2
    assert result.field == field


def test_column_to_pandas():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])
    column = table.column(0)
    series = column.to_pandas()
    assert series.name == 'a'
    assert series.shape == (5,)
    assert series.iloc[0] == -10


def test_column_asarray():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])
    column = table.column(0)

    np_arr = np.asarray(column)
    assert np_arr.tolist() == [-10, -5, 0, 5, 10]
    assert np_arr.dtype == np.dtype('int64')

    # An optional type can be specified when calling np.asarray
    np_arr = np.asarray(column, dtype='str')
    assert np_arr.tolist() == ['-10', '-5', '0', '5', '10']


def test_column_flatten():
    ty = pa.struct([pa.field('x', pa.int16()),
                    pa.field('y', pa.float32())])
    a = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)
    col = pa.Column.from_array('foo', a)
    x, y = col.flatten()
    assert x == pa.column('foo.x', pa.array([1, 3, 5], type=pa.int16()))
    assert y == pa.column('foo.y', pa.array([2.5, 4.5, 6.5],
                                            type=pa.float32()))
    # Empty column
    a = pa.array([], type=ty)
    col = pa.Column.from_array('foo', a)
    x, y = col.flatten()
    assert x == pa.column('foo.x', pa.array([], type=pa.int16()))
    assert y == pa.column('foo.y', pa.array([], type=pa.float32()))


def test_recordbatch_basics():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]

    batch = pa.RecordBatch.from_arrays(data, ['c0', 'c1'])
    assert not batch.schema.metadata

    assert len(batch) == 5
    assert batch.num_rows == 5
    assert batch.num_columns == len(data)
    assert batch.to_pydict() == OrderedDict([
        ('c0', [0, 1, 2, 3, 4]),
        ('c1', [-10, -5, 0, 5, 10])
    ])

    with pytest.raises(IndexError):
        # bounds checking
        batch[2]

    # Schema passed explicitly
    schema = pa.schema([pa.field('c0', pa.int16()),
                        pa.field('c1', pa.int32())],
                       metadata={b'foo': b'bar'})
    batch = pa.RecordBatch.from_arrays(data, schema)
    assert batch.schema == schema


def test_recordbatch_from_arrays_validate_lengths():
    # ARROW-2820
    data = [pa.array([1]), pa.array(["tokyo", "like", "happy"]),
            pa.array(["derek"])]

    with pytest.raises(ValueError):
        pa.RecordBatch.from_arrays(data, ['id', 'tags', 'name'])


def test_recordbatch_no_fields():
    batch = pa.RecordBatch.from_arrays([], [])

    assert len(batch) == 0
    assert batch.num_rows == 0
    assert batch.num_columns == 0


def test_recordbatch_from_arrays_invalid_names():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    with pytest.raises(ValueError):
        pa.RecordBatch.from_arrays(data, names=['a', 'b', 'c'])

    with pytest.raises(ValueError):
        pa.RecordBatch.from_arrays(data, names=['a'])


def test_recordbatch_empty_metadata():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]

    batch = pa.RecordBatch.from_arrays(data, ['c0', 'c1'])
    assert batch.schema.metadata is None


def test_recordbatch_pickle():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    schema = pa.schema([pa.field('ints', pa.int8()),
                        pa.field('floats', pa.float32()),
                        ]).add_metadata({b'foo': b'bar'})
    batch = pa.RecordBatch.from_arrays(data, schema)

    result = pickle.loads(pickle.dumps(batch))
    assert result.equals(batch)
    assert result.schema == schema


def test_recordbatch_slice_getitem():
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

    # Check __getitem__-based slicing
    assert batch.slice(0, 0).equals(batch[:0])
    assert batch.slice(0, 2).equals(batch[:2])
    assert batch.slice(2, 2).equals(batch[2:4])
    assert batch.slice(2, len(batch) - 2).equals(batch[2:])
    assert batch.slice(len(batch) - 2, 2).equals(batch[-2:])
    assert batch.slice(len(batch) - 4, 2).equals(batch[-4:-2])


def test_recordbatch_from_to_pandas():
    data = pd.DataFrame({
        'c1': np.array([1, 2, 3, 4, 5], dtype='int64'),
        'c2': np.array([1, 2, 3, 4, 5], dtype='uint32'),
        'c3': np.random.randn(5),
        'c4': ['foo', 'bar', None, 'baz', 'qux'],
        'c5': [False, True, False, True, False]
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
    data = pd.concat([data1, data2])
    assert_frame_equal(data, result)


def test_recordbatchlist_schema_equals():
    data1 = pd.DataFrame({'c1': np.array([1], dtype='uint32')})
    data2 = pd.DataFrame({'c1': np.array([4.0, 5.0], dtype='float64')})

    batch1 = pa.RecordBatch.from_pandas(data1)
    batch2 = pa.RecordBatch.from_pandas(data2)

    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_batches([batch1, batch2])


def test_table_to_batches():
    df1 = pd.DataFrame({'a': list(range(10))})
    df2 = pd.DataFrame({'a': list(range(10, 30))})

    batch1 = pa.RecordBatch.from_pandas(df1, preserve_index=False)
    batch2 = pa.RecordBatch.from_pandas(df2, preserve_index=False)

    table = pa.Table.from_batches([batch1, batch2, batch1])

    expected_df = pd.concat([df1, df2, df1], ignore_index=True)

    batches = table.to_batches()
    assert len(batches) == 3

    assert_frame_equal(pa.Table.from_batches(batches).to_pandas(),
                       expected_df)

    batches = table.to_batches(chunksize=15)
    assert list(map(len, batches)) == [10, 15, 5, 10]

    assert_frame_equal(table.to_pandas(), expected_df)
    assert_frame_equal(pa.Table.from_batches(batches).to_pandas(),
                       expected_df)

    table_from_iter = pa.Table.from_batches(iter([batch1, batch2, batch1]))
    assert table.equals(table_from_iter)


def test_table_basics():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b'))
    table._validate()
    assert len(table) == 5
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert table.shape == (5, 2)
    assert table.to_pydict() == OrderedDict([
        ('a', [0, 1, 2, 3, 4]),
        ('b', [-10, -5, 0, 5, 10])
    ])

    columns = []
    for col in table.itercolumns():
        columns.append(col)
        for chunk in col.data.iterchunks():
            assert chunk is not None

        with pytest.raises(IndexError):
            col.data.chunk(-1)

        with pytest.raises(IndexError):
            col.data.chunk(col.data.num_chunks)

    assert table.columns == columns


def test_table_from_arrays_invalid_names():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, names=['a', 'b', 'c'])

    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, names=['a'])


def test_table_pickle():
    data = [
        pa.chunked_array([[1, 2], [3, 4]], type=pa.uint32()),
        pa.chunked_array([["some", "strings", None, ""]], type=pa.string()),
    ]
    schema = pa.schema([pa.field('ints', pa.uint32()),
                        pa.field('strs', pa.string())],
                       metadata={b'foo': b'bar'})
    table = pa.Table.from_arrays(data, schema=schema)

    result = pickle.loads(pickle.dumps(table))
    result._validate()
    assert result.equals(table)


def test_table_select_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    assert table.column('a').equals(table.column(0))

    with pytest.raises(KeyError):
        table.column('d')

    with pytest.raises(TypeError):
        table.column(None)


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


def test_table_set_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    col = pa.Column.from_array('d', data[1])
    t2 = table.set_column(0, col)

    expected_data = list(data)
    expected_data[0] = data[1]
    expected = pa.Table.from_arrays(expected_data,
                                    names=('d', 'b', 'c'))
    assert t2.equals(expected)


def test_table_drop():
    """ drop one or more columns given labels"""
    a = pa.array(range(5))
    b = pa.array([-10, -5, 0, 5, 10])
    c = pa.array(range(5, 10))

    table = pa.Table.from_arrays([a, b, c], names=('a', 'b', 'c'))
    t2 = table.drop(['a', 'b'])

    exp = pa.Table.from_arrays([c], names=('c',))
    assert exp.equals(t2)

    # -- raise KeyError if column not in Table
    with pytest.raises(KeyError, match="Column 'd' not found"):
        table.drop(['d'])


def test_table_remove_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    t2 = table.remove_column(0)
    t2._validate()
    expected = pa.Table.from_arrays(data[1:], names=('b', 'c'))
    assert t2.equals(expected)


def test_table_remove_column_empty():
    # ARROW-1865
    data = [
        pa.array(range(5)),
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    t2 = table.remove_column(0)
    t2._validate()
    assert len(t2) == len(table)

    t3 = t2.add_column(0, table[0])
    t3._validate()
    assert t3.equals(table)


def test_table_flatten():
    ty1 = pa.struct([pa.field('x', pa.int16()),
                     pa.field('y', pa.float32())])
    ty2 = pa.struct([pa.field('nest', ty1)])
    a = pa.array([(1, 2.5), (3, 4.5)], type=ty1)
    b = pa.array([((11, 12.5),), ((13, 14.5),)], type=ty2)
    c = pa.array([False, True], type=pa.bool_())

    table = pa.Table.from_arrays([a, b, c], names=['a', 'b', 'c'])
    t2 = table.flatten()
    t2._validate()
    expected = pa.Table.from_arrays([
        pa.array([1, 3], type=pa.int16()),
        pa.array([2.5, 4.5], type=pa.float32()),
        pa.array([(11, 12.5), (13, 14.5)], type=ty1),
        c],
        names=['a.x', 'a.y', 'b.nest', 'c'])
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
    result._validate()
    assert len(result) == 10

    expected = pa.Table.from_arrays([pa.array(x + y)
                                     for x, y in zip(data, data2)],
                                    names=('a', 'b'))

    assert result.equals(expected)


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
