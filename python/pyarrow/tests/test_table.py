# -*- coding: utf-8 -*-
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
import pickle
import sys

import numpy as np
import pytest
import pyarrow as pa
from pyarrow import compat


def test_chunked_array_basics():
    data = pa.chunked_array([], type=pa.string())
    assert data.type == pa.string()
    assert data.to_pylist() == []
    data.validate()

    with pytest.raises(ValueError):
        pa.chunked_array([])

    data = pa.chunked_array([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
    ])
    assert isinstance(data.chunks, list)
    assert all(isinstance(c, pa.lib.Int64Array) for c in data.chunks)
    assert all(isinstance(c, pa.lib.Int64Array) for c in data.iterchunks())
    assert len(data.chunks) == 3
    data.validate()


def test_chunked_array_mismatch_types():
    with pytest.raises(pa.ArrowInvalid):
        pa.chunked_array([pa.array([1, 2]), pa.array(['foo', 'bar'])])


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

    assert isinstance(arr, compat.Iterable)


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
        assert x == y
        assert x != str(y)

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
        assert x != y

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

    assert not pa.chunked_array([], type=pa.int32()).equals(None)


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
    array.validate()
    result = pickle.loads(pickle.dumps(array))
    result.validate()
    assert result.equals(array)


@pytest.mark.pandas
def test_chunked_array_to_pandas():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.table(data, names=['a'])
    col = table.column(0)
    assert isinstance(col, pa.ChunkedArray)
    array = col.to_pandas()
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


def test_chunked_array_flatten():
    ty = pa.struct([pa.field('x', pa.int16()),
                    pa.field('y', pa.float32())])
    a = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)
    carr = pa.chunked_array(a)
    x, y = carr.flatten()
    assert x.equals(pa.chunked_array(pa.array([1, 3, 5], type=pa.int16())))
    assert y.equals(pa.chunked_array(pa.array([2.5, 4.5, 6.5],
                                              type=pa.float32())))

    # Empty column
    a = pa.array([], type=ty)
    carr = pa.chunked_array(a)
    x, y = carr.flatten()
    assert x.equals(pa.chunked_array(pa.array([], type=pa.int16())))
    assert y.equals(pa.chunked_array(pa.array([], type=pa.float32())))


def test_recordbatch_basics():
    data = [
        pa.array(range(5), type='int16'),
        pa.array([-10, -5, 0, 5, 10], type='int32')
    ]

    batch = pa.record_batch(data, ['c0', 'c1'])
    assert not batch.schema.metadata

    assert len(batch) == 5
    assert batch.num_rows == 5
    assert batch.num_columns == len(data)
    pydict = batch.to_pydict()
    assert pydict == OrderedDict([
        ('c0', [0, 1, 2, 3, 4]),
        ('c1', [-10, -5, 0, 5, 10])
    ])
    if sys.version_info >= (3, 7):
        assert type(pydict) == dict
    else:
        assert type(pydict) == OrderedDict

    with pytest.raises(IndexError):
        # bounds checking
        batch[2]

    # Schema passed explicitly
    schema = pa.schema([pa.field('c0', pa.int16()),
                        pa.field('c1', pa.int32())],
                       metadata={b'foo': b'bar'})
    batch = pa.record_batch(data, schema=schema)
    assert batch.schema == schema


def test_recordbatch_from_arrays_validate_schema():
    # ARROW-6263
    arr = pa.array([])
    schema = pa.schema([pa.field('f0', pa.utf8())])
    with pytest.raises(ValueError):
        pa.record_batch([arr], schema=schema)


def test_recordbatch_from_arrays_validate_lengths():
    # ARROW-2820
    data = [pa.array([1]), pa.array(["tokyo", "like", "happy"]),
            pa.array(["derek"])]

    with pytest.raises(ValueError):
        pa.record_batch(data, ['id', 'tags', 'name'])


def test_recordbatch_no_fields():
    batch = pa.record_batch([], [])

    assert len(batch) == 0
    assert batch.num_rows == 0
    assert batch.num_columns == 0


def test_recordbatch_from_arrays_invalid_names():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    with pytest.raises(ValueError):
        pa.record_batch(data, names=['a', 'b', 'c'])

    with pytest.raises(ValueError):
        pa.record_batch(data, names=['a'])


def test_recordbatch_empty_metadata():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]

    batch = pa.record_batch(data, ['c0', 'c1'])
    assert batch.schema.metadata is None


def test_recordbatch_pickle():
    data = [
        pa.array(range(5), type='int8'),
        pa.array([-10, -5, 0, 5, 10], type='float32')
    ]
    fields = [
        pa.field('ints', pa.int8()),
        pa.field('floats', pa.float32()),
    ]
    schema = pa.schema(fields, metadata={b'foo': b'bar'})
    batch = pa.record_batch(data, schema=schema)

    result = pickle.loads(pickle.dumps(batch))
    assert result.equals(batch)
    assert result.schema == schema


def _table_like_slice_tests(factory):
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    names = ['c0', 'c1']

    obj = factory(data, names=names)

    sliced = obj.slice(2)
    assert sliced.num_rows == 3

    expected = factory([x.slice(2) for x in data], names)
    assert sliced.equals(expected)

    sliced2 = obj.slice(2, 2)
    expected2 = factory([x.slice(2, 2) for x in data], names)
    assert sliced2.equals(expected2)

    # 0 offset
    assert obj.slice(0).equals(obj)

    # Slice past end of array
    assert len(obj.slice(len(obj))) == 0

    with pytest.raises(IndexError):
        obj.slice(-1)

    # Check __getitem__-based slicing
    assert obj.slice(0, 0).equals(obj[:0])
    assert obj.slice(0, 2).equals(obj[:2])
    assert obj.slice(2, 2).equals(obj[2:4])
    assert obj.slice(2, len(obj) - 2).equals(obj[2:])
    assert obj.slice(len(obj) - 2, 2).equals(obj[-2:])
    assert obj.slice(len(obj) - 4, 2).equals(obj[-4:-2])


def test_recordbatch_slice_getitem():
    return _table_like_slice_tests(pa.RecordBatch.from_arrays)


def test_table_slice_getitem():
    return _table_like_slice_tests(pa.table)


def test_recordbatchlist_schema_equals():
    a1 = np.array([1], dtype='uint32')
    a2 = np.array([4.0, 5.0], dtype='float64')
    batch1 = pa.record_batch([pa.array(a1)], ['c1'])
    batch2 = pa.record_batch([pa.array(a2)], ['c1'])

    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_batches([batch1, batch2])


def test_table_equals():
    table = pa.Table.from_arrays([], names=[])

    assert table.equals(table)
    # ARROW-4822
    assert not table.equals(None)


def test_table_from_batches_and_schema():
    schema = pa.schema([
        pa.field('a', pa.int64()),
        pa.field('b', pa.float64()),
    ])
    batch = pa.record_batch([pa.array([1]), pa.array([3.14])],
                            names=['a', 'b'])
    table = pa.Table.from_batches([batch], schema)
    assert table.schema.equals(schema)
    assert table.column(0) == pa.chunked_array([[1]])
    assert table.column(1) == pa.chunked_array([[3.14]])

    incompatible_schema = pa.schema([pa.field('a', pa.int64())])
    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_batches([batch], incompatible_schema)

    incompatible_batch = pa.record_batch([pa.array([1])], ['a'])
    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_batches([incompatible_batch], schema)


@pytest.mark.pandas
def test_table_to_batches():
    from pandas.util.testing import assert_frame_equal
    import pandas as pd

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

    batches = table.to_batches(max_chunksize=15)
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
    table = pa.table(data, names=('a', 'b'))
    table.validate()
    assert len(table) == 5
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert table.shape == (5, 2)
    pydict = table.to_pydict()
    assert pydict == OrderedDict([
        ('a', [0, 1, 2, 3, 4]),
        ('b', [-10, -5, 0, 5, 10])
    ])
    if sys.version_info >= (3, 7):
        assert type(pydict) == dict
    else:
        assert type(pydict) == OrderedDict

    columns = []
    for col in table.itercolumns():
        columns.append(col)
        for chunk in col.iterchunks():
            assert chunk is not None

        with pytest.raises(IndexError):
            col.chunk(-1)

        with pytest.raises(IndexError):
            col.chunk(col.num_chunks)

    assert table.columns == columns
    assert table == pa.table(columns, names=table.column_names)
    assert table != pa.table(columns[1:], names=table.column_names[1:])
    assert table != columns


def test_table_from_arrays_preserves_column_metadata():
    # Added to test https://issues.apache.org/jira/browse/ARROW-3866
    arr0 = pa.array([1, 2])
    arr1 = pa.array([3, 4])
    field0 = pa.field('field1', pa.int64(), metadata=dict(a="A", b="B"))
    field1 = pa.field('field2', pa.int64(), nullable=False)
    table = pa.Table.from_arrays([arr0, arr1],
                                 schema=pa.schema([field0, field1]))
    assert b"a" in table.field(0).metadata
    assert table.field(1).nullable is False


def test_table_from_arrays_invalid_names():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, names=['a', 'b', 'c'])

    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, names=['a'])


def test_table_from_lists():
    data = [
        list(range(5)),
        [-10, -5, 0, 5, 10]
    ]

    result = pa.table(data, names=['a', 'b'])
    expected = pa.Table.from_arrays(data, names=['a', 'b'])
    assert result.equals(expected)

    schema = pa.schema([
        pa.field('a', pa.uint16()),
        pa.field('b', pa.int64())
    ])
    result = pa.table(data, schema=schema)
    expected = pa.Table.from_arrays(data, schema=schema)
    assert result.equals(expected)


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
    result.validate()
    assert result.equals(table)


def test_table_get_field():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    assert table.field('a').equals(table.schema.field('a'))
    assert table.field(0).equals(table.schema.field('a'))

    with pytest.raises(KeyError):
        table.field('d')

    with pytest.raises(TypeError):
        table.field(None)

    with pytest.raises(IndexError):
        table.field(4)


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

    with pytest.raises(IndexError):
        table.column(4)


def test_table_add_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=('a', 'b', 'c'))

    new_field = pa.field('d', data[1].type)
    t2 = table.add_column(3, new_field, data[1])
    t3 = table.append_column(new_field, data[1])

    expected = pa.Table.from_arrays(data + [data[1]],
                                    names=('a', 'b', 'c', 'd'))
    assert t2.equals(expected)
    assert t3.equals(expected)

    t4 = table.add_column(0, new_field, data[1])
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

    new_field = pa.field('d', data[1].type)
    t2 = table.set_column(0, new_field, data[1])

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
    t2.validate()
    expected = pa.Table.from_arrays(data[1:], names=('b', 'c'))
    assert t2.equals(expected)


def test_table_remove_column_empty():
    # ARROW-1865
    data = [
        pa.array(range(5)),
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    t2 = table.remove_column(0)
    t2.validate()
    assert len(t2) == len(table)

    t3 = t2.add_column(0, table.field(0), table[0])
    t3.validate()
    assert t3.equals(table)


def test_table_rename_columns():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    table = pa.Table.from_arrays(data, names=['a', 'b', 'c'])
    assert table.column_names == ['a', 'b', 'c']

    t2 = table.rename_columns(['eh', 'bee', 'sea'])
    t2.validate()
    assert t2.column_names == ['eh', 'bee', 'sea']

    expected = pa.Table.from_arrays(data, names=['eh', 'bee', 'sea'])
    assert t2.equals(expected)


def test_table_flatten():
    ty1 = pa.struct([pa.field('x', pa.int16()),
                     pa.field('y', pa.float32())])
    ty2 = pa.struct([pa.field('nest', ty1)])
    a = pa.array([(1, 2.5), (3, 4.5)], type=ty1)
    b = pa.array([((11, 12.5),), ((13, 14.5),)], type=ty2)
    c = pa.array([False, True], type=pa.bool_())

    table = pa.Table.from_arrays([a, b, c], names=['a', 'b', 'c'])
    t2 = table.flatten()
    t2.validate()
    expected = pa.Table.from_arrays([
        pa.array([1, 3], type=pa.int16()),
        pa.array([2.5, 4.5], type=pa.float32()),
        pa.array([(11, 12.5), (13, 14.5)], type=ty1),
        c],
        names=['a.x', 'a.y', 'b.nest', 'c'])
    assert t2.equals(expected)


def test_table_combine_chunks():
    batch1 = pa.record_batch([pa.array([1]), pa.array(["a"])],
                             names=['f1', 'f2'])
    batch2 = pa.record_batch([pa.array([2]), pa.array(["b"])],
                             names=['f1', 'f2'])
    table = pa.Table.from_batches([batch1, batch2])
    combined = table.combine_chunks()
    combined.validate()
    assert combined.equals(table)
    for c in combined.columns:
        assert c.num_chunks == 1


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
    result.validate()
    assert len(result) == 10

    expected = pa.Table.from_arrays([pa.array(x + y)
                                     for x, y in zip(data, data2)],
                                    names=('a', 'b'))

    assert result.equals(expected)


@pytest.mark.pandas
def test_concat_tables_with_different_schema_metadata():
    import pandas as pd

    schema = pa.schema([
        pa.field('a', pa.string()),
        pa.field('b', pa.string()),
    ])

    values = list('abcdefgh')
    df1 = pd.DataFrame({'a': values, 'b': values})
    df2 = pd.DataFrame({'a': [np.nan] * 8, 'b': values})

    table1 = pa.Table.from_pandas(df1, schema=schema, preserve_index=False)
    table2 = pa.Table.from_pandas(df2, schema=schema, preserve_index=False)
    assert table1.schema.equals(table2.schema, check_metadata=False)
    assert not table1.schema.equals(table2.schema, check_metadata=True)

    table3 = pa.concat_tables([table1, table2])
    assert table1.schema.equals(table3.schema, check_metadata=True)
    assert table2.schema.equals(table3.schema, check_metadata=False)


def test_table_negative_indexing():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        pa.array(['ab', 'bc', 'cd', 'de', 'ef']),
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


def test_table_cast_to_incompatible_schema():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
    ]
    table = pa.Table.from_arrays(data, names=tuple('ab'))

    target_schema1 = pa.schema([
        pa.field('A', pa.int32()),
        pa.field('b', pa.int16()),
    ])
    target_schema2 = pa.schema([
        pa.field('a', pa.int32()),
    ])
    message = ("Target schema's field names are not matching the table's "
               "field names:.*")
    with pytest.raises(ValueError, match=message):
        table.cast(target_schema1)
    with pytest.raises(ValueError, match=message):
        table.cast(target_schema2)


def test_table_safe_casting():
    data = [
        pa.array(range(5), type=pa.int64()),
        pa.array([-10, -5, 0, 5, 10], type=pa.int32()),
        pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64()),
        pa.array(['ab', 'bc', 'cd', 'de', 'ef'], type=pa.string())
    ]
    table = pa.Table.from_arrays(data, names=tuple('abcd'))

    expected_data = [
        pa.array(range(5), type=pa.int32()),
        pa.array([-10, -5, 0, 5, 10], type=pa.int16()),
        pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        pa.array(['ab', 'bc', 'cd', 'de', 'ef'], type=pa.string())
    ]
    expected_table = pa.Table.from_arrays(expected_data, names=tuple('abcd'))

    target_schema = pa.schema([
        pa.field('a', pa.int32()),
        pa.field('b', pa.int16()),
        pa.field('c', pa.int64()),
        pa.field('d', pa.string())
    ])
    casted_table = table.cast(target_schema)

    assert casted_table.equals(expected_table)


def test_table_unsafe_casting():
    data = [
        pa.array(range(5), type=pa.int64()),
        pa.array([-10, -5, 0, 5, 10], type=pa.int32()),
        pa.array([1.1, 2.2, 3.3, 4.4, 5.5], type=pa.float64()),
        pa.array(['ab', 'bc', 'cd', 'de', 'ef'], type=pa.string())
    ]
    table = pa.Table.from_arrays(data, names=tuple('abcd'))

    expected_data = [
        pa.array(range(5), type=pa.int32()),
        pa.array([-10, -5, 0, 5, 10], type=pa.int16()),
        pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        pa.array(['ab', 'bc', 'cd', 'de', 'ef'], type=pa.string())
    ]
    expected_table = pa.Table.from_arrays(expected_data, names=tuple('abcd'))

    target_schema = pa.schema([
        pa.field('a', pa.int32()),
        pa.field('b', pa.int16()),
        pa.field('c', pa.int64()),
        pa.field('d', pa.string())
    ])

    with pytest.raises(pa.ArrowInvalid,
                       match='Floating point value truncated'):
        table.cast(target_schema)

    casted_table = table.cast(target_schema, safe=False)
    assert casted_table.equals(expected_table)


def test_invalid_table_construct():
    array = np.array([0, 1], dtype=np.uint8)
    u8 = pa.uint8()
    arrays = [pa.array(array, type=u8), pa.array(array[1:], type=u8)]

    with pytest.raises(pa.lib.ArrowInvalid):
        pa.Table.from_arrays(arrays, names=["a1", "a2"])


def test_table_from_pydict():
    table = pa.Table.from_pydict({})
    assert table.num_columns == 0
    assert table.num_rows == 0
    assert table.schema == pa.schema([])
    assert table.to_pydict() == {}

    # With arrays as values
    data = OrderedDict([('strs', pa.array([u'', u'foo', u'bar'])),
                        ('floats', pa.array([4.5, 5, None]))])
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float64())])
    table = pa.Table.from_pydict(data)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # With chunked arrays as values
    data = OrderedDict([('strs', pa.chunked_array([[u''], [u'foo', u'bar']])),
                        ('floats', pa.chunked_array([[4.5], [5., None]]))])
    table = pa.Table.from_pydict(data)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # With lists as values
    data = OrderedDict([('strs', [u'', u'foo', u'bar']),
                        ('floats', [4.5, 5, None])])
    table = pa.Table.from_pydict(data)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema
    assert table.to_pydict() == data

    # With metadata and inferred schema
    metadata = {b'foo': b'bar'}
    schema = schema.with_metadata(metadata)
    table = pa.Table.from_pydict(data, metadata=metadata)
    assert table.schema == schema
    assert table.schema.metadata == metadata
    assert table.to_pydict() == data

    # With explicit schema
    table = pa.Table.from_pydict(data, schema=schema)
    assert table.schema == schema
    assert table.schema.metadata == metadata
    assert table.to_pydict() == data

    # Cannot pass both schema and metadata
    with pytest.raises(ValueError):
        pa.Table.from_pydict(data, schema=schema, metadata=metadata)


@pytest.mark.pandas
def test_table_factory_function():
    import pandas as pd

    # Put in wrong order to make sure that lines up with schema
    d = OrderedDict([('b', ['a', 'b', 'c']), ('a', [1, 2, 3])])

    d_explicit = {'b': pa.array(['a', 'b', 'c'], type='string'),
                  'a': pa.array([1, 2, 3], type='int32')}

    schema = pa.schema([('a', pa.int32()), ('b', pa.string())])

    df = pd.DataFrame(d)
    table1 = pa.table(df)
    table2 = pa.Table.from_pandas(df)
    assert table1.equals(table2)
    table1 = pa.table(df, schema=schema)
    table2 = pa.Table.from_pandas(df, schema=schema)
    assert table1.equals(table2)

    table1 = pa.table(d_explicit)
    table2 = pa.Table.from_pydict(d_explicit)
    assert table1.equals(table2)

    # schema coerces type
    table1 = pa.table(d, schema=schema)
    table2 = pa.Table.from_pydict(d, schema=schema)
    assert table1.equals(table2)


def test_table_function_unicode_schema():
    col_a = "äääh"
    col_b = "öööf"

    # Put in wrong order to make sure that lines up with schema
    d = OrderedDict([(col_b, ['a', 'b', 'c']), (col_a, [1, 2, 3])])

    schema = pa.schema([(col_a, pa.int32()), (col_b, pa.string())])

    result = pa.table(d, schema=schema)
    assert result[0].chunk(0).equals(pa.array([1, 2, 3], type='int32'))
    assert result[1].chunk(0).equals(pa.array(['a', 'b', 'c'], type='string'))
