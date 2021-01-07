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
from collections.abc import Iterable
import pickle
import sys
import weakref

import numpy as np
import pytest
import pyarrow as pa


def test_chunked_array_basics():
    data = pa.chunked_array([], type=pa.string())
    assert data.type == pa.string()
    assert data.to_pylist() == []
    data.validate()

    data2 = pa.chunked_array([], type='binary')
    assert data2.type == pa.binary()

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
    assert data.nbytes == sum(c.nbytes for c in data.iterchunks())
    assert sys.getsizeof(data) >= object.__sizeof__(data) + data.nbytes
    data.validate()

    wr = weakref.ref(data)
    assert wr() is not None
    del data
    assert wr() is None


def test_chunked_array_construction():
    arr = pa.chunked_array([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ])
    assert arr.type == pa.int64()
    assert len(arr) == 9
    assert len(arr.chunks) == 3

    arr = pa.chunked_array([
        [1, 2, 3],
        [4., 5., 6.],
        [7, 8, 9],
    ])
    assert arr.type == pa.int64()
    assert len(arr) == 9
    assert len(arr.chunks) == 3

    arr = pa.chunked_array([
        [1, 2, 3],
        [4., 5., 6.],
        [7, 8, 9],
    ], type=pa.int8())
    assert arr.type == pa.int8()
    assert len(arr) == 9
    assert len(arr.chunks) == 3

    arr = pa.chunked_array([
        [1, 2, 3],
        []
    ])
    assert arr.type == pa.int64()
    assert len(arr) == 3
    assert len(arr.chunks) == 2

    msg = (
        "When passing an empty collection of arrays you must also pass the "
        "data type"
    )
    with pytest.raises(ValueError, match=msg):
        assert pa.chunked_array([])

    assert pa.chunked_array([], type=pa.string()).type == pa.string()
    assert pa.chunked_array([[]]).type == pa.null()
    assert pa.chunked_array([[]], type=pa.string()).type == pa.string()


def test_combine_chunks():
    # ARROW-77363
    arr = pa.array([1, 2])
    chunked_arr = pa.chunked_array([arr, arr])
    res = chunked_arr.combine_chunks()
    expected = pa.array([1, 2, 1, 2])
    assert res.equals(expected)


def test_chunked_array_to_numpy():
    data = pa.chunked_array([
        [1, 2, 3],
        [4, 5, 6],
        []
    ])
    arr1 = np.asarray(data)
    arr2 = data.to_numpy()

    assert isinstance(arr2, np.ndarray)
    assert arr2.shape == (6,)
    assert np.array_equal(arr1, arr2)


def test_chunked_array_mismatch_types():
    with pytest.raises(TypeError):
        # Given array types are different
        pa.chunked_array([
            pa.array([1, 2, 3]),
            pa.array([1., 2., 3.])
        ])

    with pytest.raises(TypeError):
        # Given array type is different from explicit type argument
        pa.chunked_array([pa.array([1, 2, 3])], type=pa.float64())


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
    # Ensure this works with numpy scalars
    assert data[np.int32(1)].as_py() == 2

    data_slice = data[2:4]
    assert data_slice.to_pylist() == [3, 4]

    data_slice = data[4:-1]
    assert data_slice.to_pylist() == [5]

    data_slice = data[99:99]
    assert data_slice.type == data.type
    assert data_slice.to_pylist() == []


def test_chunked_array_slice():
    data = [
        pa.array([1, 2, 3]),
        pa.array([4, 5, 6])
    ]
    data = pa.chunked_array(data)

    data_slice = data.slice(len(data))
    assert data_slice.type == data.type
    assert data_slice.to_pylist() == []

    data_slice = data.slice(len(data) + 10)
    assert data_slice.type == data.type
    assert data_slice.to_pylist() == []

    table = pa.Table.from_arrays([data], names=["a"])
    table_slice = table.slice(len(table))
    assert len(table_slice) == 0

    table = pa.Table.from_arrays([data], names=["a"])
    table_slice = table.slice(len(table) + 10)
    assert len(table_slice) == 0


def test_chunked_array_iter():
    data = [
        pa.array([0]),
        pa.array([1, 2, 3]),
        pa.array([4, 5, 6]),
        pa.array([7, 8, 9])
    ]
    arr = pa.chunked_array(data)

    for i, j in zip(range(10), arr):
        assert i == j.as_py()

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
    import pandas as pd

    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.table(data, names=['a'])
    col = table.column(0)
    assert isinstance(col, pa.ChunkedArray)
    series = col.to_pandas()
    assert isinstance(series, pd.Series)
    assert series.shape == (5,)
    assert series[0] == -10
    assert series.name == 'a'


@pytest.mark.pandas
def test_chunked_array_to_pandas_preserve_name():
    # https://issues.apache.org/jira/browse/ARROW-7709
    import pandas as pd
    import pandas.testing as tm

    for data in [
            pa.array([1, 2, 3]),
            pa.array(pd.Categorical(["a", "b", "a"])),
            pa.array(pd.date_range("2012", periods=3)),
            pa.array(pd.date_range("2012", periods=3, tz="Europe/Brussels")),
            pa.array([1, 2, 3], pa.timestamp("ms")),
            pa.array([1, 2, 3], pa.timestamp("ms", "Europe/Brussels"))]:
        table = pa.table({"name": data})
        result = table.column("name").to_pandas()
        assert result.name == "name"
        expected = pd.Series(data.to_pandas(), name="name")
        tm.assert_series_equal(result, expected)


@pytest.mark.pandas
@pytest.mark.nopandas
def test_chunked_array_asarray():
    # ensure this is tested both when pandas is present or not (ARROW-6564)

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

    # DictionaryType data will be converted to dense numpy array
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 0, 1]), pa.array(['a', 'b', 'c']))
    chunked_arr = pa.chunked_array([arr, arr])
    np_arr = np.asarray(chunked_arr)
    assert np_arr.dtype == np.dtype('object')
    assert np_arr.tolist() == ['a', 'b', 'c', 'a', 'b'] * 2


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
        pa.array([-10, -5, 0, None, 10], type='int32')
    ]

    batch = pa.record_batch(data, ['c0', 'c1'])
    assert not batch.schema.metadata

    assert len(batch) == 5
    assert batch.num_rows == 5
    assert batch.num_columns == len(data)
    # (only the second array has a null bitmap)
    assert batch.nbytes == (5 * 2) + (5 * 4 + 1)
    assert sys.getsizeof(batch) >= object.__sizeof__(batch) + batch.nbytes
    pydict = batch.to_pydict()
    assert pydict == OrderedDict([
        ('c0', [0, 1, 2, 3, 4]),
        ('c1', [-10, -5, 0, None, 10])
    ])
    if sys.version_info >= (3, 7):
        assert type(pydict) == dict
    else:
        assert type(pydict) == OrderedDict

    with pytest.raises(IndexError):
        # bounds checking
        batch[2]

    # Schema passed explicitly
    schema = pa.schema([pa.field('c0', pa.int16(),
                                 metadata={'key': 'value'}),
                        pa.field('c1', pa.int32())],
                       metadata={b'foo': b'bar'})
    batch = pa.record_batch(data, schema=schema)
    assert batch.schema == schema
    # schema as first positional argument
    batch = pa.record_batch(data, schema)
    assert batch.schema == schema
    assert str(batch) == """pyarrow.RecordBatch
c0: int16
c1: int32"""

    assert batch.to_string(show_metadata=True) == """\
pyarrow.RecordBatch
c0: int16
  -- field metadata --
  key: 'value'
c1: int32
-- schema metadata --
foo: 'bar'"""

    wr = weakref.ref(batch)
    assert wr() is not None
    del batch
    assert wr() is None


def test_recordbatch_equals():
    data1 = [
        pa.array(range(5), type='int16'),
        pa.array([-10, -5, 0, None, 10], type='int32')
    ]
    data2 = [
        pa.array(['a', 'b', 'c']),
        pa.array([['d'], ['e'], ['f']]),
    ]
    column_names = ['c0', 'c1']

    batch = pa.record_batch(data1, column_names)
    assert batch == pa.record_batch(data1, column_names)
    assert batch.equals(pa.record_batch(data1, column_names))

    assert batch != pa.record_batch(data2, column_names)
    assert not batch.equals(pa.record_batch(data2, column_names))

    batch_meta = pa.record_batch(data1, names=column_names,
                                 metadata={'key': 'value'})
    assert batch_meta.equals(batch)
    assert not batch_meta.equals(batch, check_metadata=True)

    # ARROW-8889
    assert not batch.equals(None)
    assert batch != "foo"


def test_recordbatch_take():
    batch = pa.record_batch(
        [pa.array([1, 2, 3, None, 5]),
         pa.array(['a', 'b', 'c', 'd', 'e'])],
        ['f1', 'f2'])
    assert batch.take(pa.array([2, 3])).equals(batch.slice(2, 2))
    assert batch.take(pa.array([2, None])).equals(
        pa.record_batch([pa.array([3, None]), pa.array(['c', None])],
                        ['f1', 'f2']))


def test_recordbatch_column_sets_private_name():
    # ARROW-6429
    rb = pa.record_batch([pa.array([1, 2, 3, 4])], names=['a0'])
    assert rb[0]._name == 'a0'


def test_recordbatch_from_arrays_validate_schema():
    # ARROW-6263
    arr = pa.array([1, 2])
    schema = pa.schema([pa.field('f0', pa.list_(pa.utf8()))])
    with pytest.raises(NotImplementedError):
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


def test_recordbatch_get_field():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    batch = pa.RecordBatch.from_arrays(data, names=('a', 'b', 'c'))

    assert batch.field('a').equals(batch.schema.field('a'))
    assert batch.field(0).equals(batch.schema.field('a'))

    with pytest.raises(KeyError):
        batch.field('d')

    with pytest.raises(TypeError):
        batch.field(None)

    with pytest.raises(IndexError):
        batch.field(4)


def test_recordbatch_select_column():
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10]),
        pa.array(range(5, 10))
    ]
    batch = pa.RecordBatch.from_arrays(data, names=('a', 'b', 'c'))

    assert batch.column('a').equals(batch.column(0))

    with pytest.raises(
            KeyError, match='Field "d" does not exist in record batch schema'):
        batch.column('d')

    with pytest.raises(TypeError):
        batch.column(None)

    with pytest.raises(IndexError):
        batch.column(4)


def test_recordbatch_from_struct_array_invalid():
    with pytest.raises(TypeError):
        pa.RecordBatch.from_struct_array(pa.array(range(5)))


def test_recordbatch_from_struct_array():
    struct_array = pa.array(
        [{"ints": 1}, {"floats": 1.0}],
        type=pa.struct([("ints", pa.int32()), ("floats", pa.float32())]),
    )
    result = pa.RecordBatch.from_struct_array(struct_array)
    assert result.equals(pa.RecordBatch.from_arrays(
        [
            pa.array([1, None], type=pa.int32()),
            pa.array([None, 1.0], type=pa.float32()),
        ], ["ints", "floats"]
    ))


def _table_like_slice_tests(factory):
    data = [
        pa.array(range(5)),
        pa.array([-10, -5, 0, 5, 10])
    ]
    names = ['c0', 'c1']

    obj = factory(data, names=names)

    sliced = obj.slice(2)
    assert sliced.num_rows == 3

    expected = factory([x.slice(2) for x in data], names=names)
    assert sliced.equals(expected)

    sliced2 = obj.slice(2, 2)
    expected2 = factory([x.slice(2, 2) for x in data], names=names)
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


@pytest.mark.pandas
def test_slice_zero_length_table():
    # ARROW-7907: a segfault on this code was fixed after 0.16.0
    table = pa.table({'a': pa.array([], type=pa.timestamp('us'))})
    table_slice = table.slice(0, 0)
    table_slice.to_pandas()

    table = pa.table({'a': pa.chunked_array([], type=pa.string())})
    table.to_pandas()


def test_recordbatchlist_schema_equals():
    a1 = np.array([1], dtype='uint32')
    a2 = np.array([4.0, 5.0], dtype='float64')
    batch1 = pa.record_batch([pa.array(a1)], ['c1'])
    batch2 = pa.record_batch([pa.array(a2)], ['c1'])

    with pytest.raises(pa.ArrowInvalid):
        pa.Table.from_batches([batch1, batch2])


def test_table_column_sets_private_name():
    # ARROW-6429
    t = pa.table([pa.array([1, 2, 3, 4])], names=['a0'])
    assert t[0]._name == 'a0'


def test_table_equals():
    table = pa.Table.from_arrays([], names=[])

    assert table.equals(table)
    # ARROW-4822
    assert not table.equals(None)

    other = pa.Table.from_arrays([], names=[], metadata={'key': 'value'})
    assert not table.equals(other, check_metadata=True)
    assert table.equals(other)


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
    from pandas.testing import assert_frame_equal
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
        pa.array(range(5), type='int64'),
        pa.array([-10, -5, 0, 5, 10], type='int64')
    ]
    table = pa.table(data, names=('a', 'b'))
    table.validate()
    assert len(table) == 5
    assert table.num_rows == 5
    assert table.num_columns == 2
    assert table.shape == (5, 2)
    assert table.nbytes == 2 * (5 * 8)
    assert sys.getsizeof(table) >= object.__sizeof__(table) + table.nbytes
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

    wr = weakref.ref(table)
    assert wr() is not None
    del table
    assert wr() is None


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

    with pytest.raises(KeyError,
                       match='Field "d" does not exist in table schema'):
        table.column('d')

    with pytest.raises(TypeError):
        table.column(None)

    with pytest.raises(IndexError):
        table.column(4)


def test_table_column_with_duplicates():
    # ARROW-8209
    table = pa.table([pa.array([1, 2, 3]),
                      pa.array([4, 5, 6]),
                      pa.array([7, 8, 9])], names=['a', 'b', 'a'])

    with pytest.raises(KeyError,
                       match='Field "a" exists 2 times in table schema'):
        table.column('a')


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
    assert table1.schema.equals(table2.schema)
    assert not table1.schema.equals(table2.schema, check_metadata=True)

    table3 = pa.concat_tables([table1, table2])
    assert table1.schema.equals(table3.schema, check_metadata=True)
    assert table2.schema.equals(table3.schema)


def test_concat_tables_with_promotion():
    t1 = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64())], ["int64_field"])
    t2 = pa.Table.from_arrays(
        [pa.array([1.0, 2.0], type=pa.float32())], ["float_field"])

    result = pa.concat_tables([t1, t2], promote=True)

    assert result.equals(pa.Table.from_arrays([
        pa.array([1, 2, None, None], type=pa.int64()),
        pa.array([None, None, 1.0, 2.0], type=pa.float32()),
    ], ["int64_field", "float_field"]))


def test_concat_tables_with_promotion_error():
    t1 = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.int64())], ["f"])
    t2 = pa.Table.from_arrays(
        [pa.array([1, 2], type=pa.float32())], ["f"])

    with pytest.raises(pa.ArrowInvalid):
        pa.concat_tables([t1, t2], promote=True)


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

    with pytest.raises(pa.ArrowInvalid, match='truncated'):
        table.cast(target_schema)

    casted_table = table.cast(target_schema, safe=False)
    assert casted_table.equals(expected_table)


def test_invalid_table_construct():
    array = np.array([0, 1], dtype=np.uint8)
    u8 = pa.uint8()
    arrays = [pa.array(array, type=u8), pa.array(array[1:], type=u8)]

    with pytest.raises(pa.lib.ArrowInvalid):
        pa.Table.from_arrays(arrays, names=["a1", "a2"])


@pytest.mark.parametrize('data, klass', [
    ((['', 'foo', 'bar'], [4.5, 5, None]), list),
    ((['', 'foo', 'bar'], [4.5, 5, None]), pa.array),
    (([[''], ['foo', 'bar']], [[4.5], [5., None]]), pa.chunked_array),
])
def test_from_arrays_schema(data, klass):
    data = [klass(data[0]), klass(data[1])]
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float32())])

    table = pa.Table.from_arrays(data, schema=schema)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # length of data and schema not matching
    schema = pa.schema([('strs', pa.utf8())])
    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, schema=schema)

    # with different but compatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float32())])
    table = pa.Table.from_arrays(data, schema=schema)
    assert pa.types.is_float32(table.column('floats').type)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # with different and incompatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.timestamp('s'))])
    with pytest.raises((NotImplementedError, TypeError)):
        pa.Table.from_pydict(data, schema=schema)

    # Cannot pass both schema and metadata / names
    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, schema=schema, names=['strs', 'floats'])

    with pytest.raises(ValueError):
        pa.Table.from_arrays(data, schema=schema, metadata={b'foo': b'bar'})


def test_table_from_pydict():
    table = pa.Table.from_pydict({})
    assert table.num_columns == 0
    assert table.num_rows == 0
    assert table.schema == pa.schema([])
    assert table.to_pydict() == {}

    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float64())])

    # With lists as values
    data = OrderedDict([('strs', ['', 'foo', 'bar']),
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

    # Non-convertible values given schema
    with pytest.raises(TypeError):
        pa.Table.from_pydict({'c0': [0, 1, 2]},
                             schema=pa.schema([("c0", pa.string())]))

    # Missing schema fields from the passed mapping
    with pytest.raises(KeyError, match="doesn\'t contain.* c, d"):
        pa.Table.from_pydict(
            {'a': [1, 2, 3], 'b': [3, 4, 5]},
            schema=pa.schema([
                ('a', pa.int64()),
                ('c', pa.int32()),
                ('d', pa.int16())
            ])
        )

    # Passed wrong schema type
    with pytest.raises(TypeError):
        pa.Table.from_pydict({'a': [1, 2, 3]}, schema={})


@pytest.mark.parametrize('data, klass', [
    ((['', 'foo', 'bar'], [4.5, 5, None]), pa.array),
    (([[''], ['foo', 'bar']], [[4.5], [5., None]]), pa.chunked_array),
])
def test_table_from_pydict_arrow_arrays(data, klass):
    data = OrderedDict([('strs', klass(data[0])), ('floats', klass(data[1]))])
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float64())])

    # With arrays as values
    table = pa.Table.from_pydict(data)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # With explicit (matching) schema
    table = pa.Table.from_pydict(data, schema=schema)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # with different but compatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float32())])
    table = pa.Table.from_pydict(data, schema=schema)
    assert pa.types.is_float32(table.column('floats').type)
    assert table.num_columns == 2
    assert table.num_rows == 3
    assert table.schema == schema

    # with different and incompatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.timestamp('s'))])
    with pytest.raises((NotImplementedError, TypeError)):
        pa.Table.from_pydict(data, schema=schema)


@pytest.mark.parametrize('data, klass', [
    ((['', 'foo', 'bar'], [4.5, 5, None]), list),
    ((['', 'foo', 'bar'], [4.5, 5, None]), pa.array),
    (([[''], ['foo', 'bar']], [[4.5], [5., None]]), pa.chunked_array),
])
def test_table_from_pydict_schema(data, klass):
    # passed schema is source of truth for the columns

    data = OrderedDict([('strs', klass(data[0])), ('floats', klass(data[1]))])

    # schema has columns not present in data -> error
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float64()),
                        ('ints', pa.int64())])
    with pytest.raises(KeyError, match='ints'):
        pa.Table.from_pydict(data, schema=schema)

    # data has columns not present in schema -> ignored
    schema = pa.schema([('strs', pa.utf8())])
    table = pa.Table.from_pydict(data, schema=schema)
    assert table.num_columns == 1
    assert table.schema == schema
    assert table.column_names == ['strs']


@pytest.mark.pandas
def test_table_from_pandas_schema():
    # passed schema is source of truth for the columns
    import pandas as pd

    df = pd.DataFrame(OrderedDict([('strs', ['', 'foo', 'bar']),
                                   ('floats', [4.5, 5, None])]))

    # with different but compatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float32())])
    table = pa.Table.from_pandas(df, schema=schema)
    assert pa.types.is_float32(table.column('floats').type)
    assert table.schema.remove_metadata() == schema

    # with different and incompatible schema
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.timestamp('s'))])
    with pytest.raises((NotImplementedError, TypeError)):
        pa.Table.from_pandas(df, schema=schema)

    # schema has columns not present in data -> error
    schema = pa.schema([('strs', pa.utf8()), ('floats', pa.float64()),
                        ('ints', pa.int64())])
    with pytest.raises(KeyError, match='ints'):
        pa.Table.from_pandas(df, schema=schema)

    # data has columns not present in schema -> ignored
    schema = pa.schema([('strs', pa.utf8())])
    table = pa.Table.from_pandas(df, schema=schema)
    assert table.num_columns == 1
    assert table.schema.remove_metadata() == schema
    assert table.column_names == ['strs']


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


def test_table_factory_function_args():
    # from_pydict not accepting names:
    with pytest.raises(ValueError):
        pa.table({'a': [1, 2, 3]}, names=['a'])

    # backwards compatibility for schema as first positional argument
    schema = pa.schema([('a', pa.int32())])
    table = pa.table({'a': pa.array([1, 2, 3], type=pa.int64())}, schema)
    assert table.column('a').type == pa.int32()

    # from_arrays: accept both names and schema as positional first argument
    data = [pa.array([1, 2, 3], type='int64')]
    names = ['a']
    table = pa.table(data, names)
    assert table.column_names == names
    schema = pa.schema([('a', pa.int64())])
    table = pa.table(data, schema)
    assert table.column_names == names


@pytest.mark.pandas
def test_table_factory_function_args_pandas():
    import pandas as pd

    # from_pandas not accepting names or metadata:
    with pytest.raises(ValueError):
        pa.table(pd.DataFrame({'a': [1, 2, 3]}), names=['a'])

    with pytest.raises(ValueError):
        pa.table(pd.DataFrame({'a': [1, 2, 3]}), metadata={b'foo': b'bar'})

    # backwards compatibility for schema as first positional argument
    schema = pa.schema([('a', pa.int32())])
    table = pa.table(pd.DataFrame({'a': [1, 2, 3]}), schema)
    assert table.column('a').type == pa.int32()


def test_factory_functions_invalid_input():
    with pytest.raises(TypeError, match="Expected pandas DataFrame, python"):
        pa.table("invalid input")

    with pytest.raises(TypeError, match="Expected pandas DataFrame"):
        pa.record_batch("invalid input")


def test_table_repr_to_string():
    # Schema passed explicitly
    schema = pa.schema([pa.field('c0', pa.int16(),
                                 metadata={'key': 'value'}),
                        pa.field('c1', pa.int32())],
                       metadata={b'foo': b'bar'})

    tab = pa.table([pa.array([1, 2, 3, 4], type='int16'),
                    pa.array([1, 2, 3, 4], type='int32')], schema=schema)
    assert str(tab) == """pyarrow.Table
c0: int16
c1: int32"""

    assert tab.to_string(show_metadata=True) == """\
pyarrow.Table
c0: int16
  -- field metadata --
  key: 'value'
c1: int32
-- schema metadata --
foo: 'bar'"""


def test_table_function_unicode_schema():
    col_a = "äääh"
    col_b = "öööf"

    # Put in wrong order to make sure that lines up with schema
    d = OrderedDict([(col_b, ['a', 'b', 'c']), (col_a, [1, 2, 3])])

    schema = pa.schema([(col_a, pa.int32()), (col_b, pa.string())])

    result = pa.table(d, schema=schema)
    assert result[0].chunk(0).equals(pa.array([1, 2, 3], type='int32'))
    assert result[1].chunk(0).equals(pa.array(['a', 'b', 'c'], type='string'))


def test_table_take_vanilla_functionality():
    table = pa.table(
        [pa.array([1, 2, 3, None, 5]),
         pa.array(['a', 'b', 'c', 'd', 'e'])],
        ['f1', 'f2'])

    assert table.take(pa.array([2, 3])).equals(table.slice(2, 2))


def test_table_take_null_index():
    table = pa.table(
        [pa.array([1, 2, 3, None, 5]),
         pa.array(['a', 'b', 'c', 'd', 'e'])],
        ['f1', 'f2'])

    result_with_null_index = pa.table(
        [pa.array([1, None]),
         pa.array(['a', None])],
        ['f1', 'f2'])

    assert table.take(pa.array([0, None])).equals(result_with_null_index)


def test_table_take_non_consecutive():
    table = pa.table(
        [pa.array([1, 2, 3, None, 5]),
         pa.array(['a', 'b', 'c', 'd', 'e'])],
        ['f1', 'f2'])

    result_non_consecutive = pa.table(
        [pa.array([2, None]),
         pa.array(['b', 'd'])],
        ['f1', 'f2'])

    assert table.take(pa.array([1, 3])).equals(result_non_consecutive)


def test_table_select():
    a1 = pa.array([1, 2, 3, None, 5])
    a2 = pa.array(['a', 'b', 'c', 'd', 'e'])
    a3 = pa.array([[1, 2], [3, 4], [5, 6], None, [9, 10]])
    table = pa.table([a1, a2, a3], ['f1', 'f2', 'f3'])

    # selecting with string names
    result = table.select(['f1'])
    expected = pa.table([a1], ['f1'])
    assert result.equals(expected)

    result = table.select(['f3', 'f2'])
    expected = pa.table([a3, a2], ['f3', 'f2'])
    assert result.equals(expected)

    # selecting with integer indices
    result = table.select([0])
    expected = pa.table([a1], ['f1'])
    assert result.equals(expected)

    result = table.select([2, 1])
    expected = pa.table([a3, a2], ['f3', 'f2'])
    assert result.equals(expected)

    # preserve metadata
    table2 = table.replace_schema_metadata({"a": "test"})
    result = table2.select(["f1", "f2"])
    assert b"a" in result.schema.metadata

    # selecting non-existing column raises
    with pytest.raises(KeyError, match='Field "f5" does not exist'):
        table.select(['f5'])

    with pytest.raises(IndexError, match="index out of bounds"):
        table.select([5])

    # duplicate selection gives duplicated names in resulting table
    result = table.select(['f2', 'f2'])
    expected = pa.table([a2, a2], ['f2', 'f2'])
    assert result.equals(expected)

    # selection duplicated column raises
    table = pa.table([a1, a2, a3], ['f1', 'f2', 'f1'])
    with pytest.raises(KeyError, match='Field "f1" exists 2 times'):
        table.select(['f1'])

    result = table.select(['f2'])
    expected = pa.table([a2], ['f2'])
    assert result.equals(expected)
