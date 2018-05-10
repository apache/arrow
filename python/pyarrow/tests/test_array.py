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

import datetime
import pytest
import struct
import sys

import numpy as np
import pandas as pd
import pandas.util.testing as tm
import pickle

import pyarrow as pa
from pyarrow.pandas_compat import get_logical_type
import pyarrow.formatting as fmt


def test_total_bytes_allocated():
    assert pa.total_allocated_bytes() == 0


def test_repr_on_pre_init_array():
    arr = pa.Array()
    assert len(repr(arr)) > 0


def test_getitem_NA():
    arr = pa.array([1, None, 2])
    assert arr[1] is pa.NA


def test_constructor_raises():
    # This could happen by wrong capitalization.
    with pytest.raises(RuntimeError):
        pa.Array([1, 2])


def test_list_format():
    arr = pa.array([[1], None, [2, 3, None]])
    result = fmt.array_format(arr)
    expected = """\
[
  [1],
  NA,
  [2,
   3,
   NA]
]"""
    assert result == expected


def test_string_format():
    arr = pa.array(['', None, 'foo'])
    result = fmt.array_format(arr)
    expected = """\
[
  '',
  NA,
  'foo'
]"""
    assert result == expected


def test_long_array_format():
    arr = pa.array(range(100))
    result = fmt.array_format(arr, window=2)
    expected = """\
[
  0,
  1,
  ...
  98,
  99
]"""
    assert result == expected


def test_to_pandas_zero_copy():
    import gc

    arr = pa.array(range(10))

    for i in range(10):
        np_arr = arr.to_pandas()
        assert sys.getrefcount(np_arr) == 2
        np_arr = None  # noqa

    assert sys.getrefcount(arr) == 2

    for i in range(10):
        arr = pa.array(range(10))
        np_arr = arr.to_pandas()
        arr = None
        gc.collect()

        # Ensure base is still valid

        # Because of py.test's assert inspection magic, if you put getrefcount
        # on the line being examined, it will be 1 higher than you expect
        base_refcount = sys.getrefcount(np_arr.base)
        assert base_refcount == 2
        np_arr.sum()


def test_array_getitem():
    arr = pa.array(range(10, 15))
    lst = arr.to_pylist()

    for idx in range(-len(arr), len(arr)):
        assert arr[idx].as_py() == lst[idx]
    for idx in range(-2 * len(arr), -len(arr)):
        with pytest.raises(IndexError):
            arr[idx]
    for idx in range(len(arr), 2 * len(arr)):
        with pytest.raises(IndexError):
            arr[idx]


def test_array_slice():
    arr = pa.array(range(10))

    sliced = arr.slice(2)
    expected = pa.array(range(2, 10))
    assert sliced.equals(expected)

    sliced2 = arr.slice(2, 4)
    expected2 = pa.array(range(2, 6))
    assert sliced2.equals(expected2)

    # 0 offset
    assert arr.slice(0).equals(arr)

    # Slice past end of array
    assert len(arr.slice(len(arr))) == 0

    with pytest.raises(IndexError):
        arr.slice(-1)

    # Test slice notation
    assert arr[2:].equals(arr.slice(2))
    assert arr[2:5].equals(arr.slice(2, 3))
    assert arr[-5:].equals(arr.slice(len(arr) - 5))
    with pytest.raises(IndexError):
        arr[::-1]
    with pytest.raises(IndexError):
        arr[::2]

    n = len(arr)
    for start in range(-n * 2, n * 2):
        for stop in range(-n * 2, n * 2):
            assert arr[start:stop].to_pylist() == arr.to_pylist()[start:stop]


def test_struct_array_slice():
    # ARROW-2311: slicing nested arrays needs special care
    ty = pa.struct([pa.field('a', pa.int8()),
                    pa.field('b', pa.float32())])
    arr = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)
    assert arr[1:].to_pylist() == [{'a': 3, 'b': 4.5},
                                   {'a': 5, 'b': 6.5}]


def test_array_factory_invalid_type():
    arr = np.array([datetime.timedelta(1), datetime.timedelta(2)])
    with pytest.raises(ValueError):
        pa.array(arr)


def test_array_ref_to_ndarray_base():
    arr = np.array([1, 2, 3])

    refcount = sys.getrefcount(arr)
    arr2 = pa.array(arr)  # noqa
    assert sys.getrefcount(arr) == (refcount + 1)


def test_array_eq_raises():
    # ARROW-2150: we are raising when comparing arrays until we define the
    # behavior to either be elementwise comparisons or data equality
    arr1 = pa.array([1, 2, 3], type=pa.int32())
    arr2 = pa.array([1, 2, 3], type=pa.int32())

    with pytest.raises(NotImplementedError):
        arr1 == arr2


def test_array_from_buffers():
    values_buf = pa.py_buffer(np.int16([4, 5, 6, 7]))
    nulls_buf = pa.py_buffer(np.uint8([0b00001101]))
    arr = pa.Array.from_buffers(pa.int16(), 4, [nulls_buf, values_buf])
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [4, None, 6, 7]

    arr = pa.Array.from_buffers(pa.int16(), 4, [None, values_buf])
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [4, 5, 6, 7]

    arr = pa.Array.from_buffers(pa.int16(), 3, [nulls_buf, values_buf],
                                offset=1)
    assert arr.type == pa.int16()
    assert arr.to_pylist() == [None, 6, 7]

    with pytest.raises(TypeError):
        pa.Array.from_buffers(pa.int16(), 3, [u'', u''], offset=1)

    with pytest.raises(NotImplementedError):
        pa.Array.from_buffers(pa.list_(pa.int16()), 4, [None, values_buf])


def test_dictionary_from_numpy():
    indices = np.repeat([0, 1, 2], 2)
    dictionary = np.array(['foo', 'bar', 'baz'], dtype=object)
    mask = np.array([False, False, True, False, False, False])

    d1 = pa.DictionaryArray.from_arrays(indices, dictionary)
    d2 = pa.DictionaryArray.from_arrays(indices, dictionary, mask=mask)

    for i in range(len(indices)):
        assert d1[i].as_py() == dictionary[indices[i]]

        if mask[i]:
            assert d2[i] is pa.NA
        else:
            assert d2[i].as_py() == dictionary[indices[i]]


def test_dictionary_from_boxed_arrays():
    indices = np.repeat([0, 1, 2], 2)
    dictionary = np.array(['foo', 'bar', 'baz'], dtype=object)

    iarr = pa.array(indices)
    darr = pa.array(dictionary)

    d1 = pa.DictionaryArray.from_arrays(iarr, darr)

    for i in range(len(indices)):
        assert d1[i].as_py() == dictionary[indices[i]]


def test_dictionary_from_arrays_boundscheck():
    indices1 = pa.array([0, 1, 2, 0, 1, 2])
    indices2 = pa.array([0, -1, 2])
    indices3 = pa.array([0, 1, 2, 3])

    dictionary = pa.array(['foo', 'bar', 'baz'])

    # Works fine
    pa.DictionaryArray.from_arrays(indices1, dictionary)

    with pytest.raises(pa.ArrowException):
        pa.DictionaryArray.from_arrays(indices2, dictionary)

    with pytest.raises(pa.ArrowException):
        pa.DictionaryArray.from_arrays(indices3, dictionary)

    # If we are confident that the indices are "safe" we can pass safe=False to
    # disable the boundschecking
    pa.DictionaryArray.from_arrays(indices2, dictionary, safe=False)


def test_dictionary_with_pandas():
    indices = np.repeat([0, 1, 2], 2)
    dictionary = np.array(['foo', 'bar', 'baz'], dtype=object)
    mask = np.array([False, False, True, False, False, False])

    d1 = pa.DictionaryArray.from_arrays(indices, dictionary)
    d2 = pa.DictionaryArray.from_arrays(indices, dictionary, mask=mask)

    pandas1 = d1.to_pandas()
    ex_pandas1 = pd.Categorical.from_codes(indices, categories=dictionary)

    tm.assert_series_equal(pd.Series(pandas1), pd.Series(ex_pandas1))

    pandas2 = d2.to_pandas()
    ex_pandas2 = pd.Categorical.from_codes(np.where(mask, -1, indices),
                                           categories=dictionary)

    tm.assert_series_equal(pd.Series(pandas2), pd.Series(ex_pandas2))


def test_list_from_arrays():
    offsets_arr = np.array([0, 2, 5, 8], dtype='i4')
    offsets = pa.array(offsets_arr, type='int32')
    pyvalues = [b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h']
    values = pa.array(pyvalues, type='binary')

    result = pa.ListArray.from_arrays(offsets, values)
    expected = pa.array([pyvalues[:2], pyvalues[2:5], pyvalues[5:8]])

    assert result.equals(expected)

    # With nulls
    offsets = [0, None, 2, 6]

    values = ['a', 'b', 'c', 'd', 'e', 'f']

    result = pa.ListArray.from_arrays(offsets, values)
    expected = pa.array([values[:2], None, values[2:]])

    assert result.equals(expected)

    # Another edge case
    offsets2 = [0, 2, None, 6]
    result = pa.ListArray.from_arrays(offsets2, values)
    expected = pa.array([values[:2], values[2:], None])
    assert result.equals(expected)


def test_union_from_dense():
    binary = pa.array([b'a', b'b', b'c', b'd'], type='binary')
    int64 = pa.array([1, 2, 3], type='int64')
    types = pa.array([0, 1, 0, 0, 1, 1, 0], type='int8')
    value_offsets = pa.array([0, 0, 2, 1, 1, 2, 3], type='int32')

    result = pa.UnionArray.from_dense(types, value_offsets, [binary, int64])

    assert result.to_pylist() == [b'a', 1, b'c', b'b', 2, 3, b'd']


def test_union_from_sparse():
    binary = pa.array([b'a', b' ', b'b', b'c', b' ', b' ', b'd'],
                      type='binary')
    int64 = pa.array([0, 1, 0, 0, 2, 3, 0], type='int64')
    types = pa.array([0, 1, 0, 0, 1, 1, 0], type='int8')

    result = pa.UnionArray.from_sparse(types, [binary, int64])

    assert result.to_pylist() == [b'a', 1, b'b', b'c', 2, 3, b'd']


def test_union_array_slice():
    # ARROW-2314
    arr = pa.UnionArray.from_sparse(pa.array([0, 0, 1, 1], type=pa.int8()),
                                    [pa.array(["a", "b", "c", "d"]),
                                     pa.array([1, 2, 3, 4])])
    assert arr[1:].to_pylist() == ["b", 3, 4]

    binary = pa.array([b'a', b'b', b'c', b'd'], type='binary')
    int64 = pa.array([1, 2, 3], type='int64')
    types = pa.array([0, 1, 0, 0, 1, 1, 0], type='int8')
    value_offsets = pa.array([0, 0, 2, 1, 1, 2, 3], type='int32')

    arr = pa.UnionArray.from_dense(types, value_offsets, [binary, int64])
    lst = arr.to_pylist()
    for i in range(len(arr)):
        for j in range(i, len(arr)):
            assert arr[i:j].to_pylist() == lst[i:j]


def test_string_from_buffers():
    array = pa.array(["a", None, "b", "c"])

    buffers = array.buffers()
    copied = pa.StringArray.from_buffers(
        len(array), buffers[1], buffers[2], buffers[0], array.null_count,
        array.offset)
    assert copied.to_pylist() == ["a", None, "b", "c"]

    copied = pa.StringArray.from_buffers(
        len(array), buffers[1], buffers[2], buffers[0])
    assert copied.to_pylist() == ["a", None, "b", "c"]

    sliced = array[1:]
    buffers = sliced.buffers()
    copied = pa.StringArray.from_buffers(
        len(sliced), buffers[1], buffers[2], buffers[0], -1, sliced.offset)
    assert copied.to_pylist() == [None, "b", "c"]
    assert copied.null_count == 1

    # Slice but exclude all null entries so that we don't need to pass
    # the null bitmap.
    sliced = array[2:]
    buffers = sliced.buffers()
    copied = pa.StringArray.from_buffers(
        len(sliced), buffers[1], buffers[2], None, -1, sliced.offset)
    assert copied.to_pylist() == ["b", "c"]
    assert copied.null_count == 0


def _check_cast_case(case, safe=True):
    in_data, in_type, out_data, out_type = case

    in_arr = pa.array(in_data, type=in_type)

    casted = in_arr.cast(out_type, safe=safe)
    expected = pa.array(out_data, type=out_type)
    assert casted.equals(expected)


def test_cast_integers_safe():
    safe_cases = [
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='i4'), pa.int32()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='u4'), pa.uint16()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='u1'), pa.uint8()),
        (np.array([0, 1, 2, 3], dtype='i1'), 'int8',
         np.array([0, 1, 2, 3], dtype='f8'), pa.float64())
    ]

    for case in safe_cases:
        _check_cast_case(case)

    unsafe_cases = [
        (np.array([50000], dtype='i4'), 'int32', 'int16'),
        (np.array([70000], dtype='i4'), 'int32', 'uint16'),
        (np.array([-1], dtype='i4'), 'int32', 'uint16'),
        (np.array([50000], dtype='u2'), 'uint16', 'int16')
    ]
    for in_data, in_type, out_type in unsafe_cases:
        in_arr = pa.array(in_data, type=in_type)

        with pytest.raises(pa.ArrowInvalid):
            in_arr.cast(out_type)


def test_cast_column():
    arrays = [pa.array([1, 2, 3]), pa.array([4, 5, 6])]

    col = pa.column('foo', arrays)

    target = pa.float64()
    casted = col.cast(target)

    expected = pa.column('foo', [x.cast(target) for x in arrays])
    assert casted.equals(expected)


def test_cast_integers_unsafe():
    # We let NumPy do the unsafe casting
    unsafe_cases = [
        (np.array([50000], dtype='i4'), 'int32',
         np.array([50000], dtype='i2'), pa.int16()),
        (np.array([70000], dtype='i4'), 'int32',
         np.array([70000], dtype='u2'), pa.uint16()),
        (np.array([-1], dtype='i4'), 'int32',
         np.array([-1], dtype='u2'), pa.uint16()),
        (np.array([50000], dtype='u2'), pa.uint16(),
         np.array([50000], dtype='i2'), pa.int16())
    ]

    for case in unsafe_cases:
        _check_cast_case(case, safe=False)


def test_cast_timestamp_unit():
    # ARROW-1680
    val = datetime.datetime.now()
    s = pd.Series([val])
    s_nyc = s.dt.tz_localize('tzlocal()').dt.tz_convert('America/New_York')

    us_with_tz = pa.timestamp('us', tz='America/New_York')

    arr = pa.Array.from_pandas(s_nyc, type=us_with_tz)

    # ARROW-1906
    assert arr.type == us_with_tz

    arr2 = pa.Array.from_pandas(s, type=pa.timestamp('us'))

    assert arr[0].as_py() == s_nyc[0]
    assert arr2[0].as_py() == s[0]

    # Disallow truncation
    arr = pa.array([123123], type='int64').cast(pa.timestamp('ms'))
    expected = pa.array([123], type='int64').cast(pa.timestamp('s'))

    target = pa.timestamp('s')
    with pytest.raises(ValueError):
        arr.cast(target)

    result = arr.cast(target, safe=False)
    assert result.equals(expected)


def test_cast_signed_to_unsigned():
    safe_cases = [
        (np.array([0, 1, 2, 3], dtype='i1'), pa.uint8(),
         np.array([0, 1, 2, 3], dtype='u1'), pa.uint8()),
        (np.array([0, 1, 2, 3], dtype='i2'), pa.uint16(),
         np.array([0, 1, 2, 3], dtype='u2'), pa.uint16())
    ]

    for case in safe_cases:
        _check_cast_case(case)


def test_unique_simple():
    cases = [
        (pa.array([1, 2, 3, 1, 2, 3]), pa.array([1, 2, 3])),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.array(['foo', 'bar']))
    ]
    for arr, expected in cases:
        result = arr.unique()
        assert result.equals(expected)


def test_dictionary_encode_simple():
    cases = [
        (pa.array([1, 2, 3, None, 1, 2, 3]),
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, 2, None, 0, 1, 2], type='int32'),
             [1, 2, 3])),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.DictionaryArray.from_arrays(
             pa.array([0, None, 1, 0], type='int32'),
             ['foo', 'bar']))
    ]
    for arr, expected in cases:
        result = arr.dictionary_encode()
        assert result.equals(expected)


def test_cast_time32_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int32'),
                   type=pa.time32('s'))
    expected = pa.array([0, 1, 2], type='i4')

    result = arr.cast('i4')
    assert result.equals(expected)


def test_cast_time64_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.time64('us'))
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')
    assert result.equals(expected)


def test_cast_timestamp_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.timestamp('us'))
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')
    assert result.equals(expected)


def test_cast_date32_to_int():
    arr = pa.array([0, 1, 2], type='i4')

    result1 = arr.cast('date32')
    result2 = result1.cast('i4')

    expected1 = pa.array([
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 2),
        datetime.date(1970, 1, 3)
    ]).cast('date32')

    assert result1.equals(expected1)
    assert result2.equals(arr)


def test_cast_date64_to_int():
    arr = pa.array(np.array([0, 1, 2], dtype='int64'),
                   type=pa.date64())
    expected = pa.array([0, 1, 2], type='i8')

    result = arr.cast('i8')

    assert result.equals(expected)


def test_simple_type_construction():
    result = pa.lib.TimestampType()
    with pytest.raises(TypeError):
        str(result)


@pytest.mark.parametrize(
    ('data', 'typ'),
    [
        ([True, False, True, True], pa.bool_()),
        ([1, 2, 4, 6], pa.int64()),
        ([1.0, 2.5, None], pa.float64()),
        (['a', None, 'b'], pa.string()),
        ([], None),
        ([[1, 2], [3]], pa.list_(pa.int64())),
        ([['a'], None, ['b', 'c']], pa.list_(pa.string())),
        ([(1, 'a'), (2, 'c'), None],
            pa.struct([pa.field('a', pa.int64()), pa.field('b', pa.string())]))
    ]
)
def test_array_pickle(data, typ):
    # Allocate here so that we don't have any Arrow data allocated.
    # This is needed to ensure that allocator tests can be reliable.
    array = pa.array(data, type=typ)
    result = pickle.loads(pickle.dumps(array))
    assert array.equals(result)


@pytest.mark.parametrize(
    ('type', 'expected'),
    [
        (pa.null(), 'empty'),
        (pa.bool_(), 'bool'),
        (pa.int8(), 'int8'),
        (pa.int16(), 'int16'),
        (pa.int32(), 'int32'),
        (pa.int64(), 'int64'),
        (pa.uint8(), 'uint8'),
        (pa.uint16(), 'uint16'),
        (pa.uint32(), 'uint32'),
        (pa.uint64(), 'uint64'),
        (pa.float16(), 'float16'),
        (pa.float32(), 'float32'),
        (pa.float64(), 'float64'),
        (pa.date32(), 'date'),
        (pa.date64(), 'date'),
        (pa.binary(), 'bytes'),
        (pa.binary(length=4), 'bytes'),
        (pa.string(), 'unicode'),
        (pa.list_(pa.list_(pa.int16())), 'list[list[int16]]'),
        (pa.decimal128(18, 3), 'decimal'),
        (pa.timestamp('ms'), 'datetime'),
        (pa.timestamp('us', 'UTC'), 'datetimetz'),
        (pa.time32('s'), 'time'),
        (pa.time64('us'), 'time')
    ]
)
def test_logical_type(type, expected):
    assert get_logical_type(type) == expected


def test_array_uint64_from_py_over_range():
    arr = pa.array([2 ** 63], type=pa.uint64())
    expected = pa.array(np.array([2 ** 63], dtype='u8'))
    assert arr.equals(expected)


def test_array_conversions_no_sentinel_values():
    arr = np.array([1, 2, 3, 4], dtype='int8')
    refcount = sys.getrefcount(arr)
    arr2 = pa.array(arr)  # noqa
    assert sys.getrefcount(arr) == (refcount + 1)

    assert arr2.type == 'int8'

    arr3 = pa.array(np.array([1, np.nan, 2, 3, np.nan, 4], dtype='float32'),
                    type='float32')
    assert arr3.type == 'float32'
    assert arr3.null_count == 0


def test_array_from_numpy_datetimeD():
    arr = np.array([None, datetime.date(2017, 4, 4)], dtype='datetime64[D]')

    result = pa.array(arr)
    expected = pa.array([None, datetime.date(2017, 4, 4)], type=pa.date32())
    assert result.equals(expected)


def test_array_from_py_float32():
    data = [[1.2, 3.4], [9.0, 42.0]]

    t = pa.float32()

    arr1 = pa.array(data[0], type=t)
    arr2 = pa.array(data, type=pa.list_(t))

    expected1 = np.array(data[0], dtype=np.float32)
    expected2 = pd.Series([np.array(data[0], dtype=np.float32),
                           np.array(data[1], dtype=np.float32)])

    assert arr1.type == t
    assert arr1.equals(pa.array(expected1))
    assert arr2.equals(pa.array(expected2))


def test_array_from_numpy_ascii():
    arr = np.array(['abcde', 'abc', ''], dtype='|S5')

    arrow_arr = pa.array(arr)
    assert arrow_arr.type == 'binary'
    expected = pa.array(['abcde', 'abc', ''], type='binary')
    assert arrow_arr.equals(expected)

    mask = np.array([False, True, False])
    arrow_arr = pa.array(arr, mask=mask)
    expected = pa.array(['abcde', None, ''], type='binary')
    assert arrow_arr.equals(expected)

    # Strided variant
    arr = np.array(['abcde', 'abc', ''] * 5, dtype='|S5')[::2]
    mask = np.array([False, True, False] * 5)[::2]
    arrow_arr = pa.array(arr, mask=mask)

    expected = pa.array(['abcde', '', None, 'abcde', '', None, 'abcde', ''],
                        type='binary')
    assert arrow_arr.equals(expected)

    # 0 itemsize
    arr = np.array(['', '', ''], dtype='|S0')
    arrow_arr = pa.array(arr)
    expected = pa.array(['', '', ''], type='binary')
    assert arrow_arr.equals(expected)


def test_array_from_numpy_unicode():
    dtypes = ['<U5', '>U5']

    for dtype in dtypes:
        arr = np.array(['abcde', 'abc', ''], dtype=dtype)

        arrow_arr = pa.array(arr)
        assert arrow_arr.type == 'utf8'
        expected = pa.array(['abcde', 'abc', ''], type='utf8')
        assert arrow_arr.equals(expected)

        mask = np.array([False, True, False])
        arrow_arr = pa.array(arr, mask=mask)
        expected = pa.array(['abcde', None, ''], type='utf8')
        assert arrow_arr.equals(expected)

        # Strided variant
        arr = np.array(['abcde', 'abc', ''] * 5, dtype=dtype)[::2]
        mask = np.array([False, True, False] * 5)[::2]
        arrow_arr = pa.array(arr, mask=mask)

        expected = pa.array(['abcde', '', None, 'abcde', '', None,
                             'abcde', ''], type='utf8')
        assert arrow_arr.equals(expected)

    # 0 itemsize
    arr = np.array(['', '', ''], dtype='<U0')
    arrow_arr = pa.array(arr)
    expected = pa.array(['', '', ''], type='utf8')
    assert arrow_arr.equals(expected)


def test_buffers_primitive():
    a = pa.array([1, 2, None, 4], type=pa.int16())
    buffers = a.buffers()
    assert len(buffers) == 2
    null_bitmap = buffers[0].to_pybytes()
    assert 1 <= len(null_bitmap) <= 64  # XXX this is varying
    assert bytearray(null_bitmap)[0] == 0b00001011

    # Slicing does not affect the buffers but the offset
    a_sliced = a[1:]
    buffers = a_sliced.buffers()
    a_sliced.offset == 1
    assert len(buffers) == 2
    null_bitmap = buffers[0].to_pybytes()
    assert 1 <= len(null_bitmap) <= 64  # XXX this is varying
    assert bytearray(null_bitmap)[0] == 0b00001011

    assert struct.unpack('hhxxh', buffers[1].to_pybytes()) == (1, 2, 4)

    a = pa.array(np.int8([4, 5, 6]))
    buffers = a.buffers()
    assert len(buffers) == 2
    # No null bitmap from Numpy int array
    assert buffers[0] is None
    assert struct.unpack('3b', buffers[1].to_pybytes()) == (4, 5, 6)

    a = pa.array([b'foo!', None, b'bar!!'])
    buffers = a.buffers()
    assert len(buffers) == 3
    null_bitmap = buffers[0].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000101
    offsets = buffers[1].to_pybytes()
    assert struct.unpack('4i', offsets) == (0, 4, 4, 9)
    values = buffers[2].to_pybytes()
    assert values == b'foo!bar!!'


def test_buffers_nested():
    a = pa.array([[1, 2], None, [3, None, 4, 5]], type=pa.list_(pa.int64()))
    buffers = a.buffers()
    assert len(buffers) == 4
    # The parent buffers
    null_bitmap = buffers[0].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000101
    offsets = buffers[1].to_pybytes()
    assert struct.unpack('4i', offsets) == (0, 2, 2, 6)
    # The child buffers
    null_bitmap = buffers[2].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00110111
    values = buffers[3].to_pybytes()
    assert struct.unpack('qqq8xqq', values) == (1, 2, 3, 4, 5)

    a = pa.array([(42, None), None, (None, 43)],
                 type=pa.struct([pa.field('a', pa.int8()),
                                 pa.field('b', pa.int16())]))
    buffers = a.buffers()
    assert len(buffers) == 5
    # The parent buffer
    null_bitmap = buffers[0].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000101
    # The child buffers: 'a'
    null_bitmap = buffers[1].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000001
    values = buffers[2].to_pybytes()
    assert struct.unpack('bxx', values) == (42,)
    # The child buffers: 'b'
    null_bitmap = buffers[3].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000100
    values = buffers[4].to_pybytes()
    assert struct.unpack('4xh', values) == (43,)


def test_invalid_tensor_constructor_repr():
    t = pa.Tensor([1])
    assert repr(t) == '<invalid pyarrow.Tensor>'


def test_invalid_tensor_operation():
    t = pa.Tensor()
    with pytest.raises(TypeError):
        t.to_numpy()


def test_struct_array_flatten():
    ty = pa.struct([pa.field('x', pa.int16()),
                    pa.field('y', pa.float32())])
    a = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)
    xs, ys = a.flatten()
    assert xs.type == pa.int16()
    assert ys.type == pa.float32()
    assert xs.to_pylist() == [1, 3, 5]
    assert ys.to_pylist() == [2.5, 4.5, 6.5]
    xs, ys = a[1:].flatten()
    assert xs.to_pylist() == [3, 5]
    assert ys.to_pylist() == [4.5, 6.5]

    a = pa.array([(1, 2.5), None, (3, 4.5)], type=ty)
    xs, ys = a.flatten()
    assert xs.to_pylist() == [1, None, 3]
    assert ys.to_pylist() == [2.5, None, 4.5]
    xs, ys = a[1:].flatten()
    assert xs.to_pylist() == [None, 3]
    assert ys.to_pylist() == [None, 4.5]

    a = pa.array([(1, None), (2, 3.5), (None, 4.5)], type=ty)
    xs, ys = a.flatten()
    assert xs.to_pylist() == [1, 2, None]
    assert ys.to_pylist() == [None, 3.5, 4.5]
    xs, ys = a[1:].flatten()
    assert xs.to_pylist() == [2, None]
    assert ys.to_pylist() == [3.5, 4.5]

    a = pa.array([(1, None), None, (None, 2.5)], type=ty)
    xs, ys = a.flatten()
    assert xs.to_pylist() == [1, None, None]
    assert ys.to_pylist() == [None, None, 2.5]
    xs, ys = a[1:].flatten()
    assert xs.to_pylist() == [None, None]
    assert ys.to_pylist() == [None, 2.5]


def test_nested_dictionary_array():
    dict_arr = pa.DictionaryArray.from_arrays([0, 1, 0], ['a', 'b'])
    list_arr = pa.ListArray.from_arrays([0, 2, 3], dict_arr)
    assert list_arr.to_pylist() == [['a', 'b'], ['a']]

    dict_arr = pa.DictionaryArray.from_arrays([0, 1, 0], ['a', 'b'])
    dict_arr2 = pa.DictionaryArray.from_arrays([0, 1, 2, 1, 0], dict_arr)
    assert dict_arr2.to_pylist() == ['a', 'b', 'a', 'b', 'a']
