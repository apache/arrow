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

from collections.abc import Iterable
import datetime
import decimal
import hypothesis as h
import hypothesis.strategies as st
import itertools
import pytest
import struct
import subprocess
import sys
import weakref

try:
    import numpy as np
except ImportError:
    np = None

import pyarrow as pa
import pyarrow.tests.strategies as past
from pyarrow.vendored.version import Version


@pytest.mark.processes
def test_total_bytes_allocated():
    code = """if 1:
    import pyarrow as pa

    assert pa.total_allocated_bytes() == 0
    """
    res = subprocess.run([sys.executable, "-c", code],
                         universal_newlines=True, stderr=subprocess.PIPE)
    if res.returncode != 0:
        print(res.stderr, file=sys.stderr)
        res.check_returncode()  # fail
    assert len(res.stderr.splitlines()) == 0


def test_weakref():
    arr = pa.array([1, 2, 3])
    wr = weakref.ref(arr)
    assert wr() is not None
    del arr
    assert wr() is None


def test_getitem_NULL():
    arr = pa.array([1, None, 2])
    assert arr[1].as_py() is None
    assert arr[1].is_valid is False
    assert isinstance(arr[1], pa.Int64Scalar)


def test_constructor_raises():
    # This could happen by wrong capitalization.
    # ARROW-2638: prevent calling extension class constructors directly
    with pytest.raises(TypeError):
        pa.Array([1, 2])


def test_list_format():
    arr = pa.array([["foo"], None, ["bar", "a longer string", None]])
    result = arr.to_string()
    expected = """\
[
  [
    "foo"
  ],
  null,
  [
    "bar",
    "a longer string",
    null
  ]
]"""
    assert result == expected

    result = arr.to_string(element_size_limit=10)
    expected = """\
[
  [
    "foo"
  ],
  null,
  [
    "bar",
    "a longer (... 7 chars omitted)",
    null
  ]
]"""
    assert result == expected


def test_string_format():
    arr = pa.array(['', None, 'foo'])
    result = arr.to_string()
    expected = """\
[
  "",
  null,
  "foo"
]"""
    assert result == expected


def test_long_array_format():
    arr = pa.array(range(100))
    result = arr.to_string(window=2)
    expected = """\
[
  0,
  1,
  ...
  98,
  99
]"""
    assert result == expected


def test_indented_string_format():
    arr = pa.array(['', None, 'foo'])
    result = arr.to_string(indent=1)
    expected = '[\n "",\n null,\n "foo"\n]'

    assert result == expected


def test_top_level_indented_string_format():
    arr = pa.array(['', None, 'foo'])
    result = arr.to_string(top_level_indent=1)
    expected = ' [\n   "",\n   null,\n   "foo"\n ]'

    assert result == expected


def test_binary_format():
    arr = pa.array([b'\x00', b'', None, b'\x01foo', b'\x80\xff'])
    result = arr.to_string()
    expected = """\
[
  00,
  ,
  null,
  01666F6F,
  80FF
]"""
    assert result == expected


def test_binary_total_values_length():
    arr = pa.array([b'0000', None, b'11111', b'222222', b'3333333'],
                   type='binary')
    large_arr = pa.array([b'0000', None, b'11111', b'222222', b'3333333'],
                         type='large_binary')

    assert arr.total_values_length == 22
    assert arr.slice(1, 3).total_values_length == 11
    assert large_arr.total_values_length == 22
    assert large_arr.slice(1, 3).total_values_length == 11


@pytest.mark.pandas
def test_to_pandas_zero_copy():
    import gc

    arr = pa.array(range(10))

    for i in range(10):
        series = arr.to_pandas()
        # In Python 3.14 interpreter might avoid some
        # reference count modifications
        assert sys.getrefcount(series) in (1, 2)
        series = None  # noqa

    assert sys.getrefcount(arr) in (1, 2)

    for i in range(10):
        arr = pa.array(range(10))
        series = arr.to_pandas()
        arr = None
        gc.collect()

        # Ensure base is still valid

        # Because of py.test's assert inspection magic, if you put getrefcount
        # on the line being examined, it will be 1 higher than you expect
        base_refcount = sys.getrefcount(series.values.base)
        assert base_refcount == 2
        series.sum()


@pytest.mark.nopandas
@pytest.mark.pandas
def test_asarray():
    # ensure this is tested both when pandas is present or not (ARROW-6564)

    arr = pa.array(range(4))

    # The iterator interface gives back an array of Int64Value's
    np_arr = np.asarray([_ for _ in arr])
    assert np_arr.tolist() == [0, 1, 2, 3]
    assert np_arr.dtype == np.dtype('O')
    assert isinstance(np_arr[0], pa.lib.Int64Value)

    # Calling with the arrow array gives back an array with 'int64' dtype
    np_arr = np.asarray(arr)
    assert np_arr.tolist() == [0, 1, 2, 3]
    assert np_arr.dtype == np.dtype('int64')

    # An optional type can be specified when calling np.asarray
    np_arr = np.asarray(arr, dtype='str')
    assert np_arr.tolist() == ['0', '1', '2', '3']

    # If PyArrow array has null values, numpy type will be changed as needed
    # to support nulls.
    arr = pa.array([0, 1, 2, None])
    assert arr.type == pa.int64()
    np_arr = np.asarray(arr)
    elements = np_arr.tolist()
    assert elements[:3] == [0., 1., 2.]
    assert np.isnan(elements[3])
    assert np_arr.dtype == np.dtype('float64')

    # DictionaryType data will be converted to dense numpy array
    arr = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 0, 1]), pa.array(['a', 'b', 'c']))
    np_arr = np.asarray(arr)
    assert np_arr.dtype == np.dtype('object')
    assert np_arr.tolist() == ['a', 'b', 'c', 'a', 'b']


@pytest.mark.parametrize('ty', [
    None,
    pa.null(),
    pa.int8(),
    pa.string()
])
def test_nulls(ty):
    arr = pa.nulls(3, type=ty)
    expected = pa.array([None, None, None], type=ty)

    assert len(arr) == 3
    assert arr.equals(expected)

    if ty is None:
        assert arr.type == pa.null()
    else:
        assert arr.type == ty


def test_array_from_scalar():
    pytz = pytest.importorskip("pytz")

    today = datetime.date.today()
    now = datetime.datetime.now()
    now_utc = now.replace(tzinfo=pytz.utc)
    now_with_tz = now_utc.astimezone(pytz.timezone('US/Eastern'))
    oneday = datetime.timedelta(days=1)

    cases = [
        (None, 1, pa.array([None])),
        (None, 10, pa.nulls(10)),
        (-1, 3, pa.array([-1, -1, -1], type=pa.int64())),
        (2.71, 2, pa.array([2.71, 2.71], type=pa.float64())),
        ("string", 4, pa.array(["string"] * 4)),
        (
            pa.scalar(8, type=pa.uint8()),
            17,
            pa.array([8] * 17, type=pa.uint8())
        ),
        (pa.scalar(None), 3, pa.array([None, None, None])),
        (pa.scalar(True), 11, pa.array([True] * 11)),
        (today, 2, pa.array([today] * 2)),
        (now, 10, pa.array([now] * 10)),
        (
            now_with_tz,
            2,
            pa.array(
                [now_utc] * 2,
                type=pa.timestamp('us', tz=pytz.timezone('US/Eastern'))
            )
        ),
        (now.time(), 9, pa.array([now.time()] * 9)),
        (oneday, 4, pa.array([oneday] * 4)),
        (False, 9, pa.array([False] * 9)),
        ([1, 2], 2, pa.array([[1, 2], [1, 2]])),
        (
            pa.scalar([-1, 3], type=pa.large_list(pa.int8())),
            5,
            pa.array([[-1, 3]] * 5, type=pa.large_list(pa.int8()))
        ),
        ({'a': 1, 'b': 2}, 3, pa.array([{'a': 1, 'b': 2}] * 3))
    ]

    for value, size, expected in cases:
        arr = pa.repeat(value, size)
        assert len(arr) == size
        assert arr.type.equals(expected.type)
        assert arr.equals(expected)
        if expected.type == pa.null():
            assert arr.null_count == size
        else:
            assert arr.null_count == 0


def test_array_from_dictionary_scalar():
    dictionary = ['foo', 'bar', 'baz']
    arr = pa.DictionaryArray.from_arrays([2, 1, 2, 0], dictionary=dictionary)

    result = pa.repeat(arr[0], 5)
    expected = pa.DictionaryArray.from_arrays([2] * 5, dictionary=dictionary)
    assert result.equals(expected)

    result = pa.repeat(arr[3], 5)
    expected = pa.DictionaryArray.from_arrays([0] * 5, dictionary=dictionary)
    assert result.equals(expected)


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
    assert len(arr.slice(len(arr) + 2)) == 0
    assert len(arr.slice(len(arr) + 2, 100)) == 0

    with pytest.raises(IndexError):
        arr.slice(-1)

    with pytest.raises(ValueError):
        arr.slice(2, -1)

    # Test slice notation
    assert arr[2:].equals(arr.slice(2))
    assert arr[2:5].equals(arr.slice(2, 3))
    assert arr[-5:].equals(arr.slice(len(arr) - 5))

    n = len(arr)
    for start in range(-n * 2, n * 2):
        for stop in range(-n * 2, n * 2):
            res = arr[start:stop]
            res.validate()
            expected = arr.to_pylist()[start:stop]
            assert res.to_pylist() == expected
            if np is not None:
                assert res.to_numpy().tolist() == expected


def test_array_slice_negative_step():
    # ARROW-2714
    values = list(range(20))
    arr = pa.array(values)
    chunked_arr = pa.chunked_array([arr])

    cases = [
        slice(None, None, -1),  # GH-46606
        slice(None, 6, -2),
        slice(10, 6, -2),
        slice(8, None, -2),
        slice(2, 10, -2),
        slice(10, 2, -2),
        slice(None, None, 2),
        slice(0, 10, 2),
        slice(15, -25, -1),  # GH-38768
        slice(-22, -22, -1),  # GH-40642
    ]

    for case in cases:
        result = arr[case]
        expected = pa.array(values[case], type=arr.type)
        assert result.equals(expected)

        result = pa.record_batch([arr], names=['f0'])[case]
        expected = pa.record_batch([expected], names=['f0'])
        assert result.equals(expected)

        result = chunked_arr[case]
        expected = pa.chunked_array([values[case]], type=arr.type)
        assert result.equals(expected)


def test_arange():
    cases = [
        (5, 103),        # Default step
        (-2, 128, 3),
        (4, 103, 5),
        (10, -7, -1),
        (100, -20, -3),
        (0, 0),         # Empty array
        (2, 10, -1),    # Empty array
        (10, 3, 1),     # Empty array
    ]
    for case in cases:
        result = pa.arange(*case)
        result.validate(full=True)
        assert result.equals(pa.array(list(range(*case)), type=pa.int64()))

    # Validate memory_pool keyword argument
    result = pa.arange(-1, 101, memory_pool=pa.default_memory_pool())
    result.validate(full=True)
    assert result.equals(pa.array(list(range(-1, 101)), type=pa.int64()))

    # Special case for invalid step (arange does not accept step of 0)
    with pytest.raises(pa.ArrowInvalid):
        pa.arange(0, 10, 0)


def test_array_diff():
    # ARROW-6252
    arr1 = pa.array(['foo'], type=pa.utf8())
    arr2 = pa.array(['foo', 'bar', None], type=pa.utf8())
    arr3 = pa.array([1, 2, 3])
    arr4 = pa.array([[], [1], None], type=pa.list_(pa.int64()))
    arr5 = pa.array([1.5, 3, 6], type=pa.float16())
    arr6 = pa.array([1, 3], type=pa.float16())

    assert arr1.diff(arr1) == ''
    assert arr1.diff(arr2) == '''
@@ -1, +1 @@
+"bar"
+null
'''
    assert arr1.diff(arr3).strip() == '# Array types differed: string vs int64'
    assert arr1.diff(arr3).strip() == '# Array types differed: string vs int64'
    assert arr1.diff(arr4).strip() == ('# Array types differed: string vs '
                                       'list<item: int64>')
    assert arr5.diff(arr5) == ''
    assert arr5.diff(arr6) == '''
@@ -0, +0 @@
-1.5
+1
@@ -2, +2 @@
-6
'''


def test_array_iter():
    arr = pa.array(range(10))

    for i, j in zip(range(10), arr):
        assert i == j.as_py()

    assert isinstance(arr, Iterable)


def test_struct_array_slice():
    # ARROW-2311: slicing nested arrays needs special care
    ty = pa.struct([pa.field('a', pa.int8()),
                    pa.field('b', pa.float32())])
    arr = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)
    assert arr[1:].to_pylist() == [{'a': 3, 'b': 4.5},
                                   {'a': 5, 'b': 6.5}]


def test_array_eq():
    # ARROW-2150 / ARROW-9445: we define the __eq__ behavior to be
    # data equality (not element-wise equality)
    arr1 = pa.array([1, 2, 3], type=pa.int32())
    arr2 = pa.array([1, 2, 3], type=pa.int32())
    arr3 = pa.array([1, 2, 3], type=pa.int64())

    assert (arr1 == arr2) is True
    assert (arr1 != arr2) is False
    assert (arr1 == arr3) is False
    assert (arr1 != arr3) is True

    assert (arr1 == 1) is False
    assert (arr1 == None) is False  # noqa: E711


def test_string_binary_from_buffers():
    array = pa.array(["a", None, "b", "c"])

    buffers = array.buffers()
    copied = pa.StringArray.from_buffers(
        len(array), buffers[1], buffers[2], buffers[0], array.null_count,
        array.offset)
    assert copied.to_pylist() == ["a", None, "b", "c"]

    binary_copy = pa.Array.from_buffers(pa.binary(), len(array),
                                        array.buffers(), array.null_count,
                                        array.offset)
    assert binary_copy.to_pylist() == [b"a", None, b"b", b"c"]

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


def test_string_view_from_buffers():
    array = pa.array(
        [
            "String longer than 12 characters",
            None,
            "short",
            "Length is 12"
        ], type=pa.string_view())

    buffers = array.buffers()
    copied = pa.StringViewArray.from_buffers(
        pa.string_view(), len(array), buffers)
    copied.validate(full=True)
    assert copied.to_pylist() == [
        "String longer than 12 characters",
        None,
        "short",
        "Length is 12"
    ]

    match = r"number of buffers is at least 2"
    with pytest.raises(ValueError, match=match):
        pa.StringViewArray.from_buffers(
            pa.string_view(), len(array), buffers[0:1])


@pytest.mark.parametrize('list_type_factory', [
    pa.list_, pa.large_list, pa.list_view, pa.large_list_view])
def test_list_from_buffers(list_type_factory):
    ty = list_type_factory(pa.int16())
    array = pa.array([[0, 1, 2], None, [], [3, 4, 5]], type=ty)
    assert array.type == ty

    buffers = array.buffers()

    with pytest.raises(ValueError):
        # No children
        pa.Array.from_buffers(ty, 4, buffers[:ty.num_buffers])

    child = pa.Array.from_buffers(pa.int16(), 6, buffers[ty.num_buffers:])
    copied = pa.Array.from_buffers(ty, 4, buffers[:ty.num_buffers], children=[child])
    assert copied.equals(array)

    with pytest.raises(ValueError):
        # too many children
        pa.Array.from_buffers(ty, 4, buffers[:ty.num_buffers],
                              children=[child, child])


def test_struct_from_buffers():
    ty = pa.struct([pa.field('a', pa.int16()), pa.field('b', pa.utf8())])
    array = pa.array([{'a': 0, 'b': 'foo'}, None, {'a': 5, 'b': ''}],
                     type=ty)
    buffers = array.buffers()

    with pytest.raises(ValueError):
        # No children
        pa.Array.from_buffers(ty, 3, [None, buffers[1]])

    children = [pa.Array.from_buffers(pa.int16(), 3, buffers[1:3]),
                pa.Array.from_buffers(pa.utf8(), 3, buffers[3:])]
    copied = pa.Array.from_buffers(ty, 3, buffers[:1], children=children)
    assert copied.equals(array)

    with pytest.raises(ValueError):
        # not enough many children
        pa.Array.from_buffers(ty, 3, [buffers[0]],
                              children=children[:1])


def test_struct_from_arrays():
    a = pa.array([4, 5, 6], type=pa.int64())
    b = pa.array(["bar", None, ""])
    c = pa.array([[1, 2], None, [3, None]])
    expected_list = [
        {'a': 4, 'b': 'bar', 'c': [1, 2]},
        {'a': 5, 'b': None, 'c': None},
        {'a': 6, 'b': '', 'c': [3, None]},
    ]

    # From field names
    arr = pa.StructArray.from_arrays([a, b, c], ["a", "b", "c"])
    assert arr.type == pa.struct(
        [("a", a.type), ("b", b.type), ("c", c.type)])
    assert arr.to_pylist() == expected_list

    with pytest.raises(ValueError):
        pa.StructArray.from_arrays([a, b, c], ["a", "b"])

    arr = pa.StructArray.from_arrays([], [])
    assert arr.type == pa.struct([])
    assert arr.to_pylist() == []

    # From fields
    fa = pa.field("a", a.type, nullable=False)
    fb = pa.field("b", b.type)
    fc = pa.field("c", c.type)
    arr = pa.StructArray.from_arrays([a, b, c], fields=[fa, fb, fc])
    assert arr.type == pa.struct([fa, fb, fc])
    assert not arr.type[0].nullable
    assert arr.to_pylist() == expected_list

    # From structtype
    structtype = pa.struct([fa, fb, fc])
    arr = pa.StructArray.from_arrays([a, b, c], type=structtype)
    assert arr.type == pa.struct([fa, fb, fc])
    assert not arr.type[0].nullable
    assert arr.to_pylist() == expected_list

    with pytest.raises(ValueError):
        pa.StructArray.from_arrays([a, b, c], fields=[fa, fb])

    arr = pa.StructArray.from_arrays([], fields=[])
    assert arr.type == pa.struct([])
    assert arr.to_pylist() == []

    # Inconsistent fields
    fa2 = pa.field("a", pa.int32())
    with pytest.raises(ValueError, match="int64 vs int32"):
        pa.StructArray.from_arrays([a, b, c], fields=[fa2, fb, fc])

    arrays = [a, b, c]
    fields = [fa, fb, fc]
    # With mask
    mask = pa.array([True, False, False])
    arr = pa.StructArray.from_arrays(arrays, fields=fields, mask=mask)
    assert arr.to_pylist() == [None] + expected_list[1:]

    arr = pa.StructArray.from_arrays(arrays, names=['a', 'b', 'c'], mask=mask)
    assert arr.to_pylist() == [None] + expected_list[1:]

    # Bad masks
    with pytest.raises(TypeError, match='Mask must be'):
        pa.StructArray.from_arrays(arrays, fields, mask=[True, False, False])

    with pytest.raises(ValueError, match='not contain nulls'):
        pa.StructArray.from_arrays(
            arrays, fields, mask=pa.array([True, False, None]))

    with pytest.raises(TypeError, match='Mask must be'):
        pa.StructArray.from_arrays(
            arrays, fields, mask=pa.chunked_array([mask]))

    # Non-empty array with no fields https://github.com/apache/arrow/issues/15109
    arr = pa.StructArray.from_arrays([], [], mask=mask)
    assert arr.is_null() == mask
    assert arr.to_pylist() == [None, {}, {}]


def test_struct_array_from_chunked():
    # ARROW-11780
    # Check that we don't segfault when trying to build
    # a StructArray from a chunked array.
    chunked_arr = pa.chunked_array([[1, 2, 3], [4, 5, 6]])

    with pytest.raises(TypeError, match="Expected Array"):
        pa.StructArray.from_arrays([chunked_arr], ["foo"])


@pytest.mark.parametrize("offset", (0, 1))
def test_dictionary_from_buffers(offset):
    a = pa.array(["one", "two", "three", "two", "one"]).dictionary_encode()
    b = pa.DictionaryArray.from_buffers(a.type, len(a)-offset,
                                        a.indices.buffers(), a.dictionary,
                                        offset=offset)
    assert a[offset:] == b


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


def test_dictionary_indices():
    # https://issues.apache.org/jira/browse/ARROW-6882
    indices = pa.array([0, 1, 2, 0, 1, 2])
    dictionary = pa.array(['foo', 'bar', 'baz'])
    arr = pa.DictionaryArray.from_arrays(indices, dictionary)
    arr.indices.validate(full=True)


@pytest.mark.parametrize(('list_array_type', 'list_type_factory'), (
    (pa.ListArray, pa.list_),
    (pa.LargeListArray, pa.large_list)
))
@pytest.mark.parametrize("arr", (
    [None, [0]],
    [None, [0, None], [0]],
    [[0], [1]],
))
def test_list_array_types_from_arrays(
    list_array_type, list_type_factory, arr
):
    arr = pa.array(arr, list_type_factory(pa.int8()))
    reconstructed_arr = list_array_type.from_arrays(
        arr.offsets, arr.values, mask=arr.is_null())
    assert arr == reconstructed_arr


@pytest.mark.parametrize(('list_array_type', 'list_type_factory'), (
    (pa.ListArray, pa.list_),
    (pa.LargeListArray, pa.large_list)
))
def test_list_array_types_from_arrays_fail(list_array_type, list_type_factory):
    # Fail when manual offsets include nulls and mask passed
    # ListArray.offsets doesn't report nulls.

    # This test case arr.offsets == [0, 1, 1, 3, 4]
    arr = pa.array([[0], None, [0, None], [0]], list_type_factory(pa.int8()))
    offsets = pa.array([0, None, 1, 3, 4])

    # Using array's offset has no nulls; gives empty lists on top level
    reconstructed_arr = list_array_type.from_arrays(arr.offsets, arr.values)
    assert reconstructed_arr.to_pylist() == [[0], [], [0, None], [0]]

    # Manually specifying offsets (with nulls) is same as mask at top level
    reconstructed_arr = list_array_type.from_arrays(offsets, arr.values)
    assert arr == reconstructed_arr
    reconstructed_arr = list_array_type.from_arrays(arr.offsets,
                                                    arr.values,
                                                    mask=arr.is_null())
    assert arr == reconstructed_arr

    # But using both is ambiguous, in this case `offsets` has nulls
    with pytest.raises(ValueError, match="Ambiguous to specify both "):
        list_array_type.from_arrays(offsets, arr.values, mask=arr.is_null())

    # Not supported to reconstruct from a slice.
    arr_slice = arr[1:]
    msg = "Null bitmap with offsets slice not supported."
    with pytest.raises(NotImplementedError, match=msg):
        list_array_type.from_arrays(
            arr_slice.offsets, arr_slice.values, mask=arr_slice.is_null())


def test_map_cast():
    # GH-38553
    t = pa.map_(pa.int64(), pa.int64())
    arr = pa.array([{1: 2}], type=t)
    result = arr.cast(pa.map_(pa.int32(), pa.int64()))

    t_expected = pa.map_(pa.int32(), pa.int64())
    expected = pa.array([{1: 2}], type=t_expected)

    assert result.equals(expected)


def test_map_labelled():
    #  ARROW-13735
    t = pa.map_(pa.field("name", "string", nullable=False), "int64")
    arr = pa.array([[('a', 1), ('b', 2)], [('c', 3)]], type=t)
    assert arr.type.key_field == pa.field("name", pa.utf8(), nullable=False)
    assert arr.type.item_field == pa.field("value", pa.int64())
    assert len(arr) == 2


def test_map_from_dict():
    # ARROW-17832
    tup_arr = pa.array([[('a', 1), ('b', 2)], [('c', 3)]],
                       pa.map_(pa.string(), pa.int64()))
    dict_arr = pa.array([{'a': 1, 'b': 2}, {'c': 3}],
                        pa.map_(pa.string(), pa.int64()))

    assert tup_arr.equals(dict_arr)


def test_fixed_size_list_from_arrays():
    values = pa.array(range(12), pa.int64())
    result = pa.FixedSizeListArray.from_arrays(values, 4)
    assert result.to_pylist() == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]
    assert result.type.equals(pa.list_(pa.int64(), 4))

    typ = pa.list_(pa.field("name", pa.int64()), 4)
    result = pa.FixedSizeListArray.from_arrays(values, type=typ)
    assert result.to_pylist() == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]
    assert result.type.equals(typ)
    assert result.type.value_field.name == "name"

    result = pa.FixedSizeListArray.from_arrays(values,
                                               type=typ,
                                               mask=pa.array([False, True, False]))
    assert result.to_pylist() == [[0, 1, 2, 3], None, [8, 9, 10, 11]]

    result = pa.FixedSizeListArray.from_arrays(values,
                                               list_size=4,
                                               mask=pa.array([False, True, False]))
    assert result.to_pylist() == [[0, 1, 2, 3], None, [8, 9, 10, 11]]

    # raise on invalid values / list_size
    with pytest.raises(ValueError):
        pa.FixedSizeListArray.from_arrays(values, -4)

    with pytest.raises(ValueError):
        # array with list size 0 cannot be constructed with from_arrays
        pa.FixedSizeListArray.from_arrays(pa.array([], pa.int64()), 0)

    with pytest.raises(ValueError):
        # length of values not multiple of 5
        pa.FixedSizeListArray.from_arrays(values, 5)

    typ = pa.list_(pa.int64(), 5)
    with pytest.raises(ValueError):
        pa.FixedSizeListArray.from_arrays(values, type=typ)

    # raise on mismatching values type
    typ = pa.list_(pa.float64(), 4)
    with pytest.raises(TypeError):
        pa.FixedSizeListArray.from_arrays(values, type=typ)

    # raise on specifying none or both of list_size / type
    with pytest.raises(ValueError):
        pa.FixedSizeListArray.from_arrays(values)

    typ = pa.list_(pa.int64(), 4)
    with pytest.raises(ValueError):
        pa.FixedSizeListArray.from_arrays(values, list_size=4, type=typ)


def test_variable_list_from_arrays():
    values = pa.array([1, 2, 3, 4], pa.int64())
    offsets = pa.array([0, 2, 4])
    result = pa.ListArray.from_arrays(offsets, values)
    assert result.to_pylist() == [[1, 2], [3, 4]]
    assert result.type.equals(pa.list_(pa.int64()))

    offsets = pa.array([0, None, 2, 4])
    result = pa.ListArray.from_arrays(offsets, values)
    assert result.to_pylist() == [[1, 2], None, [3, 4]]

    # raise if offset out of bounds
    with pytest.raises(ValueError):
        pa.ListArray.from_arrays(pa.array([-1, 2, 4]), values)

    with pytest.raises(ValueError):
        pa.ListArray.from_arrays(pa.array([0, 2, 5]), values)


def test_union_from_dense():
    binary = pa.array([b'a', b'b', b'c', b'd'], type='binary')
    int64 = pa.array([1, 2, 3], type='int64')
    types = pa.array([0, 1, 0, 0, 1, 1, 0], type='int8')
    logical_types = pa.array([11, 13, 11, 11, 13, 13, 11], type='int8')
    value_offsets = pa.array([0, 0, 1, 2, 1, 2, 3], type='int32')
    py_value = [b'a', 1, b'b', b'c', 2, 3, b'd']

    def check_result(result, expected_field_names, expected_type_codes,
                     expected_type_code_values):
        result.validate(full=True)
        actual_field_names = [result.type[i].name
                              for i in range(result.type.num_fields)]
        assert actual_field_names == expected_field_names
        assert result.type.mode == "dense"
        assert result.type.type_codes == expected_type_codes
        assert result.to_pylist() == py_value
        assert expected_type_code_values.equals(result.type_codes)
        assert value_offsets.equals(result.offsets)
        assert result.field(0).equals(binary)
        assert result.field(1).equals(int64)
        with pytest.raises(KeyError):
            result.field(-1)
        with pytest.raises(KeyError):
            result.field(2)

    # without field names and type codes
    check_result(pa.UnionArray.from_dense(types, value_offsets,
                                          [binary, int64]),
                 expected_field_names=['0', '1'],
                 expected_type_codes=[0, 1],
                 expected_type_code_values=types)

    # with field names
    check_result(pa.UnionArray.from_dense(types, value_offsets,
                                          [binary, int64],
                                          ['bin', 'int']),
                 expected_field_names=['bin', 'int'],
                 expected_type_codes=[0, 1],
                 expected_type_code_values=types)

    # with type codes
    check_result(pa.UnionArray.from_dense(logical_types, value_offsets,
                                          [binary, int64],
                                          type_codes=[11, 13]),
                 expected_field_names=['0', '1'],
                 expected_type_codes=[11, 13],
                 expected_type_code_values=logical_types)

    # with field names and type codes
    check_result(pa.UnionArray.from_dense(logical_types, value_offsets,
                                          [binary, int64],
                                          ['bin', 'int'], [11, 13]),
                 expected_field_names=['bin', 'int'],
                 expected_type_codes=[11, 13],
                 expected_type_code_values=logical_types)

    # Bad type ids
    arr = pa.UnionArray.from_dense(logical_types, value_offsets,
                                   [binary, int64])
    with pytest.raises(pa.ArrowInvalid):
        arr.validate(full=True)
    arr = pa.UnionArray.from_dense(types, value_offsets, [binary, int64],
                                   type_codes=[11, 13])
    with pytest.raises(pa.ArrowInvalid):
        arr.validate(full=True)

    # Offset larger than child size
    bad_offsets = pa.array([0, 0, 1, 2, 1, 2, 4], type='int32')
    arr = pa.UnionArray.from_dense(types, bad_offsets, [binary, int64])
    with pytest.raises(pa.ArrowInvalid):
        arr.validate(full=True)


def test_union_from_sparse():
    binary = pa.array([b'a', b' ', b'b', b'c', b' ', b' ', b'd'],
                      type='binary')
    int64 = pa.array([0, 1, 0, 0, 2, 3, 0], type='int64')
    types = pa.array([0, 1, 0, 0, 1, 1, 0], type='int8')
    logical_types = pa.array([11, 13, 11, 11, 13, 13, 11], type='int8')
    py_value = [b'a', 1, b'b', b'c', 2, 3, b'd']

    def check_result(result, expected_field_names, expected_type_codes,
                     expected_type_code_values):
        result.validate(full=True)
        assert result.to_pylist() == py_value
        actual_field_names = [result.type[i].name
                              for i in range(result.type.num_fields)]
        assert actual_field_names == expected_field_names
        assert result.type.mode == "sparse"
        assert result.type.type_codes == expected_type_codes
        assert expected_type_code_values.equals(result.type_codes)
        assert result.field(0).equals(binary)
        assert result.field(1).equals(int64)
        with pytest.raises(pa.ArrowTypeError):
            result.offsets
        with pytest.raises(KeyError):
            result.field(-1)
        with pytest.raises(KeyError):
            result.field(2)

    # without field names and type codes
    check_result(pa.UnionArray.from_sparse(types, [binary, int64]),
                 expected_field_names=['0', '1'],
                 expected_type_codes=[0, 1],
                 expected_type_code_values=types)

    # with field names
    check_result(pa.UnionArray.from_sparse(types, [binary, int64],
                                           ['bin', 'int']),
                 expected_field_names=['bin', 'int'],
                 expected_type_codes=[0, 1],
                 expected_type_code_values=types)

    # with type codes
    check_result(pa.UnionArray.from_sparse(logical_types, [binary, int64],
                                           type_codes=[11, 13]),
                 expected_field_names=['0', '1'],
                 expected_type_codes=[11, 13],
                 expected_type_code_values=logical_types)

    # with field names and type codes
    check_result(pa.UnionArray.from_sparse(logical_types, [binary, int64],
                                           ['bin', 'int'],
                                           [11, 13]),
                 expected_field_names=['bin', 'int'],
                 expected_type_codes=[11, 13],
                 expected_type_code_values=logical_types)

    # Bad type ids
    arr = pa.UnionArray.from_sparse(logical_types, [binary, int64])
    with pytest.raises(pa.ArrowInvalid):
        arr.validate(full=True)
    arr = pa.UnionArray.from_sparse(types, [binary, int64],
                                    type_codes=[11, 13])
    with pytest.raises(pa.ArrowInvalid):
        arr.validate(full=True)

    # Invalid child length
    with pytest.raises(pa.ArrowInvalid):
        arr = pa.UnionArray.from_sparse(logical_types, [binary, int64[1:]])


def test_union_array_to_pylist_with_nulls():
    # ARROW-9556
    arr = pa.UnionArray.from_sparse(
        pa.array([0, 1, 0, 0, 1], type=pa.int8()),
        [
            pa.array([0.0, 1.1, None, 3.3, 4.4]),
            pa.array([True, None, False, True, False]),
        ]
    )
    assert arr.to_pylist() == [0.0, None, None, 3.3, False]

    arr = pa.UnionArray.from_dense(
        pa.array([0, 1, 0, 0, 0, 1, 1], type=pa.int8()),
        pa.array([0, 0, 1, 2, 3, 1, 2], type=pa.int32()),
        [
            pa.array([0.0, 1.1, None, 3.3]),
            pa.array([True, None, False])
        ]
    )
    assert arr.to_pylist() == [0.0, True, 1.1, None, 3.3, None, False]


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


def _check_cast_case(case, *, safe=True, check_array_construction=True):
    in_data, in_type, out_data, out_type = case
    if isinstance(out_data, pa.Array):
        assert out_data.type == out_type
        expected = out_data
    else:
        expected = pa.array(out_data, type=out_type)

    # check casting an already created array
    if isinstance(in_data, pa.Array):
        assert in_data.type == in_type
        in_arr = in_data
    else:
        in_arr = pa.array(in_data, type=in_type)
    casted = in_arr.cast(out_type, safe=safe)
    casted.validate(full=True)
    assert casted.equals(expected)

    # constructing an array with out type which optionally involves casting
    # for more see ARROW-1949
    if check_array_construction:
        in_arr = pa.array(in_data, type=out_type, safe=safe)
        assert in_arr.equals(expected)


def test_cast_none():
    # ARROW-3735: Ensure that calling cast(None) doesn't segfault.
    arr = pa.array([1, 2, 3])

    with pytest.raises(TypeError):
        arr.cast(None)


def test_cast_list_to_primitive():
    # ARROW-8070: cast segfaults on unsupported cast from list<binary> to utf8
    arr = pa.array([[1, 2], [3, 4]])
    with pytest.raises(NotImplementedError):
        arr.cast(pa.int8())

    arr = pa.array([[b"a", b"b"], [b"c"]], pa.list_(pa.binary()))
    with pytest.raises(NotImplementedError):
        arr.cast(pa.binary())


def test_slice_chunked_array_zero_chunks():
    # ARROW-8911
    arr = pa.chunked_array([], type='int8')
    assert arr.num_chunks == 0

    result = arr[:]
    assert result.equals(arr)

    # Do not crash
    arr[:5]


def test_cast_chunked_array():
    arrays = [pa.array([1, 2, 3]), pa.array([4, 5, 6])]
    carr = pa.chunked_array(arrays)

    target = pa.float64()
    casted = carr.cast(target)
    expected = pa.chunked_array([x.cast(target) for x in arrays])
    assert casted.equals(expected)


def test_cast_chunked_array_empty():
    # ARROW-8142
    for typ1, typ2 in [(pa.dictionary(pa.int8(), pa.string()), pa.string()),
                       (pa.int64(), pa.int32())]:

        arr = pa.chunked_array([], type=typ1)
        result = arr.cast(typ2)
        expected = pa.chunked_array([], type=typ2)
        assert result.equals(expected)


def test_chunked_array_data_warns():
    with pytest.warns(FutureWarning):
        res = pa.chunked_array([[]]).data
    assert isinstance(res, pa.ChunkedArray)


def test_half_float_array_from_python():
    # GH-46611
    vals = [-5, 0, 1.0, 2.0, 3, None, 12345.6789, 1.234567, float('inf')]
    arr = pa.array(vals, type=pa.float16())
    assert arr.type == pa.float16()
    assert arr.to_pylist() == [-5, 0, 1.0, 2.0, 3, None, 12344.0,
                               1.234375, float('inf')]
    assert str(arr) == ("[\n  -5,\n  0,\n  1,\n  2,\n  3,\n  null,\n  12344,"
                        "\n  1.234375,\n  inf\n]")
    msg1 = "Could not convert 'a' with type str: tried to convert to float16"
    with pytest.raises(pa.ArrowInvalid, match=msg1):
        arr = pa.array(['a', 3, None], type=pa.float16())


def test_decimal_to_int_safe():
    safe_cases = [
        (
            [decimal.Decimal("123456"), None, decimal.Decimal("-912345")],
            pa.decimal128(32, 5),
            [123456, None, -912345],
            pa.int32()
        ),
        (
            [decimal.Decimal("1234"), None, decimal.Decimal("-9123")],
            pa.decimal128(19, 10),
            [1234, None, -9123],
            pa.int16()
        ),
        (
            [decimal.Decimal("123"), None, decimal.Decimal("-91")],
            pa.decimal128(19, 10),
            [123, None, -91],
            pa.int8()
        ),
    ]
    for case in safe_cases:
        _check_cast_case(case)
        _check_cast_case(case, safe=True)


def test_decimal_to_int_non_integer():
    non_integer_cases = [
        (
            [
                decimal.Decimal("123456.21"),
                None,
                decimal.Decimal("-912345.13")
            ],
            pa.decimal128(32, 5),
            [123456, None, -912345],
            pa.int32()
        ),
        (
            [decimal.Decimal("1234.134"), None, decimal.Decimal("-9123.1")],
            pa.decimal128(19, 10),
            [1234, None, -9123],
            pa.int16()
        ),
        (
            [decimal.Decimal("123.1451"), None, decimal.Decimal("-91.21")],
            pa.decimal128(19, 10),
            [123, None, -91],
            pa.int8()
        ),
    ]

    for case in non_integer_cases:
        # test safe casting raises
        msg_regexp = 'Rescaling Decimal value would cause data loss'
        with pytest.raises(pa.ArrowInvalid, match=msg_regexp):
            _check_cast_case(case)

        _check_cast_case(case, safe=False)


def test_decimal_to_decimal():
    arr = pa.array(
        [decimal.Decimal("1234.12"), None],
        type=pa.decimal128(19, 10)
    )
    result = arr.cast(pa.decimal128(15, 6))
    expected = pa.array(
        [decimal.Decimal("1234.12"), None],
        type=pa.decimal128(15, 6)
    )
    assert result.equals(expected)

    msg_regexp = 'Rescaling Decimal value would cause data loss'
    with pytest.raises(pa.ArrowInvalid, match=msg_regexp):
        result = arr.cast(pa.decimal128(9, 1))

    result = arr.cast(pa.decimal128(9, 1), safe=False)
    expected = pa.array(
        [decimal.Decimal("1234.1"), None],
        type=pa.decimal128(9, 1)
    )
    assert result.equals(expected)

    with pytest.raises(pa.ArrowInvalid,
                       match='Decimal value does not fit in precision'):
        result = arr.cast(pa.decimal128(5, 2))


def test_cast_from_null():
    in_data = [None] * 3
    in_type = pa.null()
    out_types = [
        pa.null(),
        pa.uint8(),
        pa.float16(),
        pa.utf8(),
        pa.binary(),
        pa.binary(10),
        pa.list_(pa.int16()),
        pa.list_(pa.int32(), 4),
        pa.large_list(pa.uint8()),
        pa.decimal128(19, 4),
        pa.timestamp('us'),
        pa.timestamp('us', tz='UTC'),
        pa.timestamp('us', tz='Europe/Paris'),
        pa.duration('us'),
        pa.month_day_nano_interval(),
        pa.struct([pa.field('a', pa.int32()),
                   pa.field('b', pa.list_(pa.int8())),
                   pa.field('c', pa.string())]),
        pa.dictionary(pa.int32(), pa.string()),
    ]
    for out_type in out_types:
        _check_cast_case((in_data, in_type, in_data, out_type))

    out_types = [

        pa.union([pa.field('a', pa.binary(10)),
                  pa.field('b', pa.string())], mode=pa.lib.UnionMode_DENSE),
        pa.union([pa.field('a', pa.binary(10)),
                  pa.field('b', pa.string())], mode=pa.lib.UnionMode_SPARSE),
    ]
    in_arr = pa.array(in_data, type=pa.null())
    for out_type in out_types:
        with pytest.raises(NotImplementedError):
            in_arr.cast(out_type)


def test_cast_string_to_number_roundtrip():
    cases = [
        (pa.array(["1", "127", "-128"]),
         pa.array([1, 127, -128], type=pa.int8())),
        (pa.array([None, "18446744073709551615"]),
         pa.array([None, 18446744073709551615], type=pa.uint64())),
    ]
    for in_arr, expected in cases:
        casted = in_arr.cast(expected.type, safe=True)
        casted.validate(full=True)
        assert casted.equals(expected)
        casted_back = casted.cast(in_arr.type, safe=True)
        casted_back.validate(full=True)
        assert casted_back.equals(in_arr)


def test_cast_dictionary():
    # cast to the value type
    arr = pa.array(
        ["foo", "bar", None],
        type=pa.dictionary(pa.int64(), pa.string())
    )
    expected = pa.array(["foo", "bar", None])
    assert arr.type == pa.dictionary(pa.int64(), pa.string())
    assert arr.cast(pa.string()) == expected

    # cast to a different key type
    for key_type in [pa.int8(), pa.int16(), pa.int32()]:
        typ = pa.dictionary(key_type, pa.string())
        expected = pa.array(
            ["foo", "bar", None],
            type=pa.dictionary(key_type, pa.string())
        )
        assert arr.cast(typ) == expected

    # shouldn't crash (ARROW-7077)
    with pytest.raises(pa.ArrowInvalid):
        arr.cast(pa.int32())


def test_view():
    # ARROW-5992
    arr = pa.array(['foo', 'bar', 'baz'], type=pa.utf8())
    expected = pa.array(['foo', 'bar', 'baz'], type=pa.binary())

    assert arr.view(pa.binary()).equals(expected)
    assert arr.view('binary').equals(expected)


def test_unique_simple():
    cases = [
        (pa.array([1, 2, 3, 1, 2, 3]), pa.array([1, 2, 3])),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.array(['foo', None, 'bar'])),
        (pa.array(['foo', None, 'bar', 'foo'], pa.large_binary()),
         pa.array(['foo', None, 'bar'], pa.large_binary())),
    ]
    for arr, expected in cases:
        result = arr.unique()
        assert result.equals(expected)
        result = pa.chunked_array([arr]).unique()
        assert result.equals(expected)


def test_value_counts_simple():
    cases = [
        (pa.array([1, 2, 3, 1, 2, 3]),
         pa.array([1, 2, 3]),
         pa.array([2, 2, 2], type=pa.int64())),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.array(['foo', None, 'bar']),
         pa.array([2, 1, 1], type=pa.int64())),
        (pa.array(['foo', None, 'bar', 'foo'], pa.large_binary()),
         pa.array(['foo', None, 'bar'], pa.large_binary()),
         pa.array([2, 1, 1], type=pa.int64())),
    ]
    for arr, expected_values, expected_counts in cases:
        for arr_in in (arr, pa.chunked_array([arr])):
            result = arr_in.value_counts()
            assert result.type.equals(
                pa.struct([pa.field("values", arr.type),
                           pa.field("counts", pa.int64())]))
            assert result.field("values").equals(expected_values)
            assert result.field("counts").equals(expected_counts)


def test_unique_value_counts_dictionary_type():
    indices = pa.array([3, 0, 0, 0, 1, 1, 3, 0, 1, 3, 0, 1])
    dictionary = pa.array(['foo', 'bar', 'baz', 'qux'])

    arr = pa.DictionaryArray.from_arrays(indices, dictionary)

    unique_result = arr.unique()
    expected = pa.DictionaryArray.from_arrays(indices.unique(), dictionary)
    assert unique_result.equals(expected)

    result = arr.value_counts()
    assert result.field('values').equals(unique_result)
    assert result.field('counts').equals(pa.array([3, 5, 4], type='int64'))

    arr = pa.DictionaryArray.from_arrays(
        pa.array([], type='int64'), dictionary)
    unique_result = arr.unique()
    expected = pa.DictionaryArray.from_arrays(pa.array([], type='int64'),
                                              pa.array([], type='utf8'))
    assert unique_result.equals(expected)

    result = arr.value_counts()
    assert result.field('values').equals(unique_result)
    assert result.field('counts').equals(pa.array([], type='int64'))


def test_dictionary_encode_simple():
    cases = [
        (pa.array([1, 2, 3, None, 1, 2, 3]),
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, 2, None, 0, 1, 2], type='int32'),
             [1, 2, 3])),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.DictionaryArray.from_arrays(
             pa.array([0, None, 1, 0], type='int32'),
             ['foo', 'bar'])),
        (pa.array(['foo', None, 'bar', 'foo'], type=pa.large_binary()),
         pa.DictionaryArray.from_arrays(
             pa.array([0, None, 1, 0], type='int32'),
             pa.array(['foo', 'bar'], type=pa.large_binary()))),
    ]
    for arr, expected in cases:
        result = arr.dictionary_encode()
        assert result.equals(expected)
        result = pa.chunked_array([arr]).dictionary_encode()
        assert result.num_chunks == 1
        assert result.chunk(0).equals(expected)
        result = pa.chunked_array([], type=arr.type).dictionary_encode()
        assert result.num_chunks == 0
        assert result.type == expected.type


def test_dictionary_encode_sliced():
    cases = [
        (pa.array([1, 2, 3, None, 1, 2, 3])[1:-1],
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, None, 2, 0], type='int32'),
             [2, 3, 1])),
        (pa.array([None, 'foo', 'bar', 'foo', 'xyzzy'])[1:-1],
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, 0], type='int32'),
             ['foo', 'bar'])),
        (pa.array([None, 'foo', 'bar', 'foo', 'xyzzy'],
                  type=pa.large_string())[1:-1],
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, 0], type='int32'),
             pa.array(['foo', 'bar'], type=pa.large_string()))),
    ]
    for arr, expected in cases:
        result = arr.dictionary_encode()
        assert result.equals(expected)
        result = pa.chunked_array([arr]).dictionary_encode()
        assert result.num_chunks == 1
        assert result.type == expected.type
        assert result.chunk(0).equals(expected)
        result = pa.chunked_array([], type=arr.type).dictionary_encode()
        assert result.num_chunks == 0
        assert result.type == expected.type

    # ARROW-9143 dictionary_encode after slice was segfaulting
    array = pa.array(['foo', 'bar', 'baz'])
    array.slice(1).dictionary_encode()


def test_dictionary_encode_zero_length():
    # User-facing experience of ARROW-7008
    arr = pa.array([], type=pa.string())
    encoded = arr.dictionary_encode()
    assert len(encoded.dictionary) == 0
    encoded.validate(full=True)


def test_dictionary_decode():
    cases = [
        (pa.array([1, 2, 3, None, 1, 2, 3]),
         pa.DictionaryArray.from_arrays(
             pa.array([0, 1, 2, None, 0, 1, 2], type='int32'),
             [1, 2, 3])),
        (pa.array(['foo', None, 'bar', 'foo']),
         pa.DictionaryArray.from_arrays(
             pa.array([0, None, 1, 0], type='int32'),
             ['foo', 'bar'])),
        (pa.array(['foo', None, 'bar', 'foo'], type=pa.large_binary()),
         pa.DictionaryArray.from_arrays(
             pa.array([0, None, 1, 0], type='int32'),
             pa.array(['foo', 'bar'], type=pa.large_binary()))),
    ]
    for expected, arr in cases:
        result = arr.dictionary_decode()
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


def test_date64_from_builtin_datetime():
    val1 = datetime.datetime(2000, 1, 1, 12, 34, 56, 123456)
    val2 = datetime.datetime(2000, 1, 1)
    result = pa.array([val1, val2], type='date64')
    result2 = pa.array([val1.date(), val2.date()], type='date64')

    assert result.equals(result2)

    as_i8 = result.view('int64')
    assert as_i8[0].as_py() == as_i8[1].as_py()


def test_create_date32_and_date64_arrays_with_mask():
    # Test Date32 array creation from Python list with mask
    arr_date32 = pa.array([0, 0, 1, 2],
                          mask=[False, False, True, False],
                          type=pa.date32())
    expected_date32 = pa.array([
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 1),
        None,
        datetime.date(1970, 1, 3),
    ], type=pa.date32())
    assert arr_date32.equals(expected_date32)

    # Test Date32 array creation from Python dates
    arr_date32_dates = pa.array([
        datetime.date(2023, 1, 1),
        datetime.date(2023, 1, 2),
        None,
        datetime.date(2023, 1, 4),
    ], type=pa.date32())
    assert arr_date32_dates.null_count == 1
    assert arr_date32_dates[2].as_py() is None

    # Test Date64 array creation from Python list with mask
    arr_date64 = pa.array([0, 86400000, 172800000, 259200000],
                          mask=[False, False, True, False],
                          type=pa.date64())
    expected_date64 = pa.array([
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 2),
        None,
        datetime.date(1970, 1, 4),
    ], type=pa.date64())
    assert arr_date64.equals(expected_date64)

    # Test Date64 array creation from Python dates
    arr_date64_dates = pa.array([
        datetime.date(2023, 1, 1),
        datetime.date(2023, 1, 2),
        None,
        datetime.date(2023, 1, 4),
    ], type=pa.date64())
    assert arr_date64_dates.null_count == 1
    assert arr_date64_dates[2].as_py() is None

    # Test Date32 with all nulls mask
    arr_all_null = pa.array([0, 1, 2, 3],
                            mask=[True, True, True, True],
                            type=pa.date32())
    assert arr_all_null.null_count == 4


@pytest.mark.parametrize(('ty', 'values'), [
    ('bool', [True, False, True]),
    ('uint8', range(0, 255)),
    ('int8', range(0, 128)),
    ('uint16', range(0, 10)),
    ('int16', range(0, 10)),
    ('uint32', range(0, 10)),
    ('int32', range(0, 10)),
    ('uint64', range(0, 10)),
    ('int64', range(0, 10)),
    ('float', [0.0, 0.1, 0.2]),
    ('double', [0.0, 0.1, 0.2]),
    ('string', ['a', 'b', 'c']),
    ('binary', [b'a', b'b', b'c']),
    (pa.binary(3), [b'abc', b'bcd', b'cde'])
])
def test_cast_identities(ty, values):
    arr = pa.array(values, type=ty)
    assert arr.cast(ty).equals(arr)


pickle_test_parametrize = pytest.mark.parametrize(
    ('data', 'typ'),
    [
        ([True, False, True, True], pa.bool_()),
        ([1, 2, 4, 6], pa.int64()),
        ([1.0, 2.5, None], pa.float64()),
        (['a', None, 'b'], pa.string()),
        ([], None),
        ([[1, 2], [3]], pa.list_(pa.int64())),
        ([[4, 5], [6]], pa.large_list(pa.int16())),
        ([['a'], None, ['b', 'c']], pa.list_(pa.string())),
        ([[1, 2], [3]], pa.list_view(pa.int64())),
        ([[4, 5], [6]], pa.large_list_view(pa.int16())),
        ([['a'], None, ['b', 'c']], pa.list_view(pa.string())),
        ([(1, 'a'), (2, 'c'), None],
            pa.struct([pa.field('a', pa.int64()), pa.field('b', pa.string())]))
    ]
)


@pickle_test_parametrize
def test_array_pickle(data, typ, pickle_module):
    # Allocate here so that we don't have any Arrow data allocated.
    # This is needed to ensure that allocator tests can be reliable.
    array = pa.array(data, type=typ)
    for proto in range(0, pickle_module.HIGHEST_PROTOCOL + 1):
        result = pickle_module.loads(pickle_module.dumps(array, proto))
        assert array.equals(result)


def test_array_pickle_dictionary(pickle_module):
    # not included in the above as dictionary array cannot be created with
    # the pa.array function
    array = pa.DictionaryArray.from_arrays([0, 1, 2, 0, 1], ['a', 'b', 'c'])
    for proto in range(0, pickle_module.HIGHEST_PROTOCOL + 1):
        result = pickle_module.loads(pickle_module.dumps(array, proto))
        assert array.equals(result)


@pickle_test_parametrize
def test_array_pickle_protocol5(data, typ, pickle_module):
    # Test zero-copy pickling with protocol 5 (PEP 574)
    array = pa.array(data, type=typ)
    addresses = [buf.address if buf is not None else 0
                 for buf in array.buffers()]

    for proto in range(5, pickle_module.HIGHEST_PROTOCOL + 1):
        buffers = []
        pickled = pickle_module.dumps(array, proto, buffer_callback=buffers.append)
        result = pickle_module.loads(pickled, buffers=buffers)
        assert array.equals(result)

        result_addresses = [buf.address if buf is not None else 0
                            for buf in result.buffers()]
        assert result_addresses == addresses


def test_time32_time64_from_integer():
    # ARROW-4111
    result = pa.array([1, 2, None], type=pa.time32('s'))
    expected = pa.array([datetime.time(second=1),
                         datetime.time(second=2), None],
                        type=pa.time32('s'))
    assert result.equals(expected)

    result = pa.array([1, 2, None], type=pa.time32('ms'))
    expected = pa.array([datetime.time(microsecond=1000),
                         datetime.time(microsecond=2000), None],
                        type=pa.time32('ms'))
    assert result.equals(expected)

    result = pa.array([1, 2, None], type=pa.time64('us'))
    expected = pa.array([datetime.time(microsecond=1),
                         datetime.time(microsecond=2), None],
                        type=pa.time64('us'))
    assert result.equals(expected)

    result = pa.array([1000, 2000, None], type=pa.time64('ns'))
    expected = pa.array([datetime.time(microsecond=1),
                         datetime.time(microsecond=2), None],
                        type=pa.time64('ns'))
    assert result.equals(expected)

@pytest.mark.pandas
def test_pandas_null_sentinels_index():
    # ARROW-7023 - ensure that when passing a pandas Index, "from_pandas"
    # semantics are used
    import pandas as pd
    idx = pd.Index([1, 2, np.nan], dtype=object)
    result = pa.array(idx)
    expected = pa.array([1, 2, np.nan], from_pandas=True)
    assert result.equals(expected)


def test_interval_array_from_timedelta():
    data = [
        None,
        datetime.timedelta(days=1, seconds=1, microseconds=1,
                           milliseconds=1, minutes=1, hours=1, weeks=1)]

    # From timedelta (explicit type required)
    arr = pa.array(data, pa.month_day_nano_interval())
    assert isinstance(arr, pa.MonthDayNanoIntervalArray)
    assert arr.type == pa.month_day_nano_interval()
    expected_list = [
        None,
        pa.MonthDayNano([0, 8,
                         (datetime.timedelta(seconds=1, microseconds=1,
                                             milliseconds=1, minutes=1,
                                             hours=1) //
                          datetime.timedelta(microseconds=1)) * 1000])]
    expected = pa.array(expected_list)
    assert arr.equals(expected)
    assert arr.to_pylist() == expected_list


@pytest.mark.pandas
def test_interval_array_from_relativedelta():
    # dateutil is dependency of pandas
    from dateutil.relativedelta import relativedelta
    from pandas import DateOffset
    data = [
        None,
        relativedelta(years=1, months=1,
                      days=1, seconds=1, microseconds=1,
                      minutes=1, hours=1, weeks=1, leapdays=1)]
    # Note leapdays are ignored.

    # From relativedelta
    arr = pa.array(data)
    assert isinstance(arr, pa.MonthDayNanoIntervalArray)
    assert arr.type == pa.month_day_nano_interval()
    expected_list = [
        None,
        pa.MonthDayNano([13, 8,
                         (datetime.timedelta(seconds=1, microseconds=1,
                                             minutes=1, hours=1) //
                          datetime.timedelta(microseconds=1)) * 1000])]
    expected = pa.array(expected_list)
    assert arr.equals(expected)
    assert arr.to_pandas().tolist() == [
        None, DateOffset(months=13, days=8,
                         microseconds=(
                             datetime.timedelta(seconds=1, microseconds=1,
                                                minutes=1, hours=1) //
                             datetime.timedelta(microseconds=1)),
                         nanoseconds=0)]
    with pytest.raises(ValueError):
        pa.array([DateOffset(years=((1 << 32) // 12), months=100)])
    with pytest.raises(ValueError):
        pa.array([DateOffset(weeks=((1 << 32) // 7), days=100)])
    with pytest.raises(ValueError):
        pa.array([DateOffset(seconds=((1 << 64) // 1000000000),
                             nanoseconds=1)])
    with pytest.raises(ValueError):
        pa.array([DateOffset(microseconds=((1 << 64) // 100))])


def test_interval_array_from_tuple():
    data = [None, (1, 2, -3)]

    # From timedelta (explicit type required)
    arr = pa.array(data, pa.month_day_nano_interval())
    assert isinstance(arr, pa.MonthDayNanoIntervalArray)
    assert arr.type == pa.month_day_nano_interval()
    expected_list = [
        None,
        pa.MonthDayNano([1, 2, -3])]
    expected = pa.array(expected_list)
    assert arr.equals(expected)
    assert arr.to_pylist() == expected_list


@pytest.mark.pandas
def test_interval_array_from_dateoffset():
    from pandas.tseries.offsets import DateOffset
    data = [
        None,
        DateOffset(years=1, months=1,
                   days=1, seconds=1, microseconds=1,
                   minutes=1, hours=1, weeks=1, nanoseconds=1),
        DateOffset()]

    arr = pa.array(data)
    assert isinstance(arr, pa.MonthDayNanoIntervalArray)
    assert arr.type == pa.month_day_nano_interval()
    expected_list = [
        None,
        pa.MonthDayNano([13, 8, 3661000001001]),
        pa.MonthDayNano([0, 0, 0])]
    expected = pa.array(expected_list)
    assert arr.equals(expected)
    expected_from_pandas = [
        None, DateOffset(months=13, days=8,
                         microseconds=(
                             datetime.timedelta(seconds=1, microseconds=1,
                                                minutes=1, hours=1) //
                             datetime.timedelta(microseconds=1)),
                         nanoseconds=1),
        DateOffset(months=0, days=0, microseconds=0, nanoseconds=0)]

    assert arr.to_pandas().tolist() == expected_from_pandas

    # nested list<interval> array conversion
    actual_list = pa.array([data]).to_pandas().tolist()
    assert len(actual_list) == 1
    assert list(actual_list[0]) == expected_from_pandas


def test_boolean_true_count_false_count():
    # ARROW-9145
    arr = pa.array([True, True, None, False, None, True] * 1000)
    assert arr.true_count == 3000
    assert arr.false_count == 1000


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
    assert bytearray(null_bitmap)[0] == 0b00000011
    values = buffers[2].to_pybytes()
    assert struct.unpack('bxx', values) == (42,)
    # The child buffers: 'b'
    null_bitmap = buffers[3].to_pybytes()
    assert bytearray(null_bitmap)[0] == 0b00000110
    values = buffers[4].to_pybytes()
    assert struct.unpack('4xh', values) == (43,)


def test_nbytes_size():
    a = pa.chunked_array([pa.array([1, None, 3], type=pa.int16()),
                          pa.array([4, 5, 6], type=pa.int16())])
    assert a.nbytes == 13


def test_invalid_tensor_constructor_repr():
    # ARROW-2638: prevent calling extension class constructors directly
    with pytest.raises(TypeError):
        repr(pa.Tensor([1]))


def test_invalid_tensor_construction():
    with pytest.raises(TypeError):
        pa.Tensor()


@pytest.mark.parametrize(('offset_type', 'list_type_factory'),
                         [(pa.int32(), pa.list_), (pa.int64(), pa.large_list)])
def test_list_array_flatten(offset_type, list_type_factory):
    typ2 = list_type_factory(
        list_type_factory(
            pa.int64()
        )
    )
    arr2 = pa.array([
        None,
        [
            [1, None, 2],
            None,
            [3, 4]
        ],
        [],
        [
            [],
            [5, 6],
            None
        ],
        [
            [7, 8]
        ]
    ], type=typ2)
    offsets2 = pa.array([0, 0, 3, 3, 6, 7], type=offset_type)

    typ1 = list_type_factory(pa.int64())
    arr1 = pa.array([
        [1, None, 2],
        None,
        [3, 4],
        [],
        [5, 6],
        None,
        [7, 8]
    ], type=typ1)
    offsets1 = pa.array([0, 3, 3, 5, 5, 7, 7, 9], type=offset_type)

    arr0 = pa.array([
        1, None, 2,
        3, 4,
        5, 6,
        7, 8
    ], type=pa.int64())

    assert arr2.flatten().equals(arr1)
    assert arr2.offsets.equals(offsets2)
    assert arr2.values.equals(arr1)
    assert arr1.flatten().equals(arr0)
    assert arr1.offsets.equals(offsets1)
    assert arr1.values.equals(arr0)
    assert arr2.flatten().flatten().equals(arr0)
    assert arr2.values.values.equals(arr0)
    assert arr2.flatten(True).equals(arr0)


@pytest.mark.parametrize('list_type', [
    pa.list_(pa.int32()),
    pa.list_(pa.int32(), list_size=2),
    pa.large_list(pa.int32())])
def test_list_value_parent_indices(list_type):
    arr = pa.array(
        [
            [0, 1],
            None,
            [None, None],
            [3, 4]
        ], type=list_type)
    expected = pa.array([0, 0, 2, 2, 3, 3], type=pa.int64())
    assert arr.value_parent_indices().equals(expected)


@pytest.mark.parametrize(('offset_type', 'list_type'),
                         [(pa.int32(), pa.list_(pa.int32())),
                          (pa.int32(), pa.list_(pa.int32(), list_size=2)),
                          (pa.int64(), pa.large_list(pa.int32())),
                          (pa.int32(), pa.list_view(pa.int32())),
                          (pa.int64(), pa.large_list_view(pa.int32()))])
def test_list_value_lengths(offset_type, list_type):

    # FixedSizeListArray needs fixed list sizes
    if getattr(list_type, "list_size", None):
        arr = pa.array(
            [
                [0, 1],
                None,
                [None, None],
                [3, 4]
            ], type=list_type)
        expected = pa.array([2, None, 2, 2], type=offset_type)

    # Otherwise create variable list sizes
    else:
        arr = pa.array(
            [
                [0, 1, 2],
                None,
                [],
                [3, 4]
            ], type=list_type)
        expected = pa.array([3, None, 0, 2], type=offset_type)
    assert arr.value_lengths().equals(expected)


@pytest.mark.parametrize('list_type_factory', [pa.list_, pa.large_list])
def test_list_array_flatten_non_canonical(list_type_factory):
    # Non-canonical list array (null elements backed by non-empty sublists)
    typ = list_type_factory(pa.int64())
    arr = pa.array([[1], [2, 3], [4, 5, 6]], type=typ)
    buffers = arr.buffers()[:2]
    buffers[0] = pa.py_buffer(b"\x05")  # validity bitmap
    arr = arr.from_buffers(arr.type, len(arr), buffers, children=[arr.values])
    assert arr.to_pylist() == [[1], None, [4, 5, 6]]
    assert arr.offsets.to_pylist() == [0, 1, 3, 6]

    flattened = arr.flatten()
    flattened.validate(full=True)
    assert flattened.type == typ.value_type
    assert flattened.to_pylist() == [1, 4, 5, 6]

    # .values is the physical values array (including masked elements)
    assert arr.values.to_pylist() == [1, 2, 3, 4, 5, 6]


@pytest.mark.parametrize('klass', [pa.ListArray, pa.LargeListArray])
def test_list_array_values_offsets_sliced(klass):
    # ARROW-7301
    arr = klass.from_arrays(offsets=[0, 3, 4, 6], values=[1, 2, 3, 4, 5, 6])
    assert arr.values.to_pylist() == [1, 2, 3, 4, 5, 6]
    assert arr.offsets.to_pylist() == [0, 3, 4, 6]

    # sliced -> values keeps referring to full values buffer, but offsets is
    # sliced as well so the offsets correctly point into the full values array
    # sliced -> flatten() will return the sliced value array.
    arr2 = arr[1:]
    assert arr2.values.to_pylist() == [1, 2, 3, 4, 5, 6]
    assert arr2.offsets.to_pylist() == [3, 4, 6]
    assert arr2.flatten().to_pylist() == [4, 5, 6]
    i = arr2.offsets[0].as_py()
    j = arr2.offsets[1].as_py()
    assert arr2[0].as_py() == arr2.values[i:j].to_pylist() == [4]


def test_fixed_size_list_array_flatten():
    typ2 = pa.list_(pa.list_(pa.int64(), 2), 3)
    arr2 = pa.array([
        [
            [1, 2],
            [3, 4],
            [5, 6],
        ],
        None,
        [
            [7, None],
            None,
            [8, 9]
        ],
    ], type=typ2)
    assert arr2.type.equals(typ2)

    typ1 = pa.list_(pa.int64(), 2)
    arr1 = pa.array([
        [1, 2], [3, 4], [5, 6],
        [7, None], None, [8, 9]
    ], type=typ1)
    assert arr1.type.equals(typ1)
    assert arr2.flatten().equals(arr1)

    typ0 = pa.int64()
    arr0 = pa.array([
        1, 2, 3, 4, 5, 6, 7, None, 8, 9,
    ], type=typ0)
    assert arr0.type.equals(typ0)
    assert arr1.flatten().equals(arr0)
    assert arr2.flatten().flatten().equals(arr0)
    assert arr2.flatten().equals(arr1)
    assert arr2.flatten(True).equals(arr0)


def test_fixed_size_list_array_flatten_with_slice():
    array = pa.array([[1], [2], [3]],
                     type=pa.list_(pa.float64(), list_size=1))
    assert array[2:].flatten() == pa.array([3], type=pa.float64())


def test_map_array_values_offsets():
    ty = pa.map_(pa.utf8(), pa.int32())
    ty_values = pa.struct([pa.field("key", pa.utf8(), nullable=False),
                           pa.field("value", pa.int32())])
    a = pa.array([[('a', 1), ('b', 2)], [('c', 3)]], type=ty)

    assert a.values.type.equals(ty_values)
    assert a.values == pa.array([
        {'key': 'a', 'value': 1},
        {'key': 'b', 'value': 2},
        {'key': 'c', 'value': 3},
    ], type=ty_values)
    assert a.keys.equals(pa.array(['a', 'b', 'c']))
    assert a.items.equals(pa.array([1, 2, 3], type=pa.int32()))

    assert pa.ListArray.from_arrays(a.offsets, a.keys).equals(
        pa.array([['a', 'b'], ['c']]))
    assert pa.ListArray.from_arrays(a.offsets, a.items).equals(
        pa.array([[1, 2], [3]], type=pa.list_(pa.int32())))

    with pytest.raises(NotImplementedError):
        a.flatten()


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


def test_struct_array_field():
    ty = pa.struct([pa.field('x', pa.int16()),
                    pa.field('y', pa.float32())])
    a = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty)

    x0 = a.field(0)
    y0 = a.field(1)
    x1 = a.field(-2)
    y1 = a.field(-1)
    x2 = a.field('x')
    y2 = a.field('y')

    assert isinstance(x0, pa.lib.Int16Array)
    assert isinstance(y1, pa.lib.FloatArray)
    assert x0.equals(pa.array([1, 3, 5], type=pa.int16()))
    assert y0.equals(pa.array([2.5, 4.5, 6.5], type=pa.float32()))
    assert x0.equals(x1)
    assert x0.equals(x2)
    assert y0.equals(y1)
    assert y0.equals(y2)

    for invalid_index in [None, pa.int16()]:
        with pytest.raises(TypeError):
            a.field(invalid_index)

    for invalid_index in [3, -3]:
        with pytest.raises(IndexError):
            a.field(invalid_index)

    for invalid_name in ['z', '']:
        with pytest.raises(KeyError):
            a.field(invalid_name)


def test_struct_array_flattened_field():
    ty = pa.struct([pa.field('x', pa.int16()),
                    pa.field('y', pa.float32())])
    a = pa.array([(1, 2.5), (3, 4.5), (5, 6.5)], type=ty,
                 mask=pa.array([False, True, False]))

    x0 = a._flattened_field(0)
    y0 = a._flattened_field(1)
    x1 = a._flattened_field(-2)
    y1 = a._flattened_field(-1)
    x2 = a._flattened_field('x')
    y2 = a._flattened_field('y')

    assert isinstance(x0, pa.lib.Int16Array)
    assert isinstance(y1, pa.lib.FloatArray)
    assert x0.equals(pa.array([1, None, 5], type=pa.int16()))
    assert y0.equals(pa.array([2.5, None, 6.5], type=pa.float32()))
    assert x0.equals(x1)
    assert x0.equals(x2)
    assert y0.equals(y1)
    assert y0.equals(y2)

    for invalid_index in [None, pa.int16()]:
        with pytest.raises(TypeError):
            a._flattened_field(invalid_index)

    for invalid_index in [3, -3]:
        with pytest.raises(IndexError):
            a._flattened_field(invalid_index)

    for invalid_name in ['z', '']:
        with pytest.raises(KeyError):
            a._flattened_field(invalid_name)


def test_empty_cast():
    types = [
        pa.null(),
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.date32(),
        pa.date64(),
        pa.binary(),
        pa.binary(length=4),
        pa.string(),
    ]

    for (t1, t2) in itertools.product(types, types):
        try:
            # ARROW-4766: Ensure that supported types conversion don't segfault
            # on empty arrays of common types
            pa.array([], type=t1).cast(t2)
        except (pa.lib.ArrowNotImplementedError, pa.ArrowInvalid):
            continue


def test_nested_dictionary_array():
    dict_arr = pa.DictionaryArray.from_arrays([0, 1, 0], ['a', 'b'])
    list_arr = pa.ListArray.from_arrays([0, 2, 3], dict_arr)
    assert list_arr.to_pylist() == [['a', 'b'], ['a']]

    dict_arr = pa.DictionaryArray.from_arrays([0, 1, 0], ['a', 'b'])
    dict_arr2 = pa.DictionaryArray.from_arrays([0, 1, 2, 1, 0], dict_arr)
    assert dict_arr2.to_pylist() == ['a', 'b', 'a', 'b', 'a']


@pytest.mark.large_memory
def test_list_child_overflow_to_chunked():
    kilobyte_string = 'x' * 1024
    two_mega = 2**21

    vals = [[kilobyte_string]] * (two_mega - 1)
    arr = pa.array(vals)
    assert isinstance(arr, pa.Array)
    assert len(arr) == two_mega - 1

    vals = [[kilobyte_string]] * two_mega
    arr = pa.array(vals)
    assert isinstance(arr, pa.ChunkedArray)
    assert len(arr) == two_mega
    assert len(arr.chunk(0)) == two_mega - 1
    assert len(arr.chunk(1)) == 1


@pytest.mark.pandas
def test_array_supported_pandas_masks():
    import pandas
    arr = pa.array(pandas.Series([0, 1], name="a", dtype="int64"),
                   mask=pandas.Series([True, False], dtype='bool'))
    assert arr.to_pylist() == [None, 1]


def test_array_from_large_pyints():
    # ARROW-5430
    with pytest.raises(OverflowError):
        # too large for int64 so dtype must be explicitly provided
        pa.array([int(2 ** 63)])


class ArrayWrapper:
    def __init__(self, data):
        self.data = data

    def __arrow_c_array__(self, requested_schema=None):
        return self.data.__arrow_c_array__(requested_schema)


class ArrayDeviceWrapper:
    def __init__(self, data):
        self.data = data

    def __arrow_c_device_array__(self, requested_schema=None, **kwargs):
        return self.data.__arrow_c_device_array__(requested_schema, **kwargs)


@pytest.mark.parametrize("wrapper_class", [ArrayWrapper, ArrayDeviceWrapper])
def test_c_array_protocol(wrapper_class):

    # Can roundtrip through the C array protocol
    arr = wrapper_class(pa.array([1, 2, 3], type=pa.int64()))
    result = pa.array(arr)
    assert result == arr.data

    # Will cast to requested type
    result = pa.array(arr, type=pa.int32())
    assert result == pa.array([1, 2, 3], type=pa.int32())


def test_c_array_protocol_device_unsupported_keyword():
    # For the device-aware version, we raise a specific error for unsupported keywords
    arr = pa.array([1, 2, 3], type=pa.int64())

    with pytest.raises(
        NotImplementedError,
        match=r"Received unsupported keyword argument\(s\): \['other'\]"
    ):
        arr.__arrow_c_device_array__(other="not-none")

    # but with None value it is ignored
    _ = arr.__arrow_c_device_array__(other=None)


def test_concat_array():
    concatenated = pa.concat_arrays(
        [pa.array([1, 2]), pa.array([3, 4])])
    assert concatenated.equals(pa.array([1, 2, 3, 4]))


def test_concat_array_different_types():
    with pytest.raises(pa.ArrowInvalid):
        pa.concat_arrays([pa.array([1]), pa.array([2.])])


def test_concat_array_invalid_type():
    # ARROW-9920 - do not segfault on non-array input

    with pytest.raises(TypeError, match="should contain Array objects"):
        pa.concat_arrays([None])

    arr = pa.chunked_array([[0, 1], [3, 4]])
    with pytest.raises(TypeError, match="should contain Array objects"):
        pa.concat_arrays(arr)


@pytest.mark.pandas
def test_to_pandas_timezone():
    # https://issues.apache.org/jira/browse/ARROW-6652
    arr = pa.array([1, 2, 3], type=pa.timestamp('s', tz='Europe/Brussels'))
    s = arr.to_pandas()
    assert s.dt.tz is not None
    arr = pa.chunked_array([arr])
    s = arr.to_pandas()
    assert s.dt.tz is not None


@pytest.mark.pandas
def test_to_pandas_float16_list():
    # https://github.com/apache/arrow/issues/36168
    expected = [[np.float16(1)], [np.float16(2)], [np.float16(3)]]
    arr = pa.array(expected)
    result = arr.to_pandas()
    assert result[0].dtype == "float16"
    assert result.tolist() == expected


def test_array_sort():
    arr = pa.array([5, 7, 35], type=pa.int64())
    sorted_arr = arr.sort("descending")
    assert sorted_arr.to_pylist() == [35, 7, 5]

    arr = pa.chunked_array([[1, 2, 3], [4, 5, 6]])
    sorted_arr = arr.sort("descending")
    assert sorted_arr.to_pylist() == [6, 5, 4, 3, 2, 1]

    arr = pa.array([5, 7, 35, None], type=pa.int64())
    sorted_arr = arr.sort("descending", null_placement="at_end")
    assert sorted_arr.to_pylist() == [35, 7, 5, None]
    sorted_arr = arr.sort("descending", null_placement="at_start")
    assert sorted_arr.to_pylist() == [None, 35, 7, 5]


def test_struct_array_sort():
    arr = pa.StructArray.from_arrays([
        pa.array([5, 7, 7, 35], type=pa.int64()),
        pa.array(["foo", "car", "bar", "foobar"])
    ], names=["a", "b"])

    sorted_arr = arr.sort("descending", by="a")
    assert sorted_arr.to_pylist() == [
        {"a": 35, "b": "foobar"},
        {"a": 7, "b": "car"},
        {"a": 7, "b": "bar"},
        {"a": 5, "b": "foo"},
    ]

    sorted_arr = arr.sort()
    assert sorted_arr.to_pylist() == [
        {"a": 5, "b": "foo"},
        {"a": 7, "b": "bar"},
        {"a": 7, "b": "car"},
        {"a": 35, "b": "foobar"},
    ]

    arr_with_nulls = pa.StructArray.from_arrays([
        pa.array([5, 7, 7, 35], type=pa.int64()),
        pa.array(["foo", "car", "bar", "foobar"])
    ], names=["a", "b"], mask=pa.array([False, False, True, False]))

    sorted_arr = arr_with_nulls.sort(
        "descending", by="a", null_placement="at_start")
    assert sorted_arr.to_pylist() == [
        None,
        {"a": 35, "b": "foobar"},
        {"a": 7, "b": "car"},
        {"a": 5, "b": "foo"},
    ]

    sorted_arr = arr_with_nulls.sort(
        "descending", by="a", null_placement="at_end")
    assert sorted_arr.to_pylist() == [
        {"a": 35, "b": "foobar"},
        {"a": 7, "b": "car"},
        {"a": 5, "b": "foo"},
        None
    ]


def test_array_accepts_pyarrow_array():
    arr = pa.array([1, 2, 3])
    result = pa.array(arr)
    assert arr == result

    # Test casting to a different type
    result = pa.array(arr, type=pa.uint8())
    expected = pa.array([1, 2, 3], type=pa.uint8())
    assert expected == result
    assert expected.type == pa.uint8()

    # Test casting with safe keyword
    arr = pa.array([2 ** 63 - 1], type=pa.int64())

    with pytest.raises(pa.ArrowInvalid):
        pa.array(arr, type=pa.int32())

    expected = pa.array([-1], type=pa.int32())
    result = pa.array(arr, type=pa.int32(), safe=False)
    assert result == expected

    # Test memory_pool keyword is accepted
    result = pa.array(arr, memory_pool=pa.default_memory_pool())
    assert arr == result


def check_run_end_encoded(ree_array, run_ends, values, logical_length, physical_length,
                          physical_offset):
    assert ree_array.run_ends.to_pylist() == run_ends
    assert ree_array.values.to_pylist() == values
    assert len(ree_array) == logical_length
    assert ree_array.find_physical_length() == physical_length
    assert ree_array.find_physical_offset() == physical_offset


def check_run_end_encoded_from_arrays_with_type(ree_type=None):
    run_ends = [3, 5, 10, 19]
    values = [1, 2, 1, 3]
    ree_array = pa.RunEndEncodedArray.from_arrays(run_ends, values, ree_type)
    check_run_end_encoded(ree_array, run_ends, values, 19, 4, 0)


def check_run_end_encoded_from_typed_arrays(ree_type):
    run_ends = [3, 5, 10, 19]
    values = [1, 2, 1, 3]
    typed_run_ends = pa.array(run_ends, ree_type.run_end_type)
    typed_values = pa.array(values, ree_type.value_type)
    ree_array = pa.RunEndEncodedArray.from_arrays(typed_run_ends, typed_values)
    assert ree_array.type == ree_type
    check_run_end_encoded(ree_array, run_ends, values, 19, 4, 0)


def test_run_end_encoded_from_arrays():
    check_run_end_encoded_from_arrays_with_type()
    for run_end_type in [pa.int16(), pa.int32(), pa.int64()]:
        for value_type in [pa.uint32(), pa.int32(), pa.uint64(), pa.int64()]:
            ree_type = pa.run_end_encoded(run_end_type, value_type)
            check_run_end_encoded_from_arrays_with_type(ree_type)
            check_run_end_encoded_from_typed_arrays(ree_type)


def test_run_end_encoded_from_buffers():
    run_ends = [3, 5, 10, 19]
    values = [1, 2, 1, 3]

    ree_type = pa.run_end_encoded(run_end_type=pa.int32(), value_type=pa.uint8())
    length = 19
    buffers = [None]
    null_count = 0
    offset = 0
    children = [run_ends, values]

    ree_array = pa.RunEndEncodedArray.from_buffers(ree_type, length, buffers,
                                                   null_count, offset,
                                                   children)
    check_run_end_encoded(ree_array, run_ends, values, 19, 4, 0)
    # buffers = []
    ree_array = pa.RunEndEncodedArray.from_buffers(ree_type, length, [],
                                                   null_count, offset,
                                                   children)
    check_run_end_encoded(ree_array, run_ends, values, 19, 4, 0)
    # null_count = -1
    ree_array = pa.RunEndEncodedArray.from_buffers(ree_type, length, buffers,
                                                   -1, offset,
                                                   children)
    check_run_end_encoded(ree_array, run_ends, values, 19, 4, 0)
    # offset = 4
    ree_array = pa.RunEndEncodedArray.from_buffers(ree_type, length - 4, buffers,
                                                   null_count, 4, children)
    check_run_end_encoded(ree_array, run_ends, values, length - 4, 3, 1)
    # buffers = [None, None]
    with pytest.raises(ValueError):
        pa.RunEndEncodedArray.from_buffers(ree_type, length, [None, None],
                                           null_count, offset, children)
    # children = None
    with pytest.raises(ValueError):
        pa.RunEndEncodedArray.from_buffers(ree_type, length, buffers,
                                           null_count, offset, None)
    # len(children) == 1
    with pytest.raises(ValueError):
        pa.RunEndEncodedArray.from_buffers(ree_type, length, buffers,
                                           null_count, offset, [run_ends])
    # null_count = 1
    with pytest.raises(ValueError):
        pa.RunEndEncodedArray.from_buffers(ree_type, length, buffers,
                                           1, offset, children)


@pytest.mark.pandas
def test_run_end_encoded_to_pandas():
    arr = [1, 2, 2, 3, 3, 3]
    ree_array = pa.array(arr, pa.run_end_encoded(pa.int32(), pa.int64()))

    assert ree_array.to_pandas().tolist() == arr

    with pytest.raises(pa.ArrowInvalid):
        ree_array.to_pandas(zero_copy_only=True)


@pytest.mark.parametrize(('list_array_type', 'list_type_factory'),
                         [(pa.ListViewArray, pa.list_view),
                          (pa.LargeListViewArray, pa.large_list_view)])
def test_list_view_from_arrays(list_array_type, list_type_factory):
    # test in order offsets, similar to ListArray representation
    values = [1, 2, 3, 4, 5, 6, None, 7]
    offsets = [0, 2, 4, 6]
    sizes = [2, 2, 2, 2]
    array = list_array_type.from_arrays(offsets, sizes, values)

    assert array.to_pylist() == [[1, 2], [3, 4], [5, 6], [None, 7]]
    assert array.values.to_pylist() == values
    assert array.offsets.to_pylist() == offsets
    assert array.sizes.to_pylist() == sizes

    # with specified type
    typ = list_type_factory(pa.field("name", pa.int64()))
    result = list_array_type.from_arrays(offsets, sizes, values, typ)
    assert result.type == typ
    assert result.type.value_field.name == "name"

    # with mismatching type
    typ = list_type_factory(pa.binary())
    with pytest.raises(TypeError):
        list_array_type.from_arrays(offsets, sizes, values, type=typ)

    # test out of order offsets with overlapping values
    values = [1, 2, 3, 4]
    offsets = [2, 1, 0]
    sizes = [2, 2, 2]
    array = list_array_type.from_arrays(offsets, sizes, values)

    assert array.to_pylist() == [[3, 4], [2, 3], [1, 2]]
    assert array.values.to_pylist() == values
    assert array.offsets.to_pylist() == offsets
    assert array.sizes.to_pylist() == sizes

    # test null offsets and empty list values
    values = []
    offsets = [0, None]
    sizes = [0, 0]
    array = list_array_type.from_arrays(offsets, sizes, values)

    assert array.to_pylist() == [[], None]
    assert array.values.to_pylist() == values
    assert array.offsets.to_pylist() == [0, 0]
    assert array.sizes.to_pylist() == sizes

    # test null sizes and empty list values
    values = []
    offsets = [0, 0]
    sizes = [None, 0]
    array = list_array_type.from_arrays(offsets, sizes, values)

    assert array.to_pylist() == [None, []]
    assert array.values.to_pylist() == values
    assert array.offsets.to_pylist() == offsets
    assert array.sizes.to_pylist() == [0, 0]

    # test null bitmask
    values = [1, 2]
    offsets = [0, 0, 1]
    sizes = [1, 0, 1]
    mask = pa.array([False, True, False])
    array = list_array_type.from_arrays(offsets, sizes, values, mask=mask)

    assert array.to_pylist() == [[1], None, [2]]
    assert array.values.to_pylist() == values
    assert array.offsets.to_pylist() == offsets
    assert array.sizes.to_pylist() == sizes


@pytest.mark.parametrize(('list_array_type', 'list_type_factory'),
                         [(pa.ListViewArray, pa.list_view),
                          (pa.LargeListViewArray, pa.large_list_view)])
def test_list_view_from_arrays_fails(list_array_type, list_type_factory):
    values = [1, 2]
    offsets = [0, 1, None]
    sizes = [1, 1, 0]
    mask = pa.array([False, False, True])

    # Ambiguous to specify both validity map and offsets or sizes with nulls
    with pytest.raises(pa.lib.ArrowInvalid):
        list_array_type.from_arrays(offsets, sizes, values, mask=mask)

    offsets = [0, 1, 1]
    array = list_array_type.from_arrays(offsets, sizes, values, mask=mask)
    array_slice = array[1:]

    # List offsets and sizes must not be slices if a validity map is specified
    with pytest.raises(pa.lib.ArrowInvalid):
        list_array_type.from_arrays(
            array_slice.offsets, array_slice.sizes,
            array_slice.values, mask=array_slice.is_null())


@pytest.mark.parametrize(('list_array_type', 'list_type_factory', 'offset_type'),
                         [(pa.ListViewArray, pa.list_view, pa.int32()),
                          (pa.LargeListViewArray, pa.large_list_view, pa.int64())])
def test_list_view_flatten(list_array_type, list_type_factory, offset_type):
    arr0 = pa.array([
        1, None, 2,
        3, 4,
        5, 6,
        7, 8
    ], type=pa.int64())

    typ1 = list_type_factory(pa.int64())
    arr1 = pa.array([
        [1, None, 2],
        None,
        [3, 4],
        [],
        [5, 6],
        None,
        [7, 8]
    ], type=typ1)
    offsets1 = pa.array([0, 3, 3, 5, 5, 7, 7], type=offset_type)
    sizes1 = pa.array([3, 0, 2, 0, 2, 0, 2], type=offset_type)

    typ2 = list_type_factory(
        list_type_factory(
            pa.int64()
        )
    )
    arr2 = pa.array([
        None,
        [
            [1, None, 2],
            None,
            [3, 4]
        ],
        [],
        [
            [],
            [5, 6],
            None
        ],
        [
            [7, 8]
        ]
    ], type=typ2)
    offsets2 = pa.array([0, 0, 3, 3, 6], type=offset_type)
    sizes2 = pa.array([0, 3, 0, 3, 1], type=offset_type)

    assert arr1.flatten().equals(arr0)
    assert arr1.offsets.equals(offsets1)
    assert arr1.sizes.equals(sizes1)
    assert arr1.values.equals(arr0)
    assert arr2.flatten().equals(arr1)
    assert arr2.offsets.equals(offsets2)
    assert arr2.sizes.equals(sizes2)
    assert arr2.values.equals(arr1)
    assert arr2.flatten().flatten().equals(arr0)
    assert arr2.values.values.equals(arr0)
    assert arr2.flatten(True).equals(arr0)

    # test out of order offsets
    values = [1, 2, 3, 4]
    offsets = [3, 2, 1, 0]
    sizes = [1, 1, 1, 1]
    array = list_array_type.from_arrays(offsets, sizes, values)

    assert array.flatten().to_pylist() == [4, 3, 2, 1]

    # test null elements backed by non-empty sublists
    mask = pa.array([False, False, False, True])
    array = list_array_type.from_arrays(offsets, sizes, values, mask=mask)

    assert array.flatten().to_pylist() == [4, 3, 2]
    assert array.values.to_pylist() == [1, 2, 3, 4]


@pytest.mark.parametrize('list_view_type', [pa.ListViewArray, pa.LargeListViewArray])
def test_list_view_slice(list_view_type):
    # sliced -> values keeps referring to full values buffer, but offsets is
    # sliced as well so the offsets correctly point into the full values array
    # sliced -> flatten() will return the sliced value array.

    array = list_view_type.from_arrays(offsets=[0, 3, 4], sizes=[
                                       3, 1, 2], values=[1, 2, 3, 4, 5, 6])
    sliced_array = array[1:]

    assert sliced_array.values.to_pylist() == [1, 2, 3, 4, 5, 6]
    assert sliced_array.offsets.to_pylist() == [3, 4]
    assert sliced_array.flatten().to_pylist() == [4, 5, 6]

    i = sliced_array.offsets[0].as_py()
    j = sliced_array.offsets[1].as_py()

    assert sliced_array[0].as_py() == sliced_array.values[i:j].to_pylist() == [4]


def test_non_cpu_array():
    cuda = pytest.importorskip("pyarrow.cuda")
    ctx = cuda.Context(0)

    data = np.arange(4, dtype=np.int32)
    validity = np.array([True, False, True, False], dtype=np.bool_)
    cuda_data_buf = ctx.buffer_from_data(data)
    cuda_validity_buf = ctx.buffer_from_data(validity)
    arr = pa.Array.from_buffers(pa.int32(), 4, [None, cuda_data_buf])
    arr2 = pa.Array.from_buffers(pa.int32(), 4, [None, cuda_data_buf])
    arr_with_nulls = pa.Array.from_buffers(
        pa.int32(), 4, [cuda_validity_buf, cuda_data_buf])

    # Supported
    arr.validate()
    assert arr.offset == 0
    assert arr.buffers() == [None, cuda_data_buf]
    assert arr.device_type == pa.DeviceAllocationType.CUDA
    assert arr.is_cpu is False
    assert len(arr) == 4
    assert arr.slice(2, 2).offset == 2
    assert repr(arr)
    assert str(arr)

    # TODO support DLPack for CUDA
    with pytest.raises(NotImplementedError):
        arr.__dlpack__()
    with pytest.raises(NotImplementedError):
        arr.__dlpack_device__()

    # Not Supported
    with pytest.raises(NotImplementedError):
        arr.diff(arr2)
    with pytest.raises(NotImplementedError):
        arr.cast(pa.int64())
    with pytest.raises(NotImplementedError):
        arr.view(pa.int64())
    with pytest.raises(NotImplementedError):
        arr.sum()
    with pytest.raises(NotImplementedError):
        arr.unique()
    with pytest.raises(NotImplementedError):
        arr.dictionary_encode()
    with pytest.raises(NotImplementedError):
        arr.value_counts()
    with pytest.raises(NotImplementedError):
        arr_with_nulls.null_count
    with pytest.raises(NotImplementedError):
        arr.nbytes
    with pytest.raises(NotImplementedError):
        arr.get_total_buffer_size()
    with pytest.raises(NotImplementedError):
        [i for i in iter(arr)]
    with pytest.raises(NotImplementedError):
        arr == arr2
    with pytest.raises(NotImplementedError):
        arr.is_null()
    with pytest.raises(NotImplementedError):
        arr.is_nan()
    with pytest.raises(NotImplementedError):
        arr.is_valid()
    with pytest.raises(NotImplementedError):
        arr.fill_null(0)
    with pytest.raises(NotImplementedError):
        arr[0]
    with pytest.raises(NotImplementedError):
        arr.take([0])
    with pytest.raises(NotImplementedError):
        arr.drop_null()
    with pytest.raises(NotImplementedError):
        arr.filter([True, True, False, False])
    with pytest.raises(NotImplementedError):
        arr.index(0)
    with pytest.raises(NotImplementedError):
        arr.sort()
    with pytest.raises(NotImplementedError):
        arr.__array__()
    with pytest.raises(NotImplementedError):
        arr.to_numpy()
    with pytest.raises(NotImplementedError):
        arr.tolist()
    with pytest.raises(NotImplementedError):
        arr.validate(full=True)
