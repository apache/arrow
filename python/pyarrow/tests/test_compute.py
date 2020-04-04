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
import pytest

import pyarrow as pa
import pyarrow.compute


all_array_types = [
    ('bool', [True, False, False, True, True]),
    ('uint8', np.arange(5)),
    ('int8', np.arange(5)),
    ('uint16', np.arange(5)),
    ('int16', np.arange(5)),
    ('uint32', np.arange(5)),
    ('int32', np.arange(5)),
    ('uint64', np.arange(5, 10)),
    ('int64', np.arange(5, 10)),
    ('float', np.arange(0, 0.5, 0.1)),
    ('double', np.arange(0, 0.5, 0.1)),
    ('string', ['a', 'b', None, 'ddd', 'ee']),
    ('binary', [b'a', b'b', b'c', b'ddd', b'ee']),
    (pa.binary(3), [b'abc', b'bcd', b'cde', b'def', b'efg']),
    (pa.list_(pa.int8()), [[1, 2], [3, 4], [5, 6], None, [9, 16]]),
    (pa.large_list(pa.int16()), [[1], [2, 3, 4], [5, 6], None, [9, 16]]),
    (pa.struct([('a', pa.int8()), ('b', pa.int8())]), [
     {'a': 1, 'b': 2}, None, {'a': 3, 'b': 4}, None, {'a': 5, 'b': 6}]),
]


numerical_arrow_types = [
    pa.int8(),
    pa.int16(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint64(),
    pa.float32(),
    pa.float64()
]


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_sum_array(arrow_type):
    arr = pa.array([1, 2, 3, 4], type=arrow_type)
    assert arr.sum() == 10
    assert pa.compute.sum(arr) == 10

    arr = pa.array([], type=arrow_type)
    assert arr.sum() == None  # noqa: E711
    assert pa.compute.sum(arr) == None  # noqa: E711


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_sum_chunked_array(arrow_type):
    arr = pa.chunked_array([pa.array([1, 2, 3, 4], type=arrow_type)])
    assert pa.compute.sum(arr) == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type), pa.array([3, 4], type=arrow_type)
    ])
    assert pa.compute.sum(arr) == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type),
        pa.array([], type=arrow_type),
        pa.array([3, 4], type=arrow_type)
    ])
    assert pa.compute.sum(arr) == 10

    arr = pa.chunked_array((), type=arrow_type)
    print(arr, type(arr))
    assert arr.num_chunks == 0
    assert pa.compute.sum(arr) == None  # noqa: E711


@pytest.mark.parametrize(('ty', 'values'), all_array_types)
def test_take(ty, values):
    arr = pa.array(values, type=ty)
    for indices_type in [pa.uint8(), pa.int64()]:
        indices = pa.array([0, 4, 2, None], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([values[0], values[4], values[2], None], type=ty)
        assert result.equals(expected)

        # empty indices
        indices = pa.array([], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([], type=ty)
        assert result.equals(expected)

    indices = pa.array([2, 5])
    with pytest.raises(IndexError):
        arr.take(indices)

    indices = pa.array([2, -1])
    with pytest.raises(IndexError):
        arr.take(indices)


def test_take_indices_types():
    arr = pa.array(range(5))

    for indices_type in ['uint8', 'int8', 'uint16', 'int16',
                         'uint32', 'int32', 'uint64', 'int64']:
        indices = pa.array([0, 4, 2, None], type=indices_type)
        result = arr.take(indices)
        result.validate()
        expected = pa.array([0, 4, 2, None])
        assert result.equals(expected)

    for indices_type in [pa.float32(), pa.float64()]:
        indices = pa.array([0, 4, 2], type=indices_type)
        with pytest.raises(TypeError):
            arr.take(indices)


@pytest.mark.parametrize('ordered', [False, True])
def test_take_dictionary(ordered):
    arr = pa.DictionaryArray.from_arrays([0, 1, 2, 0, 1, 2], ['a', 'b', 'c'],
                                         ordered=ordered)
    result = arr.take(pa.array([0, 1, 3]))
    result.validate()
    assert result.to_pylist() == ['a', 'b', 'a']
    assert result.dictionary.to_pylist() == ['a', 'b', 'c']
    assert result.type.ordered is ordered


@pytest.mark.parametrize(('ty', 'values'), all_array_types)
def test_filter(ty, values):
    arr = pa.array(values, type=ty)

    mask = pa.array([True, False, False, True, None])
    result = arr.filter(mask, null_selection_behavior='drop')
    result.validate()
    assert result.equals(pa.array([values[0], values[3]], type=ty))
    result = arr.filter(mask, null_selection_behavior='emit_null')
    result.validate()
    assert result.equals(pa.array([values[0], values[3], None], type=ty))

    # non-boolean dtype
    mask = pa.array([0, 1, 0, 1, 0])
    with pytest.raises(TypeError, match="got int64"):
        arr.filter(mask)

    # wrong length
    mask = pa.array([True, False, True])
    with pytest.raises(ValueError, match="must have identical lengths"):
        arr.filter(mask)


def test_filter_chunked_array():
    arr = pa.chunked_array([["a", None], ["c", "d", "e"]])
    expected_drop = pa.chunked_array([["a"], ["e"]])
    expected_null = pa.chunked_array([["a"], [None, "e"]])

    for mask in [
        # mask is array
        pa.array([True, False, None, False, True]),
        # mask is chunked array
        pa.chunked_array([[True, False, None], [False, True]]),
        # mask is python object
        [True, False, None, False, True]
    ]:
        result = arr.filter(mask)
        assert result.equals(expected_drop)
        result = arr.filter(mask, null_selection_behavior="emit_null")
        assert result.equals(expected_null)


def test_filter_record_batch():
    batch = pa.record_batch(
        [pa.array(["a", None, "c", "d", "e"])], names=["a'"])

    # mask is array
    mask = pa.array([True, False, None, False, True])
    result = batch.filter(mask)
    expected = pa.record_batch([pa.array(["a", "e"])], names=["a'"])
    assert result.equals(expected)

    result = batch.filter(mask, null_selection_behavior="emit_null")
    expected = pa.record_batch([pa.array(["a", None, "e"])], names=["a'"])
    assert result.equals(expected)


def test_filter_table():
    table = pa.table([pa.array(["a", None, "c", "d", "e"])], names=["a"])
    expected_drop = pa.table([pa.array(["a", "e"])], names=["a"])
    expected_null = pa.table([pa.array(["a", None, "e"])], names=["a"])

    for mask in [
        # mask is array
        pa.array([True, False, None, False, True]),
        # mask is chunked array
        pa.chunked_array([[True, False], [None, False, True]]),
        # mask is python object
        [True, False, None, False, True]
    ]:
        result = table.filter(mask)
        assert result.equals(expected_drop)
        result = table.filter(mask, null_selection_behavior="emit_null")
        assert result.equals(expected_null)


def test_filter_errors():
    arr = pa.chunked_array([["a", None], ["c", "d", "e"]])
    batch = pa.record_batch(
        [pa.array(["a", None, "c", "d", "e"])], names=["a'"])
    table = pa.table([pa.array(["a", None, "c", "d", "e"])], names=["a"])

    for obj in [arr, batch, table]:
        # non-boolean dtype
        mask = pa.array([0, 1, 0, 1, 0])
        with pytest.raises(TypeError, match="must be of boolean type"):
            obj.filter(mask)

        # wrong length
        mask = pa.array([True, False, True])
        with pytest.raises(ValueError, match="must have identical lengths"):
            obj.filter(mask)
