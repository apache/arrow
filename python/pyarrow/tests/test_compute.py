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

import numpy as np
import pytest

import pyarrow as pa


@pytest.mark.parametrize('arrow_type', [
    pa.int8(),
    pa.int16(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint64(),
    pa.float32(),
    pa.float64()
])
def test_sum(arrow_type):
    arr = pa.array([1, 2, 3, 4], type=arrow_type)
    assert arr.sum() == 10


@pytest.mark.parametrize(('ty', 'values'), [
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
    (pa.struct([('a', pa.int8()), ('b', pa.int8())]), [
     {'a': 1, 'b': 2}, None, {'a': 3, 'b': 4}, None, {'a': 5, 'b': 6}]),
])
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
