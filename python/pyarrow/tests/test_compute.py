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
import pyarrow.compute as pc


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
    assert arr.sum().as_py() == 10
    assert pc.sum(arr).as_py() == 10

    arr = pa.array([], type=arrow_type)
    assert arr.sum().as_py() is None  # noqa: E711


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_sum_chunked_array(arrow_type):
    arr = pa.chunked_array([pa.array([1, 2, 3, 4], type=arrow_type)])
    assert pc.sum(arr).as_py() == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type), pa.array([3, 4], type=arrow_type)
    ])
    assert pc.sum(arr).as_py() == 10

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type),
        pa.array([], type=arrow_type),
        pa.array([3, 4], type=arrow_type)
    ])
    assert pc.sum(arr).as_py() == 10

    arr = pa.chunked_array((), type=arrow_type)
    assert arr.num_chunks == 0
    assert pc.sum(arr).as_py() is None  # noqa: E711


def test_binary_contains_exact():
    arr = pa.array(["ab", "abc", "ba", None])
    result = pc.binary_contains_exact(arr, "ab")
    expected = pa.array([True, True, False, None])
    assert expected.equals(result)



# utf8proc claims 0x8be is UTF8PROC_CATEGORY_LO
utf8proc_issue_isalpha =  {0x8be, 0x8bf, 0x8c0, 0x8c1, 0x8c2, 0x8c3, 0x8c4, 0x8c5, 0x8c6, 0x8c7, 0xd04, 0xe86, 0xe89, 0xe8c, 0xe8e, 0xe8f, 0xe90, 0xe91, 0xe92, 0xe93, 0xe98, 0xea0, 0xea8, 0xea9, 0xeac, 0x1cf2, 0x1cf3, 0x1cfa, 0x31bb, 0x31bc, 0x31bd, 0x31be, 0x31bf, 0x4db6, 0x4db7, 0x4db8, 0x4db9, 0x4dba, 0x4dbb, 0x4dbc, 0x4dbd, 0x4dbe, 0x4dbf, 0x9ff0, 0x9ff1, 0x9ff2, 0x9ff3, 0x9ff4, 0x9ff5, 0x9ff6, 0x9ff7, 0x9ff8, 0x9ff9, 0x9ffa, 0x9ffb, 0x9ffc, 0xa7ba, 0xa7bb, 0xa7bc, 0xa7bd, 0xa7be, 0xa7bf, 0xa7c2, 0xa7c3, 0xa7c4, 0xa7c5, 0xa7c6, 0xa7c7, 0xa7c8, 0xa7c9, 0xa7ca, 0xa7f5, 0xa7f6, 0xab66, 0xab67, 0xab68, 0xab69, 0x10e80, 0x10e81, 0x10e82, 0x10e83, 0x10e84, 0x10e85, 0x10e86, 0x10e87, 0x10e88, 0x10e89, 0x10e8a, 0x10e8b, 0x10e8c, 0x10e8d, 0x10e8e, 0x10e8f, 0x10e90, 0x10e91, 0x10e92, 0x10e93, 0x10e94, 0x10e95, 0x10e96, 0x10e97, 0x10e98, 0x10e99, 0x10e9a, 0x10e9b, 0x10e9c, 0x10e9d, 0x10e9e, 0x10e9f, 0x10ea0, 0x10ea1, 0x10ea2, 0x10ea3, 0x10ea4, 0x10ea5, 0x10ea6, 0x10ea7, 0x10ea8, 0x10ea9, 0x10eb0, 0x10eb1, 0x10fb0, 0x10fb1, 0x10fb2, 0x10fb3, 0x10fb4, 0x10fb5, 0x10fb6, 0x10fb7, 0x10fb8, 0x10fb9, 0x10fba, 0x10fbb, 0x10fbc, 0x10fbd, 0x10fbe, 0x10fbf, 0x10fc0, 0x10fc1, 0x10fc2, 0x10fc3, 0x10fc4, 0x10fe0, 0x10fe1, 0x10fe2, 0x10fe3, 0x10fe4, 0x10fe5, 0x10fe6, 0x10fe7, 0x10fe8, 0x10fe9, 0x10fea, 0x10feb, 0x10fec, 0x10fed, 0x10fee, 0x10fef, 0x10ff0, 0x10ff1, 0x10ff2, 0x10ff3, 0x10ff4, 0x10ff5, 0x10ff6, }
# utf8proc claims these are upper case, they are not
utf8proc_issue_isupper = {0xa7ba, 0xa7bc, 0xa7be, 0xa7c2, 0xa7c4, 0xa7c5, 0xa7c6, 0xa7c7, 0xa7c9, 0xa7f5, }
# utf8proc misses quite a few, and does some false claims?
utf8proc_issue_islower = {0xaa, 0xba, 0x2b0, 0x2b1, 0x2b2, 0x2b3, 0x2b4, 0x2b5, 0x2b6, 0x2b7, 0x2b8, 0x2c0, 0x2c1, 0x2e0, 0x2e1, 0x2e2, 0x2e3, 0x2e4, 0x345, 0x37a, 0x1d2c, 0x1d2d, 0x1d2e, 0x1d2f, 0x1d30, 0x1d31, 0x1d32, 0x1d33, 0x1d34, 0x1d35, 0x1d36, 0x1d37, 0x1d38, 0x1d39, 0x1d3a, 0x1d3b, 0x1d3c, 0x1d3d, 0x1d3e, 0x1d3f, 0x1d40, 0x1d41, 0x1d42, 0x1d43, 0x1d44, 0x1d45, 0x1d46, 0x1d47, 0x1d48, 0x1d49, 0x1d4a, 0x1d4b, 0x1d4c, 0x1d4d, 0x1d4e, 0x1d4f, 0x1d50, 0x1d51, 0x1d52, 0x1d53, 0x1d54, 0x1d55, 0x1d56, 0x1d57, 0x1d58, 0x1d59, 0x1d5a, 0x1d5b, 0x1d5c, 0x1d5d, 0x1d5e, 0x1d5f, 0x1d60, 0x1d61, 0x1d62, 0x1d63, 0x1d64, 0x1d65, 0x1d66, 0x1d67, 0x1d68, 0x1d69, 0x1d6a, 0x1d78, 0x1d9b, 0x1d9c, 0x1d9d, 0x1d9e, 0x1d9f, 0x1da0, 0x1da1, 0x1da2, 0x1da3, 0x1da4, 0x1da5, 0x1da6, 0x1da7, 0x1da8, 0x1da9, 0x1daa, 0x1dab, 0x1dac, 0x1dad, 0x1dae, 0x1daf, 0x1db0, 0x1db1, 0x1db2, 0x1db3, 0x1db4, 0x1db5, 0x1db6, 0x1db7, 0x1db8, 0x1db9, 0x1dba, 0x1dbb, 0x1dbc, 0x1dbd, 0x1dbe, 0x1dbf, 0x2071, 0x207f, 0x2090, 0x2091, 0x2092, 0x2093, 0x2094, 0x2095, 0x2096, 0x2097, 0x2098, 0x2099, 0x209a, 0x209b, 0x209c, 0x2170, 0x2171, 0x2172, 0x2173, 0x2174, 0x2175, 0x2176, 0x2177, 0x2178, 0x2179, 0x217a, 0x217b, 0x217c, 0x217d, 0x217e, 0x217f, 0x24d0, 0x24d1, 0x24d2, 0x24d3, 0x24d4, 0x24d5, 0x24d6, 0x24d7, 0x24d8, 0x24d9, 0x24da, 0x24db, 0x24dc, 0x24dd, 0x24de, 0x24df, 0x24e0, 0x24e1, 0x24e2, 0x24e3, 0x24e4, 0x24e5, 0x24e6, 0x24e7, 0x24e8, 0x24e9, 0x2c7c, 0x2c7d, 0xa69c, 0xa69d, 0xa770, 0xa7bb, 0xa7bd, 0xa7bf, 0xa7c3, 0xa7c8, 0xa7ca, 0xa7f6, 0xa7f8, 0xa7f9, 0xab5c, 0xab5d, 0xab5e, 0xab5f, 0xab66, 0xab67, 0xab68, }
# utf8proc claims 0x8be is UTF8PROC_CATEGORY_LO
utf8proc_issue_isprintable = {0x8be, 0x8bf, 0x8c0, 0x8c1, 0x8c2, 0x8c3, 0x8c4, 0x8c5, 0x8c6, 0x8c7, 0xb55, 0xc77, 0xd04, 0xd81, 0xe86, 0xe89, 0xe8c, 0xe8e, 0xe8f, 0xe90, 0xe91, 0xe92, 0xe93, 0xe98, 0xea0, 0xea8, 0xea9, 0xeac, 0xeba, 0x1abf, 0x1ac0, 0x1cfa, 0x2b97, 0x2bc9, 0x2bff, 0x2e4f, 0x2e50, 0x2e51, 0x2e52, 0x31bb, 0x31bc, 0x31bd, 0x31be, 0x31bf, 0x32ff, 0x4db6, 0x4db7, 0x4db8, 0x4db9, 0x4dba, 0x4dbb, 0x4dbc, 0x4dbd, 0x4dbe, 0x4dbf, 0x9ff0, 0x9ff1, 0x9ff2, 0x9ff3, 0x9ff4, 0x9ff5, 0x9ff6, 0x9ff7, 0x9ff8, 0x9ff9, 0x9ffa, 0x9ffb, 0x9ffc, 0xa7ba, 0xa7bb, 0xa7bc, 0xa7bd, 0xa7be, 0xa7bf, 0xa7c2, 0xa7c3, 0xa7c4, 0xa7c5, 0xa7c6, 0xa7c7, 0xa7c8, 0xa7c9, 0xa7ca, 0xa7f5, 0xa7f6, 0xa82c, 0xab66, 0xab67, 0xab68, 0xab69, 0xab6a, 0xab6b, 0x1019c, 0x10e80, 0x10e81, 0x10e82, 0x10e83, 0x10e84, 0x10e85, 0x10e86, 0x10e87, 0x10e88, 0x10e89, 0x10e8a, 0x10e8b, 0x10e8c, 0x10e8d, 0x10e8e, 0x10e8f, 0x10e90, 0x10e91, 0x10e92, 0x10e93, 0x10e94, 0x10e95, 0x10e96, 0x10e97, 0x10e98, 0x10e99, 0x10e9a, 0x10e9b, 0x10e9c, 0x10e9d, 0x10e9e, 0x10e9f, 0x10ea0, 0x10ea1, 0x10ea2, 0x10ea3, 0x10ea4, 0x10ea5, 0x10ea6, 0x10ea7, 0x10ea8, 0x10ea9, 0x10eab, 0x10eac, 0x10ead, 0x10eb0, 0x10eb1, 0x10fb0, 0x10fb1, 0x10fb2, 0x10fb3, 0x10fb4, 0x10fb5, 0x10fb6, 0x10fb7, 0x10fb8, 0x10fb9, 0x10fba, 0x10fbb, 0x10fbc, 0x10fbd, 0x10fbe, 0x10fbf, 0x10fc0, 0x10fc1, 0x10fc2, 0x10fc3, 0x10fc4, 0x10fc5, 0x10fc6, 0x10fc7, 0x10fc8, 0x10fc9, 0x10fca, 0x10fcb, 0x10fe0, 0x10fe1, 0x10fe2, 0x10fe3, 0x10fe4, 0x10fe5, 0x10fe6, 0x10fe7, 0x10fe8, 0x10fe9, 0x10fea, 0x10feb, 0x10fec, 0x10fed, 0x10fee, 0x10fef, 0x10ff0, 0x10ff1, 0x10ff2, 0x10ff3, 0x10ff4, 0x10ff5, 0x10ff6}
# utf8proc does not store if a codepoint is numeric
numeric_info_missing = {0x3405, 0x3483, 0x382a, 0x3b4d, 0x4e00, 0x4e03, 0x4e07, 0x4e09, 0x4e5d, 0x4e8c, 0x4e94, 0x4e96, 0x4ebf, 0x4ec0, 0x4edf, 0x4ee8, 0x4f0d, 0x4f70, 0x5104, 0x5146, 0x5169, 0x516b, 0x516d, 0x5341, 0x5343, 0x5344, 0x5345, 0x534c, 0x53c1, 0x53c2, 0x53c3, 0x53c4, 0x56db, 0x58f1, 0x58f9, 0x5e7a, 0x5efe, 0x5eff, 0x5f0c, 0x5f0d, 0x5f0e, 0x5f10, 0x62fe, 0x634c, 0x67d2, 0x6f06, 0x7396, 0x767e, 0x8086, 0x842c, 0x8cae, 0x8cb3, 0x8d30, 0x9621, 0x9646, 0x964c, 0x9678, 0x96f6, 0xf96b, 0xf973, 0xf978, 0xf9b2, 0xf9d1, 0xf9d3, 0xf9fd, 0x10fc5, 0x10fc6, 0x10fc7, 0x10fc8, 0x10fc9, 0x10fca, 0x10fcb, }
# utf8proc has no no digit information
digit_info_missing = {0xb2, 0xb3, 0xb9, 0x1369, 0x136a, 0x136b, 0x136c, 0x136d, 0x136e, 0x136f, 0x1370, 0x1371, 0x19da, 0x2070, 0x2074, 0x2075, 0x2076, 0x2077, 0x2078, 0x2079, 0x2080, 0x2081, 0x2082, 0x2083, 0x2084, 0x2085, 0x2086, 0x2087, 0x2088, 0x2089, 0x2460, 0x2461, 0x2462, 0x2463, 0x2464, 0x2465, 0x2466, 0x2467, 0x2468, 0x2474, 0x2475, 0x2476, 0x2477, 0x2478, 0x2479, 0x247a, 0x247b, 0x247c, 0x2488, 0x2489, 0x248a, 0x248b, 0x248c, 0x248d, 0x248e, 0x248f, 0x2490, 0x24ea, 0x24f5, 0x24f6, 0x24f7, 0x24f8, 0x24f9, 0x24fa, 0x24fb, 0x24fc, 0x24fd, 0x24ff, 0x2776, 0x2777, 0x2778, 0x2779, 0x277a, 0x277b, 0x277c, 0x277d, 0x277e, 0x2780, 0x2781, 0x2782, 0x2783, 0x2784, 0x2785, 0x2786, 0x2787, 0x2788, 0x278a, 0x278b, 0x278c, 0x278d, 0x278e, 0x278f, 0x2790, 0x2791, 0x2792, 0x10a40, 0x10a41, 0x10a42, 0x10a43, 0x10e60, 0x10e61, 0x10e62, 0x10e63, 0x10e64, 0x10e65, 0x10e66, 0x10e67, 0x10e68}

codepoints_ignore = {
    'isalnum': numeric_info_missing | digit_info_missing | utf8proc_issue_isalpha,
    'isalpha': utf8proc_issue_isalpha,
    'isdigit': digit_info_missing,
    'isupper': utf8proc_issue_isupper,
    'isprintable': utf8proc_issue_isprintable,
    'isnumeric': numeric_info_missing,
    'islower': utf8proc_issue_islower
}

@pytest.mark.parametrize('function_name', ['isalnum', 'isalpha', 'isascii', 'isdecimal', 'isdigit', 'islower', 'isnumeric', 'isprintable', 'isspace', 'isupper',])
@pytest.mark.parametrize('ascii', [False, True])
def test_string_py_compat_boolean(function_name, ascii):
    variant = 'ascii' if ascii else 'unicode'
    arrow_name = f'string_{function_name}_{variant}'
    py_name = function_name
    for i in range(128 if ascii else 0x11000):
        if i in range(0xD800, 0xE000):
            continue  # bug? pyarrow doesn't allow utf16 surrogates
        # the issues we know of, we skip
        if i in codepoints_ignore.get(function_name, []):
            continue
        c = chr(i)
        if hasattr(pc, arrow_name):
            ar = pa.array([c])
            cpython_value = getattr(c, py_name)()
            arrow_value = getattr(pc, arrow_name)(ar)[0]
            assert arrow_value == cpython_value


@pytest.mark.parametrize(('ty', 'values'), all_array_types)
def test_take(ty, values):
    arr = pa.array(values, type=ty)
    for indices_type in [pa.int8(), pa.int64()]:
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
        with pytest.raises(NotImplementedError):
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
    with pytest.raises(NotImplementedError):
        arr.filter(mask)

    # wrong length
    mask = pa.array([True, False, True])
    with pytest.raises(ValueError, match="must all be the same length"):
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
        with pytest.raises(NotImplementedError):
            obj.filter(mask)

        # wrong length
        mask = pa.array([True, False, True])
        with pytest.raises(pa.ArrowInvalid,
                           match="must all be the same length"):
            obj.filter(mask)


@pytest.mark.parametrize("typ", ["array", "chunked_array"])
def test_compare_array(typ):
    if typ == "array":
        def con(values): return pa.array(values)
    else:
        def con(values): return pa.chunked_array([values])

    arr1 = con([1, 2, 3, 4, None])
    arr2 = con([1, 1, 4, None, 4])

    result = arr1 == arr2
    assert result.equals(con([True, False, False, None, None]))

    result = arr1 != arr2
    assert result.equals(con([False, True, True, None, None]))

    result = arr1 < arr2
    assert result.equals(con([False, False, True, None, None]))

    result = arr1 <= arr2
    assert result.equals(con([True, False, True, None, None]))

    result = arr1 > arr2
    assert result.equals(con([False, True, False, None, None]))

    result = arr1 >= arr2
    assert result.equals(con([True, True, False, None, None]))


@pytest.mark.parametrize("typ", ["array", "chunked_array"])
def test_compare_scalar(typ):
    if typ == "array":
        def con(values): return pa.array(values)
    else:
        def con(values): return pa.chunked_array([values])

    arr = con([1, 2, 3, None])
    # TODO this is a hacky way to construct a scalar ..
    scalar = pa.array([2]).sum()

    result = arr == scalar
    assert result.equals(con([False, True, False, None]))

    result = arr != scalar
    assert result.equals(con([True, False, True, None]))

    result = arr < scalar
    assert result.equals(con([True, False, False, None]))

    result = arr <= scalar
    assert result.equals(con([True, True, False, None]))

    result = arr > scalar
    assert result.equals(con([False, False, True, None]))

    result = arr >= scalar
    assert result.equals(con([False, True, True, None]))


def test_compare_chunked_array_mixed():
    arr = pa.array([1, 2, 3, 4, None])
    arr_chunked = pa.chunked_array([[1, 2, 3], [4, None]])
    arr_chunked2 = pa.chunked_array([[1, 2], [3, 4, None]])

    expected = pa.chunked_array([[True, True, True, True, None]])

    for result in [
        arr == arr_chunked,
        arr_chunked == arr,
        arr_chunked == arr_chunked2,
    ]:
        assert result.equals(expected)


def test_arithmetic_add():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.add(left, right)
    expected = pa.array([1, 1, 4, 6, 8])
    assert result.equals(expected)


def test_arithmetic_subtract():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.subtract(left, right)
    expected = pa.array([1, 3, 2, 2, 2])
    assert result.equals(expected)


def test_arithmetic_multiply():
    left = pa.array([1, 2, 3, 4, 5])
    right = pa.array([0, -1, 1, 2, 3])
    result = pc.multiply(left, right)
    expected = pa.array([0, -2, 3, 8, 15])
    assert result.equals(expected)


def test_is_null():
    arr = pa.array([1, 2, 3, None])
    result = arr.is_null()
    result = arr.is_null()
    expected = pa.array([False, False, False, True])
    assert result.equals(expected)
    assert result.equals(pc.is_null(arr))
    result = arr.is_valid()
    expected = pa.array([True, True, True, False])
    assert result.equals(expected)
    assert result.equals(pc.is_valid(arr))

    arr = pa.chunked_array([[1, 2], [3, None]])
    result = arr.is_null()
    expected = pa.chunked_array([[False, False], [False, True]])
    assert result.equals(expected)
    result = arr.is_valid()
    expected = pa.chunked_array([[True, True], [True, False]])
    assert result.equals(expected)
