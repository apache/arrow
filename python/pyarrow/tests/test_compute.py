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

from datetime import datetime
from functools import lru_cache
import pickle
import pytest
import textwrap

import numpy as np

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


exported_functions = [
    func for (name, func) in sorted(pc.__dict__.items())
    if hasattr(func, '__arrow_compute_function__')]


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


def test_exported_functions():
    # Check that all exported concrete functions can be called with
    # the right number of arguments.
    # Note that unregistered functions (e.g. with a mismatching name)
    # will raise KeyError.
    functions = exported_functions
    assert len(functions) >= 10
    for func in functions:
        args = [object()] * func.__arrow_compute_function__['arity']
        with pytest.raises(TypeError,
                           match="Got unexpected argument type "
                                 "<class 'object'> for compute function"):
            func(*args)


def test_list_functions():
    assert len(pc.list_functions()) > 10
    assert "add" in pc.list_functions()


def _check_get_function(name, expected_func_cls, expected_ker_cls,
                        min_num_kernels=1):
    func = pc.get_function(name)
    assert isinstance(func, expected_func_cls)
    n = func.num_kernels
    assert n >= min_num_kernels
    assert n == len(func.kernels)
    assert all(isinstance(ker, expected_ker_cls) for ker in func.kernels)


def test_get_function_scalar():
    _check_get_function("add", pc.ScalarFunction, pc.ScalarKernel, 8)


def test_get_function_vector():
    _check_get_function("unique", pc.VectorFunction, pc.VectorKernel, 8)


def test_get_function_aggregate():
    _check_get_function("mean", pc.ScalarAggregateFunction,
                        pc.ScalarAggregateKernel, 8)


def test_call_function_with_memory_pool():
    arr = pa.array(["foo", "bar", "baz"])
    indices = np.array([2, 2, 1])
    result1 = arr.take(indices)
    result2 = pc.call_function('take', [arr, indices],
                               memory_pool=pa.default_memory_pool())
    expected = pa.array(["baz", "baz", "bar"])
    assert result1.equals(expected)
    assert result2.equals(expected)

    result3 = pc.take(arr, indices, memory_pool=pa.default_memory_pool())
    assert result3.equals(expected)


def test_pickle_functions():
    # Pickle registered functions
    for name in pc.list_functions():
        func = pc.get_function(name)
        reconstructed = pickle.loads(pickle.dumps(func))
        assert type(reconstructed) is type(func)
        assert reconstructed.name == func.name
        assert reconstructed.arity == func.arity
        assert reconstructed.num_kernels == func.num_kernels


def test_pickle_global_functions():
    # Pickle global wrappers (manual or automatic) of registered functions
    for name in pc.list_functions():
        func = getattr(pc, name)
        reconstructed = pickle.loads(pickle.dumps(func))
        assert reconstructed is func


def test_function_attributes():
    # Sanity check attributes of registered functions
    for name in pc.list_functions():
        func = pc.get_function(name)
        assert isinstance(func, pc.Function)
        assert func.name == name
        kernels = func.kernels
        assert func.num_kernels == len(kernels)
        assert all(isinstance(ker, pc.Kernel) for ker in kernels)
        assert func.arity >= 1  # no varargs functions for now
        repr(func)
        for ker in kernels:
            repr(ker)


def test_input_type_conversion():
    # Automatic array conversion from Python
    arr = pc.add([1, 2], [4, None])
    assert arr.to_pylist() == [5, None]
    # Automatic scalar conversion from Python
    arr = pc.add([1, 2], 4)
    assert arr.to_pylist() == [5, 6]
    # Other scalar type
    assert pc.equal(["foo", "bar", None],
                    "foo").to_pylist() == [True, False, None]


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


def test_mode_array():
    # ARROW-9917
    arr = pa.array([1, 1, 3, 4, 3, 5], type='int64')
    expected = {"mode": 1, "count": 2}
    assert pc.mode(arr).as_py() == {"mode": 1, "count": 2}

    arr = pa.array([], type='int64')
    expected = {"mode": None, "count": None}
    assert pc.mode(arr).as_py() == expected


def test_mode_chunked_array():
    # ARROW-9917
    arr = pa.chunked_array([pa.array([1, 1, 3, 4, 3, 5], type='int64')])
    expected = {"mode": 1, "count": 2}
    assert pc.mode(arr).as_py() == expected

    arr = pa.chunked_array((), type='int64')
    expected = {"mode": None, "count": None}
    assert arr.num_chunks == 0
    assert pc.mode(arr).as_py() == expected


def test_variance():
    data = [1, 2, 3, 4, 5, 6, 7, 8]
    assert pc.variance(data).as_py() == 5.25
    assert pc.variance(data, ddof=0).as_py() == 5.25
    assert pc.variance(data, ddof=1).as_py() == 6.0


def test_match_substring():
    arr = pa.array(["ab", "abc", "ba", None])
    result = pc.match_substring(arr, "ab")
    expected = pa.array([True, True, False, None])
    assert expected.equals(result)


def test_min_max():
    # An example generated function wrapper with possible options
    data = [4, 5, 6, None, 1]
    s = pc.min_max(data)
    assert s.as_py() == {'min': 1, 'max': 6}
    s = pc.min_max(data, options=pc.MinMaxOptions())
    assert s.as_py() == {'min': 1, 'max': 6}
    s = pc.min_max(data, options=pc.MinMaxOptions(null_handling='skip'))
    assert s.as_py() == {'min': 1, 'max': 6}
    s = pc.min_max(data, options=pc.MinMaxOptions(null_handling='emit_null'))
    assert s.as_py() == {'min': None, 'max': None}

    # Options as dict of kwargs
    s = pc.min_max(data, options={'null_handling': 'emit_null'})
    assert s.as_py() == {'min': None, 'max': None}
    # Options as named functions arguments
    s = pc.min_max(data, null_handling='emit_null')
    assert s.as_py() == {'min': None, 'max': None}

    # Both options and named arguments
    with pytest.raises(TypeError):
        s = pc.min_max(data, options=pc.MinMaxOptions(),
                       null_handling='emit_null')

    # Wrong options type
    options = pc.TakeOptions()
    with pytest.raises(TypeError):
        s = pc.min_max(data, options=options)


def test_is_valid():
    # An example generated function wrapper without options
    data = [4, 5, None]
    assert pc.is_valid(data).to_pylist() == [True, True, False]

    with pytest.raises(TypeError):
        pc.is_valid(data, options=None)


def test_generated_docstrings():
    assert pc.min_max.__doc__ == textwrap.dedent("""\
        Call compute function 'min_max' with the given argument.

        Parameters
        ----------
        arg : Array-like or scalar-like
            Argument to compute function
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the default memory pool.
        options : pyarrow.compute.MinMaxOptions, optional
            Parameters altering compute function semantics
        **kwargs: optional
            Parameters for MinMaxOptions constructor.  Either `options`
            or `**kwargs` can be passed, but not both at the same time.
        """)
    assert pc.add.__doc__ == textwrap.dedent("""\
        Call compute function 'add' with the given arguments.

        Parameters
        ----------
        left : Array-like or scalar-like
            First argument to compute function
        right : Array-like or scalar-like
            Second argument to compute function
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the default memory pool.
        """)


# We use isprintable to find about codepoints that Python doesn't know, but
# utf8proc does (or in a future version of Python the other way around).
# These codepoints cannot be compared between Arrow and the Python
# implementation.
@lru_cache()
def find_new_unicode_codepoints():
    new = set()
    characters = [chr(c) for c in range(0x80, 0x11000)
                  if not (0xD800 <= c < 0xE000)]
    is_printable = pc.utf8_is_printable(pa.array(characters)).to_pylist()
    for i, c in enumerate(characters):
        if is_printable[i] != c.isprintable():
            new.add(ord(c))
    return new


# Python claims there are not alpha, not sure why, they are in
#  gc='Other Letter': https://graphemica.com/%E1%B3%B2
unknown_issue_is_alpha = {0x1cf2, 0x1cf3}
# utf8proc does not know if codepoints are lower case
utf8proc_issue_is_lower = {
    0xaa, 0xba, 0x2b0, 0x2b1, 0x2b2, 0x2b3, 0x2b4,
    0x2b5, 0x2b6, 0x2b7, 0x2b8, 0x2c0, 0x2c1, 0x2e0,
    0x2e1, 0x2e2, 0x2e3, 0x2e4, 0x37a, 0x1d2c, 0x1d2d,
    0x1d2e, 0x1d2f, 0x1d30, 0x1d31, 0x1d32, 0x1d33,
    0x1d34, 0x1d35, 0x1d36, 0x1d37, 0x1d38, 0x1d39,
    0x1d3a, 0x1d3b, 0x1d3c, 0x1d3d, 0x1d3e, 0x1d3f,
    0x1d40, 0x1d41, 0x1d42, 0x1d43, 0x1d44, 0x1d45,
    0x1d46, 0x1d47, 0x1d48, 0x1d49, 0x1d4a, 0x1d4b,
    0x1d4c, 0x1d4d, 0x1d4e, 0x1d4f, 0x1d50, 0x1d51,
    0x1d52, 0x1d53, 0x1d54, 0x1d55, 0x1d56, 0x1d57,
    0x1d58, 0x1d59, 0x1d5a, 0x1d5b, 0x1d5c, 0x1d5d,
    0x1d5e, 0x1d5f, 0x1d60, 0x1d61, 0x1d62, 0x1d63,
    0x1d64, 0x1d65, 0x1d66, 0x1d67, 0x1d68, 0x1d69,
    0x1d6a, 0x1d78, 0x1d9b, 0x1d9c, 0x1d9d, 0x1d9e,
    0x1d9f, 0x1da0, 0x1da1, 0x1da2, 0x1da3, 0x1da4,
    0x1da5, 0x1da6, 0x1da7, 0x1da8, 0x1da9, 0x1daa,
    0x1dab, 0x1dac, 0x1dad, 0x1dae, 0x1daf, 0x1db0,
    0x1db1, 0x1db2, 0x1db3, 0x1db4, 0x1db5, 0x1db6,
    0x1db7, 0x1db8, 0x1db9, 0x1dba, 0x1dbb, 0x1dbc,
    0x1dbd, 0x1dbe, 0x1dbf, 0x2071, 0x207f, 0x2090,
    0x2091, 0x2092, 0x2093, 0x2094, 0x2095, 0x2096,
    0x2097, 0x2098, 0x2099, 0x209a, 0x209b, 0x209c,
    0x2c7c, 0x2c7d, 0xa69c, 0xa69d, 0xa770, 0xa7f8,
    0xa7f9, 0xab5c, 0xab5d, 0xab5e, 0xab5f, }
# utf8proc does not store if a codepoint is numeric
numeric_info_missing = {
    0x3405, 0x3483, 0x382a, 0x3b4d, 0x4e00, 0x4e03,
    0x4e07, 0x4e09, 0x4e5d, 0x4e8c, 0x4e94, 0x4e96,
    0x4ebf, 0x4ec0, 0x4edf, 0x4ee8, 0x4f0d, 0x4f70,
    0x5104, 0x5146, 0x5169, 0x516b, 0x516d, 0x5341,
    0x5343, 0x5344, 0x5345, 0x534c, 0x53c1, 0x53c2,
    0x53c3, 0x53c4, 0x56db, 0x58f1, 0x58f9, 0x5e7a,
    0x5efe, 0x5eff, 0x5f0c, 0x5f0d, 0x5f0e, 0x5f10,
    0x62fe, 0x634c, 0x67d2, 0x6f06, 0x7396, 0x767e,
    0x8086, 0x842c, 0x8cae, 0x8cb3, 0x8d30, 0x9621,
    0x9646, 0x964c, 0x9678, 0x96f6, 0xf96b, 0xf973,
    0xf978, 0xf9b2, 0xf9d1, 0xf9d3, 0xf9fd, 0x10fc5,
    0x10fc6, 0x10fc7, 0x10fc8, 0x10fc9, 0x10fca,
    0x10fcb, }
# utf8proc has no no digit/numeric information
digit_info_missing = {
    0xb2, 0xb3, 0xb9, 0x1369, 0x136a, 0x136b, 0x136c,
    0x136d, 0x136e, 0x136f, 0x1370, 0x1371, 0x19da, 0x2070,
    0x2074, 0x2075, 0x2076, 0x2077, 0x2078, 0x2079, 0x2080,
    0x2081, 0x2082, 0x2083, 0x2084, 0x2085, 0x2086, 0x2087,
    0x2088, 0x2089, 0x2460, 0x2461, 0x2462, 0x2463, 0x2464,
    0x2465, 0x2466, 0x2467, 0x2468, 0x2474, 0x2475, 0x2476,
    0x2477, 0x2478, 0x2479, 0x247a, 0x247b, 0x247c, 0x2488,
    0x2489, 0x248a, 0x248b, 0x248c, 0x248d, 0x248e, 0x248f,
    0x2490, 0x24ea, 0x24f5, 0x24f6, 0x24f7, 0x24f8, 0x24f9,
    0x24fa, 0x24fb, 0x24fc, 0x24fd, 0x24ff, 0x2776, 0x2777,
    0x2778, 0x2779, 0x277a, 0x277b, 0x277c, 0x277d, 0x277e,
    0x2780, 0x2781, 0x2782, 0x2783, 0x2784, 0x2785, 0x2786,
    0x2787, 0x2788, 0x278a, 0x278b, 0x278c, 0x278d, 0x278e,
    0x278f, 0x2790, 0x2791, 0x2792, 0x10a40, 0x10a41,
    0x10a42, 0x10a43, 0x10e60, 0x10e61, 0x10e62, 0x10e63,
    0x10e64, 0x10e65, 0x10e66, 0x10e67, 0x10e68, }
numeric_info_missing = {
    0x3405, 0x3483, 0x382a, 0x3b4d, 0x4e00, 0x4e03,
    0x4e07, 0x4e09, 0x4e5d, 0x4e8c, 0x4e94, 0x4e96,
    0x4ebf, 0x4ec0, 0x4edf, 0x4ee8, 0x4f0d, 0x4f70,
    0x5104, 0x5146, 0x5169, 0x516b, 0x516d, 0x5341,
    0x5343, 0x5344, 0x5345, 0x534c, 0x53c1, 0x53c2,
    0x53c3, 0x53c4, 0x56db, 0x58f1, 0x58f9, 0x5e7a,
    0x5efe, 0x5eff, 0x5f0c, 0x5f0d, 0x5f0e, 0x5f10,
    0x62fe, 0x634c, 0x67d2, 0x6f06, 0x7396, 0x767e,
    0x8086, 0x842c, 0x8cae, 0x8cb3, 0x8d30, 0x9621,
    0x9646, 0x964c, 0x9678, 0x96f6, 0xf96b, 0xf973,
    0xf978, 0xf9b2, 0xf9d1, 0xf9d3, 0xf9fd, }

codepoints_ignore = {
    'is_alnum': numeric_info_missing | digit_info_missing |
    unknown_issue_is_alpha,
    'is_alpha': unknown_issue_is_alpha,
    'is_digit': digit_info_missing,
    'is_numeric': numeric_info_missing,
    'is_lower': utf8proc_issue_is_lower
}


@pytest.mark.parametrize('function_name', ['is_alnum', 'is_alpha',
                                           'is_ascii', 'is_decimal',
                                           'is_digit', 'is_lower',
                                           'is_numeric', 'is_printable',
                                           'is_space', 'is_upper', ])
@pytest.mark.parametrize('variant', ['ascii', 'utf8'])
def test_string_py_compat_boolean(function_name, variant):
    arrow_name = variant + "_" + function_name
    py_name = function_name.replace('_', '')
    ignore = codepoints_ignore.get(function_name, set()) |\
        find_new_unicode_codepoints()
    for i in range(128 if ascii else 0x11000):
        if i in range(0xD800, 0xE000):
            continue  # bug? pyarrow doesn't allow utf16 surrogates
        # the issues we know of, we skip
        if i in ignore:
            continue
        # Compare results with the equivalent Python predicate
        # (except "is_space" where functions are known to be incompatible)
        c = chr(i)
        if hasattr(pc, arrow_name) and function_name != 'is_space':
            ar = pa.array([c])
            arrow_func = getattr(pc, arrow_name)
            assert arrow_func(ar)[0].as_py() == getattr(c, py_name)()


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


def test_take_on_chunked_array():
    # ARROW-9504
    arr = pa.chunked_array([
        [
            "a",
            "b",
            "c",
            "d",
            "e"
        ],
        [
            "f",
            "g",
            "h",
            "i",
            "j"
        ]
    ])

    indices = np.array([0, 5, 1, 6, 9, 2])
    result = arr.take(indices)
    expected = pa.chunked_array([["a", "f", "b", "g", "j", "c"]])
    assert result.equals(expected)

    indices = pa.chunked_array([[1], [9, 2]])
    result = arr.take(indices)
    expected = pa.chunked_array([
        [
            "b"
        ],
        [
            "j",
            "c"
        ]
    ])
    assert result.equals(expected)


@pytest.mark.parametrize('ordered', [False, True])
def test_take_dictionary(ordered):
    arr = pa.DictionaryArray.from_arrays([0, 1, 2, 0, 1, 2], ['a', 'b', 'c'],
                                         ordered=ordered)
    result = arr.take(pa.array([0, 1, 3]))
    result.validate()
    assert result.to_pylist() == ['a', 'b', 'a']
    assert result.dictionary.to_pylist() == ['a', 'b', 'c']
    assert result.type.ordered is ordered


def test_take_null_type():
    # ARROW-10027
    arr = pa.array([None] * 10)
    chunked_arr = pa.chunked_array([[None] * 5] * 2)
    batch = pa.record_batch([arr], names=['a'])
    table = pa.table({'a': arr})

    indices = pa.array([1, 3, 7, None])
    assert len(arr.take(indices)) == 4
    assert len(chunked_arr.take(indices)) == 4
    assert len(batch.take(indices).column(0)) == 4
    assert len(table.take(indices).column(0)) == 4


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


def test_filter_null_type():
    # ARROW-10027
    arr = pa.array([None] * 10)
    chunked_arr = pa.chunked_array([[None] * 5] * 2)
    batch = pa.record_batch([arr], names=['a'])
    table = pa.table({'a': arr})

    mask = pa.array([True, False] * 5)
    assert len(arr.filter(mask)) == 5
    assert len(chunked_arr.filter(mask)) == 5
    assert len(batch.filter(mask).column(0)) == 5
    assert len(table.filter(mask).column(0)) == 5


@pytest.mark.parametrize("typ", ["array", "chunked_array"])
def test_compare_array(typ):
    if typ == "array":
        def con(values): return pa.array(values)
    else:
        def con(values): return pa.chunked_array([values])

    arr1 = con([1, 2, 3, 4, None])
    arr2 = con([1, 1, 4, None, 4])

    result = pc.equal(arr1, arr2)
    assert result.equals(con([True, False, False, None, None]))

    result = pc.not_equal(arr1, arr2)
    assert result.equals(con([False, True, True, None, None]))

    result = pc.less(arr1, arr2)
    assert result.equals(con([False, False, True, None, None]))

    result = pc.less_equal(arr1, arr2)
    assert result.equals(con([True, False, True, None, None]))

    result = pc.greater(arr1, arr2)
    assert result.equals(con([False, True, False, None, None]))

    result = pc.greater_equal(arr1, arr2)
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

    result = pc.equal(arr, scalar)
    assert result.equals(con([False, True, False, None]))

    result = pc.not_equal(arr, scalar)
    assert result.equals(con([True, False, True, None]))

    result = pc.less(arr, scalar)
    assert result.equals(con([True, False, False, None]))

    result = pc.less_equal(arr, scalar)
    assert result.equals(con([True, True, False, None]))

    result = pc.greater(arr, scalar)
    assert result.equals(con([False, False, True, None]))

    result = pc.greater_equal(arr, scalar)
    assert result.equals(con([False, True, True, None]))


def test_compare_chunked_array_mixed():
    arr = pa.array([1, 2, 3, 4, None])
    arr_chunked = pa.chunked_array([[1, 2, 3], [4, None]])
    arr_chunked2 = pa.chunked_array([[1, 2], [3, 4, None]])

    expected = pa.chunked_array([[True, True, True, True, None]])

    for left, right in [
        (arr, arr_chunked),
        (arr_chunked, arr),
        (arr_chunked, arr_chunked2),
    ]:
        result = pc.equal(left, right)
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


def test_fill_null():
    arr = pa.array([1, 2, None, 4], type=pa.int8())
    fill_value = pa.array([5], type=pa.int8())
    with pytest.raises(pa.ArrowInvalid, match="tried to convert to int"):
        arr.fill_null(fill_value)

    arr = pa.array([None, None, None, None], type=pa.null())
    fill_value = pa.scalar(None, type=pa.null())
    result = arr.fill_null(fill_value)
    expected = pa.array([None, None, None, None])
    assert result.equals(expected)


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_fill_null_array(arrow_type):
    arr = pa.array([1, 2, None, 4], type=arrow_type)
    fill_value = pa.scalar(5, type=arrow_type)
    result = arr.fill_null(fill_value)
    expected = pa.array([1, 2, 5, 4], type=arrow_type)
    assert result.equals(expected)

    # Implicit conversions
    result = arr.fill_null(5)
    assert result.equals(expected)

    # ARROW-9451: Unsigned integers allow this for some reason
    if not pa.types.is_unsigned_integer(arr.type):
        with pytest.raises((ValueError, TypeError)):
            arr.fill_null('5')

    result = arr.fill_null(pa.scalar(5, type='int8'))
    assert result.equals(expected)


@pytest.mark.parametrize('arrow_type', numerical_arrow_types)
def test_fill_null_chunked_array(arrow_type):
    fill_value = pa.scalar(5, type=arrow_type)
    arr = pa.chunked_array([pa.array([None, 2, 3, 4], type=arrow_type)])
    result = arr.fill_null(fill_value)
    expected = pa.chunked_array([pa.array([5, 2, 3, 4], type=arrow_type)])
    assert result.equals(expected)

    arr = pa.chunked_array([
        pa.array([1, 2], type=arrow_type),
        pa.array([], type=arrow_type),
        pa.array([None, 4], type=arrow_type)
    ])
    expected = pa.chunked_array([
        pa.array([1, 2], type=arrow_type),
        pa.array([], type=arrow_type),
        pa.array([5, 4], type=arrow_type)
    ])
    result = arr.fill_null(fill_value)
    assert result.equals(expected)

    # Implicit conversions
    result = arr.fill_null(5)
    assert result.equals(expected)

    result = arr.fill_null(pa.scalar(5, type='int8'))
    assert result.equals(expected)


def test_logical():
    a = pa.array([True, False, False, None])
    b = pa.array([True, True, False, True])

    assert pc.and_(a, b) == pa.array([True, False, False, None])
    assert pc.and_kleene(a, b) == pa.array([True, False, False, None])

    assert pc.or_(a, b) == pa.array([True, True, False, None])
    assert pc.or_kleene(a, b) == pa.array([True, True, False, True])

    assert pc.xor(a, b) == pa.array([False, True, False, None])

    assert pc.invert(a) == pa.array([False, True, True, None])


def test_cast():
    arr = pa.array([2**63 - 1], type='int64')

    with pytest.raises(pa.ArrowInvalid):
        pc.cast(arr, 'int32')

    assert pc.cast(arr, 'int32', safe=False) == pa.array([-1], type='int32')

    arr = pa.array([datetime(2010, 1, 1), datetime(2015, 1, 1)])
    expected = pa.array([1262304000000, 1420070400000], type='timestamp[ms]')
    assert pc.cast(arr, 'timestamp[ms]') == expected
