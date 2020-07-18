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


from pyarrow._compute import (  # noqa
    FilterOptions,
    Function,
    FunctionRegistry,
    function_registry,
    call_function,
    TakeOptions
)

import pyarrow as pa
import pyarrow._compute as _pc


def cast(arr, target_type, safe=True):
    """
    Cast array values to another data type. Can also be invoked as an array
    instance method.

    Parameters
    ----------
    arr : Array or ChunkedArray
    target_type : DataType or type string alias
        Type to cast to
    safe : bool, default True
        Check for overflows or other unsafe conversions

    Examples
    --------
    >>> from datetime import datetime
    >>> import pyarrow as pa
    >>> arr = pa.array([datetime(2010, 1, 1), datetime(2015, 1, 1)])
    >>> arr.type
    TimestampType(timestamp[us])

    You can use ``pyarrow.DataType`` objects to specify the target type:

    >>> cast(arr, pa.timestamp('ms'))
    <pyarrow.lib.TimestampArray object at 0x7fe93c0f6910>
    [
      2010-01-01 00:00:00.000,
      2015-01-01 00:00:00.000
    ]

    >>> cast(arr, pa.timestamp('ms')).type
    TimestampType(timestamp[ms])

    Alternatively, it is also supported to use the string aliases for these
    types:

    >>> arr.cast('timestamp[ms]')
    <pyarrow.lib.TimestampArray object at 0x10420eb88>
    [
      1262304000000,
      1420070400000
    ]
    >>> arr.cast('timestamp[ms]').type
    TimestampType(timestamp[ms])

    Returns
    -------
    casted : Array
    """
    if target_type is None:
        raise ValueError("Cast target type must not be None")
    if safe:
        options = _pc.CastOptions.safe(target_type)
    else:
        options = _pc.CastOptions.unsafe(target_type)
    return call_function("cast", [arr], options)


def _decorate_compute_function(func, name, *, arity):
    func.__arrow_compute_function__ = dict(name=name, arity=arity)
    return func


def _simple_unary_function(name):
    def func(arg):
        return call_function(name, [arg])
    return _decorate_compute_function(func, name, arity=1)


def _simple_binary_function(name):
    def func(left, right):
        return call_function(name, [left, right])
    return _decorate_compute_function(func, name, arity=2)


binary_length = _simple_unary_function('binary_length')
ascii_upper = _simple_unary_function('ascii_upper')
ascii_lower = _simple_unary_function('ascii_lower')
utf8_upper = _simple_unary_function('utf8_upper')
utf8_lower = _simple_unary_function('utf8_lower')

string_is_ascii = _simple_unary_function('string_is_ascii')

ascii_is_alnum = _simple_unary_function('ascii_is_alnum')
utf8_is_alnum = _simple_unary_function('utf8_is_alnum')
ascii_is_alpha = _simple_unary_function('ascii_is_alpha')
utf8_is_alpha = _simple_unary_function('utf8_is_alpha')
ascii_is_decimal = _simple_unary_function('ascii_is_decimal')
utf8_is_decimal = _simple_unary_function('utf8_is_decimal')
ascii_is_digit = ascii_is_decimal  # alias
utf8_is_digit = _simple_unary_function('utf8_is_digit')
ascii_is_lower = _simple_unary_function('ascii_is_lower')
utf8_is_lower = _simple_unary_function('utf8_is_lower')
ascii_is_numeric = ascii_is_decimal  # alias
utf8_is_numeric = _simple_unary_function('utf8_is_numeric')
ascii_is_printable = _simple_unary_function('ascii_is_printable')
utf8_is_printable = _simple_unary_function('utf8_is_printable')
ascii_is_title = _simple_unary_function('ascii_is_title')
utf8_is_title = _simple_unary_function('utf8_is_title')
ascii_is_upper = _simple_unary_function('ascii_is_upper')
utf8_is_upper = _simple_unary_function('utf8_is_upper')

is_valid = _simple_unary_function('is_valid')
is_null = _simple_unary_function('is_null')

list_flatten = _simple_unary_function('list_flatten')
list_parent_indices = _simple_unary_function('list_parent_indices')
list_value_length = _simple_unary_function('list_value_length')

add = _simple_binary_function('add')
subtract = _simple_binary_function('subtract')
multiply = _simple_binary_function('multiply')

equal = _simple_binary_function('equal')
not_equal = _simple_binary_function('not_equal')
greater = _simple_binary_function('greater')
greater_equal = _simple_binary_function('greater_equal')
less = _simple_binary_function('less')
less_equal = _simple_binary_function('less_equal')


def match_substring(array, pattern):
    """
    Test if substring *pattern* is contained within a value of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("match_substring", [array],
                         _pc.MatchSubstringOptions(pattern))


def sum(array):
    """
    Sum the values in a numerical (chunked) array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray

    Returns
    -------
    sum : pyarrow.Scalar
    """
    return call_function('sum', [array])


def filter(data, mask, null_selection_behavior='drop'):
    """
    Select values (or records) from array- or table-like data given boolean
    filter, where true values are selected.

    Parameters
    ----------
    data : Array, ChunkedArray, RecordBatch, or Table
    mask : Array, ChunkedArray
        Must be of boolean type
    null_selection_behavior : str, default 'drop'
        Configure the behavior on encountering a null slot in the mask.
        Allowed values are 'drop' and 'emit_null'.

        - 'drop': nulls will be treated as equivalent to False.
        - 'emit_null': nulls will result in a null in the output.

    Returns
    -------
    result : depends on inputs

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array(["a", "b", "c", None, "e"])
    >>> mask = pa.array([True, False, None, False, True])
    >>> arr.filter(mask)
    <pyarrow.lib.StringArray object at 0x7fa826df9200>
    [
      "a",
      "e"
    ]
    >>> arr.filter(mask, null_selection_behavior='emit_null')
    <pyarrow.lib.StringArray object at 0x7fa826df9200>
    [
      "a",
      null,
      "e"
    ]
    """
    options = FilterOptions(null_selection_behavior)
    return call_function('filter', [data, mask], options)


def take(data, indices, boundscheck=True):
    """
    Select values (or records) from array- or table-like data given integer
    selection indices.

    The result will be of the same type(s) as the input, with elements taken
    from the input array (or record batch / table fields) at the given
    indices. If an index is null then the corresponding value in the output
    will be null.

    Parameters
    ----------
    data : Array, ChunkedArray, RecordBatch, or Table
    indices : Array, ChunkedArray
        Must be of integer type
    boundscheck : boolean, default True
        Whether to boundscheck the indices. If False and there is an out of
        bounds index, will likely cause the process to crash.

    Returns
    -------
    result : depends on inputs

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array(["a", "b", "c", None, "e", "f"])
    >>> indices = pa.array([0, None, 4, 3])
    >>> arr.take(indices)
    <pyarrow.lib.StringArray object at 0x7ffa4fc7d368>
    [
      "a",
      null,
      "e",
      null
    ]
    """
    options = TakeOptions(boundscheck)
    return call_function('take', [data, indices], options)


def fill_null(values, fill_value):
    """
    Replace each null element in values with fill_value. The fill_value must be
    the same type as values or able to be implicitly casted to the array's
    type.

    Parameters
    ----------
    data : Array, ChunkedArray
        replace each null element with fill_value
    fill_value: Scalar-like object
        Either a pyarrow.Scalar or any python object coercible to a
        Scalar. If not same type as data will attempt to cast.

    Returns
    -------
    result : depends on inputs

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array([1, 2, None, 3], type=pa.int8())
    >>> fill_value = pa.scalar(5, type=pa.int8())
    >>> arr.fill_null(fill_value)
    pyarrow.lib.Int8Array object at 0x7f95437f01a0>
    [
      1,
      2,
      5,
      3
    ]
    """
    if not isinstance(fill_value, pa.Scalar):
        fill_value = pa.scalar(fill_value, type=values.type)
    elif values.type != fill_value.type:
        fill_value = pa.scalar(fill_value.as_py(), type=values.type)

    return call_function("fill_null", [values, fill_value])
