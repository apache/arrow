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


def _simple_unary_function(name):
    def func(arg):
        return call_function(name, [arg])
    return func


def _simple_binary_function(name):
    def func(left, right):
        return call_function(name, [left, right])
    return func


ascii_length = _simple_unary_function('ascii_length')
ascii_upper = _simple_unary_function('ascii_upper')
ascii_lower = _simple_unary_function('ascii_lower')
utf8_upper = _simple_unary_function('utf8_upper')
utf8_lower = _simple_unary_function('utf8_lower')

is_valid = _simple_unary_function('is_valid')
is_null = _simple_unary_function('is_null')

list_flatten = _simple_unary_function('list_flatten')
list_parent_indices = _simple_unary_function('list_parent_indices')
list_value_lengths = _simple_unary_function('list_value_lengths')

add = _simple_binary_function('add')
subtract = _simple_binary_function('subtract')
multiply = _simple_binary_function('multiply')


def binary_contains_exact(array, pattern):
    """
    Test if pattern is contained within a value of a binary array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("binary_contains_exact", [array],
                         _pc.BinaryContainsExactOptions(pattern))


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
