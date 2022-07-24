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

import pyarrow as pa
from ._compute_generated import *

from pyarrow._compute import (  # noqa
    Function,
    FunctionOptions,
    FunctionRegistry,
    HashAggregateFunction,
    HashAggregateKernel,
    Kernel,
    ScalarAggregateFunction,
    ScalarAggregateKernel,
    ScalarFunction,
    ScalarKernel,
    VectorFunction,
    VectorKernel,
    # Option classes
    ArraySortOptions,
    AssumeTimezoneOptions,
    CastOptions,
    CountOptions,
    CumulativeSumOptions,
    DayOfWeekOptions,
    DictionaryEncodeOptions,
    ElementWiseAggregateOptions,
    ExtractRegexOptions,
    FilterOptions,
    IndexOptions,
    JoinOptions,
    MakeStructOptions,
    MapLookupOptions,
    MatchSubstringOptions,
    ModeOptions,
    NullOptions,
    PadOptions,
    PartitionNthOptions,
    QuantileOptions,
    RandomOptions,
    RankOptions,
    ReplaceSliceOptions,
    ReplaceSubstringOptions,
    RoundOptions,
    RoundTemporalOptions,
    RoundToMultipleOptions,
    ScalarAggregateOptions,
    SelectKOptions,
    SetLookupOptions,
    SliceOptions,
    SortOptions,
    SplitOptions,
    SplitPatternOptions,
    StrftimeOptions,
    StrptimeOptions,
    StructFieldOptions,
    TakeOptions,
    TDigestOptions,
    TrimOptions,
    Utf8NormalizeOptions,
    VarianceOptions,
    WeekOptions,
    # Functions
    call_function,
    function_registry,
    get_function,
    list_functions,
    _group_by,
    # Udf
    register_scalar_function,
    ScalarUdfContext,
    # Expressions
    Expression,
)


def cast(arr, target_type=None, safe=None, options=None):
    """
    Cast array values to another data type. Can also be invoked as an array
    instance method.

    Parameters
    ----------
    arr : Array-like
    target_type : DataType or str
        Type to cast to
    safe : bool, default True
        Check for overflows or other unsafe conversions
    options : CastOptions, default None
        Additional checks pass by CastOptions

    Examples
    --------
    >>> from datetime import datetime
    >>> import pyarrow as pa
    >>> arr = pa.array([datetime(2010, 1, 1), datetime(2015, 1, 1)])
    >>> arr.type
    TimestampType(timestamp[us])

    You can use ``pyarrow.DataType`` objects to specify the target type:

    >>> cast(arr, pa.timestamp('ms'))
    <pyarrow.lib.TimestampArray object at ...>
    [
      2010-01-01 00:00:00.000,
      2015-01-01 00:00:00.000
    ]

    >>> cast(arr, pa.timestamp('ms')).type
    TimestampType(timestamp[ms])

    Alternatively, it is also supported to use the string aliases for these
    types:

    >>> arr.cast('timestamp[ms]')
    <pyarrow.lib.TimestampArray object at ...>
    [
      2010-01-01 00:00:00.000,
      2015-01-01 00:00:00.000
    ]
    >>> arr.cast('timestamp[ms]').type
    TimestampType(timestamp[ms])

    Returns
    -------
    casted : Array
    """
    safe_vars_passed = (safe is not None) or (target_type is not None)

    if safe_vars_passed and (options is not None):
        raise ValueError("Must either pass values for 'target_type' and 'safe'"
                         " or pass a value for 'options'")

    if options is None:
        target_type = pa.types.lib.ensure_type(target_type)
        if safe is False:
            options = CastOptions.unsafe(target_type)
        else:
            options = CastOptions.safe(target_type)
    return call_function("cast", [arr], options)


def index(data, value, start=None, end=None, *, memory_pool=None):
    """
    Find the index of the first occurrence of a given value.

    Parameters
    ----------
    data : Array-like
    value : Scalar-like object
        The value to search for.
    start : int, optional
    end : int, optional
    memory_pool : MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.

    Returns
    -------
    index : int
        the index, or -1 if not found
    """
    if start is not None:
        if end is not None:
            data = data.slice(start, end - start)
        else:
            data = data.slice(start)
    elif end is not None:
        data = data.slice(0, end)

    if not isinstance(value, pa.Scalar):
        value = pa.scalar(value, type=data.type)
    elif data.type != value.type:
        value = pa.scalar(value.as_py(), type=data.type)
    options = IndexOptions(value=value)
    result = call_function('index', [data], options, memory_pool)
    if start is not None and result.as_py() >= 0:
        result = pa.scalar(result.as_py() + start, type=pa.int64())
    return result


def take(data, indices, *, boundscheck=True, memory_pool=None):
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
    memory_pool : MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.

    Returns
    -------
    result : depends on inputs

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array(["a", "b", "c", None, "e", "f"])
    >>> indices = pa.array([0, None, 4, 3])
    >>> arr.take(indices)
    <pyarrow.lib.StringArray object at ...>
    [
      "a",
      null,
      "e",
      null
    ]
    """
    options = TakeOptions(boundscheck=boundscheck)
    return call_function('take', [data, indices], options, memory_pool)


def fill_null(values, fill_value):
    """
    Replace each null element in values with fill_value. The fill_value must be
    the same type as values or able to be implicitly casted to the array's
    type.

    This is an alias for :func:`coalesce`.

    Parameters
    ----------
    values : Array, ChunkedArray, or Scalar-like object
        Each null element is replaced with the corresponding value
        from fill_value.
    fill_value : Array, ChunkedArray, or Scalar-like object
        If not same type as data will attempt to cast.

    Returns
    -------
    result : depends on inputs

    Examples
    --------
    >>> import pyarrow as pa
    >>> arr = pa.array([1, 2, None, 3], type=pa.int8())
    >>> fill_value = pa.scalar(5, type=pa.int8())
    >>> arr.fill_null(fill_value)
    <pyarrow.lib.Int8Array object at ...>
    [
      1,
      2,
      5,
      3
    ]
    """
    if not isinstance(fill_value, (pa.Array, pa.ChunkedArray, pa.Scalar)):
        fill_value = pa.scalar(fill_value, type=values.type)
    elif values.type != fill_value.type:
        fill_value = pa.scalar(fill_value.as_py(), type=values.type)

    return call_function("coalesce", [values, fill_value])


def top_k_unstable(values, k, sort_keys=None, *, memory_pool=None):
    """
    Select the indices of the top-k ordered elements from array- or table-like
    data.

    This is a specialization for :func:`select_k_unstable`. Output is not
    guaranteed to be stable.

    Parameters
    ----------
    values : Array, ChunkedArray, RecordBatch, or Table
        Data to sort and get top indices from.
    k : int
        The number of `k` elements to keep.
    sort_keys : List-like
        Column key names to order by when input is table-like data.
    memory_pool : MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.

    Returns
    -------
    result : Array of indices

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array(["a", "b", "c", None, "e", "f"])
    >>> pc.top_k_unstable(arr, k=3)
    <pyarrow.lib.UInt64Array object at ...>
    [
      5,
      4,
      2
    ]
    """
    if sort_keys is None:
        sort_keys = []
    if isinstance(values, (pa.Array, pa.ChunkedArray)):
        sort_keys.append(("dummy", "descending"))
    else:
        sort_keys = map(lambda key_name: (key_name, "descending"), sort_keys)
    options = SelectKOptions(k, sort_keys)
    return call_function("select_k_unstable", [values], options, memory_pool)


def bottom_k_unstable(values, k, sort_keys=None, *, memory_pool=None):
    """
    Select the indices of the bottom-k ordered elements from
    array- or table-like data.

    This is a specialization for :func:`select_k_unstable`. Output is not
    guaranteed to be stable.

    Parameters
    ----------
    values : Array, ChunkedArray, RecordBatch, or Table
        Data to sort and get bottom indices from.
    k : int
        The number of `k` elements to keep.
    sort_keys : List-like
        Column key names to order by when input is table-like data.
    memory_pool : MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.

    Returns
    -------
    result : Array of indices

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array(["a", "b", "c", None, "e", "f"])
    >>> pc.bottom_k_unstable(arr, k=3)
    <pyarrow.lib.UInt64Array object at ...>
    [
      0,
      1,
      2
    ]
    """
    if sort_keys is None:
        sort_keys = []
    if isinstance(values, (pa.Array, pa.ChunkedArray)):
        sort_keys.append(("dummy", "ascending"))
    else:
        sort_keys = map(lambda key_name: (key_name, "ascending"), sort_keys)
    options = SelectKOptions(k, sort_keys)
    return call_function("select_k_unstable", [values], options, memory_pool)


def random(n, *, initializer='system', options=None, memory_pool=None):
    """
    Generate numbers in the range [0, 1).

    Generated values are uniformly-distributed, double-precision
    in range [0, 1). Algorithm and seed can be changed via RandomOptions.

    Parameters
    ----------
    n : int
        Number of values to generate, must be greater than or equal to 0
    initializer : int or str
        How to initialize the underlying random generator.
        If an integer is given, it is used as a seed.
        If "system" is given, the random generator is initialized with
        a system-specific source of (hopefully true) randomness.
        Other values are invalid.
    options : pyarrow.compute.RandomOptions, optional
        Alternative way of passing options.
    memory_pool : pyarrow.MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.
    """
    options = RandomOptions(initializer=initializer)
    return call_function("random", [], options, memory_pool, length=n)


def field(*name_or_index):
    """Reference a column of the dataset.

    Stores only the field's name. Type and other information is known only when
    the expression is bound to a dataset having an explicit scheme.

    Nested references are allowed by passing multiple names or a tuple of
    names. For example ``('foo', 'bar')`` references the field named "bar"
    inside the field named "foo".

    Parameters
    ----------
    *name_or_index : string, multiple strings, tuple or int
        The name or index of the (possibly nested) field the expression
        references to.

    Returns
    -------
    field_expr : Expression

    Examples
    --------
    >>> import pyarrow.compute as pc
    >>> pc.field("a")
    <pyarrow.compute.Expression a>
    >>> pc.field(1)
    <pyarrow.compute.Expression FieldPath(1)>
    >>> pc.field(("a", "b"))
    <pyarrow.compute.Expression FieldRef.Nested(FieldRef.Name(a) ...
    >>> pc.field("a", "b")
    <pyarrow.compute.Expression FieldRef.Nested(FieldRef.Name(a) ...
    """
    n = len(name_or_index)
    if n == 1:
        if isinstance(name_or_index[0], (str, int)):
            return Expression._field(name_or_index[0])
        elif isinstance(name_or_index[0], tuple):
            return Expression._nested_field(name_or_index[0])
        else:
            raise TypeError(
                "field reference should be str, multiple str, tuple or "
                f"integer, got {type(name_or_index[0])}"
            )
    # In case of multiple strings not supplied in a tuple
    else:
        return Expression._nested_field(name_or_index)


def scalar(value):
    """Expression representing a scalar value.

    Parameters
    ----------
    value : bool, int, float or string
        Python value of the scalar. Note that only a subset of types are
        currently supported.

    Returns
    -------
    scalar_expr : Expression
    """
    return Expression._scalar(value)
