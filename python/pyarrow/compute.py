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
    Function,
    FunctionRegistry,
    Kernel,
    ScalarAggregateFunction,
    ScalarAggregateKernel,
    ScalarFunction,
    ScalarKernel,
    VectorFunction,
    VectorKernel,
    # Option classes
    CastOptions,
    FilterOptions,
    MatchSubstringOptions,
    MinMaxOptions,
    PartitionNthOptions,
    SetLookupOptions,
    StrptimeOptions,
    TakeOptions,
    VarianceOptions,
    # Functions
    function_registry,
    call_function,
    get_function,
    list_functions,
)

from textwrap import dedent

import pyarrow as pa


def _decorate_compute_function(wrapper, exposed_name, func, option_class):
    wrapper.__arrow_compute_function__ = dict(name=func.name,
                                              arity=func.arity)
    wrapper.__name__ = exposed_name
    wrapper.__qualname__ = exposed_name

    # TODO (ARROW-9164): expose actual docstring from C++
    doc_pieces = []
    arg_str = "arguments" if func.arity > 1 else "argument"
    doc_pieces.append("""\
        Call compute function {!r} with the given {}.

        Parameters
        ----------
        """.format(func.name, arg_str))

    if func.arity == 1:
        doc_pieces.append("""\
            arg : Array-like or scalar-like
                Argument to compute function
            """)
    elif func.arity == 2:
        doc_pieces.append("""\
            left : Array-like or scalar-like
                First argument to compute function
            right : Array-like or scalar-like
                Second argument to compute function
            """)

    doc_pieces.append("""\
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the default memory pool.
        """)
    if option_class is not None:
        doc_pieces.append("""\
            options : pyarrow.compute.{0}, optional
                Parameters altering compute function semantics
            **kwargs: optional
                Parameters for {0} constructor.  Either `options`
                or `**kwargs` can be passed, but not both at the same time.
            """.format(option_class.__name__))

    wrapper.__doc__ = "".join(dedent(s) for s in doc_pieces)
    return wrapper


_option_classes = {
    # TODO: export the option class name from C++ metadata?
    'cast': CastOptions,
    'filter': FilterOptions,
    'index_in': SetLookupOptions,
    'is_in': SetLookupOptions,
    'match_substring': MatchSubstringOptions,
    'min_max': MinMaxOptions,
    'partition_nth_indices': PartitionNthOptions,
    'stddev': VarianceOptions,
    'strptime': StrptimeOptions,
    'take': TakeOptions,
    'variance': VarianceOptions,
}


def _handle_options(name, option_class, options, kwargs):
    if kwargs:
        if options is None:
            return option_class(**kwargs)
        raise TypeError(
            "Function {!r} called with both an 'options' argument "
            "and additional named arguments"
            .format(name))

    if options is not None:
        if isinstance(options, dict):
            return option_class(**options)
        elif isinstance(options, option_class):
            return options
        raise TypeError(
            "Function {!r} expected a {} parameter, got {}"
            .format(name, option_class, type(options)))

    return options


def _simple_unary_function(name):
    func = get_function(name)
    option_class = _option_classes.get(name)

    if option_class is not None:
        def wrapper(arg, *, options=None, memory_pool=None, **kwargs):
            options = _handle_options(name, option_class, options, kwargs)
            return func.call([arg], options, memory_pool)
    else:
        def wrapper(arg, *, memory_pool=None):
            return func.call([arg], None, memory_pool)

    return _decorate_compute_function(wrapper, name, func, option_class)


def _simple_binary_function(name):
    func = get_function(name)
    option_class = _option_classes.get(name)

    if option_class is not None:
        def wrapper(left, right, *, options=None, memory_pool=None, **kwargs):
            options = _handle_options(name, option_class, options, kwargs)
            return func.call([left, right], options, memory_pool)
    else:
        def wrapper(left, right, *, memory_pool=None):
            return func.call([left, right], None, memory_pool)

    return _decorate_compute_function(wrapper, name, func, option_class)


def _make_global_functions():
    """
    Make global functions wrapping each compute function.

    Note that some of the automatically-generated wrappers may be overriden
    by custom versions below.
    """
    g = globals()
    reg = function_registry()

    for name in reg.list_functions():
        assert name not in g, name
        func = reg.get_function(name)
        if func.arity == 1:
            g[name] = _simple_unary_function(name)
        elif func.arity == 2:
            g[name] = _simple_binary_function(name)
        else:
            raise NotImplementedError("Unsupported function arity: ",
                                      func.arity)


_make_global_functions()


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
        options = CastOptions.safe(target_type)
    else:
        options = CastOptions.unsafe(target_type)
    return call_function("cast", [arr], options)


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
                         MatchSubstringOptions(pattern))


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


def mode(array):
    """
    Return the mode (most common value) of a passed numerical
    (chunked) array. If there is more than one such value, only
    the smallest is returned.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray

    Returns
    -------
    mode : pyarrow.StructScalar

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([1, 1, 2, 2, 3, 2, 2, 2])
    >>> pc.mode(arr)
    <pyarrow.StructScalar: {'mode': 2, 'count': 5}>

    """
    return call_function("mode", [array])


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
    options = TakeOptions(boundscheck=boundscheck)
    return call_function('take', [data, indices], options, memory_pool)


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


and_ = globals()['and']
or_ = globals()['or']
