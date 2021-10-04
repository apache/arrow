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
    DayOfWeekOptions,
    DictionaryEncodeOptions,
    ElementWiseAggregateOptions,
    ExtractRegexOptions,
    FilterOptions,
    IndexOptions,
    JoinOptions,
    MakeStructOptions,
    MatchSubstringOptions,
    ModeOptions,
    NullOptions,
    PadOptions,
    PartitionNthOptions,
    QuantileOptions,
    ReplaceSliceOptions,
    ReplaceSubstringOptions,
    RoundOptions,
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
    TakeOptions,
    TDigestOptions,
    TrimOptions,
    VarianceOptions,
    WeekOptions,
    # Functions
    call_function,
    function_registry,
    get_function,
    list_functions,
)

import inspect
from textwrap import dedent
import warnings

import pyarrow as pa


def _get_arg_names(func):
    return func._doc.arg_names


def _decorate_compute_function(wrapper, exposed_name, func, option_class):
    # Decorate the given compute function wrapper with useful metadata
    # and documentation.
    wrapper.__arrow_compute_function__ = dict(name=func.name,
                                              arity=func.arity)
    wrapper.__name__ = exposed_name
    wrapper.__qualname__ = exposed_name

    doc_pieces = []

    cpp_doc = func._doc
    summary = cpp_doc.summary
    if not summary:
        arg_str = "arguments" if func.arity > 1 else "argument"
        summary = ("Call compute function {!r} with the given {}"
                   .format(func.name, arg_str))

    description = cpp_doc.description
    arg_names = _get_arg_names(func)

    doc_pieces.append("""\
        {}.

        """.format(summary))

    if description:
        doc_pieces.append("{}\n\n".format(description))

    doc_pieces.append("""\
        Parameters
        ----------
        """)

    for arg_name in arg_names:
        if func.kind in ('vector', 'scalar_aggregate'):
            arg_type = 'Array-like'
        else:
            arg_type = 'Array-like or scalar-like'
        doc_pieces.append("""\
            {} : {}
                Argument to compute function
            """.format(arg_name, arg_type))

    doc_pieces.append("""\
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the default memory pool.
        """)
    if option_class is not None:
        doc_pieces.append("""\
            options : pyarrow.compute.{0}, optional
                Parameters altering compute function semantics.
            """.format(option_class.__name__))
        options_sig = inspect.signature(option_class)
        for p in options_sig.parameters.values():
            doc_pieces.append("""\
            {0} : optional
                Parameter for {1} constructor. Either `options`
                or `{0}` can be passed, but not both at the same time.
            """.format(p.name, option_class.__name__))

    wrapper.__doc__ = "".join(dedent(s) for s in doc_pieces)
    return wrapper


def _get_options_class(func):
    class_name = func._doc.options_class
    if not class_name:
        return None
    try:
        return globals()[class_name]
    except KeyError:
        warnings.warn("Python binding for {} not exposed"
                      .format(class_name), RuntimeWarning)
        return None


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


def _make_generic_wrapper(func_name, func, option_class):
    if option_class is None:
        def wrapper(*args, memory_pool=None):
            return func.call(args, None, memory_pool)
    else:
        def wrapper(*args, memory_pool=None, options=None, **kwargs):
            options = _handle_options(func_name, option_class, options,
                                      kwargs)
            return func.call(args, options, memory_pool)
    return wrapper


def _make_signature(arg_names, var_arg_names, option_class):
    from inspect import Parameter
    params = []
    for name in arg_names:
        params.append(Parameter(name, Parameter.POSITIONAL_OR_KEYWORD))
    for name in var_arg_names:
        params.append(Parameter(name, Parameter.VAR_POSITIONAL))
    params.append(Parameter("memory_pool", Parameter.KEYWORD_ONLY,
                            default=None))
    if option_class is not None:
        params.append(Parameter("options", Parameter.KEYWORD_ONLY,
                                default=None))
        options_sig = inspect.signature(option_class)
        for p in options_sig.parameters.values():
            # XXX for now, our generic wrappers don't allow positional
            # option arguments
            params.append(p.replace(kind=Parameter.KEYWORD_ONLY))
    return inspect.Signature(params)


def _wrap_function(name, func):
    option_class = _get_options_class(func)
    arg_names = _get_arg_names(func)
    has_vararg = arg_names and arg_names[-1].startswith('*')
    if has_vararg:
        var_arg_names = [arg_names.pop().lstrip('*')]
    else:
        var_arg_names = []

    wrapper = _make_generic_wrapper(name, func, option_class)
    wrapper.__signature__ = _make_signature(arg_names, var_arg_names,
                                            option_class)
    return _decorate_compute_function(wrapper, name, func, option_class)


def _make_global_functions():
    """
    Make global functions wrapping each compute function.

    Note that some of the automatically-generated wrappers may be overriden
    by custom versions below.
    """
    g = globals()
    reg = function_registry()

    # Avoid clashes with Python keywords
    rewrites = {'and': 'and_',
                'or': 'or_'}

    for cpp_name in reg.list_functions():
        name = rewrites.get(cpp_name, cpp_name)
        func = reg.get_function(cpp_name)
        assert name not in g, name
        g[cpp_name] = g[name] = _wrap_function(name, func)


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


def count_substring(array, pattern, *, ignore_case=False):
    """
    Count the occurrences of substring *pattern* in each value of a
    string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("count_substring", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def count_substring_regex(array, pattern, *, ignore_case=False):
    """
    Count the non-overlapping matches of regex *pattern* in each value
    of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("count_substring_regex", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def find_substring(array, pattern, *, ignore_case=False):
    """
    Find the index of the first occurrence of substring *pattern* in each
    value of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("find_substring", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def find_substring_regex(array, pattern, *, ignore_case=False):
    """
    Find the index of the first match of regex *pattern* in each
    value of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        regex pattern to search for
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("find_substring_regex", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def match_like(array, pattern, *, ignore_case=False):
    """
    Test if the SQL-style LIKE pattern *pattern* matches a value of a
    string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        SQL-style LIKE pattern. '%' will match any number of
        characters, '_' will match exactly one character, and all
        other characters match themselves. To match a literal percent
        sign or underscore, precede the character with a backslash.
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray

    """
    return call_function("match_like", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def match_substring(array, pattern, *, ignore_case=False):
    """
    Test if substring *pattern* is contained within a value of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        pattern to search for exact matches
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("match_substring", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def match_substring_regex(array, pattern, *, ignore_case=False):
    """
    Test if regex *pattern* matches at any position a value of a string array.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    pattern : str
        regex pattern to search
    ignore_case : bool, default False
        Ignore case while searching.

    Returns
    -------
    result : pyarrow.Array or pyarrow.ChunkedArray
    """
    return call_function("match_substring_regex", [array],
                         MatchSubstringOptions(pattern,
                                               ignore_case=ignore_case))


def mode(array, n=1, *, skip_nulls=True, min_count=0):
    """
    Return top-n most common values and number of times they occur in a passed
    numerical (chunked) array, in descending order of occurrence. If there are
    multiple values with same count, the smaller one is returned first.

    Parameters
    ----------
    array : pyarrow.Array or pyarrow.ChunkedArray
    n : int, default 1
        Specify the top-n values.
    skip_nulls : bool, default True
        If True, ignore nulls in the input. Else return an empty array
        if any input is null.
    min_count : int, default 0
        If there are fewer than this many values in the input, return
        an empty array.

    Returns
    -------
    An array of <input type "Mode", int64_t "Count"> structs

    Examples
    --------
    >>> import pyarrow as pa
    >>> import pyarrow.compute as pc
    >>> arr = pa.array([1, 1, 2, 2, 3, 2, 2, 2])
    >>> modes = pc.mode(arr, 2)
    >>> modes[0]
    <pyarrow.StructScalar: {'mode': 2, 'count': 5}>
    >>> modes[1]
    <pyarrow.StructScalar: {'mode': 1, 'count': 2}>
    """
    options = ModeOptions(n, skip_nulls=skip_nulls, min_count=min_count)
    return call_function("mode", [array], options)


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


def index(data, value, start=None, end=None, *, memory_pool=None):
    """
    Find the index of the first occurrence of a given value.

    Parameters
    ----------
    data : Array or ChunkedArray
    value : Scalar-like object
    start : int, optional
    end : int, optional
    memory_pool : MemoryPool, optional
        If not passed, will allocate memory from the default memory pool.

    Returns
    -------
    index : the index, or -1 if not found
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
    pyarrow.lib.Int8Array object at 0x7f95437f01a0>
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
    <pyarrow.lib.UInt64Array object at 0x7fdcb19d7f30>
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
    <pyarrow.lib.UInt64Array object at 0x7fdcb19d7fa0>
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
