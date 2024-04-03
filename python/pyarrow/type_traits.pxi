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

from pyarrow.includes.libarrow cimport (is_integer,
                                        is_signed_integer,
                                        is_unsigned_integer,
                                        is_floating,
                                        is_numeric,
                                        is_decimal,
                                        is_run_end_type,
                                        is_primitive,
                                        is_base_binary_like,
                                        is_binary_like,
                                        is_large_binary_like,
                                        is_binary,
                                        is_string,
                                        is_temporal,
                                        is_time,
                                        is_date,
                                        is_interval,
                                        is_dictionary,
                                        is_fixed_size_binary,
                                        is_fixed_width,
                                        is_var_length_list,
                                        is_list,
                                        is_list_like,
                                        is_var_length_list_like,
                                        is_list_view,
                                        is_nested,
                                        is_union,
                                        bit_width,
                                        offset_bit_width)


def is_integer_type(data_type):
    """
    This function checks whether the `data_type` is an integer type, which
    includes signed and unsigned integers of various bit widths (8, 16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check against the set of supported integer types.

    Returns
    -------
    bool
        True if `data_type` is an integer type, False otherwise.
    """
    return is_integer(data_type.id)


def is_signed_integer_type(data_type):
    """
    This function checks whether the `data_type` is a signed integer type,
    which includes signed integers of various bit widths (8, 16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a signed integer type, False otherwise.
    """
    return is_signed_integer(data_type.id)


def is_unsigned_integer_type(data_type):
    """
    This function checks whether the `data_type` is an unsigned integer type,
    which includes unsigned integers of various bit widths (8, 16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is an unsigned integer type, False otherwise.
    """
    return is_unsigned_integer(data_type.id)


def is_floating_type(data_type):
    """
    This function checks whether the `data_type` is a floating type, which includes
    floating point numbers of various bit widths (16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a floating type, False otherwise.
    """
    return is_floating(data_type.id)


def is_numeric_type(data_type):
    """
    This function checks whether the `data_type` is a numeric type, which includes
    integers and floating point numbers with specific bit widths. Integer types
    include signed and unsigned integers of various bit widths (8, 16, 32, 64 bits),
    while floating point types include floating point numbers of various bit widths
    (16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a numeric type, False otherwise.
    """
    return is_numeric(data_type.id)


def is_decimal_type(data_type):
    """
    This function checks whether the `data_type` is a decimal type, which includes
    fixed-point decimal numbers with specific precision and scale.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a decimal type, False otherwise.
    """
    return is_decimal(data_type.id)


def is_run_end_type_py(data_type):
    """
    This function checks whether the `data_type` is a run end type, which includes
    integers of various bit widths (16, 32, 64 bits).

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a run end type, False otherwise.
    """
    return is_run_end_type(data_type.id)


def is_primitive_type(data_type):
    """
    This function checks whether the `data_type` is a primitive type, which includes
    boolean, integers, floating point numbers, dates, times, timestamp, duration
    and intervals types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a primitive type, False otherwise.
    """
    return is_primitive(data_type.id)


def is_base_binary_like_type(data_type):
    """
    This function checks whether the `data_type` is a base binary-like type, which
    includes binary, string, large binary, and large string types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a base binary-like type, False otherwise.
    """
    return is_base_binary_like(data_type.id)


def is_binary_like_type(data_type):
    """
    This function checks whether the `data_type` is a binary-like type, which includes
    binary and string types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a binary-like type, False otherwise.
    """
    return is_binary_like(data_type.id)


def is_large_binary_like_type(data_type):
    """
    This function checks whether the `data_type` is a large binary-like type, which
    includes large binary and large string types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a large binary-like type, False otherwise.
    """
    return is_large_binary_like(data_type.id)


def is_binary_type(data_type):
    """
    This function checks whether the `data_type` is a binary type, which includes
    binary and large binary types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a binary type, False otherwise.
    """
    return is_binary(data_type.id)


def is_string_type(data_type):
    """
    This function checks whether the `data_type` is a string type, which includes
    string and large string types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a string type, False otherwise.
    """
    return is_string(data_type.id)


def is_temporal_type(data_type):
    """
    This function checks whether the `data_type` is a temporal type, which includes
    dates (days since the UNIX epoch and milliseconds since the UNIX epoch), times
    (seconds and milliseconds since midnight), timestamp (milliseconds since the UNIX
    epoch) types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a temporal type, False otherwise.
    """
    return is_temporal(data_type.id)


def is_time_type(data_type):
    """
    This function checks whether the `data_type` is a time type, which includes
    times (seconds and milliseconds since midnight) types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a time type, False otherwise.
    """
    return is_time(data_type.id)


def is_date_type(data_type):
    """
    This function checks whether the `data_type` is a date type, which includes
    dates (days since the UNIX epoch and milliseconds since the UNIX epoch) types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a date type, False otherwise.
    """
    return is_date(data_type.id)


def is_interval_type(data_type):
    """
    This function checks whether the `data_type` is a interval type, which includes
    intervals (months, days plus nanoseconds, and day-time intervals) types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a interval type, False otherwise.
    """
    return is_interval(data_type.id)


def is_dictionary_type(data_type):
    """
    This function checks whether the `data_type` is a dictionary type.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a dictionary type, False otherwise.
    """
    return is_dictionary(data_type.id)


def is_fixed_size_binary_type(data_type):
    """
    This function checks whether the `data_type` is a fixed size binary type
    which includes decimal and fixed size binary types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a fixed size binary type, False otherwise.
    """
    return is_fixed_size_binary(data_type.id)


def is_fixed_width_type(data_type):
    """
    This function checks whether the `data_type` is a fixed width type which includes
    is_primitive, is_dictionary, is_fixed_size_binary types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a fixed width type, False otherwise.
    """
    return is_fixed_width(data_type.id)


def is_var_length_list_type(data_type):
    """
    This function checks whether the `data_type` is a variable length list type
    which includes list, large list, and map types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a variable length list type, False otherwise.
    """
    return is_var_length_list(data_type.id)


def is_list_type(data_type):
    """
    This function checks whether the `data_type` is a list type which includes
    list, large list, fixed size list types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a list type, False otherwise.
    """
    return is_list(data_type.id)


def is_list_like_type(data_type):
    """
    This function checks whether the `data_type` is a list_like type which includes
    list, large list, fixed size list, and map types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a list_like type, False otherwise.
    """
    return is_list_like(data_type.id)


def is_var_length_list_like_type(data_type):
    """
    This function checks whether the `data_type` is a variable length list like
    type which includes list, large list, list view, large list view, and map types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a variable length list like type, False otherwise.
    """
    return is_var_length_list_like(data_type.id)


def is_list_view_type(data_type):
    """
    This function checks whether the `data_type` is a list view type which includes
    list view and large list view types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a list view type, False otherwise.
    """
    return is_list_view(data_type.id)


def is_nested_type(data_type):
    """
    This function checks whether the `data_type` is a nested type which includes
    list, large list, list view, large list view, fixed size list, map, struct,
    sparse union, dense union, and run end encoded types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a nested type, False otherwise.
    """
    return is_nested(data_type.id)


def is_union_type(data_type):
    """
    This function checks whether the `data_type` is a union type which includes
    sparse union and dense union types.

    Parameters
    ----------
    data_type : DataType
        The data type to check

    Returns
    -------
    bool
        True if `data_type` is a union type, False otherwise.
    """
    return is_union(data_type.id)


def bit_width_type(data_type):
    """
    Determine the bit width of the data type.

    This function evaluates the `data_type` and returns its bit width, which is
    essential for understanding the storage size and precision of data types.
    It supports a wide range of data types, including boolean, integers, floating
    point numbers, date, time, timestamp, duration, intervals, and decimal types.

    Parameters
    ----------
    data_type : DataType
        The data type for which to determine the bit width.

    Returns
    -------
    int
        The bit width of the `data_type`, or 0 if the bit width is not applicable.
    """
    return bit_width(data_type.id)


def offset_bit_width_type(data_type):
    """
    Determine the offset bit width of the data type.

    This function evaluates the `data_type` and returns its offset bit width, which is
    essential for understanding the storage size and precision of data types.
    It supports a wide range of data types, including string, binary, list, list
    view, map, dense union, large string, large binary, large list, and large list view
    types.

    Parameters
    ----------
    data_type : DataType
        The data type for which to determine the offset bit width.

    Returns
    -------
    int
        The offset bit width of the `data_type`, or 0 if the offset bit width is not applicable.
    """
    return offset_bit_width(data_type.id)
