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

from pyarrow.lib cimport Type


def is_integer_type(data_type):
    """
    Check if the data type is an integer type.

    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    cdef Type type_id = data_type.id
    return is_integer(type_id)

def is_signed_integer_type(data_type):
    """
    Check if the data type is a signed integer type.
    """
    cdef Type type_id = data_type.id
    return is_signed_integer(type_id)

def is_unsigned_integer_type(data_type):
    """
    Check if the data type is an unsigned integer type.
    """
    cdef Type type_id = data_type.id
    return is_unsigned_integer(type_id)

def is_floating_type(data_type):
    """
    Check if the data type is a floating type.
    """
    cdef Type type_id = data_type.id
    return is_floating(type_id)

def is_numeric_type(data_type):
    """
    Check if the data type is a numeric type.
    """
    cdef Type type_id = data_type.id
    return is_numeric(type_id)

def is_decimal_type(data_type):
    """
    Check if the data type is a decimal type.
    """
    cdef Type type_id = data_type.id
    return is_decimal(type_id)

def is_run_end_type_py(data_type):
    """
    Check if the data type is a run end type.
    """
    cdef Type type_id = data_type.id
    return is_run_end_type(type_id)

def is_primitive_type(data_type):
    """
    Check if the data type is a primitive type.
    """
    cdef Type type_id = data_type.id
    return is_primitive(type_id)

def is_base_binary_like_type(data_type):
    """
    Check if the data type is a base_binary_like type.
    """
    cdef Type type_id = data_type.id
    return is_base_binary_like(type_id)

def is_binary_like_type(data_type):
    """
    Check if the data type is a _binary_like type.
    """
    cdef Type type_id = data_type.id
    return is_binary_like(type_id)

def is_large_binary_like_type(data_type):
    """
    Check if the data type is a large_binary_like type.
    """
    cdef Type type_id = data_type.id
    return is_large_binary_like(type_id)

def is_binary_type(data_type):
    """
    Check if the data type is a binary type.
    """
    cdef Type type_id = data_type.id
    return is_binary(type_id)

def is_string_type(data_type):
    """
    Check if the data type is a string type.
    """
    cdef Type type_id = data_type.id
    return is_string(type_id)

def is_temporal_type(data_type):
    """
    Check if the data type is a temporal type.
    """
    cdef Type type_id = data_type.id
    return is_temporal(type_id)

def is_time_type(data_type):
    """
    Check if the data type is a time type.
    """
    cdef Type type_id = data_type.id
    return is_time(type_id)

def is_date_type(data_type):
    """
    Check if the data type is a date type.
    """
    cdef Type type_id = data_type.id
    return is_date(type_id)

def is_interval_type(data_type):
    """
    Check if the data type is a interval type.
    """
    cdef Type type_id = data_type.id
    return is_interval(type_id)

def is_dictionary_type(data_type):
    """
    Check if the data type is a dictionary type.
    """
    cdef Type type_id = data_type.id
    return is_dictionary(type_id)

def is_fixed_size_binary_type(data_type):
    """
    Check if the data type is a fixed_size_binary type.
    """
    cdef Type type_id = data_type.id
    return is_fixed_size_binary(type_id)

def is_fixed_width_type(data_type):
    """
    Check if the data type is a fixed_width type.
    """
    cdef Type type_id = data_type.id
    return is_fixed_width(type_id)

def is_var_length_list_type(data_type):
    """
    Check if the data type is a var_length_list type.
    """
    cdef Type type_id = data_type.id
    return is_var_length_list(type_id)

def is_list_type(data_type):
    """
    Check if the data type is a list type.
    """
    cdef Type type_id = data_type.id
    return is_list(type_id)

def is_list_like_type(data_type):
    """
    Check if the data type is a list_like type.
    """
    cdef Type type_id = data_type.id
    return is_list_like(type_id)

def is_var_length_list_like_type(data_type):
    """
    Check if the data type is a var_length_list_like type.
    """
    cdef Type type_id = data_type.id
    return is_var_length_list_like(type_id)

def is_list_view_type(data_type):
    """
    Check if the data type is a list_view type.
    """
    cdef Type type_id = data_type.id
    return is_list_view(type_id)

def is_nested_type(data_type):
    """
    Check if the data type is a nested type.
    """
    cdef Type type_id = data_type.id
    return is_nested(type_id)

def is_union_type(data_type):
    """
    Check if the data type is a union type.
    """
    cdef Type type_id = data_type.id
    return is_union(type_id)

def is_bit_width_type(data_type):
    """
    Check if the data type is a bit_width type.
    """
    cdef Type type_id = data_type.id
    return bit_width(type_id)

def is_offset_bit_width_type(data_type):
    """
    Check if the data type is a offset_bit_width type.
    """
    cdef Type type_id = data_type.id
    return offset_bit_width(type_id)
