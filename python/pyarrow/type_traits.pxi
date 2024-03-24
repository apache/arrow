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
    Check if the data type is an integer type.

    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_integer(data_type.id)

def is_signed_integer_type(data_type):
    """
    Check if the data type is a signed integer type.

    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_signed_integer(data_type.id)

def is_unsigned_integer_type(data_type):
    """
    Check if the data type is an unsigned integer type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_unsigned_integer(data_type.id)

def is_floating_type(data_type):
    """
    Check if the data type is a floating type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_floating(data_type.id)

def is_numeric_type(data_type):
    """
    Check if the data type is a numeric type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_numeric(data_type.id)

def is_decimal_type(data_type):
    """
    Check if the data type is a decimal type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_decimal(data_type.id)

def is_run_end_type_py(data_type):
    """
    Check if the data type is a run end type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_run_end_type(data_type.id)

def is_primitive_type(data_type):
    """
    Check if the data type is a primitive type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_primitive(data_type.id)

def is_base_binary_like_type(data_type):
    """
    Check if the data type is a base_binary_like type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_base_binary_like(data_type.id)

def is_binary_like_type(data_type):
    """
    Check if the data type is a _binary_like type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_binary_like(data_type.id)

def is_large_binary_like_type(data_type):
    """
    Check if the data type is a large_binary_like type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_large_binary_like(data_type.id)

def is_binary_type(data_type):
    """
    Check if the data type is a binary type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_binary(data_type.id)

def is_string_type(data_type):
    """
    Check if the data type is a string type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_string(data_type.id)

def is_temporal_type(data_type):
    """
    Check if the data type is a temporal type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_temporal(data_type.id)

def is_time_type(data_type):
    """
    Check if the data type is a time type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_time(data_type.id)

def is_date_type(data_type):
    """
    Check if the data type is a date type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_date(data_type.id)

def is_interval_type(data_type):
    """
    Check if the data type is a interval type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_interval(data_type.id)

def is_dictionary_type(data_type):
    """
    Check if the data type is a dictionary type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_dictionary(data_type.id)

def is_fixed_size_binary_type(data_type):
    """
    Check if the data type is a fixed_size_binary type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_fixed_size_binary(data_type.id)

def is_fixed_width_type(data_type):
    """
    Check if the data type is a fixed_width type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_fixed_width(data_type.id)

def is_var_length_list_type(data_type):
    """
    Check if the data type is a var_length_list type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_var_length_list(data_type.id)

def is_list_type(data_type):
    """
    Check if the data type is a list type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_list(data_type.id)

def is_list_like_type(data_type):
    """
    Check if the data type is a list_like type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_list_like(data_type.id)

def is_var_length_list_like_type(data_type):
    """
    Check if the data type is a var_length_list_like type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_var_length_list_like(data_type.id)

def is_list_view_type(data_type):
    """
    Check if the data type is a list_view type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_list_view(data_type.id)

def is_nested_type(data_type):
    """
    Check if the data type is a nested type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_nested(data_type.id)

def is_union_type(data_type):
    """
    Check if the data type is a union type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return is_union(data_type.id)

def is_bit_width_type(data_type):
    """
    Check if the data type is a bit_width type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return bit_width(data_type.id)

def is_offset_bit_width_type(data_type):
    """
    Check if the data type is a offset_bit_width type.
    
    Parameters
    ----------
    data_type : DataType
        The data type to check
    """
    return offset_bit_width(data_type.id)
