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

# Tools for dealing with Arrow type metadata in Python

import pyarrow.lib as lib


_SIGNED_INTEGER_TYPES = {lib.Type_INT8, lib.Type_INT16, lib.Type_INT32,
                         lib.Type_INT64}
_UNSIGNED_INTEGER_TYPES = {lib.Type_UINT8, lib.Type_UINT16, lib.Type_UINT32,
                           lib.Type_UINT64}
_INTEGER_TYPES = _SIGNED_INTEGER_TYPES | _UNSIGNED_INTEGER_TYPES
_FLOATING_TYPES = {lib.Type_HALF_FLOAT, lib.Type_FLOAT, lib.Type_DOUBLE}
_DATE_TYPES = {lib.Type_DATE32, lib.Type_DATE64}
_TIME_TYPES = {lib.Type_TIME32, lib.Type_TIME64}
_TEMPORAL_TYPES = {lib.Type_TIMESTAMP} | _TIME_TYPES | _DATE_TYPES
_NESTED_TYPES = {lib.Type_LIST, lib.Type_STRUCT, lib.Type_UNION, lib.Type_MAP}


def is_boolean(t):
    """
    Return True if value is an instance of a boolean type
    """
    return t.id == lib.Type_BOOL


def is_integer(t):
    """
    Return True if value is an instance of an integer type
    """
    return t.id in _INTEGER_TYPES


def is_signed_integer(t):
    """
    Return True if value is an instance of a signed integer type
    """
    return t.id in _SIGNED_INTEGER_TYPES


def is_unsigned_integer(t):
    """
    Return True if value is an instance of an unsigned integer type
    """
    return t.id in _UNSIGNED_INTEGER_TYPES


def is_floating(t):
    """
    Return True if value is an instance of a floating point numeric type
    """
    return t.id in _FLOATING_TYPES


def is_list(t):
    """
    Return True if value is an instance of a list type
    """
    return t.id == lib.Type_LIST


def is_struct(t):
    """
    Return True if value is an instance of a struct type
    """
    return t.id == lib.Type_STRUCT


def is_union(t):
    """
    Return True if value is an instance of a union type
    """
    return t.id == lib.Type_UNION


def is_nested(t):
    """
    Return True if value is an instance of a nested type
    """
    return t.id in _NESTED_TYPES


def is_temporal(t):
    """
    Return True if value is an instance of a temporal (date, time, timestamp)
    type
    """
    return t.id in _TEMPORAL_TYPES


def is_timestamp(t):
    """
    Return True if value is an instance of a timestamp type
    """
    return t.id == lib.Type_TIMESTAMP


def is_time(t):
    """
    Return True if value is an instance of a time type
    """
    return t.id in _TIME_TYPES


def is_null(t):
    """
    Return True if value is an instance of a null type
    """
    return t.id == lib.Type_NA


def is_binary(t):
    """
    Return True if value is an instance of a variable-length binary type
    """
    return t.id == lib.Type_BINARY


def is_unicode(t):
    """
    Alias for is_string
    """
    return is_string(t)


def is_string(t):
    """
    Return True if value is an instance of string (utf8 unicode) type
    """
    return t.id == lib.Type_STRING


def is_fixed_size_binary(t):
    """
    Return True if value is an instance of a fixed size binary type
    """
    return t.id == lib.Type_FIXED_SIZE_BINARY


def is_date(t):
    """
    Return True if value is an instance of a date type
    """
    return t.id in _DATE_TYPES


def is_map(t):
    """
    Return True if value is an instance of a map logical type
    """
    return t.id == lib.Type_MAP


def is_decimal(t):
    """
    Return True if value is an instance of a decimal type
    """
    return t.id == lib.Type_DECIMAL


def is_dictionary(t):
    """
    Return True if value is an instance of a dictionary-encoded type
    """
    return t.id == lib.Type_DICTIONARY
