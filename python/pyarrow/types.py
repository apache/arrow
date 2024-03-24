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


from pyarrow.lib import (is_boolean_value,  # noqa
                         is_integer_value,
                         is_float_value,
                         is_integer_type,
                         is_signed_integer_type,
                         is_unsigned_integer_type,
                         is_floating_type,
                         is_numeric_type,
                         is_decimal_type,
                         is_run_end_type_py,
                         is_primitive_type,
                         is_base_binary_like_type,
                         is_binary_like_type,
                         is_large_binary_like_type,
                         is_binary_type,
                         is_string_type,
                         is_temporal_type,
                         is_time_type,
                         is_date_type,
                         is_interval_type,
                         is_dictionary_type,
                         is_fixed_size_binary_type,
                         is_fixed_width_type,
                         is_var_length_list_type,
                         is_list_type,
                         is_list_like_type,
                         is_var_length_list_like_type,
                         is_list_view_type,
                         is_nested_type,
                         is_union_type,
                         is_bit_width_type,
                         is_offset_bit_width_type)

import pyarrow.lib as lib
from pyarrow.util import doc


_DATE_TYPES = {lib.Type_DATE32, lib.Type_DATE64}
_TIME_TYPES = {lib.Type_TIME32, lib.Type_TIME64}
_INTERVAL_TYPES = {lib.Type_INTERVAL_MONTH_DAY_NANO}
_TEMPORAL_TYPES = ({lib.Type_TIMESTAMP,
                    lib.Type_DURATION} | _TIME_TYPES | _DATE_TYPES |
                   _INTERVAL_TYPES)


@doc(datatype="null")
def is_null(t):
    """
    Return True if value is an instance of type: {datatype}.

    Parameters
    ----------
    t : DataType
    """
    return t.id == lib.Type_NA


@doc(is_null, datatype="boolean")
def is_boolean(t):
    return t.id == lib.Type_BOOL


@doc(is_null, datatype="any integer")
def is_integer(t):
    return is_integer_type(t)


@doc(is_null, datatype="signed integer")
def is_signed_integer(t):
    return is_signed_integer_type(t)


@doc(is_null, datatype="unsigned integer")
def is_unsigned_integer(t):
    return is_unsigned_integer_type(t)


@doc(is_null, datatype="int8")
def is_int8(t):
    return t.id == lib.Type_INT8


@doc(is_null, datatype="int16")
def is_int16(t):
    return t.id == lib.Type_INT16


@doc(is_null, datatype="int32")
def is_int32(t):
    return t.id == lib.Type_INT32


@doc(is_null, datatype="int64")
def is_int64(t):
    return t.id == lib.Type_INT64


@doc(is_null, datatype="uint8")
def is_uint8(t):
    return t.id == lib.Type_UINT8


@doc(is_null, datatype="uint16")
def is_uint16(t):
    return t.id == lib.Type_UINT16


@doc(is_null, datatype="uint32")
def is_uint32(t):
    return t.id == lib.Type_UINT32


@doc(is_null, datatype="uint64")
def is_uint64(t):
    return t.id == lib.Type_UINT64


@doc(is_null, datatype="floating point numeric")
def is_floating(t):
    return is_floating_type(t)


@doc(is_null, datatype="float16 (half-precision)")
def is_float16(t):
    return t.id == lib.Type_HALF_FLOAT


@doc(is_null, datatype="float32 (single precision)")
def is_float32(t):
    return t.id == lib.Type_FLOAT


@doc(is_null, datatype="float64 (double precision)")
def is_float64(t):
    return t.id == lib.Type_DOUBLE


@doc(is_null, datatype="list")
def is_list(t):
    return t.id == lib.Type_LIST


@doc(is_null, datatype="large list")
def is_large_list(t):
    return t.id == lib.Type_LARGE_LIST


@doc(is_null, datatype="fixed size list")
def is_fixed_size_list(t):
    return t.id == lib.Type_FIXED_SIZE_LIST


@doc(is_null, datatype="list view")
def is_list_view(t):
    return t.id == lib.Type_LIST_VIEW


@doc(is_null, datatype="large list view")
def is_large_list_view(t):
    return t.id == lib.Type_LARGE_LIST_VIEW


@doc(is_null, datatype="struct")
def is_struct(t):
    return t.id == lib.Type_STRUCT


@doc(is_null, datatype="union")
def is_union(t):
    return is_union_type(t)


@doc(is_null, datatype="nested type")
def is_nested(t):
    return is_nested_type(t)


@doc(is_null, datatype="run-end encoded")
def is_run_end_encoded(t):
    return t.id == lib.Type_RUN_END_ENCODED


@doc(is_null, datatype="date, time, timestamp or duration")
def is_temporal(t):
    return t.id in _TEMPORAL_TYPES


@doc(is_null, datatype="timestamp")
def is_timestamp(t):
    return t.id == lib.Type_TIMESTAMP


@doc(is_null, datatype="duration")
def is_duration(t):
    return t.id == lib.Type_DURATION


@doc(is_null, datatype="time")
def is_time(t):
    return is_time_type(t)


@doc(is_null, datatype="time32")
def is_time32(t):
    return t.id == lib.Type_TIME32


@doc(is_null, datatype="time64")
def is_time64(t):
    return t.id == lib.Type_TIME64


@doc(is_null, datatype="variable-length binary")
def is_binary(t):
    return t.id == lib.Type_BINARY


@doc(is_null, datatype="large variable-length binary")
def is_large_binary(t):
    return t.id == lib.Type_LARGE_BINARY


@doc(method="is_string")
def is_unicode(t):
    """
    Alias for {method}.

    Parameters
    ----------
    t : DataType
    """
    return is_string(t)


@doc(is_null, datatype="string (utf8 unicode)")
def is_string(t):
    return t.id == lib.Type_STRING


@doc(is_unicode, method="is_large_string")
def is_large_unicode(t):
    return is_large_string(t)


@doc(is_null, datatype="large string (utf8 unicode)")
def is_large_string(t):
    return t.id == lib.Type_LARGE_STRING


@doc(is_null, datatype="fixed size binary")
def is_fixed_size_binary(t):
    return is_fixed_size_binary_type(t)


@doc(is_null, datatype="variable-length binary view")
def is_binary_view(t):
    return t.id == lib.Type_BINARY_VIEW


@doc(is_null, datatype="variable-length string (utf-8) view")
def is_string_view(t):
    return t.id == lib.Type_STRING_VIEW


@doc(is_null, datatype="date")
def is_date(t):
    return is_date_type(t)


@doc(is_null, datatype="date32 (days)")
def is_date32(t):
    return t.id == lib.Type_DATE32


@doc(is_null, datatype="date64 (milliseconds)")
def is_date64(t):
    return t.id == lib.Type_DATE64


@doc(is_null, datatype="map")
def is_map(t):
    return t.id == lib.Type_MAP


@doc(is_null, datatype="decimal")
def is_decimal(t):
    return is_decimal_type(t)


@doc(is_null, datatype="decimal128")
def is_decimal128(t):
    return t.id == lib.Type_DECIMAL128


@doc(is_null, datatype="decimal256")
def is_decimal256(t):
    return t.id == lib.Type_DECIMAL256


@doc(is_null, datatype="dictionary-encoded")
def is_dictionary(t):
    return is_dictionary_type(t)


@doc(is_null, datatype="interval")
def is_interval(t):
    return is_interval_type(t)


@doc(is_null, datatype="primitive type")
def is_primitive(t):
    return is_primitive_type(t)
