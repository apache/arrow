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
                         _is_integer,
                         _is_signed_integer,
                         _is_unsigned_integer,
                         _is_floating,
                         _is_numeric,
                         _is_decimal,
                         _is_run_end_type,
                         _is_primitive,
                         _is_base_binary_like,
                         _is_binary_like,
                         _is_large_binary_like,
                         _is_binary,
                         _is_string,
                         _is_temporal,
                         _is_time,
                         _is_date,
                         _is_interval,
                         _is_dictionary,
                         _is_fixed_size_binary,
                         _is_fixed_width,
                         _is_var_length_list,
                         _is_list,
                         _is_list_like,
                         _is_var_length_list_like,
                         _is_list_view,
                         _is_nested,
                         _is_union,
                         _bit_width,
                         _offset_bit_width)

import pyarrow.lib as lib
from pyarrow.util import doc


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
    return _is_primitive(t) and _bit_width(t) == 1


@doc(is_null, datatype="any integer")
def is_integer(t):
    return _is_integer(t) and _bit_width(t)


@doc(is_null, datatype="signed integer")
def is_signed_integer(t):
    return _is_signed_integer(t)


@doc(is_null, datatype="unsigned integer")
def is_unsigned_integer(t):
    return _is_unsigned_integer(t)


@doc(is_null, datatype="int8")
def is_int8(t):
    return _is_integer(t) and _bit_width(t) == 8


@doc(is_null, datatype="int16")
def is_int16(t):
    return _is_integer(t) and _bit_width(t) == 16


@doc(is_null, datatype="int32")
def is_int32(t):
    return _is_integer(t) and _bit_width(t) == 32


@doc(is_null, datatype="int64")
def is_int64(t):
    return _is_integer(t) and _bit_width(t) == 64


@doc(is_null, datatype="uint8")
def is_uint8(t):
    return _is_unsigned_integer(t) and _bit_width(t) == 8


@doc(is_null, datatype="uint16")
def is_uint16(t):
    return _is_unsigned_integer(t) and _bit_width(t) == 16


@doc(is_null, datatype="uint32")
def is_uint32(t):
    return _is_unsigned_integer(t) and _bit_width(t) == 32


@doc(is_null, datatype="uint64")
def is_uint64(t):
    return _is_unsigned_integer(t) and _bit_width(t) == 64


@doc(is_null, datatype="floating point numeric")
def is_floating(t):
    return _is_floating(t)


@doc(is_null, datatype="float16 (half-precision)")
def is_float16(t):
    return _is_floating(t) and _bit_width(t) == 16


@doc(is_null, datatype="float32 (single precision)")
def is_float32(t):
    return _is_floating(t) and _bit_width(t) == 32


@doc(is_null, datatype="float64 (double precision)")
def is_float64(t):
    return _is_floating(t) and _bit_width(t) == 64


@doc(is_null, datatype="list")
def is_list(t):
    return _is_list(t) and _offset_bit_width(t) == 32


@doc(is_null, datatype="large list")
def is_large_list(t):
    return _is_list(t) and _offset_bit_width(t) == 64


@doc(is_null, datatype="fixed size list")
def is_fixed_size_list(t):
    return t.id == lib.Type_FIXED_SIZE_LIST


@doc(is_null, datatype="list view")
def is_list_view(t):
    return _is_list_view(t) and _offset_bit_width(t) == 32


@doc(is_null, datatype="large list view")
def is_large_list_view(t):
    return _is_list_view(t) and _offset_bit_width(t) == 64


@doc(is_null, datatype="struct")
def is_struct(t):
    return t.id == lib.Type_STRUCT


@doc(is_null, datatype="union")
def is_union(t):
    return _is_union(t)


@doc(is_null, datatype="nested type")
def is_nested(t):
    return _is_nested(t)


@doc(is_null, datatype="run-end encoded")
def is_run_end_encoded(t):
    return t.id == lib.Type_RUN_END_ENCODED


@doc(is_null, datatype="date, time, timestamp or duration")
def is_temporal(t):
    return _is_primitive(t) and not _is_integer(t) and \
        not _is_floating(t)


@doc(is_null, datatype="timestamp")
def is_timestamp(t):
    return _is_temporal(t) and not _is_time(t) and not _is_date(t)


@doc(is_null, datatype="duration")
def is_duration(t):
    return _is_primitive(t) and not _is_integer(t) and \
        not _is_floating(t) and not _is_temporal(t) and \
        not _is_interval(t)


@doc(is_null, datatype="time")
def is_time(t):
    return _is_time(t)


@doc(is_null, datatype="time32")
def is_time32(t):
    return _is_time(t) and _bit_width(t) == 32


@doc(is_null, datatype="time64")
def is_time64(t):
    return _is_time(t) and _bit_width(t) == 64


@doc(is_null, datatype="variable-length binary")
def is_binary(t):
    return _is_binary(t) and _offset_bit_width(t) == 32


@doc(is_null, datatype="large variable-length binary")
def is_large_binary(t):
    return _is_binary(t) and _offset_bit_width(t) == 64


@doc(method="_is_string")
def is_unicode(t):
    """
    Alias for {method}.

    Parameters
    ----------
    t : DataType
    """
    return _is_string(t)


@doc(is_null, datatype="string (utf8 unicode)")
def is_string(t):
    return _is_string(t) and _offset_bit_width(t) == 32


@doc(is_unicode, method="_is_large_string")
def is_large_unicode(t):
    return is_large_string(t)


@doc(is_null, datatype="large string (utf8 unicode)")
def is_large_string(t):
    return _is_string(t) and _offset_bit_width(t) == 64


@doc(is_null, datatype="fixed size binary")
def is_fixed_size_binary(t):
    return _is_fixed_size_binary(t)


@doc(is_null, datatype="variable-length binary view")
def is_binary_view(t):
    return t.id == lib.Type_BINARY_VIEW


@doc(is_null, datatype="variable-length string (utf-8) view")
def is_string_view(t):
    return t.id == lib.Type_STRING_VIEW


@doc(is_null, datatype="date")
def is_date(t):
    return _is_date(t)


@doc(is_null, datatype="date32 (days)")
def is_date32(t):
    return _is_date(t) and _bit_width(t) == 32


@doc(is_null, datatype="date64 (milliseconds)")
def is_date64(t):
    return _is_date(t) and _bit_width(t) == 64


@doc(is_null, datatype="map")
def is_map(t):
    return _is_var_length_list(t) and not _is_list(t)


@doc(is_null, datatype="decimal")
def is_decimal(t):
    return _is_decimal(t)


@doc(is_null, datatype="decimal128")
def is_decimal128(t):
    return _is_decimal(t) and _bit_width(t) == 128


@doc(is_null, datatype="decimal256")
def is_decimal256(t):
    return _is_decimal(t) and _bit_width(t) == 256


@doc(is_null, datatype="dictionary-encoded")
def is_dictionary(t):
    return _is_dictionary(t)


@doc(is_null, datatype="interval")
def is_interval(t):
    return _is_interval(t)


@doc(is_null, datatype="primitive type")
def is_primitive(t):
    return _is_primitive(t)
