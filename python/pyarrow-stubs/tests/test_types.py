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
# mypy: warn-unused-ignores

from typing import TYPE_CHECKING, Literal

import pyarrow.lib as lib
import pyarrow.types as types

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check

if TYPE_CHECKING:
    # The stubs have a class per primitive type, but at runtime the
    # factories return plain DataType instances, so these can only be
    # referred to from type checking code.
    from pyarrow.lib import (  # noqa: F401
        BoolType,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
        StringType,
        UInt8Type,
        UInt16Type,
        UInt32Type,
        UInt64Type,
    )

    IntegerType = (
        UInt8Type | Int8Type | UInt16Type | Int16Type
        | UInt32Type | Int32Type | UInt64Type | Int64Type
    )


def _erase(t: lib.DataType) -> lib.DataType:
    # Hide the precise type, so we can test narrowing by the is_* functions.
    return t


def test_primitive_factories() -> None:
    check(assert_type(lib.bool_(), "BoolType"), lib.DataType)
    check(assert_type(lib.int32(), "Int32Type"), lib.DataType)
    check(assert_type(lib.string(), "StringType"), lib.DataType)
    check(assert_type(lib.utf8(), "StringType"), lib.DataType)


def test_temporal_types() -> None:
    ts = lib.timestamp("ms", tz="UTC")
    check(assert_type(ts, lib.TimestampType), lib.TimestampType)
    check(assert_type(ts.unit, Literal["s", "ms", "us", "ns"]), str)
    check(assert_type(ts.tz, str | None), str)

    check(assert_type(lib.time32("s").unit, Literal["s", "ms"]), str)
    check(assert_type(lib.time64("ns").unit, Literal["us", "ns"]), str)
    check(assert_type(lib.duration("us"), lib.DurationType), lib.DurationType)


def test_decimal_types() -> None:
    dec = lib.decimal128(10, 2)
    check(assert_type(dec, lib.Decimal128Type), lib.Decimal128Type)
    check(assert_type(dec.precision, int), int)
    check(assert_type(dec.scale, int), int)


def test_list_types() -> None:
    lst = lib.list_(lib.int32())
    check(assert_type(lst, lib.ListType), lib.ListType)
    check(assert_type(lst.value_type, lib.DataType), lib.DataType)
    check(assert_type(lst.value_field, lib.Field), lib.Field)

    # With an explicit list_size we can't tell statically whether it is -1.
    fixed = lib.list_(lib.int32(), 2)
    check(assert_type(fixed, lib.ListType | lib.FixedSizeListType),
          lib.FixedSizeListType)

    check(assert_type(lib.large_list(lib.int32()), lib.LargeListType),
          lib.LargeListType)
    check(assert_type(lib.list_view(lib.int32()), lib.ListViewType),
          lib.ListViewType)


def test_dictionary_type() -> None:
    dict_type = lib.dictionary(lib.int8(), lib.string())
    check(assert_type(dict_type, lib.DictionaryType), lib.DictionaryType)
    check(assert_type(dict_type.ordered, bool), bool)
    check(assert_type(dict_type.value_type, lib.DataType), lib.DataType)
    check(assert_type(dict_type.index_type, "IntegerType"), lib.DataType)


def test_map_and_run_end_encoded_types() -> None:
    map_type = lib.map_(lib.string(), lib.int64())
    check(assert_type(map_type, lib.MapType), lib.MapType)
    check(assert_type(map_type.key_type, lib.DataType), lib.DataType)
    check(assert_type(map_type.keys_sorted, bool), bool)

    ree = lib.run_end_encoded(lib.int16(), lib.string())
    check(assert_type(ree, lib.RunEndEncodedType), lib.RunEndEncodedType)
    check(assert_type(ree.run_end_type, "Int16Type | Int32Type | Int64Type"),
          lib.DataType)


def test_field() -> None:
    field = lib.field("a", lib.int32())
    check(assert_type(field, lib.Field), lib.Field)
    check(assert_type(field.name, str), str)
    check(assert_type(field.type, lib.DataType), lib.DataType)
    check(assert_type(field.nullable, bool), bool)
    check(assert_type(field.with_nullable(False), lib.Field), lib.Field)
    check(assert_type(lib.field("b", "string"), lib.Field), lib.Field)


def test_schema() -> None:
    schema = lib.schema([("a", lib.int32()), lib.field("b", lib.string())])
    check(assert_type(schema, lib.Schema), lib.Schema)
    check(assert_type(schema.names, list[str]), list)
    check(assert_type(schema.types, list[lib.DataType]), list)
    check(assert_type(schema.field("a"), lib.Field), lib.Field)
    check(assert_type(schema[0], lib.Field), lib.Field)


def test_type_predicates_narrow() -> None:
    lst = _erase(lib.list_(lib.int32()))
    assert types.is_list(lst)
    check(assert_type(lst, lib.ListType), lib.ListType)

    ts = _erase(lib.timestamp("s"))
    assert types.is_timestamp(ts)
    check(assert_type(ts, lib.TimestampType), lib.TimestampType)

    dec = _erase(lib.decimal64(5))
    assert types.is_decimal(dec)
    check(
        assert_type(
            dec,
            lib.Decimal32Type | lib.Decimal64Type | lib.Decimal128Type
            | lib.Decimal256Type,
        ),
        lib.Decimal64Type,
    )


def test_invalid_usage() -> None:
    if TYPE_CHECKING_INVALID_USAGE:
        # A Field needs a type unless it's created from an Arrow C schema.
        lib.field("a")  # type: ignore[call-overload]
        lib.list_(1)  # type: ignore[call-overload]
        lib.decimal128("10")  # type: ignore[arg-type]
