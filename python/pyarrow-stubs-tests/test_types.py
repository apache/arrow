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

from collections.abc import Hashable, Iterator
from typing import Literal

from typing_extensions import CapsuleType

import pyarrow as pa
from pyarrow._stubs_typing import (
    BoolType,
    Int16Type,
    Int32Type,
    Int64Type,
    IntegerType,
    StringType,
)
import pyarrow.types as types

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check


def _erase(t: pa.DataType) -> pa.DataType:
    # Hide the precise type, so we can test narrowing by the is_* functions.
    return t


def test_primitive_factories() -> None:
    check(assert_type(pa.bool_(), BoolType), pa.DataType)
    check(assert_type(pa.int32(), Int32Type), pa.DataType)
    check(assert_type(pa.string(), StringType), pa.DataType)
    check(assert_type(pa.utf8(), StringType), pa.DataType)


def test_temporal_types() -> None:
    ts = pa.timestamp("ms", tz="UTC")
    check(assert_type(ts, pa.TimestampType), pa.TimestampType)
    check(assert_type(ts.unit, Literal["s", "ms", "us", "ns"]), str)
    check(assert_type(ts.tz, str | None), str)

    check(assert_type(pa.time32("s").unit, Literal["s", "ms"]), str)
    check(assert_type(pa.time64("ns").unit, Literal["us", "ns"]), str)
    check(assert_type(pa.duration("us"), pa.DurationType), pa.DurationType)


def test_decimal_types() -> None:
    dec = pa.decimal128(10, 2)
    check(assert_type(dec, pa.Decimal128Type), pa.Decimal128Type)
    check(assert_type(dec.precision, int), int)
    check(assert_type(dec.scale, int), int)


def test_list_types() -> None:
    lst = pa.list_(pa.int32())
    check(assert_type(lst, pa.ListType), pa.ListType)
    check(assert_type(lst.value_type, pa.DataType), pa.DataType)
    check(assert_type(lst.value_field, pa.Field), pa.Field)

    variable = pa.list_(pa.int32(), -1)
    check(assert_type(variable, pa.ListType), pa.ListType)
    fixed = pa.list_(pa.int32(), 2)
    check(assert_type(fixed, pa.ListType | pa.FixedSizeListType),
          pa.FixedSizeListType)

    check(assert_type(pa.large_list(pa.int32()), pa.LargeListType),
          pa.LargeListType)
    check(assert_type(pa.list_view(pa.int32()), pa.ListViewType),
          pa.ListViewType)


def test_dictionary_type() -> None:
    dict_type = pa.dictionary(pa.int8(), pa.string())
    check(assert_type(dict_type, pa.DictionaryType), pa.DictionaryType)
    check(assert_type(dict_type.ordered, bool), bool)
    check(assert_type(dict_type.value_type, pa.DataType), pa.DataType)
    check(assert_type(dict_type.index_type, IntegerType), pa.DataType)


def test_map_and_run_end_encoded_types() -> None:
    map_type = pa.map_(pa.string(), pa.int64())
    check(assert_type(map_type, pa.MapType), pa.MapType)
    check(assert_type(map_type.key_type, pa.DataType), pa.DataType)
    check(assert_type(map_type.keys_sorted, bool), bool)

    ree = pa.run_end_encoded(pa.int16(), pa.string())
    check(assert_type(ree, pa.RunEndEncodedType), pa.RunEndEncodedType)
    check(assert_type(ree.run_end_type, Int16Type | Int32Type | Int64Type),
          pa.DataType)


def test_data_type_hashable() -> None:
    data_type = pa.int32()
    check(assert_type(hash(data_type), int), int)


def test_field() -> None:
    field = pa.field("a", pa.int32())
    check(assert_type(field, pa.Field), pa.Field)
    check(assert_type(field.name, str), str)
    check(assert_type(field.metadata, dict[bytes, bytes] | None), type(None))
    check(assert_type(field.type, pa.DataType), pa.DataType)
    check(assert_type(field.nullable, bool), bool)
    check(assert_type(field.with_nullable(False), pa.Field), pa.Field)
    check(assert_type(pa.field("b", "string"), pa.Field), pa.Field)

    field = pa.field("a", pa.int32(), metadata={"k": "v"})
    check(assert_type(field.metadata, dict[bytes, bytes] | None), dict)


def test_schema() -> None:
    schema = pa.schema([("a", pa.int32()), pa.field("b", pa.string())])
    check(assert_type(schema, pa.Schema), pa.Schema)
    check(assert_type(schema.names, list[str]), list)
    check(assert_type(schema.types, list[pa.DataType]), list)
    check(assert_type(schema.field("a"), pa.Field), pa.Field)
    check(assert_type(schema[0], pa.Field), pa.Field)


def test_key_value_metadata() -> None:
    # The runtime yields generators instead of Mapping views (duplicated keys)
    metadata = pa.KeyValueMetadata([(b"a", b"1"), (b"a", b"2")])
    check(assert_type(metadata.keys(), Iterator[bytes]), Iterator)
    check(assert_type(metadata.values(), Iterator[bytes]), Iterator)
    check(assert_type(metadata.items(), Iterator[tuple[bytes, bytes]]),
          Iterator)


def test_type_predicates_narrow() -> None:
    lst = _erase(pa.list_(pa.int32()))
    assert types.is_list(lst)
    check(assert_type(lst, pa.ListType), pa.ListType)

    ts = _erase(pa.timestamp("s"))
    assert types.is_timestamp(ts)
    check(assert_type(ts, pa.TimestampType), pa.TimestampType)

    dec = _erase(pa.decimal64(5))
    assert types.is_decimal(dec)
    check(
        assert_type(
            dec,
            pa.Decimal32Type | pa.Decimal64Type | pa.Decimal128Type
            | pa.Decimal256Type,
        ),
        pa.Decimal64Type,
    )

    # Decimal types subclass FixedSizeBinaryType, but don't match it, so
    # they must not be narrowed away when the check fails.
    dec128 = _erase(pa.decimal128(5, 2))
    if not types.is_fixed_size_binary(dec128):
        assert types.is_decimal(dec128)
        check(
            assert_type(
                dec128,
                pa.Decimal32Type | pa.Decimal64Type | pa.Decimal128Type
                | pa.Decimal256Type,
            ),
            pa.Decimal128Type,
        )

    # Run-end encoded types are nested, so this doesn't narrow to Never.
    ree = pa.run_end_encoded(pa.int32(), pa.string())
    assert types.is_nested(ree)
    check(assert_type(ree, pa.RunEndEncodedType), pa.RunEndEncodedType)


def test_import_from_c_capsule() -> None:
    capsule = pa.int32().__arrow_c_schema__()
    check(assert_type(capsule, CapsuleType), CapsuleType)

    # The import methods are static and return whatever type the C schema
    # holds, not the class they are called on.
    imported = pa.ListType._import_from_c_capsule(capsule)
    check(assert_type(imported, pa.DataType), pa.DataType)
    assert not isinstance(imported, pa.ListType)

    field = pa.field("a", pa.int32())
    field_capsule = field.__arrow_c_schema__()
    check(assert_type(field_capsule, CapsuleType), CapsuleType)

    imported_field = pa.Field._import_from_c_capsule(field_capsule)
    check(assert_type(imported_field, pa.Field), pa.Field)

    schema = pa.schema([field])
    schema_capsule = schema.__arrow_c_schema__()
    check(assert_type(schema_capsule, CapsuleType), CapsuleType)

    imported_schema = pa.Schema._import_from_c_capsule(schema_capsule)
    check(assert_type(imported_schema, pa.Schema), pa.Schema)


def test_invalid_usage() -> None:
    if TYPE_CHECKING_INVALID_USAGE:
        # A Field needs a type unless it's created from an Arrow C schema.
        pa.field("a")  # type: ignore[call-overload]
        pa.list_(1)  # type: ignore[call-overload]
        pa.decimal128("10")  # type: ignore[arg-type]
        _: Hashable = pa.KeyValueMetadata({b"a": b"1"})  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501
