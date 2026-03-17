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

import datetime as dt  # noqa: F401

from collections.abc import Iterable, Iterator, Mapping, Sequence
from decimal import Decimal  # noqa: F401
from typing import Any, Generic, Literal, Protocol, TypeAlias

import numpy as np
import pandas as pd

from typing_extensions import Self, TypeVar, deprecated

from pyarrow._stubs_typing import SupportsArrowSchema, TimeUnit
from pyarrow.lib import (  # noqa: F401
    Array,
    Buffer,
    ChunkedArray,
    ExtensionArray,
    ExtensionScalar,
    MemoryPool,
    MonthDayNano,
    Table,
)

class _Weakrefable: ...
class _Metadata(_Weakrefable): ...

class DataType(_Weakrefable):
    def field(self, i: int) -> Field[Any]: ...
    @property
    def id(self) -> int: ...
    @property
    def bit_width(self) -> int: ...
    @property
    def byte_width(self) -> int: ...
    @property
    def num_fields(self) -> int: ...
    @property
    def num_buffers(self) -> int: ...
    @property
    def has_variadic_buffers(self) -> bool: ...

    def __hash__(self) -> int: ...
    def equals(
        self, other: DataType | str, *, check_metadata: bool = False
    ) -> bool: ...
    def to_pandas_dtype(self) -> np.generic: ...
    def _export_to_c(self, out_ptr: int) -> None: ...
    @classmethod
    def _import_from_c(cls, in_ptr: int) -> Self: ...
    def __arrow_c_schema__(self) -> Any: ...
    @classmethod
    def _import_from_c_capsule(cls, schema) -> Self: ...

_AsPyType = TypeVar("_AsPyType")
_DataTypeT = TypeVar("_DataTypeT", bound=DataType)
_DataTypeT_co = TypeVar("_DataTypeT_co", bound=DataType, covariant=True)

class _BasicDataType(DataType, Generic[_AsPyType]): ...
class NullType(_BasicDataType[None]): ...
class BoolType(_BasicDataType[bool]): ...
class UInt8Type(_BasicDataType[int]): ...
class Int8Type(_BasicDataType[int]): ...
class UInt16Type(_BasicDataType[int]): ...
class Int16Type(_BasicDataType[int]): ...
class UInt32Type(_BasicDataType[int]): ...
class Int32Type(_BasicDataType[int]): ...
class UInt64Type(_BasicDataType[int]): ...
class Int64Type(_BasicDataType[int]): ...
class Float16Type(_BasicDataType[float]): ...
class Float32Type(_BasicDataType[float]): ...
class Float64Type(_BasicDataType[float]): ...
class Date32Type(_BasicDataType[dt.date]): ...
class Date64Type(_BasicDataType[dt.date]): ...
class MonthDayNanoIntervalType(_BasicDataType[MonthDayNano]): ...
class StringType(_BasicDataType[str]): ...
class LargeStringType(_BasicDataType[str]): ...
class StringViewType(_BasicDataType[str]): ...
class BinaryType(_BasicDataType[bytes]): ...
class LargeBinaryType(_BasicDataType[bytes]): ...
class BinaryViewType(_BasicDataType[bytes]): ...

_Unit = TypeVar("_Unit", bound=TimeUnit, default=Literal["us"])
_Tz = TypeVar("_Tz", str, None, default=None)

class TimestampType(_BasicDataType[int], Generic[_Unit, _Tz]):
    @property
    def unit(self) -> _Unit: ...
    @property
    def tz(self) -> _Tz: ...

_Time32Unit = TypeVar("_Time32Unit", bound=Literal["s", "ms"])

class Time32Type(_BasicDataType[dt.time], Generic[_Time32Unit]):
    @property
    def unit(self) -> _Time32Unit: ...

_Time64Unit = TypeVar("_Time64Unit", bound=Literal["us", "ns"])

class Time64Type(_BasicDataType[dt.time], Generic[_Time64Unit]):
    @property
    def unit(self) -> _Time64Unit: ...

class DurationType(_BasicDataType[dt.timedelta], Generic[_Unit]):
    @property
    def unit(self) -> _Unit: ...

_FixedSizeBinaryAsPyType = TypeVar("_FixedSizeBinaryAsPyType", default=bytes)

class FixedSizeBinaryType(_BasicDataType[_FixedSizeBinaryAsPyType]): ...

_Precision = TypeVar("_Precision", default=Any)
_Scale = TypeVar("_Scale", default=Any)
_Precision_co = TypeVar("_Precision_co", default=Any, covariant=True)
_Scale_co = TypeVar("_Scale_co", default=Any, covariant=True)

class _HasPrecisionScale(Protocol[_Precision_co, _Scale_co]):
    @property
    def precision(self) -> _Precision_co: ...
    @property
    def scale(self) -> _Scale_co: ...

class Decimal32Type(
    FixedSizeBinaryType[Decimal], _HasPrecisionScale[_Precision_co, _Scale_co]
): ...

class Decimal64Type(
    FixedSizeBinaryType[Decimal], _HasPrecisionScale[_Precision_co, _Scale_co]
): ...

class Decimal128Type(
    FixedSizeBinaryType[Decimal], _HasPrecisionScale[_Precision_co, _Scale_co]
): ...

class Decimal256Type(
    FixedSizeBinaryType[Decimal], _HasPrecisionScale[_Precision_co, _Scale_co]
): ...

class ListType(DataType, Generic[_DataTypeT_co]):
    @property
    def value_field(self) -> Field[_DataTypeT_co]: ...
    @property
    def value_type(self) -> _DataTypeT_co: ...

class LargeListType(DataType, Generic[_DataTypeT_co]):
    @property
    def value_field(self) -> Field[_DataTypeT_co]: ...
    @property
    def value_type(self) -> _DataTypeT_co: ...

class ListViewType(DataType, Generic[_DataTypeT_co]):
    @property
    def value_field(self) -> Field[_DataTypeT_co]: ...
    @property
    def value_type(self) -> _DataTypeT_co: ...

class LargeListViewType(DataType, Generic[_DataTypeT_co]):
    @property
    def value_field(self) -> Field[_DataTypeT_co]: ...
    @property
    def value_type(self) -> _DataTypeT_co: ...

class FixedSizeListType(DataType, Generic[_DataTypeT_co, _Size]):
    @property
    def value_field(self) -> Field[_DataTypeT_co]: ...
    @property
    def value_type(self) -> _DataTypeT_co: ...
    @property
    def list_size(self) -> int: ...

class DictionaryMemo(_Weakrefable): ...

_IndexT = TypeVar(
    "_IndexT",
    UInt8Type,
    Int8Type,
    UInt16Type,
    Int16Type,
    UInt32Type,
    Int32Type,
    UInt64Type,
    Int64Type,
)
_BasicValueT = TypeVar("_BasicValueT", bound=_BasicDataType, default=_BasicDataType)
_ValueT = TypeVar("_ValueT", bound=DataType, default=DataType)
_K = TypeVar("_K", bound=DataType, default=DataType)
_Ordered = TypeVar("_Ordered", Literal[True], Literal[False], default=Literal[False])

class DictionaryType(DataType, Generic[_IndexT, _BasicValueT, _Ordered]):
    @property
    def ordered(self) -> _Ordered: ...
    @property
    def index_type(self) -> _IndexT: ...
    @property
    def value_type(self) -> _BasicValueT: ...

class MapType(DataType, Generic[_K, _ValueT, _Ordered]):
    @property
    def key_field(self) -> Field[_K]: ...
    @property
    def key_type(self) -> _K: ...
    @property
    def item_field(self) -> Field[_ValueT]: ...
    @property
    def item_type(self) -> _ValueT: ...
    @property
    def keys_sorted(self) -> _Ordered: ...

_Size = TypeVar("_Size", default=int)

class StructType(DataType):
    def get_field_index(self, name: str) -> int: ...
    def field(self, i: int | str) -> Field: ...
    def get_all_field_indices(self, name: str) -> list[int]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Field]: ...

    __getitem__ = field
    @property
    def names(self) -> list[str]: ...
    @property
    def fields(self) -> list[Field]: ...

class UnionType(DataType):
    @property
    def mode(self) -> Literal["sparse", "dense"]: ...
    @property
    def type_codes(self) -> list[int]: ...
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[Field]: ...
    def field(self, i: int) -> Field: ...

    __getitem__ = field

class SparseUnionType(UnionType):
    @property
    def mode(self) -> Literal["sparse"]: ...

class DenseUnionType(UnionType):
    @property
    def mode(self) -> Literal["dense"]: ...

_RunEndType = TypeVar("_RunEndType", Int16Type, Int32Type, Int64Type)

class RunEndEncodedType(DataType, Generic[_RunEndType, _BasicValueT]):
    @property
    def run_end_type(self) -> _RunEndType: ...
    @property
    def value_type(self) -> _BasicValueT: ...

_StorageT = TypeVar("_StorageT", bound=Array | ChunkedArray)

class BaseExtensionType(DataType):
    def __arrow_ext_class__(self) -> type[ExtensionArray]: ...
    def __arrow_ext_scalar_class__(self) -> type[ExtensionScalar]: ...
    @property
    def extension_name(self) -> str: ...
    @property
    def storage_type(self) -> DataType: ...
    def wrap_array(self, storage: _StorageT) -> _StorageT: ...

class ExtensionType(BaseExtensionType):
    def __init__(self, storage_type: DataType, extension_name: str) -> None: ...
    def __arrow_ext_serialize__(self) -> bytes: ...
    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: DataType, serialized: bytes
    ) -> Self: ...

class FixedShapeTensorType(BaseExtensionType, Generic[_ValueT]):
    @property
    def value_type(self) -> _ValueT: ...
    @property
    def shape(self) -> list[int]: ...
    @property
    def dim_names(self) -> list[str] | None: ...
    @property
    def permutation(self) -> list[int] | None: ...

class Bool8Type(BaseExtensionType): ...
class UuidType(BaseExtensionType): ...
class JsonType(BaseExtensionType): ...

class OpaqueType(BaseExtensionType):
    @property
    def type_name(self) -> str: ...
    @property
    def vendor_name(self) -> str: ...

class UnknownExtensionType(ExtensionType):
    def __init__(self, storage_type: DataType, serialized: bytes) -> None: ...

def register_extension_type(ext_type: ExtensionType) -> None: ...
def unregister_extension_type(type_name: str) -> None: ...

_StrOrBytes: TypeAlias = str | bytes
_MetadataMapping: TypeAlias = Mapping[_StrOrBytes, _StrOrBytes]
_MetadataIterable: TypeAlias = Iterable[tuple[_StrOrBytes, _StrOrBytes]]
_KeyValueMetadataInput: TypeAlias = _MetadataMapping | _MetadataIterable | None
_DataTypeAlias: TypeAlias = Literal[
    "null",
    "bool",
    "boolean",
    "i1",
    "int8",
    "i2",
    "int16",
    "i4",
    "int32",
    "i8",
    "int64",
    "u1",
    "uint8",
    "u2",
    "uint16",
    "u4",
    "uint32",
    "u8",
    "uint64",
    "f2",
    "halffloat",
    "float16",
    "f4",
    "float",
    "float32",
    "f8",
    "double",
    "float64",
    "string",
    "str",
    "utf8",
    "binary",
    "large_string",
    "large_str",
    "large_utf8",
    "large_binary",
    "binary_view",
    "string_view",
    "date32",
    "date64",
    "date32[day]",
    "date64[ms]",
    "time32[s]",
    "time32[ms]",
    "time64[us]",
    "time64[ns]",
    "timestamp[s]",
    "timestamp[ms]",
    "timestamp[us]",
    "timestamp[ns]",
    "duration[s]",
    "duration[ms]",
    "duration[us]",
    "duration[ns]",
    "month_day_nano_interval",
]
_DataTypeAliasInput: TypeAlias = _DataTypeAlias | str
_DataTypeLike: TypeAlias = DataType | _DataTypeAliasInput
_FieldTypeInput: TypeAlias = _DataTypeLike | None
_SchemaMetadataInput: TypeAlias = (
    Mapping[bytes, bytes]
    | Mapping[str, str]
    | Mapping[bytes, str]
    | Mapping[str, bytes]
)

class KeyValueMetadata(_Metadata, Mapping[bytes, bytes]):
    def __init__(
        self,
        __arg0__: _KeyValueMetadataInput | KeyValueMetadata = None,
        **kwargs: str,
    ) -> None: ...
    def equals(self, other: KeyValueMetadata) -> bool: ...
    def __len__(self) -> int: ...
    def __contains__(self, /, __key: object) -> bool: ...
    def __getitem__(self, /, __key: Any) -> Any: ...
    def __iter__(self) -> Iterator[bytes]: ...
    def get_all(self, key: str) -> list[bytes]: ...
    def to_dict(self) -> dict[bytes, bytes]: ...

class Field(_Weakrefable, Generic[_DataTypeT_co]):
    def equals(self, other: Field, check_metadata: bool = False) -> bool: ...
    def __hash__(self) -> int: ...
    @property
    def nullable(self) -> bool: ...
    @property
    def name(self) -> str: ...
    @property
    def metadata(self) -> dict[bytes, bytes] | None: ...
    @property
    def type(self) -> _DataTypeT_co: ...
    def with_metadata(
        self,
        metadata: _MetadataMapping | Any,
    ) -> Self: ...
    def remove_metadata(self) -> Self: ...
    def with_type(self, new_type: DataType) -> Field: ...
    def with_name(self, name: str) -> Self: ...
    def with_nullable(self, nullable: bool) -> Field[_DataTypeT_co]: ...
    def flatten(self) -> list[Field]: ...
    def _export_to_c(self, out_ptr: int) -> None: ...
    @classmethod
    def _import_from_c(cls, in_ptr: int) -> Self: ...
    def __arrow_c_schema__(self) -> Any: ...
    @classmethod
    def _import_from_c_capsule(cls, schema) -> Self: ...

_StructFieldTuple: TypeAlias = (
    tuple[str, Field[Any] | None] | tuple[str, _FieldTypeInput]
)
_StructFieldsInput: TypeAlias = (
    Iterable[Field[Any] | _StructFieldTuple]
    | Mapping[str, Field[Any] | _FieldTypeInput]
)

class Schema(_Weakrefable):
    def __len__(self) -> int: ...
    def __getitem__(self, key: str | int) -> Field: ...

    _field = __getitem__
    def __iter__(self) -> Iterator[Field]: ...
    def __hash__(self) -> int: ...
    def __sizeof__(self) -> int: ...
    @property
    def pandas_metadata(self) -> dict: ...
    @property
    def names(self) -> list[str]: ...
    @property
    def types(self) -> list[DataType]: ...
    @property
    def metadata(self) -> dict[bytes, bytes]: ...
    def empty_table(self) -> Table: ...
    def equals(self, other: Schema, check_metadata: bool = False) -> bool: ...
    @classmethod
    def from_pandas(
        cls, df: pd.DataFrame, preserve_index: bool | None = None
    ) -> Schema: ...
    def field(self, i: int | str | bytes) -> Field: ...
    @deprecated("Use 'field' instead")
    def field_by_name(self, name: str) -> Field: ...
    def get_field_index(self, name: str) -> int: ...
    def get_all_field_indices(self, name: str) -> list[int]: ...
    def append(self, field: Field) -> Schema: ...
    def insert(self, i: int, field: Field) -> Schema: ...
    def remove(self, i: int) -> Schema: ...
    def set(self, i: int, field: Field) -> Schema: ...
    @deprecated("Use 'with_metadata' instead")
    def add_metadata(self, metadata: _MetadataMapping) -> Schema: ...
    def with_metadata(self, metadata: _MetadataMapping) -> Schema: ...
    def serialize(self, memory_pool: MemoryPool | None = None) -> Buffer: ...
    def remove_metadata(self) -> Schema: ...
    def to_string(
        self,
        truncate_metadata: bool = True,
        show_field_metadata: bool = True,
        show_schema_metadata: bool = True,
        element_size_limit: int | None = None,
    ) -> str: ...
    def _export_to_c(self, out_ptr: int) -> None: ...
    @classmethod
    def _import_from_c(cls, in_ptr: int) -> Schema: ...
    def __arrow_c_schema__(self) -> Any: ...
    @staticmethod
    def _import_from_c_capsule(schema: Any) -> Schema: ...

def unify_schemas(
    schemas: Sequence[Schema],
    *,
    promote_options: Literal["default", "permissive"] = "default",
) -> Schema: ...
def field(
    name: SupportsArrowSchema | str | bytes,
    type: _DataTypeT | _DataTypeAliasInput | None = None,
    nullable: bool = True,
    metadata: _MetadataMapping | None = None,
) -> Field[_DataTypeT] | Field[Any]: ...
def null() -> NullType: ...
def bool_() -> BoolType: ...
def uint8() -> UInt8Type: ...
def int8() -> Int8Type: ...
def uint16() -> UInt16Type: ...
def int16() -> Int16Type: ...
def uint32() -> UInt32Type: ...
def int32() -> Int32Type: ...
def int64() -> Int64Type: ...
def uint64() -> UInt64Type: ...
def timestamp(
    unit: _Unit | str, tz: _Tz | None = None
) -> TimestampType[_Unit, _Tz]: ...
def time32(unit: _Time32Unit | str) -> Time32Type[_Time32Unit]: ...
def time64(unit: _Time64Unit | str) -> Time64Type[_Time64Unit]: ...
def duration(unit: _Unit | str) -> DurationType[_Unit]: ...
def month_day_nano_interval() -> MonthDayNanoIntervalType: ...
def date32() -> Date32Type: ...
def date64() -> Date64Type: ...
def float16() -> Float16Type: ...
def float32() -> Float32Type: ...
def float64() -> Float64Type: ...
def decimal32(
    precision: _Precision, scale: _Scale | Literal[0] = 0
) -> Decimal32Type[_Precision, _Scale | Literal[0]]: ...
def decimal64(
    precision: _Precision, scale: _Scale | Literal[0] = 0
) -> Decimal64Type[_Precision, _Scale | Literal[0]]: ...
def decimal128(
    precision: _Precision, scale: _Scale | Literal[0] = 0
) -> Decimal128Type[_Precision, _Scale | Literal[0]]: ...
def decimal256(
    precision: _Precision, scale: _Scale | Literal[0] = 0
) -> Decimal256Type[_Precision, _Scale | Literal[0]]: ...
def string() -> StringType: ...

utf8 = string

def binary(
    length: Literal[-1] | int = ...,
) -> BinaryType | FixedSizeBinaryType[bytes]: ...
def large_binary() -> LargeBinaryType: ...
def large_string() -> LargeStringType: ...

large_utf8 = large_string

def binary_view() -> BinaryViewType: ...
def string_view() -> StringViewType: ...
def list_(
    value_type: _DataTypeT | Field[_DataTypeT] | None = None,
    list_size: Literal[-1] | _Size | None = None,
) -> ListType[_DataTypeT] | FixedSizeListType[_DataTypeT, _Size]: ...
def large_list(
    value_type: _DataTypeT | Field[_DataTypeT] | None = None,
) -> LargeListType[_DataTypeT]: ...
def list_view(
    value_type: _DataTypeT | Field[_DataTypeT] | None = None,
) -> ListViewType[_DataTypeT]: ...
def large_list_view(
    value_type: _DataTypeT | Field[_DataTypeT] | None = None,
) -> LargeListViewType[_DataTypeT]: ...
def map_(
    key_type: _K | Field | _FieldTypeInput = None,
    item_type: _ValueT | Field | _FieldTypeInput = None,
    keys_sorted: _Ordered | None = None,
) -> MapType[_K, _ValueT, _Ordered]: ...
def dictionary(
    index_type: _IndexT | _DataTypeAliasInput,
    value_type: _BasicValueT | _DataTypeAliasInput,
    ordered: _Ordered | None = None,
) -> DictionaryType[_IndexT, _BasicValueT, _Ordered]: ...
def struct(
    fields: _StructFieldsInput,
) -> StructType: ...
def sparse_union(
    child_fields: list[Field[Any]], type_codes: list[int] | None = None
) -> SparseUnionType: ...
def dense_union(
    child_fields: list[Field[Any]], type_codes: list[int] | None = None
) -> DenseUnionType: ...
def union(
    child_fields: list[Field[Any]],
    mode: Literal["sparse", "dense"] | int,
    type_codes: list[int] | None = None,
) -> SparseUnionType | DenseUnionType: ...
def run_end_encoded(
    run_end_type: _RunEndType | _DataTypeAliasInput | None,
    value_type: _BasicValueT | _DataTypeAliasInput | None,
) -> RunEndEncodedType[_RunEndType, _BasicValueT]: ...
def json_(storage_type: DataType = ...) -> JsonType: ...
def uuid() -> UuidType: ...
def fixed_shape_tensor(
    value_type: _ValueT,
    shape: Sequence[int],
    dim_names: Sequence[str] | None = None,
    permutation: Sequence[int] | None = None,
) -> FixedShapeTensorType[_ValueT]: ...
def bool8() -> Bool8Type: ...
def opaque(storage_type: DataType, type_name: str, vendor_name: str) -> OpaqueType: ...
def type_for_alias(name: Any) -> DataType: ...
def schema(
    fields: (
        Iterable[Field[Any]]
        | Iterable[tuple[str, _FieldTypeInput]]
        | Mapping[Any, _FieldTypeInput]
    ),
    metadata: _SchemaMetadataInput | None = None,
) -> Schema: ...
def from_numpy_dtype(dtype: np.dtype[Any] | type | str) -> DataType: ...

__all__ = [
    "_Weakrefable",
    "_Metadata",
    "DataType",
    "_BasicDataType",
    "NullType",
    "BoolType",
    "UInt8Type",
    "Int8Type",
    "UInt16Type",
    "Int16Type",
    "UInt32Type",
    "Int32Type",
    "UInt64Type",
    "Int64Type",
    "Float16Type",
    "Float32Type",
    "Float64Type",
    "Date32Type",
    "Date64Type",
    "MonthDayNanoIntervalType",
    "StringType",
    "LargeStringType",
    "StringViewType",
    "BinaryType",
    "LargeBinaryType",
    "BinaryViewType",
    "TimestampType",
    "Time32Type",
    "Time64Type",
    "DurationType",
    "FixedSizeBinaryType",
    "Decimal32Type",
    "Decimal64Type",
    "Decimal128Type",
    "Decimal256Type",
    "ListType",
    "LargeListType",
    "ListViewType",
    "LargeListViewType",
    "FixedSizeListType",
    "DictionaryMemo",
    "DictionaryType",
    "MapType",
    "StructType",
    "UnionType",
    "SparseUnionType",
    "DenseUnionType",
    "RunEndEncodedType",
    "BaseExtensionType",
    "ExtensionType",
    "FixedShapeTensorType",
    "Bool8Type",
    "UuidType",
    "JsonType",
    "OpaqueType",
    "UnknownExtensionType",
    "register_extension_type",
    "unregister_extension_type",
    "KeyValueMetadata",
    "Field",
    "Schema",
    "unify_schemas",
    "field",
    "null",
    "bool_",
    "uint8",
    "int8",
    "uint16",
    "int16",
    "uint32",
    "int32",
    "int64",
    "uint64",
    "timestamp",
    "time32",
    "time64",
    "duration",
    "month_day_nano_interval",
    "date32",
    "date64",
    "float16",
    "float32",
    "float64",
    "decimal32",
    "decimal64",
    "decimal128",
    "decimal256",
    "string",
    "utf8",
    "binary",
    "large_binary",
    "large_string",
    "large_utf8",
    "binary_view",
    "string_view",
    "list_",
    "large_list",
    "list_view",
    "large_list_view",
    "map_",
    "dictionary",
    "struct",
    "sparse_union",
    "dense_union",
    "union",
    "run_end_encoded",
    "json_",
    "uuid",
    "fixed_shape_tensor",
    "bool8",
    "opaque",
    "type_for_alias",
    "schema",
    "from_numpy_dtype",
    "_Unit",
    "_Tz",
    "_Time32Unit",
    "_Time64Unit",
    "_DataTypeT",
]
