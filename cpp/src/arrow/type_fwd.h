// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

template <typename T>
class Iterator;

template <typename T>
class Result;

class Status;

class Buffer;
class Device;
class MemoryManager;
class MemoryPool;
class MutableBuffer;
class ResizableBuffer;

using BufferVector = std::vector<std::shared_ptr<Buffer>>;

class DataType;
class Field;
class FieldRef;
class KeyValueMetadata;
class Schema;

using DataTypeVector = std::vector<std::shared_ptr<DataType>>;
using FieldVector = std::vector<std::shared_ptr<Field>>;

class Array;
struct ArrayData;
class ArrayBuilder;
class Tensor;
struct Scalar;

using ArrayDataVector = std::vector<std::shared_ptr<ArrayData>>;
using ArrayVector = std::vector<std::shared_ptr<Array>>;
using ScalarVector = std::vector<std::shared_ptr<Scalar>>;

class ChunkedArray;
class RecordBatch;
class Table;

using ChunkedArrayVector = std::vector<std::shared_ptr<ChunkedArray>>;
using RecordBatchVector = std::vector<std::shared_ptr<RecordBatch>>;
using RecordBatchIterator = Iterator<std::shared_ptr<RecordBatch>>;

class DictionaryType;
class DictionaryArray;
struct DictionaryScalar;

class NullType;
class NullArray;
class NullBuilder;
struct NullScalar;

class BooleanType;
class BooleanArray;
class BooleanBuilder;
struct BooleanScalar;

class BinaryType;
class BinaryArray;
class BinaryBuilder;
struct BinaryScalar;

class LargeBinaryType;
class LargeBinaryArray;
class LargeBinaryBuilder;
struct LargeBinaryScalar;

class FixedSizeBinaryType;
class FixedSizeBinaryArray;
class FixedSizeBinaryBuilder;
struct FixedSizeBinaryScalar;

class StringType;
class StringArray;
class StringBuilder;
struct StringScalar;

class LargeStringType;
class LargeStringArray;
class LargeStringBuilder;
struct LargeStringScalar;

class ListType;
class ListArray;
class ListBuilder;
struct ListScalar;

class LargeListType;
class LargeListArray;
class LargeListBuilder;
struct LargeListScalar;

class MapType;
class MapArray;
class MapBuilder;
struct MapScalar;

class FixedSizeListType;
class FixedSizeListArray;
class FixedSizeListBuilder;
struct FixedSizeListScalar;

class StructType;
class StructArray;
class StructBuilder;
struct StructScalar;

class Decimal128;
class Decimal128Type;
class Decimal128Array;
class Decimal128Builder;
struct Decimal128Scalar;

class UnionType;
class UnionArray;
struct UnionScalar;
struct UnionMode {
  enum type { SPARSE, DENSE };
};

template <typename TypeClass>
class NumericArray;

template <typename TypeClass>
class NumericBuilder;

template <typename TypeClass>
class NumericTensor;

#define _NUMERIC_TYPE_DECL(KLASS)                     \
  class KLASS##Type;                                  \
  using KLASS##Array = NumericArray<KLASS##Type>;     \
  using KLASS##Builder = NumericBuilder<KLASS##Type>; \
  struct KLASS##Scalar;                               \
  using KLASS##Tensor = NumericTensor<KLASS##Type>;

_NUMERIC_TYPE_DECL(Int8)
_NUMERIC_TYPE_DECL(Int16)
_NUMERIC_TYPE_DECL(Int32)
_NUMERIC_TYPE_DECL(Int64)
_NUMERIC_TYPE_DECL(UInt8)
_NUMERIC_TYPE_DECL(UInt16)
_NUMERIC_TYPE_DECL(UInt32)
_NUMERIC_TYPE_DECL(UInt64)
_NUMERIC_TYPE_DECL(HalfFloat)
_NUMERIC_TYPE_DECL(Float)
_NUMERIC_TYPE_DECL(Double)

#undef _NUMERIC_TYPE_DECL

class Date32Type;
using Date32Array = NumericArray<Date32Type>;
using Date32Builder = NumericBuilder<Date32Type>;
struct Date32Scalar;

class Date64Type;
using Date64Array = NumericArray<Date64Type>;
using Date64Builder = NumericBuilder<Date64Type>;
struct Date64Scalar;

struct TimeUnit {
  /// The unit for a time or timestamp DataType
  enum type { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };
};

class Time32Type;
using Time32Array = NumericArray<Time32Type>;
using Time32Builder = NumericBuilder<Time32Type>;
struct Time32Scalar;

class Time64Type;
using Time64Array = NumericArray<Time64Type>;
using Time64Builder = NumericBuilder<Time64Type>;
struct Time64Scalar;

class TimestampType;
using TimestampArray = NumericArray<TimestampType>;
using TimestampBuilder = NumericBuilder<TimestampType>;
struct TimestampScalar;

class MonthIntervalType;
using MonthIntervalArray = NumericArray<MonthIntervalType>;
using MonthIntervalBuilder = NumericBuilder<MonthIntervalType>;
struct MonthIntervalScalar;

class DayTimeIntervalType;
class DayTimeIntervalArray;
class DayTimeIntervalBuilder;
struct DayTimeIntervalScalar;

class DurationType;
using DurationArray = NumericArray<DurationType>;
using DurationBuilder = NumericBuilder<DurationType>;
struct DurationScalar;

class ExtensionType;
class ExtensionArray;
struct ExtensionScalar;

// ----------------------------------------------------------------------

/// \defgroup type-factories Factory functions for creating data types
///
/// Factory functions for creating data types
/// @{

/// \brief Return a NullType instance
std::shared_ptr<DataType> ARROW_EXPORT null();
/// \brief Return a BooleanType instance
std::shared_ptr<DataType> ARROW_EXPORT boolean();
/// \brief Return a Int8Type instance
std::shared_ptr<DataType> ARROW_EXPORT int8();
/// \brief Return a Int16Type instance
std::shared_ptr<DataType> ARROW_EXPORT int16();
/// \brief Return a Int32Type instance
std::shared_ptr<DataType> ARROW_EXPORT int32();
/// \brief Return a Int64Type instance
std::shared_ptr<DataType> ARROW_EXPORT int64();
/// \brief Return a UInt8Type instance
std::shared_ptr<DataType> ARROW_EXPORT uint8();
/// \brief Return a UInt16Type instance
std::shared_ptr<DataType> ARROW_EXPORT uint16();
/// \brief Return a UInt32Type instance
std::shared_ptr<DataType> ARROW_EXPORT uint32();
/// \brief Return a UInt64Type instance
std::shared_ptr<DataType> ARROW_EXPORT uint64();
/// \brief Return a HalfFloatType instance
std::shared_ptr<DataType> ARROW_EXPORT float16();
/// \brief Return a FloatType instance
std::shared_ptr<DataType> ARROW_EXPORT float32();
/// \brief Return a DoubleType instance
std::shared_ptr<DataType> ARROW_EXPORT float64();
/// \brief Return a StringType instance
std::shared_ptr<DataType> ARROW_EXPORT utf8();
/// \brief Return a LargeStringType instance
std::shared_ptr<DataType> ARROW_EXPORT large_utf8();
/// \brief Return a BinaryType instance
std::shared_ptr<DataType> ARROW_EXPORT binary();
/// \brief Return a LargeBinaryType instance
std::shared_ptr<DataType> ARROW_EXPORT large_binary();
/// \brief Return a Date32Type instance
std::shared_ptr<DataType> ARROW_EXPORT date32();
/// \brief Return a Date64Type instance
std::shared_ptr<DataType> ARROW_EXPORT date64();

/// \brief Create a FixedSizeBinaryType instance.
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_binary(int32_t byte_width);

/// \brief Create a Decimal128Type instance
ARROW_EXPORT
std::shared_ptr<DataType> decimal(int32_t precision, int32_t scale);

/// \brief Create a ListType instance from its child Field type
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_type);

/// \brief Create a ListType instance from its child DataType
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type);

/// \brief Create a LargeListType instance from its child Field type
ARROW_EXPORT
std::shared_ptr<DataType> large_list(const std::shared_ptr<Field>& value_type);

/// \brief Create a LargeListType instance from its child DataType
ARROW_EXPORT
std::shared_ptr<DataType> large_list(const std::shared_ptr<DataType>& value_type);

/// \brief Create a MapType instance from its key and value DataTypes
ARROW_EXPORT
std::shared_ptr<DataType> map(const std::shared_ptr<DataType>& key_type,
                              const std::shared_ptr<DataType>& item_type,
                              bool keys_sorted = false);

/// \brief Create a MapType instance from its key DataType and value field.
///
/// The field override is provided to communicate nullability of the value.
ARROW_EXPORT
std::shared_ptr<DataType> map(const std::shared_ptr<DataType>& key_type,
                              const std::shared_ptr<Field>& item_field,
                              bool keys_sorted = false);

/// \brief Create a FixedSizeListType instance from its child Field type
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<Field>& value_type,
                                          int32_t list_size);

/// \brief Create a FixedSizeListType instance from its child DataType
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_list(const std::shared_ptr<DataType>& value_type,
                                          int32_t list_size);
/// \brief Return a Duration instance (naming use _type to avoid namespace conflict with
/// built in time clases).
std::shared_ptr<DataType> ARROW_EXPORT duration(TimeUnit::type unit);

/// \brief Return a DayTimeIntervalType instance
std::shared_ptr<DataType> ARROW_EXPORT day_time_interval();

/// \brief Return a MonthIntervalType instance
std::shared_ptr<DataType> ARROW_EXPORT month_interval();

/// \brief Create a TimestampType instance from its unit
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit);

/// \brief Create a TimestampType instance from its unit and timezone
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit, const std::string& timezone);

/// \brief Create a 32-bit time type instance
///
/// Unit can be either SECOND or MILLI
std::shared_ptr<DataType> ARROW_EXPORT time32(TimeUnit::type unit);

/// \brief Create a 64-bit time type instance
///
/// Unit can be either MICRO or NANO
std::shared_ptr<DataType> ARROW_EXPORT time64(TimeUnit::type unit);

/// \brief Create a StructType instance
std::shared_ptr<DataType> ARROW_EXPORT
struct_(const std::vector<std::shared_ptr<Field>>& fields);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Field>>& child_fields,
       const std::vector<int8_t>& type_codes, UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Field>>& child_fields,
       UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT union_(UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       const std::vector<std::string>& field_names, const std::vector<int8_t>& type_codes,
       UnionMode::type mode = UnionMode::SPARSE);

/// \brief Create a UnionType instance
inline std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       const std::vector<std::string>& field_names,
       UnionMode::type mode = UnionMode::SPARSE) {
  return union_(children, field_names, {}, mode);
}

/// \brief Create a UnionType instance
inline std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Array>>& children,
       UnionMode::type mode = UnionMode::SPARSE) {
  return union_(children, {}, {}, mode);
}

/// \brief Create a DictionaryType instance
/// \param[in] index_type the type of the dictionary indices (must be
/// a signed integer)
/// \param[in] dict_type the type of the values in the variable dictionary
/// \param[in] ordered true if the order of the dictionary values has
/// semantic meaning and should be preserved where possible
ARROW_EXPORT
std::shared_ptr<DataType> dictionary(const std::shared_ptr<DataType>& index_type,
                                     const std::shared_ptr<DataType>& dict_type,
                                     bool ordered = false);

/// @}

/// \defgroup schema-factories Factory functions for fields and schemas
///
/// Factory functions for fields and schemas
/// @{

/// \brief Create a Field instance
///
/// \param name the field name
/// \param type the field value type
/// \param nullable whether the values are nullable, default true
/// \param metadata any custom key-value metadata, default null
std::shared_ptr<Field> ARROW_EXPORT
field(std::string name, std::shared_ptr<DataType> type, bool nullable = true,
      std::shared_ptr<const KeyValueMetadata> metadata = NULLPTR);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields
/// \param metadata any custom key-value metadata, default null
/// \return schema shared_ptr to Schema
ARROW_EXPORT
std::shared_ptr<Schema> schema(
    std::vector<std::shared_ptr<Field>> fields,
    std::shared_ptr<const KeyValueMetadata> metadata = NULLPTR);

/// @}

/// Return the process-wide default memory pool.
ARROW_EXPORT MemoryPool* default_memory_pool();

}  // namespace arrow
