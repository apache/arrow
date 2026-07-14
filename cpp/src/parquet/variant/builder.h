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

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "parquet/platform.h"

namespace parquet::variant {

using ::arrow::Array;
using ::arrow::BinaryViewArray;
using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::MemoryPool;
using ::arrow::StructArray;
using ::arrow::extension::VariantArray;

struct PARQUET_EXPORT EncodedVariantValue {
  std::shared_ptr<Buffer> metadata;
  std::shared_ptr<Buffer> value;
};

class VariantObjectBuilder;
class VariantListBuilder;

class PARQUET_EXPORT VariantBuilder {
 public:
  explicit VariantBuilder(MemoryPool* pool = ::arrow::default_memory_pool());
  ~VariantBuilder();
  VariantBuilder(const VariantBuilder&) = delete;
  VariantBuilder& operator=(const VariantBuilder&) = delete;
  VariantBuilder(VariantBuilder&&) noexcept;
  VariantBuilder& operator=(VariantBuilder&&) noexcept;

  void ReserveFieldNames(int64_t capacity);
  uint32_t AddFieldName(std::string_view name);

  void AppendVariantNull();
  void AppendBoolean(bool value);
  void AppendInt8(int8_t value);
  void AppendInt16(int16_t value);
  void AppendInt32(int32_t value);
  void AppendInt64(int64_t value);
  void AppendFloat(float value);
  void AppendDouble(double value);
  void AppendBinary(std::string_view value);
  void AppendString(std::string_view value);
  void AppendDate(int32_t days);
  void AppendTimeNTZMicros(int64_t micros);
  void AppendUuid(std::string_view big_endian_bytes);
  void AppendDecimal4(int32_t unscaled_value, uint8_t scale);
  void AppendDecimal8(int64_t unscaled_value, uint8_t scale);
  void AppendDecimal16(std::string_view little_endian_unscaled_value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);

  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  EncodedVariantValue Finish();
  void Reset();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

namespace internal {
struct NestedVariantBuilderImpl;
}

class PARQUET_EXPORT VariantObjectBuilder {
 public:
  ~VariantObjectBuilder();
  VariantObjectBuilder(const VariantObjectBuilder&) = delete;
  VariantObjectBuilder& operator=(const VariantObjectBuilder&) = delete;
  VariantObjectBuilder(VariantObjectBuilder&&) noexcept;
  VariantObjectBuilder& operator=(VariantObjectBuilder&&) noexcept;

  void AppendVariantNull(std::string_view field_name);
  void AppendBoolean(std::string_view field_name, bool value);
  void AppendInt8(std::string_view field_name, int8_t value);
  void AppendInt16(std::string_view field_name, int16_t value);
  void AppendInt32(std::string_view field_name, int32_t value);
  void AppendInt64(std::string_view field_name, int64_t value);
  void AppendFloat(std::string_view field_name, float value);
  void AppendDouble(std::string_view field_name, double value);
  void AppendBinary(std::string_view field_name, std::string_view value);
  void AppendString(std::string_view field_name, std::string_view value);
  void AppendDate(std::string_view field_name, int32_t days);
  void AppendTimeNTZMicros(std::string_view field_name, int64_t micros);
  void AppendUuid(std::string_view field_name, std::string_view big_endian_bytes);
  void AppendDecimal4(std::string_view field_name, int32_t unscaled_value, uint8_t scale);
  void AppendDecimal8(std::string_view field_name, int64_t unscaled_value, uint8_t scale);
  void AppendDecimal16(std::string_view field_name,
                       std::string_view little_endian_unscaled_value, uint8_t scale);
  void AppendShortString(std::string_view field_name, std::string_view value);
  void AppendTimestampMicros(std::string_view field_name, int64_t micros,
                             bool adjusted_to_utc);
  void AppendTimestampNanos(std::string_view field_name, int64_t nanos,
                            bool adjusted_to_utc);

  VariantObjectBuilder StartObject(std::string_view field_name);
  VariantListBuilder StartList(std::string_view field_name);

  /// Commit this nested object into its parent builder. Destroying an unfinished
  /// nested builder rolls back the object contents written through this builder.
  void Finish();

 private:
  friend class VariantBuilder;
  friend class VariantListBuilder;
  friend class VariantArrayBuilder;

  explicit VariantObjectBuilder(std::unique_ptr<internal::NestedVariantBuilderImpl> impl);

  std::unique_ptr<internal::NestedVariantBuilderImpl> impl_;
};

class PARQUET_EXPORT VariantListBuilder {
 public:
  ~VariantListBuilder();
  VariantListBuilder(const VariantListBuilder&) = delete;
  VariantListBuilder& operator=(const VariantListBuilder&) = delete;
  VariantListBuilder(VariantListBuilder&&) noexcept;
  VariantListBuilder& operator=(VariantListBuilder&&) noexcept;

  void AppendVariantNull();
  void AppendBoolean(bool value);
  void AppendInt8(int8_t value);
  void AppendInt16(int16_t value);
  void AppendInt32(int32_t value);
  void AppendInt64(int64_t value);
  void AppendFloat(float value);
  void AppendDouble(double value);
  void AppendBinary(std::string_view value);
  void AppendString(std::string_view value);
  void AppendDate(int32_t days);
  void AppendTimeNTZMicros(int64_t micros);
  void AppendUuid(std::string_view big_endian_bytes);
  void AppendDecimal4(int32_t unscaled_value, uint8_t scale);
  void AppendDecimal8(int64_t unscaled_value, uint8_t scale);
  void AppendDecimal16(std::string_view little_endian_unscaled_value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);

  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  /// Commit this nested list into its parent builder. Destroying an unfinished nested
  /// builder rolls back the list contents written through this builder.
  void Finish();

 private:
  friend class VariantBuilder;
  friend class VariantObjectBuilder;
  friend class VariantArrayBuilder;

  explicit VariantListBuilder(std::unique_ptr<internal::NestedVariantBuilderImpl> impl);

  std::unique_ptr<internal::NestedVariantBuilderImpl> impl_;
};

class PARQUET_EXPORT VariantArrayBuilder {
 public:
  explicit VariantArrayBuilder(MemoryPool* pool = ::arrow::default_memory_pool());
  ~VariantArrayBuilder();
  VariantArrayBuilder(const VariantArrayBuilder&) = delete;
  VariantArrayBuilder& operator=(const VariantArrayBuilder&) = delete;
  VariantArrayBuilder(VariantArrayBuilder&&) noexcept;
  VariantArrayBuilder& operator=(VariantArrayBuilder&&) noexcept;

  void AppendNull();
  void AppendVariantNull();
  void AppendBoolean(bool value);
  void AppendInt8(int8_t value);
  void AppendInt16(int16_t value);
  void AppendInt32(int32_t value);
  void AppendInt64(int64_t value);
  void AppendFloat(float value);
  void AppendDouble(double value);
  void AppendBinary(std::string_view value);
  void AppendString(std::string_view value);
  void AppendDate(int32_t days);
  void AppendTimeNTZMicros(int64_t micros);
  void AppendUuid(std::string_view big_endian_bytes);
  void AppendDecimal4(int32_t unscaled_value, uint8_t scale);
  void AppendDecimal8(int64_t unscaled_value, uint8_t scale);
  void AppendDecimal16(std::string_view little_endian_unscaled_value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);
  void AppendEncoded(const EncodedVariantValue& value);

  /// The returned nested builder borrows this builder's internal buffers. Finish or
  /// destroy it before calling any other VariantArrayBuilder method, including Finish()
  /// or Reset().
  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  std::shared_ptr<VariantArray> Finish();
  void Reset();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

PARQUET_EXPORT
std::shared_ptr<VariantArray> MakeVariantArrayFromStorage(
    std::shared_ptr<StructArray> storage);

PARQUET_EXPORT
std::shared_ptr<VariantArray> MakeVariantArrayFromChildren(
    std::shared_ptr<DataType> storage_type, std::vector<std::shared_ptr<Array>> children,
    std::shared_ptr<Buffer> null_bitmap = nullptr);

}  // namespace parquet::variant
