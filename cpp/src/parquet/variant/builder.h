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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/memory_pool.h"
#include "arrow/type_fwd.h"
#include "parquet/platform.h"

namespace parquet::variant {

using ::arrow::Array;
using ::arrow::BinaryViewArray;
using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::MemoryPool;
using ::arrow::StructArray;
using ::arrow::extension::VariantArray;

class VariantMetadataView;
class VariantObjectBuilder;
class VariantListBuilder;
class VariantValueArrayBuilder;

namespace internal {
class MetadataDictionary;
struct NestedVariantBuilderImpl;
}  // namespace internal

/// \brief Builder for encoded Parquet Variant metadata.
///
/// Field IDs are assigned in insertion order; retained fields keep their IDs.
class PARQUET_EXPORT MetadataBuilder {
 public:
  explicit MetadataBuilder(MemoryPool* pool = ::arrow::default_memory_pool());
  ~MetadataBuilder();

  void Reserve(int64_t capacity);
  /// Return an existing or newly assigned field ID, or throw if the field cannot
  /// be added.
  uint32_t TryUpsert(std::string_view name);
  /// Return the number of fields.
  size_t size() const;
  /// Encode the dictionary and return its buffer, then reset for reuse.
  /// Encoding failure leaves fields unchanged. Do not call Finish() while this
  /// metadata is borrowed by a value builder.
  std::shared_ptr<Buffer> Finish();

 private:
  friend class VariantBuilder;
  friend class VariantArrayBuilder;
  friend class internal::MetadataDictionary;

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Encoded metadata and value for one unshredded Variant value.
struct PARQUET_EXPORT EncodedVariantValue {
  std::shared_ptr<Buffer> metadata;
  std::shared_ptr<Buffer> value;
};

/// \brief Options for constructing a VariantBuilder.
struct PARQUET_EXPORT VariantBuilderOptions {
  /// Whether Finish() validates the completed metadata and value.
  bool validate_on_finish = true;

  /// Memory pool used for encoded metadata and value buffers.
  MemoryPool* pool = ::arrow::default_memory_pool();
};

/// \brief Builder for one encoded Parquet Variant value.
///
/// Build one root value per Finish()/Reset() cycle. A second root value or Finish()
/// on an empty builder or with an active container throws ParquetException.
///
/// \code{.cpp}
/// VariantBuilder builder;
/// auto object = builder.StartObject();
/// object.AddInt64("id", 42);
/// std::move(object).Finish();
/// EncodedVariantValue encoded = builder.Finish();
/// \endcode
class PARQUET_EXPORT VariantBuilder {
 public:
  explicit VariantBuilder(VariantBuilderOptions options = {});
  ~VariantBuilder();
  VariantBuilder(VariantBuilder&&) noexcept;
  VariantBuilder& operator=(VariantBuilder&&) noexcept;

  void ReserveFieldNames(int64_t capacity);
  uint32_t AddFieldName(std::string_view name);

  void SetVariantNull();
  void SetBoolean(bool value);
  void SetInt8(int8_t value);
  void SetInt16(int16_t value);
  void SetInt32(int32_t value);
  void SetInt64(int64_t value);
  void SetFloat(float value);
  void SetDouble(double value);
  void SetBinary(std::string_view value);
  void SetString(std::string_view value);
  void SetDate(int32_t days);
  void SetTimeNTZMicros(int64_t micros);
  void SetUuid(std::string_view big_endian_bytes);
  void SetDecimal4(const ::arrow::Decimal32& value, uint8_t scale);
  void SetDecimal8(const ::arrow::Decimal64& value, uint8_t scale);
  void SetDecimal16(const ::arrow::Decimal128& value, uint8_t scale);
  void SetShortString(std::string_view value);
  void SetTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void SetTimestampNanos(int64_t nanos, bool adjusted_to_utc);
  void SetEncodedValue(std::string_view value);

  // Both StartObject() and StartList() return a nested builder that borrows this
  // builder's state. This builder must outlive it. Finish or destroy the nested
  // builder before calling any other method on this builder, including Finish()
  // or Reset().
  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  /// Return the encoded metadata and value buffers, then reset the builder for reuse.
  EncodedVariantValue Finish();
  /// Discard the value and metadata.
  void Reset();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Builder for a nested Variant object.
///
/// Add fields by name; adding a duplicate field name throws ParquetException.
class PARQUET_EXPORT VariantObjectBuilder {
 public:
  ~VariantObjectBuilder();
  VariantObjectBuilder(VariantObjectBuilder&&) noexcept;
  VariantObjectBuilder& operator=(VariantObjectBuilder&&) noexcept;

  void AddVariantNull(std::string_view field_name);
  void AddBoolean(std::string_view field_name, bool value);
  void AddInt8(std::string_view field_name, int8_t value);
  void AddInt16(std::string_view field_name, int16_t value);
  void AddInt32(std::string_view field_name, int32_t value);
  void AddInt64(std::string_view field_name, int64_t value);
  void AddFloat(std::string_view field_name, float value);
  void AddDouble(std::string_view field_name, double value);
  void AddBinary(std::string_view field_name, std::string_view value);
  void AddString(std::string_view field_name, std::string_view value);
  void AddDate(std::string_view field_name, int32_t days);
  void AddTimeNTZMicros(std::string_view field_name, int64_t micros);
  void AddUuid(std::string_view field_name, std::string_view big_endian_bytes);
  void AddDecimal4(std::string_view field_name, const ::arrow::Decimal32& value,
                   uint8_t scale);
  void AddDecimal8(std::string_view field_name, const ::arrow::Decimal64& value,
                   uint8_t scale);
  void AddDecimal16(std::string_view field_name, const ::arrow::Decimal128& value,
                    uint8_t scale);
  void AddShortString(std::string_view field_name, std::string_view value);
  void AddTimestampMicros(std::string_view field_name, int64_t micros,
                          bool adjusted_to_utc);
  void AddTimestampNanos(std::string_view field_name, int64_t nanos,
                         bool adjusted_to_utc);
  void AddEncodedValue(std::string_view field_name, std::string_view value);

  // Both StartObject() and StartList() return a nested builder that borrows this
  // builder's state. This builder must outlive it. Finish or destroy the nested
  // builder before calling any other method on this builder, including Finish().
  VariantObjectBuilder StartObject(std::string_view field_name);
  VariantListBuilder StartList(std::string_view field_name);

  /// Commit the object to its parent. Destruction without Finish() rolls it back.
  void Finish() &&;

 private:
  friend class VariantBuilder;
  friend class VariantListBuilder;
  friend class VariantArrayBuilder;
  friend class VariantValueArrayBuilder;

  explicit VariantObjectBuilder(std::unique_ptr<internal::NestedVariantBuilderImpl> impl);

  std::unique_ptr<internal::NestedVariantBuilderImpl> impl_;
};

/// \brief Builder for a nested Variant list.
///
/// Append elements in order.
class PARQUET_EXPORT VariantListBuilder {
 public:
  ~VariantListBuilder();
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
  void AppendDecimal4(const ::arrow::Decimal32& value, uint8_t scale);
  void AppendDecimal8(const ::arrow::Decimal64& value, uint8_t scale);
  void AppendDecimal16(const ::arrow::Decimal128& value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);
  void AppendEncodedValue(std::string_view value);

  // Both StartObject() and StartList() return a nested builder that borrows this
  // builder's state. This builder must outlive it. Finish or destroy the nested
  // builder before calling any other method on this builder, including Finish().
  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  /// Commit the list to its parent. Destruction without Finish() rolls it back.
  void Finish() &&;

 private:
  friend class VariantBuilder;
  friend class VariantObjectBuilder;
  friend class VariantArrayBuilder;
  friend class VariantValueArrayBuilder;

  explicit VariantListBuilder(std::unique_ptr<internal::NestedVariantBuilderImpl> impl);

  std::unique_ptr<internal::NestedVariantBuilderImpl> impl_;
};

/// \brief Builder for an Arrow column of unshredded Variant values.
///
/// Encode primitive values directly into the column's value buffer, avoiding
/// intermediate per-row value buffers.
class PARQUET_EXPORT VariantArrayBuilder {
 public:
  explicit VariantArrayBuilder(MemoryPool* pool = ::arrow::default_memory_pool());
  ~VariantArrayBuilder();
  VariantArrayBuilder(VariantArrayBuilder&&) noexcept;
  VariantArrayBuilder& operator=(VariantArrayBuilder&&) noexcept;

  /// Reserve space for additional rows without changing the array length.
  /// Only view entries and validity bits are reserved, not encoded data bytes.
  /// additional_capacity must be non-negative.
  void Reserve(int64_t additional_capacity);

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
  void AppendDecimal4(const ::arrow::Decimal32& value, uint8_t scale);
  void AppendDecimal8(const ::arrow::Decimal64& value, uint8_t scale);
  void AppendDecimal16(const ::arrow::Decimal128& value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);
  void AppendEncoded(const EncodedVariantValue& value);

  // Both StartObject() and StartList() return a nested builder that borrows this
  // builder's internal buffers. This builder must outlive it. Finish or destroy the
  // nested builder before calling any other method on this builder, including
  // Finish() or Reset().
  VariantObjectBuilder StartObject();
  VariantListBuilder StartList();

  // Return a VariantArray for use in an arrow::RecordBatch or arrow::Table,
  // then reset the builder for reuse.
  std::shared_ptr<VariantArray> Finish();
  void Reset();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// \brief Builder for a Variant value array without a metadata column.
///
/// Append values directly, or build objects and lists using an external dictionary.
/// Finish() returns a BinaryViewArray and resets the builder. The caller pairs each
/// value with the metadata used to encode it.
class PARQUET_EXPORT VariantValueArrayBuilder {
 public:
  explicit VariantValueArrayBuilder(MemoryPool* pool = ::arrow::default_memory_pool());
  ~VariantValueArrayBuilder();
  VariantValueArrayBuilder(VariantValueArrayBuilder&&) noexcept;
  VariantValueArrayBuilder& operator=(VariantValueArrayBuilder&&) noexcept;

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
  void AppendDecimal4(const ::arrow::Decimal32& value, uint8_t scale);
  void AppendDecimal8(const ::arrow::Decimal64& value, uint8_t scale);
  void AppendDecimal16(const ::arrow::Decimal128& value, uint8_t scale);
  void AppendShortString(std::string_view value);
  void AppendTimestampMicros(int64_t micros, bool adjusted_to_utc);
  void AppendTimestampNanos(int64_t nanos, bool adjusted_to_utc);
  void AppendEncodedValue(std::string_view value);

  // The StartObject()/StartList() overloads below return a builder that borrows this
  // builder and metadata. Both must remain valid until it is finished or destroyed.
  // Until then, do not call other methods on this builder, including Finish() or
  // Reset(). Nested containers use the same dictionary.
  // Finishing the root appends one row; destroying it unfinished discards its value.

  // For metadata views, keep the underlying bytes alive and unchanged.
  VariantObjectBuilder StartObject(const VariantMetadataView& metadata);
  VariantObjectBuilder StartObject(const VariantMetadataView&& metadata) = delete;
  VariantListBuilder StartList(const VariantMetadataView& metadata);
  VariantListBuilder StartList(const VariantMetadataView&& metadata) = delete;

  // Writable metadata retains fields from successful roots and rolls back additions
  // from discarded roots. While borrowed, modify it only through the nested builder.
  // Call metadata.Finish() after building all rows that share the dictionary.
  VariantObjectBuilder StartObject(MetadataBuilder& metadata);
  VariantListBuilder StartList(MetadataBuilder& metadata);

  std::shared_ptr<BinaryViewArray> Finish();
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
