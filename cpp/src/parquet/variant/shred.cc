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

#include "parquet/variant/shred.h"

#include <concepts>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension/uuid.h"
#include "arrow/extension_type.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/unreachable.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/decoding.h"

namespace parquet::variant {

using ::arrow::Array;
using ::arrow::ArrayBuilder;
using ::arrow::DataType;
using ::arrow::Field;
using ::arrow::MemoryPool;
using ::arrow::Scalar;
using ::arrow::StructArray;
using ::arrow::TimeUnit;
using ::arrow::extension::VariantArray;
using ::arrow::internal::checked_cast;

namespace {

struct ShreddedArrayParts {
  std::shared_ptr<Array> value;
  std::shared_ptr<Array> typed_value;
  std::shared_ptr<::arrow::Buffer> null_bitmap;
  int64_t null_count = 0;
};

std::shared_ptr<DataType> FieldGroupType(const std::shared_ptr<DataType>& typed_type) {
  return ::arrow::struct_({::arrow::field("value", ::arrow::binary_view()),
                           ::arrow::field("typed_value", typed_type)});
}

std::shared_ptr<StructArray> MakeFieldGroup(ShreddedArrayParts parts) {
  auto type = FieldGroupType(parts.typed_value->type());
  PARQUET_ASSIGN_OR_THROW(
      auto out,
      StructArray::Make({std::move(parts.value), std::move(parts.typed_value)},
                        type->fields(), std::move(parts.null_bitmap), parts.null_count));
  return out;
}

class ShredNode {
 public:
  ShredNode(std::shared_ptr<DataType> typed_type, MemoryPool* pool)
      : typed_type_(std::move(typed_type)), residual_(pool), group_validity_(pool) {}
  virtual ~ShredNode() = default;

  const std::shared_ptr<DataType>& typed_type() const { return typed_type_; }
  std::shared_ptr<DataType> field_group_type() const {
    return FieldGroupType(typed_type_);
  }

  virtual void AppendParentNull() = 0;
  virtual void AppendMissing(const VariantMetadataView& metadata) = 0;
  virtual void AppendValue(const VariantMetadataView& metadata,
                           const VariantValueView& value) = 0;
  virtual ShreddedArrayParts Finish() = 0;

 protected:
  void AppendGroupValidity(bool valid) {
    PARQUET_THROW_NOT_OK(group_validity_.Append(valid));
  }
  void AppendResidualNull() { residual_.AppendNull(); }
  void AppendResidual(const VariantValueView& value) {
    residual_.AppendEncodedValue(value.value());
  }

  ShreddedArrayParts FinishParts(std::shared_ptr<Array> typed_value) {
    const auto null_count = group_validity_.false_count();
    return ShreddedArrayParts{.value = residual_.Finish(),
                              .typed_value = std::move(typed_value),
                              .null_bitmap = internal::FinishNullBitmap(group_validity_),
                              .null_count = null_count};
  }

  std::shared_ptr<DataType> typed_type_;
  VariantValueArrayBuilder residual_;
  ::arrow::TypedBufferBuilder<bool> group_validity_;
};

template <typename T>
T LoadLittleEndian(std::string_view bytes) {
  return ::arrow::bit_util::FromLittleEndian(
      ::arrow::util::SafeLoadAs<T>(reinterpret_cast<const uint8_t*>(bytes.data())));
}

bool TryAppendString(const DataType& storage_type, ArrayBuilder& builder,
                     std::string_view value) {
  switch (storage_type.id()) {
    case ::arrow::Type::STRING:
      PARQUET_THROW_NOT_OK(checked_cast<::arrow::StringBuilder&>(builder).Append(value));
      return true;
    case ::arrow::Type::LARGE_STRING:
      PARQUET_THROW_NOT_OK(
          checked_cast<::arrow::LargeStringBuilder&>(builder).Append(value));
      return true;
    case ::arrow::Type::STRING_VIEW:
      PARQUET_THROW_NOT_OK(
          checked_cast<::arrow::StringViewBuilder&>(builder).Append(value));
      return true;
    default:
      return false;
  }
}

bool TryAppendBinary(const DataType& storage_type, ArrayBuilder& builder,
                     std::string_view value) {
  switch (storage_type.id()) {
    case ::arrow::Type::BINARY:
      PARQUET_THROW_NOT_OK(checked_cast<::arrow::BinaryBuilder&>(builder).Append(value));
      return true;
    case ::arrow::Type::LARGE_BINARY:
      PARQUET_THROW_NOT_OK(
          checked_cast<::arrow::LargeBinaryBuilder&>(builder).Append(value));
      return true;
    case ::arrow::Type::BINARY_VIEW:
      PARQUET_THROW_NOT_OK(
          checked_cast<::arrow::BinaryViewBuilder&>(builder).Append(value));
      return true;
    default:
      return false;
  }
}

bool TryAppendPrimitiveDirect(const VariantValueView& value, const DataType& storage_type,
                              ArrayBuilder& builder) {
  if (const auto* short_string = std::get_if<VariantShortStringView>(&value.data())) {
    return TryAppendString(storage_type, builder, short_string->string());
  }
  const auto* primitive = std::get_if<VariantPrimitiveView>(&value.data());
  if (primitive == nullptr) {
    return false;
  }

  const auto payload = primitive->payload();
  switch (primitive->type()) {
    case VariantPrimitiveType::kBinary:
      return TryAppendBinary(storage_type, builder, payload.substr(4));
    case VariantPrimitiveType::kString:
      return TryAppendString(storage_type, builder, payload.substr(4));
    default:
      return false;
  }
}

std::shared_ptr<Scalar> DecodeSourceScalar(const VariantValueView& value) {
  if (const auto* short_string = std::get_if<VariantShortStringView>(&value.data())) {
    return std::make_shared<::arrow::StringScalar>(std::string(short_string->string()));
  }
  const auto* primitive = std::get_if<VariantPrimitiveView>(&value.data());
  if (primitive == nullptr) {
    return nullptr;
  }

  const auto payload = primitive->payload();
  switch (primitive->type()) {
    case VariantPrimitiveType::kNull:
      return nullptr;
    case VariantPrimitiveType::kBooleanTrue:
      return std::make_shared<::arrow::BooleanScalar>(true);
    case VariantPrimitiveType::kBooleanFalse:
      return std::make_shared<::arrow::BooleanScalar>(false);
    case VariantPrimitiveType::kInt8:
      return std::make_shared<::arrow::Int8Scalar>(LoadLittleEndian<int8_t>(payload));
    case VariantPrimitiveType::kInt16:
      return std::make_shared<::arrow::Int16Scalar>(LoadLittleEndian<int16_t>(payload));
    case VariantPrimitiveType::kInt32:
      return std::make_shared<::arrow::Int32Scalar>(LoadLittleEndian<int32_t>(payload));
    case VariantPrimitiveType::kInt64:
      return std::make_shared<::arrow::Int64Scalar>(LoadLittleEndian<int64_t>(payload));
    case VariantPrimitiveType::kFloat:
      return std::make_shared<::arrow::FloatScalar>(LoadLittleEndian<float>(payload));
    case VariantPrimitiveType::kDouble:
      return std::make_shared<::arrow::DoubleScalar>(LoadLittleEndian<double>(payload));
    case VariantPrimitiveType::kDecimal4: {
      const auto scale = static_cast<uint8_t>(payload[0]);
      auto type = ::arrow::decimal32(/*precision=*/9, scale);
      return std::make_shared<::arrow::Decimal32Scalar>(
          ::arrow::Decimal32(LoadLittleEndian<int32_t>(payload.substr(1))), type);
    }
    case VariantPrimitiveType::kDecimal8: {
      const auto scale = static_cast<uint8_t>(payload[0]);
      auto type = ::arrow::decimal64(/*precision=*/18, scale);
      return std::make_shared<::arrow::Decimal64Scalar>(
          ::arrow::Decimal64(LoadLittleEndian<int64_t>(payload.substr(1))), type);
    }
    case VariantPrimitiveType::kDecimal16: {
      const auto scale = static_cast<uint8_t>(payload[0]);
      const auto low = LoadLittleEndian<uint64_t>(payload.substr(1));
      const auto high = LoadLittleEndian<int64_t>(payload.substr(9));
      auto type = ::arrow::decimal128(/*precision=*/38, scale);
      return std::make_shared<::arrow::Decimal128Scalar>(::arrow::Decimal128(high, low),
                                                         type);
    }
    case VariantPrimitiveType::kDate:
      return std::make_shared<::arrow::Date32Scalar>(LoadLittleEndian<int32_t>(payload));
    case VariantPrimitiveType::kTimeNTZMicros:
      return std::make_shared<::arrow::Time64Scalar>(LoadLittleEndian<int64_t>(payload),
                                                     TimeUnit::MICRO);
    case VariantPrimitiveType::kTimestampMicros:
    case VariantPrimitiveType::kTimestampNTZMicros:
    case VariantPrimitiveType::kTimestampNanos:
    case VariantPrimitiveType::kTimestampNTZNanos: {
      const bool nanos = primitive->type() == VariantPrimitiveType::kTimestampNanos ||
                         primitive->type() == VariantPrimitiveType::kTimestampNTZNanos;
      const bool adjusted = primitive->type() == VariantPrimitiveType::kTimestampMicros ||
                            primitive->type() == VariantPrimitiveType::kTimestampNanos;
      const auto unit = nanos ? TimeUnit::NANO : TimeUnit::MICRO;
      auto type = adjusted ? ::arrow::timestamp(unit, "UTC") : ::arrow::timestamp(unit);
      return std::make_shared<::arrow::TimestampScalar>(
          LoadLittleEndian<int64_t>(payload), type);
    }
    case VariantPrimitiveType::kBinary:
      return std::make_shared<::arrow::BinaryScalar>(std::string(payload.substr(4)));
    case VariantPrimitiveType::kString:
      return std::make_shared<::arrow::StringScalar>(std::string(payload.substr(4)));
    case VariantPrimitiveType::kUuid:
      return std::make_shared<::arrow::FixedSizeBinaryScalar>(
          ::arrow::Buffer::FromString(std::string(payload)),
          ::arrow::fixed_size_binary(16));
  }
  ::arrow::Unreachable("Unexpected Variant primitive type");
}

bool CanAttemptCast(const Scalar& source, const DataType& target) {
  const auto source_id = source.type->id();
  switch (target.id()) {
    case ::arrow::Type::BOOL:
      return source_id == ::arrow::Type::BOOL || ::arrow::is_signed_integer(source_id) ||
             ::arrow::is_physical_floating(source_id) ||
             source_id == ::arrow::Type::STRING;
    case ::arrow::Type::INT8:
    case ::arrow::Type::INT16:
    case ::arrow::Type::INT32:
    case ::arrow::Type::INT64:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DOUBLE:
      return source_id == ::arrow::Type::BOOL || ::arrow::is_signed_integer(source_id) ||
             ::arrow::is_physical_floating(source_id) || ::arrow::is_decimal(source_id);
    case ::arrow::Type::DECIMAL32:
    case ::arrow::Type::DECIMAL64:
    case ::arrow::Type::DECIMAL128:
      return ::arrow::is_signed_integer(source_id) ||
             ::arrow::is_physical_floating(source_id) || ::arrow::is_decimal(source_id) ||
             source_id == ::arrow::Type::STRING;
    case ::arrow::Type::STRING:
    case ::arrow::Type::LARGE_STRING:
    case ::arrow::Type::STRING_VIEW:
      return source_id == ::arrow::Type::STRING;
    case ::arrow::Type::BINARY:
    case ::arrow::Type::LARGE_BINARY:
    case ::arrow::Type::BINARY_VIEW:
      return source_id == ::arrow::Type::BINARY;
    case ::arrow::Type::DATE32:
      return source_id == ::arrow::Type::DATE32;
    case ::arrow::Type::TIME64:
      return source.type->Equals(target);
    case ::arrow::Type::FIXED_SIZE_BINARY:
      return source.type->Equals(target);
    case ::arrow::Type::TIMESTAMP: {
      if (source_id != ::arrow::Type::TIMESTAMP) {
        return false;
      }
      const auto& source_type = checked_cast<const ::arrow::TimestampType&>(*source.type);
      const auto& target_type = checked_cast<const ::arrow::TimestampType&>(target);
      return source_type.timezone().empty() == target_type.timezone().empty() &&
             (source_type.unit() == target_type.unit() ||
              (source_type.unit() == TimeUnit::MICRO &&
               target_type.unit() == TimeUnit::NANO));
    }
    default:
      return false;
  }
}

class PrimitiveShredNode : public ShredNode {
 public:
  PrimitiveShredNode(std::shared_ptr<DataType> target, ::arrow::compute::ExecContext* ctx,
                     MemoryPool* pool)
      : ShredNode(std::move(target), pool), ctx_(ctx) {
    if (typed_type_->id() == ::arrow::Type::EXTENSION) {
      storage_type_ =
          checked_cast<const ::arrow::ExtensionType&>(*typed_type_).storage_type();
    } else {
      storage_type_ = typed_type_;
    }
    PARQUET_ASSIGN_OR_THROW(builder_, ::arrow::MakeBuilder(storage_type_, pool));
  }

  void AppendParentNull() override {
    AppendGroupValidity(false);
    AppendResidualNull();
    PARQUET_THROW_NOT_OK(builder_->AppendNull());
  }

  void AppendMissing(const VariantMetadataView&) override {
    AppendGroupValidity(true);
    AppendResidualNull();
    PARQUET_THROW_NOT_OK(builder_->AppendNull());
  }

  void AppendValue(const VariantMetadataView&, const VariantValueView& value) override {
    AppendGroupValidity(true);
    if (TryAppendPrimitiveDirect(value, *storage_type_, *builder_)) {
      AppendResidualNull();
      return;
    }
    const auto source = DecodeSourceScalar(value);
    if (source != nullptr && CanAttemptCast(*source, *storage_type_)) {
      auto casted = ::arrow::compute::Cast(::arrow::Datum(source), storage_type_,
                                           ::arrow::compute::CastOptions::Safe(), ctx_);
      if (casted.ok()) {
        AppendResidualNull();
        PARQUET_THROW_NOT_OK(builder_->AppendScalar(*casted->scalar()));
        return;
      }
      if (!casted.status().IsInvalid()) {
        PARQUET_THROW_NOT_OK(casted.status());
      }
    }
    AppendResidual(value);
    PARQUET_THROW_NOT_OK(builder_->AppendNull());
  }

  ShreddedArrayParts Finish() override {
    std::shared_ptr<Array> typed;
    PARQUET_THROW_NOT_OK(builder_->Finish(&typed));
    if (typed_type_->id() == ::arrow::Type::EXTENSION) {
      typed = ::arrow::ExtensionType::WrapArray(typed_type_, std::move(typed));
    }
    return FinishParts(std::move(typed));
  }

 private:
  ::arrow::compute::ExecContext* ctx_;
  std::shared_ptr<DataType> storage_type_;
  std::unique_ptr<ArrayBuilder> builder_;
};

std::unique_ptr<ShredNode> CompileShredNode(const std::shared_ptr<DataType>& target,
                                            ::arrow::compute::ExecContext* ctx,
                                            MemoryPool* pool);

class ObjectShredNode : public ShredNode {
 public:
  ObjectShredNode(const ::arrow::StructType& type, ::arrow::compute::ExecContext* ctx,
                  MemoryPool* pool)
      : ShredNode(nullptr, pool), typed_validity_(pool) {
    if (type.num_fields() == 0) {
      throw ParquetException("Variant shredding object target must not be empty");
    }

    std::vector<std::shared_ptr<Field>> fields;
    fields.reserve(type.num_fields());
    children_.reserve(type.num_fields());
    requested_field_names_.reserve(type.num_fields());
    for (const auto& field : type.fields()) {
      auto child = CompileShredNode(field->type(), ctx, pool);
      auto compiled_field = ::arrow::field(field->name(), child->field_group_type(),
                                           /*nullable=*/false);
      if (!requested_field_names_.insert(compiled_field->name()).second) {
        throw ParquetException("Duplicate Variant shredding field: ", field->name());
      }
      fields.push_back(std::move(compiled_field));
      children_.push_back(std::move(child));
    }
    typed_type_ = ::arrow::struct_(fields);
  }

  void AppendParentNull() override {
    AppendGroupValidity(false);
    AppendResidualNull();
    AppendTypedNull();
  }

  void AppendMissing(const VariantMetadataView& metadata) override {
    ValidateMetadata(metadata);
    AppendGroupValidity(true);
    AppendResidualNull();
    AppendTypedNull();
  }

  void AppendValue(const VariantMetadataView& metadata,
                   const VariantValueView& value) override {
    ValidateMetadata(metadata);
    AppendGroupValidity(true);
    const auto* object = std::get_if<VariantObjectView>(&value.data());
    if (object == nullptr) {
      AppendResidual(value);
      AppendTypedNull();
      return;
    }

    const auto& object_fields = object->fields();
    const auto first_residual = std::ranges::find_if(
        object_fields,
        [&](const auto& field) { return !requested_field_names_.contains(field.name); });

    if (first_residual == object_fields.end()) {
      AppendResidualNull();
    } else {
      auto row = residual_.BindMetadata(metadata);
      auto residual_object = row.StartObject();
      for (auto it = first_residual; it != object_fields.end(); ++it) {
        if (!requested_field_names_.contains(it->name)) {
          residual_object.AppendEncodedValue(it->name, it->value);
        }
      }
      residual_object.Finish();
      row.Finish();
    }

    PARQUET_THROW_NOT_OK(typed_validity_.Append(true));
    const auto& fields = checked_cast<const ::arrow::StructType&>(*typed_type_).fields();
    DCHECK_EQ(fields.size(), children_.size());
    for (size_t i = 0; i < children_.size(); ++i) {
      auto child_value = object->GetField(fields[i]->name());
      if (child_value.has_value()) {
        children_[i]->AppendValue(metadata, *child_value);
      } else {
        children_[i]->AppendMissing(metadata);
      }
    }
  }

  ShreddedArrayParts Finish() override {
    std::vector<std::shared_ptr<Array>> fields;
    fields.reserve(children_.size());
    for (auto& child : children_) {
      fields.push_back(MakeFieldGroup(child->Finish()));
    }
    const auto null_count = typed_validity_.false_count();
    PARQUET_ASSIGN_OR_THROW(
        auto typed,
        StructArray::Make(std::move(fields), typed_type_->fields(),
                          internal::FinishNullBitmap(typed_validity_), null_count));
    return FinishParts(std::move(typed));
  }

 private:
  void AppendTypedNull() {
    PARQUET_THROW_NOT_OK(typed_validity_.Append(false));
    for (auto& child : children_) {
      child->AppendParentNull();
    }
  }

  void ValidateMetadata(const VariantMetadataView& metadata) {
    if (last_validated_metadata_.has_value() &&
        *last_validated_metadata_ == metadata.metadata()) {
      return;
    }
    const auto& fields = checked_cast<const ::arrow::StructType&>(*typed_type_).fields();
    for (const auto& field : fields) {
      if (!metadata.FindString(field->name()).has_value()) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: shredded field '", field->name(),
            "' is not in metadata dictionary");
      }
    }
    last_validated_metadata_ = metadata.metadata();
  }

  std::unordered_set<std::string_view> requested_field_names_;
  std::vector<std::unique_ptr<ShredNode>> children_;
  ::arrow::TypedBufferBuilder<bool> typed_validity_;
  std::optional<std::string_view> last_validated_metadata_;
};

template <typename Offset>
void AppendIndex(::arrow::TypedBufferBuilder<Offset>* builder, int64_t value) {
  if (value > std::numeric_limits<Offset>::max()) {
    throw ParquetException("Variant array has too many elements");
  }
  PARQUET_THROW_NOT_OK(builder->Append(static_cast<Offset>(value)));
}

template <typename Offset>
std::shared_ptr<::arrow::Buffer> FinishBuffer(
    ::arrow::TypedBufferBuilder<Offset>* builder) {
  PARQUET_ASSIGN_OR_THROW(auto buffer, builder->Finish());
  return buffer;
}

template <std::derived_from<::arrow::BaseListType> TypeClass>
class ListLayout;

template <std::derived_from<::arrow::BaseListType> TypeClass>
  requires ::arrow::is_var_length_list_type<TypeClass>::value
class ListLayout<TypeClass> {
 public:
  using ArrayType = typename ::arrow::TypeTraits<TypeClass>::ArrayType;
  using Offset = typename TypeClass::offset_type;

  ListLayout(const TypeClass&, MemoryPool* pool) : offsets_(pool) {
    AppendIndex(&offsets_, 0);
  }

  std::shared_ptr<DataType> MakeType(std::shared_ptr<Field> element_field) const {
    return std::make_shared<TypeClass>(std::move(element_field));
  }

  void AppendPresent(int64_t, int64_t end) { AppendIndex(&offsets_, end); }
  void AppendNull(int64_t child_length) { AppendIndex(&offsets_, child_length); }

  std::shared_ptr<Array> FinishArray(const std::shared_ptr<DataType>& type,
                                     int64_t length, std::shared_ptr<Array> values,
                                     std::shared_ptr<::arrow::Buffer> null_bitmap,
                                     int64_t null_count) {
    return std::make_shared<ArrayType>(type, length, FinishBuffer(&offsets_),
                                       std::move(values), std::move(null_bitmap),
                                       null_count);
  }

 private:
  ::arrow::TypedBufferBuilder<Offset> offsets_;
};

template <std::derived_from<::arrow::BaseListType> TypeClass>
  requires ::arrow::is_list_view_type<TypeClass>::value
class ListLayout<TypeClass> {
 public:
  using ArrayType = typename ::arrow::TypeTraits<TypeClass>::ArrayType;
  using Offset = typename TypeClass::offset_type;

  ListLayout(const TypeClass&, MemoryPool* pool) : offsets_(pool), sizes_(pool) {}

  std::shared_ptr<DataType> MakeType(std::shared_ptr<Field> element_field) const {
    return std::make_shared<TypeClass>(std::move(element_field));
  }

  void AppendPresent(int64_t start, int64_t end) {
    AppendIndex(&offsets_, start);
    AppendIndex(&sizes_, end - start);
  }

  void AppendNull(int64_t child_length) {
    AppendIndex(&offsets_, child_length);
    AppendIndex(&sizes_, 0);
  }

  std::shared_ptr<Array> FinishArray(const std::shared_ptr<DataType>& type,
                                     int64_t length, std::shared_ptr<Array> values,
                                     std::shared_ptr<::arrow::Buffer> null_bitmap,
                                     int64_t null_count) {
    return std::make_shared<ArrayType>(type, length, FinishBuffer(&offsets_),
                                       FinishBuffer(&sizes_), std::move(values),
                                       std::move(null_bitmap), null_count);
  }

 private:
  ::arrow::TypedBufferBuilder<Offset> offsets_;
  ::arrow::TypedBufferBuilder<Offset> sizes_;
};

template <>
class ListLayout<::arrow::FixedSizeListType> {
 public:
  using ArrayType = typename ::arrow::TypeTraits<::arrow::FixedSizeListType>::ArrayType;

  ListLayout(const ::arrow::FixedSizeListType& type, MemoryPool*)
      : list_size_(type.list_size()) {
    if (list_size_ < 0) {
      throw ParquetException("Invalid fixed-size list size: ", list_size_);
    }
  }

  std::shared_ptr<DataType> MakeType(std::shared_ptr<Field> element_field) const {
    return std::make_shared<::arrow::FixedSizeListType>(std::move(element_field),
                                                        list_size_);
  }

  void ValidateLength(size_t length) const {
    if (std::cmp_not_equal(length, list_size_)) {
      throw ParquetException("Expected fixed-size list of size ", list_size_, ", got ",
                             length);
    }
  }

  int32_t list_size() const { return list_size_; }
  void AppendPresent(int64_t, int64_t) {}
  void AppendNull(int64_t) {}

  std::shared_ptr<Array> FinishArray(const std::shared_ptr<DataType>& type,
                                     int64_t length, std::shared_ptr<Array> values,
                                     std::shared_ptr<::arrow::Buffer> null_bitmap,
                                     int64_t null_count) {
    return std::make_shared<ArrayType>(type, length, std::move(values),
                                       std::move(null_bitmap), null_count);
  }

 private:
  int32_t list_size_;
};

template <std::derived_from<arrow::BaseListType> TypeClass>
class ListShredNode : public ShredNode {
 public:
  ListShredNode(const TypeClass& target, ::arrow::compute::ExecContext* ctx,
                MemoryPool* pool)
      : ShredNode(nullptr, pool), typed_validity_(pool), layout_(target, pool) {
    child_ = CompileShredNode(target.value_type(), ctx, pool);
    const auto& value_field = target.value_field();
    auto element_field = arrow::field(value_field->name(), child_->field_group_type(),
                                      /*nullable=*/false, value_field->metadata());
    typed_type_ = layout_.MakeType(std::move(element_field));
  }

  void AppendParentNull() override {
    AppendGroupValidity(false);
    AppendResidualNull();
    AppendTypedNull();
  }

  void AppendMissing(const VariantMetadataView&) override {
    AppendGroupValidity(true);
    AppendResidualNull();
    AppendTypedNull();
  }

  void AppendValue(const VariantMetadataView& metadata,
                   const VariantValueView& value) override {
    const auto* array = std::get_if<VariantArrayView>(&value.data());
    if constexpr (std::same_as<TypeClass, ::arrow::FixedSizeListType>) {
      if (array != nullptr) {
        layout_.ValidateLength(array->elements().size());
      }
    }
    AppendGroupValidity(true);
    if (array == nullptr) {
      AppendResidual(value);
      AppendTypedNull();
      return;
    }

    AppendResidualNull();
    PARQUET_THROW_NOT_OK(typed_validity_.Append(true));
    const auto start = child_length_;
    for (size_t i = 0; i < array->elements().size(); ++i) {
      auto element = array->GetElement(i);
      DCHECK(element.has_value());
      child_->AppendValue(metadata, *element);
      ++child_length_;
    }
    layout_.AppendPresent(start, child_length_);
  }

  ShreddedArrayParts Finish() override {
    auto values = MakeFieldGroup(child_->Finish());
    const auto length = typed_validity_.length();
    const auto null_count = typed_validity_.false_count();
    auto null_bitmap = internal::FinishNullBitmap(typed_validity_);
    auto typed = layout_.FinishArray(typed_type_, length, std::move(values),
                                     std::move(null_bitmap), null_count);
    return FinishParts(std::move(typed));
  }

 private:
  void AppendTypedNull() {
    PARQUET_THROW_NOT_OK(typed_validity_.Append(false));
    if constexpr (std::same_as<TypeClass, ::arrow::FixedSizeListType>) {
      for (int32_t i = 0; i < layout_.list_size(); ++i) {
        child_->AppendParentNull();
        ++child_length_;
      }
    }
    layout_.AppendNull(child_length_);
  }

  std::unique_ptr<ShredNode> child_;
  ::arrow::TypedBufferBuilder<bool> typed_validity_;
  ListLayout<TypeClass> layout_;
  int64_t child_length_ = 0;
};

void ValidatePrimitiveTarget(const std::shared_ptr<DataType>& target) {
  switch (target->id()) {
    case ::arrow::Type::BOOL:
    case ::arrow::Type::INT8:
    case ::arrow::Type::INT16:
    case ::arrow::Type::INT32:
    case ::arrow::Type::INT64:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DOUBLE:
    case ::arrow::Type::DATE32:
    case ::arrow::Type::BINARY:
    case ::arrow::Type::LARGE_BINARY:
    case ::arrow::Type::BINARY_VIEW:
    case ::arrow::Type::STRING:
    case ::arrow::Type::LARGE_STRING:
    case ::arrow::Type::STRING_VIEW:
      return;
    case ::arrow::Type::DECIMAL32:
    case ::arrow::Type::DECIMAL64:
    case ::arrow::Type::DECIMAL128: {
      const auto& decimal = checked_cast<const ::arrow::DecimalType&>(*target);
      if (decimal.scale() < 0 || decimal.scale() > decimal.precision()) {
        throw ParquetException("Invalid Variant shredding decimal type: ",
                               target->ToString());
      }
      return;
    }
    case ::arrow::Type::TIME64:
      if (checked_cast<const ::arrow::Time64Type&>(*target).unit() == TimeUnit::MICRO) {
        return;
      }
      break;
    case ::arrow::Type::TIMESTAMP: {
      const auto unit = checked_cast<const ::arrow::TimestampType&>(*target).unit();
      if (unit == TimeUnit::MICRO || unit == TimeUnit::NANO) {
        return;
      }
      break;
    }
    case ::arrow::Type::FIXED_SIZE_BINARY:
      if (checked_cast<const ::arrow::FixedSizeBinaryType&>(*target).byte_width() == 16) {
        return;
      }
      break;
    case ::arrow::Type::EXTENSION: {
      const auto& extension = checked_cast<const ::arrow::ExtensionType&>(*target);
      if (extension.extension_name() == "arrow.uuid" &&
          ::arrow::extension::UuidType::IsSupportedStorageType(
              extension.storage_type())) {
        return;
      }
      break;
    }
    default:
      break;
  }
  throw ParquetException("Unsupported Variant shredding type: ", target->ToString());
}

std::unique_ptr<ShredNode> CompileShredNode(const std::shared_ptr<DataType>& target,
                                            ::arrow::compute::ExecContext* ctx,
                                            MemoryPool* pool) {
  if (target == nullptr) {
    throw ParquetException("Variant shredding target type must not be null");
  }
  switch (target->id()) {
    case ::arrow::Type::STRUCT:
      return std::make_unique<ObjectShredNode>(
          checked_cast<const ::arrow::StructType&>(*target), ctx, pool);
    case ::arrow::Type::LIST:
      return std::make_unique<ListShredNode<::arrow::ListType>>(
          checked_cast<const ::arrow::ListType&>(*target), ctx, pool);
    case ::arrow::Type::LARGE_LIST:
      return std::make_unique<ListShredNode<::arrow::LargeListType>>(
          checked_cast<const ::arrow::LargeListType&>(*target), ctx, pool);
    case ::arrow::Type::LIST_VIEW:
      return std::make_unique<ListShredNode<::arrow::ListViewType>>(
          checked_cast<const ::arrow::ListViewType&>(*target), ctx, pool);
    case ::arrow::Type::LARGE_LIST_VIEW:
      return std::make_unique<ListShredNode<::arrow::LargeListViewType>>(
          checked_cast<const ::arrow::LargeListViewType&>(*target), ctx, pool);
    case ::arrow::Type::FIXED_SIZE_LIST:
      return std::make_unique<ListShredNode<::arrow::FixedSizeListType>>(
          checked_cast<const ::arrow::FixedSizeListType&>(*target), ctx, pool);
    default:
      ValidatePrimitiveTarget(target);
      return std::make_unique<PrimitiveShredNode>(target, ctx, pool);
  }
}

}  // namespace

std::shared_ptr<VariantArray> ShredVariantArray(
    const VariantArray& array, const std::shared_ptr<DataType>& typed_value_type,
    MemoryPool* pool) {
  if (array.is_shredded()) {
    throw ParquetException("Cannot shred an already shredded Variant array");
  }
  auto metadata_array = array.metadata();
  auto value_array = array.value();
  DCHECK_NE(metadata_array, nullptr);
  DCHECK_NE(value_array, nullptr);

  ::arrow::compute::ExecContext ctx(pool);
  auto root = CompileShredNode(typed_value_type, &ctx, pool);
  auto output_type = ::arrow::struct_(
      {::arrow::field("metadata", metadata_array->type(), /*nullable=*/false),
       ::arrow::field("value", ::arrow::binary_view()),
       ::arrow::field("typed_value", root->typed_type())});
  DCHECK(::arrow::extension::VariantExtensionType::IsSupportedStorageType(output_type));

  std::string_view last_metadata_bytes;
  std::optional<VariantMetadataView> last_metadata;
  for (int64_t row = 0; row < array.length(); ++row) {
    if (array.IsNull(row)) {
      root->AppendParentNull();
      continue;
    }
    if (metadata_array->IsNull(row) || value_array->IsNull(row)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid unshredded Variant: visible metadata or value is null");
    }
    const auto metadata_bytes = internal::BinaryFieldView(*metadata_array, row);
    if (!last_metadata.has_value() || last_metadata_bytes != metadata_bytes) {
      last_metadata = VariantMetadataView::Make(metadata_bytes);
      last_metadata_bytes = metadata_bytes;
    }
    auto value = VariantValueView::Make(internal::BinaryFieldView(*value_array, row),
                                        *last_metadata);
    root->AppendValue(*last_metadata, value);
  }

  auto parts = root->Finish();
  return MakeVariantArrayFromChildren(
      std::move(output_type),
      {std::move(metadata_array), std::move(parts.value), std::move(parts.typed_value)},
      internal::NullBitmapForOutput(array, pool));
}

}  // namespace parquet::variant
