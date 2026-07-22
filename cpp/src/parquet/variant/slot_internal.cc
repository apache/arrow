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

#include "parquet/variant/slot_internal.h"

#include <optional>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/extension_type.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/unreachable.h"
#include "parquet/exception.h"
#include "parquet/variant/append_table_internal.h"
#include "parquet/variant/array_internal.h"

namespace parquet::variant::internal {

namespace {

using ::arrow::Array;
using ::arrow::DataType;
using ::arrow::ExtensionArray;
using ::arrow::StructArray;
using ::arrow::internal::checked_cast;

#ifdef _MSC_VER
#  define VARIANT_TARGET_DECL(...) (BuildTarget * target, ##__VA_ARGS__)
#  define VARIANT_OBJECT_CALL_ARGS(...) destination.field_name, ##__VA_ARGS__
#else
#  define VARIANT_TARGET_DECL(...) \
    (BuildTarget * target __VA_OPT__(, ) __VA_ARGS__)  // NOLINT
#  define VARIANT_OBJECT_CALL_ARGS(...) \
    destination.field_name __VA_OPT__(, ) __VA_ARGS__  // NOLINT
#endif

#define DEFINE_TARGET_APPEND(NAME, decl_args, ...)                          \
  void Append##NAME VARIANT_TARGET_DECL decl_args {                         \
    if (target == nullptr) {                                                \
      return;                                                               \
    }                                                                       \
    std::visit(                                                             \
        [&](auto& destination) {                                            \
          using Target = std::decay_t<decltype(destination)>;               \
          auto& builder = destination.builder.get();                        \
          if constexpr (std::is_same_v<Target, BuildTarget::ObjectField>) { \
            builder.Append##NAME(VARIANT_OBJECT_CALL_ARGS(__VA_ARGS__));    \
          } else {                                                          \
            builder.Append##NAME(__VA_ARGS__);                              \
          }                                                                 \
        },                                                                  \
        target->destination);                                               \
  }
PARQUET_VARIANT_DIRECT_APPEND_LIST(DEFINE_TARGET_APPEND)
PARQUET_VARIANT_SPECIAL_APPEND_LIST(DEFINE_TARGET_APPEND)
DEFINE_TARGET_APPEND(EncodedValue, (std::string_view value), value)
#undef DEFINE_TARGET_APPEND

#undef VARIANT_OBJECT_CALL_ARGS
#undef VARIANT_TARGET_DECL

VariantObjectBuilder StartObject(BuildTarget& target) {
  return std::visit(
      [](auto& destination) -> VariantObjectBuilder {
        using Target = std::decay_t<decltype(destination)>;
        auto& builder = destination.builder.get();
        if constexpr (std::is_same_v<Target, BuildTarget::ObjectField>) {
          return builder.StartObject(destination.field_name);
        } else {
          return builder.StartObject();
        }
      },
      target.destination);
}

VariantListBuilder StartList(BuildTarget& target) {
  return std::visit(
      [](auto& destination) -> VariantListBuilder {
        using Target = std::decay_t<decltype(destination)>;
        auto& builder = destination.builder.get();
        if constexpr (std::is_same_v<Target, BuildTarget::ObjectField>) {
          return builder.StartList(destination.field_name);
        } else {
          return builder.StartList();
        }
      },
      target.destination);
}

[[noreturn]] void UnsupportedType(const DataType& type) {
  throw ParquetInvalidOrCorruptedFileException("Illegal shredded value type: ",
                                               type.ToString());
}

std::optional<std::string_view> GetValueSlot(const std::shared_ptr<Array>& value_array,
                                             int64_t row) {
  if (value_array == nullptr || value_array->IsNull(row)) {
    return std::nullopt;
  }
  return BinaryFieldView(*value_array, row);
}

CompiledVariantRowPlan CompileVariantRowPlanImpl(
    const std::shared_ptr<Array>& value_array, const std::shared_ptr<Array>& typed_array,
    std::string_view path);

CompiledVariantRowPlan::FieldGroup CompileFieldGroupPlan(
    const std::shared_ptr<Array>& field_group_array, std::string_view path,
    CompiledVariantRowPlan& parent) {
  if (field_group_array->type_id() != ::arrow::Type::STRUCT) {
    throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant field at ",
                                                 path, ": expected struct storage, got ",
                                                 field_group_array->type()->ToString());
  }
  const auto& field_struct = checked_cast<const StructArray&>(*field_group_array);
  const auto child_plan_index = parent.children.size();
  parent.children.push_back(
      CompileVariantRowPlanImpl(field_struct.GetFieldByName("value"),
                                field_struct.GetFieldByName("typed_value"), path));
  return {
      .array = field_group_array,
      .child_plan_index = child_plan_index,
  };
}

CompiledTypedScalarPlan CompileTypedScalarPlan(
    const std::shared_ptr<Array>& typed_array) {
  switch (typed_array->type_id()) {
    case ::arrow::Type::BOOL:
      return {.kind = TypedScalarKind::kBoolean};
    case ::arrow::Type::INT8:
      return {.kind = TypedScalarKind::kInt8};
    case ::arrow::Type::INT16:
      return {.kind = TypedScalarKind::kInt16};
    case ::arrow::Type::INT32:
      return {.kind = TypedScalarKind::kInt32};
    case ::arrow::Type::INT64:
      return {.kind = TypedScalarKind::kInt64};
    case ::arrow::Type::FLOAT:
      return {.kind = TypedScalarKind::kFloat};
    case ::arrow::Type::DOUBLE:
      return {.kind = TypedScalarKind::kDouble};
    case ::arrow::Type::BINARY:
    case ::arrow::Type::LARGE_BINARY:
    case ::arrow::Type::BINARY_VIEW:
      return {.kind = TypedScalarKind::kBinary, .physical_type = typed_array->type_id()};
    case ::arrow::Type::STRING:
    case ::arrow::Type::LARGE_STRING:
    case ::arrow::Type::STRING_VIEW:
      return {.kind = TypedScalarKind::kString, .physical_type = typed_array->type_id()};
    case ::arrow::Type::DATE32:
      return {.kind = TypedScalarKind::kDate};
    case ::arrow::Type::TIME64: {
      const auto& type = checked_cast<const ::arrow::Time64Type&>(*typed_array->type());
      if (type.unit() != ::arrow::TimeUnit::MICRO) {
        UnsupportedType(*typed_array->type());
      }
      return {.kind = TypedScalarKind::kTimeNTZMicros};
    }
    case ::arrow::Type::TIMESTAMP: {
      const auto& type =
          checked_cast<const ::arrow::TimestampType&>(*typed_array->type());
      switch (type.unit()) {
        case ::arrow::TimeUnit::MICRO:
          return {.kind = TypedScalarKind::kTimestampMicros,
                  .adjusted_to_utc = !type.timezone().empty()};
        case ::arrow::TimeUnit::NANO:
          return {.kind = TypedScalarKind::kTimestampNanos,
                  .adjusted_to_utc = !type.timezone().empty()};
        case ::arrow::TimeUnit::SECOND:
        case ::arrow::TimeUnit::MILLI:
          UnsupportedType(*typed_array->type());
      }
      ::arrow::Unreachable("Unexpected timestamp unit");
    }
    case ::arrow::Type::DECIMAL32: {
      const auto& type =
          checked_cast<const ::arrow::Decimal32Type&>(*typed_array->type());
      return {.kind = TypedScalarKind::kDecimal4From32,
              .scale = static_cast<uint8_t>(type.scale())};
    }
    case ::arrow::Type::DECIMAL64: {
      const auto& type =
          checked_cast<const ::arrow::Decimal64Type&>(*typed_array->type());
      if (type.precision() <= 9) {
        return {.kind = TypedScalarKind::kDecimal4From64,
                .scale = static_cast<uint8_t>(type.scale())};
      }
      return {.kind = TypedScalarKind::kDecimal8From64,
              .scale = static_cast<uint8_t>(type.scale())};
    }
    case ::arrow::Type::DECIMAL128: {
      const auto& type =
          checked_cast<const ::arrow::Decimal128Type&>(*typed_array->type());
      if (type.precision() <= 9) {
        return {.kind = TypedScalarKind::kDecimal4From128,
                .scale = static_cast<uint8_t>(type.scale())};
      }
      if (type.precision() <= 18) {
        return {.kind = TypedScalarKind::kDecimal8From128,
                .scale = static_cast<uint8_t>(type.scale())};
      }
      return {.kind = TypedScalarKind::kDecimal16From128,
              .scale = static_cast<uint8_t>(type.scale())};
    }
    case ::arrow::Type::FIXED_SIZE_BINARY: {
      const auto& type =
          checked_cast<const ::arrow::FixedSizeBinaryType&>(*typed_array->type());
      if (type.byte_width() != 16) {
        UnsupportedType(*typed_array->type());
      }
      return {.kind = TypedScalarKind::kUuidFixed};
    }
    case ::arrow::Type::EXTENSION: {
      const auto& ext_type =
          checked_cast<const ::arrow::ExtensionType&>(*typed_array->type());
      if (ext_type.extension_name() != "arrow.uuid") {
        UnsupportedType(*typed_array->type());
      }
      return {.kind = TypedScalarKind::kUuidExtension};
    }
    default:
      UnsupportedType(*typed_array->type());
  }
}

CompiledVariantRowPlan CompileVariantRowPlanImpl(
    const std::shared_ptr<Array>& value_array, const std::shared_ptr<Array>& typed_array,
    std::string_view path) {
  CompiledVariantRowPlan plan{
      .value_array = value_array,
      .typed = std::nullopt,
      .children = {},
  };
  if (typed_array == nullptr) {
    return plan;
  }

  switch (typed_array->type_id()) {
    case ::arrow::Type::STRUCT: {
      const auto& typed_struct = checked_cast<const StructArray&>(*typed_array);
      CompiledVariantRowPlan::Object object;
      object.fields.reserve(typed_struct.struct_type()->num_fields());
      object.field_names.reserve(typed_struct.struct_type()->num_fields());
      for (int i = 0; i < typed_struct.struct_type()->num_fields(); ++i) {
        const auto& field_name = typed_struct.struct_type()->field(i)->name();
        if (!object.field_names.insert(field_name).second) {
          throw ParquetInvalidOrCorruptedFileException(
              "Invalid shredded Variant: duplicate shredded object field '", field_name,
              "'");
        }
        auto field_group = CompileFieldGroupPlan(typed_struct.field(i), field_name, plan);
        object.fields.push_back(
            {.field_name = field_name, .field_group = std::move(field_group)});
      }
      plan.typed.emplace(CompiledVariantRowPlan::Typed{
          .array = typed_array,
          .plan = std::move(object),
      });
      return plan;
    }
    case ::arrow::Type::LIST:
    case ::arrow::Type::LARGE_LIST:
    case ::arrow::Type::LIST_VIEW:
    case ::arrow::Type::LARGE_LIST_VIEW:
    case ::arrow::Type::FIXED_SIZE_LIST: {
      auto values = ValuesArray(*typed_array);
      auto element = CompileFieldGroupPlan(values, path, plan);
      plan.typed.emplace(CompiledVariantRowPlan::Typed{
          .array = typed_array,
          .plan = CompiledVariantRowPlan::Array{.element = std::move(element)},
      });
      return plan;
    }
    default:
      plan.typed.emplace(CompiledVariantRowPlan::Typed{
          .array = typed_array,
          .plan =
              CompiledVariantRowPlan::Primitive{
                  .scalar = CompileTypedScalarPlan(typed_array),
              },
      });
      return plan;
  }
}

void ValidateTypedFieldNames(const VariantMetadataView& metadata,
                             const CompiledVariantRowPlan::Object& object) {
  for (const auto& field : object.fields) {
    if (!metadata.FindString(field.field_name).has_value()) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid shredded Variant: shredded field '", field.field_name,
          "' is not in metadata dictionary");
    }
  }
}

std::string_view GetStringScalarView(const CompiledTypedScalarPlan& plan,
                                     const std::shared_ptr<Array>& typed_array,
                                     int64_t row) {
  switch (plan.physical_type) {
    case ::arrow::Type::STRING:
      return checked_cast<const ::arrow::StringArray&>(*typed_array).GetView(row);
    case ::arrow::Type::LARGE_STRING:
      return checked_cast<const ::arrow::LargeStringArray&>(*typed_array).GetView(row);
    case ::arrow::Type::STRING_VIEW:
      return checked_cast<const ::arrow::StringViewArray&>(*typed_array).GetView(row);
    default:
      DCHECK(false);
      return {};
  }
}

void AppendTypedScalar(BuildTarget* target, const CompiledTypedScalarPlan& plan,
                       const std::shared_ptr<Array>& typed_array, int64_t row) {
  if (target == nullptr) {
    return;
  }
  switch (plan.kind) {
    case TypedScalarKind::kBoolean:
      AppendBoolean(target,
                    checked_cast<const ::arrow::BooleanArray&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kInt8:
      AppendInt8(target,
                 checked_cast<const ::arrow::Int8Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kInt16:
      AppendInt16(target,
                  checked_cast<const ::arrow::Int16Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kInt32:
      AppendInt32(target,
                  checked_cast<const ::arrow::Int32Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kInt64:
      AppendInt64(target,
                  checked_cast<const ::arrow::Int64Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kFloat:
      AppendFloat(target,
                  checked_cast<const ::arrow::FloatArray&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kDouble:
      AppendDouble(target,
                   checked_cast<const ::arrow::DoubleArray&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kBinary:
      AppendBinary(target, BinaryFieldView(*typed_array, row));
      return;
    case TypedScalarKind::kString: {
      auto value = GetStringScalarView(plan, typed_array, row);
      if (value.size() <= 0x3f) {
        AppendShortString(target, value);
      } else {
        AppendString(target, value);
      }
      return;
    }
    case TypedScalarKind::kDate:
      AppendDate(target,
                 checked_cast<const ::arrow::Date32Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kTimeNTZMicros:
      AppendTimeNTZMicros(
          target, checked_cast<const ::arrow::Time64Array&>(*typed_array).Value(row));
      return;
    case TypedScalarKind::kTimestampMicros:
      AppendTimestampMicros(
          target, checked_cast<const ::arrow::TimestampArray&>(*typed_array).Value(row),
          plan.adjusted_to_utc);
      return;
    case TypedScalarKind::kTimestampNanos:
      AppendTimestampNanos(
          target, checked_cast<const ::arrow::TimestampArray&>(*typed_array).Value(row),
          plan.adjusted_to_utc);
      return;
    case TypedScalarKind::kDecimal4From32: {
      const auto value = ::arrow::Decimal32(
          checked_cast<const ::arrow::Decimal32Array&>(*typed_array).GetValue(row));
      AppendDecimal4(target, value, plan.scale);
      return;
    }
    case TypedScalarKind::kDecimal4From64: {
      const auto value = ::arrow::Decimal64(
          checked_cast<const ::arrow::Decimal64Array&>(*typed_array).GetValue(row));
      PARQUET_ASSIGN_OR_THROW(auto raw, value.ToInteger<int32_t>());
      AppendDecimal4(target, ::arrow::Decimal32(raw), plan.scale);
      return;
    }
    case TypedScalarKind::kDecimal8From64: {
      const auto value = ::arrow::Decimal64(
          checked_cast<const ::arrow::Decimal64Array&>(*typed_array).GetValue(row));
      AppendDecimal8(target, value, plan.scale);
      return;
    }
    case TypedScalarKind::kDecimal4From128: {
      const auto value = ::arrow::Decimal128(
          checked_cast<const ::arrow::Decimal128Array&>(*typed_array).GetValue(row));
      PARQUET_ASSIGN_OR_THROW(auto raw, value.ToInteger<int32_t>());
      AppendDecimal4(target, ::arrow::Decimal32(raw), plan.scale);
      return;
    }
    case TypedScalarKind::kDecimal8From128: {
      const auto value = ::arrow::Decimal128(
          checked_cast<const ::arrow::Decimal128Array&>(*typed_array).GetValue(row));
      PARQUET_ASSIGN_OR_THROW(auto raw, value.ToInteger<int64_t>());
      AppendDecimal8(target, ::arrow::Decimal64(raw), plan.scale);
      return;
    }
    case TypedScalarKind::kDecimal16From128: {
      const auto value = ::arrow::Decimal128(
          checked_cast<const ::arrow::Decimal128Array&>(*typed_array).GetValue(row));
      AppendDecimal16(target, value, plan.scale);
      return;
    }
    case TypedScalarKind::kUuidFixed: {
      auto value =
          checked_cast<const ::arrow::FixedSizeBinaryArray&>(*typed_array).GetView(row);
      AppendUuid(target, value);
      return;
    }
    case TypedScalarKind::kUuidExtension: {
      const auto& ext_array = checked_cast<const ExtensionArray&>(*typed_array);
      auto storage = ext_array.storage();
      auto value =
          checked_cast<const ::arrow::FixedSizeBinaryArray&>(*storage).GetView(row);
      AppendUuid(target, value);
      return;
    }
  }
}

template <bool strict>
void ProcessListElement(const VariantMetadataView& metadata,
                        const CompiledVariantRowPlan& parent_plan,
                        const CompiledVariantRowPlan::FieldGroup& field_group_plan,
                        int64_t row, std::string_view path, BuildTarget* target) {
  const auto& field_array = *field_group_plan.array;
  if (field_array.IsNull(row)) {
    if constexpr (strict) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid shredded Variant field at ", path, ": field group must be required");
    } else {
      AppendVariantNull(target);
      return;
    }
  }

  DCHECK_LT(field_group_plan.child_plan_index, parent_plan.children.size());
  const auto& row_plan = parent_plan.children[field_group_plan.child_plan_index];
  ProcessSlot<strict>(metadata, row_plan, row, target, path);
}

template <bool strict>
void ProcessTypedArraySlot(const VariantMetadataView& metadata,
                           const CompiledVariantRowPlan& parent_plan,
                           const CompiledVariantRowPlan::Typed& typed,
                           const CompiledVariantRowPlan::Array& array, int64_t row,
                           std::string_view path, BuildTarget* target) {
  DCHECK(!typed.array->IsNull(row));

  std::optional<VariantListBuilder> list_builder;
  std::optional<BuildTarget> child_target;
  BuildTarget* child_target_ptr = nullptr;
  if (target != nullptr) {
    list_builder.emplace(StartList(*target));
    child_target.emplace(BuildTarget{BuildTarget::ListElement{*list_builder}});
    child_target_ptr = &*child_target;
  }
  const auto [offset, length] = ValuesRangeAt(*typed.array, row);
  for (int64_t i = 0; i < length; ++i) {
    ProcessListElement<strict>(metadata, parent_plan, array.element, offset + i, path,
                               child_target_ptr);
  }
  if (list_builder.has_value()) {
    list_builder->Finish();
  }
}

template <bool strict>
bool TryProcessSlot(const VariantMetadataView& metadata,
                    const CompiledVariantRowPlan& plan, int64_t row,
                    std::string_view path, BuildTarget* target);

template <bool strict>
void ProcessObjectField(const VariantMetadataView& metadata,
                        const CompiledVariantRowPlan& parent_plan,
                        const CompiledVariantRowPlan::ObjectField& field, int64_t row,
                        BuildTarget* target) {
  const auto& field_array = *field.field_group.array;
  if (field_array.IsNull(row)) {
    if constexpr (strict) {
      throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant field at ",
                                                   field.field_name,
                                                   ": field group must be required");
    } else {
      return;
    }
  }

  DCHECK_LT(field.field_group.child_plan_index, parent_plan.children.size());
  const auto& row_plan = parent_plan.children[field.field_group.child_plan_index];
  if constexpr (strict) {
    if (row_plan.value_array == nullptr) {
      throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant field at ",
                                                   field.field_name,
                                                   ": missing value field");
    }
  }

  if (target != nullptr) {
    std::get<BuildTarget::ObjectField>(target->destination).field_name = field.field_name;
  }
  TryProcessSlot<strict>(metadata, row_plan, row, field.field_name, target);
}

template <bool strict>
void ProcessObjectResidual(const VariantValueView& value_view,
                           const CompiledVariantRowPlan::Object& object,
                           BuildTarget* target) {
  DCHECK_EQ(value_view.basic_type(), VariantBasicType::kObject);
  const auto& object_view = std::get<VariantObjectView>(value_view.data());
  for (const auto& field : object_view.fields()) {
    if (object.field_names.contains(field.name)) {
      if (strict || target != nullptr) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: value object contains shredded field: ",
            field.name);
      }
      continue;
    }
    if (target == nullptr) {
      continue;
    }
    BuildTarget residual_target = *target;
    std::get<BuildTarget::ObjectField>(residual_target.destination).field_name =
        field.name;
    AppendEncodedValue(&residual_target, field.value);
  }
}

template <bool strict>
void ProcessTypedObjectSlot(const VariantMetadataView& metadata,
                            const CompiledVariantRowPlan& parent_plan,
                            const CompiledVariantRowPlan::Typed& typed,
                            const CompiledVariantRowPlan::Object& object, int64_t row,
                            std::optional<std::string_view> value, BuildTarget* target) {
  DCHECK(!typed.array->IsNull(row));

  std::optional<VariantValueView> object_value_view;
  if (value.has_value()) {
    if (VariantValueView::PeekBasicType(*value) != VariantBasicType::kObject) {
      throw ParquetInvalidOrCorruptedFileException(
          "Expected object in value field for partially shredded struct");
    }
    if (target != nullptr) {
      object_value_view = VariantValueView::Make(*value, metadata);
    } else if constexpr (strict) {
      object_value_view = VariantValueView::MakeWithValidate(*value, metadata);
    } else {
      VariantValueView::Validate(*value, metadata);
    }
  }

  std::optional<VariantObjectBuilder> object_builder;
  std::optional<BuildTarget> child_target;
  BuildTarget* child_target_ptr = nullptr;
  if (target != nullptr) {
    object_builder.emplace(StartObject(*target));
    child_target.emplace(BuildTarget{BuildTarget::ObjectField{
        .builder = *object_builder,
        .field_name = {},
    }});
    child_target_ptr = &*child_target;
  }
  for (const auto& field : object.fields) {
    ProcessObjectField<strict>(metadata, parent_plan, field, row, child_target_ptr);
  }
  if (object_value_view.has_value()) {
    ProcessObjectResidual<strict>(*object_value_view, object, child_target_ptr);
  }
  if (object_builder.has_value()) {
    object_builder->Finish();
  }
}

template <bool strict>
bool TryProcessSlot(const VariantMetadataView& metadata,
                    const CompiledVariantRowPlan& plan, int64_t row,
                    std::string_view path, BuildTarget* target) {
  const auto value = GetValueSlot(plan.value_array, row);
  if (!plan.typed.has_value()) {
    if (!value.has_value()) {
      return false;
    }
    if (target == nullptr) {
      VariantValueView::Validate(*value, metadata);
    }
    AppendEncodedValue(target, *value);
    return true;
  }

  const auto& typed = *plan.typed;
  return std::visit(
      [&](const auto& typed_plan) -> bool {
        using Plan = std::decay_t<decltype(typed_plan)>;
        const bool typed_present = !typed.array->IsNull(row);

        if constexpr (std::is_same_v<Plan, CompiledVariantRowPlan::Object>) {
          ValidateTypedFieldNames(metadata, typed_plan);
        }

        if (!typed_present) {
          if (!value.has_value()) {
            return false;
          }
          if (target == nullptr) {
            VariantValueView::Validate(*value, metadata);
          }
          if constexpr (std::is_same_v<Plan, CompiledVariantRowPlan::Array> ||
                        std::is_same_v<Plan, CompiledVariantRowPlan::Object>) {
            const auto basic_type = VariantValueView::PeekBasicType(*value);
            if constexpr (std::is_same_v<Plan, CompiledVariantRowPlan::Array>) {
              if (basic_type == VariantBasicType::kArray) {
                throw ParquetInvalidOrCorruptedFileException(
                    "Invalid shredded Variant: array value must be stored in "
                    "typed_value");
              }
            } else if (basic_type == VariantBasicType::kObject) {
              throw ParquetInvalidOrCorruptedFileException(
                  "Invalid shredded Variant: object value requires object typed_value");
            }
          }
          AppendEncodedValue(target, *value);
          return true;
        }

        if constexpr (!std::is_same_v<Plan, CompiledVariantRowPlan::Object>) {
          if (value.has_value()) {
            throw ParquetInvalidOrCorruptedFileException(
                "Invalid shredded variant: both value and typed_value are non-null");
          }
        }

        if constexpr (std::is_same_v<Plan, CompiledVariantRowPlan::Primitive>) {
          AppendTypedScalar(target, typed_plan.scalar, typed.array, row);
        } else if constexpr (std::is_same_v<Plan, CompiledVariantRowPlan::Array>) {
          ProcessTypedArraySlot<strict>(metadata, plan, typed, typed_plan, row, path,
                                        target);
        } else {
          ProcessTypedObjectSlot<strict>(metadata, plan, typed, typed_plan, row, value,
                                         target);
        }
        return true;
      },
      typed.plan);
}

}  // namespace

CompiledVariantRowPlan CompileVariantRowPlan(const std::shared_ptr<Array>& value_array,
                                             const std::shared_ptr<Array>& typed_array) {
  return CompileVariantRowPlanImpl(value_array, typed_array, "root");
}

template <bool strict>
void ProcessSlot(const VariantMetadataView& metadata, const CompiledVariantRowPlan& plan,
                 int64_t row, BuildTarget* target, std::string_view path) {
  if (TryProcessSlot<strict>(metadata, plan, row, path, target)) {
    return;
  }

  if constexpr (strict) {
    throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant at ", path,
                                                 ": value and typed_value are both null");
  }
  AppendVariantNull(target);
}

template void ProcessSlot<true>(const VariantMetadataView& metadata,
                                const CompiledVariantRowPlan& plan, int64_t row,
                                BuildTarget* target, std::string_view path);
template void ProcessSlot<false>(const VariantMetadataView& metadata,
                                 const CompiledVariantRowPlan& plan, int64_t row,
                                 BuildTarget* target, std::string_view path);

}  // namespace parquet::variant::internal
