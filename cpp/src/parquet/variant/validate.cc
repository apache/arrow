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

#include "parquet/variant/validate.h"

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "parquet/variant/encoding.h"

namespace parquet::variant {

namespace {

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::ExtensionArray;
using ::arrow::ExtensionType;
using ::arrow::MemoryPool;
using ::arrow::Result;
using ::arrow::Status;
using ::arrow::extension::kVariantExtensionName;
using ::arrow::internal::checked_cast;

enum class VariantShreddedTypeKind {
  None,
  Primitive,
  Object,
  Array,
};

struct VariantValidationPlan {
  std::shared_ptr<Array> array;
  std::vector<VariantValidationPlan> children;
};

Result<std::shared_ptr<Array>> ValuesArray(const Array& array) {
  switch (array.type_id()) {
    case ::arrow::Type::LIST_VIEW:
      return checked_cast<const ::arrow::ListViewArray&>(array).values();
    case ::arrow::Type::LARGE_LIST_VIEW:
      return checked_cast<const ::arrow::LargeListViewArray&>(array).values();
    case ::arrow::Type::LIST:
      return checked_cast<const ::arrow::ListArray&>(array).values();
    case ::arrow::Type::LARGE_LIST:
      return checked_cast<const ::arrow::LargeListArray&>(array).values();
    case ::arrow::Type::FIXED_SIZE_LIST:
      return checked_cast<const ::arrow::FixedSizeListArray&>(array).values();
    case ::arrow::Type::MAP:
      return checked_cast<const ::arrow::MapArray&>(array).values();
    default:
      return Status::Invalid("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

Result<std::optional<VariantValidationPlan>> BuildVariantValidationPlan(
    std::shared_ptr<Array> array) {
  switch (array->type_id()) {
    case ::arrow::Type::EXTENSION: {
      const auto& ext_array = checked_cast<const ExtensionArray&>(*array);
      const auto& ext_type = checked_cast<const ExtensionType&>(*array->type());
      if (ext_type.extension_name() == kVariantExtensionName) {
        return VariantValidationPlan{.array = std::move(array), .children = {}};
      }
      return BuildVariantValidationPlan(ext_array.storage());
    }
    case ::arrow::Type::STRUCT: {
      const auto& struct_array = checked_cast<const ::arrow::StructArray&>(*array);
      std::vector<VariantValidationPlan> children;
      for (auto field : struct_array.fields()) {
        ARROW_ASSIGN_OR_RAISE(auto child_plan,
                              BuildVariantValidationPlan(std::move(field)));
        if (child_plan.has_value()) {
          children.push_back(std::move(*child_plan));
        }
      }
      if (children.empty()) {
        return std::nullopt;
      }
      return VariantValidationPlan{.array = std::move(array),
                                   .children = std::move(children)};
    }
    case ::arrow::Type::LIST_VIEW:
    case ::arrow::Type::LARGE_LIST_VIEW:
    case ::arrow::Type::LIST:
    case ::arrow::Type::LARGE_LIST:
    case ::arrow::Type::FIXED_SIZE_LIST:
    case ::arrow::Type::MAP: {
      ARROW_ASSIGN_OR_RAISE(auto values, ValuesArray(*array));
      ARROW_ASSIGN_OR_RAISE(auto child_plan,
                            BuildVariantValidationPlan(std::move(values)));
      if (!child_plan.has_value()) {
        return std::nullopt;
      }
      std::vector<VariantValidationPlan> children;
      children.push_back(std::move(*child_plan));
      return VariantValidationPlan{.array = std::move(array),
                                   .children = std::move(children)};
    }
    default: {
      std::vector<VariantValidationPlan> children;
      for (const auto& child_data : array->data()->child_data) {
        if (child_data != nullptr) {
          ARROW_ASSIGN_OR_RAISE(auto child_plan, BuildVariantValidationPlan(
                                                     ::arrow::MakeArray(child_data)));
          if (child_plan.has_value()) {
            children.push_back(std::move(*child_plan));
          }
        }
      }
      if (children.empty()) {
        return std::nullopt;
      }
      return VariantValidationPlan{.array = std::move(array),
                                   .children = std::move(children)};
    }
  }
}

Result<std::string_view> BinaryFieldView(const Array& array, int64_t row) {
  switch (array.type_id()) {
    case ::arrow::Type::BINARY:
      return checked_cast<const ::arrow::BinaryArray&>(array).GetView(row);
    case ::arrow::Type::LARGE_BINARY:
      return checked_cast<const ::arrow::LargeBinaryArray&>(array).GetView(row);
    case ::arrow::Type::BINARY_VIEW:
      return checked_cast<const ::arrow::BinaryViewArray&>(array).GetView(row);
    default:
      return Status::Invalid("Expected binary Variant field, got ",
                             array.type()->ToString());
  }
}

VariantShreddedTypeKind ShreddedTypeKind(const DataType& type) {
  switch (type.id()) {
    case ::arrow::Type::STRUCT:
      return VariantShreddedTypeKind::Object;
    case ::arrow::Type::LIST:
    case ::arrow::Type::LARGE_LIST:
    case ::arrow::Type::LIST_VIEW:
    case ::arrow::Type::LARGE_LIST_VIEW:
    case ::arrow::Type::FIXED_SIZE_LIST:
      return VariantShreddedTypeKind::Array;
    default:
      return VariantShreddedTypeKind::Primitive;
  }
}

Status ValidateVariantShredding(
    const VariantMetadataView& metadata, std::string_view value, bool typed_value_present,
    VariantShreddedTypeKind typed_value_kind,
    std::span<const std::string_view> shredded_field_names = {}) {
  if (typed_value_kind == VariantShreddedTypeKind::None) {
    if (value.empty()) {
      return Status::OK();
    }
    return VariantValueView::Validate(value, metadata);
  }

  if (typed_value_kind == VariantShreddedTypeKind::Object) {
    for (const auto& name : shredded_field_names) {
      if (!metadata.FindString(name).has_value()) {
        return Status::Invalid("Invalid shredded Variant: shredded field '", name,
                               "' is not in metadata dictionary");
      }
    }
  }

  if (value.empty()) {
    return Status::OK();
  }
  ARROW_ASSIGN_OR_RAISE(auto value_view, VariantValueView::Make(value, metadata));

  switch (typed_value_kind) {
    case VariantShreddedTypeKind::Primitive:
      if (typed_value_present) {
        return Status::Invalid(
            "Invalid shredded Variant: value and primitive typed_value are both "
            "non-null");
      }
      break;
    case VariantShreddedTypeKind::Array:
      if (typed_value_present) {
        return Status::Invalid(
            "Invalid shredded Variant: value and array typed_value are both "
            "non-null");
      }
      if (value_view.basic_type() == VariantBasicType::kArray) {
        return Status::Invalid(
            "Invalid shredded Variant: array value must be stored in typed_value");
      }
      break;
    case VariantShreddedTypeKind::Object:
    default:
      if (value_view.basic_type() == VariantBasicType::kObject) {
        if (!typed_value_present) {
          return Status::Invalid(
              "Invalid shredded Variant: object value requires object typed_value");
        }
        for (const auto& name : shredded_field_names) {
          if (std::get<VariantObjectView>(value_view.data()).ContainsField(name)) {
            return Status::Invalid(
                "Invalid shredded Variant: value object contains shredded field: ", name);
          }
        }
      } else if (typed_value_present) {
        return Status::Invalid(
            "Invalid shredded Variant: partially shredded value must be an object");
      }
      break;
  }
  return Status::OK();
}

template <typename ArrayType>
std::pair<int64_t, int64_t> ValueOffsetAndLength(const Array& array, int64_t row) {
  const auto& typed_array = checked_cast<const ArrayType&>(array);
  return {typed_array.value_offset(row), typed_array.value_length(row)};
}

Result<std::pair<int64_t, int64_t>> ValuesRangeAt(const Array& array, int64_t row) {
  switch (array.type_id()) {
    case ::arrow::Type::LIST:
      return ValueOffsetAndLength<::arrow::ListArray>(array, row);
    case ::arrow::Type::LARGE_LIST:
      return ValueOffsetAndLength<::arrow::LargeListArray>(array, row);
    case ::arrow::Type::LIST_VIEW:
      return ValueOffsetAndLength<::arrow::ListViewArray>(array, row);
    case ::arrow::Type::LARGE_LIST_VIEW:
      return ValueOffsetAndLength<::arrow::LargeListViewArray>(array, row);
    case ::arrow::Type::MAP:
      return ValueOffsetAndLength<::arrow::MapArray>(array, row);
    case ::arrow::Type::FIXED_SIZE_LIST:
      return ValueOffsetAndLength<::arrow::FixedSizeListArray>(array, row);
    default:
      return Status::Invalid("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

Status ValidateShreddedVariantSlot(const VariantMetadataView& metadata,
                                   const std::shared_ptr<Array>& value_array,
                                   const std::shared_ptr<Array>& typed_array, int64_t row,
                                   bool allow_missing, std::string_view path);

Status ValidateShreddedVariantField(const VariantMetadataView& metadata,
                                    const Array& field_array, int64_t row,
                                    bool allow_missing, std::string_view path) {
  if (field_array.type_id() != ::arrow::Type::STRUCT) {
    return Status::Invalid("Invalid shredded Variant field at ", path,
                           ": expected struct storage, got ",
                           field_array.type()->ToString());
  }
  if (field_array.IsNull(row)) {
    return Status::Invalid("Invalid shredded Variant field at ", path,
                           ": field group must be required");
  }
  const auto& field_struct = checked_cast<const ::arrow::StructArray&>(field_array);
  auto value_array = field_struct.GetFieldByName("value");
  auto typed_array = field_struct.GetFieldByName("typed_value");
  return ValidateShreddedVariantSlot(metadata, value_array, typed_array, row,
                                     allow_missing, path);
}

Status ValidateShreddedVariantSlot(const VariantMetadataView& metadata,
                                   const std::shared_ptr<Array>& value_array,
                                   const std::shared_ptr<Array>& typed_array, int64_t row,
                                   bool allow_missing, std::string_view path) {
  std::string_view value;
  if (value_array != nullptr && !value_array->IsNull(row)) {
    ARROW_ASSIGN_OR_RAISE(value, BinaryFieldView(*value_array, row));
    if (value.empty()) {
      ARROW_RETURN_NOT_OK(VariantValueView::Validate(value, metadata));
    }
  }

  const bool typed_value_present = typed_array != nullptr && !typed_array->IsNull(row);
  if (value.empty() && !typed_value_present) {
    if (allow_missing) {
      return Status::OK();
    }
    return Status::Invalid("Invalid shredded Variant at ", path,
                           ": value and typed_value are both null");
  }

  const auto typed_kind = typed_array == nullptr ? VariantShreddedTypeKind::None
                                                 : ShreddedTypeKind(*typed_array->type());
  std::vector<std::string_view> field_names;
  if (typed_kind == VariantShreddedTypeKind::Object) {
    field_names.reserve(typed_array->type()->num_fields());
    for (const auto& field : typed_array->type()->fields()) {
      field_names.emplace_back(field->name());
    }
  }
  ARROW_RETURN_NOT_OK(ValidateVariantShredding(metadata, value, typed_value_present,
                                               typed_kind, field_names));

  if (!typed_value_present) {
    return Status::OK();
  }

  if (typed_kind == VariantShreddedTypeKind::Object) {
    const auto& typed_struct = checked_cast<const ::arrow::StructArray&>(*typed_array);
    for (int i = 0; i < typed_struct.struct_type()->num_fields(); ++i) {
      ARROW_RETURN_NOT_OK(ValidateShreddedVariantField(
          metadata, *typed_struct.field(i), row, /*allow_missing=*/true,
          typed_struct.struct_type()->field(i)->name()));
    }
  } else if (typed_kind == VariantShreddedTypeKind::Array) {
    ARROW_ASSIGN_OR_RAISE(auto values, ValuesArray(*typed_array));
    ARROW_ASSIGN_OR_RAISE(auto range, ValuesRangeAt(*typed_array, row));
    const auto [offset, length] = range;
    auto elements = values->Slice(offset, length);
    for (int64_t i = 0; i < elements->length(); ++i) {
      ARROW_RETURN_NOT_OK(ValidateShreddedVariantField(metadata, *elements, i,
                                                       /*allow_missing=*/false, path));
    }
  }
  return Status::OK();
}

template <typename VisitVisible>
  requires requires(VisitVisible& visit_visible, int64_t row) {
    ::arrow::ToStatus(visit_visible(row));
  }
Status VisitVisibleRows(const std::shared_ptr<Buffer>& valid_rows, const Array& array,
                        VisitVisible&& visit_visible) {
  if (valid_rows == nullptr && !array.data()->MayHaveNulls()) {
    for (int64_t row = 0; row < array.length(); ++row) {
      ARROW_RETURN_NOT_OK(visit_visible(row));
    }
    return Status::OK();
  }
  return ::arrow::internal::VisitTwoBitBlocks(
      valid_rows != nullptr ? valid_rows->data() : nullptr, /*left_offset=*/0,
      array.data()->MayHaveNulls() ? array.null_bitmap_data() : nullptr, array.offset(),
      array.length(), std::forward<VisitVisible>(visit_visible),
      [] { return Status::OK(); });
}

Status ValidateVariantExtensionArray(const ExtensionArray& array,
                                     const std::shared_ptr<Buffer>& valid_rows) {
  const auto& storage = checked_cast<const ::arrow::StructArray&>(*array.storage());
  auto metadata_array = storage.GetFieldByName("metadata");
  auto value_array = storage.GetFieldByName("value");
  auto typed_array = storage.GetFieldByName("typed_value");

  std::string_view last_metadata_bytes;
  std::optional<VariantMetadataView> last_metadata;
  return VisitVisibleRows(valid_rows, array, [&](int64_t row) {
    if (metadata_array->IsNull(row)) {
      return Status::Invalid("Invalid Variant extension storage: metadata is null");
    }

    ARROW_ASSIGN_OR_RAISE(auto metadata_value, BinaryFieldView(*metadata_array, row));
    if (!last_metadata.has_value() || last_metadata_bytes != metadata_value) {
      ARROW_ASSIGN_OR_RAISE(last_metadata, VariantMetadataView::Make(metadata_value));
      last_metadata_bytes = metadata_value;
    }

    if (typed_array == nullptr) {
      if (value_array->IsNull(row)) {
        return Status::Invalid(
            "Invalid Variant extension storage: unshredded value is null");
      }
      ARROW_ASSIGN_OR_RAISE(auto value, BinaryFieldView(*value_array, row));
      ARROW_RETURN_NOT_OK(VariantValueView::Validate(value, *last_metadata));
    } else {
      ARROW_RETURN_NOT_OK(
          ValidateShreddedVariantSlot(*last_metadata, value_array, typed_array, row,
                                      /*allow_missing=*/false, "variant"));
    }
    return Status::OK();
  });
}

Status ValidateVariantPlan(const VariantValidationPlan& plan, MemoryPool* pool,
                           const std::shared_ptr<Buffer>& valid_rows) {
  switch (plan.array->type_id()) {
    case ::arrow::Type::EXTENSION: {
      return ValidateVariantExtensionArray(
          checked_cast<const ExtensionArray&>(*plan.array), valid_rows);
    }
    case ::arrow::Type::STRUCT: {
      const auto& struct_array = checked_cast<const ::arrow::StructArray&>(*plan.array);
      std::shared_ptr<Buffer> child_valid_rows = valid_rows;
      if (struct_array.data()->MayHaveNulls()) {
        ARROW_ASSIGN_OR_RAISE(
            child_valid_rows,
            ::arrow::internal::OptionalBitmapAnd(
                pool, valid_rows, /*left_offset=*/0, struct_array.null_bitmap(),
                struct_array.offset(), struct_array.length(), /*out_offset=*/0));
      }
      for (const auto& child : plan.children) {
        ARROW_RETURN_NOT_OK(ValidateVariantPlan(child, pool, child_valid_rows));
      }
      return Status::OK();
    }
    case ::arrow::Type::LIST_VIEW:
    case ::arrow::Type::LARGE_LIST_VIEW:
    case ::arrow::Type::LIST:
    case ::arrow::Type::LARGE_LIST:
    case ::arrow::Type::MAP:
    case ::arrow::Type::FIXED_SIZE_LIST: {
      DCHECK_EQ(plan.children.size(), 1);
      ARROW_ASSIGN_OR_RAISE(auto values, ValuesArray(*plan.array));
      ARROW_ASSIGN_OR_RAISE(auto values_valid_rows,
                            ::arrow::AllocateEmptyBitmap(values->length(), pool));
      ARROW_RETURN_NOT_OK(
          VisitVisibleRows(valid_rows, *plan.array, [&](int64_t row) -> Status {
            ARROW_ASSIGN_OR_RAISE(auto range, ValuesRangeAt(*plan.array, row));
            const auto [offset, length] = range;
            ::arrow::bit_util::SetBitsTo(values_valid_rows->mutable_data(), offset,
                                         length, true);
            return Status::OK();
          }));
      return ValidateVariantPlan(plan.children[0], pool, values_valid_rows);
    }
    default: {
      for (const auto& child : plan.children) {
        ARROW_RETURN_NOT_OK(ValidateVariantPlan(child, pool, valid_rows));
      }
      return Status::OK();
    }
  }
}

}  // namespace

Status ValidateVariants(const ::arrow::ChunkedArray& data, MemoryPool* pool) {
  for (const auto& chunk : data.chunks()) {
    ARROW_ASSIGN_OR_RAISE(auto plan, BuildVariantValidationPlan(chunk));
    if (plan.has_value()) {
      ARROW_RETURN_NOT_OK(ValidateVariantPlan(*plan, pool, nullptr));
    }
  }
  return Status::OK();
}

}  // namespace parquet::variant
