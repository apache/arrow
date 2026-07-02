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
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"
#include "parquet/variant/encoding.h"

namespace parquet::variant {

namespace {

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::ExtensionArray;
using ::arrow::ExtensionType;
using ::arrow::MemoryPool;
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

std::shared_ptr<Array> ValuesArray(const Array& array) {
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
      throw ParquetException("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

std::optional<VariantValidationPlan> BuildVariantValidationPlan(
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
        auto child_plan = BuildVariantValidationPlan(std::move(field));
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
      auto values = ValuesArray(*array);
      auto child_plan = BuildVariantValidationPlan(std::move(values));
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
          auto child_plan = BuildVariantValidationPlan(::arrow::MakeArray(child_data));
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

std::string_view BinaryFieldView(const Array& array, int64_t row) {
  switch (array.type_id()) {
    case ::arrow::Type::BINARY:
      return checked_cast<const ::arrow::BinaryArray&>(array).GetView(row);
    case ::arrow::Type::LARGE_BINARY:
      return checked_cast<const ::arrow::LargeBinaryArray&>(array).GetView(row);
    case ::arrow::Type::BINARY_VIEW:
      return checked_cast<const ::arrow::BinaryViewArray&>(array).GetView(row);
    default:
      throw ParquetInvalidOrCorruptedFileException("Expected binary Variant field, got ",
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

void ValidateVariantShredding(
    const VariantMetadataView& metadata, std::string_view value, bool typed_value_present,
    VariantShreddedTypeKind typed_value_kind,
    std::span<const std::string_view> shredded_field_names = {}) {
  if (typed_value_kind == VariantShreddedTypeKind::None) {
    if (value.empty()) {
      return;
    }
    VariantValueView::Validate(value, metadata);
    return;
  }

  if (typed_value_kind == VariantShreddedTypeKind::Object) {
    for (const auto& name : shredded_field_names) {
      if (!metadata.FindString(name).has_value()) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: shredded field '", name,
            "' is not in metadata dictionary");
      }
    }
  }

  if (value.empty()) {
    return;
  }
  auto value_view = VariantValueView::Make(value, metadata);

  switch (typed_value_kind) {
    case VariantShreddedTypeKind::Primitive:
      if (typed_value_present) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: value and primitive typed_value are both "
            "non-null");
      }
      break;
    case VariantShreddedTypeKind::Array:
      if (typed_value_present) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: value and array typed_value are both "
            "non-null");
      }
      if (value_view.basic_type() == VariantBasicType::kArray) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: array value must be stored in typed_value");
      }
      break;
    case VariantShreddedTypeKind::Object:
    default:
      if (value_view.basic_type() == VariantBasicType::kObject) {
        if (!typed_value_present) {
          throw ParquetInvalidOrCorruptedFileException(
              "Invalid shredded Variant: object value requires object typed_value");
        }
        for (const auto& name : shredded_field_names) {
          if (std::get<VariantObjectView>(value_view.data()).ContainsField(name)) {
            throw ParquetInvalidOrCorruptedFileException(
                "Invalid shredded Variant: value object contains shredded field: ", name);
          }
        }
      } else if (typed_value_present) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid shredded Variant: partially shredded value must be an object");
      }
      break;
  }
}

template <typename ArrayType>
std::pair<int64_t, int64_t> ValueOffsetAndLength(const Array& array, int64_t row) {
  const auto& typed_array = checked_cast<const ArrayType&>(array);
  return {typed_array.value_offset(row), typed_array.value_length(row)};
}

std::pair<int64_t, int64_t> ValuesRangeAt(const Array& array, int64_t row) {
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
      throw ParquetException("Expected list or map storage, got ",
                             array.type()->ToString());
  }
}

void ValidateShreddedVariantSlot(const VariantMetadataView& metadata,
                                 const std::shared_ptr<Array>& value_array,
                                 const std::shared_ptr<Array>& typed_array, int64_t row,
                                 bool allow_missing, std::string_view path);

void ValidateShreddedVariantField(const VariantMetadataView& metadata,
                                  const Array& field_array, int64_t row,
                                  bool allow_missing, std::string_view path) {
  if (field_array.type_id() != ::arrow::Type::STRUCT) {
    throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant field at ",
                                                 path, ": expected struct storage, got ",
                                                 field_array.type()->ToString());
  }
  if (field_array.IsNull(row)) {
    throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant field at ",
                                                 path, ": field group must be required");
  }
  const auto& field_struct = checked_cast<const ::arrow::StructArray&>(field_array);
  auto value_array = field_struct.GetFieldByName("value");
  auto typed_array = field_struct.GetFieldByName("typed_value");
  ValidateShreddedVariantSlot(metadata, value_array, typed_array, row, allow_missing,
                              path);
}

void ValidateShreddedVariantSlot(const VariantMetadataView& metadata,
                                 const std::shared_ptr<Array>& value_array,
                                 const std::shared_ptr<Array>& typed_array, int64_t row,
                                 bool allow_missing, std::string_view path) {
  std::string_view value;
  if (value_array != nullptr && !value_array->IsNull(row)) {
    value = BinaryFieldView(*value_array, row);
    if (value.empty()) {
      VariantValueView::Validate(value, metadata);
    }
  }

  const bool typed_value_present = typed_array != nullptr && !typed_array->IsNull(row);
  if (value.empty() && !typed_value_present) {
    if (allow_missing) {
      return;
    }
    throw ParquetInvalidOrCorruptedFileException("Invalid shredded Variant at ", path,
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
  ValidateVariantShredding(metadata, value, typed_value_present, typed_kind, field_names);

  if (!typed_value_present) {
    return;
  }

  if (typed_kind == VariantShreddedTypeKind::Object) {
    const auto& typed_struct = checked_cast<const ::arrow::StructArray&>(*typed_array);
    for (int i = 0; i < typed_struct.struct_type()->num_fields(); ++i) {
      ValidateShreddedVariantField(metadata, *typed_struct.field(i), row,
                                   /*allow_missing=*/true,
                                   typed_struct.struct_type()->field(i)->name());
    }
  } else if (typed_kind == VariantShreddedTypeKind::Array) {
    auto values = ValuesArray(*typed_array);
    auto range = ValuesRangeAt(*typed_array, row);
    const auto [offset, length] = range;
    auto elements = values->Slice(offset, length);
    for (int64_t i = 0; i < elements->length(); ++i) {
      ValidateShreddedVariantField(metadata, *elements, i,
                                   /*allow_missing=*/false, path);
    }
  }
}

template <typename VisitVisible>
void VisitVisibleRows(const std::shared_ptr<Buffer>& valid_rows, const Array& array,
                      VisitVisible&& visit_visible) {
  if (valid_rows == nullptr && !array.data()->MayHaveNulls()) {
    for (int64_t row = 0; row < array.length(); ++row) {
      visit_visible(row);
    }
    return;
  }
  PARQUET_THROW_NOT_OK(::arrow::internal::VisitTwoBitBlocks(
      valid_rows != nullptr ? valid_rows->data() : nullptr, /*left_offset=*/0,
      array.data()->MayHaveNulls() ? array.null_bitmap_data() : nullptr, array.offset(),
      array.length(),
      [&](int64_t row) {
        visit_visible(row);
        return Status::OK();
      },
      [] { return Status::OK(); }));
}

void ValidateVariantExtensionArray(const ExtensionArray& array,
                                   const std::shared_ptr<Buffer>& valid_rows) {
  const auto& storage = checked_cast<const ::arrow::StructArray&>(*array.storage());
  auto metadata_array = storage.GetFieldByName("metadata");
  auto value_array = storage.GetFieldByName("value");
  auto typed_array = storage.GetFieldByName("typed_value");

  std::string_view last_metadata_bytes;
  std::optional<VariantMetadataView> last_metadata;
  VisitVisibleRows(valid_rows, array, [&](int64_t row) {
    if (metadata_array->IsNull(row)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant extension storage: metadata is null");
    }

    auto metadata_value = BinaryFieldView(*metadata_array, row);
    if (!last_metadata.has_value() || last_metadata_bytes != metadata_value) {
      last_metadata = VariantMetadataView::Make(metadata_value);
      last_metadata_bytes = metadata_value;
    }

    if (typed_array == nullptr) {
      if (value_array->IsNull(row)) {
        throw ParquetInvalidOrCorruptedFileException(
            "Invalid Variant extension storage: unshredded value is null");
      }
      auto value = BinaryFieldView(*value_array, row);
      VariantValueView::Validate(value, *last_metadata);
    } else {
      ValidateShreddedVariantSlot(*last_metadata, value_array, typed_array, row,
                                  /*allow_missing=*/false, "variant");
    }
  });
}

void ValidateVariantPlan(const VariantValidationPlan& plan, MemoryPool* pool,
                         const std::shared_ptr<Buffer>& valid_rows) {
  switch (plan.array->type_id()) {
    case ::arrow::Type::EXTENSION: {
      ValidateVariantExtensionArray(checked_cast<const ExtensionArray&>(*plan.array),
                                    valid_rows);
      return;
    }
    case ::arrow::Type::STRUCT: {
      const auto& struct_array = checked_cast<const ::arrow::StructArray&>(*plan.array);
      std::shared_ptr<Buffer> child_valid_rows = valid_rows;
      if (struct_array.data()->MayHaveNulls()) {
        PARQUET_ASSIGN_OR_THROW(
            child_valid_rows,
            ::arrow::internal::OptionalBitmapAnd(
                pool, valid_rows, /*left_offset=*/0, struct_array.null_bitmap(),
                struct_array.offset(), struct_array.length(), /*out_offset=*/0));
      }
      for (const auto& child : plan.children) {
        ValidateVariantPlan(child, pool, child_valid_rows);
      }
      return;
    }
    case ::arrow::Type::LIST_VIEW:
    case ::arrow::Type::LARGE_LIST_VIEW:
    case ::arrow::Type::LIST:
    case ::arrow::Type::LARGE_LIST:
    case ::arrow::Type::MAP:
    case ::arrow::Type::FIXED_SIZE_LIST: {
      DCHECK_EQ(plan.children.size(), 1);
      auto values = ValuesArray(*plan.array);
      std::shared_ptr<Buffer> values_valid_rows;
      PARQUET_ASSIGN_OR_THROW(values_valid_rows,
                              ::arrow::AllocateEmptyBitmap(values->length(), pool));
      VisitVisibleRows(valid_rows, *plan.array, [&](int64_t row) {
        const auto [offset, length] = ValuesRangeAt(*plan.array, row);
        ::arrow::bit_util::SetBitsTo(values_valid_rows->mutable_data(), offset, length,
                                     true);
      });
      ValidateVariantPlan(plan.children[0], pool, values_valid_rows);
      return;
    }
    default: {
      for (const auto& child : plan.children) {
        ValidateVariantPlan(child, pool, valid_rows);
      }
      return;
    }
  }
}

}  // namespace

void ValidateVariants(const ::arrow::ChunkedArray& data, MemoryPool* pool) {
  for (const auto& chunk : data.chunks()) {
    auto plan = BuildVariantValidationPlan(chunk);
    if (plan.has_value()) {
      ValidateVariantPlan(*plan, pool, nullptr);
    }
  }
}

}  // namespace parquet::variant
