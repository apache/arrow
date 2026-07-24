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
#include <string_view>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/slot_internal.h"

namespace parquet::variant {

namespace {

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::ChunkedArray;
using ::arrow::ExtensionArray;
using ::arrow::ExtensionType;
using ::arrow::MemoryPool;
using ::arrow::StructArray;
using ::arrow::extension::kVariantExtensionName;
using ::arrow::extension::VariantArray;
using ::arrow::internal::checked_cast;

struct VariantValidationPlan {
  std::shared_ptr<Array> array;
  std::vector<VariantValidationPlan> children;
};

template <bool strict>
void ValidateVariantArrayRows(const VariantArray& array,
                              const std::shared_ptr<Buffer>& valid_rows) {
  auto metadata_array = array.metadata();
  auto value_array = array.value();
  auto typed_array = array.typed_value();

  if constexpr (strict) {
    if (value_array == nullptr) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant extension storage: missing top-level value field");
    }
  }

  auto row_plan = internal::CompileVariantRowPlan(value_array, typed_array);
  std::string_view last_metadata_bytes;
  std::optional<VariantMetadataView> last_metadata;
  internal::VisitVisibleRows(valid_rows, array, [&](int64_t row) {
    if (metadata_array->IsNull(row)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant extension storage: metadata is null");
    }
    auto metadata_value = internal::BinaryFieldView(*metadata_array, row);
    if (!last_metadata.has_value() || last_metadata_bytes != metadata_value) {
      last_metadata = VariantMetadataView::Make(metadata_value);
      last_metadata_bytes = metadata_value;
    }
    internal::ProcessSlot<strict>(*last_metadata, row_plan, row);
  });
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
      const auto& struct_array = checked_cast<const StructArray&>(*array);
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
      auto values = internal::ValuesArray(*array);
      auto child_plan = BuildVariantValidationPlan(std::move(values));
      if (!child_plan.has_value()) {
        return std::nullopt;
      }
      return VariantValidationPlan{.array = std::move(array),
                                   .children = {std::move(*child_plan)}};
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

template <bool strict>
void ValidateVariantPlan(const VariantValidationPlan& plan, MemoryPool* pool,
                         const std::shared_ptr<Buffer>& valid_rows) {
  switch (plan.array->type_id()) {
    case ::arrow::Type::EXTENSION:
      ValidateVariantArrayRows<strict>(checked_cast<const VariantArray&>(*plan.array),
                                       valid_rows);
      return;
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
        ValidateVariantPlan<strict>(child, pool, child_valid_rows);
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
      auto values = internal::ValuesArray(*plan.array);
      PARQUET_ASSIGN_OR_THROW(auto values_valid_rows,
                              ::arrow::AllocateEmptyBitmap(values->length(), pool));
      internal::VisitVisibleRows(valid_rows, *plan.array, [&](int64_t row) {
        const auto [offset, length] = internal::ValuesRangeAt(*plan.array, row);
        ::arrow::bit_util::SetBitsTo(values_valid_rows->mutable_data(), offset, length,
                                     true);
      });
      ValidateVariantPlan<strict>(plan.children[0], pool, values_valid_rows);
      return;
    }
    default:
      for (const auto& child : plan.children) {
        ValidateVariantPlan<strict>(child, pool, valid_rows);
      }
      return;
  }
}

}  // namespace

template <bool strict>
void ValidateVariants(const ::arrow::ChunkedArray& data, MemoryPool* pool) {
  for (const auto& chunk : data.chunks()) {
    auto plan = BuildVariantValidationPlan(chunk);
    if (plan.has_value()) {
      ValidateVariantPlan<strict>(*plan, pool, nullptr);
    }
  }
}

template PARQUET_TEMPLATE_EXPORT void ValidateVariants<true>(
    const ::arrow::ChunkedArray& data, MemoryPool* pool);
template PARQUET_TEMPLATE_EXPORT void ValidateVariants<false>(
    const ::arrow::ChunkedArray& data, MemoryPool* pool);

}  // namespace parquet::variant
