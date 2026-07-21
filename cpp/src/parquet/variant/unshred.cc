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

#include "parquet/variant/unshred.h"

#include "arrow/buffer.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/slot_internal.h"

namespace parquet::variant {

using ::arrow::binary_view;
using ::arrow::field;
using ::arrow::struct_;
using ::arrow::StructArray;
using ::arrow::extension::VariantArray;
using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

namespace {

std::shared_ptr<::arrow::Buffer> ParentNullBitmap(const VariantArray& array,
                                                  ::arrow::MemoryPool* pool) {
  if (array.null_bitmap() == nullptr) {
    return nullptr;
  }
  if (array.offset() == 0) {
    return array.null_bitmap();
  }
  PARQUET_ASSIGN_OR_THROW(auto null_bitmap,
                          ::arrow::internal::CopyBitmap(pool, array.null_bitmap_data(),
                                                        array.offset(), array.length()));
  return null_bitmap;
}

void AppendUnshreddedRow(std::string_view metadata_value,
                         const internal::CompiledVariantRowPlan& row_plan, int64_t row,
                         VariantValueArrayBuilder& value_builder) {
  auto metadata = VariantMetadataView::Make(metadata_value);
  auto row_builder = value_builder.BindMetadata(metadata);
  internal::BuildTarget target{internal::BuildTarget::Row{row_builder}};
  internal::ProcessSlot<false>(metadata, row_plan, row, &target);
  row_builder.Finish();
}

}  // namespace

EncodedVariantValue UnshredVariantRow(const VariantArray& array, int64_t row,
                                      ::arrow::MemoryPool* pool) {
  if (row < 0 || row >= array.length()) {
    throw ParquetException("Variant row out of bounds: ", row);
  }
  if (array.IsNull(row)) {
    throw ParquetException("Cannot unshred null Variant row");
  }
  const auto& storage = checked_cast<const StructArray&>(*array.storage());
  auto metadata_array = storage.GetFieldByName("metadata");
  auto value_array = storage.GetFieldByName("value");
  auto typed_array = storage.GetFieldByName("typed_value");
  if (metadata_array->IsNull(row)) {
    throw ParquetInvalidOrCorruptedFileException(
        "Invalid Variant extension storage: metadata is null");
  }
  auto metadata_value = internal::BinaryFieldView(*metadata_array, row);

  if (typed_array == nullptr && value_array != nullptr) {
    if (value_array->IsNull(row)) {
      throw ParquetInvalidOrCorruptedFileException(
          "Invalid Variant extension storage: value is null");
    }
    return EncodedVariantValue{
        .metadata = ::arrow::Buffer::FromString(std::string(metadata_value)),
        .value = ::arrow::Buffer::FromString(
            std::string(internal::BinaryFieldView(*value_array, row)))};
  }

  auto row_plan = internal::CompileVariantRowPlan(value_array, typed_array);
  VariantValueArrayBuilder value_builder(pool);
  AppendUnshreddedRow(metadata_value, row_plan, row, value_builder);
  auto unshredded_value = value_builder.Finish();
  return EncodedVariantValue{
      .metadata = ::arrow::Buffer::FromString(std::string(metadata_value)),
      .value = ::arrow::Buffer::FromString(std::string(unshredded_value->GetView(0)))};
}

std::shared_ptr<::arrow::extension::VariantArray> UnshredVariantArray(
    const ::arrow::extension::VariantArray& array, ::arrow::MemoryPool* pool) {
  const auto& storage = checked_cast<const StructArray&>(*array.storage());
  auto metadata_array = storage.GetFieldByName("metadata");
  auto value_array = storage.GetFieldByName("value");
  auto typed_array = storage.GetFieldByName("typed_value");

  if (typed_array == nullptr && value_array != nullptr) {
    return MakeVariantArrayFromStorage(
        checked_pointer_cast<StructArray>(array.storage()));
  }

  auto row_plan = internal::CompileVariantRowPlan(value_array, typed_array);
  VariantValueArrayBuilder value_builder(pool);
  ::arrow::internal::VisitBitBlocksVoid(
      array.null_bitmap_data(), array.offset(), array.length(),
      [&](int64_t row) {
        if (metadata_array->IsNull(row)) {
          throw ParquetInvalidOrCorruptedFileException(
              "Invalid Variant extension storage: metadata is null");
        }
        auto metadata_value = internal::BinaryFieldView(*metadata_array, row);
        AppendUnshreddedRow(metadata_value, row_plan, row, value_builder);
      },
      [&] { value_builder.AppendNull(); });

  std::shared_ptr<::arrow::Array> unshredded_value = value_builder.Finish();
  auto storage_type =
      struct_({field("metadata", metadata_array->type(), /*nullable=*/false),
               field("value", binary_view(), /*nullable=*/false)});
  return MakeVariantArrayFromChildren(
      std::move(storage_type), {std::move(metadata_array), std::move(unshredded_value)},
      ParentNullBitmap(array, pool));
}

}  // namespace parquet::variant
