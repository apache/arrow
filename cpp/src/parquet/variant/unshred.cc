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

#include "arrow/extension/parquet_variant.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/slot_internal.h"

namespace parquet::variant {

using ::arrow::binary_view;
using ::arrow::field;
using ::arrow::struct_;
using ::arrow::extension::VariantArray;

namespace {

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

std::shared_ptr<::arrow::extension::VariantArray> UnshredVariantArray(
    const ::arrow::extension::VariantArray& array, ::arrow::MemoryPool* pool) {
  auto metadata_array = array.metadata();
  auto value_array = array.value();
  auto typed_array = array.typed_value();

  if (!array.is_shredded()) {
    return std::make_shared<VariantArray>(array.data());
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
      internal::NullBitmapForOutput(array, pool));
}

}  // namespace parquet::variant
