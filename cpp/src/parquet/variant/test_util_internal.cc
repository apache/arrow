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

#include "parquet/variant/test_util_internal.h"

#include <optional>
#include <string>
#include <utility>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/array/builder_binary.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/unshred.h"

namespace parquet::variant::internal {

std::shared_ptr<::arrow::Table> VariantTable(
    const std::shared_ptr<DataType>& variant_type,
    const std::vector<std::shared_ptr<Array>>& storage_children,
    const FieldVector& storage_fields) {
  PARQUET_ASSIGN_OR_THROW(auto storage,
                          ::arrow::StructArray::Make(storage_children, storage_fields));
  auto array = ::arrow::ExtensionType::WrapArray(variant_type, storage);
  return ::arrow::Table::Make(::arrow::schema({::arrow::field("variant", variant_type)}),
                              {array});
}

Result<std::shared_ptr<Buffer>> WriteVariantTable(
    const std::shared_ptr<::arrow::Table>& table,
    std::shared_ptr<WriterProperties> writer_properties,
    std::shared_ptr<ArrowWriterProperties> arrow_properties) {
  ARROW_ASSIGN_OR_RAISE(auto sink, ::arrow::io::BufferOutputStream::Create(
                                       1024, ::arrow::default_memory_pool()));
  RETURN_NOT_OK(parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(), sink,
                                           /*chunk_size=*/table->num_rows(),
                                           std::move(writer_properties),
                                           std::move(arrow_properties)));
  return sink->Finish();
}

Status WriteVariantRecordBatch(const std::shared_ptr<::arrow::Table>& table,
                               std::shared_ptr<ArrowWriterProperties> arrow_properties) {
  ARROW_ASSIGN_OR_RAISE(auto sink, ::arrow::io::BufferOutputStream::Create(
                                       1024, ::arrow::default_memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        parquet::arrow::FileWriter::Open(
                            *table->schema(), ::arrow::default_memory_pool(), sink,
                            default_writer_properties(), std::move(arrow_properties)));
  ARROW_ASSIGN_OR_RAISE(auto batch, table->CombineChunksToBatch());
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  return writer->Close();
}

Result<std::shared_ptr<::arrow::extension::VariantArray>> RoundTripVariantArray(
    const std::shared_ptr<::arrow::extension::VariantArray>& array) {
  auto table = ::arrow::Table::Make(
      ::arrow::schema({::arrow::field("variant", array->type())}), {array});
  auto arrow_properties =
      ArrowWriterProperties::Builder().set_variant_validation_enabled(true)->build();
  ARROW_ASSIGN_OR_RAISE(auto buffer, WriteVariantTable(table, default_writer_properties(),
                                                       arrow_properties));

  std::optional<::arrow::ExtensionTypeGuard> guard;
  if (::arrow::GetExtensionType(std::string(::arrow::extension::kVariantExtensionName)) ==
      nullptr) {
    guard.emplace(array->type());
  }
  ArrowReaderProperties reader_properties;
  reader_properties.set_arrow_extensions_enabled(true);
  reader_properties.set_variant_validation_enabled(true);
  parquet::arrow::FileReaderBuilder builder;
  RETURN_NOT_OK(
      builder.Open(std::make_shared<::arrow::io::BufferReader>(std::move(buffer))));
  builder.properties(reader_properties);
  ARROW_ASSIGN_OR_RAISE(auto reader, builder.Build());
  ARROW_ASSIGN_OR_RAISE(auto read_table, reader->ReadTable());
  RETURN_NOT_OK(read_table->ValidateFull());

  auto column = read_table->GetColumnByName("variant");
  if (column == nullptr || column->num_chunks() != 1) {
    return Status::Invalid("Expected one Variant output chunk");
  }
  return ::arrow::internal::checked_pointer_cast<::arrow::extension::VariantArray>(
      column->chunk(0));
}

std::shared_ptr<Buffer> EmptyVariantMetadata() {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto encoded = builder.Finish();
  return std::move(encoded.metadata);
}

EncodedVariantValue Int8Variant(int8_t value) {
  VariantBuilder builder;
  builder.AppendInt8(value);
  return builder.Finish();
}

std::shared_ptr<Array> BinaryArrayFromValues(
    const std::vector<std::optional<std::string_view>>& values) {
  ::arrow::BinaryBuilder builder;
  for (const auto& value : values) {
    ARROW_EXPECT_OK(builder.AppendOrNull(value));
  }
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<::arrow::StructArray> MakeInt64FieldGroup(
    const std::vector<std::optional<std::string_view>>& values,
    std::string_view typed_values, const std::vector<bool>& is_valid) {
  auto field_group_type =
      ::arrow::struct_({::arrow::field("value", ::arrow::binary()),
                        ::arrow::field("typed_value", ::arrow::int64())});
  std::shared_ptr<::arrow::Buffer> null_bitmap;
  if (!is_valid.empty()) {
    ::arrow::BitmapFromVector(is_valid, &null_bitmap);
  }
  PARQUET_ASSIGN_OR_THROW(
      auto field_group,
      ::arrow::StructArray::Make(
          {BinaryArrayFromValues(values),
           ::arrow::ArrayFromJSON(::arrow::int64(), std::string(typed_values))},
          field_group_type->fields(), std::move(null_bitmap)));
  return field_group;
}

void AssertEncodedRow(const ::arrow::extension::VariantArray& array, int64_t row,
                      const EncodedVariantValue& expected) {
  ASSERT_EQ(std::string_view{*expected.metadata},
            BinaryFieldView(*array.metadata(), row));
  ASSERT_EQ(std::string_view{*expected.value}, BinaryFieldView(*array.value(), row));
}

void AssertUnshreddedValue(const ::arrow::extension::VariantArray& array, int64_t row,
                           const EncodedVariantValue& expected) {
  auto unshredded = UnshredVariantArray(array);
  AssertEncodedRow(*unshredded, row, expected);
}

}  // namespace parquet::variant::internal
