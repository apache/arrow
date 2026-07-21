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

#include <utility>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/array/builder_binary.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

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

::arrow::Status WriteVariantRecordBatch(
    const std::shared_ptr<::arrow::Table>& table,
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

}  // namespace parquet::variant::internal
