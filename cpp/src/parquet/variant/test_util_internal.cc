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

#include <cstdlib>
#include <utility>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/extension/uuid.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"

namespace parquet::variant::internal {

Result<VariantValueView> MakeVariantValueView(const EncodedVariantValue& encoded) {
  ARROW_ASSIGN_OR_RAISE(auto metadata,
                        VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  return VariantValueView::Make(std::string_view{*encoded.value}, metadata);
}

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

std::optional<std::string> ShreddedVariantTestingDir() {
  const char* data = std::getenv("PARQUET_TEST_DATA");
  if (data == nullptr) {
    return std::nullopt;
  }
  std::string path(data);
  const auto pos = path.find_last_of("/\\");
  if (pos == std::string::npos) {
    return std::nullopt;
  }
  return path.substr(0, pos) + "/shredded_variant";
}

Result<std::shared_ptr<::arrow::Table>> ReadVariantTestingTable(const std::string& path) {
  ArrowReaderProperties reader_properties;
  reader_properties.set_arrow_extensions_enabled(true);

  parquet::arrow::FileReaderBuilder builder;
  RETURN_NOT_OK(builder.OpenFile(path, /*memory_map=*/false));
  builder.properties(reader_properties);
  ARROW_ASSIGN_OR_RAISE(auto reader, builder.Build());
  return reader->ReadTable();
}

Result<std::shared_ptr<Buffer>> EmptyVariantMetadata() {
  VariantBuilder builder;
  ARROW_RETURN_NOT_OK(builder.AppendVariantNull());
  ARROW_ASSIGN_OR_RAISE(auto encoded, builder.Finish());
  return encoded.metadata;
}

Result<EncodedVariantValue> Int8Variant(int8_t value) {
  VariantBuilder builder;
  ARROW_RETURN_NOT_OK(builder.AppendInt8(value));
  return builder.Finish();
}

std::shared_ptr<Array> BinaryArrayFromValues(
    const std::vector<std::optional<std::string_view>>& values) {
  ::arrow::BinaryBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_EXPECT_OK(builder.Append(*value));
    } else {
      ARROW_EXPECT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<Array> BinaryViewArrayFromValues(
    const std::vector<std::optional<std::string_view>>& values) {
  ::arrow::BinaryViewBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_EXPECT_OK(builder.Append(*value));
    } else {
      ARROW_EXPECT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<Array> Int64ArrayFromValues(
    const std::vector<std::optional<int64_t>>& values) {
  ::arrow::Int64Builder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_EXPECT_OK(builder.Append(*value));
    } else {
      ARROW_EXPECT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<Array> Int32ArrayFromValues(const std::vector<int32_t>& values) {
  ::arrow::Int32Builder builder;
  ARROW_EXPECT_OK(builder.AppendValues(values));
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<Array> StringArrayFromValues(
    const std::vector<std::optional<std::string>>& values) {
  ::arrow::StringBuilder builder;
  for (const auto& value : values) {
    if (value.has_value()) {
      ARROW_EXPECT_OK(builder.Append(*value));
    } else {
      ARROW_EXPECT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<Array> out;
  ARROW_EXPECT_OK(builder.Finish(&out));
  return out;
}

std::shared_ptr<Array> UuidArrayFromValues(const std::vector<std::string_view>& values) {
  ::arrow::FixedSizeBinaryBuilder builder(::arrow::fixed_size_binary(16));
  for (const auto& value : values) {
    ARROW_EXPECT_OK(builder.Append(value));
  }
  std::shared_ptr<Array> storage;
  ARROW_EXPECT_OK(builder.Finish(&storage));
  return ::arrow::ExtensionType::WrapArray(::arrow::extension::uuid(), storage);
}

}  // namespace parquet::variant::internal
