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

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "parquet/arrow/reader.h"
#include "parquet/variant/test_util_internal.h"

namespace parquet::variant {

using ::arrow::binary;
using ::arrow::field;
using ::arrow::struct_;
using internal::BinaryArrayFromValues;
using internal::Int32ArrayFromValues;
using internal::Int8Variant;
using internal::ReadVariantTestingTable;
using internal::ShreddedVariantTestingDir;
using internal::VariantTable;
using internal::WriteVariantTable;

TEST(TestVariantValidate, ListView) {
  ASSERT_OK_AND_ASSIGN(auto encoded, Int8Variant(42));

  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto value_array = BinaryArrayFromValues(
      {std::string_view{*encoded.value}, std::string_view("\xff", 1)});
  ASSERT_OK_AND_ASSIGN(
      auto storage,
      ::arrow::StructArray::Make({metadata_array, value_array}, storage_type->fields()));
  auto variant_array = ::arrow::ExtensionType::WrapArray(variant_type, storage);

  ASSERT_OK_AND_ASSIGN(auto valid_list,
                       ::arrow::ListViewArray::FromArrays(
                           *Int32ArrayFromValues({0}), *Int32ArrayFromValues({1}),
                           *variant_array, ::arrow::default_memory_pool()));
  ::arrow::ChunkedArray valid_data{valid_list};
  ASSERT_OK(ValidateVariants(valid_data, ::arrow::default_memory_pool()));

  ASSERT_OK_AND_ASSIGN(auto invalid_list,
                       ::arrow::ListViewArray::FromArrays(
                           *Int32ArrayFromValues({1}), *Int32ArrayFromValues({1}),
                           *variant_array, ::arrow::default_memory_pool()));
  ::arrow::ChunkedArray invalid_data{invalid_list};
  ASSERT_RAISES(Invalid, ValidateVariants(invalid_data, ::arrow::default_memory_pool()));
}

TEST(TestVariantValidate, ParquetTestingShredded) {
  auto maybe_dir = ShreddedVariantTestingDir();
  if (!maybe_dir.has_value()) {
    GTEST_SKIP() << "PARQUET_TEST_DATA not set";
  }
  if (!std::filesystem::exists(*maybe_dir)) {
    GTEST_SKIP() << *maybe_dir << " does not exist";
  }

  auto registered_storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                                          field("value", binary(), /*nullable=*/false)});
  ::arrow::ExtensionTypeGuard guard(::arrow::extension::variant(registered_storage_type));

  struct ShreddedCase {
    std::string file_name;
    bool has_top_level_value;
  };
  const std::vector<ShreddedCase> cases = {
      {.file_name = "case-041.parquet", .has_top_level_value = false},
      {.file_name = "case-088.parquet", .has_top_level_value = true},
      {.file_name = "case-131.parquet", .has_top_level_value = false},
      {.file_name = "case-132.parquet", .has_top_level_value = true},
      {.file_name = "case-138.parquet", .has_top_level_value = false},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.file_name);
    ASSERT_OK_AND_ASSIGN(auto table,
                         ReadVariantTestingTable(*maybe_dir + "/" + test_case.file_name));
    ASSERT_OK(table->ValidateFull());

    auto field = table->schema()->GetFieldByName("var");
    ASSERT_NE(nullptr, field);
    auto variant_type =
        std::dynamic_pointer_cast<::arrow::extension::VariantExtensionType>(
            field->type());
    ASSERT_NE(nullptr, variant_type);
    ASSERT_EQ(test_case.has_top_level_value, variant_type->value() != nullptr);

    auto column = table->GetColumnByName("var");
    ASSERT_NE(nullptr, column);
    ASSERT_OK(ValidateVariants(*column, ::arrow::default_memory_pool()));
  }
}

TEST(TestVariantValidate, DictionaryMetadata) {
  ASSERT_OK_AND_ASSIGN(auto encoded, Int8Variant(42));

  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto value_array = BinaryArrayFromValues(
      {std::string_view{*encoded.value}, std::string_view{*encoded.value}});
  auto table =
      VariantTable(variant_type, {metadata_array, value_array}, storage_type->fields());

  ASSERT_OK_AND_ASSIGN(
      auto buffer,
      WriteVariantTable(table, WriterProperties::Builder().enable_dictionary()->build()));

  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(buffer);
  ArrowReaderProperties reader_properties;
  reader_properties.set_arrow_extensions_enabled(true);
  ::arrow::ExtensionTypeGuard guard(::arrow::extension::variant(storage_type));
  parquet::arrow::FileReaderBuilder builder;
  ASSERT_OK(builder.Open(buffer_reader));
  builder.properties(reader_properties);
  ASSERT_OK_AND_ASSIGN(auto reader, builder.Build());

  ASSERT_OK_AND_ASSIGN(auto read_table, reader->ReadTable());
  auto column = read_table->GetColumnByName("variant");
  ASSERT_NE(nullptr, column);
  ASSERT_OK(ValidateVariants(*column, ::arrow::default_memory_pool()));
}

TEST(TestVariantValidate, ReadDictionaryOption) {
  ASSERT_OK_AND_ASSIGN(auto encoded, Int8Variant(42));

  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto value_array = BinaryArrayFromValues(
      {std::string_view{*encoded.value}, std::string_view{*encoded.value}});
  auto table =
      VariantTable(variant_type, {metadata_array, value_array}, storage_type->fields());

  ASSERT_OK_AND_ASSIGN(auto buffer, WriteVariantTable(table));

  auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(buffer);
  ArrowReaderProperties reader_properties;
  reader_properties.set_arrow_extensions_enabled(true);
  reader_properties.set_read_dictionary(0, true);
  reader_properties.set_read_dictionary(1, true);
  ::arrow::ExtensionTypeGuard guard(::arrow::extension::variant(storage_type));
  parquet::arrow::FileReaderBuilder builder;
  ASSERT_OK(builder.Open(buffer_reader));
  builder.properties(reader_properties);
  ASSERT_OK_AND_ASSIGN(auto reader, builder.Build());

  ASSERT_OK_AND_ASSIGN(auto read_table, reader->ReadTable());
  auto column = read_table->GetColumnByName("variant");
  ASSERT_NE(nullptr, column);
  ASSERT_OK(ValidateVariants(*column, ::arrow::default_memory_pool()));
}

}  // namespace parquet::variant
