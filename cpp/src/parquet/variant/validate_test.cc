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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/test_common.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "parquet/arrow/reader.h"
#include "parquet/exception.h"
#include "parquet/variant/array_internal.h"
#include "parquet/variant/shred.h"
#include "parquet/variant/test_util_internal.h"
#include "parquet/variant/unshred.h"

namespace parquet::variant {

using ::arrow::binary;
using ::arrow::field;
using ::arrow::struct_;
using internal::BinaryArrayFromValues;
using internal::Int8Variant;
using internal::VariantTable;
using internal::WriteVariantTable;

TEST(TestVariantValidate, DictionaryMetadataFieldView) {
  for (const auto& value_type :
       {binary(), ::arrow::large_binary(), ::arrow::binary_view()}) {
    auto values = ::arrow::ArrayFromJSON(value_type, R"(["padding", "a", null, "b", ""])")
                      ->Slice(1);
    SCOPED_TRACE(value_type->ToString());
    auto indices =
        ::arrow::ArrayFromJSON(::arrow::int32(), "[0, 2, 0, null, 1, 2, 3, 0]")->Slice(1);
    ASSERT_OK_AND_ASSIGN(auto dictionary,
                         ::arrow::DictionaryArray::FromArrays(indices, values));
    auto sliced = dictionary->Slice(1, 5);
    ASSERT_OK(sliced->ValidateFull());
    const std::vector<std::optional<std::string_view>> expected{"a", std::nullopt,
                                                                std::nullopt, "b", ""};
    for (int64_t row = 0; row < sliced->length(); ++row) {
      if (expected[row].has_value()) {
        ASSERT_EQ(*expected[row], internal::MetadataFieldView(*sliced, row));
      } else {
        ASSERT_THROW(internal::MetadataFieldView(*sliced, row),
                     ParquetInvalidOrCorruptedFileException);
      }
    }
    const auto first = internal::MetadataFieldView(*sliced, 0);
    ASSERT_EQ(first.data(), internal::BinaryFieldView(*values, 0).data());
  }
}

TEST(TestVariantValidate, RunEndEncodedMetadataFieldView) {
  auto values =
      ::arrow::ArrayFromJSON(binary(), R"(["padding", "a", null, "b", ""])")->Slice(1);
  auto ends = ::arrow::ArrayFromJSON(::arrow::int32(), "[1, 3, 5, 6, 8]")->Slice(1);
  ASSERT_OK_AND_ASSIGN(auto ree, ::arrow::RunEndEncodedArray::Make(8, ends, values));
  auto sliced = ree->Slice(1, 6);
  ASSERT_OK(sliced->ValidateFull());
  const std::vector<std::optional<std::string_view>> expected{
      "a", "a", std::nullopt, std::nullopt, "b", ""};
  for (int64_t row = 0; row < sliced->length(); ++row) {
    if (expected[row].has_value()) {
      ASSERT_EQ(*expected[row], internal::MetadataFieldView(*sliced, row));
    } else {
      ASSERT_THROW(internal::MetadataFieldView(*sliced, row),
                   ParquetInvalidOrCorruptedFileException);
    }
  }
  const auto first = internal::MetadataFieldView(*sliced, 0);
  ASSERT_EQ(first.data(), internal::BinaryFieldView(*values, 0).data());
}

TEST(TestVariantValidate, EncodedMetadata) {
  VariantArrayBuilder builder;
  for (int64_t row = 0; row < 5; ++row) {
    if (row == 2) {
      builder.AppendNull();
    } else {
      auto object = builder.StartObject();
      // Both dictionaries contain a and b, but assign their field IDs differently.
      object.AddInt64(row < 2 ? "a" : "b", row);
      object.AddInt64(row < 2 ? "b" : "a", row + 10);
      std::move(object).Finish();
    }
  }
  auto baseline = builder.Finish();
  const auto target =
      struct_({field("a", ::arrow::int64()), field("b", ::arrow::int64())});
  auto expected = UnshredVariantArray(*ShredVariantArray(*baseline, target));
  const auto metadata_a = internal::BinaryFieldView(*baseline->metadata(), 0);
  const auto metadata_b = internal::BinaryFieldView(*baseline->metadata(), 3);
  ASSERT_OK_AND_ASSIGN(
      auto dictionary,
      ::arrow::DictionaryArray::FromArrays(
          ::arrow::ArrayFromJSON(::arrow::int8(), "[0, 0, null, 1, 1]"),
          BinaryArrayFromValues({metadata_a, metadata_b, std::nullopt})));
  ASSERT_OK_AND_ASSIGN(
      auto ree, ::arrow::RunEndEncodedArray::Make(
                    5, ::arrow::ArrayFromJSON(::arrow::int16(), "[2, 3, 5]"),
                    BinaryArrayFromValues({metadata_a, std::nullopt, metadata_b})));
  for (const auto& metadata : ::arrow::ArrayVector{dictionary, ree}) {
    SCOPED_TRACE(metadata->type()->ToString());
    auto storage_type = struct_({field("metadata", metadata->type(), false),
                                 field("value", baseline->value()->type(), false)});
    auto input = MakeVariantArrayFromChildren(storage_type, {metadata, baseline->value()},
                                              baseline->null_bitmap());
    auto sliced =
        ::arrow::internal::checked_pointer_cast<::arrow::extension::VariantArray>(
            input->Slice(1, 3));
    ASSERT_OK(sliced->ValidateFull());
    ValidateVariants<true>(::arrow::ChunkedArray{sliced});

    auto shredded = ShredVariantArray(*sliced, target);
    ASSERT_TRUE(shredded->metadata()->Equals(sliced->metadata()));
    ValidateVariants<true>(::arrow::ChunkedArray{shredded});
    auto unshredded = UnshredVariantArray(*shredded);
    ASSERT_TRUE(unshredded->metadata()->Equals(sliced->metadata()));
    ValidateVariants<true>(::arrow::ChunkedArray{unshredded});
    for (int64_t row = 0; row < sliced->length(); ++row) {
      ASSERT_EQ(sliced->IsNull(row), unshredded->IsNull(row));
      if (!sliced->IsNull(row)) {
        ASSERT_EQ(internal::BinaryFieldView(*baseline->metadata(), row + 1),
                  internal::MetadataFieldView(*unshredded->metadata(), row));
        ASSERT_EQ(internal::BinaryFieldView(*expected->value(), row + 1),
                  internal::BinaryFieldView(*unshredded->value(), row));
      }
    }

    ::arrow::ExtensionTypeGuard guard(sliced->type());
    auto batch = ::arrow::RecordBatch::Make(
        ::arrow::schema({field("variant", sliced->type())}), sliced->length(), {sliced});
    std::shared_ptr<::arrow::RecordBatch> restored;
    ASSERT_OK(::arrow::ipc::test::RoundtripBatch(batch, &restored));
    ASSERT_NE(nullptr, restored);
    ASSERT_OK(restored->ValidateFull());
    ASSERT_TRUE(restored->Equals(*batch));
    ValidateVariants<true>(::arrow::ChunkedArray{restored->column(0)});
  }
}

TEST(TestVariantValidate, EncodedMetadataNulls) {
  auto encoded = Int8Variant(42);
  auto dictionary_values =
      BinaryArrayFromValues({std::string_view{*encoded.metadata}, std::nullopt, ""});
  ASSERT_OK_AND_ASSIGN(
      auto null_index,
      ::arrow::DictionaryArray::FromArrays(
          ::arrow::ArrayFromJSON(::arrow::int8(), "[null]"), dictionary_values));
  ASSERT_OK_AND_ASSIGN(
      auto null_value,
      ::arrow::DictionaryArray::FromArrays(
          ::arrow::ArrayFromJSON(::arrow::int32(), "[1]"), dictionary_values));
  ASSERT_OK_AND_ASSIGN(
      auto empty_value,
      ::arrow::DictionaryArray::FromArrays(
          ::arrow::ArrayFromJSON(::arrow::int32(), "[2]"), dictionary_values));
  ASSERT_OK_AND_ASSIGN(auto null_run,
                       ::arrow::RunEndEncodedArray::Make(
                           1, ::arrow::ArrayFromJSON(::arrow::int64(), "[1]"),
                           BinaryArrayFromValues({std::nullopt})));
  auto values = BinaryArrayFromValues({std::string_view{*encoded.value}});
  std::shared_ptr<::arrow::Buffer> hidden;
  ASSERT_OK(::arrow::GetBitmapFromVector(std::vector<bool>{false}, &hidden));
  for (const auto& metadata :
       ::arrow::ArrayVector{null_index, null_value, empty_value, null_run}) {
    auto storage_type = struct_(
        {field("metadata", metadata->type(), false), field("value", binary(), false)});
    auto input = MakeVariantArrayFromChildren(storage_type, {metadata, values});
    ASSERT_THROW(ValidateVariants<false>(::arrow::ChunkedArray{input}),
                 ParquetInvalidOrCorruptedFileException);
    ASSERT_THROW(ShredVariantArray(*input, ::arrow::int64()),
                 ParquetInvalidOrCorruptedFileException);

    auto masked = MakeVariantArrayFromChildren(storage_type, {metadata, values}, hidden);
    ValidateVariants<true>(::arrow::ChunkedArray{masked});
    ASSERT_OK_AND_ASSIGN(
        auto parent,
        ::arrow::StructArray::Make({input}, {field("item", input->type())}, hidden));
    ValidateVariants<true>(::arrow::ChunkedArray{parent});

    auto shredded = MakeVariantArrayFromChildren(
        struct_({field("metadata", metadata->type(), false), field("value", binary()),
                 field("typed_value", ::arrow::int64())}),
        {metadata, BinaryArrayFromValues({std::nullopt}),
         ::arrow::ArrayFromJSON(::arrow::int64(), "[42]")});
    ASSERT_THROW(UnshredVariantArray(*shredded), ParquetInvalidOrCorruptedFileException);
  }
  ASSERT_OK_AND_ASSIGN(
      auto unreferenced_null,
      ::arrow::DictionaryArray::FromArrays(::arrow::ArrayFromJSON(::arrow::int8(), "[0]"),
                                           dictionary_values));
  auto valid = MakeVariantArrayFromChildren(
      struct_({field("metadata", unreferenced_null->type(), false),
               field("value", binary(), false)}),
      {unreferenced_null, values});
  ValidateVariants<true>(::arrow::ChunkedArray{valid});
}

TEST(TestVariantValidate, ListView) {
  auto encoded = Int8Variant(42);

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

  ASSERT_OK_AND_ASSIGN(
      auto valid_list,
      ::arrow::ListViewArray::FromArrays(*::arrow::ArrayFromJSON(::arrow::int32(), "[0]"),
                                         *::arrow::ArrayFromJSON(::arrow::int32(), "[1]"),
                                         *variant_array));
  ::arrow::ChunkedArray valid_data{valid_list};
  ValidateVariants<true>(valid_data);

  ASSERT_OK_AND_ASSIGN(
      auto invalid_list,
      ::arrow::ListViewArray::FromArrays(*::arrow::ArrayFromJSON(::arrow::int32(), "[1]"),
                                         *::arrow::ArrayFromJSON(::arrow::int32(), "[1]"),
                                         *variant_array));
  ::arrow::ChunkedArray invalid_data{invalid_list};
  ASSERT_THROW(ValidateVariants<true>(invalid_data),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantValidate, DictionaryMetadata) {
  auto encoded = Int8Variant(42);

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
  ValidateVariants<true>(*column);
}

TEST(TestVariantValidate, ReadDictionaryOption) {
  auto encoded = Int8Variant(42);

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
  ValidateVariants<true>(*column);
}

}  // namespace parquet::variant
