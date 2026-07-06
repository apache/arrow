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

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension/uuid.h"
#include "arrow/io/memory.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/test_util_internal.h"

#include <string>
#include <string_view>
#include <vector>

namespace parquet::arrow {

using ::arrow::binary;
using ::arrow::field;
using ::arrow::struct_;
using variant::internal::BinaryArrayFromValues;
using variant::internal::BinaryViewArrayFromValues;
using variant::internal::EmptyVariantMetadata;
using variant::internal::Int32ArrayFromValues;
using variant::internal::Int64ArrayFromValues;
using variant::internal::Int8Variant;
using variant::internal::StringArrayFromValues;
using variant::internal::UuidArrayFromValues;
using variant::internal::VariantTable;
using variant::internal::WriteVariantRecordBatch;
using variant::internal::WriteVariantTable;

TEST(TestVariantExtensionType, WriterValidatesUnshreddedVariantBytes) {
  auto encoded = Int8Variant(42);

  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryArrayFromValues({std::string_view{*encoded.metadata}});
  auto value_array = BinaryArrayFromValues({std::string_view{*encoded.value}});
  auto table =
      VariantTable(variant_type, {metadata_array, value_array}, storage_type->fields());
  ASSERT_OK(WriteVariantTable(table));

  auto invalid_value = BinaryArrayFromValues({std::string_view("\xff", 1)});
  auto invalid_table =
      VariantTable(variant_type, {metadata_array, invalid_value}, storage_type->fields());
  ASSERT_RAISES(Invalid, WriteVariantTable(invalid_table));

  auto no_validation =
      ArrowWriterProperties::Builder().set_variant_validation_enabled(false)->build();
  ASSERT_OK(WriteVariantTable(invalid_table, default_writer_properties(), no_validation));
}

TEST(TestVariantExtensionType, WriteRecordBatchValidatesVariantBytes) {
  auto metadata = EmptyVariantMetadata();
  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);

  auto metadata_array = BinaryArrayFromValues({std::string_view{*metadata}});
  auto invalid_value = BinaryArrayFromValues({std::string_view("\xff", 1)});
  auto table =
      VariantTable(variant_type, {metadata_array, invalid_value}, storage_type->fields());

  ASSERT_RAISES(Invalid, WriteVariantRecordBatch(table));

  auto no_validation =
      ArrowWriterProperties::Builder().set_variant_validation_enabled(false)->build();
  ASSERT_OK(WriteVariantRecordBatch(table, no_validation));
}

TEST(TestVariantExtensionType, WriteRecordBatchValidatesBatch) {
  auto encoded = Int8Variant(42);

  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = StringArrayFromValues({"not binary"});
  auto value_array = BinaryArrayFromValues({std::string_view{*encoded.value}});
  ASSERT_OK_AND_ASSIGN(
      auto storage,
      ::arrow::StructArray::Make({metadata_array, value_array}, storage_type->fields()));
  auto variant_array = ::arrow::ExtensionType::WrapArray(variant_type, storage);
  auto schema = ::arrow::schema({field("variant", variant_type)});
  auto batch = ::arrow::RecordBatch::Make(schema, 1, {variant_array});

  ASSERT_OK_AND_ASSIGN(auto sink, ::arrow::io::BufferOutputStream::Create(
                                      1024, ::arrow::default_memory_pool()));
  ASSERT_OK_AND_ASSIGN(
      auto writer,
      FileWriter::Open(*schema, ::arrow::default_memory_pool(), sink,
                       default_writer_properties(), default_arrow_writer_properties()));

  const auto status = writer->WriteRecordBatch(*batch);
  ASSERT_TRUE(status.IsInvalid()) << status;
  ASSERT_NE(std::string::npos, status.ToStringWithoutContextLines().find(
                                   "Struct child array #0 does not match type field"))
      << status;
}

TEST(TestVariantExtensionType, WriterValidatesBinaryViewVariantBytes) {
  auto encoded = Int8Variant(42);

  auto storage_type =
      struct_({field("metadata", ::arrow::binary_view(), /*nullable=*/false),
               field("value", ::arrow::binary_view(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryViewArrayFromValues({std::string_view{*encoded.metadata}});
  auto value_array = BinaryViewArrayFromValues({std::string_view{*encoded.value}});
  auto table =
      VariantTable(variant_type, {metadata_array, value_array}, storage_type->fields());
  ASSERT_OK(WriteVariantTable(table));
}

TEST(TestVariantExtensionType, WriterSkipsNullParents) {
  auto metadata = EmptyVariantMetadata();
  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("value", binary(), /*nullable=*/false)});
  auto variant_type = ::arrow::extension::variant(storage_type);
  auto metadata_array = BinaryArrayFromValues({std::string_view{*metadata}});
  auto invalid_value = BinaryArrayFromValues({std::string_view("\xff", 1)});
  PARQUET_ASSIGN_OR_THROW(auto storage,
                          ::arrow::StructArray::Make({metadata_array, invalid_value},
                                                     storage_type->fields()));
  auto variant_array = ::arrow::ExtensionType::WrapArray(variant_type, storage);

  auto parent_type = struct_({field("child", variant_type)});
  PARQUET_ASSIGN_OR_THROW(
      auto parent_array,
      ::arrow::StructArray::Make({variant_array}, parent_type->fields(),
                                 ::arrow::Buffer::FromString(std::string("\0", 1))));
  auto parent_table = ::arrow::Table::Make(
      ::arrow::schema({field("parent", parent_type)}), {parent_array});
  ASSERT_OK(WriteVariantTable(parent_table));

  auto map_type = ::arrow::map(::arrow::utf8(), field("item", variant_type));
  ASSERT_OK_AND_ASSIGN(
      auto map_array,
      ::arrow::MapArray::FromArrays(map_type, Int32ArrayFromValues({0, 1}),
                                    StringArrayFromValues({"hidden"}), variant_array,
                                    ::arrow::default_memory_pool(),
                                    ::arrow::Buffer::FromString(std::string("\0", 1))));
  auto map_table = ::arrow::Table::Make(::arrow::schema({field("variant_map", map_type)}),
                                        {map_array});
  ASSERT_OK(WriteVariantTable(map_table));
}

TEST(TestVariantExtensionType, WriterValidatesShreddedPrimitiveConflicts) {
  auto encoded = Int8Variant(42);

  auto storage_type =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value", ::arrow::int64())});
  auto variant_type = ::arrow::extension::variant(storage_type);

  auto metadata_array = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto value_array =
      BinaryArrayFromValues({std::nullopt, std::string_view{*encoded.value}});
  auto typed_array = Int64ArrayFromValues({34, 100});
  auto table = VariantTable(variant_type, {metadata_array, value_array, typed_array},
                            storage_type->fields());
  ASSERT_RAISES(Invalid, WriteVariantTable(table));

  auto empty_value = BinaryArrayFromValues({std::string_view{}, std::nullopt});
  auto empty_table = VariantTable(
      variant_type, {metadata_array, empty_value, typed_array}, storage_type->fields());
  ASSERT_RAISES(Invalid, WriteVariantTable(empty_table));

  auto valid_values =
      BinaryArrayFromValues({std::nullopt, std::string_view{*encoded.value}});
  auto valid_typed = Int64ArrayFromValues({34, std::nullopt});
  auto valid_table = VariantTable(
      variant_type, {metadata_array, valid_values, valid_typed}, storage_type->fields());
  ASSERT_OK(WriteVariantTable(valid_table));
}

TEST(TestVariantExtensionType, WriterRejectsShreddedWithoutValue) {
  auto metadata = EmptyVariantMetadata();
  auto storage_type = struct_({field("metadata", binary(), /*nullable=*/false),
                               field("typed_value", ::arrow::int64())});
  ASSERT_OK_AND_ASSIGN(auto variant_type,
                       ::arrow::extension::VariantExtensionType::Make(storage_type));

  auto metadata_array = BinaryArrayFromValues({std::string_view{*metadata}});
  auto typed_array = Int64ArrayFromValues({34});
  auto table =
      VariantTable(variant_type, {metadata_array, typed_array}, storage_type->fields());
  ASSERT_RAISES(Invalid, WriteVariantTable(table));
}

TEST(TestVariantExtensionType, ReadsDictionaryEncodedMetadata) {
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
  FileReaderBuilder builder;
  ASSERT_OK(builder.Open(buffer_reader));
  builder.properties(reader_properties);
  ASSERT_OK_AND_ASSIGN(auto reader, builder.Build());

  ASSERT_TRUE(reader->parquet_reader()
                  ->metadata()
                  ->RowGroup(0)
                  ->ColumnChunk(0)
                  ->has_dictionary_page());

  ASSERT_OK_AND_ASSIGN(auto read_table, reader->ReadTable());
  ASSERT_OK(read_table->ValidateFull());

  auto field = read_table->schema()->GetFieldByName("variant");
  ASSERT_NE(nullptr, field);
  auto read_variant_type =
      std::dynamic_pointer_cast<::arrow::extension::VariantExtensionType>(field->type());
  ASSERT_NE(nullptr, read_variant_type);
  ASSERT_EQ(::arrow::Type::BINARY, read_variant_type->metadata()->type()->id());

  ASSERT_NE(nullptr, read_table->GetColumnByName(field->name()));
}

TEST(TestVariantExtensionType, ReadsWithDictionaryOption) {
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
  FileReaderBuilder builder;
  ASSERT_OK(builder.Open(buffer_reader));
  builder.properties(reader_properties);
  ASSERT_OK_AND_ASSIGN(auto reader, builder.Build());

  ASSERT_OK_AND_ASSIGN(auto read_table, reader->ReadTable());
  ASSERT_OK(read_table->ValidateFull());

  auto field = read_table->schema()->GetFieldByName("variant");
  ASSERT_NE(nullptr, field);
  auto read_variant_type =
      std::dynamic_pointer_cast<::arrow::extension::VariantExtensionType>(field->type());
  ASSERT_NE(nullptr, read_variant_type);
  ASSERT_EQ(::arrow::Type::BINARY, read_variant_type->metadata()->type()->id());
  ASSERT_EQ(::arrow::Type::BINARY, read_variant_type->value()->type()->id());

  ASSERT_NE(nullptr, read_table->GetColumnByName(field->name()));
}

TEST(TestVariantExtensionType, WriterWritesUuid) {
  auto metadata = EmptyVariantMetadata();
  auto storage_type =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value", ::arrow::extension::uuid())});
  auto variant_type = ::arrow::extension::variant(storage_type);

  auto metadata_array = BinaryArrayFromValues({std::string_view{*metadata}});
  auto value_array = BinaryArrayFromValues({std::nullopt});
  auto typed_array = UuidArrayFromValues({std::string_view("0123456789abcdef", 16)});
  auto table = VariantTable(variant_type, {metadata_array, value_array, typed_array},
                            storage_type->fields());
  ASSERT_OK(WriteVariantTable(table));
}

TEST(TestVariantExtensionType, WriterValidatesShreddedObjectConflicts) {
  variant::VariantBuilder object_builder;
  auto object = object_builder.StartObject();
  object.AppendShortString("event_type", "login");
  object.Finish();
  auto encoded = object_builder.Finish();

  auto field_group_type =
      struct_({field("value", binary()), field("typed_value", ::arrow::utf8())});
  auto typed_value_type =
      struct_({field("event_type", field_group_type, /*nullable=*/false)});
  auto storage_type =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value", typed_value_type)});
  auto variant_type = ::arrow::extension::variant(storage_type);

  auto metadata_array = BinaryArrayFromValues({std::string_view{*encoded.metadata}});
  auto value_array = BinaryArrayFromValues({std::string_view{*encoded.value}});
  ASSERT_OK_AND_ASSIGN(auto event_type_group,
                       ::arrow::StructArray::Make({BinaryArrayFromValues({std::nullopt}),
                                                   StringArrayFromValues({"login"})},
                                                  field_group_type->fields()));
  ASSERT_OK_AND_ASSIGN(
      auto typed_array,
      ::arrow::StructArray::Make({event_type_group}, typed_value_type->fields()));

  auto table = VariantTable(variant_type, {metadata_array, value_array, typed_array},
                            storage_type->fields());
  ASSERT_RAISES(Invalid, WriteVariantTable(table));

  auto valid_value_array = BinaryArrayFromValues({std::nullopt});
  auto valid_table =
      VariantTable(variant_type, {metadata_array, valid_value_array, typed_array},
                   storage_type->fields());
  ASSERT_OK(WriteVariantTable(valid_table));

  ASSERT_OK_AND_ASSIGN(auto missing_event_type_group,
                       ::arrow::StructArray::Make({BinaryArrayFromValues({std::nullopt}),
                                                   StringArrayFromValues({std::nullopt})},
                                                  field_group_type->fields()));
  ASSERT_OK_AND_ASSIGN(
      auto missing_typed_array,
      ::arrow::StructArray::Make({missing_event_type_group}, typed_value_type->fields()));
  auto missing_table =
      VariantTable(variant_type, {metadata_array, valid_value_array, missing_typed_array},
                   storage_type->fields());
  ASSERT_OK(WriteVariantTable(missing_table));
}

}  // namespace parquet::arrow
