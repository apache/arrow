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

#include <concepts>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"  // IWYU pragma: keep

#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/decimal.h"
#include "parquet/exception.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/test_util_internal.h"
#include "parquet/variant/validate.h"

namespace parquet::variant {

using ::arrow::binary;
using ::arrow::field;
using ::arrow::int64;
using ::arrow::struct_;
using ::arrow::internal::checked_pointer_cast;
using internal::BinaryArrayFromValues;

namespace {

void AssertUnshreddedRow(const ::arrow::extension::VariantArray& array,
                         const EncodedVariantValue& expected) {
  auto actual = UnshredVariantRow(array, /*row=*/0);
  ASSERT_EQ(std::string_view{*expected.metadata}, std::string_view{*actual.metadata});
  ASSERT_EQ(std::string_view{*expected.value}, std::string_view{*actual.value});
}

EncodedVariantValue ObjectWithInt64(std::string_view field_name, int64_t value) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt64(field_name, value);
  object.Finish();
  return builder.Finish();
}

EncodedVariantValue EmptyObjectWithField(std::string_view field_name) {
  VariantBuilder builder;
  builder.AddFieldName(field_name);
  auto object = builder.StartObject();
  object.Finish();
  return builder.Finish();
}

std::shared_ptr<::arrow::StructArray> MakeInt64FieldGroup(
    const std::vector<std::optional<std::string_view>>& values,
    std::string_view typed_values, const std::vector<bool>& is_valid = {}) {
  auto field_group_type =
      struct_({field("value", binary()), field("typed_value", int64())});
  std::shared_ptr<::arrow::Buffer> null_bitmap;
  if (!is_valid.empty()) {
    ::arrow::BitmapFromVector(is_valid, &null_bitmap);
  }
  PARQUET_ASSIGN_OR_THROW(
      auto field_group, ::arrow::StructArray::Make(
                            {BinaryArrayFromValues(values),
                             ::arrow::ArrayFromJSON(int64(), std::string(typed_values))},
                            field_group_type->fields(), std::move(null_bitmap)));
  return field_group;
}

std::shared_ptr<::arrow::extension::VariantArray> MakeSingleRowVariantArray(
    std::string_view metadata, std::optional<std::string_view> value,
    const std::shared_ptr<::arrow::Array>& typed_value) {
  auto storage_type =
      struct_({field("metadata", binary(), /*nullable=*/false), field("value", binary()),
               field("typed_value", typed_value->type())});
  return MakeVariantArrayFromChildren(
      storage_type,
      {BinaryArrayFromValues({metadata}), BinaryArrayFromValues({value}), typed_value});
}

}  // namespace

TEST(TestVariantUnshred, FastPathKeepsStorage) {
  VariantArrayBuilder builder;
  builder.AppendNull();
  builder.AppendInt32(42);
  auto array = builder.Finish();

  auto unshredded = UnshredVariantArray(*array);
  const auto& input_storage =
      checked_pointer_cast<::arrow::StructArray>(array->storage());
  const auto& output_storage =
      checked_pointer_cast<::arrow::StructArray>(unshredded->storage());
  ASSERT_EQ(nullptr, output_storage->GetFieldByName("typed_value").get());

  const auto& input_metadata = checked_pointer_cast<::arrow::BinaryViewArray>(
      input_storage->GetFieldByName("metadata"));
  const auto& input_value = checked_pointer_cast<::arrow::BinaryViewArray>(
      input_storage->GetFieldByName("value"));
  const auto& output_metadata = checked_pointer_cast<::arrow::BinaryViewArray>(
      output_storage->GetFieldByName("metadata"));
  const auto& output_value = checked_pointer_cast<::arrow::BinaryViewArray>(
      output_storage->GetFieldByName("value"));

  ASSERT_EQ(input_metadata->data().get(), output_metadata->data().get());
  ASSERT_EQ(input_value->data().get(), output_value->data().get());
}

TEST(TestVariantUnshred, UnshredsValuesWithoutCopyingMetadata) {
  VariantBuilder typed_only_row_builder;
  typed_only_row_builder.AppendInt64(1);
  auto typed_only_row = typed_only_row_builder.Finish();
  VariantBuilder residual_value_row_builder;
  residual_value_row_builder.AppendInt8(7);
  auto residual_value_row = residual_value_row_builder.Finish();

  const auto metadata_value = std::string_view{*typed_only_row.metadata};
  ASSERT_EQ(metadata_value, std::string_view{*residual_value_row.metadata});

  auto input_metadata = BinaryArrayFromValues({metadata_value, metadata_value});
  auto input_values =
      BinaryArrayFromValues({std::nullopt, std::string_view{*residual_value_row.value}});
  auto input_typed_values = ArrayFromJSON(int64(), "[1, null]");
  auto input_storage_type =
      struct_({field("metadata", binary(), false), field("value", binary()),
               field("typed_value", int64())});

  auto input_array = MakeVariantArrayFromChildren(
      input_storage_type, {input_metadata, input_values, input_typed_values});
  auto unshredded = UnshredVariantArray(*input_array);

  const auto& output_storage =
      checked_pointer_cast<::arrow::StructArray>(unshredded->storage());
  const auto& output_metadata = output_storage->GetFieldByName("metadata");
  ASSERT_EQ(input_metadata->data().get(), output_metadata->data().get());
  const auto& output_values = checked_pointer_cast<::arrow::BinaryViewArray>(
      output_storage->GetFieldByName("value"));
  ASSERT_EQ(2, output_values->length());
  ASSERT_EQ(std::string_view{*typed_only_row.value}, output_values->GetView(0));
  ASSERT_EQ(std::string_view{*residual_value_row.value}, output_values->GetView(1));
}

TEST(TestVariantUnshred, UnshredsDecimalTypedValues) {
  auto assert_decimal = [](std::shared_ptr<::arrow::DataType> type, std::string_view json,
                           std::invocable<VariantBuilder&> auto&& append_expected) {
    VariantBuilder expected_builder;
    std::invoke(std::forward<decltype(append_expected)>(append_expected),
                expected_builder);
    auto expected = expected_builder.Finish();
    auto typed_value = ::arrow::ArrayFromJSON(type, std::string(json));
    auto array = MakeSingleRowVariantArray(std::string_view{*expected.metadata},
                                           std::nullopt, typed_value);

    AssertUnshreddedRow(*array, expected);
  };

  assert_decimal(::arrow::decimal32(/*precision=*/9, /*scale=*/2), R"(["-1234567.89"])",
                 [](VariantBuilder& builder) {
                   builder.AppendDecimal4(::arrow::Decimal32(-123456789), /*scale=*/2);
                 });
  assert_decimal(::arrow::decimal64(/*precision=*/9, /*scale=*/0), R"(["123456789"])",
                 [](VariantBuilder& builder) {
                   builder.AppendDecimal4(::arrow::Decimal32(123456789), /*scale=*/0);
                 });
  assert_decimal(::arrow::decimal64(/*precision=*/18, /*scale=*/0),
                 R"(["-123456789012345678"])", [](VariantBuilder& builder) {
                   builder.AppendDecimal8(::arrow::Decimal64(-123456789012345678),
                                          /*scale=*/0);
                 });
  assert_decimal(::arrow::decimal128(/*precision=*/9, /*scale=*/0), R"(["-123456789"])",
                 [](VariantBuilder& builder) {
                   builder.AppendDecimal4(::arrow::Decimal32(-123456789), /*scale=*/0);
                 });
  assert_decimal(::arrow::decimal128(/*precision=*/18, /*scale=*/0),
                 R"(["123456789012345678"])", [](VariantBuilder& builder) {
                   builder.AppendDecimal8(::arrow::Decimal64(123456789012345678),
                                          /*scale=*/0);
                 });
  assert_decimal(::arrow::decimal128(/*precision=*/21, /*scale=*/0),
                 R"(["100000000000000000000"])", [](VariantBuilder& builder) {
                   builder.AppendDecimal16(
                       ::arrow::Decimal128(std::string("100000000000000000000")),
                       /*scale=*/0);
                 });
}

TEST(TestVariantUnshred, MissingTopLevelValue) {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto expected = builder.Finish();
  auto array =
      MakeSingleRowVariantArray(std::string_view{*expected.metadata}, std::nullopt,
                                ::arrow::ArrayFromJSON(int64(), "[null]"));
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ASSERT_THROW(ValidateVariants<true>(chunked), ParquetInvalidOrCorruptedFileException);
  ValidateVariants<false>(chunked);
  AssertUnshreddedRow(*array, expected);
}

TEST(TestVariantUnshred, MissingObjectField) {
  auto expected = EmptyObjectWithField("a");
  auto field_group = MakeInt64FieldGroup({std::nullopt}, "[null]");
  auto typed_object_type = struct_({field("a", field_group->type(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(
      auto typed_object,
      ::arrow::StructArray::Make({field_group}, typed_object_type->fields()));
  auto array = MakeSingleRowVariantArray(std::string_view{*expected.metadata},
                                         std::nullopt, typed_object);
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ValidateVariants<true>(chunked);
  ValidateVariants<false>(chunked);
  AssertUnshreddedRow(*array, expected);
}

TEST(TestVariantUnshred, NullObjectFieldGroup) {
  auto expected = EmptyObjectWithField("a");
  auto field_group = MakeInt64FieldGroup({std::nullopt}, "[null]", {false});
  auto typed_object_type = struct_({field("a", field_group->type(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(
      auto typed_object,
      ::arrow::StructArray::Make({field_group}, typed_object_type->fields()));
  auto array = MakeSingleRowVariantArray(std::string_view{*expected.metadata},
                                         std::nullopt, typed_object);
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ASSERT_THROW(ValidateVariants<true>(chunked), ParquetInvalidOrCorruptedFileException);
  ValidateVariants<false>(chunked);
  AssertUnshreddedRow(*array, expected);
}

TEST(TestVariantUnshred, MissingListElements) {
  VariantBuilder builder;
  auto list = builder.StartList();
  list.AppendVariantNull();
  list.AppendVariantNull();
  list.Finish();
  auto expected = builder.Finish();
  auto field_group =
      MakeInt64FieldGroup({std::nullopt, std::nullopt}, "[null, null]", {true, false});
  auto typed_list_type =
      ::arrow::list(field("element", field_group->type(), /*nullable=*/false));
  ASSERT_OK_AND_ASSIGN(
      auto typed_list,
      ::arrow::ListArray::FromArrays(typed_list_type,
                                     *::arrow::ArrayFromJSON(::arrow::int32(), "[0, 2]"),
                                     *field_group));
  auto array = MakeSingleRowVariantArray(std::string_view{*expected.metadata},
                                         std::nullopt, typed_list);
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ASSERT_THROW(ValidateVariants<true>(chunked), ParquetInvalidOrCorruptedFileException);
  ValidateVariants<false>(chunked);
  AssertUnshreddedRow(*array, expected);
}

TEST(TestVariantUnshred, RejectsNullRow) {
  VariantArrayBuilder builder;
  builder.AppendNull();
  auto array = builder.Finish();

  ASSERT_THROW(UnshredVariantRow(*array, /*row=*/0), ParquetException);
}

TEST(TestVariantUnshred, PreservesSliceNullBitmap) {
  VariantBuilder builder;
  builder.AppendInt64(3);
  auto encoded = builder.Finish();
  auto metadata = BinaryArrayFromValues({std::string_view{*encoded.metadata},
                                         std::string_view{*encoded.metadata},
                                         std::string_view{*encoded.metadata}});
  auto values = BinaryArrayFromValues({std::nullopt, std::nullopt, std::nullopt});
  auto typed = ArrayFromJSON(int64(), "[1, 2, 3]");
  std::shared_ptr<::arrow::Buffer> null_bitmap;
  ::arrow::BitmapFromVector(std::vector<bool>{true, false, true}, &null_bitmap);
  auto storage_type = struct_({field("metadata", binary(), false),
                               field("value", binary()), field("typed_value", int64())});

  auto array =
      MakeVariantArrayFromChildren(storage_type, {metadata, values, typed}, null_bitmap);
  auto sliced =
      checked_pointer_cast<::arrow::extension::VariantArray>(array->Slice(1, 2));
  auto unshredded = UnshredVariantArray(*sliced);

  ASSERT_EQ(2, unshredded->length());
  ASSERT_TRUE(unshredded->IsNull(0));
  ASSERT_FALSE(unshredded->IsNull(1));
  ASSERT_THROW(UnshredVariantRow(*unshredded, /*row=*/0), ParquetException);
  auto actual = UnshredVariantRow(*unshredded, /*row=*/1);
  ASSERT_EQ(std::string_view{*encoded.metadata}, std::string_view{*actual.metadata});
  ASSERT_EQ(std::string_view{*encoded.value}, std::string_view{*actual.value});
}

TEST(TestVariantUnshred, RejectsDuplicateResidualOnUnshred) {
  auto encoded = ObjectWithInt64("a", 1);
  auto field_group = MakeInt64FieldGroup({std::nullopt}, "[2]");
  auto typed_value_type = struct_({field("a", field_group->type(), /*nullable=*/false)});
  PARQUET_ASSIGN_OR_THROW(
      auto typed_value,
      ::arrow::StructArray::Make({field_group}, typed_value_type->fields()));
  auto array = MakeSingleRowVariantArray(std::string_view{*encoded.metadata},
                                         std::string_view{*encoded.value}, typed_value);
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ValidateVariants<false>(chunked);
  ASSERT_THROW(UnshredVariantRow(*array, /*row=*/0),
               ParquetInvalidOrCorruptedFileException);
  ASSERT_THROW(UnshredVariantArray(*array), ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantUnshred, DuplicateFields) {
  auto encoded = ObjectWithInt64("a", 0);
  auto first = MakeInt64FieldGroup({std::nullopt}, "[1]");
  auto second = MakeInt64FieldGroup({std::nullopt}, "[2]");
  auto typed_value_type = struct_({field("a", first->type(), /*nullable=*/false),
                                   field("a", second->type(), /*nullable=*/false)});
  PARQUET_ASSIGN_OR_THROW(
      auto typed_value,
      ::arrow::StructArray::Make({first, second}, typed_value_type->fields()));
  auto array = MakeSingleRowVariantArray(std::string_view{*encoded.metadata},
                                         std::nullopt, typed_value);
  ::arrow::ChunkedArray chunked(::arrow::ArrayVector{array});

  ASSERT_THROW(ValidateVariants<true>(chunked), ParquetInvalidOrCorruptedFileException);
  ASSERT_THROW(ValidateVariants<false>(chunked), ParquetInvalidOrCorruptedFileException);
  ASSERT_THROW(UnshredVariantRow(*array, /*row=*/0),
               ParquetInvalidOrCorruptedFileException);
  ASSERT_THROW(UnshredVariantArray(*array), ParquetInvalidOrCorruptedFileException);
}

}  // namespace parquet::variant
