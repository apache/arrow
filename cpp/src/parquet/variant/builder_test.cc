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

#include "parquet/variant/builder.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/test_util_internal.h"

#include <array>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/array/util.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

using namespace std::string_view_literals;  // NOLINT

namespace parquet::variant {

namespace {

using ::arrow::ArrayFromJSON;
using ::arrow::binary;
using ::arrow::binary_view;
using ::arrow::BinaryViewArray;
using ::arrow::BinaryViewScalar;
using ::arrow::default_memory_pool;
using ::arrow::field;
using ::arrow::int64;
using ::arrow::MakeArrayFromScalar;
using ::arrow::ProxyMemoryPool;
using ::arrow::struct_;
using ::arrow::StructArray;
using ::arrow::Type;
using ::arrow::extension::variant;
using internal::BinaryArrayFromValues;

template <typename Metadata>
concept CanBindMetadata =
    (requires(VariantValueArrayBuilder& builder, Metadata&& metadata) {
      builder.BindMetadata(std::forward<Metadata>(metadata));
    });

static_assert(CanBindMetadata<const VariantMetadataView&>);
static_assert(!CanBindMetadata<VariantMetadataView>);

void AssertPrimitiveType(std::string_view value, const VariantMetadataView& metadata,
                         VariantPrimitiveType expected) {
  auto view = VariantValueView::Make(value, metadata);
  ASSERT_EQ(VariantBasicType::kPrimitive, view.basic_type());
  ASSERT_EQ(expected, std::get<VariantPrimitiveView>(view.data()).type());
}

void AssertPrimitiveFieldType(const VariantObjectView& object, std::string_view name,
                              VariantPrimitiveType expected) {
  auto field = object.GetField(name);
  ASSERT_TRUE(field.has_value()) << "Missing Variant object field: " << name;
  ASSERT_EQ(VariantBasicType::kPrimitive, field->basic_type());
  ASSERT_EQ(expected, std::get<VariantPrimitiveView>(field->data()).type());
}

std::pair<EncodedVariantValue, VariantMetadataView> MakeEmptyMetadata() {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  return {std::move(encoded), std::move(metadata)};
}

}  // namespace

TEST(TestVariantBuilder, Object) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendVariantNull("c");
  object.AppendVariantNull("b");
  object.AppendVariantNull("a");
  object.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  ASSERT_EQ(VariantBasicType::kObject, view.basic_type());
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(3, fields.size());
  ASSERT_EQ("a", fields[0].name);
  ASSERT_EQ("b", fields[1].name);
  ASSERT_EQ("c", fields[2].name);
}

TEST(TestVariantBuilder, List) {
  VariantBuilder builder;
  auto list = builder.StartList();
  list.AppendVariantNull();
  list.AppendInt32(42);
  list.AppendString("x");
  list.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  ASSERT_EQ(VariantBasicType::kArray, view.basic_type());
  const auto& array = std::get<VariantArrayView>(view.data());
  ASSERT_EQ(3, array.elements().size());

  auto element = array.GetElement(1);
  ASSERT_TRUE(element.has_value());
  ASSERT_EQ(VariantPrimitiveType::kInt32,
            std::get<VariantPrimitiveView>(element->data()).type());
}

TEST(TestVariantBuilder, Reset) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt32("old", 1);
  object.Finish();
  auto first = builder.Finish();

  auto list = builder.StartList();
  list.AppendInt32(2);
  list.Finish();
  auto second = builder.Finish();

  auto first_metadata = VariantMetadataView::Make(std::string_view{*first.metadata});
  ASSERT_TRUE(first_metadata.FindString("old").has_value());

  auto second_metadata = VariantMetadataView::Make(std::string_view{*second.metadata});
  ASSERT_FALSE(second_metadata.FindString("old").has_value());

  auto second_view =
      VariantValueView::Make(std::string_view{*second.value}, second_metadata);
  const auto& elements = std::get<VariantArrayView>(second_view.data()).elements();
  ASSERT_EQ(1, elements.size());
  AssertPrimitiveType(elements[0], second_metadata, VariantPrimitiveType::kInt32);
}

TEST(TestVariantBuilder, Nested) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  auto list = object.StartList("items");
  list.AppendInt32(1);
  auto child = list.StartObject();
  child.AppendString("name", "x");
  child.Finish();
  list.Finish();
  object.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto root = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& root_object = std::get<VariantObjectView>(root.data());
  ASSERT_EQ(1, root_object.fields().size());
  ASSERT_EQ("items", root_object.fields()[0].name);

  auto items = root_object.GetField("items");
  ASSERT_TRUE(items.has_value());
  const auto& items_array = std::get<VariantArrayView>(items->data());
  ASSERT_EQ(2, items_array.elements().size());
  auto item = items_array.GetElement(1);
  ASSERT_TRUE(item.has_value());
  ASSERT_EQ("name", std::get<VariantObjectView>(item->data()).fields()[0].name);
}

TEST(TestVariantBuilder, ObjectAppends) {
  const std::string uuid(16, '\1');
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt8("int8", 1);
  object.AppendInt64("int64", 2);
  object.AppendDouble("double", 3);
  object.AppendDecimal4("decimal", ::arrow::Decimal32(1234), 2);
  object.AppendBinary("binary", "abc");
  object.AppendDate("date", 1);
  object.AppendTimestampNanos("ts", 2, true);
  object.AppendUuid("uuid", uuid);
  object.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto root = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& object_view = std::get<VariantObjectView>(root.data());
  ASSERT_EQ(8, object_view.fields().size());
  AssertPrimitiveFieldType(object_view, "int8", VariantPrimitiveType::kInt8);
  AssertPrimitiveFieldType(object_view, "int64", VariantPrimitiveType::kInt64);
  AssertPrimitiveFieldType(object_view, "double", VariantPrimitiveType::kDouble);
  AssertPrimitiveFieldType(object_view, "decimal", VariantPrimitiveType::kDecimal4);
  AssertPrimitiveFieldType(object_view, "binary", VariantPrimitiveType::kBinary);
  AssertPrimitiveFieldType(object_view, "date", VariantPrimitiveType::kDate);
  AssertPrimitiveFieldType(object_view, "ts", VariantPrimitiveType::kTimestampNanos);
  AssertPrimitiveFieldType(object_view, "uuid", VariantPrimitiveType::kUuid);
}

TEST(TestVariantBuilder, ListAppends) {
  const std::string uuid(16, '\2');
  VariantBuilder builder;
  auto list = builder.StartList();
  list.AppendInt8(1);
  list.AppendInt64(2);
  list.AppendDouble(3);
  list.AppendDecimal4(::arrow::Decimal32(1234), 2);
  list.AppendBinary("abc");
  list.AppendDate(1);
  list.AppendTimestampNanos(2, true);
  list.AppendUuid(uuid);
  list.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto root = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& elements = std::get<VariantArrayView>(root.data()).elements();
  ASSERT_EQ(8, elements.size());
  AssertPrimitiveType(elements[0], metadata, VariantPrimitiveType::kInt8);
  AssertPrimitiveType(elements[1], metadata, VariantPrimitiveType::kInt64);
  AssertPrimitiveType(elements[2], metadata, VariantPrimitiveType::kDouble);
  AssertPrimitiveType(elements[3], metadata, VariantPrimitiveType::kDecimal4);
  AssertPrimitiveType(elements[4], metadata, VariantPrimitiveType::kBinary);
  AssertPrimitiveType(elements[5], metadata, VariantPrimitiveType::kDate);
  AssertPrimitiveType(elements[6], metadata, VariantPrimitiveType::kTimestampNanos);
  AssertPrimitiveType(elements[7], metadata, VariantPrimitiveType::kUuid);
}

TEST(TestVariantBuilder, ObjectRollback) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  {
    auto child = object.StartObject("value");
    child.AppendString("drop", "x");
  }
  object.AppendInt32("value", 1);
  object.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_FALSE(metadata.FindString("drop").has_value());
  ASSERT_TRUE(metadata.sorted_strings());
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("value", fields[0].name);
}

TEST(TestVariantBuilder, ListRollback) {
  VariantBuilder builder;
  auto list = builder.StartList();
  {
    auto child = list.StartObject();
    child.AppendString("drop", "x");
  }
  list.AppendInt32(1);
  list.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_FALSE(metadata.FindString("drop").has_value());
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& elements = std::get<VariantArrayView>(view.data()).elements();
  ASSERT_EQ(1, elements.size());
  AssertPrimitiveType(elements[0], metadata, VariantPrimitiveType::kInt32);
}

TEST(TestVariantBuilder, MoveRollback) {
  VariantBuilder replacement_builder;
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendString("drop", "x");

  auto replacement = replacement_builder.StartObject();
  object = std::move(replacement);

  auto restored = builder.StartObject();
  restored.AppendInt32("value", 1);
  restored.Finish();
  auto encoded = builder.Finish();

  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_FALSE(metadata.FindString("drop").has_value());
  object.Finish();
  replacement_builder.Finish();
}

TEST(TestVariantBuilder, Duplicate) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt32("a", 1);
  ASSERT_THROW(object.AppendString("a", "x"), ParquetException);
}

TEST(TestVariantBuilder, UsesMemoryPool) {
  ProxyMemoryPool pool(default_memory_pool());
  VariantBuilder builder(&pool);
  builder.AppendString(std::string(128, 'x'));
  auto encoded = builder.Finish();
  ASSERT_GT(pool.total_bytes_allocated(), 0);

  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  VariantValueView::Validate(std::string_view{*encoded.value}, metadata);
}

TEST(TestVariantBuilder, FinishValidates) {
  const std::string invalid_value(1, '\x54');

  VariantBuilder builder1;
  builder1.AppendEncodedValue(invalid_value);
  ASSERT_THROW(builder1.Finish(), ParquetInvalidOrCorruptedFileException);

  // Invalid case, skip validation
  VariantBuilder builder2(default_memory_pool(), /*validate=*/false);
  builder2.AppendEncodedValue(invalid_value);
  auto encoded = builder2.Finish();
  ASSERT_EQ("\x01\x00\x00"sv, std::string_view{*encoded.metadata});
  ASSERT_EQ(invalid_value, std::string_view{*encoded.value});
}

TEST(TestVariantBuilder, EmptyEncodedValue) {
  VariantBuilder builder;
  ASSERT_THROW(builder.AppendEncodedValue(""), ParquetException);
  auto [metadata_owner, metadata] = MakeEmptyMetadata();
  VariantValueArrayBuilder value_builder;
  ASSERT_THROW(value_builder.AppendEncodedValue(""), ParquetException);
  auto row = value_builder.BindMetadata(metadata);
  ASSERT_THROW(row.AppendEncodedValue(""), ParquetException);
}

TEST(TestVariantBuilder, ArrayBuilder) {
  VariantArrayBuilder builder;
  builder.AppendNull();
  builder.AppendVariantNull();
  builder.AppendInt32(42);
  builder.AppendInt8(1);
  builder.AppendDouble(2);
  builder.AppendBinary("abc");
  builder.AppendTimestampMicros(3, true);
  auto object = builder.StartObject();
  object.AppendString("a", "x");
  object.Finish();

  auto array = builder.Finish();
  ASSERT_EQ(8, array->length());
  ASSERT_TRUE(array->IsNull(0));
  ASSERT_FALSE(array->IsNull(1));

  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  ASSERT_FALSE(storage.type()->field(0)->nullable());
  ASSERT_FALSE(storage.type()->field(1)->nullable());
  ASSERT_EQ(Type::BINARY_VIEW, storage.field(0)->type_id());
  ASSERT_EQ(Type::BINARY_VIEW, storage.field(1)->type_id());
}

TEST(TestVariantBuilder, Tags) {
  VariantArrayBuilder builder;
  builder.AppendBoolean(true);
  builder.AppendBoolean(false);
  builder.AppendTimestampMicros(1, true);
  builder.AppendTimestampMicros(2, false);
  builder.AppendTimestampNanos(3, true);
  builder.AppendTimestampNanos(4, false);

  auto array = builder.Finish();
  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  const auto& metadata_values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(0));
  const auto& values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(1));
  const std::array expected = {
      VariantPrimitiveType::kBooleanTrue,     VariantPrimitiveType::kBooleanFalse,
      VariantPrimitiveType::kTimestampMicros, VariantPrimitiveType::kTimestampNTZMicros,
      VariantPrimitiveType::kTimestampNanos,  VariantPrimitiveType::kTimestampNTZNanos,
  };
  ASSERT_EQ(static_cast<int64_t>(expected.size()), array->length());
  for (int64_t row = 0; row < array->length(); ++row) {
    auto metadata = VariantMetadataView::Make(metadata_values.GetView(row));
    AssertPrimitiveType(values.GetView(row), metadata, expected[row]);
  }
}

TEST(TestVariantBuilder, ArrayBuilderViews) {
  VariantBuilder value;
  value.AppendString("x");
  auto short_encoded = value.Finish();

  VariantArrayBuilder builder;
  builder.AppendEncoded(short_encoded);
  auto object = builder.StartObject();
  object.AppendString("abcdefghijklm", std::string(32, 'y'));
  object.Finish();

  auto array = builder.Finish();
  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  const auto& metadata =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(0));
  const auto& values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(1));

  auto* metadata_views = metadata.data()->GetValues<::arrow::BinaryViewType::c_type>(1);
  auto* value_views = values.data()->GetValues<::arrow::BinaryViewType::c_type>(1);
  ASSERT_TRUE(metadata_views[0].is_inline());
  ASSERT_TRUE(value_views[0].is_inline());
  ASSERT_FALSE(metadata_views[1].is_inline());
  ASSERT_FALSE(value_views[1].is_inline());
}

TEST(TestVariantBuilder, ValueArrayBuilder) {
  auto [metadata_owner, metadata] = MakeEmptyMetadata();

  VariantBuilder encoded_builder;
  encoded_builder.AppendString("hello");
  auto encoded = encoded_builder.Finish();

  VariantValueArrayBuilder builder;
  builder.AppendNull();
  builder.AppendEncodedValue(std::string_view{*encoded.value});
  auto row = builder.BindMetadata(metadata);
  row.AppendInt32(42);
  row.Finish();

  auto array = builder.Finish();
  ASSERT_EQ(3, array->length());
  ASSERT_TRUE(array->IsNull(0));
  AssertPrimitiveType(array->GetView(1), metadata, VariantPrimitiveType::kString);
  AssertPrimitiveType(array->GetView(2), metadata, VariantPrimitiveType::kInt32);
}

TEST(TestVariantBuilder, SharedMetadata) {
  constexpr std::string_view kId = "customer_identifier";
  constexpr std::string_view kName = "customer_name";

  VariantBuilder metadata_builder;
  metadata_builder.AddFieldName(kId);
  metadata_builder.AddFieldName(kName);
  metadata_builder.AppendVariantNull();
  auto metadata_encoded = metadata_builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*metadata_encoded.metadata});

  VariantValueArrayBuilder value_builder;
  {
    auto row = value_builder.BindMetadata(metadata);
    auto object = row.StartObject();
    object.AppendInt32(kId, 1);
    object.Finish();
    row.Finish();
  }
  {
    auto row = value_builder.BindMetadata(metadata);
    auto object = row.StartObject();
    object.AppendString(kName, "Alice");
    object.Finish();
    row.Finish();
  }
  auto values = value_builder.Finish();

  ASSERT_OK_AND_ASSIGN(
      auto metadata_values,
      MakeArrayFromScalar(BinaryViewScalar(metadata_encoded.metadata), values->length()));
  auto storage_type = struct_({field("metadata", binary_view(), /*nullable=*/false),
                               field("value", binary_view(), /*nullable=*/false)});
  auto array = MakeVariantArrayFromChildren(
      std::move(storage_type), {std::move(metadata_values), std::move(values)});

  ASSERT_EQ(2, array->length());
  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  const auto& metadata_array =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(0));
  const auto& value_array =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*storage.field(1));
  ASSERT_EQ(std::string_view{*metadata_encoded.metadata}, metadata_array.GetView(0));
  ASSERT_EQ(metadata_array.GetView(0), metadata_array.GetView(1));
  ASSERT_EQ(metadata_encoded.metadata, metadata_array.data()->buffers[2]);

  auto first = VariantValueView::Make(value_array.GetView(0), metadata);
  AssertPrimitiveFieldType(std::get<VariantObjectView>(first.data()), kId,
                           VariantPrimitiveType::kInt32);

  auto second = VariantValueView::Make(value_array.GetView(1), metadata);
  AssertPrimitiveFieldType(std::get<VariantObjectView>(second.data()), kName,
                           VariantPrimitiveType::kString);
}

TEST(TestVariantBuilder, ValueRowFinish) {
  auto [metadata_owner, metadata] = MakeEmptyMetadata();

  VariantValueArrayBuilder builder;
  auto row = builder.BindMetadata(metadata);
  row.AppendInt32(42);
  row.Finish();
  ASSERT_THROW(row.Finish(), ParquetException);

  auto array = builder.Finish();
  ASSERT_EQ(1, array->length());
  AssertPrimitiveType(array->GetView(0), metadata, VariantPrimitiveType::kInt32);
}

TEST(TestVariantBuilder, ValueRowMove) {
  auto [metadata_owner, metadata] = MakeEmptyMetadata();

  VariantValueArrayBuilder replacement_builder;
  VariantValueArrayBuilder builder;
  auto row = builder.BindMetadata(metadata);
  row.AppendInt32(1);
  auto replacement = replacement_builder.BindMetadata(metadata);
  row = std::move(replacement);

  auto restored = builder.BindMetadata(metadata);
  restored.AppendString(std::string(32, 'x'));
  restored.Finish();
  auto values = builder.Finish();
  ASSERT_EQ(1, values->length());
  ASSERT_EQ(values->GetView(0).size(), values->data()->buffers[2]->size());

  row.AppendVariantNull();
  row.Finish();
  replacement_builder.Finish();
}

TEST(TestVariantBuilder, ValueArrayBuilderObject) {
  VariantBuilder metadata_builder;
  auto metadata_object = metadata_builder.StartObject();
  metadata_object.AppendInt32("a", 1);
  auto metadata_list = metadata_object.StartList("b");
  metadata_list.AppendVariantNull();
  metadata_list.Finish();
  metadata_object.Finish();
  auto metadata_encoded = metadata_builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*metadata_encoded.metadata});

  VariantValueArrayBuilder builder;
  auto row = builder.BindMetadata(metadata);
  auto object = row.StartObject();
  object.AppendInt32("a", 42);
  auto list = object.StartList("b");
  list.AppendString("x");
  list.Finish();
  object.Finish();
  row.Finish();

  auto array = builder.Finish();
  ASSERT_EQ(1, array->length());
  auto view = VariantValueView::Make(array->GetView(0), metadata);
  ASSERT_EQ(VariantBasicType::kObject, view.basic_type());
  const auto& object_view = std::get<VariantObjectView>(view.data());
  ASSERT_EQ(2, object_view.fields().size());
  AssertPrimitiveFieldType(object_view, "a", VariantPrimitiveType::kInt32);
  auto list_view = object_view.GetField("b");
  ASSERT_TRUE(list_view.has_value());
  ASSERT_EQ(VariantBasicType::kArray, list_view->basic_type());
}

TEST(TestVariantBuilder, ValueArrayBuilderMissingField) {
  auto [metadata_owner, metadata] = MakeEmptyMetadata();

  VariantValueArrayBuilder builder;
  auto row = builder.BindMetadata(metadata);
  auto object = row.StartObject();
  ASSERT_THROW(object.AppendInt32("missing", 1), ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantBuilder, FromStorage) {
  VariantBuilder value;
  value.AppendInt32(1);
  auto encoded = value.Finish();

  auto metadata = BinaryArrayFromValues({std::string_view{*encoded.metadata}});
  auto values = BinaryArrayFromValues({std::string_view{*encoded.value}});
  ASSERT_OK_AND_ASSIGN(
      auto storage,
      StructArray::Make({metadata, values}, {field("metadata", binary(), false),
                                             field("value", binary(), false)}));

  auto array = MakeVariantArrayFromStorage(storage);
  ASSERT_EQ(1, array->length());
  ASSERT_TRUE(array->type()->Equals(variant(storage->type())));
}

TEST(TestVariantBuilder, FromShredded) {
  VariantBuilder value;
  value.AppendVariantNull();
  auto encoded = value.Finish();

  auto metadata = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto values = BinaryArrayFromValues({std::nullopt, std::string_view{*encoded.value}});
  auto typed = ArrayFromJSON(int64(), "[1, null]");
  auto storage_type = struct_({field("metadata", binary(), false),
                               field("value", binary()), field("typed_value", int64())});

  auto array = MakeVariantArrayFromChildren(storage_type, {metadata, values, typed});
  ASSERT_EQ(2, array->length());
  ASSERT_TRUE(array->type()->Equals(variant(storage_type)));
}

}  // namespace parquet::variant
