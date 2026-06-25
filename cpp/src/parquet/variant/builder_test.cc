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
#include "parquet/variant/encoding.h"
#include "parquet/variant/test_util_internal.h"

#include <optional>
#include <string>
#include <string_view>

#include "arrow/array.h"  // IWYU pragma: keep
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace parquet::variant {

namespace {

using ::arrow::ArrayFromJSON;
using ::arrow::binary;
using ::arrow::default_memory_pool;
using ::arrow::field;
using ::arrow::int64;
using ::arrow::ProxyMemoryPool;
using ::arrow::struct_;
using ::arrow::StructArray;
using ::arrow::Type;
using ::arrow::extension::variant;
using internal::BinaryArrayFromValues;
using internal::MakeVariantValueView;

void AssertPrimitiveType(std::string_view value, const VariantMetadataView& metadata,
                         VariantPrimitiveType expected) {
  ASSERT_OK_AND_ASSIGN(auto view, VariantValueView::Make(value, metadata));
  ASSERT_EQ(VariantBasicType::kPrimitive, view.basic_type());
  ASSERT_EQ(expected, std::get<VariantPrimitiveView>(view.data()).type());
}

void AssertPrimitiveFieldType(const VariantObjectView& object, std::string_view name,
                              const VariantMetadataView& metadata,
                              VariantPrimitiveType expected) {
  const auto* field = object.FindField(name);
  ASSERT_NE(nullptr, field) << "Missing Variant object field: " << name;
  AssertPrimitiveType(field->value, metadata, expected);
}

}  // namespace

TEST(TestVariantBuilder, Object) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK(object.AppendVariantNull("c"));
  ASSERT_OK(object.AppendVariantNull("b"));
  ASSERT_OK(object.AppendVariantNull("a"));
  ASSERT_OK(object.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto view, MakeVariantValueView(encoded));
  ASSERT_EQ(VariantBasicType::kObject, view.basic_type());
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(3, fields.size());
  ASSERT_EQ("a", fields[0].name);
  ASSERT_EQ("b", fields[1].name);
  ASSERT_EQ("c", fields[2].name);
}

TEST(TestVariantBuilder, List) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto list, builder.StartList());
  ASSERT_OK(list.AppendVariantNull());
  ASSERT_OK(list.AppendInt32(42));
  ASSERT_OK(list.AppendString("x"));
  ASSERT_OK(list.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_OK_AND_ASSIGN(
      auto view, VariantValueView::Make(std::string_view{*encoded.value}, metadata));
  ASSERT_EQ(VariantBasicType::kArray, view.basic_type());
  const auto& elements = std::get<VariantArrayView>(view.data()).elements();
  ASSERT_EQ(3, elements.size());

  ASSERT_OK_AND_ASSIGN(auto element, VariantValueView::Make(elements[1], metadata));
  ASSERT_EQ(VariantPrimitiveType::kInt32,
            std::get<VariantPrimitiveView>(element.data()).type());
}

TEST(TestVariantBuilder, Nested) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK_AND_ASSIGN(auto list, object.StartList("items"));
  ASSERT_OK(list.AppendInt32(1));
  ASSERT_OK_AND_ASSIGN(auto child, list.StartObject());
  ASSERT_OK(child.AppendString("name", "x"));
  ASSERT_OK(child.Finish());
  ASSERT_OK(list.Finish());
  ASSERT_OK(object.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_OK_AND_ASSIGN(
      auto root, VariantValueView::Make(std::string_view{*encoded.value}, metadata));
  const auto& root_fields = std::get<VariantObjectView>(root.data()).fields();
  ASSERT_EQ(1, root_fields.size());
  ASSERT_EQ("items", root_fields[0].name);

  ASSERT_OK_AND_ASSIGN(auto items,
                       VariantValueView::Make(root_fields[0].value, metadata));
  const auto& item_values = std::get<VariantArrayView>(items.data()).elements();
  ASSERT_EQ(2, item_values.size());
  ASSERT_OK_AND_ASSIGN(auto item, VariantValueView::Make(item_values[1], metadata));
  ASSERT_EQ("name", std::get<VariantObjectView>(item.data()).fields()[0].name);
}

TEST(TestVariantBuilder, ObjectAppends) {
  const std::string uuid(16, '\1');
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK(object.AppendInt8("int8", 1));
  ASSERT_OK(object.AppendInt64("int64", 2));
  ASSERT_OK(object.AppendDouble("double", 3));
  ASSERT_OK(object.AppendDecimal4("decimal", 1234, 2));
  ASSERT_OK(object.AppendBinary("binary", "abc"));
  ASSERT_OK(object.AppendDate("date", 1));
  ASSERT_OK(object.AppendTimestampNanos("ts", 2, true));
  ASSERT_OK(object.AppendUuid("uuid", uuid));
  ASSERT_OK(object.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_OK_AND_ASSIGN(
      auto root, VariantValueView::Make(std::string_view{*encoded.value}, metadata));
  const auto& object_view = std::get<VariantObjectView>(root.data());
  ASSERT_EQ(8, object_view.fields().size());
  AssertPrimitiveFieldType(object_view, "int8", metadata, VariantPrimitiveType::kInt8);
  AssertPrimitiveFieldType(object_view, "int64", metadata, VariantPrimitiveType::kInt64);
  AssertPrimitiveFieldType(object_view, "double", metadata,
                           VariantPrimitiveType::kDouble);
  AssertPrimitiveFieldType(object_view, "decimal", metadata,
                           VariantPrimitiveType::kDecimal4);
  AssertPrimitiveFieldType(object_view, "binary", metadata,
                           VariantPrimitiveType::kBinary);
  AssertPrimitiveFieldType(object_view, "date", metadata, VariantPrimitiveType::kDate);
  AssertPrimitiveFieldType(object_view, "ts", metadata,
                           VariantPrimitiveType::kTimestampNanos);
  AssertPrimitiveFieldType(object_view, "uuid", metadata, VariantPrimitiveType::kUuid);
}

TEST(TestVariantBuilder, ListAppends) {
  const std::string uuid(16, '\2');
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto list, builder.StartList());
  ASSERT_OK(list.AppendInt8(1));
  ASSERT_OK(list.AppendInt64(2));
  ASSERT_OK(list.AppendDouble(3));
  ASSERT_OK(list.AppendDecimal4(1234, 2));
  ASSERT_OK(list.AppendBinary("abc"));
  ASSERT_OK(list.AppendDate(1));
  ASSERT_OK(list.AppendTimestampNanos(2, true));
  ASSERT_OK(list.AppendUuid(uuid));
  ASSERT_OK(list.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_OK_AND_ASSIGN(
      auto root, VariantValueView::Make(std::string_view{*encoded.value}, metadata));
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

TEST(TestVariantBuilder, Rollback) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  {
    ASSERT_OK_AND_ASSIGN(auto child, object.StartObject("drop"));
    ASSERT_OK(child.AppendString("nested", "x"));
  }
  ASSERT_OK(object.AppendInt32("ok", 1));
  ASSERT_OK(object.Finish());

  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto view, MakeVariantValueView(encoded));
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("ok", fields[0].name);
}

TEST(TestVariantBuilder, Duplicate) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK(object.AppendInt32("a", 1));
  ASSERT_RAISES(Invalid, object.AppendString("a", "x"));
}

TEST(TestVariantBuilder, UsesMemoryPool) {
  ProxyMemoryPool pool(default_memory_pool());
  VariantBuilder builder(&pool);
  ASSERT_OK(builder.AppendString(std::string(128, 'x')));
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_GT(pool.total_bytes_allocated(), 0);

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_OK(VariantValueView::Validate(std::string_view{*encoded.value}, metadata));
}

TEST(TestVariantBuilder, ArrayBuilder) {
  VariantArrayBuilder builder;
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendVariantNull());
  ASSERT_OK(builder.AppendInt32(42));
  ASSERT_OK(builder.AppendInt8(1));
  ASSERT_OK(builder.AppendDouble(2));
  ASSERT_OK(builder.AppendBinary("abc"));
  ASSERT_OK(builder.AppendTimestampMicros(3, true));
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK(object.AppendString("a", "x"));
  ASSERT_OK(object.Finish());

  ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());
  ASSERT_EQ(8, array->length());
  ASSERT_TRUE(array->IsNull(0));
  ASSERT_FALSE(array->IsNull(1));

  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  ASSERT_FALSE(storage.type()->field(0)->nullable());
  ASSERT_FALSE(storage.type()->field(1)->nullable());
  ASSERT_EQ(Type::BINARY, storage.field(0)->type_id());
  ASSERT_EQ(Type::BINARY, storage.field(1)->type_id());
}

TEST(TestVariantBuilder, FromStorage) {
  VariantBuilder value;
  ASSERT_OK(value.AppendInt32(1));
  ASSERT_OK_AND_ASSIGN(auto encoded, value.Finish());

  auto metadata = BinaryArrayFromValues({std::string_view{*encoded.metadata}});
  auto values = BinaryArrayFromValues({std::string_view{*encoded.value}});
  ASSERT_OK_AND_ASSIGN(
      auto storage,
      StructArray::Make({metadata, values}, {field("metadata", binary(), false),
                                             field("value", binary(), false)}));

  ASSERT_OK_AND_ASSIGN(auto array, MakeVariantArrayFromStorage(storage));
  ASSERT_EQ(1, array->length());
  ASSERT_TRUE(array->type()->Equals(variant(storage->type())));
}

TEST(TestVariantBuilder, FromShredded) {
  VariantBuilder value;
  ASSERT_OK(value.AppendVariantNull());
  ASSERT_OK_AND_ASSIGN(auto encoded, value.Finish());

  auto metadata = BinaryArrayFromValues(
      {std::string_view{*encoded.metadata}, std::string_view{*encoded.metadata}});
  auto values = BinaryArrayFromValues({std::nullopt, std::string_view{*encoded.value}});
  auto typed = ArrayFromJSON(int64(), "[1, null]");
  auto storage_type = struct_({field("metadata", binary(), false),
                               field("value", binary()), field("typed_value", int64())});

  ASSERT_OK_AND_ASSIGN(
      auto array, MakeVariantArrayFromChildren(storage_type, {metadata, values, typed}));
  ASSERT_EQ(2, array->length());
  ASSERT_TRUE(array->type()->Equals(variant(storage_type)));
}

TEST(TestVariantBuilder, FallbackValue) {
  VariantBuilder value;
  ASSERT_OK(value.AppendString("x"));
  ASSERT_OK_AND_ASSIGN(auto encoded, value.Finish());

  VariantValueArrayBuilder builder;
  ASSERT_OK(builder.AppendNull());
  ASSERT_OK(builder.AppendEncodedValue(std::string_view{*encoded.metadata},
                                       std::string_view{*encoded.value}));
  ASSERT_OK_AND_ASSIGN(auto array, builder.Finish());
  ASSERT_EQ(2, array->length());
  ASSERT_TRUE(array->IsNull(0));
  ASSERT_EQ(std::string_view{*encoded.value}, array->GetView(1));
}

}  // namespace parquet::variant
