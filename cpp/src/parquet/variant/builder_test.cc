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
  auto view = VariantValueView::Make(value, metadata);
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
  auto object = builder.StartObject();
  object.AppendVariantNull("c");
  object.AppendVariantNull("b");
  object.AppendVariantNull("a");
  object.Finish();

  auto encoded = builder.Finish();
  auto view = MakeVariantValueView(encoded);
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
  const auto& elements = std::get<VariantArrayView>(view.data()).elements();
  ASSERT_EQ(3, elements.size());

  auto element = VariantValueView::Make(elements[1], metadata);
  ASSERT_EQ(VariantPrimitiveType::kInt32,
            std::get<VariantPrimitiveView>(element.data()).type());
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
  const auto& root_fields = std::get<VariantObjectView>(root.data()).fields();
  ASSERT_EQ(1, root_fields.size());
  ASSERT_EQ("items", root_fields[0].name);

  auto items = VariantValueView::Make(root_fields[0].value, metadata);
  const auto& item_values = std::get<VariantArrayView>(items.data()).elements();
  ASSERT_EQ(2, item_values.size());
  auto item = VariantValueView::Make(item_values[1], metadata);
  ASSERT_EQ("name", std::get<VariantObjectView>(item.data()).fields()[0].name);
}

TEST(TestVariantBuilder, ObjectAppends) {
  const std::string uuid(16, '\1');
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt8("int8", 1);
  object.AppendInt64("int64", 2);
  object.AppendDouble("double", 3);
  object.AppendDecimal4("decimal", 1234, 2);
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
  auto list = builder.StartList();
  list.AppendInt8(1);
  list.AppendInt64(2);
  list.AppendDouble(3);
  list.AppendDecimal4(1234, 2);
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

TEST(TestVariantBuilder, Rollback) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  {
    auto child = object.StartObject("drop");
    child.AppendString("nested", "x");
  }
  object.AppendInt32("ok", 1);
  object.Finish();

  auto encoded = builder.Finish();
  auto view = MakeVariantValueView(encoded);
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("ok", fields[0].name);
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
  ASSERT_EQ(Type::BINARY, storage.field(0)->type_id());
  ASSERT_EQ(Type::BINARY, storage.field(1)->type_id());
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

TEST(TestVariantBuilder, FallbackValue) {
  VariantBuilder value;
  value.AppendString("x");
  auto encoded = value.Finish();

  VariantValueArrayBuilder builder;
  builder.AppendNull();
  builder.AppendEncodedValue(std::string_view{*encoded.metadata},
                             std::string_view{*encoded.value});
  auto array = builder.Finish();
  ASSERT_EQ(2, array->length());
  ASSERT_TRUE(array->IsNull(0));
  ASSERT_EQ(std::string_view{*encoded.value}, array->GetView(1));
}

}  // namespace parquet::variant
