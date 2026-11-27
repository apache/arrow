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
#include <limits>
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
concept CanStartValueObject =
    (requires(VariantValueArrayBuilder& builder, Metadata&& metadata) {
      builder.StartObject(std::forward<Metadata>(metadata));
    });

template <typename Metadata>
concept CanStartValueList =
    (requires(VariantValueArrayBuilder& builder, Metadata&& metadata) {
      builder.StartList(std::forward<Metadata>(metadata));
    });

static_assert(CanStartValueObject<MetadataBuilder&>);
static_assert(CanStartValueObject<const VariantMetadataView&>);
static_assert(!CanStartValueObject<VariantMetadataView>);
static_assert(!CanStartValueObject<const VariantMetadataView>);
static_assert(CanStartValueList<MetadataBuilder&>);
static_assert(CanStartValueList<const VariantMetadataView&>);
static_assert(!CanStartValueList<VariantMetadataView>);
static_assert(!CanStartValueList<const VariantMetadataView>);

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
  builder.SetVariantNull();
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  return {std::move(encoded), std::move(metadata)};
}

}  // namespace

TEST(TestVariantBuilder, MetadataBuilders) {
  MetadataBuilder metadata;
  metadata.Reserve(3);
  ASSERT_EQ(0, metadata.TryUpsert("z"));
  ASSERT_EQ(1, metadata.TryUpsert("a"));
  ASSERT_EQ(0, metadata.TryUpsert("z"));
  ASSERT_THROW(metadata.TryUpsert("\xff"), ParquetException);
  ASSERT_EQ(2, metadata.size());
  ASSERT_EQ(2, metadata.TryUpsert("b"));

  auto encoded = metadata.Finish();
  ASSERT_EQ(0, metadata.size());
  ASSERT_EQ(0, metadata.TryUpsert("fresh"));
  auto second_encoded = metadata.Finish();
  ASSERT_EQ(0, metadata.size());
  auto empty_encoded = metadata.Finish();
  ASSERT_EQ(0, metadata.size());

  // Parse previously returned buffers after reusing the builder.
  auto view = VariantMetadataView::Make(std::string_view{*encoded});
  ASSERT_EQ(3, view.dictionary_size());
  ASSERT_FALSE(view.sorted_strings());
  ASSERT_EQ("z", view.string(0));
  ASSERT_EQ("a", view.string(1));
  ASSERT_EQ("b", view.string(2));
  auto second = VariantMetadataView::Make(std::string_view{*second_encoded});
  ASSERT_EQ(1, second.dictionary_size());
  ASSERT_TRUE(second.sorted_strings());
  ASSERT_EQ("fresh", second.string(0));

  ASSERT_EQ("\x01\x00\x00"sv, std::string_view{*empty_encoded});
}

TEST(TestVariantBuilder, MetadataEncodingFailure) {
  ProxyMemoryPool pool(default_memory_pool());
  constexpr int64_t kMemoryLimit = 64;
  ::arrow::CappedMemoryPool capped_pool(&pool, kMemoryLimit);
  MetadataBuilder metadata(&capped_pool);
  ASSERT_EQ(0, metadata.TryUpsert("a"));
  ASSERT_EQ(1, metadata.TryUpsert("z"));
  {
    ASSERT_OK_AND_ASSIGN(
        auto occupied,
        ::arrow::AllocateBuffer(kMemoryLimit - pool.bytes_allocated(), &capped_pool));
    ASSERT_THROW(metadata.Finish(), ParquetException);
    ASSERT_EQ(2, metadata.size());
    ASSERT_EQ(0, metadata.TryUpsert("a"));
    ASSERT_EQ(1, metadata.TryUpsert("z"));
  }

  auto encoded = metadata.Finish();
  ASSERT_EQ(0, metadata.size());
  auto view = VariantMetadataView::Make(std::string_view{*encoded});
  ASSERT_EQ(2, view.dictionary_size());
  ASSERT_EQ("a", view.string(0));
  ASSERT_EQ("z", view.string(1));
}

TEST(TestVariantBuilder, Object) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AddVariantNull("c");
  object.AddVariantNull("b");
  object.AddVariantNull("a");
  std::move(object).Finish();

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
  std::move(list).Finish();

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
  builder.SetInt32(42);
  ASSERT_THROW(builder.SetString("second"), ParquetException);
  ASSERT_THROW(builder.StartObject(), ParquetException);
  ASSERT_THROW(builder.StartList(), ParquetException);
  builder.ReserveFieldNames(1);
  ASSERT_EQ(0, builder.AddFieldName("prefilled"));
  auto primitive = builder.Finish();
  ASSERT_EQ("\x14\x2a\x00\x00\x00"sv, std::string_view{*primitive.value});
  auto primitive_metadata =
      VariantMetadataView::Make(std::string_view{*primitive.metadata});
  ASSERT_EQ(1, primitive_metadata.dictionary_size());
  ASSERT_EQ("prefilled", primitive_metadata.string(0));

  builder.AddFieldName("discarded");
  builder.SetInt8(1);
  builder.Reset();

  auto object = builder.StartObject();
  object.AddInt32("old", 1);
  std::move(object).Finish();
  auto first = builder.Finish();

  auto list = builder.StartList();
  list.AppendInt32(2);
  std::move(list).Finish();
  auto second = builder.Finish();

  auto first_metadata = VariantMetadataView::Make(std::string_view{*first.metadata});
  ASSERT_EQ(1, first_metadata.dictionary_size());
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
  child.AddString("name", "x");
  std::move(child).Finish();
  std::move(list).Finish();
  std::move(object).Finish();

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

TEST(TestVariantBuilder, ObjectAdds) {
  const std::string uuid(16, '\1');
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AddInt8("int8", 1);
  object.AddInt64("int64", 2);
  object.AddDouble("double", 3);
  object.AddDecimal4("decimal", ::arrow::Decimal32(1234), 2);
  object.AddBinary("binary", "abc");
  object.AddDate("date", 1);
  object.AddTimestampNanos("ts", 2, true);
  object.AddUuid("uuid", uuid);
  std::move(object).Finish();

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
  std::move(list).Finish();

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
  builder.AddFieldName("keep");
  {
    auto dropped = builder.StartObject();
    dropped.AddInt32("discarded", 1);
  }
  auto object = builder.StartObject();
  {
    auto child = object.StartObject("value");
    child.AddString("drop", "x");
  }
  object.AddInt32("value", 1);
  std::move(object).Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_EQ(2, metadata.dictionary_size());
  ASSERT_EQ("keep", metadata.string(0));
  ASSERT_FALSE(metadata.FindString("drop").has_value());
  ASSERT_TRUE(metadata.sorted_strings());
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  const auto& fields = std::get<VariantObjectView>(view.data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("value", fields[0].name);
}

TEST(TestVariantBuilder, ListRollback) {
  VariantBuilder builder;
  builder.AddFieldName("keep");
  {
    auto dropped = builder.StartList();
    auto child = dropped.StartObject();
    child.AddInt32("discarded", 1);
    std::move(child).Finish();
  }
  auto list = builder.StartList();
  {
    auto child = list.StartObject();
    child.AddString("drop", "x");
  }
  list.AppendInt32(1);
  std::move(list).Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_EQ(1, metadata.dictionary_size());
  ASSERT_EQ("keep", metadata.string(0));
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
  object.AddString("drop", "x");

  auto replacement = replacement_builder.StartObject();
  object = std::move(replacement);

  auto restored = builder.StartObject();
  restored.AddInt32("value", 1);
  std::move(restored).Finish();
  auto encoded = builder.Finish();

  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_FALSE(metadata.FindString("drop").has_value());
  std::move(object).Finish();
  replacement_builder.Finish();
}

TEST(TestVariantBuilder, Duplicate) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AddInt32("a", 1);
  ASSERT_THROW(object.AddString("a", "x"), ParquetException);
}

TEST(TestVariantBuilder, UsesMemoryPool) {
  ProxyMemoryPool pool(default_memory_pool());
  VariantBuilder builder({.pool = &pool});
  builder.SetString(std::string(128, 'x'));
  auto encoded = builder.Finish();
  ASSERT_GT(pool.total_bytes_allocated(), 0);

  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  VariantValueView::Validate(std::string_view{*encoded.value}, metadata);
}

TEST(TestVariantBuilder, FinishValidates) {
  const std::string invalid_value(1, '\x54');

  VariantBuilder builder1;
  builder1.AddFieldName("prefilled");
  ASSERT_THROW(builder1.Finish(), ParquetException);
  ASSERT_EQ(1, builder1.AddFieldName("next"));
  builder1.SetEncodedValue(invalid_value);
  ASSERT_THROW(builder1.Finish(), ParquetInvalidOrCorruptedFileException);
  builder1.SetInt8(1);
  auto primitive = builder1.Finish();
  ASSERT_EQ("\x01\x00\x00"sv, std::string_view{*primitive.metadata});

  // Invalid case, skip validation
  VariantBuilder builder2({.validate_on_finish = false});
  builder2.SetEncodedValue(invalid_value);
  auto encoded = builder2.Finish();
  ASSERT_EQ("\x01\x00\x00"sv, std::string_view{*encoded.metadata});
  ASSERT_EQ(invalid_value, std::string_view{*encoded.value});
}

TEST(TestVariantBuilder, EmptyEncodedValue) {
  VariantBuilder builder;
  ASSERT_THROW(builder.SetEncodedValue(""), ParquetException);
  VariantValueArrayBuilder value_builder;
  ASSERT_THROW(value_builder.AppendEncodedValue(""), ParquetException);
}

TEST(TestVariantBuilder, ArrayBuilder) {
  ProxyMemoryPool pool(default_memory_pool());
  VariantArrayBuilder builder(&pool);
  ASSERT_THROW(builder.Reserve(-1), ParquetException);
  builder.Reserve(8);
  const auto num_allocations = pool.num_allocations();

  builder.AppendNull();
  ASSERT_THROW(builder.Reserve(std::numeric_limits<int64_t>::max()), ParquetException);
  builder.Reserve(7);
  ASSERT_EQ(num_allocations, pool.num_allocations());
  builder.AppendVariantNull();
  builder.AppendInt32(42);
  builder.AppendInt8(1);
  builder.AppendDouble(2);
  builder.AppendBinary("abc");
  builder.AppendTimestampMicros(3, true);
  auto object = builder.StartObject();
  object.AddString("a", "x");
  std::move(object).Finish();

  auto array = builder.Finish();
  ASSERT_EQ(8, array->length());
  ASSERT_TRUE(array->IsNull(0));
  ASSERT_FALSE(array->IsNull(1));
  ASSERT_FALSE(array->is_shredded());

  const auto& storage =
      ::arrow::internal::checked_cast<const StructArray&>(*array->storage());
  ASSERT_FALSE(storage.type()->field(0)->nullable());
  ASSERT_FALSE(storage.type()->field(1)->nullable());
  ASSERT_EQ(Type::BINARY_VIEW, storage.field(0)->type_id());
  ASSERT_EQ(Type::BINARY_VIEW, storage.field(1)->type_id());
}

TEST(TestVariantBuilder, ArrayBuilderFailure) {
  ProxyMemoryPool pool(default_memory_pool());
  ::arrow::CappedMemoryPool capped_pool(&pool, 4096);
  VariantArrayBuilder builder(&capped_pool);
  {
    auto object = builder.StartObject();
    object.AddInt32("before", 1);
    std::move(object).Finish();
  }
  {
    auto object = builder.StartObject();
    object.AddInt32(std::string(16384, 'x'), 2);
    ASSERT_THROW(std::move(object).Finish(), ParquetException);
  }
  {
    auto object = builder.StartObject();
    object.AddInt32("after", 3);
    std::move(object).Finish();
  }

  auto array = builder.Finish();
  ASSERT_EQ(2, array->length());
  const auto& metadata_array =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->metadata());
  const auto& values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->value());
  const std::array expected_names = {"before", "after"};
  for (int64_t row = 0; row < array->length(); ++row) {
    auto metadata = VariantMetadataView::Make(metadata_array.GetView(row));
    ASSERT_EQ(1, metadata.dictionary_size());
    ASSERT_EQ(expected_names[row], metadata.string(0));
    auto value = VariantValueView::MakeWithValidate(values.GetView(row), metadata);
    const auto& fields = std::get<VariantObjectView>(value.data()).fields();
    ASSERT_EQ(1, fields.size());
    ASSERT_EQ(0, fields[0].field_id);
  }
}

TEST(TestVariantBuilder, ArrayBuilderCommitFailure) {
  VariantBuilder encoded_builder;
  {
    auto object = encoded_builder.StartObject();
    object.AddString("original_field", std::string(16, 'x'));
    std::move(object).Finish();
  }
  auto encoded = encoded_builder.Finish();

  ProxyMemoryPool pool(default_memory_pool());
  constexpr int64_t kMemoryLimit = 4096;
  ::arrow::CappedMemoryPool capped_pool(&pool, kMemoryLimit);
  VariantArrayBuilder builder(&capped_pool);
  builder.Reserve(4);
  builder.AppendEncoded(encoded);
  builder.AppendNull();
  for (int i = 0; i < 2; ++i) {
    auto object = builder.StartObject();
    object.AddInt32("a", i);
    std::move(object).Finish();
  }

  {
    auto object = builder.StartObject();
    object.AddInt32("failed", 42);
    // The warmed 64-byte arenas fit the metadata and value, including the header.
    // Four views fill the reserved capacity. Exhaust the pool so dictionary encoding
    // succeeds, but appending the fifth metadata view cannot grow its buffer.
    ASSERT_OK_AND_ASSIGN(
        auto occupied,
        ::arrow::AllocateBuffer(kMemoryLimit - pool.bytes_allocated(), &capped_pool));
    ASSERT_EQ(kMemoryLimit, pool.bytes_allocated());
    ASSERT_THROW(std::move(object).Finish(), ParquetException);
  }
  {
    auto object = builder.StartObject();
    object.AddInt32("next", 43);
    std::move(object).Finish();
  }
  builder.AppendNull();

  auto array = builder.Finish();
  ASSERT_EQ(6, array->length());
  for (int64_t row = 0; row < array->length(); ++row) {
    ASSERT_EQ(row == 1 || row == 5, array->IsNull(row));
  }
  internal::AssertEncodedRow(*array, 0, encoded);

  const std::array expected_values = {0, 1, 43};
  for (int64_t row : {2, 3, 4}) {
    VariantBuilder expected;
    auto object = expected.StartObject();
    object.AddInt32(row == 4 ? "next" : "a", expected_values[row - 2]);
    std::move(object).Finish();
    internal::AssertEncodedRow(*array, row, expected.Finish());
  }
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
  const auto& metadata_values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->metadata());
  const auto& values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->value());
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
  value.SetString("x");
  auto short_encoded = value.Finish();

  VariantArrayBuilder builder;
  builder.AppendEncoded(short_encoded);
  auto object = builder.StartObject();
  object.AddString("abcdefghijklm", std::string(32, 'y'));
  std::move(object).Finish();

  auto array = builder.Finish();
  const auto& metadata =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->metadata());
  const auto& values =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->value());

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
  encoded_builder.SetString("hello");
  auto encoded = encoded_builder.Finish();

  VariantValueArrayBuilder builder;
  builder.AppendNull();
  builder.AppendEncodedValue(std::string_view{*encoded.value});
  builder.AppendInt32(42);
  builder.AppendString(std::string(32, 'x'));

  auto array = builder.Finish();
  ASSERT_EQ(4, array->length());
  ASSERT_TRUE(array->IsNull(0));
  AssertPrimitiveType(array->GetView(1), metadata, VariantPrimitiveType::kString);
  AssertPrimitiveType(array->GetView(2), metadata, VariantPrimitiveType::kInt32);
  AssertPrimitiveType(array->GetView(3), metadata, VariantPrimitiveType::kString);

  builder.AppendBoolean(true);
  auto reused = builder.Finish();
  ASSERT_EQ(1, reused->length());
  AssertPrimitiveType(reused->GetView(0), metadata, VariantPrimitiveType::kBooleanTrue);
}

TEST(TestVariantBuilder, SharedMetadata) {
  constexpr std::string_view kId = "customer_identifier";
  constexpr std::string_view kName = "customer_name";

  MetadataBuilder metadata_builder;
  metadata_builder.TryUpsert(kId);
  metadata_builder.TryUpsert(kName);
  VariantValueArrayBuilder value_builder;
  {
    auto object = value_builder.StartObject(metadata_builder);
    object.AddInt32(kId, 1);
    std::move(object).Finish();
  }
  {
    auto object = value_builder.StartObject(metadata_builder);
    object.AddString(kName, "Alice");
    std::move(object).Finish();
  }
  auto values = value_builder.Finish();

  ASSERT_EQ(2, metadata_builder.size());
  auto metadata_encoded = metadata_builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*metadata_encoded});
  ASSERT_EQ(0, metadata_builder.size());

  ASSERT_OK_AND_ASSIGN(
      auto metadata_values,
      MakeArrayFromScalar(BinaryViewScalar(metadata_encoded), values->length()));
  auto storage_type = struct_({field("metadata", binary_view(), /*nullable=*/false),
                               field("value", binary_view(), /*nullable=*/false)});
  auto array = MakeVariantArrayFromChildren(
      std::move(storage_type), {std::move(metadata_values), std::move(values)});

  ASSERT_EQ(2, array->length());
  const auto& metadata_array =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->metadata());
  const auto& value_array =
      ::arrow::internal::checked_cast<const BinaryViewArray&>(*array->value());
  ASSERT_EQ(std::string_view{*metadata_encoded}, metadata_array.GetView(0));
  ASSERT_EQ(metadata_array.GetView(0), metadata_array.GetView(1));
  ASSERT_EQ(metadata_encoded, metadata_array.data()->buffers[2]);

  auto first = VariantValueView::Make(value_array.GetView(0), metadata);
  AssertPrimitiveFieldType(std::get<VariantObjectView>(first.data()), kId,
                           VariantPrimitiveType::kInt32);

  auto second = VariantValueView::Make(value_array.GetView(1), metadata);
  AssertPrimitiveFieldType(std::get<VariantObjectView>(second.data()), kName,
                           VariantPrimitiveType::kString);
}

TEST(TestVariantBuilder, ValueArrayBuilderWritableMetadata) {
  MetadataBuilder dictionary;
  ASSERT_EQ(0, dictionary.TryUpsert("keep"));
  VariantValueArrayBuilder builder;
  {
    auto object = builder.StartObject(dictionary);
    object.AddInt32("first", 1);
    std::move(object).Finish();
  }
  ASSERT_EQ(2, dictionary.size());
  {
    {
      auto dropped = builder.StartObject(dictionary);
      dropped.AddInt32("abandoned", 2);
    }
    auto list = builder.StartList(dictionary);
    auto object = list.StartObject();
    object.AddInt32("second", 2);
    std::move(object).Finish();
    std::move(list).Finish();
  }
  ASSERT_EQ(3, dictionary.size());
  ASSERT_EQ(2, dictionary.TryUpsert("second"));
  {
    auto dropped = builder.StartList(dictionary);
    auto child = dropped.StartObject();
    child.AddInt32("discarded", 3);
    std::move(child).Finish();
  }
  ASSERT_EQ(3, dictionary.size());
  auto values = builder.Finish();
  ASSERT_EQ(2, values->length());
  auto encoded_metadata = dictionary.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded_metadata});
  ASSERT_EQ(3, metadata.dictionary_size());
  ASSERT_FALSE(metadata.FindString("abandoned").has_value());
  ASSERT_FALSE(metadata.FindString("discarded").has_value());
  for (int64_t row = 0; row < values->length(); ++row) {
    VariantValueView::Validate(values->GetView(row), metadata);
  }
  auto first = VariantValueView::Make(values->GetView(0), metadata);
  ASSERT_EQ(1, std::get<VariantObjectView>(first.data()).fields()[0].field_id);
  auto second = VariantValueView::Make(values->GetView(1), metadata);
  auto child = std::get<VariantArrayView>(second.data()).GetElement(0);
  ASSERT_TRUE(child.has_value());
  ASSERT_EQ(2, std::get<VariantObjectView>(child->data()).fields()[0].field_id);
}

TEST(TestVariantBuilder, ValueArrayBuilderCommitFailure) {
  MetadataBuilder dictionary;
  dictionary.TryUpsert("keep");
  ProxyMemoryPool pool(default_memory_pool());
  constexpr int64_t kMemoryLimit = 1024;
  ::arrow::CappedMemoryPool capped_pool(&pool, kMemoryLimit);
  VariantValueArrayBuilder builder(&capped_pool);
  for (int i = 0; i < 4; ++i) {
    builder.AppendInt32(i);
  }
  {
    auto object = builder.StartObject(dictionary);
    object.AddInt32("discarded", 1);
    // The value fits the existing arena, but the fifth view needs more capacity.
    ASSERT_OK_AND_ASSIGN(
        auto occupied,
        ::arrow::AllocateBuffer(kMemoryLimit - pool.bytes_allocated(), &capped_pool));
    ASSERT_THROW(std::move(object).Finish(), ParquetException);
  }
  ASSERT_EQ(1, dictionary.size());
  ASSERT_EQ(0, dictionary.TryUpsert("keep"));
  {
    auto object = builder.StartObject(dictionary);
    object.AddInt32("retained", 42);
    std::move(object).Finish();
  }
  ASSERT_EQ(2, dictionary.size());
  ASSERT_EQ(1, dictionary.TryUpsert("retained"));
  auto values = builder.Finish();
  ASSERT_EQ(5, values->length());
  ASSERT_EQ(0, values->null_count());
  auto encoded_metadata = dictionary.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded_metadata});
  for (int i = 0; i < 4; ++i) {
    VariantBuilder expected;
    expected.SetInt32(i);
    ASSERT_EQ(std::string_view{*expected.Finish().value}, values->GetView(i));
  }
  auto value = VariantValueView::MakeWithValidate(values->GetView(4), metadata);
  const auto& fields = std::get<VariantObjectView>(value.data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("retained", fields[0].name);
  ASSERT_EQ(1, fields[0].field_id);
}

TEST(TestVariantBuilder, ValueArrayBuilderMoveRollback) {
  MetadataBuilder metadata;
  MetadataBuilder replacement_metadata;
  VariantValueArrayBuilder replacement_builder;
  VariantValueArrayBuilder builder;
  auto object = builder.StartObject(metadata);
  object.AddInt32("drop", 1);
  auto replacement = replacement_builder.StartObject(replacement_metadata);
  object = std::move(replacement);
  ASSERT_EQ(0, metadata.size());

  auto restored = builder.StartList(metadata);
  restored.AppendString(std::string(32, 'x'));
  std::move(restored).Finish();
  auto values = builder.Finish();
  ASSERT_EQ(1, values->length());
  ASSERT_EQ(values->GetView(0).size(), values->data()->buffers[2]->size());

  object.AddInt32("keep", 1);
  std::move(object).Finish();
  ASSERT_EQ(1, replacement_metadata.size());
  ASSERT_EQ(1, replacement_builder.Finish()->length());
}

TEST(TestVariantBuilder, ValueArrayBuilderObject) {
  MetadataBuilder metadata_builder;
  metadata_builder.TryUpsert("a");
  metadata_builder.TryUpsert("b");
  auto metadata_encoded = metadata_builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*metadata_encoded});

  VariantValueArrayBuilder builder;
  {
    auto object = builder.StartObject(metadata);
    object.AddInt32("a", 42);
    auto list = object.StartList("b");
    list.AppendString("x");
    std::move(list).Finish();
    std::move(object).Finish();
  }
  metadata_builder.TryUpsert("other");
  auto second_encoded = metadata_builder.Finish();
  auto second_metadata = VariantMetadataView::Make(std::string_view{*second_encoded});
  {
    auto list = builder.StartList(second_metadata);
    auto object = list.StartObject();
    object.AddBoolean("other", true);
    std::move(object).Finish();
    std::move(list).Finish();
  }

  auto array = builder.Finish();
  ASSERT_EQ(2, array->length());
  auto view = VariantValueView::Make(array->GetView(0), metadata);
  ASSERT_EQ(VariantBasicType::kObject, view.basic_type());
  const auto& object_view = std::get<VariantObjectView>(view.data());
  ASSERT_EQ(2, object_view.fields().size());
  AssertPrimitiveFieldType(object_view, "a", VariantPrimitiveType::kInt32);
  auto list_view = object_view.GetField("b");
  ASSERT_TRUE(list_view.has_value());
  ASSERT_EQ(VariantBasicType::kArray, list_view->basic_type());
  auto second = VariantValueView::MakeWithValidate(array->GetView(1), second_metadata);
  auto child = std::get<VariantArrayView>(second.data()).GetElement(0);
  ASSERT_TRUE(child.has_value());
  const auto& fields = std::get<VariantObjectView>(child->data()).fields();
  ASSERT_EQ(1, fields.size());
  ASSERT_EQ("other", fields[0].name);
  ASSERT_EQ(0, fields[0].field_id);
}

TEST(TestVariantBuilder, ValueArrayBuilderMissingField) {
  auto [metadata_owner, metadata] = MakeEmptyMetadata();

  VariantValueArrayBuilder builder;
  auto object = builder.StartObject(metadata);
  ASSERT_THROW(object.AddInt32("missing", 1), ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantBuilder, FromStorage) {
  VariantBuilder value;
  value.SetInt32(1);
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
  ASSERT_FALSE(array->is_shredded());
}

TEST(TestVariantBuilder, FromShredded) {
  VariantBuilder value;
  value.SetVariantNull();
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
  ASSERT_TRUE(array->is_shredded());
}

}  // namespace parquet::variant
