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

#include "parquet/variant/decoding.h"
#include "parquet/variant/builder.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/util/decimal.h"
#include "parquet/exception.h"

using namespace std::string_view_literals;  // NOLINT

namespace parquet::variant {

template <typename Metadata>
concept CanMakeVariantValueView = (requires(std::string_view value, Metadata&& metadata) {
  VariantValueView::Make(value, std::forward<Metadata>(metadata));
});

static_assert(CanMakeVariantValueView<const VariantMetadataView&>);
static_assert(!CanMakeVariantValueView<VariantMetadataView>);

template <typename Metadata>
concept CanMakeValidatedVariantValueView =
    (requires(std::string_view value, Metadata&& metadata) {
      VariantValueView::MakeWithValidate(value, std::forward<Metadata>(metadata));
    });

static_assert(CanMakeValidatedVariantValueView<const VariantMetadataView&>);
static_assert(!CanMakeValidatedVariantValueView<VariantMetadataView>);

TEST(TestVariantEncoding, EmptyMetadata) {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto encoded = builder.Finish();
  ASSERT_EQ("\x01\x00\x00"sv, std::string_view{*encoded.metadata});

  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  ASSERT_FALSE(metadata.sorted_strings());
  ASSERT_EQ(0, metadata.dictionary_size());
}

TEST(TestVariantEncoding, MetadataPrefix) {
  constexpr auto encoded = "\x01\x00\x00\x00"sv;
  size_t metadata_size;
  auto metadata = VariantMetadataView::ParsePrefix(encoded, &metadata_size);
  ASSERT_EQ(3, metadata_size);
  ASSERT_EQ("\x01\x00\x00"sv, metadata.metadata());
  ASSERT_THROW(VariantMetadataView::Make(encoded),
               ParquetInvalidOrCorruptedFileException);
  ASSERT_NO_THROW(VariantValueView::Validate(encoded.substr(metadata_size), metadata));
}

TEST(TestVariantEncoding, Primitive) {
  std::array<char, 16> uuid = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

  auto check = [](auto append, VariantPrimitiveType expected) {
    VariantBuilder builder;
    append(builder);
    auto encoded = builder.Finish();
    auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
    auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
    ASSERT_EQ(VariantBasicType::kPrimitive, view.basic_type());
    ASSERT_EQ(expected, std::get<VariantPrimitiveView>(view.data()).type());
  };

  check([](VariantBuilder& b) { b.AppendVariantNull(); }, VariantPrimitiveType::kNull);
  check([](VariantBuilder& b) { b.AppendBoolean(true); },
        VariantPrimitiveType::kBooleanTrue);
  check([](VariantBuilder& b) { b.AppendInt8(1); }, VariantPrimitiveType::kInt8);
  check([](VariantBuilder& b) { b.AppendInt16(2); }, VariantPrimitiveType::kInt16);
  check([](VariantBuilder& b) { b.AppendInt32(3); }, VariantPrimitiveType::kInt32);
  check([](VariantBuilder& b) { b.AppendInt64(4); }, VariantPrimitiveType::kInt64);
  check([](VariantBuilder& b) { b.AppendFloat(1.5F); }, VariantPrimitiveType::kFloat);
  check([](VariantBuilder& b) { b.AppendDouble(2.5); }, VariantPrimitiveType::kDouble);
  check([](VariantBuilder& b) { b.AppendDecimal4(::arrow::Decimal32(123), 2); },
        VariantPrimitiveType::kDecimal4);
  check([](VariantBuilder& b) { b.AppendDecimal8(::arrow::Decimal64(123), 2); },
        VariantPrimitiveType::kDecimal8);
  check([&](VariantBuilder& b) { b.AppendDecimal16(::arrow::Decimal128(1, 0), 2); },
        VariantPrimitiveType::kDecimal16);
  check([](VariantBuilder& b) { b.AppendBinary("abc"); }, VariantPrimitiveType::kBinary);
  check([](VariantBuilder& b) { b.AppendString("abc"); }, VariantPrimitiveType::kString);
  check([](VariantBuilder& b) { b.AppendDate(1); }, VariantPrimitiveType::kDate);
  check([](VariantBuilder& b) { b.AppendTimeNTZMicros(1); },
        VariantPrimitiveType::kTimeNTZMicros);
  check([](VariantBuilder& b) { b.AppendTimestampMicros(1, true); },
        VariantPrimitiveType::kTimestampMicros);
  check([](VariantBuilder& b) { b.AppendTimestampMicros(1, false); },
        VariantPrimitiveType::kTimestampNTZMicros);
  check([](VariantBuilder& b) { b.AppendTimestampNanos(1, true); },
        VariantPrimitiveType::kTimestampNanos);
  check([](VariantBuilder& b) { b.AppendTimestampNanos(1, false); },
        VariantPrimitiveType::kTimestampNTZNanos);
  check(
      [&](VariantBuilder& b) {
        b.AppendUuid(std::string_view(uuid.data(), uuid.size()));
      },
      VariantPrimitiveType::kUuid);
}

TEST(TestVariantEncoding, ShortString) {
  VariantBuilder builder;
  builder.AppendShortString("abc");
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto view = VariantValueView::Make(std::string_view{*encoded.value}, metadata);
  ASSERT_EQ(VariantBasicType::kShortString, view.basic_type());
  ASSERT_EQ("abc", std::get<VariantShortStringView>(view.data()).string());
}

TEST(TestVariantEncoding, InvalidMetadata) {
  // Metadata version is 2 instead of 1.
  ASSERT_THROW(VariantMetadataView::Make("\x02\x00\x00"sv),
               ParquetInvalidOrCorruptedFileException);
  // Dictionary offsets are truncated.
  ASSERT_THROW(VariantMetadataView::Make("\x01\x01\x00"sv),
               ParquetInvalidOrCorruptedFileException);
  // Dictionary string payload is not valid UTF-8.
  ASSERT_THROW(VariantMetadataView::Make("\x01\x01\x00\x01\xff"sv),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantEncoding, ReservedBits) {
  // Metadata bytes:
  // - 0x21: version 1 with reserved bit 0x20 set
  // - 0x00: zero dictionary entries, encoded with one-byte width
  // - 0x00: first and final dictionary byte offset
  auto metadata = VariantMetadataView::Make("\x21\x00\x00"sv);
  ASSERT_EQ(0, metadata.dictionary_size());

  // Object value bytes:
  // - 0x82: basic type Object with reserved header bit 0x20 set
  // - 0x00: zero fields
  // - 0x00: first and final value offset
  VariantValueView::Validate("\x82\x00\x00"sv, metadata);
  // Array value bytes:
  // - 0xe3: basic type Array with reserved header bits 0x38 set
  // - 0x00: zero elements
  // - 0x00: first and final value offset
  VariantValueView::Validate("\xe3\x00\x00"sv, metadata);
}

TEST(TestVariantEncoding, PeekBasicType) {
  ASSERT_EQ(VariantBasicType::kPrimitive, VariantValueView::PeekBasicType("\x00"sv));
  ASSERT_EQ(VariantBasicType::kShortString, VariantValueView::PeekBasicType("\x01"sv));
  ASSERT_EQ(VariantBasicType::kObject, VariantValueView::PeekBasicType("\x02"sv));
  ASSERT_EQ(VariantBasicType::kArray, VariantValueView::PeekBasicType("\x03"sv));
  ASSERT_THROW(VariantValueView::PeekBasicType({}),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantEncoding, ValidateValue) {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});

  VariantValueView::Validate(std::string_view{*encoded.value}, metadata);
  std::string invalid_value(std::string_view{*encoded.value});
  invalid_value.push_back('\0');
  ASSERT_THROW(VariantValueView::Validate(invalid_value, metadata),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantEncoding, ChildAccess) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  auto list = object.StartList("items");
  list.AppendInt32(42);
  list.Finish();
  object.Finish();

  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});
  auto root = VariantValueView::Make(std::string_view{*encoded.value}, metadata);

  const auto& object_view = std::get<VariantObjectView>(root.data());
  auto items_by_name = object_view.GetField("items");
  auto items_by_index = object_view.GetField(0);
  ASSERT_TRUE(items_by_name.has_value());
  ASSERT_TRUE(items_by_index.has_value());
  ASSERT_EQ(items_by_name->value(), items_by_index->value());
  ASSERT_FALSE(object_view.GetField("missing").has_value());
  ASSERT_FALSE(object_view.GetField(1).has_value());

  const auto& array = std::get<VariantArrayView>(items_by_name->data());
  auto element = array.GetElement(0);
  ASSERT_TRUE(element.has_value());
  ASSERT_EQ(VariantPrimitiveType::kInt32,
            std::get<VariantPrimitiveView>(element->data()).type());
  ASSERT_FALSE(array.GetElement(1).has_value());
}

TEST(TestVariantEncoding, InvalidValue) {
  VariantBuilder builder;
  builder.AppendVariantNull();
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});

  // Header byte 0x54 encodes Primitive with primitive tag 21, which is unknown.
  ASSERT_THROW(VariantValueView::Make("\x54"sv, metadata),
               ParquetInvalidOrCorruptedFileException);
  // Short string payload is not valid UTF-8.
  ASSERT_THROW(VariantValueView::Make("\x05\xff"sv, metadata),
               ParquetInvalidOrCorruptedFileException);
  // Null primitive has trailing bytes after the encoded value.
  ASSERT_THROW(VariantValueView::Make("\x00\x00"sv, metadata),
               ParquetInvalidOrCorruptedFileException);
  // Object references field id 0, but the metadata dictionary is empty.
  ASSERT_THROW(VariantValueView::Make("\x02\x01\x00\x00\x01\x00"sv, metadata),
               ParquetInvalidOrCorruptedFileException);
  // The outer and inner array offsets are valid, but the grandchild primitive tag is
  // unknown. Shallow parsing accepts the root and rejects the child when accessed.
  constexpr auto invalid_grandchild = "\x03\x01\x00\x05\x03\x01\x00\x01\x54"sv;
  auto root = VariantValueView::Make(invalid_grandchild, metadata);
  const auto& outer = std::get<VariantArrayView>(root.data());
  auto inner_value = outer.GetElement(0);
  ASSERT_TRUE(inner_value.has_value());
  const auto& inner = std::get<VariantArrayView>(inner_value->data());
  ASSERT_THROW(inner.GetElement(0), ParquetInvalidOrCorruptedFileException);
  ASSERT_THROW(VariantValueView::MakeWithValidate(invalid_grandchild, metadata),
               ParquetInvalidOrCorruptedFileException);
}

TEST(TestVariantEncoding, InvalidObject) {
  VariantBuilder builder;
  auto object = builder.StartObject();
  object.AppendVariantNull("a");
  object.AppendVariantNull("b");
  object.Finish();
  auto encoded = builder.Finish();
  auto metadata = VariantMetadataView::Make(std::string_view{*encoded.metadata});

  // Object value bytes:
  // - 0x02: basic type Object, one-byte field ids, one-byte offsets, one-byte count
  // - 0x02: two fields
  // - 0x00 0x01: field ids for "a" and "b"
  // - 0x00 0x00 0x01: duplicate field start offsets 0/0, final value size 1
  // - 0x00: encoded Variant null primitive value
  const auto duplicate_offsets = "\x02\x02\x00\x01\x00\x00\x01\x00"sv;
  ASSERT_THROW(VariantValueView::Validate(duplicate_offsets, metadata),
               ParquetInvalidOrCorruptedFileException);
}

}  // namespace parquet::variant
