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

#include "parquet/variant/encoding.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/test_util_internal.h"

#include <array>
#include <string>
#include <string_view>

#include "arrow/testing/gtest_util.h"

namespace parquet::variant {

TEST(TestVariantEncoding, EmptyMetadata) {
  VariantBuilder builder;
  ASSERT_OK(builder.AppendVariantNull());
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_EQ(std::string_view("\x01\x00\x00", 3), std::string_view{*encoded.metadata});

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));
  ASSERT_FALSE(metadata.sorted_strings());
  ASSERT_EQ(0, metadata.dictionary_size());
}

TEST(TestVariantEncoding, Primitive) {
  std::array<char, 16> decimal16 = {0};
  std::array<char, 16> uuid = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

  auto check = [](auto append, VariantPrimitiveType expected) {
    VariantBuilder builder;
    ASSERT_OK(append(builder));
    ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto view, internal::MakeVariantValueView(encoded));
    ASSERT_EQ(VariantBasicType::kPrimitive, view.basic_type());
    ASSERT_EQ(expected, std::get<VariantPrimitiveView>(view.data()).type());
  };

  check([](VariantBuilder& b) { return b.AppendVariantNull(); },
        VariantPrimitiveType::kNull);
  check([](VariantBuilder& b) { return b.AppendBoolean(true); },
        VariantPrimitiveType::kBooleanTrue);
  check([](VariantBuilder& b) { return b.AppendInt8(1); }, VariantPrimitiveType::kInt8);
  check([](VariantBuilder& b) { return b.AppendInt16(2); }, VariantPrimitiveType::kInt16);
  check([](VariantBuilder& b) { return b.AppendInt32(3); }, VariantPrimitiveType::kInt32);
  check([](VariantBuilder& b) { return b.AppendInt64(4); }, VariantPrimitiveType::kInt64);
  check([](VariantBuilder& b) { return b.AppendFloat(1.5F); },
        VariantPrimitiveType::kFloat);
  check([](VariantBuilder& b) { return b.AppendDouble(2.5); },
        VariantPrimitiveType::kDouble);
  check([](VariantBuilder& b) { return b.AppendDecimal4(123, 2); },
        VariantPrimitiveType::kDecimal4);
  check([](VariantBuilder& b) { return b.AppendDecimal8(123, 2); },
        VariantPrimitiveType::kDecimal8);
  check(
      [&](VariantBuilder& b) {
        return b.AppendDecimal16(std::string_view(decimal16.data(), decimal16.size()), 2);
      },
      VariantPrimitiveType::kDecimal16);
  check([](VariantBuilder& b) { return b.AppendBinary("abc"); },
        VariantPrimitiveType::kBinary);
  check([](VariantBuilder& b) { return b.AppendString("abc"); },
        VariantPrimitiveType::kString);
  check([](VariantBuilder& b) { return b.AppendDate(1); }, VariantPrimitiveType::kDate);
  check([](VariantBuilder& b) { return b.AppendTimeNTZMicros(1); },
        VariantPrimitiveType::kTimeNTZMicros);
  check([](VariantBuilder& b) { return b.AppendTimestampMicros(1, true); },
        VariantPrimitiveType::kTimestampMicros);
  check([](VariantBuilder& b) { return b.AppendTimestampMicros(1, false); },
        VariantPrimitiveType::kTimestampNTZMicros);
  check([](VariantBuilder& b) { return b.AppendTimestampNanos(1, true); },
        VariantPrimitiveType::kTimestampNanos);
  check([](VariantBuilder& b) { return b.AppendTimestampNanos(1, false); },
        VariantPrimitiveType::kTimestampNTZNanos);
  check(
      [&](VariantBuilder& b) {
        return b.AppendUuid(std::string_view(uuid.data(), uuid.size()));
      },
      VariantPrimitiveType::kUuid);
}

TEST(TestVariantEncoding, ShortString) {
  VariantBuilder builder;
  ASSERT_OK(builder.AppendShortString("abc"));
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto view, internal::MakeVariantValueView(encoded));
  ASSERT_EQ(VariantBasicType::kShortString, view.basic_type());
  ASSERT_EQ("abc", std::get<VariantShortStringView>(view.data()).string());
}

TEST(TestVariantEncoding, InvalidMetadata) {
  // Metadata version is 2 instead of 1.
  ASSERT_RAISES(Invalid, VariantMetadataView::Make(std::string("\x02\x00\x00", 3)));
  // Dictionary offsets are truncated.
  ASSERT_RAISES(Invalid, VariantMetadataView::Make(std::string("\x01\x01\x00", 3)));
  // Dictionary string payload is not valid UTF-8.
  ASSERT_RAISES(Invalid,
                VariantMetadataView::Make(std::string("\x01\x01\x00\x01\xff", 5)));
}

TEST(TestVariantEncoding, ReservedBits) {
  // Metadata bytes:
  // - 0x21: version 1 with reserved bit 0x20 set
  // - 0x00: zero dictionary entries, encoded with one-byte width
  // - 0x00: first and final dictionary byte offset
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string("\x21\x00\x00", 3)));
  ASSERT_EQ(0, metadata.dictionary_size());

  // Object value bytes:
  // - 0x82: basic type Object with reserved header bit 0x20 set
  // - 0x00: zero fields
  // - 0x00: first and final value offset
  ASSERT_OK(VariantValueView::Validate(std::string("\x82\x00\x00", 3), metadata));
  // Array value bytes:
  // - 0xe3: basic type Array with reserved header bits 0x38 set
  // - 0x00: zero elements
  // - 0x00: first and final value offset
  ASSERT_OK(VariantValueView::Validate(std::string("\xe3\x00\x00", 3), metadata));
}

TEST(TestVariantEncoding, ValidateValue) {
  VariantBuilder builder;
  ASSERT_OK(builder.AppendVariantNull());
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));

  ASSERT_OK(VariantValueView::Validate(std::string_view{*encoded.value}, metadata));
  std::string invalid_value(std::string_view{*encoded.value});
  invalid_value.push_back('\0');
  ASSERT_RAISES(Invalid, VariantValueView::Validate(invalid_value, metadata));
}

TEST(TestVariantEncoding, InvalidValue) {
  VariantBuilder builder;
  ASSERT_OK(builder.AppendVariantNull());
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));

  // Header byte 0x54 encodes Primitive with primitive tag 21, which is unknown.
  ASSERT_RAISES(
      Invalid, VariantValueView::Make(std::string(1, static_cast<char>(0x54)), metadata));
  // Short string payload is not valid UTF-8.
  ASSERT_RAISES(Invalid, VariantValueView::Make(std::string("\x05\xff", 2), metadata));
  // Null primitive has trailing bytes after the encoded value.
  ASSERT_RAISES(Invalid, VariantValueView::Make(std::string("\x00\x00", 2), metadata));
  // Object references field id 0, but the metadata dictionary is empty.
  ASSERT_RAISES(Invalid, VariantValueView::Make(
                             std::string("\x02\x01\x00\x00\x01\x00", 6), metadata));
}

TEST(TestVariantEncoding, InvalidObject) {
  VariantBuilder builder;
  ASSERT_OK_AND_ASSIGN(auto object, builder.StartObject());
  ASSERT_OK(object.AppendVariantNull("a"));
  ASSERT_OK(object.AppendVariantNull("b"));
  ASSERT_OK(object.Finish());
  ASSERT_OK_AND_ASSIGN(auto encoded, builder.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       VariantMetadataView::Make(std::string_view{*encoded.metadata}));

  // Object value bytes:
  // - 0x02: basic type Object, one-byte field ids, one-byte offsets, one-byte count
  // - 0x02: two fields
  // - 0x00 0x01: field ids for "a" and "b"
  // - 0x00 0x00 0x01: duplicate field start offsets 0/0, final value size 1
  // - 0x00: encoded Variant null primitive value
  const std::string duplicate_offsets("\x02\x02\x00\x01\x00\x00\x01\x00", 8);
  ASSERT_RAISES(Invalid, VariantValueView::Validate(duplicate_offsets, metadata));
}

}  // namespace parquet::variant
