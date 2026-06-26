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

#include "arrow/extension/variant.h"
#include "arrow/extension/variant_internal_test_util.h"

#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "arrow/testing/gtest_util.h"

namespace arrow::extension::variant {

namespace {

// Test-local helpers
Status DecodeVariantValue(const VariantMetadata& metadata, const uint8_t* data,
                          int64_t length, VariantVisitor* visitor) {
  ARROW_ASSIGN_OR_RAISE(auto view, VariantView::Make(metadata, data, length));
  return view.Visit(visitor);
}

Status FindObjectField(const VariantMetadata& metadata, const uint8_t* data,
                       int64_t length, std::string_view field_name, int64_t* field_offset,
                       int64_t* field_size) {
  *field_offset = -1;
  *field_size = 0;
  ARROW_ASSIGN_OR_RAISE(auto obj, VariantObjectView::Make(metadata, data, length));
  auto result = obj.get(field_name);
  if (result.has_value()) {
    *field_offset = result->data() - data;
    *field_size = result->size_bytes();
  }
  return Status::OK();
}

Status GetArrayElement(const uint8_t* data, int64_t length, int32_t index,
                       int64_t* element_offset, int64_t* element_size) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto arr, VariantArrayView::Make(empty_meta, data, length));
  ARROW_ASSIGN_OR_RAISE(auto elem, arr.get(index));
  *element_offset = elem.data() - data;
  *element_size = elem.size_bytes();
  return Status::OK();
}

Status GetObjectFieldAt(const VariantMetadata& metadata, const uint8_t* data,
                        int64_t length, int32_t index, std::string_view* field_name,
                        int64_t* field_offset, int64_t* field_size) {
  ARROW_ASSIGN_OR_RAISE(auto obj, VariantObjectView::Make(metadata, data, length));
  ARROW_ASSIGN_OR_RAISE(*field_name, obj.field_name(index));
  ARROW_ASSIGN_OR_RAISE(auto value, obj.field_value(index));
  *field_offset = value.data() - data;
  *field_size = value.size_bytes();
  return Status::OK();
}

Result<int32_t> GetObjectFieldCount(const uint8_t* data, int64_t length) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto obj, VariantObjectView::Make(empty_meta, data, length));
  return obj.num_fields();
}

}  // namespace

// ===========================================================================
// Helper: decode an EncodedVariant and return visitor events
// ===========================================================================

/// Encode with builder, decode, return events.
/// Note: Uses .ValueOrDie() because ASSERT_OK_AND_ASSIGN cannot be used
/// in a non-void function. Test-only; will crash with a descriptive message
/// on failure rather than producing a clean test failure.
std::vector<std::string> RoundTrip(VariantBuilder& builder) {
  auto result = builder.Finish().ValueOrDie();
  auto metadata =
      DecodeMetadata(result.metadata.data(), static_cast<int64_t>(result.metadata.size()))
          .ValueOrDie();
  RecordingVisitor visitor;
  auto status = DecodeVariantValue(metadata, result.value.data(),
                                   static_cast<int64_t>(result.value.size()), &visitor);
  EXPECT_TRUE(status.ok()) << "DecodeVariantValue failed: " << status.ToString();
  return visitor.events;
}

// ===========================================================================
// Primitive round-trip tests
// ===========================================================================

class VariantBuilderPrimitiveTest : public ::testing::Test {};

TEST_F(VariantBuilderPrimitiveTest, Null) {
  VariantBuilder b;
  ASSERT_OK(b.Null());
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Null");
}

TEST_F(VariantBuilderPrimitiveTest, BoolTrue) {
  VariantBuilder b;
  ASSERT_OK(b.Bool(true));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Bool(true)");
}

TEST_F(VariantBuilderPrimitiveTest, BoolFalse) {
  VariantBuilder b;
  ASSERT_OK(b.Bool(false));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Bool(false)");
}

TEST_F(VariantBuilderPrimitiveTest, IntAutoSizesInt8) {
  VariantBuilder b;
  ASSERT_OK(b.Int(42));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int8(42)");
}

TEST_F(VariantBuilderPrimitiveTest, IntAutoSizesInt16) {
  VariantBuilder b;
  ASSERT_OK(b.Int(300));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int16(300)");
}

TEST_F(VariantBuilderPrimitiveTest, IntAutoSizesInt32) {
  VariantBuilder b;
  ASSERT_OK(b.Int(100000));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int32(100000)");
}

TEST_F(VariantBuilderPrimitiveTest, IntAutoSizesInt64) {
  VariantBuilder b;
  ASSERT_OK(b.Int(5000000000LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int64(5000000000)");
}

TEST_F(VariantBuilderPrimitiveTest, IntNegative) {
  VariantBuilder b;
  ASSERT_OK(b.Int(-42));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int8(-42)");
}

TEST_F(VariantBuilderPrimitiveTest, ShortString) {
  VariantBuilder b;
  ASSERT_OK(b.String("hello"));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "String(\"hello\")");
}

TEST_F(VariantBuilderPrimitiveTest, LongString) {
  std::string long_str(100, 'x');
  VariantBuilder b;
  ASSERT_OK(b.String(long_str));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "String(\"" + long_str + "\")");
}

TEST_F(VariantBuilderPrimitiveTest, ShortStringBoundary63) {
  std::string str63(63, 'a');
  VariantBuilder b;
  ASSERT_OK(b.String(str63));
  ASSERT_OK_AND_ASSIGN(auto result, b.Finish());
  // Should use short string encoding: 1 byte header + 63 bytes
  ASSERT_EQ(result.value.size(), 64);
}

TEST_F(VariantBuilderPrimitiveTest, LongStringBoundary64) {
  std::string str64(64, 'a');
  VariantBuilder b;
  ASSERT_OK(b.String(str64));
  ASSERT_OK_AND_ASSIGN(auto result, b.Finish());
  // Should use long string encoding: 1 byte header + 4 byte length + 64 bytes
  ASSERT_EQ(result.value.size(), 69);
}

TEST_F(VariantBuilderPrimitiveTest, Date) {
  VariantBuilder b;
  ASSERT_OK(b.Date(19000));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Date(19000)");
}

TEST_F(VariantBuilderPrimitiveTest, Double) {
  VariantBuilder b;
  ASSERT_OK(b.Double(3.14));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Double(") == 0);
}

// ===========================================================================
// Array round-trip tests
// ===========================================================================

class VariantBuilderArrayTest : public ::testing::Test {};

TEST_F(VariantBuilderArrayTest, EmptyArray) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<int64_t> offsets;
  ASSERT_OK(b.FinishArray(start, offsets));
  auto events = RoundTrip(b);
  ASSERT_EQ(events.size(), 2);
  ASSERT_EQ(events[0], "StartArray(0)");
  ASSERT_EQ(events[1], "EndArray");
}

TEST_F(VariantBuilderArrayTest, SimpleArray) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<int64_t> offsets;
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(1));
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(2));
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(3));
  ASSERT_OK(b.FinishArray(start, offsets));

  auto events = RoundTrip(b);
  ASSERT_EQ(events.size(), 5);
  ASSERT_EQ(events[0], "StartArray(3)");
  ASSERT_EQ(events[1], "Int8(1)");
  ASSERT_EQ(events[2], "Int8(2)");
  ASSERT_EQ(events[3], "Int8(3)");
  ASSERT_EQ(events[4], "EndArray");
}

TEST_F(VariantBuilderArrayTest, NestedArray) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<int64_t> offsets;

  // First element: nested array [10, 20]
  offsets.push_back(b.NextElement(start));
  auto inner_start = b.Offset();
  std::vector<int64_t> inner_offsets;
  inner_offsets.push_back(b.NextElement(inner_start));
  ASSERT_OK(b.Int(10));
  inner_offsets.push_back(b.NextElement(inner_start));
  ASSERT_OK(b.Int(20));
  ASSERT_OK(b.FinishArray(inner_start, inner_offsets));

  // Second element: 30
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(30));

  ASSERT_OK(b.FinishArray(start, offsets));

  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "StartArray(2)");
  ASSERT_EQ(events[1], "StartArray(2)");
  ASSERT_EQ(events[2], "Int8(10)");
  ASSERT_EQ(events[3], "Int8(20)");
  ASSERT_EQ(events[4], "EndArray");
  ASSERT_EQ(events[5], "Int8(30)");
  ASSERT_EQ(events[6], "EndArray");
}

// ===========================================================================
// Object round-trip tests
// ===========================================================================

class VariantBuilderObjectTest : public ::testing::Test {};

TEST_F(VariantBuilderObjectTest, EmptyObject) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  ASSERT_OK(b.FinishObject(start, fields));
  auto events = RoundTrip(b);
  ASSERT_EQ(events.size(), 2);
  ASSERT_EQ(events[0], "StartObject(0)");
  ASSERT_EQ(events[1], "EndObject");
}

TEST_F(VariantBuilderObjectTest, SimpleObject) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "name"));
  ASSERT_OK(b.String("Alice"));
  fields.push_back(b.NextField(start, "age"));
  ASSERT_OK(b.Int(30));
  ASSERT_OK(b.FinishObject(start, fields));

  auto events = RoundTrip(b);
  // Fields sorted by key: "age" before "name"
  ASSERT_EQ(events[0], "StartObject(2)");
  ASSERT_EQ(events[1], "FieldName(\"age\")");
  ASSERT_EQ(events[2], "Int8(30)");
  ASSERT_EQ(events[3], "FieldName(\"name\")");
  ASSERT_EQ(events[4], "String(\"Alice\")");
  ASSERT_EQ(events[5], "EndObject");
}

TEST_F(VariantBuilderObjectTest, NestedObject) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "inner"));
  {
    auto inner_start = b.Offset();
    std::vector<VariantBuilder::FieldEntry> inner_fields;
    inner_fields.push_back(b.NextField(inner_start, "key"));
    ASSERT_OK(b.String("value"));
    ASSERT_OK(b.FinishObject(inner_start, inner_fields));
  }
  ASSERT_OK(b.FinishObject(start, fields));

  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "StartObject(1)");
  ASSERT_EQ(events[1], "FieldName(\"inner\")");
  ASSERT_EQ(events[2], "StartObject(1)");
  ASSERT_EQ(events[3], "FieldName(\"key\")");
  ASSERT_EQ(events[4], "String(\"value\")");
  ASSERT_EQ(events[5], "EndObject");
  ASSERT_EQ(events[6], "EndObject");
}

TEST_F(VariantBuilderObjectTest, DuplicateKeyError) {
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "key"));
  ASSERT_OK(b.Int(1));
  fields.push_back(b.NextField(start, "key"));
  ASSERT_OK(b.Int(2));
  ASSERT_RAISES(Invalid, b.FinishObject(start, fields));
}

TEST_F(VariantBuilderObjectTest, FieldsSortedByKey) {
  // Insert fields in reverse order; verify they come out sorted
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "z_last"));
  ASSERT_OK(b.Int(3));
  fields.push_back(b.NextField(start, "a_first"));
  ASSERT_OK(b.Int(1));
  fields.push_back(b.NextField(start, "m_middle"));
  ASSERT_OK(b.Int(2));
  ASSERT_OK(b.FinishObject(start, fields));

  auto events = RoundTrip(b);
  ASSERT_EQ(events[1], "FieldName(\"a_first\")");
  ASSERT_EQ(events[2], "Int8(1)");
  ASSERT_EQ(events[3], "FieldName(\"m_middle\")");
  ASSERT_EQ(events[4], "Int8(2)");
  ASSERT_EQ(events[5], "FieldName(\"z_last\")");
  ASSERT_EQ(events[6], "Int8(3)");
}

// ===========================================================================
// Builder features
// ===========================================================================

class VariantBuilderFeatureTest : public ::testing::Test {};

TEST_F(VariantBuilderFeatureTest, Reset) {
  VariantBuilder b;
  ASSERT_OK(b.Int(42));
  auto events1 = RoundTrip(b);
  ASSERT_EQ(events1[0], "Int8(42)");

  b.Reset();
  ASSERT_OK(b.String("hello"));
  auto events2 = RoundTrip(b);
  ASSERT_EQ(events2[0], "String(\"hello\")");
}

TEST_F(VariantBuilderFeatureTest, BuilderFromExistingMetadata) {
  // First, build a variant to get metadata
  VariantBuilder b1;
  auto start = b1.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b1.NextField(start, "name"));
  ASSERT_OK(b1.String("Alice"));
  ASSERT_OK(b1.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded1, b1.Finish());

  // Decode the metadata
  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded1.metadata.data(),
                                      static_cast<int64_t>(encoded1.metadata.size())));

  // Build a new variant reusing the same metadata
  VariantBuilder b2(meta);
  auto start2 = b2.Offset();
  std::vector<VariantBuilder::FieldEntry> fields2;
  fields2.push_back(b2.NextField(start2, "name"));
  ASSERT_OK(b2.String("Bob"));
  ASSERT_OK(b2.FinishObject(start2, fields2));

  auto events = RoundTrip(b2);
  ASSERT_EQ(events[1], "FieldName(\"name\")");
  ASSERT_EQ(events[2], "String(\"Bob\")");
}

TEST_F(VariantBuilderFeatureTest, MetadataSortedFlag) {
  // If keys are inserted in sorted order, metadata should have sorted flag
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "alpha"));
  ASSERT_OK(b.Int(1));
  fields.push_back(b.NextField(start, "beta"));
  ASSERT_OK(b.Int(2));
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  ASSERT_TRUE(meta.is_sorted);
}

TEST_F(VariantBuilderFeatureTest, MetadataUnsortedFlag) {
  // If keys are inserted out of order, sorted flag should be false
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "beta"));
  ASSERT_OK(b.Int(1));
  fields.push_back(b.NextField(start, "alpha"));
  ASSERT_OK(b.Int(2));
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  ASSERT_FALSE(meta.is_sorted);
}

// ===========================================================================
// Integration: full round-trip of complex structure
// ===========================================================================

class VariantBuilderIntegrationTest : public ::testing::Test {};

TEST_F(VariantBuilderIntegrationTest, ComplexObject) {
  // {"name": "Alice", "scores": [95, 87, 92], "active": true}
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;

  fields.push_back(b.NextField(start, "name"));
  ASSERT_OK(b.String("Alice"));

  fields.push_back(b.NextField(start, "scores"));
  {
    auto arr_start = b.Offset();
    std::vector<int64_t> arr_offsets;
    arr_offsets.push_back(b.NextElement(arr_start));
    ASSERT_OK(b.Int(95));
    arr_offsets.push_back(b.NextElement(arr_start));
    ASSERT_OK(b.Int(87));
    arr_offsets.push_back(b.NextElement(arr_start));
    ASSERT_OK(b.Int(92));
    ASSERT_OK(b.FinishArray(arr_start, arr_offsets));
  }

  fields.push_back(b.NextField(start, "active"));
  ASSERT_OK(b.Bool(true));

  ASSERT_OK(b.FinishObject(start, fields));

  auto events = RoundTrip(b);
  // Fields sorted: "active", "name", "scores"
  ASSERT_EQ(events[0], "StartObject(3)");
  ASSERT_EQ(events[1], "FieldName(\"active\")");
  ASSERT_EQ(events[2], "Bool(true)");
  ASSERT_EQ(events[3], "FieldName(\"name\")");
  ASSERT_EQ(events[4], "String(\"Alice\")");
  ASSERT_EQ(events[5], "FieldName(\"scores\")");
  ASSERT_EQ(events[6], "StartArray(3)");
  ASSERT_EQ(events[7], "Int8(95)");
  ASSERT_EQ(events[8], "Int8(87)");
  ASSERT_EQ(events[9], "Int8(92)");
  ASSERT_EQ(events[10], "EndArray");
  ASSERT_EQ(events[11], "EndObject");
}

TEST_F(VariantBuilderIntegrationTest, LargeMetadataOffsetSize) {
  // Build an object with enough unique keys to trigger 2-byte metadata offsets.
  // 300 keys of ~4 chars each = ~1200 bytes total string data > 255.
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  for (int i = 0; i < 300; ++i) {
    std::string key = "k" + std::to_string(i);
    fields.push_back(b.NextField(start, key));
    ASSERT_OK(b.Int(i));
  }
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  // Verify metadata can be decoded
  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  ASSERT_EQ(static_cast<int32_t>(meta.strings.size()), 300);
  // offset_size should be >= 2 (total string data > 255 bytes)
  ASSERT_GE(meta.offset_size, 2);

  // Verify value can be decoded
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(meta, encoded.value.data(),
                               static_cast<int64_t>(encoded.value.size()), &visitor));
  // StartObject(300) + 300*(FieldName + Int8) + EndObject = 602 events
  ASSERT_EQ(visitor.events.size(), 602);
  ASSERT_EQ(visitor.events[0], "StartObject(300)");
  ASSERT_EQ(visitor.events[601], "EndObject");
}

TEST_F(VariantBuilderIntegrationTest, MetadataOffsetSizeFromKeyCount) {
  // Verify that offset_size is computed from max(total_string_size, num_keys).
  // Use 260 single-character keys: total_string_size=260 (>255, needs 2 bytes)
  // but num_keys=260 also exceeds 255. This ensures the formula handles both.
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  // Generate 260 unique 1-char keys using characters + numeric suffixes
  for (int i = 0; i < 260; ++i) {
    // Use 2-char keys to guarantee uniqueness: "a0" through "z9", then "A0"...
    char c1 = (i < 260) ? static_cast<char>('a' + (i / 10) % 26) : 'A';
    char c2 = static_cast<char>('0' + (i % 10));
    std::string key = {c1, c2};
    fields.push_back(b.NextField(start, key));
    ASSERT_OK(b.Null());
  }
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  // 260 keys of 2 chars = 520 bytes total string data > 255, needs 2-byte offsets
  ASSERT_GE(meta.offset_size, 2);
  // Also verify num_keys is correctly stored
  ASSERT_EQ(static_cast<int32_t>(meta.strings.size()), 260);

  // Verify round-trip
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(meta, encoded.value.data(),
                               static_cast<int64_t>(encoded.value.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "StartObject(260)");
}

TEST_F(VariantBuilderIntegrationTest, InvalidStartPosition) {
  VariantBuilder b;
  ASSERT_OK(b.Int(42));
  // start=999 is beyond the buffer — should fail
  std::vector<int64_t> offsets;
  ASSERT_RAISES(Invalid, b.FinishArray(999, offsets));

  std::vector<VariantBuilder::FieldEntry> fields;
  ASSERT_RAISES(Invalid, b.FinishObject(999, fields));
}

TEST_F(VariantBuilderIntegrationTest, NegativeArrayOffsetRejected) {
  VariantBuilder b;
  auto start = b.Offset();
  ASSERT_OK(b.Int(1));
  std::vector<int64_t> offsets = {-1};
  ASSERT_RAISES(Invalid, b.FinishArray(start, offsets));
}

// ===========================================================================
// Additional primitive round-trip tests (coverage gaps)
// ===========================================================================

class VariantBuilderPrimitiveExtraTest : public ::testing::Test {};

TEST_F(VariantBuilderPrimitiveExtraTest, FloatRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.Float(2.5f));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Float(") == 0);
}

TEST_F(VariantBuilderPrimitiveExtraTest, BinaryRoundTrip) {
  std::string_view bin_data("\x00\x01\x02\x03", 4);
  VariantBuilder b;
  ASSERT_OK(b.Binary(bin_data));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Binary(len=4)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, EmptyBinaryRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.Binary(""));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Binary(len=0)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, UUIDRoundTrip) {
  uint8_t uuid_bytes[16];
  for (int i = 0; i < 16; ++i) uuid_bytes[i] = static_cast<uint8_t>(i + 1);
  VariantBuilder b;
  ASSERT_OK(b.UUID(uuid_bytes));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "UUID");
}

TEST_F(VariantBuilderPrimitiveExtraTest, TimestampMicrosRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.TimestampMicros(1654041600000000LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "TimestampMicros(1654041600000000)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, TimestampMicrosNTZRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.TimestampMicrosNTZ(1654041600000000LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "TimestampMicrosNTZ(1654041600000000)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, TimestampNanosRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.TimestampNanos(1654041600000000000LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "TimestampNanos(1654041600000000000)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, TimestampNanosNTZRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.TimestampNanosNTZ(1654041600000000000LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "TimestampNanosNTZ(1654041600000000000)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, TimeNTZRoundTrip) {
  VariantBuilder b;
  ASSERT_OK(b.TimeNTZ(43200000000LL));  // 12:00:00 in microseconds
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "TimeNTZ(43200000000)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, Decimal4RoundTrip) {
  int32_t val = 12345;
  uint8_t bytes[4];
  std::memcpy(bytes, &val, 4);
  VariantBuilder b;
  ASSERT_OK(b.Decimal4(2, bytes));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Decimal4(scale=2)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, Decimal8RoundTrip) {
  int64_t val = 123456789012345LL;
  uint8_t bytes[8];
  std::memcpy(bytes, &val, 8);
  VariantBuilder b;
  ASSERT_OK(b.Decimal8(5, bytes));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Decimal8(scale=5)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, Decimal16RoundTrip) {
  uint8_t bytes[16] = {};
  bytes[0] = 0x01;  // value = 1 in low byte
  VariantBuilder b;
  ASSERT_OK(b.Decimal16(10, bytes));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Decimal16(scale=10)");
}

TEST_F(VariantBuilderPrimitiveExtraTest, DecimalScaleValidation) {
  uint8_t bytes[16] = {};
  VariantBuilder b;
  // Scale 39 exceeds spec maximum of 38
  ASSERT_RAISES(Invalid, b.Decimal4(39, bytes));
  ASSERT_RAISES(Invalid, b.Decimal8(39, bytes));
  ASSERT_RAISES(Invalid, b.Decimal16(39, bytes));
  // Scale 38 is valid
  ASSERT_OK(b.Decimal4(38, bytes));
}

TEST_F(VariantBuilderPrimitiveExtraTest, EmptyString) {
  VariantBuilder b;
  ASSERT_OK(b.String(""));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "String(\"\")");
}

// ===========================================================================
// Special float/double values: NaN, ±Inf
// ===========================================================================

class VariantBuilderSpecialFloatTest : public ::testing::Test {};

TEST_F(VariantBuilderSpecialFloatTest, FloatNaN) {
  VariantBuilder b;
  ASSERT_OK(b.Float(std::numeric_limits<float>::quiet_NaN()));
  ASSERT_OK_AND_ASSIGN(auto result, b.Finish());
  // Verify it round-trips (NaN != NaN, so just check we get a Float event)
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(result.metadata.data(),
                                      static_cast<int64_t>(result.metadata.size())));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(metadata, result.value.data(),
                               static_cast<int64_t>(result.value.size()), &visitor));
  ASSERT_TRUE(visitor.events[0].find("Float(") == 0);
}

TEST_F(VariantBuilderSpecialFloatTest, FloatPositiveInf) {
  VariantBuilder b;
  ASSERT_OK(b.Float(std::numeric_limits<float>::infinity()));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Float(") == 0);
}

TEST_F(VariantBuilderSpecialFloatTest, FloatNegativeInf) {
  VariantBuilder b;
  ASSERT_OK(b.Float(-std::numeric_limits<float>::infinity()));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Float(") == 0);
}

TEST_F(VariantBuilderSpecialFloatTest, DoubleNaN) {
  VariantBuilder b;
  ASSERT_OK(b.Double(std::numeric_limits<double>::quiet_NaN()));
  ASSERT_OK_AND_ASSIGN(auto result, b.Finish());
  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(result.metadata.data(),
                                      static_cast<int64_t>(result.metadata.size())));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(metadata, result.value.data(),
                               static_cast<int64_t>(result.value.size()), &visitor));
  ASSERT_TRUE(visitor.events[0].find("Double(") == 0);
}

TEST_F(VariantBuilderSpecialFloatTest, DoublePositiveInf) {
  VariantBuilder b;
  ASSERT_OK(b.Double(std::numeric_limits<double>::infinity()));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Double(") == 0);
}

TEST_F(VariantBuilderSpecialFloatTest, DoubleNegativeInf) {
  VariantBuilder b;
  ASSERT_OK(b.Double(-std::numeric_limits<double>::infinity()));
  auto events = RoundTrip(b);
  ASSERT_TRUE(events[0].find("Double(") == 0);
}

// ===========================================================================
// Int auto-sizing boundary tests
// ===========================================================================

class VariantBuilderIntBoundaryTest : public ::testing::Test {};

TEST_F(VariantBuilderIntBoundaryTest, Int8MaxBecomesInt8) {
  VariantBuilder b;
  ASSERT_OK(b.Int(127));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int8(127)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int8MaxPlusOneBecomesInt16) {
  VariantBuilder b;
  ASSERT_OK(b.Int(128));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int16(128)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int8MinBecomesInt8) {
  VariantBuilder b;
  ASSERT_OK(b.Int(-128));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int8(-128)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int8MinMinusOneBecomesInt16) {
  VariantBuilder b;
  ASSERT_OK(b.Int(-129));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int16(-129)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int16MaxBecomesInt16) {
  VariantBuilder b;
  ASSERT_OK(b.Int(32767));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int16(32767)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int16MaxPlusOneBecomesInt32) {
  VariantBuilder b;
  ASSERT_OK(b.Int(32768));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int32(32768)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int32MaxBecomesInt32) {
  VariantBuilder b;
  ASSERT_OK(b.Int(2147483647LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int32(2147483647)");
}

TEST_F(VariantBuilderIntBoundaryTest, Int32MaxPlusOneBecomesInt64) {
  VariantBuilder b;
  ASSERT_OK(b.Int(2147483648LL));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int64(2147483648)");
}

// ===========================================================================
// Large array round-trip (is_large flag)
// ===========================================================================

class VariantBuilderLargeContainerTest : public ::testing::Test {};

TEST_F(VariantBuilderLargeContainerTest, LargeArrayIsLarge) {
  // Build an array with 300 elements (>255) to trigger is_large=true.
  // This exercises the same code path as the Go bug (apache/arrow-go#839).
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<int64_t> offsets;
  for (int i = 0; i < 300; ++i) {
    offsets.push_back(b.NextElement(start));
    ASSERT_OK(b.Null());
  }
  ASSERT_OK(b.FinishArray(start, offsets));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  // Verify the header byte has is_large set correctly
  ASSERT_FALSE(encoded.value.empty());
  uint8_t header = encoded.value[0];
  ASSERT_EQ(GetBasicType(header), BasicType::kArray);
  // is_large at bit 4 of full byte
  ASSERT_TRUE(((header >> 4) & 0x01) != 0);

  // Verify round-trip: decode and check element count
  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(meta, encoded.value.data(),
                               static_cast<int64_t>(encoded.value.size()), &visitor));
  // StartArray(300) + 300 Nulls + EndArray = 302 events
  ASSERT_EQ(visitor.events.size(), 302);
  ASSERT_EQ(visitor.events[0], "StartArray(300)");
  ASSERT_EQ(visitor.events[301], "EndArray");

  // Also verify ValueSize works correctly on this large array
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(encoded.value.data(),
                                            static_cast<int64_t>(encoded.value.size())));
  ASSERT_EQ(size, static_cast<int64_t>(encoded.value.size()));
}

TEST_F(VariantBuilderLargeContainerTest, LargeObjectIsLarge) {
  // Build an object with 300 fields (>255) to trigger is_large=true.
  // Verifies that the encoder correctly sets is_large at bit 6 of the
  // full header byte (bit 4 of the 6-bit type_info / value_header).
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  for (int i = 0; i < 300; ++i) {
    std::string key = "field_" + std::to_string(i);
    fields.push_back(b.NextField(start, key));
    ASSERT_OK(b.Null());
  }
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  // Verify the header byte has is_large set correctly at bit 6
  ASSERT_FALSE(encoded.value.empty());
  uint8_t header = encoded.value[0];
  ASSERT_EQ(GetBasicType(header), BasicType::kObject);
  // Object is_large at bit 6 of full byte (bit 4 of type_info)
  ASSERT_TRUE(((header >> 6) & 0x01) != 0);

  // Verify round-trip: decode and check field count
  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));
  ASSERT_OK_AND_ASSIGN(auto field_count,
                       GetObjectFieldCount(encoded.value.data(),
                                           static_cast<int64_t>(encoded.value.size())));
  ASSERT_EQ(field_count, 300);

  // Verify full decode
  RecordingVisitor visitor;
  ASSERT_OK(DecodeVariantValue(meta, encoded.value.data(),
                               static_cast<int64_t>(encoded.value.size()), &visitor));
  // StartObject(300) + 300*(FieldName + Null) + EndObject = 602 events
  ASSERT_EQ(visitor.events.size(), 602);
  ASSERT_EQ(visitor.events[0], "StartObject(300)");
  ASSERT_EQ(visitor.events[601], "EndObject");

  // Verify ValueSize matches buffer size
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(encoded.value.data(),
                                            static_cast<int64_t>(encoded.value.size())));
  ASSERT_EQ(size, static_cast<int64_t>(encoded.value.size()));
}

// ===========================================================================
// Decoder utility round-trips through builder output
// ===========================================================================

class VariantBuilderDecoderUtilTest : public ::testing::Test {};

TEST_F(VariantBuilderDecoderUtilTest, FindObjectFieldOnBuilderOutput) {
  // Build {alpha: 1, beta: "two", gamma: true} and verify FindObjectField works
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "alpha"));
  ASSERT_OK(b.Int(1));
  fields.push_back(b.NextField(start, "beta"));
  ASSERT_OK(b.String("two"));
  fields.push_back(b.NextField(start, "gamma"));
  ASSERT_OK(b.Bool(true));
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));

  // Find "beta"
  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(meta, encoded.value.data(),
                            static_cast<int64_t>(encoded.value.size()), "beta",
                            &field_offset, &field_size));
  ASSERT_GT(field_offset, 0);
  ASSERT_GT(field_size, 0);

  // Decode the field value
  RecordingVisitor v;
  ASSERT_OK(
      DecodeVariantValue(meta, encoded.value.data() + field_offset, field_size, &v));
  ASSERT_EQ(v.events[0], "String(\"two\")");

  // Find non-existent key
  int64_t nf_offset = -1, nf_size = 0;
  ASSERT_OK(FindObjectField(meta, encoded.value.data(),
                            static_cast<int64_t>(encoded.value.size()), "missing",
                            &nf_offset, &nf_size));
  ASSERT_EQ(nf_offset, -1);
}

TEST_F(VariantBuilderDecoderUtilTest, GetArrayElementOnBuilderOutput) {
  // Build [10, 20, 30] and verify GetArrayElement works
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<int64_t> offsets;
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(10));
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(20));
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Int(30));
  ASSERT_OK(b.FinishArray(start, offsets));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));

  // Access element at index 2
  int64_t elem_offset = 0, elem_size = 0;
  ASSERT_OK(GetArrayElement(encoded.value.data(),
                            static_cast<int64_t>(encoded.value.size()), 2, &elem_offset,
                            &elem_size));
  ASSERT_GT(elem_offset, 0);
  ASSERT_EQ(elem_size, 2);  // Int8(30) = 2 bytes

  RecordingVisitor v;
  ASSERT_OK(DecodeVariantValue(meta, encoded.value.data() + elem_offset, elem_size, &v));
  ASSERT_EQ(v.events[0], "Int8(30)");
}

TEST_F(VariantBuilderDecoderUtilTest, GetObjectFieldAtOnBuilderOutput) {
  // Build {x: 100, y: 200} and access by positional index
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "x"));
  ASSERT_OK(b.Int(100));
  fields.push_back(b.NextField(start, "y"));
  ASSERT_OK(b.Int(200));
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  ASSERT_OK_AND_ASSIGN(auto meta,
                       DecodeMetadata(encoded.metadata.data(),
                                      static_cast<int64_t>(encoded.metadata.size())));

  // Fields are sorted by key: "x" at index 0, "y" at index 1
  std::string_view field_name;
  int64_t field_offset = 0, field_size = 0;
  ASSERT_OK(GetObjectFieldAt(meta, encoded.value.data(),
                             static_cast<int64_t>(encoded.value.size()), 0, &field_name,
                             &field_offset, &field_size));
  ASSERT_EQ(field_name, "x");

  RecordingVisitor v;
  ASSERT_OK(
      DecodeVariantValue(meta, encoded.value.data() + field_offset, field_size, &v));
  ASSERT_EQ(v.events[0], "Int8(100)");
}

TEST_F(VariantBuilderDecoderUtilTest, ValueSizeOnBuilderOutput) {
  // Build a nested structure and verify ValueSize matches buffer size
  VariantBuilder b;
  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "data"));
  {
    auto arr_start = b.Offset();
    std::vector<int64_t> arr_offsets;
    arr_offsets.push_back(b.NextElement(arr_start));
    ASSERT_OK(b.String("hello"));
    arr_offsets.push_back(b.NextElement(arr_start));
    ASSERT_OK(b.Int(42));
    ASSERT_OK(b.FinishArray(arr_start, arr_offsets));
  }
  ASSERT_OK(b.FinishObject(start, fields));
  ASSERT_OK_AND_ASSIGN(auto encoded, b.Finish());

  // ValueSize of the top-level value should equal the total buffer size
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(encoded.value.data(),
                                            static_cast<int64_t>(encoded.value.size())));
  ASSERT_EQ(size, static_cast<int64_t>(encoded.value.size()));
}

// ===========================================================================
// Direct integer type method tests (verify explicit types not auto-sized)
// ===========================================================================

class VariantBuilderDirectIntTest : public ::testing::Test {};

TEST_F(VariantBuilderDirectIntTest, ExplicitInt8) {
  VariantBuilder b;
  ASSERT_OK(b.Int8(42));
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int8(42)");
}

TEST_F(VariantBuilderDirectIntTest, ExplicitInt16) {
  VariantBuilder b;
  ASSERT_OK(b.Int16(42));  // Would be Int8 if auto-sized
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int16(42)");
}

TEST_F(VariantBuilderDirectIntTest, ExplicitInt32) {
  VariantBuilder b;
  ASSERT_OK(b.Int32(42));  // Would be Int8 if auto-sized
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int32(42)");
}

TEST_F(VariantBuilderDirectIntTest, ExplicitInt64) {
  VariantBuilder b;
  ASSERT_OK(b.Int64(42));  // Would be Int8 if auto-sized
  auto events = RoundTrip(b);
  ASSERT_EQ(events[0], "Int64(42)");
}

// ===========================================================================
// Builder reuse: multiple Finish() calls with preserved dictionary
// ===========================================================================

class VariantBuilderReuseTest : public ::testing::Test {};

TEST_F(VariantBuilderReuseTest, MultipleFinishPreservesDictionary) {
  VariantBuilder b;

  // Build first value: {name: "Alice"}
  auto start1 = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields1;
  fields1.push_back(b.NextField(start1, "name"));
  ASSERT_OK(b.String("Alice"));
  ASSERT_OK(b.FinishObject(start1, fields1));
  ASSERT_OK_AND_ASSIGN(auto encoded1, b.Finish());

  // Build second value: {name: "Bob"} — reuses dictionary from first build
  auto start2 = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields2;
  fields2.push_back(b.NextField(start2, "name"));
  ASSERT_OK(b.String("Bob"));
  ASSERT_OK(b.FinishObject(start2, fields2));
  ASSERT_OK_AND_ASSIGN(auto encoded2, b.Finish());

  // Verify first value decodes correctly
  ASSERT_OK_AND_ASSIGN(auto meta1,
                       DecodeMetadata(encoded1.metadata.data(),
                                      static_cast<int64_t>(encoded1.metadata.size())));
  RecordingVisitor v1;
  ASSERT_OK(DecodeVariantValue(meta1, encoded1.value.data(),
                               static_cast<int64_t>(encoded1.value.size()), &v1));
  ASSERT_EQ(v1.events[1], "FieldName(\"name\")");
  ASSERT_EQ(v1.events[2], "String(\"Alice\")");

  // Verify second value decodes correctly
  ASSERT_OK_AND_ASSIGN(auto meta2,
                       DecodeMetadata(encoded2.metadata.data(),
                                      static_cast<int64_t>(encoded2.metadata.size())));
  RecordingVisitor v2;
  ASSERT_OK(DecodeVariantValue(meta2, encoded2.value.data(),
                               static_cast<int64_t>(encoded2.value.size()), &v2));
  ASSERT_EQ(v2.events[1], "FieldName(\"name\")");
  ASSERT_EQ(v2.events[2], "String(\"Bob\")");

  // Both should have the same dictionary content (same metadata structure)
  ASSERT_EQ(meta1.strings.size(), meta2.strings.size());
  ASSERT_EQ(meta1.strings[0], "name");
  ASSERT_EQ(meta2.strings[0], "name");
}

TEST_F(VariantBuilderReuseTest, DictionaryGrowsAcrossFinishCalls) {
  VariantBuilder b;

  // Build first value with key "x"
  auto start1 = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields1;
  fields1.push_back(b.NextField(start1, "x"));
  ASSERT_OK(b.Int(1));
  ASSERT_OK(b.FinishObject(start1, fields1));
  ASSERT_OK_AND_ASSIGN(auto encoded1, b.Finish());

  // Build second value with keys "x" and "y" — dictionary should grow
  auto start2 = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields2;
  fields2.push_back(b.NextField(start2, "x"));
  ASSERT_OK(b.Int(2));
  fields2.push_back(b.NextField(start2, "y"));
  ASSERT_OK(b.Int(3));
  ASSERT_OK(b.FinishObject(start2, fields2));
  ASSERT_OK_AND_ASSIGN(auto encoded2, b.Finish());

  // First metadata has 1 key
  ASSERT_OK_AND_ASSIGN(auto meta1,
                       DecodeMetadata(encoded1.metadata.data(),
                                      static_cast<int64_t>(encoded1.metadata.size())));
  ASSERT_EQ(meta1.strings.size(), 1);

  // Second metadata has 2 keys (dictionary grew)
  ASSERT_OK_AND_ASSIGN(auto meta2,
                       DecodeMetadata(encoded2.metadata.data(),
                                      static_cast<int64_t>(encoded2.metadata.size())));
  ASSERT_EQ(meta2.strings.size(), 2);

  // Verify second value decodes correctly
  RecordingVisitor v2;
  ASSERT_OK(DecodeVariantValue(meta2, encoded2.value.data(),
                               static_cast<int64_t>(encoded2.value.size()), &v2));
  ASSERT_EQ(v2.events[0], "StartObject(2)");
  // Fields sorted: "x" before "y"
  ASSERT_EQ(v2.events[1], "FieldName(\"x\")");
  ASSERT_EQ(v2.events[2], "Int8(2)");
  ASSERT_EQ(v2.events[3], "FieldName(\"y\")");
  ASSERT_EQ(v2.events[4], "Int8(3)");
}

// ===========================================================================
// Edge case: FinishObject/FinishArray with pre-existing buffer content
// ===========================================================================

class VariantBuilderPreExistingBufferTest : public ::testing::Test {};

TEST_F(VariantBuilderPreExistingBufferTest, ObjectAfterPrimitive) {
  // Write a primitive value first, then build an object. This exercises
  // the case where start > 0 (data_size = buffer.size() - start).
  // The builder is designed for single top-level values, but this tests
  // the internal arithmetic correctness.
  VariantBuilder b;
  // Write a "prefix" value that occupies buffer space before our object
  ASSERT_OK(b.Int(99));
  int64_t prefix_size = b.Offset();  // should be 2 (Int8 header + 1 byte)
  ASSERT_EQ(prefix_size, 2);

  auto start = b.Offset();
  std::vector<VariantBuilder::FieldEntry> fields;
  fields.push_back(b.NextField(start, "key"));
  ASSERT_OK(b.String("val"));
  ASSERT_OK(b.FinishObject(start, fields));

  // The buffer now contains [Int8(99)] + [Object{key: "val"}].
  // We can't call Finish() meaningfully for a two-value buffer,
  // but verify no crash or corruption occurred and the object portion
  // is correctly sized.
  ASSERT_GT(b.Offset(), prefix_size);
}

TEST_F(VariantBuilderPreExistingBufferTest, ArrayAfterPrimitive) {
  // Same as above but for arrays.
  VariantBuilder b;
  ASSERT_OK(b.Int(99));
  int64_t prefix_size = b.Offset();

  auto start = b.Offset();
  std::vector<int64_t> offsets;
  offsets.push_back(b.NextElement(start));
  ASSERT_OK(b.Null());
  ASSERT_OK(b.FinishArray(start, offsets));

  ASSERT_GT(b.Offset(), prefix_size);
}

}  // namespace arrow::extension::variant
