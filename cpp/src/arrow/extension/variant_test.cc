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

#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "arrow/testing/gtest_util.h"

namespace arrow::extension::variant {

// ===========================================================================
// Test helpers
// ===========================================================================

namespace {

/// \brief Decode and visit a variant value (convenience wrapper for tests).
/// Replaces the old DecodeVariantValue free function.
Status DecodeAndVisit(const VariantMetadata& metadata, const uint8_t* data,
                      int64_t length, VariantVisitor* visitor) {
  ARROW_ASSIGN_OR_RAISE(auto view, VariantView::Make(metadata, data, length));
  return view.Visit(visitor);
}

/// \brief Get the basic type of a value (convenience for tests).
Result<BasicType> GetValueBasicType(const uint8_t* data, int64_t length) {
  if (data == nullptr || length < 1) {
    return Status::Invalid("buffer is null or empty");
  }
  return GetBasicType(data[0]);
}

/// \brief Get object field count (convenience for tests).
Result<int32_t> GetObjectFieldCount(const uint8_t* data, int64_t length) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto obj, VariantObjectView::Make(empty_meta, data, length));
  return obj.num_fields();
}

/// \brief Get array element count (convenience for tests).
Result<int32_t> GetArrayElementCount(const uint8_t* data, int64_t length) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto arr, VariantArrayView::Make(empty_meta, data, length));
  return arr.num_elements();
}

/// \brief Find object field by name (convenience for tests).
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

/// \brief Get array element by index (convenience for tests).
Status GetArrayElement(const uint8_t* data, int64_t length, int32_t index,
                       int64_t* element_offset, int64_t* element_size) {
  VariantMetadata empty_meta;
  ARROW_ASSIGN_OR_RAISE(auto arr, VariantArrayView::Make(empty_meta, data, length));
  ARROW_ASSIGN_OR_RAISE(auto elem, arr.get(index));
  *element_offset = elem.data() - data;
  *element_size = elem.size_bytes();
  return Status::OK();
}

/// \brief Get object field at index (convenience for tests).
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

/// \brief Build a metadata buffer from a list of strings.
///
/// Uses offset_size=1, version=1, sorted flag as specified.
std::vector<uint8_t> BuildMetadataBuffer(const std::vector<std::string>& strings,
                                         bool sorted = false, int32_t offset_size = 1) {
  std::vector<uint8_t> buffer;

  // Header byte: version=1, sorted flag, offset_size
  uint8_t header = kVariantVersion;
  if (sorted) {
    header |= (1 << 4);
  }
  header |= static_cast<uint8_t>((offset_size - 1) << 6);
  buffer.push_back(header);

  // Dictionary size
  auto dict_size = static_cast<uint32_t>(strings.size());
  for (int32_t b = 0; b < offset_size; ++b) {
    buffer.push_back(static_cast<uint8_t>((dict_size >> (b * 8)) & 0xFF));
  }

  // Compute string offsets
  std::vector<uint32_t> offsets(dict_size + 1);
  offsets[0] = 0;
  for (uint32_t i = 0; i < dict_size; ++i) {
    offsets[i + 1] = offsets[i] + static_cast<uint32_t>(strings[i].size());
  }

  // Write offsets
  for (uint32_t i = 0; i <= dict_size; ++i) {
    for (int32_t b = 0; b < offset_size; ++b) {
      buffer.push_back(static_cast<uint8_t>((offsets[i] >> (b * 8)) & 0xFF));
    }
  }

  // Write string data
  for (const auto& s : strings) {
    buffer.insert(buffer.end(), s.begin(), s.end());
  }

  return buffer;
}

/// \brief Build a primitive value header byte.
uint8_t PrimitiveHeader(PrimitiveType type) {
  return static_cast<uint8_t>(BasicType::kPrimitive) | (static_cast<uint8_t>(type) << 2);
}

/// \brief Build a short string value buffer.
std::vector<uint8_t> BuildShortString(const std::string& s) {
  std::vector<uint8_t> buffer;
  auto len = static_cast<uint8_t>(s.size());
  uint8_t header = static_cast<uint8_t>(BasicType::kShortString) | (len << 2);
  buffer.push_back(header);
  buffer.insert(buffer.end(), s.begin(), s.end());
  return buffer;
}

/// \brief Build an object value buffer.
///
/// \param field_ids Dictionary indices for each field name
/// \param field_values Serialized variant values for each field
/// \param field_id_size Bytes per field ID (1-4)
/// \param field_offset_size Bytes per offset (1-4)
std::vector<uint8_t> BuildObject(const std::vector<uint32_t>& field_ids,
                                 const std::vector<std::vector<uint8_t>>& field_values,
                                 int32_t field_id_size = 1,
                                 int32_t field_offset_size = 1) {
  auto num_fields = static_cast<uint32_t>(field_ids.size());
  bool is_large = (num_fields > 255);

  std::vector<uint8_t> buffer;

  // Header per spec: basic_type=2 in bits 0-1,
  //   bits 2-3: field_offset_size-1
  //   bits 4-5: field_id_size-1
  //   bit 6: is_large
  uint8_t header = static_cast<uint8_t>(BasicType::kObject);
  header |= static_cast<uint8_t>((field_offset_size - 1) << 2);
  header |= static_cast<uint8_t>((field_id_size - 1) << 4);
  if (is_large) {
    header |= (1 << 6);
  }
  buffer.push_back(header);

  // num_fields: 1 byte or 4 bytes depending on is_large
  int32_t num_fields_size = is_large ? 4 : 1;
  for (int32_t b = 0; b < num_fields_size; ++b) {
    buffer.push_back(static_cast<uint8_t>((num_fields >> (b * 8)) & 0xFF));
  }

  // field_ids
  for (auto fid : field_ids) {
    for (int32_t b = 0; b < field_id_size; ++b) {
      buffer.push_back(static_cast<uint8_t>((fid >> (b * 8)) & 0xFF));
    }
  }

  // Compute offsets
  std::vector<uint32_t> offsets(num_fields + 1);
  offsets[0] = 0;
  for (uint32_t i = 0; i < num_fields; ++i) {
    offsets[i + 1] = offsets[i] + static_cast<uint32_t>(field_values[i].size());
  }

  // Write offsets
  for (uint32_t i = 0; i <= num_fields; ++i) {
    for (int32_t b = 0; b < field_offset_size; ++b) {
      buffer.push_back(static_cast<uint8_t>((offsets[i] >> (b * 8)) & 0xFF));
    }
  }

  // Write field value data
  for (const auto& fv : field_values) {
    buffer.insert(buffer.end(), fv.begin(), fv.end());
  }

  return buffer;
}

/// \brief Build an array value buffer.
///
/// \param elements Serialized variant values for each element
/// \param field_offset_size Bytes per offset (1-4)
std::vector<uint8_t> BuildArray(const std::vector<std::vector<uint8_t>>& elements,
                                int32_t field_offset_size = 1) {
  auto num_elements = static_cast<uint32_t>(elements.size());
  bool is_large = (num_elements > 255);

  std::vector<uint8_t> buffer;

  // Header per spec: basic_type=3 in bits 0-1,
  //   bits 2-3: field_offset_size-1
  //   bit 4: is_large
  uint8_t header = static_cast<uint8_t>(BasicType::kArray);
  header |= static_cast<uint8_t>((field_offset_size - 1) << 2);
  if (is_large) {
    header |= (1 << 4);
  }
  buffer.push_back(header);

  // num_elements: 1 byte or 4 bytes depending on is_large
  int32_t num_elements_size = is_large ? 4 : 1;
  for (int32_t b = 0; b < num_elements_size; ++b) {
    buffer.push_back(static_cast<uint8_t>((num_elements >> (b * 8)) & 0xFF));
  }

  // Compute offsets
  std::vector<uint32_t> offsets(num_elements + 1);
  offsets[0] = 0;
  for (uint32_t i = 0; i < num_elements; ++i) {
    offsets[i + 1] = offsets[i] + static_cast<uint32_t>(elements[i].size());
  }

  // Write offsets
  for (uint32_t i = 0; i <= num_elements; ++i) {
    for (int32_t b = 0; b < field_offset_size; ++b) {
      buffer.push_back(static_cast<uint8_t>((offsets[i] >> (b * 8)) & 0xFF));
    }
  }

  // Write element data
  for (const auto& elem : elements) {
    buffer.insert(buffer.end(), elem.begin(), elem.end());
  }

  return buffer;
}

}  // namespace

// ===========================================================================
// Metadata decoding tests
// ===========================================================================

class VariantMetadataTest : public ::testing::Test {};

TEST_F(VariantMetadataTest, EmptyDictionary) {
  auto buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.version, 1);
  ASSERT_FALSE(metadata.is_sorted);
  ASSERT_EQ(metadata.offset_size, 1);
  ASSERT_EQ(metadata.strings.size(), 0);
}

TEST_F(VariantMetadataTest, SingleString) {
  auto buf = BuildMetadataBuffer({"hello"});
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.strings.size(), 1);
  ASSERT_EQ(metadata.strings[0], "hello");
}

TEST_F(VariantMetadataTest, MultipleStrings) {
  auto buf = BuildMetadataBuffer({"name", "age", "scores"});
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.strings.size(), 3);
  ASSERT_EQ(metadata.strings[0], "name");
  ASSERT_EQ(metadata.strings[1], "age");
  ASSERT_EQ(metadata.strings[2], "scores");
}

TEST_F(VariantMetadataTest, SortedFlag) {
  auto buf = BuildMetadataBuffer({"age", "name", "score"}, true);
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_TRUE(metadata.is_sorted);
}

TEST_F(VariantMetadataTest, OffsetSize2) {
  auto buf = BuildMetadataBuffer({"key1", "key2"}, false, 2);
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.offset_size, 2);
  ASSERT_EQ(metadata.strings.size(), 2);
  ASSERT_EQ(metadata.strings[0], "key1");
  ASSERT_EQ(metadata.strings[1], "key2");
}

TEST_F(VariantMetadataTest, OffsetSize4) {
  auto buf = BuildMetadataBuffer({"a", "bb", "ccc"}, false, 4);
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.offset_size, 4);
  ASSERT_EQ(metadata.strings.size(), 3);
  ASSERT_EQ(metadata.strings[0], "a");
  ASSERT_EQ(metadata.strings[1], "bb");
  ASSERT_EQ(metadata.strings[2], "ccc");
}

TEST_F(VariantMetadataTest, EmptyStrings) {
  auto buf = BuildMetadataBuffer({"", "nonempty", ""});
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.strings.size(), 3);
  ASSERT_EQ(metadata.strings[0], "");
  ASSERT_EQ(metadata.strings[1], "nonempty");
  ASSERT_EQ(metadata.strings[2], "");
}

// Error cases

TEST_F(VariantMetadataTest, NullBuffer) {
  ASSERT_RAISES(Invalid, DecodeMetadata(nullptr, 0));
}

TEST_F(VariantMetadataTest, EmptyBuffer) {
  uint8_t data = 0;
  ASSERT_RAISES(Invalid, DecodeMetadata(&data, 0));
}

TEST_F(VariantMetadataTest, UnsupportedVersion) {
  // Version 2 (unsupported)
  uint8_t data[] = {0x02, 0x00};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantMetadataTest, TruncatedDictionarySize) {
  // Header says offset_size=2 (bits 6-7 = 01), but only 1 byte follows
  uint8_t data[] = {0x41, 0x00};  // version=1, offset_size=2
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantMetadataTest, TruncatedStringOffsets) {
  // Claims dict_size=5 but buffer is too short for offsets
  uint8_t data[] = {0x01, 0x05, 0x00};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantMetadataTest, OffsetSize3) {
  auto buf = BuildMetadataBuffer({"foo", "bar"}, false, 3);
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.offset_size, 3);
  ASSERT_EQ(metadata.strings.size(), 2);
  ASSERT_EQ(metadata.strings[0], "foo");
  ASSERT_EQ(metadata.strings[1], "bar");
}

TEST_F(VariantMetadataTest, ReservedBit5Set) {
  // Header with bit 5 set: 0x21 = version=1, bit5=1
  uint8_t data[] = {0x21, 0x00, 0x00};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantMetadataTest, NonMonotonicStringOffsets) {
  // Manually construct metadata where string offsets are NOT monotonically
  // non-decreasing. ValidateOffsets should reject this.
  // Header: version=1, offset_size=1
  // dict_size=2, offsets=[0, 5, 3] — 3 < 5, non-monotonic
  // String data: "helloabc" (8 bytes, but offsets claim 3 as last)
  uint8_t data[] = {0x01,              // header: version=1, offset_size=1
                    0x02,              // dict_size = 2
                    0x00, 0x05, 0x03,  // offsets: [0, 5, 3] — non-monotonic
                    'h',  'e',  'l',  'l', 'o', 'a', 'b', 'c'};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

// ===========================================================================
// Primitive value decoding tests
// ===========================================================================

class VariantPrimitiveTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantPrimitiveTest, DecodeNull) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kNull)};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events.size(), 1);
  ASSERT_EQ(visitor.events[0], "Null");
}

TEST_F(VariantPrimitiveTest, DecodeTrue) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kTrue)};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events.size(), 1);
  ASSERT_EQ(visitor.events[0], "Bool(true)");
}

TEST_F(VariantPrimitiveTest, DecodeFalse) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kFalse)};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events.size(), 1);
  ASSERT_EQ(visitor.events[0], "Bool(false)");
}

TEST_F(VariantPrimitiveTest, DecodeInt8) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt8), 0x2A};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int8(42)");
}

TEST_F(VariantPrimitiveTest, DecodeInt8Negative) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt8), 0xD6};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int8(-42)");
}

TEST_F(VariantPrimitiveTest, DecodeInt16) {
  // 300 = 0x012C in little-endian: 0x2C, 0x01
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt16), 0x2C, 0x01};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int16(300)");
}

TEST_F(VariantPrimitiveTest, DecodeInt32) {
  // 100000 = 0x000186A0 in LE: A0 86 01 00
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt32), 0xA0, 0x86, 0x01, 0x00};
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int32(100000)");
}

TEST_F(VariantPrimitiveTest, DecodeInt32Max) {
  int32_t val = std::numeric_limits<int32_t>::max();
  uint8_t data[5];
  data[0] = PrimitiveHeader(PrimitiveType::kInt32);
  std::memcpy(data + 1, &val, 4);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int32(" + std::to_string(val) + ")");
}

TEST_F(VariantPrimitiveTest, DecodeInt64) {
  int64_t val = 1234567890123LL;
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kInt64);
  std::memcpy(data + 1, &val, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int64(" + std::to_string(val) + ")");
}

TEST_F(VariantPrimitiveTest, DecodeFloat) {
  float val = 3.14f;
  uint8_t data[5];
  data[0] = PrimitiveHeader(PrimitiveType::kFloat);
  std::memcpy(data + 1, &val, 4);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  // Float string representation may vary; just check it starts with Float(
  ASSERT_TRUE(visitor.events[0].find("Float(") == 0);
}

TEST_F(VariantPrimitiveTest, DecodeDouble) {
  double val = 2.718281828459045;
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kDouble);
  std::memcpy(data + 1, &val, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_TRUE(visitor.events[0].find("Double(") == 0);
}

TEST_F(VariantPrimitiveTest, DecodeDate) {
  // Days since epoch: 19000 (approximately 2022-01-01)
  int32_t days = 19000;
  uint8_t data[5];
  data[0] = PrimitiveHeader(PrimitiveType::kDate);
  std::memcpy(data + 1, &days, 4);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Date(19000)");
}

TEST_F(VariantPrimitiveTest, DecodeTimestampMicros) {
  int64_t micros = 1654041600000000LL;  // some timestamp
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kTimestampMicros);
  std::memcpy(data + 1, &micros, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "TimestampMicros(" + std::to_string(micros) + ")");
}

TEST_F(VariantPrimitiveTest, DecodeTimestampMicrosNTZ) {
  int64_t micros = 1654041600000000LL;
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kTimestampMicrosNTZ);
  std::memcpy(data + 1, &micros, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "TimestampMicrosNTZ(" + std::to_string(micros) + ")");
}

TEST_F(VariantPrimitiveTest, DecodeDecimal4) {
  // Spec layout: 1 byte scale, then 4 bytes LE unscaled value
  uint8_t data[6];
  data[0] = PrimitiveHeader(PrimitiveType::kDecimal4);
  data[1] = 2;  // scale = 2
  int32_t val = 12345;
  std::memcpy(data + 2, &val, 4);  // unscaled value
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Decimal4(scale=2)");
}

TEST_F(VariantPrimitiveTest, DecodeDecimal4MaxScale) {
  // Scale at maximum per spec: 38
  uint8_t data[6];
  data[0] = PrimitiveHeader(PrimitiveType::kDecimal4);
  data[1] = 38;  // scale = 38 (maximum per spec)
  int32_t val = 12345;
  std::memcpy(data + 2, &val, 4);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Decimal4(scale=38)");
}

TEST_F(VariantPrimitiveTest, DecodeDecimal8) {
  // Spec layout: 1 byte scale, then 8 bytes LE unscaled value
  uint8_t data[10];
  data[0] = PrimitiveHeader(PrimitiveType::kDecimal8);
  data[1] = 5;  // scale = 5
  int64_t val = 123456789012345LL;
  std::memcpy(data + 2, &val, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Decimal8(scale=5)");
}

TEST_F(VariantPrimitiveTest, DecodeDecimal16) {
  // Spec layout: 1 byte scale, then 16 bytes LE unscaled value
  uint8_t data[18];
  data[0] = PrimitiveHeader(PrimitiveType::kDecimal16);
  data[1] = 10;  // scale = 10
  std::memset(data + 2, 0, 16);
  data[2] = 0x01;  // low byte = 1
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Decimal16(scale=10)");
}

TEST_F(VariantPrimitiveTest, DecodeLongString) {
  // Long string: primitive type kString with 4-byte length prefix
  std::string test_str = "hello world, this is a long string";
  auto str_len = static_cast<uint32_t>(test_str.size());

  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kString));
  // 4-byte little-endian length
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((str_len >> (b * 8)) & 0xFF));
  }
  data.insert(data.end(), test_str.begin(), test_str.end());

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"hello world, this is a long string\")");
}

TEST_F(VariantPrimitiveTest, DecodeBinary) {
  std::vector<uint8_t> bin_bytes = {0x00, 0x01, 0x02, 0x03};
  auto bin_len = static_cast<uint32_t>(bin_bytes.size());

  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kBinary));
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((bin_len >> (b * 8)) & 0xFF));
  }
  data.insert(data.end(), bin_bytes.begin(), bin_bytes.end());

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "Binary(len=4)");
}

// Truncation errors

TEST_F(VariantPrimitiveTest, TruncatedInt32) {
  // Only 2 bytes after header, but Int32 needs 4
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt32), 0x00, 0x00};
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
}

TEST_F(VariantPrimitiveTest, EmptyValueBuffer) {
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(empty_metadata_, nullptr, 0, &visitor));
}

// ===========================================================================
// Short string tests
// ===========================================================================

class VariantShortStringTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantShortStringTest, EmptyShortString) {
  auto data = BuildShortString("");
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"\")");
}

TEST_F(VariantShortStringTest, SimpleShortString) {
  auto data = BuildShortString("hi");
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"hi\")");
}

TEST_F(VariantShortStringTest, MaxLengthShortString) {
  // Maximum short string is 63 bytes
  std::string max_str(63, 'x');
  auto data = BuildShortString(max_str);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"" + max_str + "\")");
}

TEST_F(VariantShortStringTest, TruncatedShortString) {
  // Header says length=10 but buffer only has 3 bytes total
  uint8_t data[] = {static_cast<uint8_t>(BasicType::kShortString) | (10 << 2), 'a', 'b'};
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
}

// ===========================================================================
// Object decoding tests
// ===========================================================================

class VariantObjectTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = false;
    metadata_.offset_size = 1;
    metadata_.strings = {"name", "age", "scores"};
  }
};

TEST_F(VariantObjectTest, EmptyObject) {
  auto data = BuildObject({}, {});
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));
  ASSERT_EQ(visitor.events.size(), 2);
  ASSERT_EQ(visitor.events[0], "StartObject(0)");
  ASSERT_EQ(visitor.events[1], "EndObject");
}

TEST_F(VariantObjectTest, SingleField) {
  // Object with one field: name -> "Alice" (short string)
  auto value = BuildShortString("Alice");
  auto data = BuildObject({0}, {value});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));
  ASSERT_EQ(visitor.events.size(), 4);
  ASSERT_EQ(visitor.events[0], "StartObject(1)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[2], "String(\"Alice\")");
  ASSERT_EQ(visitor.events[3], "EndObject");
}

TEST_F(VariantObjectTest, MultipleFields) {
  // Object: {name: "Bob", age: 30}
  auto name_val = BuildShortString("Bob");
  // age: Int32(30)
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};

  auto data = BuildObject({0, 1}, {name_val, age_val});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));
  ASSERT_EQ(visitor.events.size(), 6);
  ASSERT_EQ(visitor.events[0], "StartObject(2)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[2], "String(\"Bob\")");
  ASSERT_EQ(visitor.events[3], "FieldName(\"age\")");
  ASSERT_EQ(visitor.events[4], "Int32(30)");
  ASSERT_EQ(visitor.events[5], "EndObject");
}

TEST_F(VariantObjectTest, InvalidFieldId) {
  // field_id=99 exceeds dictionary size of 3
  auto value = BuildShortString("oops");
  auto data = BuildObject({99}, {value});

  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(metadata_, data.data(),
                                        static_cast<int64_t>(data.size()), &visitor));
}

TEST_F(VariantObjectTest, ThreeByteOffsetSize) {
  // Exercises value decoding with 3-byte field_offset_size and field_id_size.
  // Object with 2 fields: {name: "test", age: 42}
  auto name_val = BuildShortString("test");
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 42, 0, 0, 0};
  auto data = BuildObject({0, 1}, {name_val, age_val},
                          /*field_id_size=*/3, /*field_offset_size=*/3);

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));
  ASSERT_EQ(visitor.events.size(), 6);
  ASSERT_EQ(visitor.events[0], "StartObject(2)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[2], "String(\"test\")");
  ASSERT_EQ(visitor.events[3], "FieldName(\"age\")");
  ASSERT_EQ(visitor.events[4], "Int32(42)");
  ASSERT_EQ(visitor.events[5], "EndObject");
}

// ===========================================================================
// Array decoding tests
// ===========================================================================

class VariantArrayTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantArrayTest, EmptyArray) {
  auto data = BuildArray({});
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events.size(), 2);
  ASSERT_EQ(visitor.events[0], "StartArray(0)");
  ASSERT_EQ(visitor.events[1], "EndArray");
}

TEST_F(VariantArrayTest, SingleElement) {
  std::vector<uint8_t> elem = {PrimitiveHeader(PrimitiveType::kInt32), 42, 0, 0, 0};
  auto data = BuildArray({elem});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events.size(), 3);
  ASSERT_EQ(visitor.events[0], "StartArray(1)");
  ASSERT_EQ(visitor.events[1], "Int32(42)");
  ASSERT_EQ(visitor.events[2], "EndArray");
}

TEST_F(VariantArrayTest, HeterogeneousElements) {
  // Array with mixed types: [42, "hello", true]
  std::vector<uint8_t> int_elem = {PrimitiveHeader(PrimitiveType::kInt32), 42, 0, 0, 0};
  auto str_elem = BuildShortString("hello");
  std::vector<uint8_t> bool_elem = {PrimitiveHeader(PrimitiveType::kTrue)};

  auto data = BuildArray({int_elem, str_elem, bool_elem});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events.size(), 5);
  ASSERT_EQ(visitor.events[0], "StartArray(3)");
  ASSERT_EQ(visitor.events[1], "Int32(42)");
  ASSERT_EQ(visitor.events[2], "String(\"hello\")");
  ASSERT_EQ(visitor.events[3], "Bool(true)");
  ASSERT_EQ(visitor.events[4], "EndArray");
}

TEST_F(VariantArrayTest, LargeArrayIsLargeFlag) {
  // Build an array with 256 elements to exercise is_large=true (4-byte
  // num_elements). Each element is a Null primitive (1 byte each).
  // Use field_offset_size=2 since total data (256 bytes) exceeds 1-byte max.
  std::vector<std::vector<uint8_t>> elements;
  elements.reserve(256);
  for (int i = 0; i < 256; ++i) {
    elements.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildArray(elements, /*field_offset_size=*/2);

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  // StartArray(256) + 256 Nulls + EndArray = 258 events
  ASSERT_EQ(visitor.events.size(), 258);
  ASSERT_EQ(visitor.events[0], "StartArray(256)");
  ASSERT_EQ(visitor.events[1], "Null");
  ASSERT_EQ(visitor.events[256], "Null");
  ASSERT_EQ(visitor.events[257], "EndArray");
}

// ===========================================================================
// Nested structure tests
// ===========================================================================

class VariantNestedTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = false;
    metadata_.offset_size = 1;
    metadata_.strings = {"name", "scores", "inner"};
  }
};

TEST_F(VariantNestedTest, ObjectWithNestedArray) {
  // {name: "Alice", scores: [95, 87]}
  auto name_val = BuildShortString("Alice");

  // scores array: [Int32(95), Int32(87)]
  std::vector<uint8_t> score1 = {PrimitiveHeader(PrimitiveType::kInt32), 95, 0, 0, 0};
  std::vector<uint8_t> score2 = {PrimitiveHeader(PrimitiveType::kInt32), 87, 0, 0, 0};
  auto scores_val = BuildArray({score1, score2});

  auto data = BuildObject({0, 1}, {name_val, scores_val});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));

  // Expected events:
  // StartObject(2), FieldName("name"), String("Alice"),
  // FieldName("scores"), StartArray(2), Int32(95), Int32(87), EndArray,
  // EndObject
  ASSERT_EQ(visitor.events.size(), 9);
  ASSERT_EQ(visitor.events[0], "StartObject(2)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[2], "String(\"Alice\")");
  ASSERT_EQ(visitor.events[3], "FieldName(\"scores\")");
  ASSERT_EQ(visitor.events[4], "StartArray(2)");
  ASSERT_EQ(visitor.events[5], "Int32(95)");
  ASSERT_EQ(visitor.events[6], "Int32(87)");
  ASSERT_EQ(visitor.events[7], "EndArray");
  ASSERT_EQ(visitor.events[8], "EndObject");
}

TEST_F(VariantNestedTest, NestedObjects) {
  // {inner: {name: "deep"}}
  auto deep_name = BuildShortString("deep");
  auto inner_obj = BuildObject({0}, {deep_name});
  auto data = BuildObject({2}, {inner_obj});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));

  ASSERT_EQ(visitor.events.size(), 7);
  ASSERT_EQ(visitor.events[0], "StartObject(1)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"inner\")");
  ASSERT_EQ(visitor.events[2], "StartObject(1)");
  ASSERT_EQ(visitor.events[3], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[4], "String(\"deep\")");
  ASSERT_EQ(visitor.events[5], "EndObject");
  ASSERT_EQ(visitor.events[6], "EndObject");
}

TEST_F(VariantNestedTest, ArrayOfObjects) {
  // [{name: "a"}, {name: "b"}]
  auto val_a = BuildShortString("a");
  auto obj_a = BuildObject({0}, {val_a});

  auto val_b = BuildShortString("b");
  auto obj_b = BuildObject({0}, {val_b});

  auto data = BuildArray({obj_a, obj_b});

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));

  ASSERT_EQ(visitor.events.size(), 10);
  ASSERT_EQ(visitor.events[0], "StartArray(2)");
  ASSERT_EQ(visitor.events[1], "StartObject(1)");
  ASSERT_EQ(visitor.events[2], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[3], "String(\"a\")");
  ASSERT_EQ(visitor.events[4], "EndObject");
  ASSERT_EQ(visitor.events[5], "StartObject(1)");
  ASSERT_EQ(visitor.events[6], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[7], "String(\"b\")");
  ASSERT_EQ(visitor.events[8], "EndObject");
  ASSERT_EQ(visitor.events[9], "EndArray");
}

// ===========================================================================
// Recursion depth limit test
// ===========================================================================

class VariantDepthTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = false;
    metadata_.offset_size = 1;
    metadata_.strings = {"x"};
  }
};

TEST_F(VariantDepthTest, ExceedsMaxNestingDepth) {
  // Build a deeply nested array: [[[[...]]]]
  // Each level wraps the inner in a 1-element array with offset_size=2
  // to allow buffers larger than 255 bytes.
  std::vector<uint8_t> inner = {PrimitiveHeader(PrimitiveType::kNull)};

  // Wrap 130 times (exceeds kMaxNestingDepth=128)
  for (int i = 0; i < 130; ++i) {
    inner = BuildArray({inner}, /*field_offset_size=*/2);
  }

  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(metadata_, inner.data(),
                                        static_cast<int64_t>(inner.size()), &visitor));
}

TEST_F(VariantDepthTest, AtMaxNestingDepthSucceeds) {
  // Build 50 levels of nesting — well within kMaxNestingDepth=128
  // and within offset_size=1 limits (each level adds ~4 bytes).
  std::vector<uint8_t> inner = {PrimitiveHeader(PrimitiveType::kNull)};

  for (int i = 0; i < 50; ++i) {
    inner = BuildArray({inner});
  }

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, inner.data(), static_cast<int64_t>(inner.size()),
                           &visitor));
}

// ===========================================================================
// Utility function tests
// ===========================================================================

class VariantUtilTest : public ::testing::Test {};

TEST_F(VariantUtilTest, GetValueBasicTypePrimitive) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt32), 0, 0, 0, 0};
  ASSERT_OK_AND_ASSIGN(auto bt, GetValueBasicType(data, sizeof(data)));
  ASSERT_EQ(bt, BasicType::kPrimitive);
}

TEST_F(VariantUtilTest, GetValueBasicTypeShortString) {
  auto data = BuildShortString("test");
  ASSERT_OK_AND_ASSIGN(auto bt,
                       GetValueBasicType(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(bt, BasicType::kShortString);
}

TEST_F(VariantUtilTest, GetValueBasicTypeObject) {
  VariantMetadata meta;
  meta.version = 1;
  meta.strings = {"key"};
  auto val = BuildShortString("val");
  auto data = BuildObject({0}, {val});
  ASSERT_OK_AND_ASSIGN(auto bt,
                       GetValueBasicType(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(bt, BasicType::kObject);
}

TEST_F(VariantUtilTest, GetValueBasicTypeArray) {
  auto data = BuildArray({});
  ASSERT_OK_AND_ASSIGN(auto bt,
                       GetValueBasicType(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(bt, BasicType::kArray);
}

TEST_F(VariantUtilTest, GetValueBasicTypeEmptyBuffer) {
  ASSERT_RAISES(Invalid, GetValueBasicType(nullptr, 0));
}

TEST_F(VariantUtilTest, GetObjectFieldCount) {
  VariantMetadata meta;
  meta.version = 1;
  meta.strings = {"a", "b", "c"};
  auto v1 = BuildShortString("x");
  auto v2 = BuildShortString("y");
  auto data = BuildObject({0, 1}, {v1, v2});
  ASSERT_OK_AND_ASSIGN(
      auto count, GetObjectFieldCount(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(count, 2);
}

TEST_F(VariantUtilTest, GetArrayElementCount) {
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kNull)};
  std::vector<uint8_t> e2 = {PrimitiveHeader(PrimitiveType::kTrue)};
  std::vector<uint8_t> e3 = {PrimitiveHeader(PrimitiveType::kFalse)};
  auto data = BuildArray({e1, e2, e3});
  ASSERT_OK_AND_ASSIGN(
      auto count, GetArrayElementCount(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(count, 3);
}

TEST_F(VariantUtilTest, PrimitiveValueSizes) {
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kNull), 0);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTrue), 0);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kFalse), 0);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kInt8), 1);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kInt16), 2);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kInt32), 4);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kInt64), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kFloat), 4);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kDouble), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kDate), 4);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTimestampMicros), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTimestampMicrosNTZ), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTimeNTZ), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTimestampNanos), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kTimestampNanosNTZ), 8);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kUUID), 16);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kDecimal4), 5);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kDecimal8), 9);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kDecimal16), 17);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kBinary), -1);
  ASSERT_EQ(PrimitiveValueSize(PrimitiveType::kString), -1);
}

// ===========================================================================
// Integration: Metadata + Value decoding together
// ===========================================================================

class VariantIntegrationTest : public ::testing::Test {};

TEST_F(VariantIntegrationTest, FullRoundTrip) {
  // Build a complete variant: {name: "Alice", age: 30, scores: [95, 87]}
  auto meta_buf = BuildMetadataBuffer({"name", "age", "scores"});

  auto name_val = BuildShortString("Alice");
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};
  std::vector<uint8_t> s1 = {PrimitiveHeader(PrimitiveType::kInt32), 95, 0, 0, 0};
  std::vector<uint8_t> s2 = {PrimitiveHeader(PrimitiveType::kInt32), 87, 0, 0, 0};
  auto scores_val = BuildArray({s1, s2});

  auto value_buf = BuildObject({0, 1, 2}, {name_val, age_val, scores_val});

  // Decode metadata
  ASSERT_OK_AND_ASSIGN(
      auto metadata,
      DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));
  ASSERT_EQ(metadata.strings.size(), 3);

  // Decode value
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_buf.data(),
                           static_cast<int64_t>(value_buf.size()), &visitor));

  // Verify full event sequence
  ASSERT_EQ(visitor.events.size(), 11);
  ASSERT_EQ(visitor.events[0], "StartObject(3)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"name\")");
  ASSERT_EQ(visitor.events[2], "String(\"Alice\")");
  ASSERT_EQ(visitor.events[3], "FieldName(\"age\")");
  ASSERT_EQ(visitor.events[4], "Int32(30)");
  ASSERT_EQ(visitor.events[5], "FieldName(\"scores\")");
  ASSERT_EQ(visitor.events[6], "StartArray(2)");
  ASSERT_EQ(visitor.events[7], "Int32(95)");
  ASSERT_EQ(visitor.events[8], "Int32(87)");
  ASSERT_EQ(visitor.events[9], "EndArray");
  ASSERT_EQ(visitor.events[10], "EndObject");
}

// ===========================================================================
// Visitor early abort test
// ===========================================================================

/// \brief A visitor that aborts after receiving a specific number of events.
class AbortingVisitor : public VariantVisitor {
 public:
  int32_t abort_after;
  int32_t count = 0;

  explicit AbortingVisitor(int32_t abort_after) : abort_after(abort_after) {}

  Status MaybeAbort() {
    ++count;
    if (count >= abort_after) {
      return Status::Cancelled("Visitor aborted after ", count, " events");
    }
    return Status::OK();
  }

  Status Null() override { return MaybeAbort(); }
  Status Bool(bool /*value*/) override { return MaybeAbort(); }
  Status Int8(int8_t /*value*/) override { return MaybeAbort(); }
  Status Int16(int16_t /*value*/) override { return MaybeAbort(); }
  Status Int32(int32_t /*value*/) override { return MaybeAbort(); }
  Status Int64(int64_t /*value*/) override { return MaybeAbort(); }
  Status Float(float /*value*/) override { return MaybeAbort(); }
  Status Double(double /*value*/) override { return MaybeAbort(); }
  Status Decimal4(const uint8_t* /*bytes*/, int32_t /*s*/) override {
    return MaybeAbort();
  }
  Status Decimal8(const uint8_t* /*bytes*/, int32_t /*s*/) override {
    return MaybeAbort();
  }
  Status Decimal16(const uint8_t* /*bytes*/, int32_t /*s*/) override {
    return MaybeAbort();
  }
  Status Date(int32_t /*days*/) override { return MaybeAbort(); }
  Status TimestampMicros(int64_t /*micros*/) override { return MaybeAbort(); }
  Status TimestampMicrosNTZ(int64_t /*micros*/) override { return MaybeAbort(); }
  Status String(std::string_view /*value*/) override { return MaybeAbort(); }
  Status Binary(std::string_view /*value*/) override { return MaybeAbort(); }
  Status TimeNTZ(int64_t /*micros*/) override { return MaybeAbort(); }
  Status TimestampNanos(int64_t /*nanos*/) override { return MaybeAbort(); }
  Status TimestampNanosNTZ(int64_t /*nanos*/) override { return MaybeAbort(); }
  Status UUID(const uint8_t* /*bytes*/) override { return MaybeAbort(); }
  Status StartObject(int32_t /*num_fields*/) override { return MaybeAbort(); }
  Status FieldName(std::string_view /*name*/) override { return MaybeAbort(); }
  Status EndObject() override { return MaybeAbort(); }
  Status StartArray(int32_t /*num_elements*/) override { return MaybeAbort(); }
  Status EndArray() override { return MaybeAbort(); }
};

class VariantAbortTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = false;
    metadata_.offset_size = 1;
    metadata_.strings = {"name", "age"};
  }
};

TEST_F(VariantAbortTest, VisitorAbortsEarly) {
  // Object: {name: "Alice", age: 30}
  auto name_val = BuildShortString("Alice");
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};
  auto data = BuildObject({0, 1}, {name_val, age_val});

  // Abort after 3 events (StartObject, FieldName, String)
  // Should NOT reach the second field
  AbortingVisitor visitor(3);
  auto status =
      DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()), &visitor);
  ASSERT_TRUE(status.IsCancelled());
  ASSERT_EQ(visitor.count, 3);
}

TEST_F(VariantAbortTest, VisitorAbortsOnFirstEvent) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kNull)};
  AbortingVisitor visitor(1);
  auto status = DecodeAndVisit(metadata_, data, sizeof(data), &visitor);
  ASSERT_TRUE(status.IsCancelled());
}

// ===========================================================================
// Spec-conformance test with hardcoded byte sequences
// ===========================================================================

class VariantSpecTest : public ::testing::Test {};

TEST_F(VariantSpecTest, HandcraftedNullValue) {
  // Variant Encoding Spec: Null is basic_type=0, primitive_type=0
  // Header byte: 0x00 (bits 0-1=00 for primitive, bits 2-7=000000 for null)
  uint8_t metadata_bytes[] = {0x01, 0x00, 0x00};  // v1, 0 strings, offset[0]=0
  uint8_t value_bytes[] = {0x00};                 // null

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_bytes, sizeof(value_bytes), &visitor));
  ASSERT_EQ(visitor.events.size(), 1);
  ASSERT_EQ(visitor.events[0], "Null");
}

TEST_F(VariantSpecTest, HandcraftedInt32Value) {
  // Int32(42): basic_type=0, primitive_type=5
  // Header: (5 << 2) | 0 = 0x14
  // Value: 42 as LE int32 = 2A 00 00 00
  uint8_t metadata_bytes[] = {0x01, 0x00, 0x00};
  uint8_t value_bytes[] = {0x14, 0x2A, 0x00, 0x00, 0x00};

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_bytes, sizeof(value_bytes), &visitor));
  ASSERT_EQ(visitor.events[0], "Int32(42)");
}

TEST_F(VariantSpecTest, HandcraftedShortString) {
  // Short string "hello": basic_type=1, length=5
  // Header: (5 << 2) | 1 = 0x15
  // Followed by 5 bytes of UTF-8 "hello"
  uint8_t metadata_bytes[] = {0x01, 0x00, 0x00};
  uint8_t value_bytes[] = {0x15, 'h', 'e', 'l', 'l', 'o'};

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_bytes, sizeof(value_bytes), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"hello\")");
}

TEST_F(VariantSpecTest, HandcraftedSimpleObject) {
  // Object {"a": 1} with metadata dictionary ["a"]
  //
  // Metadata: version=1, sorted=false, offset_size=1
  //   header=0x01, dict_size=0x01, offsets=[0x00, 0x01], data="a"
  uint8_t metadata_bytes[] = {0x01, 0x01, 0x00, 0x01, 'a'};
  //
  // Value: object with 1 field
  //   header: basic_type=2, field_id_size=1(bits2-3=00),
  //           offset_size=1(bits4-5=00), num_fields_size=1(bits6-7=00)
  //           = 0x02
  //   num_fields: 0x01
  //   field_ids: [0x00] (index into metadata for "a")
  //   offsets: [0x00, 0x05] (field 0 at offset 0, total size 5)
  //   field value: Int32(1) = header 0x14 + LE bytes 01 00 00 00
  uint8_t value_bytes[] = {
      0x02,                         // object header
      0x01,                         // num_fields = 1
      0x00,                         // field_id[0] = 0
      0x00, 0x05,                   // offsets: [0, 5]
      0x14, 0x01, 0x00, 0x00, 0x00  // Int32(1)
  };

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_bytes, sizeof(value_bytes), &visitor));
  ASSERT_EQ(visitor.events.size(), 4);
  ASSERT_EQ(visitor.events[0], "StartObject(1)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"a\")");
  ASSERT_EQ(visitor.events[2], "Int32(1)");
  ASSERT_EQ(visitor.events[3], "EndObject");
}

TEST_F(VariantSpecTest, HandcraftedTrueAndFalse) {
  // True: basic_type=0, primitive_type=1 → header = (1<<2)|0 = 0x04
  // False: basic_type=0, primitive_type=2 → header = (2<<2)|0 = 0x08
  uint8_t metadata_bytes[] = {0x01, 0x00, 0x00};

  uint8_t true_bytes[] = {0x04};
  uint8_t false_bytes[] = {0x08};

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));

  RecordingVisitor v1;
  ASSERT_OK(DecodeAndVisit(metadata, true_bytes, sizeof(true_bytes), &v1));
  ASSERT_EQ(v1.events[0], "Bool(true)");

  RecordingVisitor v2;
  ASSERT_OK(DecodeAndVisit(metadata, false_bytes, sizeof(false_bytes), &v2));
  ASSERT_EQ(v2.events[0], "Bool(false)");
}

TEST_F(VariantSpecTest, HandcraftedDouble) {
  // Double: basic_type=0, primitive_type=7 → header = (7<<2)|0 = 0x1C
  // Value: 3.14 as IEEE 754 double LE
  uint8_t metadata_bytes[] = {0x01, 0x00, 0x00};
  uint8_t value_bytes[9];
  value_bytes[0] = 0x1C;
  double val = 3.14;
  std::memcpy(value_bytes + 1, &val, 8);

  ASSERT_OK_AND_ASSIGN(auto metadata,
                       DecodeMetadata(metadata_bytes, sizeof(metadata_bytes)));
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata, value_bytes, sizeof(value_bytes), &visitor));
  ASSERT_TRUE(visitor.events[0].find("Double(") == 0);
}

// ===========================================================================
// ValueSize tests
// ===========================================================================

class VariantValueSizeTest : public ::testing::Test {};

TEST_F(VariantValueSizeTest, NullSize) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kNull)};
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(data, sizeof(data)));
  ASSERT_EQ(size, 1);
}

TEST_F(VariantValueSizeTest, Int32Size) {
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt32), 0, 0, 0, 0};
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(data, sizeof(data)));
  ASSERT_EQ(size, 5);
}

TEST_F(VariantValueSizeTest, ShortStringSize) {
  auto data = BuildShortString("hello");
  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, 6);  // 1 header + 5 chars
}

TEST_F(VariantValueSizeTest, ObjectSize) {
  VariantMetadata meta;
  meta.version = 1;
  meta.strings = {"key"};
  auto val = BuildShortString("val");
  auto data = BuildObject({0}, {val});
  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}

TEST_F(VariantValueSizeTest, ArraySize) {
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kNull)};
  std::vector<uint8_t> e2 = {PrimitiveHeader(PrimitiveType::kTrue)};
  auto data = BuildArray({e1, e2});
  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}

TEST_F(VariantValueSizeTest, UUIDSize) {
  uint8_t data[17];
  data[0] = PrimitiveHeader(PrimitiveType::kUUID);
  std::memset(data + 1, 0, 16);
  ASSERT_OK_AND_ASSIGN(auto size, ValueSize(data, sizeof(data)));
  ASSERT_EQ(size, 17);
}

// ===========================================================================
// Random access tests
// ===========================================================================

class VariantRandomAccessTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = true;
    metadata_.offset_size = 1;
    // Sorted lexicographically for binary search
    metadata_.strings = {"age", "name", "score"};
  }
};

TEST_F(VariantRandomAccessTest, FindObjectFieldExists) {
  // Object: {age: 30, name: "Alice", score: 95}
  // field_ids must be in lex order of keys: age=0, name=1, score=2
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};
  auto name_val = BuildShortString("Alice");
  std::vector<uint8_t> score_val = {PrimitiveHeader(PrimitiveType::kInt32), 95, 0, 0, 0};
  auto data = BuildObject({0, 1, 2}, {age_val, name_val, score_val});

  int64_t offset = -1, size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "name", &offset, &size));
  ASSERT_GT(offset, 0);
  ASSERT_EQ(size, 6);  // short string "Alice" = 1 + 5

  // Verify we can decode just that field
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data() + offset, size, &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"Alice\")");
}

TEST_F(VariantRandomAccessTest, FindObjectFieldNotFound) {
  auto val = BuildShortString("x");
  auto data = BuildObject({0}, {val});

  int64_t offset = -1, size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "nonexistent", &offset, &size));
  ASSERT_EQ(offset, -1);
  ASSERT_EQ(size, 0);
}

TEST_F(VariantRandomAccessTest, GetArrayElementFirst) {
  std::vector<uint8_t> e0 = {PrimitiveHeader(PrimitiveType::kInt32), 42, 0, 0, 0};
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kNull)};
  auto data = BuildArray({e0, e1});

  int64_t offset = 0, size = 0;
  ASSERT_OK(
      GetArrayElement(data.data(), static_cast<int64_t>(data.size()), 0, &offset, &size));
  ASSERT_EQ(size, 5);  // Int32 = 5 bytes

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data() + offset, size, &visitor));
  ASSERT_EQ(visitor.events[0], "Int32(42)");
}

TEST_F(VariantRandomAccessTest, GetArrayElementLast) {
  std::vector<uint8_t> e0 = {PrimitiveHeader(PrimitiveType::kInt32), 42, 0, 0, 0};
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kNull)};
  auto data = BuildArray({e0, e1});

  int64_t offset = 0, size = 0;
  ASSERT_OK(
      GetArrayElement(data.data(), static_cast<int64_t>(data.size()), 1, &offset, &size));
  ASSERT_EQ(size, 1);  // Null = 1 byte
}

TEST_F(VariantRandomAccessTest, GetArrayElementOutOfRange) {
  std::vector<uint8_t> e0 = {PrimitiveHeader(PrimitiveType::kNull)};
  auto data = BuildArray({e0});

  int64_t offset = 0, size = 0;
  ASSERT_RAISES(Invalid, GetArrayElement(data.data(), static_cast<int64_t>(data.size()),
                                         5, &offset, &size));
}

TEST_F(VariantRandomAccessTest, GetObjectFieldAtByIndex) {
  std::vector<uint8_t> age_val = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};
  auto name_val = BuildShortString("Bob");
  auto data = BuildObject({0, 1}, {age_val, name_val});

  std::string_view name;
  int64_t offset = 0, size = 0;
  ASSERT_OK(GetObjectFieldAt(metadata_, data.data(), static_cast<int64_t>(data.size()), 1,
                             &name, &offset, &size));
  ASSERT_EQ(name, "name");
  ASSERT_EQ(size, 4);  // short string "Bob" = 1 + 3
}

TEST_F(VariantRandomAccessTest, GetObjectFieldAtOutOfRange) {
  auto val = BuildShortString("x");
  auto data = BuildObject({0}, {val});

  std::string_view name;
  int64_t offset = 0, size = 0;
  ASSERT_RAISES(
      Invalid, GetObjectFieldAt(metadata_, data.data(), static_cast<int64_t>(data.size()),
                                99, &name, &offset, &size));
}

// ===========================================================================
// FindMetadataKey tests
// ===========================================================================

class VariantFindMetadataKeyTest : public ::testing::Test {};

TEST_F(VariantFindMetadataKeyTest, SortedFound) {
  VariantMetadata meta;
  meta.is_sorted = true;
  meta.strings = {"age", "name", "score"};
  ASSERT_EQ(FindMetadataKey(meta, "name"), 1);
  ASSERT_EQ(FindMetadataKey(meta, "age"), 0);
  ASSERT_EQ(FindMetadataKey(meta, "score"), 2);
}

TEST_F(VariantFindMetadataKeyTest, SortedNotFound) {
  VariantMetadata meta;
  meta.is_sorted = true;
  meta.strings = {"age", "name", "score"};
  ASSERT_EQ(FindMetadataKey(meta, "missing"), -1);
}

TEST_F(VariantFindMetadataKeyTest, UnsortedFound) {
  VariantMetadata meta;
  meta.is_sorted = false;
  meta.strings = {"name", "age", "score"};
  ASSERT_EQ(FindMetadataKey(meta, "age"), 1);
}

TEST_F(VariantFindMetadataKeyTest, UnsortedNotFound) {
  VariantMetadata meta;
  meta.is_sorted = false;
  meta.strings = {"name", "age"};
  ASSERT_EQ(FindMetadataKey(meta, "missing"), -1);
}

// ===========================================================================
// ValueSize regression tests (Go bug: array is_large bit position)
// ===========================================================================

class VariantValueSizeRegressionTest : public ::testing::Test {};

TEST_F(VariantValueSizeRegressionTest, LargeArrayIsLargeBit) {
  // Build a large array with 300 elements (>255) to trigger is_large=true.
  // This verifies the is_large bit is read at bit 2 of type_info (bit 4 of
  // full byte), NOT bit 4 of type_info (bit 6 of full byte) which was the
  // Go bug (apache/arrow-go#839).
  std::vector<std::vector<uint8_t>> elements;
  elements.reserve(300);
  for (int i = 0; i < 300; ++i) {
    elements.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildArray(elements, /*field_offset_size=*/2);

  // Verify the header byte is correctly structured
  uint8_t header = data[0];
  ASSERT_EQ(GetBasicType(header), BasicType::kArray);
  // is_large should be set at bit 4 of the full header byte
  ASSERT_TRUE(((header >> 4) & 0x01) != 0);

  // ValueSize must return the total size of the buffer
  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}

TEST_F(VariantValueSizeRegressionTest, SmallArrayIsLargeFalse) {
  // Array with 3 elements — is_large=false
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kNull)};
  std::vector<uint8_t> e2 = {PrimitiveHeader(PrimitiveType::kTrue)};
  std::vector<uint8_t> e3 = {PrimitiveHeader(PrimitiveType::kFalse)};
  auto data = BuildArray({e1, e2, e3});

  // Verify is_large is NOT set
  uint8_t header = data[0];
  ASSERT_FALSE(((header >> 4) & 0x01) != 0);

  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}

TEST_F(VariantValueSizeRegressionTest, LargeObjectIsLargeBit) {
  // Object with 300 fields to trigger is_large=true (bit 6 of full byte)
  std::vector<uint32_t> field_ids;
  std::vector<std::vector<uint8_t>> values;
  for (int i = 0; i < 300; ++i) {
    field_ids.push_back(static_cast<uint32_t>(i));
    values.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data =
      BuildObject(field_ids, values, /*field_id_size=*/2, /*field_offset_size=*/2);

  // Verify is_large is set at bit 6 of the full header byte
  uint8_t header = data[0];
  ASSERT_EQ(GetBasicType(header), BasicType::kObject);
  ASSERT_TRUE(((header >> 6) & 0x01) != 0);

  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, static_cast<int64_t>(data.size()));
}

// ===========================================================================
// Additional primitive decoding tests
// ===========================================================================

class VariantPrimitiveExtraTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantPrimitiveExtraTest, DecodeTimeNTZ) {
  int64_t micros = 43200000000LL;  // 12:00:00 in microseconds
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kTimeNTZ);
  std::memcpy(data + 1, &micros, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "TimeNTZ(" + std::to_string(micros) + ")");
}

TEST_F(VariantPrimitiveExtraTest, DecodeTimestampNanos) {
  int64_t nanos = 1654041600000000000LL;
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kTimestampNanos);
  std::memcpy(data + 1, &nanos, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "TimestampNanos(" + std::to_string(nanos) + ")");
}

TEST_F(VariantPrimitiveExtraTest, DecodeTimestampNanosNTZ) {
  int64_t nanos = 1654041600000000000LL;
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kTimestampNanosNTZ);
  std::memcpy(data + 1, &nanos, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "TimestampNanosNTZ(" + std::to_string(nanos) + ")");
}

TEST_F(VariantPrimitiveExtraTest, DecodeUUID) {
  uint8_t data[17];
  data[0] = PrimitiveHeader(PrimitiveType::kUUID);
  // Fill UUID with recognizable pattern (big-endian per spec)
  for (int i = 0; i < 16; ++i) {
    data[1 + i] = static_cast<uint8_t>(i + 1);
  }
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "UUID");
}

TEST_F(VariantPrimitiveExtraTest, DecodeInt8Boundaries) {
  // INT8_MIN = -128
  {
    uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt8), 0x80};
    RecordingVisitor visitor;
    ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
    ASSERT_EQ(visitor.events[0], "Int8(-128)");
  }
  // INT8_MAX = 127
  {
    uint8_t data[] = {PrimitiveHeader(PrimitiveType::kInt8), 0x7F};
    RecordingVisitor visitor;
    ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
    ASSERT_EQ(visitor.events[0], "Int8(127)");
  }
}

TEST_F(VariantPrimitiveExtraTest, DecodeInt16Boundaries) {
  // INT16_MIN = -32768
  {
    int16_t val = std::numeric_limits<int16_t>::min();
    uint8_t data[3];
    data[0] = PrimitiveHeader(PrimitiveType::kInt16);
    std::memcpy(data + 1, &val, 2);
    RecordingVisitor visitor;
    ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
    ASSERT_EQ(visitor.events[0], "Int16(-32768)");
  }
  // INT16_MAX = 32767
  {
    int16_t val = std::numeric_limits<int16_t>::max();
    uint8_t data[3];
    data[0] = PrimitiveHeader(PrimitiveType::kInt16);
    std::memcpy(data + 1, &val, 2);
    RecordingVisitor visitor;
    ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
    ASSERT_EQ(visitor.events[0], "Int16(32767)");
  }
}

TEST_F(VariantPrimitiveExtraTest, DecodeInt64Min) {
  int64_t val = std::numeric_limits<int64_t>::min();
  uint8_t data[9];
  data[0] = PrimitiveHeader(PrimitiveType::kInt64);
  std::memcpy(data + 1, &val, 8);
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
  ASSERT_EQ(visitor.events[0], "Int64(" + std::to_string(val) + ")");
}

TEST_F(VariantPrimitiveExtraTest, DecodeEmptyBinary) {
  // Binary with zero length
  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kBinary));
  uint32_t len = 0;
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((len >> (b * 8)) & 0xFF));
  }
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "Binary(len=0)");
}

TEST_F(VariantPrimitiveExtraTest, DecodeEmptyLongString) {
  // Long string with zero length
  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kString));
  uint32_t len = 0;
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((len >> (b * 8)) & 0xFF));
  }
  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(empty_metadata_, data.data(),
                           static_cast<int64_t>(data.size()), &visitor));
  ASSERT_EQ(visitor.events[0], "String(\"\")");
}

// ===========================================================================
// Object with non-monotonic offsets (spec-compliant)
// ===========================================================================

class VariantObjectNonMonotonicTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = true;
    metadata_.offset_size = 1;
    // Sorted lexicographically
    metadata_.strings = {"a", "b", "c"};
  }
};

TEST_F(VariantObjectNonMonotonicTest, NonMonotonicObjectOffsets) {
  // Per spec: "field IDs and offsets must be listed in the order of the
  // corresponding field names, sorted lexicographically" but "the actual
  // value entries do not need to be in any particular order" and "the
  // field_offset values may not be monotonically increasing."
  //
  // Construct: {a: 1, b: 2, c: 3} where values are stored as [3, 1, 2]
  // in the data area but offsets point to them in key-sorted order.
  std::vector<uint8_t> val_a = {PrimitiveHeader(PrimitiveType::kInt8), 1};
  std::vector<uint8_t> val_b = {PrimitiveHeader(PrimitiveType::kInt8), 2};
  std::vector<uint8_t> val_c = {PrimitiveHeader(PrimitiveType::kInt8), 3};

  // Data area stores: val_c (2 bytes) | val_a (2 bytes) | val_b (2 bytes)
  // Offsets: a->2, b->4, c->0, end->6
  uint8_t header = static_cast<uint8_t>(BasicType::kObject);  // offset_size=1, id_size=1
  std::vector<uint8_t> data;
  data.push_back(header);
  data.push_back(3);  // num_fields = 3
  data.push_back(0);  // field_id[0] = 0 ("a")
  data.push_back(1);  // field_id[1] = 1 ("b")
  data.push_back(2);  // field_id[2] = 2 ("c")
  data.push_back(2);  // offset[0] = 2 (val_a starts at byte 2)
  data.push_back(4);  // offset[1] = 4 (val_b starts at byte 4)
  data.push_back(0);  // offset[2] = 0 (val_c starts at byte 0)
  data.push_back(6);  // offset[3] = 6 (total data size)
  // Data area: val_c, val_a, val_b
  data.insert(data.end(), val_c.begin(), val_c.end());
  data.insert(data.end(), val_a.begin(), val_a.end());
  data.insert(data.end(), val_b.begin(), val_b.end());

  RecordingVisitor visitor;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data(), static_cast<int64_t>(data.size()),
                           &visitor));
  // Field iteration order follows field_ids (sorted by key): a, b, c
  ASSERT_EQ(visitor.events.size(), 8);
  ASSERT_EQ(visitor.events[0], "StartObject(3)");
  ASSERT_EQ(visitor.events[1], "FieldName(\"a\")");
  ASSERT_EQ(visitor.events[2], "Int8(1)");
  ASSERT_EQ(visitor.events[3], "FieldName(\"b\")");
  ASSERT_EQ(visitor.events[4], "Int8(2)");
  ASSERT_EQ(visitor.events[5], "FieldName(\"c\")");
  ASSERT_EQ(visitor.events[6], "Int8(3)");
  ASSERT_EQ(visitor.events[7], "EndObject");
}

TEST_F(VariantObjectNonMonotonicTest, FindFieldWithNonMonotonicOffsets) {
  // Same layout as above: values stored out-of-order
  uint8_t header = static_cast<uint8_t>(BasicType::kObject);
  std::vector<uint8_t> data;
  data.push_back(header);
  data.push_back(3);
  data.push_back(0);
  data.push_back(1);
  data.push_back(2);
  data.push_back(2);  // a -> offset 2
  data.push_back(4);  // b -> offset 4
  data.push_back(0);  // c -> offset 0
  data.push_back(6);  // end = 6
  // Data: [Int8(3), Int8(1), Int8(2)]
  data.push_back(PrimitiveHeader(PrimitiveType::kInt8));
  data.push_back(3);
  data.push_back(PrimitiveHeader(PrimitiveType::kInt8));
  data.push_back(1);
  data.push_back(PrimitiveHeader(PrimitiveType::kInt8));
  data.push_back(2);

  // FindObjectField should find "c" at offset 0 of data area
  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "c", &field_offset, &field_size));
  ASSERT_GT(field_offset, 0);
  ASSERT_EQ(field_size, 2);  // Int8 = 2 bytes

  // Decode the value at that offset and verify it's 3 (val_c)
  RecordingVisitor v;
  ASSERT_OK(DecodeAndVisit(metadata_, data.data() + field_offset, field_size, &v));
  ASSERT_EQ(v.events[0], "Int8(3)");
}

// ===========================================================================
// ValueSize for variable-length primitives
// ===========================================================================

class VariantValueSizeVarLenTest : public ::testing::Test {};

TEST_F(VariantValueSizeVarLenTest, LongStringSize) {
  // Long string "hello" (5 chars): header(1) + length(4) + data(5) = 10
  std::string s = "hello";
  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kString));
  auto len = static_cast<uint32_t>(s.size());
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((len >> (b * 8)) & 0xFF));
  }
  data.insert(data.end(), s.begin(), s.end());

  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, 10);
}

TEST_F(VariantValueSizeVarLenTest, BinarySize) {
  // Binary with 4 bytes: header(1) + length(4) + data(4) = 9
  std::vector<uint8_t> data;
  data.push_back(PrimitiveHeader(PrimitiveType::kBinary));
  uint32_t len = 4;
  for (int b = 0; b < 4; ++b) {
    data.push_back(static_cast<uint8_t>((len >> (b * 8)) & 0xFF));
  }
  data.push_back(0x00);
  data.push_back(0x01);
  data.push_back(0x02);
  data.push_back(0x03);

  ASSERT_OK_AND_ASSIGN(auto size,
                       ValueSize(data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(size, 9);
}

TEST_F(VariantValueSizeVarLenTest, TruncatedLongString) {
  // Only header byte, no length field
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kString)};
  ASSERT_RAISES(Invalid, ValueSize(data, sizeof(data)));
}

// ===========================================================================
// Unknown/invalid type tests
// ===========================================================================

class VariantUnknownTypeTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantUnknownTypeTest, UnknownPrimitiveType) {
  // Primitive type ID 25 (beyond kUUID=20) should produce an error.
  // Header: (25 << 2) | 0 = 0x64
  uint8_t data[] = {0x64};
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
}

TEST_F(VariantUnknownTypeTest, UnknownPrimitiveTypeValueSize) {
  // ValueSize on an unknown primitive type should still return a value
  // (PrimitiveValueSize returns -1, triggering variable-length path).
  // With only 1 byte, variable-length path requires 5 bytes → truncated.
  uint8_t data[] = {0x64};
  ASSERT_RAISES(Invalid, ValueSize(data, sizeof(data)));
}

// ===========================================================================
// Array non-monotonic offset rejection test
// ===========================================================================

class VariantArrayNonMonotonicTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantArrayNonMonotonicTest, RejectsNonMonotonicOffsets) {
  // Manually craft an array with 2 elements where offsets go [0, 3, 1]
  // (non-monotonic: 1 < 3). This should be rejected.
  // header: basic_type=3, offset_size=1, is_large=false → 0x03
  // num_elements: 2
  // offsets: [0, 3, 1] — non-monotonic
  // data: 3 bytes of nulls
  uint8_t data[] = {
      0x03,  // array header: basic_type=3, offset_size=1, is_large=false
      0x02,  // num_elements = 2
      0x00,
      0x03,
      0x01,  // offsets: [0, 3, 1] — non-monotonic!
      PrimitiveHeader(PrimitiveType::kNull),
      PrimitiveHeader(PrimitiveType::kNull),
      PrimitiveHeader(PrimitiveType::kNull),
  };
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(empty_metadata_, data, sizeof(data), &visitor));
}

// ===========================================================================
// Object field offset out-of-bounds test
// ===========================================================================

class VariantObjectOffsetBoundsTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = false;
    metadata_.offset_size = 1;
    metadata_.strings = {"key"};
  }
};

TEST_F(VariantObjectOffsetBoundsTest, FieldOffsetExceedsDataSize) {
  // Object with 1 field where field_offset[0] = 99 (beyond total_data_size).
  // header: basic_type=2, offset_size=1, id_size=1, is_large=false → 0x02
  // num_fields: 1
  // field_ids: [0]
  // offsets: [99, 2] — field 0 at offset 99, total=2
  // data: 2 bytes (Null)
  uint8_t data[] = {
      0x02,  // object header
      0x01,  // num_fields = 1
      0x00,  // field_id[0] = 0
      0x63,
      0x02,  // offsets: [99, 2] — 99 > total_data_size(2)
      PrimitiveHeader(PrimitiveType::kNull),
      PrimitiveHeader(PrimitiveType::kNull),
  };
  RecordingVisitor visitor;
  ASSERT_RAISES(Invalid, DecodeAndVisit(metadata_, data, sizeof(data), &visitor));
}

// ===========================================================================
// Empty metadata with various offset sizes
// ===========================================================================

class VariantMetadataOffsetSizeTest : public ::testing::Test {};

TEST_F(VariantMetadataOffsetSizeTest, EmptyDictionaryOffsetSize4) {
  // Valid metadata with 0 strings but offset_size=4.
  auto buf = BuildMetadataBuffer({}, false, 4);
  ASSERT_OK_AND_ASSIGN(auto metadata, DecodeMetadata(buf.data(), buf.size()));
  ASSERT_EQ(metadata.version, 1);
  ASSERT_EQ(metadata.offset_size, 4);
  ASSERT_EQ(metadata.strings.size(), 0);
}

// ===========================================================================
// FindObjectField with binary search (large object >= 32 fields)
// ===========================================================================

class VariantFindFieldBinarySearchTest : public ::testing::Test {
 protected:
  VariantMetadata metadata_;
  // Backing storage for string_views in metadata (must outlive metadata_).
  // Do NOT modify key_storage_ after SetUp(); reallocation invalidates
  // the string_views stored in metadata_.strings.
  std::vector<std::string> key_storage_;

  void SetUp() override {
    metadata_.version = 1;
    metadata_.is_sorted = true;
    metadata_.offset_size = 1;
    // 40 keys in sorted order to trigger binary search path
    key_storage_.reserve(40);
    for (int i = 0; i < 40; ++i) {
      std::string key = "k" + std::string(i < 10 ? "0" : "") + std::to_string(i);
      key_storage_.emplace_back(key);
    }
    for (const auto& k : key_storage_) {
      metadata_.strings.push_back(k);
    }
  }
};

TEST_F(VariantFindFieldBinarySearchTest, FindMiddleField) {
  // Build object with 40 fields, all null values
  std::vector<uint32_t> field_ids;
  std::vector<std::vector<uint8_t>> values;
  for (int i = 0; i < 40; ++i) {
    field_ids.push_back(static_cast<uint32_t>(i));
    values.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildObject(field_ids, values);

  // Search for "k20" (middle of the sorted range)
  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "k20", &field_offset, &field_size));
  ASSERT_GT(field_offset, 0);
  ASSERT_EQ(field_size, 1);  // Null = 1 byte
}

TEST_F(VariantFindFieldBinarySearchTest, FindFirstField) {
  std::vector<uint32_t> field_ids;
  std::vector<std::vector<uint8_t>> values;
  for (int i = 0; i < 40; ++i) {
    field_ids.push_back(static_cast<uint32_t>(i));
    values.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildObject(field_ids, values);

  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "k00", &field_offset, &field_size));
  ASSERT_GT(field_offset, 0);
}

TEST_F(VariantFindFieldBinarySearchTest, FindLastField) {
  std::vector<uint32_t> field_ids;
  std::vector<std::vector<uint8_t>> values;
  for (int i = 0; i < 40; ++i) {
    field_ids.push_back(static_cast<uint32_t>(i));
    values.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildObject(field_ids, values);

  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "k39", &field_offset, &field_size));
  ASSERT_GT(field_offset, 0);
}

TEST_F(VariantFindFieldBinarySearchTest, NotFoundInLargeObject) {
  std::vector<uint32_t> field_ids;
  std::vector<std::vector<uint8_t>> values;
  for (int i = 0; i < 40; ++i) {
    field_ids.push_back(static_cast<uint32_t>(i));
    values.push_back({PrimitiveHeader(PrimitiveType::kNull)});
  }
  auto data = BuildObject(field_ids, values);

  int64_t field_offset = -1, field_size = 0;
  ASSERT_OK(FindObjectField(metadata_, data.data(), static_cast<int64_t>(data.size()),
                            "zzz", &field_offset, &field_size));
  ASSERT_EQ(field_offset, -1);
}

// ===========================================================================
// GetArrayElement middle index
// ===========================================================================

class VariantGetArrayElementExtraTest : public ::testing::Test {};

TEST_F(VariantGetArrayElementExtraTest, MiddleElement) {
  // Array of [Int32(10), Int32(20), Int32(30)]
  std::vector<uint8_t> e0 = {PrimitiveHeader(PrimitiveType::kInt32), 10, 0, 0, 0};
  std::vector<uint8_t> e1 = {PrimitiveHeader(PrimitiveType::kInt32), 20, 0, 0, 0};
  std::vector<uint8_t> e2 = {PrimitiveHeader(PrimitiveType::kInt32), 30, 0, 0, 0};
  auto data = BuildArray({e0, e1, e2});

  int64_t elem_offset = 0, elem_size = 0;
  ASSERT_OK(GetArrayElement(data.data(), static_cast<int64_t>(data.size()), 1,
                            &elem_offset, &elem_size));
  ASSERT_EQ(elem_size, 5);  // Int32 = 5 bytes

  // Decode the middle element
  VariantMetadata meta;
  meta.version = 1;
  RecordingVisitor v;
  ASSERT_OK(DecodeAndVisit(meta, data.data() + elem_offset, elem_size, &v));
  ASSERT_EQ(v.events[0], "Int32(20)");
}

TEST_F(VariantGetArrayElementExtraTest, EmptyArrayOutOfRange) {
  auto data = BuildArray({});
  int64_t elem_offset = 0, elem_size = 0;
  ASSERT_RAISES(Invalid, GetArrayElement(data.data(), static_cast<int64_t>(data.size()),
                                         0, &elem_offset, &elem_size));
}

// ===========================================================================
// Additional error case tests (missing coverage)
// ===========================================================================

class VariantErrorCaseTest : public ::testing::Test {
 protected:
  VariantMetadata empty_metadata_;

  void SetUp() override {
    empty_metadata_.version = 1;
    empty_metadata_.is_sorted = false;
    empty_metadata_.offset_size = 1;
  }
};

TEST_F(VariantErrorCaseTest, MetadataVersionZero) {
  // Version 0 is not supported (only version 1 is valid per spec)
  uint8_t data[] = {0x00, 0x00, 0x00};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantErrorCaseTest, GetObjectFieldCountOnArray) {
  // Calling GetObjectFieldCount on an array value should produce an error
  auto data = BuildArray({});
  ASSERT_RAISES(Invalid,
                GetObjectFieldCount(data.data(), static_cast<int64_t>(data.size())));
}

TEST_F(VariantErrorCaseTest, GetArrayElementCountOnObject) {
  // Calling GetArrayElementCount on an object value should produce an error
  auto data = BuildObject({}, {});
  ASSERT_RAISES(Invalid,
                GetArrayElementCount(data.data(), static_cast<int64_t>(data.size())));
}

TEST_F(VariantErrorCaseTest, GetObjectFieldCountOnPrimitive) {
  // Calling GetObjectFieldCount on a primitive should produce an error
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kNull)};
  ASSERT_RAISES(Invalid, GetObjectFieldCount(data, sizeof(data)));
}

TEST_F(VariantErrorCaseTest, GetArrayElementCountOnPrimitive) {
  // Calling GetArrayElementCount on a primitive should produce an error
  uint8_t data[] = {PrimitiveHeader(PrimitiveType::kNull)};
  ASSERT_RAISES(Invalid, GetArrayElementCount(data, sizeof(data)));
}

TEST_F(VariantErrorCaseTest, MetadataStringOffsetExceedsBuffer) {
  // Metadata where the last string offset claims more data than the buffer
  // contains. This exercises the ValidateOffsets check for offsets.back() >
  // data_length.
  // Header: version=1, offset_size=1
  // dict_size=1, offsets=[0, 100] — but only 3 bytes of string data
  uint8_t data[] = {0x01,        // header: version=1, offset_size=1
                    0x01,        // dict_size = 1
                    0x00, 0x64,  // offsets: [0, 100] — 100 exceeds available string data
                    'a',  'b',  'c'};
  ASSERT_RAISES(Invalid, DecodeMetadata(data, sizeof(data)));
}

TEST_F(VariantErrorCaseTest, GetArrayElementNegativeIndex) {
  std::vector<uint8_t> e0 = {PrimitiveHeader(PrimitiveType::kNull)};
  auto data = BuildArray({e0});
  int64_t elem_offset = 0, elem_size = 0;
  ASSERT_RAISES(Invalid, GetArrayElement(data.data(), static_cast<int64_t>(data.size()),
                                         -1, &elem_offset, &elem_size));
}

TEST_F(VariantErrorCaseTest, FindObjectFieldOnNonObject) {
  // Calling FindObjectField on an array should produce an error
  auto data = BuildArray({});
  int64_t field_offset = -1, field_size = 0;
  ASSERT_RAISES(Invalid, FindObjectField(empty_metadata_, data.data(),
                                         static_cast<int64_t>(data.size()), "key",
                                         &field_offset, &field_size));
}

// TODO: Add fuzz targets for DecodeMetadata and DecodeVariantValue to exercise
// adversarial/malformed input. Fuzz tests in Arrow are typically registered as
// separate executables under cpp/src/arrow/testing/fuzzing/ — see GH-45948.

// ===========================================================================
// View API tests (new — demonstrates C++ ergonomic approach)
// ===========================================================================

class VariantViewTest : public ::testing::Test {};

TEST_F(VariantViewTest, PrimitiveInt32) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  // Build an Int32 value: header + 4 bytes LE
  std::vector<uint8_t> data = {PrimitiveHeader(PrimitiveType::kInt32), 0x2A, 0x00, 0x00,
                               0x00};

  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(view.type(), BasicType::kPrimitive);
  ASSERT_FALSE(view.is_null());
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int32());
  ASSERT_EQ(val, 42);
  ASSERT_EQ(view.size_bytes(), 5);
}

TEST_F(VariantViewTest, PrimitiveNull) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  std::vector<uint8_t> data = {PrimitiveHeader(PrimitiveType::kNull)};
  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));
  ASSERT_TRUE(view.is_null());
  ASSERT_EQ(view.size_bytes(), 1);
}

TEST_F(VariantViewTest, ShortString) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  auto data = BuildShortString("hello");
  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(view.type(), BasicType::kShortString);
  ASSERT_OK_AND_ASSIGN(auto str, view.as_string());
  ASSERT_EQ(str, "hello");
}

TEST_F(VariantViewTest, TypeMismatchReturnsError) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  // Build a boolean true value
  std::vector<uint8_t> data = {PrimitiveHeader(PrimitiveType::kTrue)};
  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));

  // Accessing as wrong type should fail
  ASSERT_RAISES(Invalid, view.as_int32());
  ASSERT_RAISES(Invalid, view.as_string());
  ASSERT_RAISES(Invalid, view.as_object());
}

class VariantObjectViewTest : public ::testing::Test {};

TEST_F(VariantObjectViewTest, SimpleObject) {
  // Build {"name": "Alice", "age": 42}
  auto meta_buf = BuildMetadataBuffer({"age", "name"}, /*sorted=*/true);
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  // Field values
  auto val_name = BuildShortString("Alice");
  std::vector<uint8_t> val_age = {PrimitiveHeader(PrimitiveType::kInt8), 42};

  // Build object: field IDs sorted by key name → age=0, name=1
  auto data = BuildObject({0, 1}, {val_age, val_name});

  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(view.type(), BasicType::kObject);

  ASSERT_OK_AND_ASSIGN(auto obj, view.as_object());
  ASSERT_EQ(obj.num_fields(), 2);

  // Lookup by name
  auto age = obj.get("age");
  ASSERT_TRUE(age.has_value());
  ASSERT_OK_AND_ASSIGN(auto age_val, age->as_int8());
  ASSERT_EQ(age_val, 42);

  auto name = obj.get("name");
  ASSERT_TRUE(name.has_value());
  ASSERT_OK_AND_ASSIGN(auto name_val, name->as_string());
  ASSERT_EQ(name_val, "Alice");

  // Not found
  auto missing = obj.get("nonexistent");
  ASSERT_FALSE(missing.has_value());
}

TEST_F(VariantObjectViewTest, NestedNavigation) {
  // Build: {"addresses": {"postal": {"city": "New York"}}}
  // This test addresses reviewer comment #4 (nested field navigation)
  auto meta_buf = BuildMetadataBuffer({"addresses", "city", "postal"}, /*sorted=*/true);
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  // innermost: {"city": "New York"} — field_id for "city" = 1
  auto val_city = BuildShortString("New York");
  auto inner_obj = BuildObject({1}, {val_city});

  // middle: {"postal": <inner_obj>} — field_id for "postal" = 2
  auto mid_obj = BuildObject({2}, {inner_obj});

  // outer: {"addresses": <mid_obj>} — field_id for "addresses" = 0
  auto outer_obj = BuildObject({0}, {mid_obj});

  ASSERT_OK_AND_ASSIGN(
      auto root,
      VariantView::Make(meta, outer_obj.data(), static_cast<int64_t>(outer_obj.size())));

  // Navigate: root -> addresses -> postal -> city
  ASSERT_OK_AND_ASSIGN(auto root_obj, root.as_object());
  auto addresses = root_obj.get("addresses");
  ASSERT_TRUE(addresses.has_value());

  ASSERT_OK_AND_ASSIGN(auto addr_obj, addresses->as_object());
  auto postal = addr_obj.get("postal");
  ASSERT_TRUE(postal.has_value());

  ASSERT_OK_AND_ASSIGN(auto postal_obj, postal->as_object());
  auto city = postal_obj.get("city");
  ASSERT_TRUE(city.has_value());

  ASSERT_OK_AND_ASSIGN(auto city_val, city->as_string());
  ASSERT_EQ(city_val, "New York");
}

TEST_F(VariantObjectViewTest, IterateFields) {
  auto meta_buf = BuildMetadataBuffer({"a", "b", "c"}, /*sorted=*/true);
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  std::vector<uint8_t> val_a = {PrimitiveHeader(PrimitiveType::kInt8), 1};
  std::vector<uint8_t> val_b = {PrimitiveHeader(PrimitiveType::kInt8), 2};
  std::vector<uint8_t> val_c = {PrimitiveHeader(PrimitiveType::kInt8), 3};
  auto data = BuildObject({0, 1, 2}, {val_a, val_b, val_c});

  ASSERT_OK_AND_ASSIGN(
      auto obj,
      VariantObjectView::Make(meta, data.data(), static_cast<int64_t>(data.size())));

  std::vector<std::string> names;
  for (auto [name, value] : obj) {
    names.push_back(std::string(name));
  }
  ASSERT_EQ(names.size(), 3);
  ASSERT_EQ(names[0], "a");
  ASSERT_EQ(names[1], "b");
  ASSERT_EQ(names[2], "c");
}

class VariantArrayViewTest : public ::testing::Test {};

TEST_F(VariantArrayViewTest, SimpleArray) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  // Build array [42, 100]
  std::vector<uint8_t> val_a = {PrimitiveHeader(PrimitiveType::kInt8), 42};
  std::vector<uint8_t> val_b = {PrimitiveHeader(PrimitiveType::kInt8), 100};
  auto data = BuildArray({val_a, val_b});

  ASSERT_OK_AND_ASSIGN(
      auto view, VariantView::Make(meta, data.data(), static_cast<int64_t>(data.size())));
  ASSERT_EQ(view.type(), BasicType::kArray);

  ASSERT_OK_AND_ASSIGN(auto arr, view.as_array());
  ASSERT_EQ(arr.num_elements(), 2);

  ASSERT_OK_AND_ASSIGN(auto elem0, arr.get(0));
  ASSERT_OK_AND_ASSIGN(auto v0, elem0.as_int8());
  ASSERT_EQ(v0, 42);

  ASSERT_OK_AND_ASSIGN(auto elem1, arr.get(1));
  ASSERT_OK_AND_ASSIGN(auto v1, elem1.as_int8());
  ASSERT_EQ(v1, 100);

  // Out of range
  ASSERT_RAISES(Invalid, arr.get(2));
  ASSERT_RAISES(Invalid, arr.get(-1));
}

TEST_F(VariantArrayViewTest, IterateElements) {
  auto meta_buf = BuildMetadataBuffer({});
  ASSERT_OK_AND_ASSIGN(
      auto meta, DecodeMetadata(meta_buf.data(), static_cast<int64_t>(meta_buf.size())));

  std::vector<uint8_t> val_a = {PrimitiveHeader(PrimitiveType::kInt8), 10};
  std::vector<uint8_t> val_b = {PrimitiveHeader(PrimitiveType::kInt8), 20};
  std::vector<uint8_t> val_c = {PrimitiveHeader(PrimitiveType::kInt8), 30};
  auto data = BuildArray({val_a, val_b, val_c});

  ASSERT_OK_AND_ASSIGN(
      auto arr,
      VariantArrayView::Make(meta, data.data(), static_cast<int64_t>(data.size())));

  int count = 0;
  for ([[maybe_unused]] auto elem : arr) {
    ++count;
  }
  ASSERT_EQ(count, 3);
}
// ===========================================================================
// Widening numeric accessor tests
// ===========================================================================

class VariantCoercionTest : public ::testing::Test {};

TEST_F(VariantCoercionTest, Int8CoercesToInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int8(42));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int64_coerced());
  ASSERT_EQ(val, 42);
}

TEST_F(VariantCoercionTest, Int16CoercesToInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int16(1000));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int64_coerced());
  ASSERT_EQ(val, 1000);
}

TEST_F(VariantCoercionTest, Int32CoercesToInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int32(100000));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int64_coerced());
  ASSERT_EQ(val, 100000);
}

TEST_F(VariantCoercionTest, Int64CoercesToInt64Identity) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int64(9876543210LL));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int64_coerced());
  ASSERT_EQ(val, 9876543210LL);
}

TEST_F(VariantCoercionTest, NegativeInt8CoercesToInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int8(-42));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_int64_coerced());
  ASSERT_EQ(val, -42);
}

TEST_F(VariantCoercionTest, StringDoesNotCoerceToInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.String("hello"));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_NOT_OK(view.as_int64_coerced());
}

TEST_F(VariantCoercionTest, Int32CoercedRejectsInt64) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int64(9876543210LL));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_NOT_OK(view.as_int32_coerced());
}

TEST_F(VariantCoercionTest, DoubleCoercedFromFloat) {
  VariantBuilder builder;
  ASSERT_OK(builder.Float(3.14f));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_double_coerced());
  ASSERT_NEAR(val, 3.14, 0.001);
}

TEST_F(VariantCoercionTest, DoubleCoercedFromInt32) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int32(42));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK_AND_ASSIGN(
      auto view,
      VariantView::Make(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
  ASSERT_OK_AND_ASSIGN(auto val, view.as_double_coerced());
  ASSERT_EQ(val, 42.0);
}

// ===========================================================================
// ValidateVariant tests
// ===========================================================================

class VariantValidationTest : public ::testing::Test {};

TEST_F(VariantValidationTest, ValidPrimitive) {
  VariantBuilder builder;
  ASSERT_OK(builder.Int(42));
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK(
      ValidateVariant(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
}

TEST_F(VariantValidationTest, ValidNestedObject) {
  VariantBuilder builder;
  auto obj = builder.StartObject();
  ASSERT_OK(obj.Insert("name", std::string_view("Alice")));
  ASSERT_OK(obj.Insert("age", static_cast<int64_t>(30)));
  auto inner = obj.InsertObject("address");
  ASSERT_OK(inner.Insert("city", std::string_view("NYC")));
  ASSERT_OK(inner.Finish());
  ASSERT_OK(obj.Finish());
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK(
      ValidateVariant(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
}

TEST_F(VariantValidationTest, ValidArray) {
  VariantBuilder builder;
  auto list = builder.StartList();
  ASSERT_OK(list.Append(static_cast<int64_t>(1)));
  ASSERT_OK(list.Append(static_cast<int64_t>(2)));
  ASSERT_OK(list.Append(static_cast<int64_t>(3)));
  ASSERT_OK(list.Finish());
  ASSERT_OK_AND_ASSIGN(auto enc, builder.Finish());
  ASSERT_OK_AND_ASSIGN(
      auto meta,
      DecodeMetadata(enc.metadata.data(), static_cast<int64_t>(enc.metadata.size())));
  ASSERT_OK(
      ValidateVariant(meta, enc.value.data(), static_cast<int64_t>(enc.value.size())));
}

TEST_F(VariantValidationTest, NullBuffer) {
  ASSERT_NOT_OK(ValidateVariant(VariantMetadata{}, nullptr, 0));
}

TEST_F(VariantValidationTest, TruncatedPrimitive) {
  // A valid Int32 header (type=5 << 2 | 0 = 20 = 0x14) but no payload
  uint8_t data[] = {0x14};
  VariantMetadata meta;
  meta.version = 1;
  ASSERT_NOT_OK(ValidateVariant(meta, data, 1));
}

}  // namespace arrow::extension::variant
