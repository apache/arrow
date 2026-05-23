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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/extension/range.h"
#include "arrow/extension_type.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_pointer_cast;

// ---------------------------------------------------------------------------
// Helpers

static std::shared_ptr<extension::RangeType> RangeInt32Right() {
  return checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), extension::RangeClosed::Right));
}

static std::shared_ptr<extension::RangeType> RangeInt32Both() {
  return checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), extension::RangeClosed::Both));
}

static std::shared_ptr<extension::RangeType> RangeInt64Left() {
  return checked_pointer_cast<extension::RangeType>(
      extension::range(int64(), extension::RangeClosed::Left));
}

// ---------------------------------------------------------------------------
// Basics

TEST(RangeType, Basics) {
  auto type = RangeInt32Right();
  ASSERT_EQ("arrow.range", type->extension_name());
  ASSERT_EQ(*int32(), *type->value_type());
  ASSERT_EQ(extension::RangeClosed::Right, type->closed());
  ASSERT_EQ(*type, *type);
  ASSERT_NE(*arrow::null(), *type);
  ASSERT_THAT(type->Serialize(), ::testing::Not(::testing::IsEmpty()));
  ASSERT_EQ(R"({"closed":"right"})", type->Serialize());
  ASSERT_EQ("extension<arrow.range[value_type=int32, closed=right]>",
            type->ToString(false));
}

TEST(RangeType, AllClosedValues) {
  using C = extension::RangeClosed;
  auto left = checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), C::Left));
  auto right = checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), C::Right));
  auto both = checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), C::Both));
  auto neither = checked_pointer_cast<extension::RangeType>(
      extension::range(int32(), C::Neither));

  ASSERT_EQ(R"({"closed":"left"})", left->Serialize());
  ASSERT_EQ(R"({"closed":"right"})", right->Serialize());
  ASSERT_EQ(R"({"closed":"both"})", both->Serialize());
  ASSERT_EQ(R"({"closed":"neither"})", neither->Serialize());
}

// ---------------------------------------------------------------------------
// Equals

TEST(RangeType, Equals) {
  auto type_i32_right = RangeInt32Right();
  auto type_i32_both = RangeInt32Both();
  auto type_i64_left = RangeInt64Left();
  auto type_i32_right2 = RangeInt32Right();

  // Same object.
  ASSERT_EQ(*type_i32_right, *type_i32_right);

  // Different instances but same parameters.
  ASSERT_EQ(*type_i32_right, *type_i32_right2);

  // Different closed value.
  ASSERT_NE(*type_i32_right, *type_i32_both);

  // Different value_type.
  ASSERT_NE(*type_i32_right, *type_i64_left);

  // Not equal to a non-range type.
  ASSERT_NE(*type_i32_right, *arrow::null());
  ASSERT_NE(*type_i32_right, *arrow::int32());
}

// ---------------------------------------------------------------------------
// CreateFromArray

TEST(RangeType, CreateFromArray) {
  auto type = RangeInt32Right();
  // Build a StructArray that matches the storage type.
  auto storage_type = type->storage_type();
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  ASSERT_OK_AND_ASSIGN(auto storage, StructArray::Make({lower, upper},
                                                       {field("lower", int32(), true),
                                                        field("upper", int32(), true)}));
  auto array = ExtensionType::WrapArray(type, storage);
  ASSERT_EQ(3, array->length());
  ASSERT_EQ(0, array->null_count());
}

// ---------------------------------------------------------------------------
// Deserialize - valid cases

void CheckDeserialize(const std::string& serialized,
                      const std::shared_ptr<DataType>& expected) {
  auto type = checked_pointer_cast<extension::RangeType>(expected);
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       type->Deserialize(type->storage_type(), serialized));
  ASSERT_EQ(*expected, *deserialized);
}

TEST(RangeType, Deserialize) {
  // Normal JSON
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"closed": "right"})",
                       extension::range(int32(), extension::RangeClosed::Right)));
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"closed": "left"})",
                       extension::range(int32(), extension::RangeClosed::Left)));
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"closed": "both"})",
                       extension::range(int32(), extension::RangeClosed::Both)));
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"closed": "neither"})",
                       extension::range(int32(), extension::RangeClosed::Neither)));

  // Extra fields are tolerated (forward-compatibility).
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize(R"({"closed": "right", "extra": 42})",
                       extension::range(int32(), extension::RangeClosed::Right)));

  // Empty metadata defaults to "right".
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize("", extension::range(int32(), extension::RangeClosed::Right)));

  // Empty JSON object (no "closed" key) also defaults to "right".
  ASSERT_NO_FATAL_FAILURE(
      CheckDeserialize("{}", extension::range(int32(), extension::RangeClosed::Right)));
}

// ---------------------------------------------------------------------------
// Deserialize - invalid cases

TEST(RangeType, DeserializeInvalidMetadata) {
  auto type = RangeInt32Right();

  // Empty string is valid (defaults to "right"); truly malformed JSON fails.
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Missing a name for object member"),
      type->Deserialize(type->storage_type(), "{"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("not an object"),
                                  type->Deserialize(type->storage_type(), "[]"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("\"closed\" is not a string"),
      type->Deserialize(type->storage_type(), R"({"closed": 42})"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid value for RangeType"),
      type->Deserialize(type->storage_type(), R"({"closed": "unknown"})"));
}

TEST(RangeType, DeserializeInvalidStorage) {
  auto type = RangeInt32Right();
  auto wrong_storage_not_struct = int32();

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("must be a Struct"),
      type->Deserialize(wrong_storage_not_struct, R"({"closed":"right"})"));

  // Wrong number of fields.
  auto one_field = struct_({field("lower", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("exactly 2 fields"),
      type->Deserialize(one_field, R"({"closed":"right"})"));

  // Wrong field name for field 0.
  auto bad_lower_name =
      struct_({field("start", int32(), true), field("upper", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("named \"lower\""),
      type->Deserialize(bad_lower_name, R"({"closed":"right"})"));

  // Wrong field name for field 1.
  auto bad_upper_name =
      struct_({field("lower", int32(), true), field("end", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("named \"upper\""),
      type->Deserialize(bad_upper_name, R"({"closed":"right"})"));

  // Fields have different types.
  auto mismatched_types =
      struct_({field("lower", int32(), true), field("upper", int64(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("same type"),
      type->Deserialize(mismatched_types, R"({"closed":"right"})"));

  // Non-nullable lower field.
  auto lower_not_nullable =
      struct_({field("lower", int32(), /*nullable=*/false), field("upper", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("\"lower\" must be nullable"),
      type->Deserialize(lower_not_nullable, R"({"closed":"right"})"));

  // Non-nullable upper field.
  auto upper_not_nullable =
      struct_({field("lower", int32(), true), field("upper", int32(), /*nullable=*/false)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("\"upper\" must be nullable"),
      type->Deserialize(upper_not_nullable, R"({"closed":"right"})"));
}

// ---------------------------------------------------------------------------
// Metadata (Serialize/Deserialize) round-trip

TEST(RangeType, MetadataRoundTrip) {
  using C = extension::RangeClosed;
  for (const auto& type :
       {extension::range(int32(), C::Left), extension::range(int32(), C::Right),
        extension::range(int32(), C::Both), extension::range(int32(), C::Neither),
        extension::range(int64(), C::Right), extension::range(date32(), C::Both)}) {
    auto rt = checked_pointer_cast<extension::RangeType>(type);
    std::string serialized = rt->Serialize();
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         rt->Deserialize(rt->storage_type(), serialized));
    ASSERT_EQ(*type, *deserialized) << "Round-trip failed for: " << type->ToString();
  }
}

// ---------------------------------------------------------------------------
// IPC (BatchRoundTrip) -- registration round-trip

TEST(RangeType, BatchRoundTrip) {
  auto type = RangeInt32Right();
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  ASSERT_OK_AND_ASSIGN(auto storage, StructArray::Make({lower, upper},
                                                       {field("lower", int32(), true),
                                                        field("upper", int32(), true)}));
  auto array = ExtensionType::WrapArray(type, storage);
  auto batch =
      RecordBatch::Make(schema({field("rng", type)}), array->length(), {array});

  std::shared_ptr<RecordBatch> written;
  {
    ASSERT_OK_AND_ASSIGN(auto out_stream, io::BufferOutputStream::Create());
    ASSERT_OK(ipc::WriteRecordBatchStream({batch}, ipc::IpcWriteOptions::Defaults(),
                                          out_stream.get()));
    ASSERT_OK_AND_ASSIGN(auto complete_ipc_stream, out_stream->Finish());

    io::BufferReader reader(complete_ipc_stream);
    std::shared_ptr<RecordBatchReader> batch_reader;
    ASSERT_OK_AND_ASSIGN(batch_reader, ipc::RecordBatchStreamReader::Open(&reader));
    ASSERT_OK(batch_reader->ReadNext(&written));
  }

  ASSERT_EQ(*batch->schema(), *written->schema());
  ASSERT_BATCHES_EQUAL(*batch, *written);
}

}  // namespace arrow
