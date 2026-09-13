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

#include "arrow/array/array_nested.h"
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

static std::shared_ptr<extension::FixedClosednessRangeType> RangeInt32Right() {
  return checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Right));
}

static std::shared_ptr<extension::FixedClosednessRangeType> RangeInt32Both() {
  return checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Both));
}

static std::shared_ptr<extension::FixedClosednessRangeType> RangeInt64Left() {
  return checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int64(), extension::RangeClosed::Left));
}

// ---------------------------------------------------------------------------
// Basics

TEST(FixedClosednessRangeType, Basics) {
  auto type = RangeInt32Right();
  ASSERT_EQ("arrow.fixed_closedness_range", type->extension_name());
  ASSERT_EQ(*int32(), *type->value_type());
  ASSERT_EQ(extension::RangeClosed::Right, type->closed());
  ASSERT_EQ(*type, *type);
  ASSERT_NE(*arrow::null(), *type);
  ASSERT_THAT(type->Serialize(), ::testing::Not(::testing::IsEmpty()));
  ASSERT_EQ(R"({"closed":"right"})", type->Serialize());
  ASSERT_EQ("extension<arrow.fixed_closedness_range[value_type=int32, closed=right]>",
            type->ToString(false));
}

TEST(FixedClosednessRangeType, AllClosedValues) {
  using C = extension::RangeClosed;
  auto left = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), C::Left));
  auto right = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), C::Right));
  auto both = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), C::Both));
  auto neither = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), C::Neither));

  ASSERT_EQ(R"({"closed":"left"})", left->Serialize());
  ASSERT_EQ(R"({"closed":"right"})", right->Serialize());
  ASSERT_EQ(R"({"closed":"both"})", both->Serialize());
  ASSERT_EQ(R"({"closed":"neither"})", neither->Serialize());
}

// ---------------------------------------------------------------------------
// Equals

TEST(FixedClosednessRangeType, Equals) {
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

TEST(FixedClosednessRangeType, CreateFromArray) {
  auto type = RangeInt32Right();
  // Build a StructArray that matches the storage type.
  auto storage_type = type->storage_type();
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  ASSERT_OK_AND_ASSIGN(
      auto storage, StructArray::Make({lower, upper}, {field("lower", int32(), true),
                                                       field("upper", int32(), true)}));
  auto array = ExtensionType::WrapArray(type, storage);
  ASSERT_EQ(3, array->length());
  ASSERT_EQ(0, array->null_count());
}

// ---------------------------------------------------------------------------
// Deserialize - valid cases

namespace {

void CheckRangeDeserialize(const std::string& serialized,
                           const std::shared_ptr<DataType>& expected) {
  auto type = checked_pointer_cast<extension::FixedClosednessRangeType>(expected);
  ASSERT_OK_AND_ASSIGN(auto deserialized,
                       type->Deserialize(type->storage_type(), serialized));
  ASSERT_EQ(*expected, *deserialized);
}

}  // namespace

TEST(FixedClosednessRangeType, Deserialize) {
  // Normal JSON
  ASSERT_NO_FATAL_FAILURE(CheckRangeDeserialize(
      R"({"closed": "right"})",
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Right)));
  ASSERT_NO_FATAL_FAILURE(CheckRangeDeserialize(
      R"({"closed": "left"})",
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Left)));
  ASSERT_NO_FATAL_FAILURE(CheckRangeDeserialize(
      R"({"closed": "both"})",
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Both)));
  ASSERT_NO_FATAL_FAILURE(CheckRangeDeserialize(
      R"({"closed": "neither"})",
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Neither)));

  // Extra fields are tolerated (forward-compatibility).
  ASSERT_NO_FATAL_FAILURE(CheckRangeDeserialize(
      R"({"closed": "right", "extra": 42})",
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Right)));
}

TEST(FixedClosednessRangeType, DefaultClosedIsLeft) {
  // The C++ convenience default is left-closed; the wire format still always
  // carries an explicit "closed".
  auto type = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32()));
  ASSERT_EQ(extension::RangeClosed::Left, type->closed());
  ASSERT_EQ(R"({"closed":"left"})", type->Serialize());
}

// ---------------------------------------------------------------------------
// Deserialize - invalid cases

TEST(FixedClosednessRangeType, DeserializeInvalidMetadata) {
  auto type = RangeInt32Right();

  // "closed" is required on the wire: empty metadata is invalid.
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("empty string"),
                                  type->Deserialize(type->storage_type(), ""));

  // A JSON object without the "closed" key is invalid.
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("missing the required \"closed\" key"),
      type->Deserialize(type->storage_type(), "{}"));

  // Truly malformed JSON fails.
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid serialized JSON data"),
                                  type->Deserialize(type->storage_type(), "{"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid serialized JSON data"),
                                  type->Deserialize(type->storage_type(), "[]"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("\"closed\" is not a string"),
      type->Deserialize(type->storage_type(), R"({"closed": 42})"));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("Invalid value for FixedClosednessRangeType"),
      type->Deserialize(type->storage_type(), R"({"closed": "unknown"})"));
}

TEST(FixedClosednessRangeType, DeserializeInvalidStorage) {
  auto type = RangeInt32Right();
  auto wrong_storage_not_struct = int32();

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, testing::HasSubstr("must be a Struct"),
      type->Deserialize(wrong_storage_not_struct, R"({"closed":"right"})"));

  // Wrong number of fields.
  auto one_field = struct_({field("lower", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("exactly 2 fields"),
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
}

// ---------------------------------------------------------------------------
// Non-nullable / asymmetric bounds
//
// Bound nullability is only needed to represent an unbounded (infinite)
// endpoint; non-nullable bounds describe a finite-only range and are accepted.

TEST(FixedClosednessRangeType, NonNullableBounds) {
  auto type = RangeInt32Right();

  // Both bounds non-nullable: accepted (a finite-only range).
  auto both_non_nullable = struct_({field("lower", int32(), /*nullable=*/false),
                                    field("upper", int32(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(auto from_non_nullable,
                       type->Deserialize(both_non_nullable, R"({"closed":"right"})"));
  ASSERT_EQ(*int32(),
            *checked_pointer_cast<extension::FixedClosednessRangeType>(from_non_nullable)
                 ->value_type());

  // Asymmetric: lower nullable (may be -inf), upper non-nullable (always finite).
  auto asymmetric = struct_({field("lower", int32(), /*nullable=*/true),
                             field("upper", int32(), /*nullable=*/false)});
  ASSERT_OK_AND_ASSIGN(auto from_asymmetric,
                       type->Deserialize(asymmetric, R"({"closed":"left"})"));
  ASSERT_EQ(extension::RangeClosed::Left,
            checked_pointer_cast<extension::FixedClosednessRangeType>(from_asymmetric)
                ->closed());

  // The factory can build non-nullable bounds via allow_unbounded=false.
  auto finite = checked_pointer_cast<extension::FixedClosednessRangeType>(
      extension::fixed_closedness_range(int32(), extension::RangeClosed::Both,
                                        /*allow_unbounded=*/false));
  const auto& finite_storage =
      internal::checked_cast<const StructType&>(*finite->storage_type());
  ASSERT_FALSE(finite_storage.field(0)->nullable());
  ASSERT_FALSE(finite_storage.field(1)->nullable());
}

// ---------------------------------------------------------------------------
// Metadata (Serialize/Deserialize) round-trip

TEST(FixedClosednessRangeType, MetadataRoundTrip) {
  using C = extension::RangeClosed;
  for (const auto& type : {extension::fixed_closedness_range(int32(), C::Left),
                           extension::fixed_closedness_range(int32(), C::Right),
                           extension::fixed_closedness_range(int32(), C::Both),
                           extension::fixed_closedness_range(int32(), C::Neither),
                           extension::fixed_closedness_range(int64(), C::Right),
                           extension::fixed_closedness_range(date32(), C::Both)}) {
    auto rt = checked_pointer_cast<extension::FixedClosednessRangeType>(type);
    std::string serialized = rt->Serialize();
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         rt->Deserialize(rt->storage_type(), serialized));
    ASSERT_EQ(*type, *deserialized) << "Round-trip failed for: " << type->ToString();
  }
}

// ---------------------------------------------------------------------------
// IPC (BatchRoundTrip) -- registration round-trip

TEST(FixedClosednessRangeType, BatchRoundTrip) {
  auto type = RangeInt32Right();
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  ASSERT_OK_AND_ASSIGN(
      auto storage, StructArray::Make({lower, upper}, {field("lower", int32(), true),
                                                       field("upper", int32(), true)}));
  auto array = ExtensionType::WrapArray(type, storage);
  auto batch = RecordBatch::Make(schema({field("rng", type)}), array->length(), {array});

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

// ===========================================================================
// VariableClosednessRangeType -- per-value bound inclusivity
// ===========================================================================

namespace {

std::shared_ptr<DataType> VariableClosednessStorage(
    const std::shared_ptr<DataType>& value_type, bool nullable_bounds = true) {
  return struct_({field("lower", value_type, nullable_bounds),
                  field("upper", value_type, nullable_bounds),
                  field("lower_inc", boolean(), /*nullable=*/false),
                  field("upper_inc", boolean(), /*nullable=*/false)});
}

}  // namespace

// ---------------------------------------------------------------------------
// Basics

TEST(VariableClosednessRangeType, Basics) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));
  ASSERT_EQ("arrow.variable_closedness_range", type->extension_name());
  ASSERT_EQ(*int32(), *type->value_type());
  ASSERT_EQ(*type, *type);
  ASSERT_NE(*arrow::null(), *type);
  // No type-level parameters: metadata is the empty JSON object.
  ASSERT_EQ("{}", type->Serialize());
  ASSERT_EQ("extension<arrow.variable_closedness_range[value_type=int32]>",
            type->ToString(false));
  // Storage carries the two non-nullable boolean inclusivity fields.
  const auto& storage = internal::checked_cast<const StructType&>(*type->storage_type());
  ASSERT_EQ(4, storage.num_fields());
  ASSERT_EQ("lower_inc", storage.field(2)->name());
  ASSERT_EQ("upper_inc", storage.field(3)->name());
  ASSERT_EQ(*boolean(), *storage.field(2)->type());
  ASSERT_FALSE(storage.field(2)->nullable());
  ASSERT_FALSE(storage.field(3)->nullable());
}

// ---------------------------------------------------------------------------
// Equals

TEST(VariableClosednessRangeType, Equals) {
  auto i32 = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));
  auto i32b = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));
  auto i64 = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int64()));
  auto i32_finite = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32(), /*allow_unbounded=*/false));

  // Same object / same parameters.
  ASSERT_EQ(*i32, *i32);
  ASSERT_EQ(*i32, *i32b);

  // Different value type.
  ASSERT_NE(*i32, *i64);

  // Different bound nullability is part of storage, hence a different type.
  ASSERT_NE(*i32, *i32_finite);

  // Not equal to non-range types, including a plain arrow.fixed_closedness_range.
  ASSERT_NE(*i32, *arrow::int32());
  ASSERT_NE(*i32, *extension::fixed_closedness_range(int32()));
}

// ---------------------------------------------------------------------------
// CreateFromArray

TEST(VariableClosednessRangeType, CreateFromArray) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  auto lower_inc = ArrayFromJSON(boolean(), "[true, false, true]");
  auto upper_inc = ArrayFromJSON(boolean(), "[false, false, true]");
  ASSERT_OK_AND_ASSIGN(auto storage,
                       StructArray::Make({lower, upper, lower_inc, upper_inc},
                                         type->storage_type()->fields()));
  auto array = ExtensionType::WrapArray(type, storage);
  ASSERT_EQ(3, array->length());
  ASSERT_EQ(0, array->null_count());
}

// ---------------------------------------------------------------------------
// Deserialize - valid cases (metadata carries no parameters)

TEST(VariableClosednessRangeType, DeserializeMetadata) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));

  // Empty string, empty object, and extra keys are all accepted.
  for (const auto& serialized :
       {std::string(""), std::string("{}"), std::string(R"({"extra": 42})")}) {
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         type->Deserialize(type->storage_type(), serialized));
    ASSERT_EQ(*type, *deserialized) << "Failed for metadata: " << serialized;
  }
}

// ---------------------------------------------------------------------------
// Deserialize - invalid cases

TEST(VariableClosednessRangeType, DeserializeInvalidMetadata) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid serialized JSON data"),
                                  type->Deserialize(type->storage_type(), "{"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid serialized JSON data"),
                                  type->Deserialize(type->storage_type(), "[]"));
}

TEST(VariableClosednessRangeType, DeserializeInvalidStorage) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));

  // Not a struct.
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("must be a Struct"),
                                  type->Deserialize(int32(), "{}"));

  // Wrong number of fields (a plain 2-field range struct).
  auto two_fields =
      struct_({field("lower", int32(), true), field("upper", int32(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("exactly 4 fields"),
                                  type->Deserialize(two_fields, "{}"));

  // Wrong inc field names.
  auto bad_inc_name =
      struct_({field("lower", int32(), true), field("upper", int32(), true),
               field("lo_inc", boolean(), false), field("upper_inc", boolean(), false)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("named \"lower_inc\""),
                                  type->Deserialize(bad_inc_name, "{}"));

  // Inc fields not boolean.
  auto non_bool_inc =
      struct_({field("lower", int32(), true), field("upper", int32(), true),
               field("lower_inc", int8(), false), field("upper_inc", int8(), false)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("must be boolean"),
                                  type->Deserialize(non_bool_inc, "{}"));

  // Inc fields nullable: rejected (would produce ambiguous data).
  auto nullable_inc =
      struct_({field("lower", int32(), true), field("upper", int32(), true),
               field("lower_inc", boolean(), true), field("upper_inc", boolean(), true)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("must be non-nullable"),
                                  type->Deserialize(nullable_inc, "{}"));

  // Bounds have different types.
  auto mismatched = struct_({field("lower", int32(), true), field("upper", int64(), true),
                             field("lower_inc", boolean(), false),
                             field("upper_inc", boolean(), false)});
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, testing::HasSubstr("same type"),
                                  type->Deserialize(mismatched, "{}"));
}

// ---------------------------------------------------------------------------
// Non-nullable bounds

TEST(VariableClosednessRangeType, NonNullableBounds) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));

  // Both bounds non-nullable: accepted (a finite-only range).
  ASSERT_OK_AND_ASSIGN(
      auto from_non_nullable,
      type->Deserialize(VariableClosednessStorage(int32(), /*nullable_bounds=*/false),
                        "{}"));
  ASSERT_EQ(*int32(), *checked_pointer_cast<extension::VariableClosednessRangeType>(
                           from_non_nullable)
                           ->value_type());

  // The factory can build non-nullable bounds via allow_unbounded=false.
  auto finite = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32(), /*allow_unbounded=*/false));
  const auto& finite_storage =
      internal::checked_cast<const StructType&>(*finite->storage_type());
  ASSERT_FALSE(finite_storage.field(0)->nullable());
  ASSERT_FALSE(finite_storage.field(1)->nullable());
  // The inc fields are non-nullable regardless of allow_unbounded.
  ASSERT_FALSE(finite_storage.field(2)->nullable());
  ASSERT_FALSE(finite_storage.field(3)->nullable());
}

// ---------------------------------------------------------------------------
// Metadata round-trip

TEST(VariableClosednessRangeType, MetadataRoundTrip) {
  for (const auto& type : {extension::variable_closedness_range(int32()),
                           extension::variable_closedness_range(int64()),
                           extension::variable_closedness_range(date32()),
                           extension::variable_closedness_range(int32(), false)}) {
    auto rt = checked_pointer_cast<extension::VariableClosednessRangeType>(type);
    std::string serialized = rt->Serialize();
    ASSERT_OK_AND_ASSIGN(auto deserialized,
                         rt->Deserialize(rt->storage_type(), serialized));
    ASSERT_EQ(*type, *deserialized) << "Round-trip failed for: " << type->ToString();
  }
}

// ---------------------------------------------------------------------------
// IPC (BatchRoundTrip) -- registration round-trip

TEST(VariableClosednessRangeType, BatchRoundTrip) {
  auto type = checked_pointer_cast<extension::VariableClosednessRangeType>(
      extension::variable_closedness_range(int32()));
  auto lower = ArrayFromJSON(int32(), "[1, null, 5]");
  auto upper = ArrayFromJSON(int32(), "[10, 20, null]");
  auto lower_inc = ArrayFromJSON(boolean(), "[true, false, true]");
  auto upper_inc = ArrayFromJSON(boolean(), "[false, false, true]");
  ASSERT_OK_AND_ASSIGN(auto storage,
                       StructArray::Make({lower, upper, lower_inc, upper_inc},
                                         type->storage_type()->fields()));
  auto array = ExtensionType::WrapArray(type, storage);
  auto batch = RecordBatch::Make(schema({field("rng", type)}), array->length(), {array});

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
