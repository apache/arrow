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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_run_end.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/pretty_print.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Run-end encoded array tests

namespace {

class TestRunEndEncodedArray
    : public ::testing::TestWithParam<std::shared_ptr<DataType>> {
 protected:
  std::shared_ptr<DataType> run_end_type;
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;
  std::shared_ptr<Array> run_end_values;
  std::shared_ptr<Array> run_end_only_null;

  void SetUp() override {
    run_end_type = GetParam();

    string_values = ArrayFromJSON(utf8(), R"(["Hello", "World", null])");
    int32_values = ArrayFromJSON(int32(), "[10, 20, 30]");
    int16_values = ArrayFromJSON(int16(), "[10, 20, 30]");
    run_end_values = ArrayFromJSON(run_end_type, "[10, 20, 30]");
    run_end_only_null = ArrayFromJSON(run_end_type, "[null, null, null]");
  }

  std::shared_ptr<RunEndEncodedArray> RunEndEncodedArrayFromJSON(
      int64_t logical_length, std::shared_ptr<DataType> value_type,
      std::string_view run_ends_json, std::string_view values_json,
      int64_t logical_offset = 0) {
    auto run_ends = ArrayFromJSON(run_end_type, run_ends_json);
    auto values = ArrayFromJSON(value_type, values_json);
    return RunEndEncodedArray::Make(logical_length, std::move(run_ends),
                                    std::move(values), logical_offset)
        .ValueOrDie();
  }
};

TEST_P(TestRunEndEncodedArray, MakeArray) {
  ASSERT_OK_AND_ASSIGN(auto ree_array,
                       RunEndEncodedArray::Make(30, int32_values, string_values));
  auto array_data = ree_array->data();
  auto new_array = MakeArray(array_data);
  ASSERT_ARRAYS_EQUAL(*new_array, *ree_array);
  // Should be the exact same ArrayData object
  ASSERT_EQ(new_array->data(), array_data);
  ASSERT_NE(std::dynamic_pointer_cast<RunEndEncodedArray>(new_array), NULLPTR);
}

TEST_P(TestRunEndEncodedArray, FromRunEndsAndValues) {
  std::shared_ptr<RunEndEncodedArray> ree_array;

  ASSERT_OK_AND_ASSIGN(ree_array,
                       RunEndEncodedArray::Make(30, run_end_values, int32_values));
  ASSERT_EQ(ree_array->length(), 30);
  ASSERT_ARRAYS_EQUAL(*ree_array->values(), *int32_values);
  ASSERT_ARRAYS_EQUAL(*ree_array->run_ends(), *run_end_values);
  ASSERT_EQ(ree_array->offset(), 0);
  ASSERT_EQ(ree_array->data()->null_count, 0);
  // Existing code might assume at least one buffer,
  // so RunEndEncodedArray should be built with one
  ASSERT_EQ(ree_array->data()->buffers.size(), 1);

  // Passing a non-zero logical offset
  ASSERT_OK_AND_ASSIGN(ree_array,
                       RunEndEncodedArray::Make(29, run_end_values, string_values, 1));
  ASSERT_EQ(ree_array->length(), 29);
  ASSERT_ARRAYS_EQUAL(*ree_array->values(), *string_values);
  ASSERT_ARRAYS_EQUAL(*ree_array->run_ends(), *run_end_values);
  ASSERT_EQ(ree_array->data()->null_count, 0);
  ASSERT_EQ(ree_array->offset(), 1);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Run end type must be int16, int32 or int64"),
      RunEndEncodedArray::Make(30, string_values, int32_values));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Null count must be 0 for run ends array, but is 3"),
      RunEndEncodedArray::Make(30, run_end_only_null, int32_values));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Invalid: Length of run_ends is greater than the length of values: 3 > 2"),
      RunEndEncodedArray::Make(30, run_end_values, ArrayFromJSON(int32(), "[2, 0]")));
}

TEST_P(TestRunEndEncodedArray, FindOffsetAndLength) {
  auto run_ends = ArrayFromJSON(run_end_type, "[100, 200, 300, 400, 500]");
  auto values = ArrayFromJSON(utf8(), R"(["Hello", "beautiful", "world", "of", "REE"])");
  ASSERT_OK_AND_ASSIGN(auto ree_array, RunEndEncodedArray::Make(500, run_ends, values));

  ASSERT_EQ(ree_array->FindPhysicalOffset(), 0);
  ASSERT_EQ(ree_array->FindPhysicalLength(), 5);

  auto slice = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(199, 5));
  ASSERT_EQ(slice->FindPhysicalOffset(), 1);
  ASSERT_EQ(slice->FindPhysicalLength(), 2);

  auto slice2 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(199, 101));
  ASSERT_EQ(slice2->FindPhysicalOffset(), 1);
  ASSERT_EQ(slice2->FindPhysicalLength(), 2);

  auto slice3 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(400, 100));
  ASSERT_EQ(slice3->FindPhysicalOffset(), 4);
  ASSERT_EQ(slice3->FindPhysicalLength(), 1);

  auto slice4 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(0, 150));
  ASSERT_EQ(slice4->FindPhysicalOffset(), 0);
  ASSERT_EQ(slice4->FindPhysicalLength(), 2);

  auto zero_length_at_end =
      std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(500, 0));
  ASSERT_EQ(zero_length_at_end->FindPhysicalOffset(), 5);
  ASSERT_EQ(zero_length_at_end->FindPhysicalLength(), 0);
}

TEST_P(TestRunEndEncodedArray, LogicalRunEnds) {
  auto run_ends = ArrayFromJSON(run_end_type, "[100, 200, 300, 400, 500]");
  auto values = ArrayFromJSON(utf8(), R"(["Hello", "beautiful", "world", "of", "REE"])");
  ASSERT_OK_AND_ASSIGN(auto ree_array, RunEndEncodedArray::Make(500, run_ends, values));

  auto* pool = default_memory_pool();
  ASSERT_OK_AND_ASSIGN(auto logical_run_ends, ree_array->LogicalRunEnds(pool));
  ASSERT_ARRAYS_EQUAL(*logical_run_ends, *run_ends);

  // offset=0, length=0
  auto slice = ree_array->Slice(0, 0);
  auto ree_slice = checked_cast<const RunEndEncodedArray*>(slice.get());
  ASSERT_OK_AND_ASSIGN(logical_run_ends, ree_slice->LogicalRunEnds(pool));
  ASSERT_ARRAYS_EQUAL(*logical_run_ends, *ArrayFromJSON(run_end_type, "[]"));

  // offset=0, length=<a run-end>
  for (int i = 1; i < 5; i++) {
    auto expected_run_ends = run_ends->Slice(0, i);
    slice = ree_array->Slice(0, i * 100);
    ree_slice = checked_cast<const RunEndEncodedArray*>(slice.get());
    ASSERT_OK_AND_ASSIGN(logical_run_ends, ree_slice->LogicalRunEnds(pool));
    ASSERT_ARRAYS_EQUAL(*logical_run_ends, *expected_run_ends);
  }

  // offset=0, length=<length in the middle of a run>
  for (int i = 2; i < 5; i++) {
    std::shared_ptr<Array> expected_run_ends;
    {
      std::string expected_run_ends_json = "[100";
      for (int j = 2; j < i; j++) {
        expected_run_ends_json += ", " + std::to_string(j * 100);
      }
      expected_run_ends_json += ", " + std::to_string(i * 100 - 50) + "]";
      expected_run_ends = ArrayFromJSON(run_end_type, expected_run_ends_json);
    }
    slice = ree_array->Slice(0, i * 100 - 50);
    ree_slice = checked_cast<const RunEndEncodedArray*>(slice.get());
    ASSERT_OK_AND_ASSIGN(logical_run_ends, ree_slice->LogicalRunEnds(pool));
    ASSERT_ARRAYS_EQUAL(*logical_run_ends, *expected_run_ends);
  }

  // offset != 0
  slice = ree_array->Slice(50, 400);
  ree_slice = checked_cast<const RunEndEncodedArray*>(slice.get());
  const auto expected_run_ends = ArrayFromJSON(run_end_type, "[50, 150, 250, 350, 400]");
  ASSERT_OK_AND_ASSIGN(logical_run_ends, ree_slice->LogicalRunEnds(pool));
  ASSERT_ARRAYS_EQUAL(*logical_run_ends, *expected_run_ends);
}

TEST_P(TestRunEndEncodedArray, Builder) {
  auto value_type = utf8();
  auto ree_type = run_end_encoded(run_end_type, value_type);

  auto BuilderEquals = [this, value_type, ree_type](
                           ArrayBuilder& builder, int64_t expected_length,
                           std::string_view run_ends_json,
                           std::string_view values_json) -> Status {
    const int64_t length = builder.length();
    if (length != expected_length) {
      return Status::Invalid("Length from builder differs from expected length: ", length,
                             " != ", expected_length);
    }
    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    auto ree_array = std::dynamic_pointer_cast<RunEndEncodedArray>(array);
    if (length != ree_array->length()) {
      return Status::Invalid("Length from builder differs from length of built array: ",
                             length, " != ", ree_array->length());
    }
    auto expected_ree_array =
        RunEndEncodedArrayFromJSON(length, value_type, run_ends_json, values_json, 0);
    ASSERT_ARRAYS_EQUAL(*expected_ree_array->run_ends(), *ree_array->run_ends());
    ASSERT_ARRAYS_EQUAL(*expected_ree_array->values(), *ree_array->values());
    ASSERT_ARRAYS_EQUAL(*expected_ree_array, *ree_array);
    return Status::OK();
  };

  auto appended_array = RunEndEncodedArrayFromJSON(200, value_type, R"([110, 210])",
                                                   R"(["common", "appended"])", 10);
  auto appended_span = ArraySpan(*appended_array->data());

  for (int step = 0;; step++) {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ArrayBuilder> builder, MakeBuilder(ree_type));
    if (step == 0) {
      auto ree_builder = std::dynamic_pointer_cast<RunEndEncodedBuilder>(builder);
      ASSERT_NE(ree_builder, NULLPTR);
      ASSERT_OK(BuilderEquals(*builder, 0, "[]", "[]"));
      continue;
    }
    ASSERT_OK(builder->AppendScalar(*MakeScalar("unique")));
    if (step == 1) {
      ASSERT_OK(BuilderEquals(*builder, 1, "[1]", R"(["unique"])"));
      continue;
    }
    ASSERT_OK(builder->AppendNull());
    if (step == 2) {
      ASSERT_OK(BuilderEquals(*builder, 2, "[1, 2]", R"(["unique", null])"));
      continue;
    }
    ASSERT_OK(builder->AppendScalar(*MakeNullScalar(utf8())));
    if (step == 3) {
      ASSERT_OK(BuilderEquals(*builder, 3, "[1, 3]", R"(["unique", null])"));
      continue;
    }
    ASSERT_OK(builder->AppendScalar(*MakeScalar("common"), 100));
    if (step == 4) {
      ASSERT_OK(
          BuilderEquals(*builder, 103, "[1, 3, 103]", R"(["unique", null, "common"])"));
      continue;
    }
    ASSERT_OK(builder->AppendScalar(*MakeScalar("common")));
    if (step == 5) {
      ASSERT_OK(
          BuilderEquals(*builder, 104, "[1, 3, 104]", R"(["unique", null, "common"])"));
      continue;
    }
    ASSERT_OK(builder->AppendScalar(*MakeScalar("common")));
    if (step == 6) {
      ASSERT_OK(
          BuilderEquals(*builder, 105, "[1, 3, 105]", R"(["unique", null, "common"])"));
      continue;
    }
    // Append span that starts with the same value as the previous run ends. They
    // are currently not merged for simplicity and performance. This is still a
    // valid REE array.
    //
    // builder->Append([(60 * "common")..., (40 * "appended")...])
    ASSERT_OK(builder->AppendArraySlice(appended_span, 40, 100));
    if (step == 7) {
      ASSERT_OK(BuilderEquals(*builder, 205, "[1, 3, 105, 165, 205]",
                              R"(["unique", null, "common", "common", "appended"])"));
      continue;
    }
    // builder->Append([(100 * "common")...])
    ASSERT_OK(builder->AppendArraySlice(appended_span, 0, 100));
    if (step == 8) {
      ASSERT_OK(
          BuilderEquals(*builder, 305, "[1, 3, 105, 165, 205, 305]",
                        R"(["unique", null, "common", "common", "appended", "common"])"));
      continue;
    }
    // Append an entire array
    ASSERT_OK(builder->AppendArraySlice(appended_span, 0, appended_span.length));
    if (step == 9) {
      ASSERT_OK(BuilderEquals(
          *builder, 505, "[1, 3, 105, 165, 205, 305, 405, 505]",
          R"(["unique", null, "common", "common", "appended", "common", "common", "appended"])"));
      continue;
    }
    // Append empty values
    ASSERT_OK(builder->AppendEmptyValues(10));
    if (step == 11) {
      ASSERT_EQ(builder->length(), 515);
      ASSERT_OK(BuilderEquals(
          *builder, 515, "[1, 3, 105, 165, 205, 305, 405, 505, 515]",
          R"(["unique", null, "common", "common", "appended", "common", "common", "appended", ""])"));
      continue;
    }
    // Append NULL after empty
    ASSERT_OK(builder->AppendNull());
    if (step == 12) {
      ASSERT_EQ(builder->length(), 516);
      ASSERT_OK(BuilderEquals(
          *builder, 516, "[1, 3, 105, 165, 205, 305, 405, 505, 515, 516]",
          R"(["unique", null, "common", "common", "appended", "common", "common", "appended", "", null])"));
      continue;
    }
    if (step == 13) {
      ASSERT_EQ(builder->length(), 516);
      ASSERT_EQ(*builder->type(), *run_end_encoded(run_end_type, utf8()));

      auto expected_run_ends =
          ArrayFromJSON(run_end_type, "[1, 3, 105, 165, 205, 305, 405, 505, 515, 516]");
      auto expected_values = ArrayFromJSON(
          value_type,
          R"(["unique", null, "common", "common", "appended", "common", "common", "appended", "", null])");

      ASSERT_OK_AND_ASSIGN(auto array, builder->Finish());
      auto ree_array = std::dynamic_pointer_cast<RunEndEncodedArray>(array);
      ASSERT_NE(ree_array, NULLPTR);
      ASSERT_ARRAYS_EQUAL(*expected_run_ends, *ree_array->run_ends());
      ASSERT_ARRAYS_EQUAL(*expected_values, *ree_array->values());
      ASSERT_EQ(array->length(), 516);
      ASSERT_EQ(array->offset(), 0);
      break;
    }
  }
}

TEST_P(TestRunEndEncodedArray, Validate) {
  auto run_ends_good = ArrayFromJSON(run_end_type, "[10, 20, 30, 40]");
  auto values = ArrayFromJSON(utf8(), R"(["A", "B", "C", null])");
  auto run_ends_with_zero = ArrayFromJSON(run_end_type, "[0, 20, 30, 40]");
  auto run_ends_with_null = ArrayFromJSON(run_end_type, "[0, 20, 30, null]");
  auto run_ends_not_ordered = ArrayFromJSON(run_end_type, "[10, 20, 40, 40]");
  auto run_ends_too_low = ArrayFromJSON(run_end_type, "[10, 20, 40, 39]");
  auto empty_ints = ArrayFromJSON(run_end_type, "[]");
  auto run_ends_require64 = ArrayFromJSON(int64(), "[10, 9223372036854775807]");
  int64_t long_length = 0;
  if (run_end_type->id() == Type::INT16) {
    long_length = std::numeric_limits<int16_t>::max();
  } else if (run_end_type->id() == Type::INT32) {
    long_length = std::numeric_limits<int32_t>::max();
  } else {
    long_length = std::numeric_limits<int64_t>::max();
  }
  auto run_ends_long = ArrayFromJSON(
      run_end_type, std::string("[10, ") + std::to_string(long_length) + "]");

  ASSERT_OK_AND_ASSIGN(auto good_array,
                       RunEndEncodedArray::Make(40, run_ends_good, values));
  ASSERT_OK(good_array->ValidateFull());

  ASSERT_OK_AND_ASSIGN(
      auto require64_array,
      RunEndEncodedArray::Make(9223372036854775806, run_ends_require64, values));
  ASSERT_OK(require64_array->ValidateFull());

  auto sliced = good_array->Slice(5, 20);
  ASSERT_OK(sliced->ValidateFull());

  auto sliced_at_run_end = good_array->Slice(10, 20);
  ASSERT_OK(sliced_at_run_end->ValidateFull());

  ASSERT_OK_AND_ASSIGN(
      auto sliced_children,
      RunEndEncodedArray::Make(15, run_ends_good->Slice(1, 2), values->Slice(1, 3)));
  ASSERT_OK(sliced_children->ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunEndEncodedArray::Make(0, empty_ints, empty_ints));
  ASSERT_OK(empty_array->ValidateFull());

  auto empty_run_ends = MakeArray(empty_array->data()->Copy());
  empty_run_ends->data()->length = 1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Run-end encoded array has non-zero length 1, "
                           "but run ends array has zero length"),
      empty_run_ends->Validate());

  auto offset_length_overflow = MakeArray(good_array->data()->Copy());
  offset_length_overflow->data()->offset = std::numeric_limits<int64_t>::max();
  offset_length_overflow->data()->length = 1;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          std::string("Invalid: Array of type run_end_encoded<run_ends: ") +
          run_end_type->ToString() +
          ", values: string> has impossibly large length and offset"),
      offset_length_overflow->Validate());

  ASSERT_OK_AND_ASSIGN(auto too_large_for_ree16,
                       RunEndEncodedArray::Make(40, run_ends_long, values));
  too_large_for_ree16->data()->offset = std::numeric_limits<int16_t>::max();
  too_large_for_ree16->data()->length = 1;
  if (run_end_type->id() == Type::INT16) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "Invalid: Offset + length of a run-end encoded array must fit in a value"
            " of the run end type int16, but offset + length is 32768 while"
            " the allowed maximum is 32767"),
        too_large_for_ree16->Validate());
  } else {
    ASSERT_OK(too_large_for_ree16->ValidateFull());
  }

  ASSERT_OK_AND_ASSIGN(auto too_large_for_ree32,
                       RunEndEncodedArray::Make(40, run_ends_long, values));
  too_large_for_ree32->data()->offset = std::numeric_limits<int32_t>::max();
  too_large_for_ree32->data()->length = 1;
  if (run_end_type->id() == Type::INT16) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "Invalid: Offset + length of a run-end encoded array must fit in a "
            "value of the run end type int16, but offset + length "
            "is 2147483648 while the allowed maximum is 32767"),
        too_large_for_ree32->Validate());
  } else if (run_end_type->id() == Type::INT32) {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "Invalid: Offset + length of a run-end encoded array must fit in a "
            "value of the run end type int32, but offset + length "
            "is 2147483648 while the allowed maximum is 2147483647"),
        too_large_for_ree32->Validate());
  } else {
    ASSERT_OK(too_large_for_ree32->ValidateFull());
  }

  std::shared_ptr<Array> has_null_buffer = MakeArray(good_array->data()->Copy());
  std::shared_ptr<Buffer> null_bitmap;
  BitmapFromVector<bool>({true, false}, &null_bitmap);
  has_null_buffer->data()->buffers[0] = null_bitmap;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          std::string("Invalid: Run end encoded array should not have a null bitmap.")),
      has_null_buffer->Validate());

  auto too_many_children = MakeArray(good_array->data()->Copy());
  too_many_children->data()->child_data.push_back(NULLPTR);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          std::string("Invalid: Expected 2 child arrays in array of type "
                      "run_end_encoded<run_ends: ") +
          run_end_type->ToString() + ", values: string>, got 3"),
      too_many_children->Validate());

  auto run_ends_nullptr = MakeArray(good_array->data()->Copy());
  run_ends_nullptr->data()->child_data[0] = NULLPTR;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Invalid: Run ends array is null pointer"),
      run_ends_nullptr->Validate());

  auto values_nullptr = MakeArray(good_array->data()->Copy());
  values_nullptr->data()->child_data[1] = NULLPTR;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Invalid: Values array is null pointer"),
      values_nullptr->Validate());

  auto run_ends_string = MakeArray(good_array->data()->Copy());
  run_ends_string->data()->child_data[0] = values->data();
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          std::string("Invalid: Run ends array of run_end_encoded<run_ends: ") +
          run_end_type->ToString() + ", values: string> must be " +
          run_end_type->ToString() + ", but run end type is string"),
      run_ends_string->Validate());

  auto wrong_type = MakeArray(good_array->data()->Copy());
  wrong_type->data()->type = run_end_encoded(run_end_type, uint16());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Parent type says this array encodes uint16 "
                           "values, but value type is string"),
      wrong_type->Validate());

  {
    // malformed_array has its buffers deallocated after the RunEndEncodedArray is
    // constructed because it is UB to create an REE array with invalid run ends
    auto malformed_array = ArrayFromJSON(run_end_type, "[10, 20, 30, 40]");
    ASSERT_OK_AND_ASSIGN(auto run_ends_malformed,
                         RunEndEncodedArray::Make(40, malformed_array, values));
    malformed_array->data()->buffers.emplace_back(NULLPTR);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            std::string(
                "Invalid: Run ends array invalid: Expected 2 buffers in array of type ") +
            run_end_type->ToString() + ", got 3"),
        run_ends_malformed->Validate());
  }

  {
    auto malformed_array = ArrayFromJSON(int32(), "[1, 2, 3, 4]");
    ASSERT_OK_AND_ASSIGN(auto values_malformed,
                         RunEndEncodedArray::Make(40, run_ends_good, malformed_array));
    malformed_array->data()->buffers.emplace_back(NULLPTR);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("Invalid: Values array invalid: Expected 2 buffers in array "
                             "of type int32, got 3"),
        values_malformed->Validate());
  }

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> run_end_zero_array,
                       RunEndEncodedArray::Make(40, run_ends_with_zero, values));
  ASSERT_OK(run_end_zero_array->Validate());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Invalid: All run ends must be greater than 0 but the first run end is 0"),
      run_end_zero_array->ValidateFull());
  // The whole run ends array has to be valid even if the parent is sliced
  run_end_zero_array = run_end_zero_array->Slice(30, 0);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          "Invalid: All run ends must be greater than 0 but the first run end is 0"),
      run_end_zero_array->ValidateFull());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> run_ends_not_ordered_array,
                       RunEndEncodedArray::Make(40, run_ends_not_ordered, values));
  ASSERT_OK(run_ends_not_ordered_array->Validate());
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Every run end must be strictly greater than the "
                           "previous run end, but "
                           "run_ends[3] is 40 and run_ends[2] is 40"),
      run_ends_not_ordered_array->ValidateFull());
  // The whole run ends array has to be valid even if the parent is sliced
  run_ends_not_ordered_array = run_ends_not_ordered_array->Slice(30, 0);
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Every run end must be strictly greater than the "
                           "previous run end, but "
                           "run_ends[3] is 40 and run_ends[2] is 40"),
      run_ends_not_ordered_array->ValidateFull());

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr("Invalid: Last run end is 39 but it should match 40"
                           " (offset: 0, length: 40)"),
      RunEndEncodedArray::Make(40, run_ends_too_low, values));
}

TEST_P(TestRunEndEncodedArray, Compare) {
  ASSERT_OK_AND_ASSIGN(auto ree_array,
                       RunEndEncodedArray::Make(30, run_end_values, string_values));

  auto copy = MakeArray(ree_array->data()->Copy());
  ASSERT_ARRAYS_EQUAL(*ree_array, *copy);

  ASSERT_FALSE(ree_array->Slice(0, 29)->Equals(*ree_array->Slice(1, 29)));

  // Two same-length slice pairs
  ASSERT_ARRAYS_EQUAL(*ree_array->Slice(0, 9), *ree_array->Slice(1, 9));
  ASSERT_FALSE(ree_array->Slice(5, 9)->Equals(*ree_array->Slice(6, 9)));

  // Array that is logically the same as our ree_array, but has 2
  // small runs for the first value instead of a single larger run
  auto equivalent_run_ends = ArrayFromJSON(run_end_type, "[5, 10, 20, 30]");
  auto string_values = ArrayFromJSON(utf8(), R"(["Hello", "Hello", "World", null])");
  ASSERT_OK_AND_ASSIGN(auto equivalent_array,
                       RunEndEncodedArray::Make(30, equivalent_run_ends, string_values));
  ASSERT_ARRAYS_EQUAL(*ree_array, *equivalent_array);

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunEndEncodedArray::Make(0, ArrayFromJSON(run_end_type, "[]"),
                                                ArrayFromJSON(binary(), "[]")));
  ASSERT_ARRAYS_EQUAL(*empty_array, *MakeArray(empty_array->data()->Copy()));

  // Three different slices that have the value [3, 3, 3, 4, 4, 4, 4]
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_a,
      RunEndEncodedArray::Make(60, ArrayFromJSON(run_end_type, "[2, 5, 12, 58, 60]"),
                               ArrayFromJSON(int64(), "[1, 2, 3, 4, 5]")));
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_b,
      RunEndEncodedArray::Make(100, ArrayFromJSON(run_end_type, "[81, 86, 99, 100]"),
                               ArrayFromJSON(int64(), "[2, 3, 4, 5]")));
  ASSERT_OK_AND_ASSIGN(auto different_offsets_c,
                       RunEndEncodedArray::Make(7, ArrayFromJSON(run_end_type, "[3, 7]"),
                                                ArrayFromJSON(int64(), "[3, 4]")));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7),
                      *different_offsets_b->Slice(83, 7));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7), *different_offsets_c);
  ASSERT_ARRAYS_EQUAL(*different_offsets_b->Slice(83, 7), *different_offsets_c);
}

TEST_P(TestRunEndEncodedArray, Concatenate) {
  ASSERT_OK_AND_ASSIGN(auto int32_array,
                       RunEndEncodedArray::Make(30, run_end_values, int32_values));
  ASSERT_OK_AND_ASSIGN(auto string_array,
                       RunEndEncodedArray::Make(30, run_end_values, string_values));
  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunEndEncodedArray::Make(0, ArrayFromJSON(run_end_type, "[]"),
                                                ArrayFromJSON(int32(), "[]")));

  ASSERT_OK_AND_ASSIGN(auto expected_102030_twice,
                       RunEndEncodedArray::Make(
                           60, ArrayFromJSON(run_end_type, "[10, 20, 30, 40, 50, 60]"),
                           ArrayFromJSON(int32(), "[10, 20, 30, 10, 20, 30]")));
  ASSERT_OK_AND_ASSIGN(auto result,
                       Concatenate({int32_array, int32_array}, default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*expected_102030_twice, *result);

  ArrayVector sliced_back_together = {
      int32_array->Slice(0, 1),  int32_array->Slice(5, 5),
      int32_array->Slice(0, 4),  int32_array->Slice(10, 7),
      int32_array->Slice(3, 0),  int32_array->Slice(10, 1),
      int32_array->Slice(18, 7), empty_array,
      int32_array->Slice(25, 5)};
  ASSERT_OK_AND_ASSIGN(result, Concatenate(sliced_back_together, default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*int32_array, *result);

  ASSERT_OK_AND_ASSIGN(result, Concatenate({empty_array, empty_array, empty_array},
                                           default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*empty_array, *result);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      ::testing::HasSubstr(
          std::string("Invalid: arrays to be concatenated must be identically typed, but "
                      "run_end_encoded<run_ends: ") +
          run_end_type->ToString() + ", values: int32> and run_end_encoded<run_ends: " +
          run_end_type->ToString() + ", values: string> were encountered."),
      Concatenate({int32_array, string_array}, default_memory_pool()));
}

TEST_P(TestRunEndEncodedArray, Printing) {
  ASSERT_OK_AND_ASSIGN(auto int_array,
                       RunEndEncodedArray::Make(30, run_end_values, int32_values));
  std::stringstream ss;
  ASSERT_OK(PrettyPrint(*int_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run_ends:\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]");

  ASSERT_OK_AND_ASSIGN(auto string_array,
                       RunEndEncodedArray::Make(30, run_end_values, string_values));
  ss.str("");
  ASSERT_OK(PrettyPrint(*string_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run_ends:\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    \"Hello\",\n"
            "    \"World\",\n"
            "    null\n"
            "  ]");

  auto sliced_array = string_array->Slice(15, 6);
  ss.str("");
  ASSERT_OK(PrettyPrint(*sliced_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run_ends:\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    \"Hello\",\n"
            "    \"World\",\n"
            "    null\n"
            "  ]");

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunEndEncodedArray::Make(0, ArrayFromJSON(run_end_type, "[]"),
                                                ArrayFromJSON(binary(), "[]")));

  ss.str("");
  ASSERT_OK(PrettyPrint(*empty_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run_ends:\n"
            "  []\n"
            "-- values:\n"
            "  []");
}

}  // anonymous namespace

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunEndEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));

}  // namespace arrow
