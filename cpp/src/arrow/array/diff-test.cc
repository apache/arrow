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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/diff.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

constexpr random::SeedType kSeed = 0xdeadbeef;
static const auto edits_type =
    struct_({field("insert", boolean()), field("run_length", int64())});

Status ValidateEditScript(const Array& edits, const Array& base, const Array& target) {
  // beginning (in base) of the run before the current hunk
  int64_t base_run_begin = 0;
  return VisitEditScript(edits, [&](int64_t delete_begin, int64_t delete_end,
                                    int64_t insert_begin, int64_t insert_end) {
    auto target_run_begin = insert_begin - (delete_begin - base_run_begin);
    if (!base.RangeEquals(base_run_begin, delete_begin, target_run_begin, target)) {
      return Status::Invalid("base and target were unequal in a run");
    }

    base_run_begin = delete_end;
    for (int64_t i = insert_begin; i < insert_end; ++i) {
      for (int64_t d = delete_begin; d < delete_end; ++d) {
        if (target.RangeEquals(i, i + 1, d, base)) {
          return Status::Invalid("a deleted element was simultaneously inserted");
        }
      }
    }

    return Status::OK();
  });
}

class DiffTest : public ::testing::Test {
 protected:
  DiffTest() : rng_(kSeed) {}

  void DoDiff() {
    auto edits = Diff(*base_, *target_, default_memory_pool());
    ASSERT_OK(edits.status());
    edits_ = edits.ValueOrDie();
    ASSERT_OK(ValidateArray(*edits_));
    ASSERT_TRUE(edits_->type()->Equals(edits_type));
    insert_ = checked_pointer_cast<BooleanArray>(edits_->field(0));
    run_lengths_ = checked_pointer_cast<Int64Array>(edits_->field(1));
  }

  void DoDiffAndFormat(std::stringstream* out) {
    DoDiff();
    auto formatter = MakeUnifiedDiffFormatter(*base_->type(), out);
    ASSERT_OK(formatter.status());
    ASSERT_OK(formatter.ValueOrDie()(*edits_, *base_, *target_));
  }

  // validate diff and assert that it formats as expected, both directly
  // and through Array::Equals
  void AssertDiffAndFormat(const std::string& formatted_expected) {
    std::stringstream formatted;

    DoDiffAndFormat(&formatted);
    ASSERT_EQ(formatted.str(), formatted_expected) << "formatted diff incorrectly";
    formatted.str("");

    ASSERT_EQ(edits_->length() == 1,
              base_->Equals(*target_, EqualOptions().diff_sink(&formatted)));
    ASSERT_EQ(formatted.str(), formatted_expected)
        << "Array::Equals formatted diff incorrectly";
  }

  void AssertInsertIs(const std::string& insert_json) {
    ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(boolean(), insert_json), *insert_);
  }

  void AssertRunLengthIs(const std::string& run_lengths_json) {
    ASSERT_ARRAYS_EQUAL(*ArrayFromJSON(int64(), run_lengths_json), *run_lengths_);
  }

  random::RandomArrayGenerator rng_;
  std::shared_ptr<StructArray> edits_;
  std::shared_ptr<Array> base_, target_;
  std::shared_ptr<BooleanArray> insert_;
  std::shared_ptr<Int64Array> run_lengths_;
};

TEST_F(DiffTest, Trivial) {
  base_ = ArrayFromJSON(int32(), "[]");
  target_ = ArrayFromJSON(int32(), "[]");
  DoDiff();
  AssertInsertIs("[false]");
  AssertRunLengthIs("[0]");

  base_ = ArrayFromJSON(null(), "[null, null]");
  target_ = ArrayFromJSON(null(), "[null, null, null, null]");
  DoDiff();
  AssertInsertIs("[false, true, true]");
  AssertRunLengthIs("[2, 0, 0]");

  base_ = ArrayFromJSON(int32(), "[1, 2, 3]");
  target_ = ArrayFromJSON(int32(), "[1, 2, 3]");
  DoDiff();
  AssertInsertIs("[false]");
  AssertRunLengthIs("[3]");
}

TEST_F(DiffTest, Errors) {
  std::stringstream formatted;

  base_ = ArrayFromJSON(int32(), "[]");
  target_ = ArrayFromJSON(utf8(), "[]");
  ASSERT_RAISES(TypeError, Diff(*base_, *target_, default_memory_pool()));

  ASSERT_FALSE(base_->Equals(*target_, EqualOptions().diff_sink(&formatted)));
  ASSERT_EQ(formatted.str(), R"(# Array types differed: int32 vs string)");
}

template <typename ArrowType>
class DiffTestWithNumeric : public DiffTest {
 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_CASE(DiffTestWithNumeric, NumericArrowTypes);

TYPED_TEST(DiffTestWithNumeric, Basics) {
  // insert one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->DoDiff();
  this->AssertInsertIs("[false, true]");
  this->AssertRunLengthIs("[2, 2]");

  // delete one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 5]");
  this->DoDiff();
  this->AssertInsertIs("[false, false]");
  this->AssertRunLengthIs("[2, 2]");

  // change one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 23, null, 5]");
  this->DoDiff();
  this->AssertInsertIs("[false, false, true]");
  this->AssertRunLengthIs("[2, 0, 2]");

  // null out one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, null, 5]");
  this->DoDiff();
  this->AssertInsertIs("[false, false, true]");
  this->AssertRunLengthIs("[2, 1, 1]");

  // append some
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5, 6, 7, 8, 9]");
  this->DoDiff();
  this->AssertInsertIs("[false, true, true, true, true]");
  this->AssertRunLengthIs("[5, 0, 0, 0, 0]");

  // prepend some
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[6, 4, 2, 0, 1, 2, 3, null, 5]");
  this->DoDiff();
  this->AssertInsertIs("[false, true, true, true, true]");
  this->AssertRunLengthIs("[0, 0, 0, 0, 5]");
}

TEST_F(DiffTest, CompareRandomInt64) {
  compute::FunctionContext ctx;
  for (auto null_probability : {0.0, 0.25}) {
    auto values = this->rng_.Int64(1 << 10, 0, 127, null_probability);
    for (const double filter_probability : {0.99, 0.75, 0.5}) {
      auto filter_1 = this->rng_.Boolean(values->length(), filter_probability, 0.0);
      auto filter_2 = this->rng_.Boolean(values->length(), filter_probability, 0.0);

      ASSERT_OK(compute::Filter(&ctx, *values, *filter_1, &this->base_));
      ASSERT_OK(compute::Filter(&ctx, *values, *filter_2, &this->target_));

      std::stringstream formatted;
      this->DoDiffAndFormat(&formatted);
      auto st = ValidateEditScript(*this->edits_, *this->base_, *this->target_);
      if (!st.ok()) {
        ASSERT_OK(Status(st.code(), st.message() + "\n" + formatted.str()));
      }
    }
  }
}

TEST_F(DiffTest, CompareRandomStrings) {
  compute::FunctionContext ctx;
  for (auto null_probability : {0.0, 0.25}) {
    auto values = this->rng_.StringWithRepeats(1 << 10, 1 << 8, 0, 32, null_probability);
    for (const double filter_probability : {0.99, 0.75, 0.5}) {
      auto filter_1 = this->rng_.Boolean(values->length(), filter_probability, 0.0);
      auto filter_2 = this->rng_.Boolean(values->length(), filter_probability, 0.0);

      ASSERT_OK(compute::Filter(&ctx, *values, *filter_1, &this->base_));
      ASSERT_OK(compute::Filter(&ctx, *values, *filter_2, &this->target_));

      std::stringstream formatted;
      this->DoDiffAndFormat(&formatted);
      auto st = ValidateEditScript(*this->edits_, *this->base_, *this->target_);
      if (!st.ok()) {
        ASSERT_OK(Status(st.code(), st.message() + "\n" + formatted.str()));
      }
    }
  }
}

TEST_F(DiffTest, BasicsWithStrings) {
  // insert one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  DoDiff();
  AssertInsertIs("[false, true]");
  AssertRunLengthIs("[1, 2]");

  // delete one
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  DoDiff();
  AssertInsertIs("[false, false]");
  AssertRunLengthIs("[1, 2]");

  // change one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["gimme", "a", "break"])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[0, 0, 2]");

  // null out one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", null])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[2, 0, 0]");
}

TEST_F(DiffTest, BasicsWithLists) {
  // insert one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [5, 9], [], [13]])");
  DoDiff();
  AssertInsertIs("[false, true]");
  AssertRunLengthIs("[1, 2]");

  // delete one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [5, 9], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  DoDiff();
  AssertInsertIs("[false, false]");
  AssertRunLengthIs("[1, 2]");

  // change one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[3, 3, 3], [], [13]])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[0, 0, 2]");

  // null out one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], null])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[2, 0, 0]");
}

TEST_F(DiffTest, BasicsWithStructs) {
  auto type = struct_({field("foo", utf8()), field("bar", int32())});

  // insert one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ =
      ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}])");
  DoDiff();
  AssertInsertIs("[false, true]");
  AssertRunLengthIs("[1, 2]");

  // delete one
  base_ =
      ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  DoDiff();
  AssertInsertIs("[false, false]");
  AssertRunLengthIs("[1, 2]");

  // change one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 2}, {}, {"bar": 13}])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[0, 0, 2]");

  // null out one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, null])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[2, 0, 0]");
}

TEST_F(DiffTest, BasicsWithUnions) {
  auto type = union_({field("foo", utf8()), field("bar", int32())}, {2, 5});

  // insert one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [2, "?"], [5, 3], [5, 13]])");
  DoDiff();
  AssertInsertIs("[false, true]");
  AssertRunLengthIs("[1, 2]");

  // delete one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [2, "?"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  DoDiff();
  AssertInsertIs("[false, false]");
  AssertRunLengthIs("[1, 2]");

  // change one
  base_ = ArrayFromJSON(type, R"([[5, 3], [2, "!"], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "3"], [2, "!"], [5, 13]])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[0, 0, 2]");

  // null out one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], null])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[2, 0, 0]");
}

TEST_F(DiffTest, UnifiedDiffFormatter) {
  // no changes
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  AssertDiffAndFormat(R"()");

  // insert one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  AssertDiffAndFormat(R"(
@@ -1, +1 @@
+"me"
)");

  // delete one
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  AssertDiffAndFormat(R"(
@@ -1, +1 @@
-"me"
)");

  // change one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["gimme", "a", "break"])");
  AssertDiffAndFormat(R"(
@@ -0, +0 @@
-"give"
+"gimme"
)");

  // null out one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", null])");
  AssertDiffAndFormat(R"(
@@ -2, +2 @@
-"break"
+null
)");

  // strings with escaped chars
  base_ = ArrayFromJSON(utf8(), R"(["newline:\n", "quote:'", "backslash:\\"])");
  target_ =
      ArrayFromJSON(utf8(), R"(["newline:\n", "tab:\t", "quote:\"", "backslash:\\"])");
  AssertDiffAndFormat(R"(
@@ -1, +1 @@
-"quote:'"
+"tab:\t"
+"quote:\""
)");

  // date32
  base_ = ArrayFromJSON(date32(), R"([0, 1, 2, 31, 4])");
  target_ = ArrayFromJSON(date32(), R"([0, 1, 31, 2, 4])");
  AssertDiffAndFormat(R"(
@@ -2, +2 @@
-1970-01-03
@@ -4, +3 @@
+1970-01-03
)");

  // date64
  constexpr int64_t ms_per_day = 24 * 60 * 60 * 1000;
  ArrayFromVector<Date64Type>(
      {0 * ms_per_day, 1 * ms_per_day, 2 * ms_per_day, 31 * ms_per_day, 4 * ms_per_day},
      &base_);
  ArrayFromVector<Date64Type>(
      {0 * ms_per_day, 1 * ms_per_day, 31 * ms_per_day, 2 * ms_per_day, 4 * ms_per_day},
      &target_);
  AssertDiffAndFormat(R"(
@@ -2, +2 @@
-1970-01-03
@@ -4, +3 @@
+1970-01-03
)");

  // timestamp
  auto x = 678 + 1000000 * (5 + 60 * (4 + 60 * (3 + 24 * int64_t(1))));
  ArrayFromVector<TimestampType>(timestamp(TimeUnit::MICRO), {0, 1, x, 2, 4}, &base_);
  ArrayFromVector<TimestampType>(timestamp(TimeUnit::MICRO), {0, 1, 2, x, 4}, &target_);
  AssertDiffAndFormat(R"(
@@ -2, +2 @@
-1970-01-02 03:04:05.000678
@@ -4, +3 @@
+1970-01-02 03:04:05.000678
)");

  // lists
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13], []])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [5, 9], [], [13]])");
  AssertDiffAndFormat(R"(
@@ -1, +1 @@
+[5, 9]
@@ -3, +4 @@
-[]
)");

  // maps
  base_ = ArrayFromJSON(map(utf8(), int32()), R"([
    [["foo", 2], ["bar", 3], ["baz", 1]],
    [],
    [["quux", 13]],
    []
  ])");
  target_ = ArrayFromJSON(map(utf8(), int32()), R"([
    [["foo", 2], ["bar", 3], ["baz", 1]],
    [["ytho", 11]],
    [],
    [["quux", 13]]
  ])");
  AssertDiffAndFormat(R"(
@@ -1, +1 @@
+[{key: "ytho", value: 11}]
@@ -3, +4 @@
-[]
)");

  // structs
  auto type = struct_({field("foo", utf8()), field("bar", int32())});
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": null, "bar": 2}, {}, {"bar": 13}])");
  AssertDiffAndFormat(R"(
@@ -0, +0 @@
-{foo: "!", bar: 3}
+{bar: 2}
)");

  // unions
  for (auto mode : {UnionMode::SPARSE, UnionMode::DENSE}) {
    type = union_({field("foo", utf8()), field("bar", int32())}, {2, 5}, mode);
    base_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
    target_ = ArrayFromJSON(type, R"([[2, "!"], [2, "3"], [5, 13]])");
    AssertDiffAndFormat(R"(
@@ -1, +1 @@
-{5: 3}
+{2: "3"}
)");
  }

  for (auto type : {int8(), uint8(),  // verify that these are printed as numbers rather
                                      // than their ascii characters
                    int16(), uint16()}) {
    // small difference
    base_ = ArrayFromJSON(type, "[0, 1, 2, 3, 5, 8, 11, 13, 17]");
    target_ = ArrayFromJSON(type, "[2, 3, 5, 7, 11, 13, 17, 19]");
    AssertDiffAndFormat(R"(
@@ -0, +0 @@
-0
-1
@@ -5, +3 @@
-8
+7
@@ -9, +7 @@
+19
)");

    // large difference
    base_ = ArrayFromJSON(type, "[57, 10, 22, 126, 42]");
    target_ = ArrayFromJSON(type, "[58, 57, 75, 93, 53, 8, 22, 42, 79, 11]");
    AssertDiffAndFormat(R"(
@@ -0, +0 @@
+58
@@ -1, +2 @@
-10
+75
+93
+53
+8
@@ -3, +7 @@
-126
@@ -5, +8 @@
+79
+11
)");
  }
}

TEST_F(DiffTest, DictionaryDiffFormatter) {
  std::stringstream formatted;

  // differing indices
  auto base_dict = ArrayFromJSON(utf8(), R"(["a", "b", "c"])");
  auto base_indices = ArrayFromJSON(int8(), "[0, 1, 2, 2, 0, 1]");
  ASSERT_OK(
      DictionaryArray::FromArrays(dictionary(base_indices->type(), base_dict->type()),
                                  base_indices, base_dict, &base_));

  auto target_dict = base_dict;
  auto target_indices = ArrayFromJSON(int8(), "[0, 1, 2, 2, 1, 1]");
  ASSERT_OK(
      DictionaryArray::FromArrays(dictionary(target_indices->type(), target_dict->type()),
                                  target_indices, target_dict, &target_));

  base_->Equals(*target_, EqualOptions().diff_sink(&formatted));
  ASSERT_EQ(formatted.str(), R"(# Dictionary arrays differed
## dictionary diff
## indices diff
@@ -4, +4 @@
-0
@@ -6, +5 @@
+1
)");

  // differing dictionaries
  target_dict = ArrayFromJSON(utf8(), R"(["b", "c", "a"])");
  target_indices = base_indices;
  ASSERT_OK(
      DictionaryArray::FromArrays(dictionary(target_indices->type(), target_dict->type()),
                                  target_indices, target_dict, &target_));

  formatted.str("");
  base_->Equals(*target_, EqualOptions().diff_sink(&formatted));
  ASSERT_EQ(formatted.str(), R"(# Dictionary arrays differed
## dictionary diff
@@ -0, +0 @@
-"a"
@@ -3, +2 @@
+"a"
## indices diff
)");
}

void MakeSameLength(std::shared_ptr<Array>* a, std::shared_ptr<Array>* b) {
  auto length = std::min((*a)->length(), (*b)->length());
  *a = (*a)->Slice(0, length);
  *b = (*b)->Slice(0, length);
}

TEST_F(DiffTest, CompareRandomStruct) {
  compute::FunctionContext ctx;
  for (auto null_probability : {0.0, 0.25}) {
    constexpr auto length = 1 << 10;
    auto int32_values = this->rng_.Int32(length, 0, 127, null_probability);
    auto utf8_values = this->rng_.String(length, 0, 16, null_probability);
    for (const double filter_probability : {0.9999, 0.75}) {
      std::shared_ptr<Array> int32_base, int32_target, utf8_base, utf8_target;
      ASSERT_OK(compute::Filter(&ctx, *int32_values,
                                *this->rng_.Boolean(length, filter_probability, 0.0),
                                &int32_base));
      ASSERT_OK(compute::Filter(&ctx, *utf8_values,
                                *this->rng_.Boolean(length, filter_probability, 0.0),
                                &utf8_base));
      MakeSameLength(&int32_base, &utf8_base);

      ASSERT_OK(compute::Filter(&ctx, *int32_values,
                                *this->rng_.Boolean(length, filter_probability, 0.0),
                                &int32_target));
      ASSERT_OK(compute::Filter(&ctx, *utf8_values,
                                *this->rng_.Boolean(length, filter_probability, 0.0),
                                &utf8_target));
      MakeSameLength(&int32_target, &utf8_target);

      auto type = struct_({field("i", int32()), field("s", utf8())});
      auto base_res = StructArray::Make({int32_base, utf8_base}, type->children());
      ASSERT_OK(base_res.status());
      base_ = base_res.ValueOrDie();
      auto target_res = StructArray::Make({int32_target, utf8_target}, type->children());
      ASSERT_OK(target_res.status());
      target_ = target_res.ValueOrDie();

      std::stringstream formatted;
      this->DoDiffAndFormat(&formatted);
      auto st = ValidateEditScript(*this->edits_, *this->base_, *this->target_);
      if (!st.ok()) {
        ASSERT_OK(Status(st.code(), st.message() + "\n" + formatted.str()));
      }
    }
  }
}

}  // namespace arrow
