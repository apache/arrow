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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/diff.h"
#include "arrow/compute/api.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

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
    ASSERT_OK(edits_->ValidateFull());
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
    AssertArraysEqual(*ArrayFromJSON(boolean(), insert_json), *insert_, /*verbose=*/true);
  }

  void AssertRunLengthIs(const std::string& run_lengths_json) {
    AssertArraysEqual(*ArrayFromJSON(int64(), run_lengths_json), *run_lengths_,
                      /*verbose=*/true);
  }

  void BaseAndTargetFromRandomFilter(std::shared_ptr<Array> values,
                                     double filter_probability) {
    std::shared_ptr<Array> base_filter, target_filter;
    do {
      base_filter = this->rng_.Boolean(values->length(), filter_probability, 0.0);
      target_filter = this->rng_.Boolean(values->length(), filter_probability, 0.0);
    } while (base_filter->Equals(target_filter));

    ASSERT_OK_AND_ASSIGN(Datum out_datum, compute::Filter(values, base_filter));
    base_ = out_datum.make_array();

    ASSERT_OK_AND_ASSIGN(out_datum, compute::Filter(values, target_filter));
    target_ = out_datum.make_array();
  }

  void TestBasicsWithUnions(UnionMode::type mode) {
    ASSERT_OK_AND_ASSIGN(
        auto type,
        UnionType::Make({field("foo", utf8()), field("bar", int32())}, {2, 5}, mode));

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

  std::shared_ptr<RunEndEncodedArray> RunEndEncodedArrayFromJSON(
      int64_t logical_length, const std::shared_ptr<DataType>& ree_type,
      std::string_view run_ends_json, std::string_view values_json,
      int64_t logical_offset = 0) {
    auto& ree_type_ref = checked_cast<const RunEndEncodedType&>(*ree_type);
    auto run_ends = ArrayFromJSON(ree_type_ref.run_end_type(), run_ends_json);
    auto values = ArrayFromJSON(ree_type_ref.value_type(), values_json);
    return RunEndEncodedArray::Make(logical_length, std::move(run_ends),
                                    std::move(values), logical_offset)
        .ValueOrDie();
  }

  template <typename RunEndType>
  void TestBasicsWithREEs() {
    auto run_end_type = std::make_shared<RunEndType>();
    auto value_type = utf8();
    auto ree_type = run_end_encoded(run_end_type, value_type);

    // empty REEs
    base_ = RunEndEncodedArrayFromJSON(0, ree_type, "[]", "[]");
    target_ = RunEndEncodedArrayFromJSON(0, ree_type, "[]", "[]");
    DoDiff();
    AssertInsertIs("[false]");
    AssertRunLengthIs("[0]");

    // null REE arrays of different lengths
    base_ = RunEndEncodedArrayFromJSON(2, ree_type, "[2]", "[null]");
    target_ = RunEndEncodedArrayFromJSON(4, ree_type, "[4]", "[null]");
    DoDiff();
    AssertInsertIs("[false, true, true]");
    AssertRunLengthIs("[2, 0, 0]");

    // identical REE arrays w/ offsets
    base_ =
        RunEndEncodedArrayFromJSON(110, ree_type, R"([20, 120])", R"(["a", "b"])", 10);
    target_ =
        RunEndEncodedArrayFromJSON(110, ree_type, R"([20, 120])", R"(["a", "b"])", 10);
    DoDiff();
    AssertInsertIs("[false]");
    AssertRunLengthIs("[110]");

    // equivalent REE arrays
    base_ = RunEndEncodedArrayFromJSON(120, ree_type, R"([10, 20, 120])",
                                       R"(["a", "a", "b"])");
    target_ = RunEndEncodedArrayFromJSON(120, ree_type, R"([20, 30, 120])",
                                         R"(["a", "b", "b"])");
    DoDiff();
    AssertInsertIs("[false]");
    AssertRunLengthIs("[120]");

    // slice so last run-end goes beyond length
    base_ = base_->Slice(5, 105);
    target_ = target_->Slice(5, 105);
    DoDiff();
    AssertInsertIs("[false]");
    AssertRunLengthIs("[105]");

    // insert one
    base_ = RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 12])", R"(["a", "b"])");
    target_ = RunEndEncodedArrayFromJSON(13, ree_type, R"([3, 13])", R"(["a", "b"])");
    DoDiff();
    AssertInsertIs("[false, true]");
    AssertRunLengthIs("[2, 10]");

    // delete one
    base_ =
        RunEndEncodedArrayFromJSON(13, ree_type, R"([2, 5, 13])", R"(["a", "b", "c"])");
    target_ =
        RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 4, 12])", R"(["a", "b", "c"])");
    DoDiff();
    AssertInsertIs("[false, false]");
    AssertRunLengthIs("[4, 8]");

    // null out one
    base_ =
        RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 5, 12])", R"(["a", "b", "c"])");
    target_ = RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 4, 5, 12])",
                                         R"(["a", "b", null, "c"])");
    DoDiff();
    AssertInsertIs("[false, false, true]");
    AssertRunLengthIs("[4, 0, 7]");

    // append some
    base_ = RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 4, 8, 12])",
                                       R"(["a", "b", "c", "d"])");
    target_ = RunEndEncodedArrayFromJSON(15, ree_type, R"([2, 4, 8, 13, 15])",
                                         R"(["a", "b", "c", "d", "e"])");
    DoDiff();
    AssertInsertIs("[false, true, true, true]");
    AssertRunLengthIs("[12, 0, 0, 0]");

    // prepend some
    base_ = RunEndEncodedArrayFromJSON(12, ree_type, R"([2, 4, 8, 12])",
                                       R"(["c", "d", "e", "f"])");
    target_ = RunEndEncodedArrayFromJSON(15, ree_type, R"([1, 3, 5, 7, 11, 15])",
                                         R"(["a", "b", "c", "d", "e", "f"])");
    DoDiff();
    AssertInsertIs("[false, true, true, true]");
    AssertRunLengthIs("[0, 0, 0, 12]");
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
  ASSERT_EQ(formatted.str(), "# Array types differed: int32 vs string\n");
}

template <typename ArrowType>
class DiffTestWithNumeric : public DiffTest {
 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TYPED_TEST_SUITE(DiffTestWithNumeric, NumericArrowTypes);

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
  for (auto null_probability : {0.0, 0.25}) {
    auto values = this->rng_.Int64(1 << 10, 0, 127, null_probability);
    for (const double filter_probability : {0.99, 0.75, 0.5}) {
      this->BaseAndTargetFromRandomFilter(values, filter_probability);

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
  for (auto null_probability : {0.0, 0.25}) {
    auto values = this->rng_.StringWithRepeats(1 << 10, 1 << 8, 0, 32, null_probability);
    for (const double filter_probability : {0.99, 0.75, 0.5}) {
      this->BaseAndTargetFromRandomFilter(values, filter_probability);

      std::stringstream formatted;
      this->DoDiffAndFormat(&formatted);
      auto st = ValidateEditScript(*this->edits_, *this->base_, *this->target_);
      if (!st.ok()) {
        ASSERT_OK(Status(st.code(), st.message() + "\n" + formatted.str()));
      }
    }
  }
}

TEST_F(DiffTest, BasicsWithBooleans) {
  // insert one
  base_ = ArrayFromJSON(boolean(), R"([true, true, true])");
  target_ = ArrayFromJSON(boolean(), R"([true, false, true, true])");
  DoDiff();
  AssertInsertIs("[false, true]");
  AssertRunLengthIs("[1, 2]");

  // delete one
  base_ = ArrayFromJSON(boolean(), R"([true, false, true, true])");
  target_ = ArrayFromJSON(boolean(), R"([true, true, true])");
  DoDiff();
  AssertInsertIs("[false, false]");
  AssertRunLengthIs("[1, 2]");

  // change one
  base_ = ArrayFromJSON(boolean(), R"([false, false, true])");
  target_ = ArrayFromJSON(boolean(), R"([true, false, true])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[0, 0, 2]");

  // null out one
  base_ = ArrayFromJSON(boolean(), R"([true, false, true])");
  target_ = ArrayFromJSON(boolean(), R"([true, false, null])");
  DoDiff();
  AssertInsertIs("[false, false, true]");
  AssertRunLengthIs("[2, 0, 0]");
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

TEST_F(DiffTest, BasicsWithSparseUnions) { TestBasicsWithUnions(UnionMode::SPARSE); }

TEST_F(DiffTest, BasicsWithDenseUnions) { TestBasicsWithUnions(UnionMode::DENSE); }

TEST_F(DiffTest, BasicsWithREEs) {
  TestBasicsWithREEs<Int16Type>();
  TestBasicsWithREEs<Int32Type>();
  TestBasicsWithREEs<Int64Type>();
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

  // Month, Day, Nano Intervals
  base_ = ArrayFromJSON(month_day_nano_interval(), R"([[2, 3, 1]])");
  target_ = ArrayFromJSON(month_day_nano_interval(), R"([])");
  AssertDiffAndFormat(R"(
@@ -0, +0 @@
-2M3d1ns
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
  for (auto union_ : UnionTypeFactories()) {
    type = union_({field("foo", utf8()), field("bar", int32())}, {2, 5});
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

  for (const auto& type : {
           decimal128(10, 4),
           decimal256(10, 4),
       }) {
    base_ = ArrayFromJSON(type, R"(["123.4567", "-78.9000"])");
    target_ = ArrayFromJSON(type, R"(["123.4567", "-123.4567"])");
    AssertDiffAndFormat(R"(
@@ -1, +1 @@
--78.9000
+-123.4567
)");
  }
}

TEST_F(DiffTest, DictionaryDiffFormatter) {
  std::stringstream formatted;

  // differing indices
  auto base_dict = ArrayFromJSON(utf8(), R"(["a", "b", "c"])");
  auto base_indices = ArrayFromJSON(int8(), "[0, 1, 2, 2, 0, 1]");
  ASSERT_OK_AND_ASSIGN(base_, DictionaryArray::FromArrays(
                                  dictionary(base_indices->type(), base_dict->type()),
                                  base_indices, base_dict));

  auto target_dict = base_dict;
  auto target_indices = ArrayFromJSON(int8(), "[0, 1, 2, 2, 1, 1]");
  ASSERT_OK_AND_ASSIGN(
      target_,
      DictionaryArray::FromArrays(dictionary(target_indices->type(), target_dict->type()),
                                  target_indices, target_dict));

  base_->Equals(*target_, EqualOptions().diff_sink(&formatted));
  auto formatted_expected_indices = R"(# Dictionary arrays differed
## dictionary diff
## indices diff
@@ -4, +4 @@
-0
@@ -6, +5 @@
+1
)";
  ASSERT_EQ(formatted.str(), formatted_expected_indices);

  // Note: Diff doesn't work at the moment with dictionary arrays
  ASSERT_RAISES(NotImplemented, Diff(*base_, *target_));

  // differing dictionaries
  target_dict = ArrayFromJSON(utf8(), R"(["b", "c", "a"])");
  target_indices = base_indices;
  ASSERT_OK_AND_ASSIGN(
      target_,
      DictionaryArray::FromArrays(dictionary(target_indices->type(), target_dict->type()),
                                  target_indices, target_dict));

  formatted.str("");
  base_->Equals(*target_, EqualOptions().diff_sink(&formatted));
  auto formatted_expected_values = R"(# Dictionary arrays differed
## dictionary diff
@@ -0, +0 @@
-"a"
@@ -3, +2 @@
+"a"
## indices diff
)";
  ASSERT_EQ(formatted.str(), formatted_expected_values);
}

void MakeSameLength(std::shared_ptr<Array>* a, std::shared_ptr<Array>* b) {
  auto length = std::min((*a)->length(), (*b)->length());
  *a = (*a)->Slice(0, length);
  *b = (*b)->Slice(0, length);
}

TEST_F(DiffTest, CompareRandomStruct) {
  for (auto null_probability : {0.0, 0.25}) {
    constexpr auto length = 1 << 10;
    auto int32_values = this->rng_.Int32(length, 0, 127, null_probability);
    auto utf8_values = this->rng_.String(length, 0, 16, null_probability);
    for (const double filter_probability : {0.9999, 0.75}) {
      this->BaseAndTargetFromRandomFilter(int32_values, filter_probability);
      auto int32_base = this->base_;
      auto int32_target = this->base_;

      this->BaseAndTargetFromRandomFilter(utf8_values, filter_probability);
      auto utf8_base = this->base_;
      auto utf8_target = this->base_;

      MakeSameLength(&int32_base, &utf8_base);
      MakeSameLength(&int32_target, &utf8_target);

      auto type = struct_({field("i", int32()), field("s", utf8())});
      auto base_res = StructArray::Make({int32_base, utf8_base}, type->fields());
      ASSERT_OK(base_res.status());
      base_ = base_res.ValueOrDie();
      auto target_res = StructArray::Make({int32_target, utf8_target}, type->fields());
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
