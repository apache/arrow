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
    struct_({field("insert", boolean()), field("run_length", uint64())});

struct AssertEditScript : DiffVisitor {
  AssertEditScript(const Array& base, const Array& target)
      : base_(base), target_(target) {}

  Status Insert(int64_t target_index) override {
    ++target_end_;
    return Status::OK();
  }

  Status Delete(int64_t base_index) override {
    for (int64_t i = target_begin_; i < target_end_; ++i) {
      if (target_.RangeEquals(i, i + 1, base_end_, base_)) {
        return Status::Invalid("a deleted element was simultaneously inserted");
      }
    }
    ++base_end_;
    return Status::OK();
  }

  Status Run(int64_t length) override {
    if (!base_.RangeEquals(base_end_, base_end_ + length, target_end_, target_)) {
      return Status::Invalid("base and target were not equal in a run");
    }
    base_begin_ = base_end_ += length;
    target_begin_ = target_end_ += length;
    return Status::OK();
  }

  int64_t base_begin_ = 0, base_end_ = 0, target_begin_ = 0, target_end_ = 0;
  const Array& base_;
  const Array& target_;
};

class DiffTest : public ::testing::Test {
 protected:
  DiffTest() : rng_(kSeed) {}

  void DoDiff() {
    ASSERT_OK(Diff(*base_, *target_, default_memory_pool(), &edits_));
    ASSERT_OK(ValidateArray(*edits_));
    ASSERT_TRUE(edits_->type()->Equals(edits_type));
    const auto& edits = checked_cast<const StructArray&>(*edits_);
    insert_ = checked_pointer_cast<BooleanArray>(edits.field(0));
    run_lengths_ = checked_pointer_cast<UInt64Array>(edits.field(1));
  }

  void DoDiffAndFormat(std::stringstream* out) {
    DoDiff();
    auto formatter = MakeUnifiedDiffFormatter(out, *base_, *target_);
    ASSERT_OK(formatter.status());
    ASSERT_OK(formatter.ValueOrDie()->Visit(*edits_));
  }

  // validate diff and assert that it formats as expected, both directly
  // and through Array::Equals
  void AssertDiffAndFormat(const std::string& formatted_expected) {
    std::stringstream formatted;

    DoDiffAndFormat(&formatted);
    ASSERT_EQ(formatted.str(), formatted_expected) << "formatted diff incorrectly";
    formatted.str("");

    if (base_->Equals(*target_, EqualOptions().diff_sink(&formatted))) {
      formatted << std::endl;
    }
    ASSERT_EQ(formatted.str(), formatted_expected)
        << "Array::Equals formatted diff incorrectly";
  }

  random::RandomArrayGenerator rng_;
  std::shared_ptr<Array> base_, target_, edits_;
  std::shared_ptr<BooleanArray> insert_;
  std::shared_ptr<UInt64Array> run_lengths_;
};

TEST_F(DiffTest, Trivial) {
  base_ = ArrayFromJSON(int32(), "[]");
  target_ = ArrayFromJSON(int32(), "[]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 1);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 0);

  base_ = ArrayFromJSON(null(), "[null, null]");
  target_ = ArrayFromJSON(null(), "[null, null, null, null]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 0);

  base_ = ArrayFromJSON(int32(), "[1, 2, 3]");
  target_ = ArrayFromJSON(int32(), "[1, 2, 3]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 1);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 3);
}

template <typename ArrowType>
class DiffTestWithNumeric : public DiffTest {
 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TEST_F(DiffTest, Errors) {
  std::stringstream formatted;

  base_ = ArrayFromJSON(int32(), "[]");
  target_ = ArrayFromJSON(utf8(), "[]");
  ASSERT_RAISES(TypeError, Diff(*base_, *target_, default_memory_pool(), &edits_));

  ASSERT_FALSE(base_->Equals(*target_, EqualOptions().diff_sink(&formatted)));
  ASSERT_EQ(formatted.str(), R"(# Array types differed: int32 vs string)");
}

TYPED_TEST_CASE(DiffTestWithNumeric, NumericArrowTypes);

TYPED_TEST(DiffTestWithNumeric, Basics) {
  // insert one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 2);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), true);
  ASSERT_EQ(this->run_lengths_->Value(1), 2);

  // delete one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 2);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), false);
  ASSERT_EQ(this->run_lengths_->Value(1), 2);

  // change one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 23, null, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 3);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), false);
  ASSERT_EQ(this->run_lengths_->Value(1), 0);
  ASSERT_EQ(this->insert_->Value(2), true);
  ASSERT_EQ(this->run_lengths_->Value(2), 2);

  // null out one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, null, null, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 3);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), false);
  ASSERT_EQ(this->run_lengths_->Value(1), 1);
  ASSERT_EQ(this->insert_->Value(2), true);
  ASSERT_EQ(this->run_lengths_->Value(2), 1);

  // append some
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, null, 5, 6, 7, 8, 9]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 5);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 5);
  for (int64_t i = 1; i < this->edits_->length(); ++i) {
    ASSERT_EQ(this->insert_->Value(i), true);
    ASSERT_EQ(this->run_lengths_->Value(i), 0);
  }
}

TYPED_TEST(DiffTestWithNumeric, CompareRandomNumeric) {
  compute::FunctionContext ctx;
  for (auto null_probability : {0.0, 0.1, 0.5}) {
    auto values =
        this->rng_.template Numeric<TypeParam>(1 << 10, 0, 127, null_probability);
    for (const double filter_probability : {0.99, 0.9, 0.75, 0.5}) {
      auto filter_1 = this->rng_.Boolean(values->length(), filter_probability, 0.0);
      auto filter_2 = this->rng_.Boolean(values->length(), filter_probability, 0.0);

      ASSERT_OK(compute::Filter(&ctx, *values, *filter_1, &this->base_));
      ASSERT_OK(compute::Filter(&ctx, *values, *filter_2, &this->target_));

      std::stringstream formatted;
      this->DoDiffAndFormat(&formatted);
      auto st = AssertEditScript{*this->base_, *this->target_}.Visit(*this->edits_);
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
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["gimme", "a", "break"])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 0);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);

  // null out one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", null])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 0);
}

TEST_F(DiffTest, BasicsWithLists) {
  // insert one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [5, 9], [], [13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [5, 9], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[3, 3, 3], [], [13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 0);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);

  // null out one
  base_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], [13]])");
  target_ = ArrayFromJSON(list(int32()), R"([[2, 3, 1], [], null])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 0);
}

TEST_F(DiffTest, BasicsWithStructs) {
  auto type = struct_({field("foo", utf8()), field("bar", int32())});

  // insert one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ =
      ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ =
      ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {"foo": "?"}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 2}, {}, {"bar": 13}])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 0);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);

  // null out one
  base_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, {"bar": 13}])");
  target_ = ArrayFromJSON(type, R"([{"foo": "!", "bar": 3}, {}, null])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 0);
}

TEST_F(DiffTest, BasicsWithUnions) {
  auto type = union_({field("foo", utf8()), field("bar", int32())}, {2, 5});

  // insert one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [2, "?"], [5, 3], [5, 13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [2, "?"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(type, R"([[5, 3], [2, "!"], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "3"], [2, "!"], [5, 13]])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 0);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);

  // null out one
  base_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], [5, 13]])");
  target_ = ArrayFromJSON(type, R"([[2, "!"], [5, 3], null])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(insert_->Value(0), false);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 0);
}

TEST_F(DiffTest, UnifiedDiffFormatter) {
  // no changes
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  AssertDiffAndFormat(R"(
)");

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

  // small difference
  base_ = ArrayFromJSON(uint16(), "[0, 1, 2, 3, 5, 8, 11, 13, 17]");
  target_ = ArrayFromJSON(uint16(), "[2, 3, 5, 7, 11, 13, 17, 19]");
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
  base_ = ArrayFromJSON(uint16(), "[57, 10, 22, 126, 42]");
  target_ = ArrayFromJSON(uint16(), "[58, 57, 75, 93, 53, 8, 22, 42, 79, 11]");
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
  for (auto null_probability : {0.0, 0.1, 0.5}) {
    constexpr auto length = 1 << 10;
    auto int32_values = this->rng_.Int32(length, 0, 127, null_probability);
    auto utf8_values = this->rng_.String(length, 0, 16, null_probability);
    for (const double filter_probability : {0.9999, 0.999, 0.75}) {
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
      auto st = AssertEditScript{*this->base_, *this->target_}.Visit(*this->edits_);
      if (!st.ok()) {
        ASSERT_OK(Status(st.code(), st.message() + "\n" + formatted.str()));
      }
    }
  }
}

}  // namespace arrow
