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

  void DoFormat(std::string* out) {
    DoDiff();
    std::stringstream ss;
    auto formatter = MakeUnifiedDiffFormatter(ss, *base_, *target_);
    ASSERT_OK(formatter.status());
    ASSERT_OK(formatter.ValueOrDie()->Visit(*edits_));
    *out = ss.str();
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
  ASSERT_EQ(run_lengths_->Value(0), 0);

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
  base_ = ArrayFromJSON(int32(), "[]");
  target_ = ArrayFromJSON(utf8(), "[]");
  ASSERT_RAISES(TypeError, Diff(*base_, *target_, default_memory_pool(), &edits_));
}

TYPED_TEST_CASE(DiffTestWithNumeric, NumericArrowTypes);

TYPED_TEST(DiffTestWithNumeric, Basics) {
  // insert one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 4, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 2);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), true);
  ASSERT_EQ(this->run_lengths_->Value(1), 2);

  // delete one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 4, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 2);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), false);
  ASSERT_EQ(this->run_lengths_->Value(1), 2);

  // change one
  this->base_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5]");
  this->target_ = ArrayFromJSON(this->type_singleton(), "[1, 2, 23, 4, 5]");
  this->DoDiff();
  ASSERT_EQ(this->edits_->length(), 3);
  ASSERT_EQ(this->insert_->Value(0), false);
  ASSERT_EQ(this->run_lengths_->Value(0), 2);
  ASSERT_EQ(this->insert_->Value(1), false);
  ASSERT_EQ(this->run_lengths_->Value(1), 0);
  ASSERT_EQ(this->insert_->Value(2), true);
  ASSERT_EQ(this->run_lengths_->Value(2), 2);
}

TYPED_TEST(DiffTestWithNumeric, CompareRandomNumeric) {
  compute::FunctionContext ctx;
  for (auto null_probability : {0.0}) {
    auto values =
        this->rng_.template Numeric<TypeParam>(1 << 13, 0, 127, null_probability);
    for (size_t j = 0; j < 13; j++) {
      const int64_t length = static_cast<int64_t>(1ULL << j);
      const double filter_probability = static_cast<double>(length) / values->length();
      auto filter_1 = this->rng_.Boolean(values->length(), filter_probability, 0.0);
      auto filter_2 = this->rng_.Boolean(values->length(), filter_probability, 0.0);

      ASSERT_OK(compute::Filter(&ctx, *values, *filter_1, &this->base_));
      ASSERT_OK(compute::Filter(&ctx, *values, *filter_2, &this->target_));
      this->DoDiff();
      auto st = AssertEditScript{*this->base_, *this->target_}.Visit(*this->edits_);
      if (!st.ok()) {
        std::string formatted;
        this->DoFormat(&formatted);
        ASSERT_TRUE(false) << formatted;
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
}

TEST_F(DiffTest, UnifiedDiffFormatter) {
  std::string formatted;

  // insert one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  DoFormat(&formatted);
  ASSERT_EQ(formatted, R"(
@@ -1, +1 @@
+"me"
)");

  // delete one
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  DoFormat(&formatted);
  ASSERT_EQ(formatted, R"(
@@ -1, +1 @@
-"me"
)");

  // change one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["gimme", "a", "break"])");
  DoFormat(&formatted);
  ASSERT_EQ(formatted, R"(
@@ -0, +0 @@
-"give"
+"gimme"
)");
}

}  // namespace arrow
