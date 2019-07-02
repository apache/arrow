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

class DiffTest : public ::testing::Test {
 protected:
  DiffTest()
      : rng_(kSeed),
        sizes_({0, 1, 2, 4, 16, 31, 1234}),
        null_probabilities_({0.0, 0.1, 0.5, 0.9, 1.0}) {}

  void DoDiff() {
    ASSERT_OK(Diff(*base_, *target_, default_memory_pool(), &edits_));
    ASSERT_OK(ValidateArray(*edits_));
    ASSERT_TRUE(edits_->type()->Equals(edits_type));
    const auto& edits = checked_cast<const StructArray&>(*edits_);
    insert_ = checked_pointer_cast<BooleanArray>(edits.field(0));
    run_lengths_ = checked_pointer_cast<UInt64Array>(edits.field(1));
  }

  random::RandomArrayGenerator rng_;
  std::shared_ptr<Array> base_, target_, edits_;
  std::shared_ptr<BooleanArray> insert_;
  std::shared_ptr<UInt64Array> run_lengths_;
  std::vector<int32_t> sizes_;
  std::vector<double> null_probabilities_;
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
  ASSERT_EQ(run_lengths_->Value(0), 3);
}

TEST_F(DiffTest, Basics) {
  // insert one
  base_ = ArrayFromJSON(int32(), "[1, 2, 4, 5]");
  target_ = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5]");
  target_ = ArrayFromJSON(int32(), "[1, 2, 4, 5]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5]");
  target_ = ArrayFromJSON(int32(), "[1, 2, -3, 4, 5]");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(run_lengths_->Value(0), 2);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);
}

TEST_F(DiffTest, BasicsWithStrings) {
  // insert one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), true);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // delete one
  base_ = ArrayFromJSON(utf8(), R"(["give", "me", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 2);
  ASSERT_EQ(run_lengths_->Value(0), 1);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 2);

  // change one
  base_ = ArrayFromJSON(utf8(), R"(["give", "a", "break"])");
  target_ = ArrayFromJSON(utf8(), R"(["gimme", "a", "break"])");
  DoDiff();
  ASSERT_EQ(edits_->length(), 3);
  ASSERT_EQ(run_lengths_->Value(0), 0);
  ASSERT_EQ(insert_->Value(1), false);
  ASSERT_EQ(run_lengths_->Value(1), 0);
  ASSERT_EQ(insert_->Value(2), true);
  ASSERT_EQ(run_lengths_->Value(2), 2);
}

}  // namespace arrow
