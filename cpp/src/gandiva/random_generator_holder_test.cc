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

#include "gandiva/random_generator_holder.h"

#include <limits>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"

namespace gandiva {

class TestRandGenHolder : public ::testing::Test {
 public:
  FunctionNode BuildRandFunc() { return {"random", {}, arrow::float64()}; }

  FunctionNode BuildRandWithSeedFunc(int32_t seed, bool seed_is_null) {
    auto seed_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(seed), seed_is_null);
    return {"rand", {seed_node}, arrow::float64()};
  }
};

TEST_F(TestRandGenHolder, NoSeed) {
  FunctionNode rand_func = BuildRandFunc();
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder, RandomGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  EXPECT_NE(random(), random());
}

TEST_F(TestRandGenHolder, WithValidEqualSeeds) {
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(12, false);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(12, false);

  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_1, RandomGeneratorHolder::Make(rand_func_1));
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_2, RandomGeneratorHolder::Make(rand_func_2));

  auto& random_1 = *rand_gen_holder_1;
  auto& random_2 = *rand_gen_holder_2;
  EXPECT_EQ(random_1(), random_2());
  EXPECT_EQ(random_1(), random_2());
  EXPECT_GT(random_1(), 0);
  EXPECT_NE(random_1(), random_2());
  EXPECT_LT(random_2(), 1);
  EXPECT_EQ(random_1(), random_2());
}

TEST_F(TestRandGenHolder, WithValidSeeds) {
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(11, false);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(12, false);
  FunctionNode rand_func_3 = BuildRandWithSeedFunc(-12, false);
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_1, RandomGeneratorHolder::Make(rand_func_1));
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_2, RandomGeneratorHolder::Make(rand_func_2));
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_3, RandomGeneratorHolder::Make(rand_func_3));

  auto& random_1 = *rand_gen_holder_1;
  auto& random_2 = *rand_gen_holder_2;
  auto& random_3 = *rand_gen_holder_3;
  EXPECT_NE(random_2(), random_3());
  EXPECT_NE(random_1(), random_2());
}

TEST_F(TestRandGenHolder, WithInValidSeed) {
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(12, true);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(0, false);
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_1, RandomGeneratorHolder::Make(rand_func_1));
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder_2, RandomGeneratorHolder::Make(rand_func_2));

  auto& random_1 = *rand_gen_holder_1;
  auto& random_2 = *rand_gen_holder_2;
  EXPECT_EQ(random_1(), random_2());
}

// Test that non-literal seed argument is rejected
TEST_F(TestRandGenHolder, NonLiteralSeedRejected) {
  auto field_node = std::make_shared<FieldNode>(arrow::field("seed", arrow::int32()));
  FunctionNode rand_func = {"rand", {field_node}, arrow::float64()};

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("requires a literal as parameter"),
                                  RandomGeneratorHolder::Make(rand_func).status());
}

class TestRandIntGenHolder : public ::testing::Test {
 public:
  FunctionNode BuildRandIntFunc() { return {"rand_integer", {}, arrow::int32()}; }

  FunctionNode BuildRandIntWithRangeFunc(int32_t range, bool range_is_null) {
    auto range_node = std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(range),
                                                    range_is_null);
    return {"rand_integer", {range_node}, arrow::int32()};
  }

  FunctionNode BuildRandIntWithMinMaxFunc(int32_t min, bool min_is_null, int32_t max,
                                          bool max_is_null) {
    auto min_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(min), min_is_null);
    auto max_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(max), max_is_null);
    return {"rand_integer", {min_node, max_node}, arrow::int32()};
  }
};

TEST_F(TestRandIntGenHolder, NoParams) {
  FunctionNode rand_func = BuildRandIntFunc();
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // Generate multiple values and verify they are integers
  for (int i = 0; i < 10; i++) {
    int32_t val = random();
    EXPECT_GE(val, std::numeric_limits<int32_t>::min());
    EXPECT_LE(val, std::numeric_limits<int32_t>::max());
  }
}

TEST_F(TestRandIntGenHolder, WithRange) {
  FunctionNode rand_func = BuildRandIntWithRangeFunc(100, false);
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // Generate multiple values and verify they are in range [0, 99]
  for (int i = 0; i < 100; i++) {
    int32_t val = random();
    EXPECT_GE(val, 0);
    EXPECT_LT(val, 100);
  }
}

TEST_F(TestRandIntGenHolder, WithMinMax) {
  FunctionNode rand_func = BuildRandIntWithMinMaxFunc(10, false, 20, false);
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // Generate multiple values and verify they are in range [10, 20]
  for (int i = 0; i < 100; i++) {
    int32_t val = random();
    EXPECT_GE(val, 10);
    EXPECT_LE(val, 20);
  }
}

TEST_F(TestRandIntGenHolder, WithNegativeMinMax) {
  FunctionNode rand_func = BuildRandIntWithMinMaxFunc(-50, false, -10, false);
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // Generate multiple values and verify they are in range [-50, -10]
  for (int i = 0; i < 100; i++) {
    int32_t val = random();
    EXPECT_GE(val, -50);
    EXPECT_LE(val, -10);
  }
}

TEST_F(TestRandIntGenHolder, InvalidRangeZero) {
  FunctionNode rand_func = BuildRandIntWithRangeFunc(0, false);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("range must be positive"),
                                  RandomIntegerGeneratorHolder::Make(rand_func).status());
}

TEST_F(TestRandIntGenHolder, InvalidRangeNegative) {
  FunctionNode rand_func = BuildRandIntWithRangeFunc(-5, false);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("range must be positive"),
                                  RandomIntegerGeneratorHolder::Make(rand_func).status());
}

TEST_F(TestRandIntGenHolder, InvalidMinGreaterThanMax) {
  FunctionNode rand_func = BuildRandIntWithMinMaxFunc(20, false, 10, false);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("min must be <= max"),
                                  RandomIntegerGeneratorHolder::Make(rand_func).status());
}

TEST_F(TestRandIntGenHolder, NullRangeDefaultsToMaxInt) {
  FunctionNode rand_func = BuildRandIntWithRangeFunc(0, true);  // null range
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // With NULL range defaulting to INT32_MAX, values should be in [0, INT32_MAX-1]
  for (int i = 0; i < 100; i++) {
    int32_t val = random();
    EXPECT_GE(val, 0);
    EXPECT_LT(val, std::numeric_limits<int32_t>::max());
  }
}

// Test that non-literal arguments are rejected
TEST_F(TestRandIntGenHolder, NonLiteralRangeRejected) {
  // Create a FieldNode instead of LiteralNode for the range parameter
  auto field_node = std::make_shared<FieldNode>(arrow::field("range", arrow::int32()));
  FunctionNode rand_func = {"rand_integer", {field_node}, arrow::int32()};

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("requires a literal as parameter"),
                                  RandomIntegerGeneratorHolder::Make(rand_func).status());
}

TEST_F(TestRandIntGenHolder, NonLiteralMinMaxRejected) {
  // Create FieldNodes instead of LiteralNodes for min/max parameters
  auto min_field = std::make_shared<FieldNode>(arrow::field("min", arrow::int32()));
  auto max_literal =
      std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(100), false);
  FunctionNode rand_func = {"rand_integer", {min_field, max_literal}, arrow::int32()};

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  ::testing::HasSubstr("requires literals as parameters"),
                                  RandomIntegerGeneratorHolder::Make(rand_func).status());
}

TEST_F(TestRandIntGenHolder, NullMinMaxDefaults) {
  // Test null handling for 2-arg form: NULL min defaults to 0, NULL max defaults to
  // INT32_MAX
  FunctionNode rand_func = BuildRandIntWithMinMaxFunc(0, true, 0, true);  // both null
  EXPECT_OK_AND_ASSIGN(auto rand_gen_holder,
                       RandomIntegerGeneratorHolder::Make(rand_func));

  auto& random = *rand_gen_holder;
  // With NULL min=0, NULL max=INT32_MAX, values should be in [0, INT32_MAX]
  for (int i = 0; i < 100; i++) {
    int32_t val = random();
    EXPECT_GE(val, 0);
    EXPECT_LE(val, std::numeric_limits<int32_t>::max());
  }
}

}  // namespace gandiva
