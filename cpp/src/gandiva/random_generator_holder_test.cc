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

#include <memory>

#include <gtest/gtest.h>

namespace gandiva {

class TestRandGenHolder : public ::testing::Test {
 public:
  FunctionNode BuildRandFunc() { return FunctionNode("random", {}, arrow::float64()); }

  FunctionNode BuildRandWithSeedFunc(int32_t seed, bool seed_is_null) {
    auto seed_node =
        std::make_shared<LiteralNode>(arrow::int32(), LiteralHolder(seed), seed_is_null);
    return FunctionNode("rand", {seed_node}, arrow::float64());
  }
};

TEST_F(TestRandGenHolder, NoSeed) {
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder;
  FunctionNode rand_func = BuildRandFunc();
  auto status = RandomGeneratorHolder::Make(rand_func, &rand_gen_holder);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& random = *rand_gen_holder;
  EXPECT_NE(random(), random());
}

TEST_F(TestRandGenHolder, WithValidEqualSeeds) {
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_1;
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_2;
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(12, false);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(12, false);
  auto status = RandomGeneratorHolder::Make(rand_func_1, &rand_gen_holder_1);
  EXPECT_EQ(status.ok(), true) << status.message();
  status = RandomGeneratorHolder::Make(rand_func_2, &rand_gen_holder_2);
  EXPECT_EQ(status.ok(), true) << status.message();

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
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_1;
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_2;
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_3;
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(11, false);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(12, false);
  FunctionNode rand_func_3 = BuildRandWithSeedFunc(-12, false);
  auto status = RandomGeneratorHolder::Make(rand_func_1, &rand_gen_holder_1);
  EXPECT_EQ(status.ok(), true) << status.message();
  status = RandomGeneratorHolder::Make(rand_func_2, &rand_gen_holder_2);
  EXPECT_EQ(status.ok(), true) << status.message();
  status = RandomGeneratorHolder::Make(rand_func_3, &rand_gen_holder_3);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& random_1 = *rand_gen_holder_1;
  auto& random_2 = *rand_gen_holder_2;
  auto& random_3 = *rand_gen_holder_3;
  EXPECT_NE(random_2(), random_3());
  EXPECT_NE(random_1(), random_2());
}

TEST_F(TestRandGenHolder, WithInValidSeed) {
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_1;
  std::shared_ptr<RandomGeneratorHolder> rand_gen_holder_2;
  FunctionNode rand_func_1 = BuildRandWithSeedFunc(12, true);
  FunctionNode rand_func_2 = BuildRandWithSeedFunc(0, false);
  auto status = RandomGeneratorHolder::Make(rand_func_1, &rand_gen_holder_1);
  EXPECT_EQ(status.ok(), true) << status.message();
  status = RandomGeneratorHolder::Make(rand_func_2, &rand_gen_holder_2);
  EXPECT_EQ(status.ok(), true) << status.message();

  auto& random_1 = *rand_gen_holder_1;
  auto& random_2 = *rand_gen_holder_2;
  EXPECT_EQ(random_1(), random_2());
}

}  // namespace gandiva
