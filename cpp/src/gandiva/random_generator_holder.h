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

#pragma once

#include <limits>
#include <memory>
#include <random>

#include "arrow/status.h"
#include "arrow/util/io_util.h"

#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Function Holder for 'random'
class GANDIVA_EXPORT RandomGeneratorHolder : public FunctionHolder {
 public:
  ~RandomGeneratorHolder() override = default;

  static Result<std::shared_ptr<RandomGeneratorHolder>> Make(const FunctionNode& node);

  double operator()() { return distribution_(generator_); }

 private:
  explicit RandomGeneratorHolder(int seed) : distribution_(0, 1) {
    int64_t seed64 = static_cast<int64_t>(seed);
    seed64 = (seed64 ^ 0x00000005DEECE66D) & 0x0000ffffffffffff;
    generator_.seed(static_cast<uint64_t>(seed64));
  }

  RandomGeneratorHolder() : distribution_(0, 1) {
    generator_.seed(::arrow::internal::GetRandomSeed());
  }

  std::mt19937_64 generator_;
  std::uniform_real_distribution<> distribution_;
};

/// Function Holder for 'rand_integer'
class GANDIVA_EXPORT RandomIntegerGeneratorHolder : public FunctionHolder {
 public:
  ~RandomIntegerGeneratorHolder() override = default;

  static Result<std::shared_ptr<RandomIntegerGeneratorHolder>> Make(
      const FunctionNode& node);

  int32_t operator()() { return distribution_(generator_); }

 private:
  // Full range: [INT32_MIN, INT32_MAX]
  RandomIntegerGeneratorHolder()
      : distribution_(std::numeric_limits<int32_t>::min(),
                      std::numeric_limits<int32_t>::max()) {
    generator_.seed(::arrow::internal::GetRandomSeed());
  }

  // Range: [0, range - 1]
  explicit RandomIntegerGeneratorHolder(int32_t range) : distribution_(0, range - 1) {
    generator_.seed(::arrow::internal::GetRandomSeed());
  }

  // Min/Max: [min, max] inclusive
  RandomIntegerGeneratorHolder(int32_t min, int32_t max) : distribution_(min, max) {
    generator_.seed(::arrow::internal::GetRandomSeed());
  }

  std::mt19937_64 generator_;
  std::uniform_int_distribution<int32_t> distribution_;
};

}  // namespace gandiva
