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

#include "benchmark/benchmark.h"

#include <string>
#include <vector>

#include "arrow/util/decimal.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace Decimal {

static void FromString(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<std::string> values = {"0",
                                     "1.23",
                                     "12.345e6",
                                     "-12.345e-6",
                                     "123456789.123456789",
                                     "1231234567890.451234567890"};

  for (auto _ : state) {
    for (const auto& value : values) {
      Decimal128 dec;
      int32_t scale, precision;
      benchmark::DoNotOptimize(Decimal128::FromString(value, &dec, &scale, &precision));
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

constexpr int32_t kValueSize = 10;

static void BinaryCompareOp(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }
  for (auto _ : state) {
    for (int x = 0; x < kValueSize; x += 4) {
      benchmark::DoNotOptimize(v1[x] == v2[x]);
      benchmark::DoNotOptimize(v1[x + 1] <= v2[x + 1]);
      benchmark::DoNotOptimize(v1[x + 2] >= v2[x + 2]);
      benchmark::DoNotOptimize(v1[x + 3] >= v1[x + 3]);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryCompareOpConstant(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
  }
  BasicDecimal128 constant(313, 212);
  for (auto _ : state) {
    for (int x = 0; x < kValueSize; x += 4) {
      benchmark::DoNotOptimize(v1[x] == constant);
      benchmark::DoNotOptimize(v1[x + 1] <= constant);
      benchmark::DoNotOptimize(v1[x + 2] >= constant);
      benchmark::DoNotOptimize(v1[x + 3] != constant);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpAggregate(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v;
  for (int x = 0; x < kValueSize; x++) {
    v.emplace_back(100 + x, 100 + x);
  }

  for (auto _ : state) {
    BasicDecimal128 result;
    for (int x = 0; x < 100; x++) {
      result += v[x];
    }
    benchmark::DoNotOptimize(result);
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOp(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; x += 5) {
      benchmark::DoNotOptimize(v1[x] - v2[x]);
      benchmark::DoNotOptimize(v1[x + 1] + v2[x + 1]);
      benchmark::DoNotOptimize(v1[x + 2] * v2[x + 2]);
      benchmark::DoNotOptimize(v1[x + 3] / v2[x + 3]);
      benchmark::DoNotOptimize(v1[x + 4] % v2[x + 4]);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void UnaryOp(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v;
  for (int x = 0; x < kValueSize; x++) {
    v.emplace_back(100 + x, 100 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; x += 2) {
      benchmark::DoNotOptimize(v[x].Abs());
      benchmark::DoNotOptimize(v[x + 1].Negate());
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void Constants(benchmark::State& state) {  // NOLINT non-const reference
  BasicDecimal128 d1(-546, 123), d2(-123, 456);
  for (auto _ : state) {
    benchmark::DoNotOptimize(BasicDecimal128::GetMaxValue() - d1);
    benchmark::DoNotOptimize(BasicDecimal128::GetScaleMultiplier(3) + d2);
  }
  state.SetItemsProcessed(state.iterations() * 2);
}

static void BinaryBitOp(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; x += 2) {
      benchmark::DoNotOptimize(v1[x] |= v2[x]);
      benchmark::DoNotOptimize(v1[x + 1] &= v2[x + 1]);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

BENCHMARK(FromString);
BENCHMARK(BinaryMathOp);
BENCHMARK(BinaryMathOpAggregate);
BENCHMARK(BinaryCompareOp);
BENCHMARK(BinaryCompareOpConstant);
BENCHMARK(UnaryOp);
BENCHMARK(Constants);
BENCHMARK(BinaryBitOp);

}  // namespace Decimal
}  // namespace arrow
