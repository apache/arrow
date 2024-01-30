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
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace Decimal {

static const std::vector<std::string>& GetValuesAsString() {
  static const std::vector<std::string> kValues = {"0",
                                                   "1.23",
                                                   "12.345e6",
                                                   "-12.345e-6",
                                                   "123456789.123456789",
                                                   "1231234567890.451234567890"};
  return kValues;
}

struct DecimalValueAndScale {
  Decimal128 decimal;
  int32_t scale;
};

static std::vector<DecimalValueAndScale> GetDecimalValuesAndScales() {
  const std::vector<std::string>& value_strs = GetValuesAsString();
  std::vector<DecimalValueAndScale> result(value_strs.size());
  for (size_t i = 0; i < value_strs.size(); ++i) {
    int32_t precision;
    ARROW_CHECK_OK(Decimal128::FromString(value_strs[i], &result[i].decimal,
                                          &result[i].scale, &precision));
  }
  return result;
}

static void FromString(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<std::string>& values = GetValuesAsString();
  for (auto _ : state) {
    for (const auto& value : values) {
      Decimal128 dec;
      int32_t scale, precision;
      auto status = Decimal128::FromString(value, &dec, &scale, &precision);
      benchmark::DoNotOptimize(status);
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void ToString(benchmark::State& state) {  // NOLINT non-const reference
  static const std::vector<DecimalValueAndScale> values = GetDecimalValuesAndScales();
  for (auto _ : state) {
    for (const DecimalValueAndScale& item : values) {
      auto string = item.decimal.ToString(item.scale);
      benchmark::DoNotOptimize(string);
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
      auto equal = v1[x] == v2[x];
      benchmark::DoNotOptimize(equal);
      auto less_than_or_equal = v1[x + 1] <= v2[x + 1];
      benchmark::DoNotOptimize(less_than_or_equal);
      auto greater_than_or_equal1 = v1[x + 2] >= v2[x + 2];
      benchmark::DoNotOptimize(greater_than_or_equal1);
      auto greater_than_or_equal2 = v1[x + 3] >= v1[x + 3];
      benchmark::DoNotOptimize(greater_than_or_equal2);
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
      auto equal = v1[x] == constant;
      benchmark::DoNotOptimize(equal);
      auto less_than_or_equal = v1[x + 1] <= constant;
      benchmark::DoNotOptimize(less_than_or_equal);
      auto greater_than_or_equal = v1[x + 2] >= constant;
      benchmark::DoNotOptimize(greater_than_or_equal);
      auto not_equal = v1[x + 3] != constant;
      benchmark::DoNotOptimize(not_equal);
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

static void BinaryMathOpAdd128(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto add = v1[x] + v2[x];
      benchmark::DoNotOptimize(add);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpMultiply128(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto multiply = v1[x] * v2[x];
      benchmark::DoNotOptimize(multiply);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpDivide128(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal128> v1, v2;
  for (int x = 0; x < kValueSize; x++) {
    v1.emplace_back(100 + x, 100 + x);
    v2.emplace_back(200 + x, 200 + x);
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto divide = v1[x] / v2[x];
      benchmark::DoNotOptimize(divide);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpAdd256(benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal256> v1, v2;
  for (uint64_t x = 0; x < kValueSize; x++) {
    v1.push_back(BasicDecimal256({100 + x, 100 + x, 100 + x, 100 + x}));
    v2.push_back(BasicDecimal256({200 + x, 200 + x, 200 + x, 200 + x}));
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto add = v1[x] + v2[x];
      benchmark::DoNotOptimize(add);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpMultiply256(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal256> v1, v2;
  for (uint64_t x = 0; x < kValueSize; x++) {
    v1.push_back(BasicDecimal256({100 + x, 100 + x, 100 + x, 100 + x}));
    v2.push_back(BasicDecimal256({200 + x, 200 + x, 200 + x, 200 + x}));
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto multiply = v1[x] * v2[x];
      benchmark::DoNotOptimize(multiply);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void BinaryMathOpDivide256(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<BasicDecimal256> v1, v2;
  for (uint64_t x = 0; x < kValueSize; x++) {
    v1.push_back(BasicDecimal256({100 + x, 100 + x, 100 + x, 100 + x}));
    v2.push_back(BasicDecimal256({200 + x, 200 + x, 200 + x, 200 + x}));
  }

  for (auto _ : state) {
    for (int x = 0; x < kValueSize; ++x) {
      auto divide = v1[x] / v2[x];
      benchmark::DoNotOptimize(divide);
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
      auto abs = v[x].Abs();
      benchmark::DoNotOptimize(abs);
      auto negate = v[x + 1].Negate();
      benchmark::DoNotOptimize(negate);
    }
  }
  state.SetItemsProcessed(state.iterations() * kValueSize);
}

static void Constants(benchmark::State& state) {  // NOLINT non-const reference
  BasicDecimal128 d1(-546, 123), d2(-123, 456);
  for (auto _ : state) {
    auto sub = BasicDecimal128::GetMaxValue() - d1;
    benchmark::DoNotOptimize(sub);
    auto add = BasicDecimal128::GetScaleMultiplier(3) + d2;
    benchmark::DoNotOptimize(add);
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
BENCHMARK(ToString);
BENCHMARK(BinaryMathOpAdd128);
BENCHMARK(BinaryMathOpMultiply128);
BENCHMARK(BinaryMathOpDivide128);
BENCHMARK(BinaryMathOpAdd256);
BENCHMARK(BinaryMathOpMultiply256);
BENCHMARK(BinaryMathOpDivide256);
BENCHMARK(BinaryMathOpAggregate);
BENCHMARK(BinaryCompareOp);
BENCHMARK(BinaryCompareOpConstant);
BENCHMARK(UnaryOp);
BENCHMARK(Constants);
BENCHMARK(BinaryBitOp);

}  // namespace Decimal
}  // namespace arrow
