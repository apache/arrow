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

#include <arrow/array/concatenate.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/testing/random.h>
#include <benchmark/benchmark.h>

namespace arrow {
namespace compute {

const int64_t elems = 1024 * 1024;

template <typename Type, typename Enable = void>
struct SetBytesProcessed {};

template <typename Type>
struct SetBytesProcessed<Type, enable_if_number<Type>> {
  static void Set(const std::shared_ptr<Array>& cond, const std::shared_ptr<Array>& left,
                  const std::shared_ptr<Array>& right, benchmark::State* state) {
    using CType = typename Type::c_type;
    state->SetBytesProcessed(state->iterations() *
                             (cond->length() / 8 + 2 * cond->length() * sizeof(CType)));
  }
};

template <typename Type>
struct SetBytesProcessed<Type, enable_if_base_binary<Type>> {
  static void Set(const std::shared_ptr<Array>& cond, const std::shared_ptr<Array>& left,
                  const std::shared_ptr<Array>& right, benchmark::State* state) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    using OffsetType = typename TypeTraits<Type>::OffsetType::c_type;

    state->SetBytesProcessed(
        state->iterations() *
        (cond->length() / 8 + 2 * cond->length() * sizeof(OffsetType) +
         std::static_pointer_cast<ArrayType>(left)->total_values_length() +
         std::static_pointer_cast<ArrayType>(right)->total_values_length()));
  }
};

template <typename Type>
static void IfElseBench(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  random::RandomArrayGenerator rand(/*seed=*/0);

  auto cond = std::static_pointer_cast<BooleanArray>(
                  rand.ArrayOf(boolean(), len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto left = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto right = std::static_pointer_cast<ArrayType>(
                   rand.ArrayOf(type, len, /*null_probability=*/0.01))
                   ->Slice(offset);

  for (auto _ : state) {
    ABORT_NOT_OK(IfElse(cond, left, right));
  }

  SetBytesProcessed<Type>::Set(cond, left, right, &state);
}

template <typename Type>
static void IfElseBenchContiguous(benchmark::State& state) {
  auto type = TypeTraits<Type>::type_singleton();
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  int64_t len = state.range(0);
  int64_t offset = state.range(1);

  ASSERT_OK_AND_ASSIGN(auto temp1, MakeArrayFromScalar(BooleanScalar(true), len / 2));
  ASSERT_OK_AND_ASSIGN(auto temp2,
                       MakeArrayFromScalar(BooleanScalar(false), len - len / 2));
  ASSERT_OK_AND_ASSIGN(auto concat, Concatenate({temp1, temp2}));
  auto cond = std::static_pointer_cast<BooleanArray>(concat)->Slice(offset);

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto left = std::static_pointer_cast<ArrayType>(
                  rand.ArrayOf(type, len, /*null_probability=*/0.01))
                  ->Slice(offset);
  auto right = std::static_pointer_cast<ArrayType>(
                   rand.ArrayOf(type, len, /*null_probability=*/0.01))
                   ->Slice(offset);

  for (auto _ : state) {
    ABORT_NOT_OK(IfElse(cond, left, right));
  }

  SetBytesProcessed<Type>::Set(cond, left, right, &state);
}

static void IfElseBench64(benchmark::State& state) {
  return IfElseBench<UInt64Type>(state);
}

static void IfElseBench32(benchmark::State& state) {
  return IfElseBench<UInt32Type>(state);
}

static void IfElseBenchString32(benchmark::State& state) {
  return IfElseBench<StringType>(state);
}

static void IfElseBenchString64(benchmark::State& state) {
  return IfElseBench<LargeStringType>(state);
}

static void IfElseBench64Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt64Type>(state);
}

static void IfElseBench32Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt32Type>(state);
}

static void IfElseBenchString64Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt64Type>(state);
}

static void IfElseBenchString32Contiguous(benchmark::State& state) {
  return IfElseBenchContiguous<UInt32Type>(state);
}

BENCHMARK(IfElseBench32)->Args({elems, 0});
BENCHMARK(IfElseBench64)->Args({elems, 0});

BENCHMARK(IfElseBench32)->Args({elems, 99});
BENCHMARK(IfElseBench64)->Args({elems, 99});

BENCHMARK(IfElseBench32Contiguous)->Args({elems, 0});
BENCHMARK(IfElseBench64Contiguous)->Args({elems, 0});

BENCHMARK(IfElseBench32Contiguous)->Args({elems, 99});
BENCHMARK(IfElseBench64Contiguous)->Args({elems, 99});

BENCHMARK(IfElseBenchString32)->Args({elems, 0});
BENCHMARK(IfElseBenchString64)->Args({elems, 0});

BENCHMARK(IfElseBenchString32Contiguous)->Args({elems, 99});
BENCHMARK(IfElseBenchString64Contiguous)->Args({elems, 99});

}  // namespace compute
}  // namespace arrow
