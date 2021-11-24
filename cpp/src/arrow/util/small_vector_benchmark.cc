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
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/testing/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/small_vector.h"

namespace arrow {
namespace internal {

template <typename T>
T ValueInitializer();
template <typename T>
T ValueInitializer(int seed);

template <>
int ValueInitializer<int>() {
  return 42;
}
template <>
int ValueInitializer<int>(int seed) {
  return 42;
}

template <>
std::string ValueInitializer<std::string>() {
  return "42";
}
template <>
std::string ValueInitializer<std::string>(int seed) {
  return std::string("x", seed & 0x3f);  // avoid making string too long
}

template <>
std::shared_ptr<int> ValueInitializer<std::shared_ptr<int>>() {
  return std::make_shared<int>(42);
}
template <>
std::shared_ptr<int> ValueInitializer<std::shared_ptr<int>>(int seed) {
  return std::make_shared<int>(seed);
}

template <typename Vector>
ARROW_NOINLINE int64_t ConsumeVector(Vector v) {
  return reinterpret_cast<intptr_t>(v.data());
}

template <typename Vector>
ARROW_NOINLINE int64_t IngestVector(const Vector& v) {
  return reinterpret_cast<intptr_t>(v.data());
}

// With ARROW_NOINLINE, try to make sure the number of items is not constant-propagated
template <typename Vector>
ARROW_NOINLINE void BenchmarkMoveVector(benchmark::State& state, Vector vec) {
  constexpr int kNumIters = 1000;

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      Vector tmp(std::move(vec));
      dummy += IngestVector(tmp);
      vec = std::move(tmp);
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters * 2);
}

template <typename Vector>
void MoveEmptyVector(benchmark::State& state) {
  BenchmarkMoveVector(state, Vector{});
}

template <typename Vector>
void MoveShortVector(benchmark::State& state) {
  using T = typename Vector::value_type;
  constexpr int kSize = 3;
  const auto initializer = ValueInitializer<T>();

  BenchmarkMoveVector(state, Vector(kSize, initializer));
}

template <typename Vector>
void CopyEmptyVector(benchmark::State& state) {
  constexpr int kNumIters = 1000;

  const Vector vec{};

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      dummy += ConsumeVector(vec);
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters);
}

template <typename Vector>
void CopyShortVector(benchmark::State& state) {
  constexpr int kSize = 3;
  constexpr int kNumIters = 1000;

  const Vector vec(kSize);

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      dummy += ConsumeVector(vec);
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters);
}

// With ARROW_NOINLINE, try to make sure the number of items is not constant-propagated
template <typename Vector>
ARROW_NOINLINE void BenchmarkConstructFromStdVector(benchmark::State& state,
                                                    const int nitems) {
  using T = typename Vector::value_type;
  constexpr int kNumIters = 1000;
  const std::vector<T> src(nitems, ValueInitializer<T>());

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      Vector vec(src);
      dummy += IngestVector(vec);
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters);
}

template <typename Vector>
void ConstructFromEmptyStdVector(benchmark::State& state) {
  BenchmarkConstructFromStdVector<Vector>(state, 0);
}

template <typename Vector>
void ConstructFromShortStdVector(benchmark::State& state) {
  BenchmarkConstructFromStdVector<Vector>(state, 3);
}

// With ARROW_NOINLINE, try to make sure the number of items is not constant-propagated
template <typename Vector>
ARROW_NOINLINE void BenchmarkVectorPushBack(benchmark::State& state, const int nitems) {
  using T = typename Vector::value_type;
  constexpr int kNumIters = 1000;

  ARROW_CHECK_LE(static_cast<size_t>(nitems), Vector{}.max_size());

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      Vector vec;
      vec.reserve(nitems);
      for (int j = 0; j < nitems; ++j) {
        vec.push_back(ValueInitializer<T>(j));
      }
      dummy += reinterpret_cast<intptr_t>(vec.data());
      benchmark::ClobberMemory();
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters * nitems);
}

template <typename Vector>
void ShortVectorPushBack(benchmark::State& state) {
  BenchmarkVectorPushBack<Vector>(state, 3);
}

template <typename Vector>
void LongVectorPushBack(benchmark::State& state) {
  BenchmarkVectorPushBack<Vector>(state, 100);
}

// With ARROW_NOINLINE, try to make sure the source data is not constant-propagated
// (we could also use random data)
template <typename Vector, typename T = typename Vector::value_type>
ARROW_NOINLINE void BenchmarkShortVectorInsert(benchmark::State& state,
                                               const std::vector<T>& src) {
  constexpr int kNumIters = 1000;

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      Vector vec;
      vec.reserve(4);
      vec.insert(vec.begin(), src.begin(), src.begin() + 2);
      vec.insert(vec.begin(), src.begin() + 2, src.end());
      dummy += reinterpret_cast<intptr_t>(vec.data());
      benchmark::ClobberMemory();
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters * 4);
}

template <typename Vector>
void ShortVectorInsert(benchmark::State& state) {
  using T = typename Vector::value_type;
  const std::vector<T> src(4, ValueInitializer<T>());
  BenchmarkShortVectorInsert<Vector>(state, src);
}

template <typename Vector>
ARROW_NOINLINE void BenchmarkVectorInsertAtEnd(benchmark::State& state,
                                               const int nitems) {
  using T = typename Vector::value_type;
  constexpr int kNumIters = 1000;

  ARROW_CHECK_LE(static_cast<size_t>(nitems), Vector{}.max_size());
  ARROW_CHECK_EQ(nitems % 2, 0);

  std::vector<T> src;
  for (int j = 0; j < nitems / 2; ++j) {
    src.push_back(ValueInitializer<T>(j));
  }

  for (auto _ : state) {
    int64_t dummy = 0;
    for (int i = 0; i < kNumIters; ++i) {
      Vector vec;
      vec.reserve(nitems);
      vec.insert(vec.end(), src.begin(), src.end());
      vec.insert(vec.end(), src.begin(), src.end());
      dummy += reinterpret_cast<intptr_t>(vec.data());
      benchmark::ClobberMemory();
    }
    benchmark::DoNotOptimize(dummy);
  }

  state.SetItemsProcessed(state.iterations() * kNumIters * nitems);
}

template <typename Vector>
void ShortVectorInsertAtEnd(benchmark::State& state) {
  BenchmarkVectorInsertAtEnd<Vector>(state, 4);
}

template <typename Vector>
void LongVectorInsertAtEnd(benchmark::State& state) {
  BenchmarkVectorInsertAtEnd<Vector>(state, 100);
}

#define SHORT_VECTOR_BENCHMARKS(VEC_TYPE_FACTORY)                                  \
  BENCHMARK_TEMPLATE(MoveEmptyVector, VEC_TYPE_FACTORY(int));                      \
  BENCHMARK_TEMPLATE(MoveEmptyVector, VEC_TYPE_FACTORY(std::string));              \
  BENCHMARK_TEMPLATE(MoveEmptyVector, VEC_TYPE_FACTORY(std::shared_ptr<int>));     \
  BENCHMARK_TEMPLATE(MoveShortVector, VEC_TYPE_FACTORY(int));                      \
  BENCHMARK_TEMPLATE(MoveShortVector, VEC_TYPE_FACTORY(std::string));              \
  BENCHMARK_TEMPLATE(MoveShortVector, VEC_TYPE_FACTORY(std::shared_ptr<int>));     \
  BENCHMARK_TEMPLATE(CopyEmptyVector, VEC_TYPE_FACTORY(int));                      \
  BENCHMARK_TEMPLATE(CopyEmptyVector, VEC_TYPE_FACTORY(std::string));              \
  BENCHMARK_TEMPLATE(CopyEmptyVector, VEC_TYPE_FACTORY(std::shared_ptr<int>));     \
  BENCHMARK_TEMPLATE(CopyShortVector, VEC_TYPE_FACTORY(int));                      \
  BENCHMARK_TEMPLATE(CopyShortVector, VEC_TYPE_FACTORY(std::string));              \
  BENCHMARK_TEMPLATE(CopyShortVector, VEC_TYPE_FACTORY(std::shared_ptr<int>));     \
  BENCHMARK_TEMPLATE(ConstructFromEmptyStdVector, VEC_TYPE_FACTORY(int));          \
  BENCHMARK_TEMPLATE(ConstructFromEmptyStdVector, VEC_TYPE_FACTORY(std::string));  \
  BENCHMARK_TEMPLATE(ConstructFromEmptyStdVector,                                  \
                     VEC_TYPE_FACTORY(std::shared_ptr<int>));                      \
  BENCHMARK_TEMPLATE(ConstructFromShortStdVector, VEC_TYPE_FACTORY(int));          \
  BENCHMARK_TEMPLATE(ConstructFromShortStdVector, VEC_TYPE_FACTORY(std::string));  \
  BENCHMARK_TEMPLATE(ConstructFromShortStdVector,                                  \
                     VEC_TYPE_FACTORY(std::shared_ptr<int>));                      \
  BENCHMARK_TEMPLATE(ShortVectorPushBack, VEC_TYPE_FACTORY(int));                  \
  BENCHMARK_TEMPLATE(ShortVectorPushBack, VEC_TYPE_FACTORY(std::string));          \
  BENCHMARK_TEMPLATE(ShortVectorPushBack, VEC_TYPE_FACTORY(std::shared_ptr<int>)); \
  BENCHMARK_TEMPLATE(ShortVectorInsert, VEC_TYPE_FACTORY(int));                    \
  BENCHMARK_TEMPLATE(ShortVectorInsert, VEC_TYPE_FACTORY(std::string));            \
  BENCHMARK_TEMPLATE(ShortVectorInsert, VEC_TYPE_FACTORY(std::shared_ptr<int>));   \
  BENCHMARK_TEMPLATE(ShortVectorInsertAtEnd, VEC_TYPE_FACTORY(int));               \
  BENCHMARK_TEMPLATE(ShortVectorInsertAtEnd, VEC_TYPE_FACTORY(std::string));       \
  BENCHMARK_TEMPLATE(ShortVectorInsertAtEnd, VEC_TYPE_FACTORY(std::shared_ptr<int>));

#define LONG_VECTOR_BENCHMARKS(VEC_TYPE_FACTORY)                                  \
  BENCHMARK_TEMPLATE(LongVectorPushBack, VEC_TYPE_FACTORY(int));                  \
  BENCHMARK_TEMPLATE(LongVectorPushBack, VEC_TYPE_FACTORY(std::string));          \
  BENCHMARK_TEMPLATE(LongVectorPushBack, VEC_TYPE_FACTORY(std::shared_ptr<int>)); \
  BENCHMARK_TEMPLATE(LongVectorInsertAtEnd, VEC_TYPE_FACTORY(int));               \
  BENCHMARK_TEMPLATE(LongVectorInsertAtEnd, VEC_TYPE_FACTORY(std::string));       \
  BENCHMARK_TEMPLATE(LongVectorInsertAtEnd, VEC_TYPE_FACTORY(std::shared_ptr<int>));

// NOTE: the macro name below (STD_VECTOR etc.) is reflected in the
// benchmark name, so use descriptive names.

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

#define STD_VECTOR(T) std::vector<T>
SHORT_VECTOR_BENCHMARKS(STD_VECTOR);
LONG_VECTOR_BENCHMARKS(STD_VECTOR);
#undef STD_VECTOR

#endif

#define STATIC_VECTOR(T) StaticVector<T, 4>
SHORT_VECTOR_BENCHMARKS(STATIC_VECTOR);
#undef STATIC_VECTOR

#define SMALL_VECTOR(T) SmallVector<T, 4>
SHORT_VECTOR_BENCHMARKS(SMALL_VECTOR);
LONG_VECTOR_BENCHMARKS(SMALL_VECTOR);
#undef SMALL_VECTOR

#undef SHORT_VECTOR_BENCHMARKS
#undef LONG_VECTOR_BENCHMARKS

}  // namespace internal
}  // namespace arrow
