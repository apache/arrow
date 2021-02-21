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

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/datum.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/variant.h"

namespace arrow {

using internal::checked_pointer_cast;

namespace util {

using TrivialVariant = arrow::util::Variant<int32_t, float>;

using NonTrivialVariant = arrow::util::Variant<int32_t, std::string>;

std::vector<int32_t> MakeInts(int64_t nitems) {
  auto rng = arrow::random::RandomArrayGenerator(42);
  auto array = checked_pointer_cast<Int32Array>(rng.Int32(nitems, 0, 1 << 30));
  std::vector<int32_t> items(nitems);
  for (int64_t i = 0; i < nitems; ++i) {
    items[i] = array->Value(i);
  }
  return items;
}

std::vector<float> MakeFloats(int64_t nitems) {
  auto rng = arrow::random::RandomArrayGenerator(42);
  auto array = checked_pointer_cast<FloatArray>(rng.Float32(nitems, 0.0, 1.0));
  std::vector<float> items(nitems);
  for (int64_t i = 0; i < nitems; ++i) {
    items[i] = array->Value(i);
  }
  return items;
}

std::vector<std::string> MakeStrings(int64_t nitems) {
  auto rng = arrow::random::RandomArrayGenerator(42);
  // Some std::string's will use short string optimization, but not all...
  auto array = checked_pointer_cast<StringArray>(rng.String(nitems, 5, 40));
  std::vector<std::string> items(nitems);
  for (int64_t i = 0; i < nitems; ++i) {
    items[i] = array->GetString(i);
  }
  return items;
}

static void ConstructTrivialVariant(benchmark::State& state) {
  const int64_t N = 10000;
  const auto ints = MakeInts(N);
  const auto floats = MakeFloats(N);

  for (auto _ : state) {
    for (int64_t i = 0; i < N; ++i) {
      // About type selection: we ensure 50% of each type, but try to avoid
      // branch mispredictions by creating runs of the same type.
      if (i & 0x10) {
        TrivialVariant v{ints[i]};
        const int32_t* val = &arrow::util::get<int32_t>(v);
        benchmark::DoNotOptimize(val);
      } else {
        TrivialVariant v{floats[i]};
        const float* val = &arrow::util::get<float>(v);
        benchmark::DoNotOptimize(val);
      }
    }
  }

  state.SetItemsProcessed(state.iterations() * N);
}

static void ConstructNonTrivialVariant(benchmark::State& state) {
  const int64_t N = 10000;
  const auto ints = MakeInts(N);
  const auto strings = MakeStrings(N);

  for (auto _ : state) {
    for (int64_t i = 0; i < N; ++i) {
      if (i & 0x10) {
        NonTrivialVariant v{ints[i]};
        const int32_t* val = &arrow::util::get<int32_t>(v);
        benchmark::DoNotOptimize(val);
      } else {
        NonTrivialVariant v{strings[i]};
        const std::string* val = &arrow::util::get<std::string>(v);
        benchmark::DoNotOptimize(val);
      }
    }
  }

  state.SetItemsProcessed(state.iterations() * N);
}

struct VariantVisitor {
  int64_t total = 0;

  void operator()(const int32_t& v) { total += v; }
  void operator()(const float& v) {
    // Avoid potentially costly float-to-int conversion
    int32_t x;
    memcpy(&x, &v, 4);
    total += x;
  }
  void operator()(const std::string& v) { total += static_cast<int64_t>(v.length()); }
};

template <typename VariantType>
static void VisitVariant(benchmark::State& state,
                         const std::vector<VariantType>& variants) {
  for (auto _ : state) {
    VariantVisitor visitor;
    for (const auto& v : variants) {
      visit(visitor, v);
    }
    benchmark::DoNotOptimize(visitor.total);
  }

  state.SetItemsProcessed(state.iterations() * variants.size());
}

static void VisitTrivialVariant(benchmark::State& state) {
  const int64_t N = 10000;
  const auto ints = MakeInts(N);
  const auto floats = MakeFloats(N);

  std::vector<TrivialVariant> variants;
  variants.reserve(N);
  for (int64_t i = 0; i < N; ++i) {
    if (i & 0x10) {
      variants.emplace_back(ints[i]);
    } else {
      variants.emplace_back(floats[i]);
    }
  }

  VisitVariant(state, variants);
}

static void VisitNonTrivialVariant(benchmark::State& state) {
  const int64_t N = 10000;
  const auto ints = MakeInts(N);
  const auto strings = MakeStrings(N);

  std::vector<NonTrivialVariant> variants;
  variants.reserve(N);
  for (int64_t i = 0; i < N; ++i) {
    if (i & 0x10) {
      variants.emplace_back(ints[i]);
    } else {
      variants.emplace_back(strings[i]);
    }
  }

  VisitVariant(state, variants);
}

static void ConstructDatum(benchmark::State& state) {
  const int64_t N = 10000;
  auto array = *MakeArrayOfNull(int8(), 100);
  auto chunked_array = std::make_shared<ChunkedArray>(ArrayVector{array, array});

  for (auto _ : state) {
    for (int64_t i = 0; i < N; ++i) {
      if (i & 0x10) {
        Datum datum{array};
        const ArrayData* val = datum.array().get();
        benchmark::DoNotOptimize(val);
      } else {
        Datum datum{chunked_array};
        const ChunkedArray* val = datum.chunked_array().get();
        benchmark::DoNotOptimize(val);
      }
    }
  }

  state.SetItemsProcessed(state.iterations() * N);
}

static void VisitDatum(benchmark::State& state) {
  const int64_t N = 10000;
  auto array = *MakeArrayOfNull(int8(), 100);
  auto chunked_array = std::make_shared<ChunkedArray>(ArrayVector{array, array});

  std::vector<Datum> datums;
  datums.reserve(N);
  for (int64_t i = 0; i < N; ++i) {
    if (i & 0x10) {
      datums.emplace_back(array);
    } else {
      datums.emplace_back(chunked_array);
    }
  }

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto& datum : datums) {
      // The .is_XXX() methods are the usual idiom when visiting a Datum,
      // rather than the visit() function.
      if (datum.is_array()) {
        total += datum.array()->length;
      } else {
        total += datum.chunked_array()->length();
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * datums.size());
}

BENCHMARK(ConstructTrivialVariant);
BENCHMARK(ConstructNonTrivialVariant);
BENCHMARK(VisitTrivialVariant);
BENCHMARK(VisitNonTrivialVariant);
BENCHMARK(ConstructDatum);
BENCHMARK(VisitDatum);

}  // namespace util
}  // namespace arrow
