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
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/testing/random.h"
#include "arrow/util/cache_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

static constexpr int32_t kCacheSize = 100;
static constexpr int32_t kSmallKeyLength = 8;
static constexpr int32_t kLargeKeyLength = 64;
static constexpr int32_t kSmallValueLength = 16;
static constexpr int32_t kLargeValueLength = 1024;

static std::vector<std::string> MakeStrings(int64_t nvalues, int64_t min_length,
                                            int64_t max_length) {
  auto rng = ::arrow::random::RandomArrayGenerator(42);
  auto arr = checked_pointer_cast<StringArray>(rng.String(
      nvalues, static_cast<int32_t>(min_length), static_cast<int32_t>(max_length)));
  std::vector<std::string> vec(nvalues);
  for (int64_t i = 0; i < nvalues; ++i) {
    vec[i] = arr->GetString(i);
  }
  return vec;
}

static std::vector<std::string> MakeStrings(int64_t nvalues, int64_t length) {
  return MakeStrings(nvalues, length, length);
}

template <typename Cache, typename Key, typename Value>
static void BenchmarkCacheLookups(benchmark::State& state, const std::vector<Key>& keys,
                                  const std::vector<Value>& values) {
  const int32_t nitems = static_cast<int32_t>(keys.size());
  Cache cache(nitems);
  for (int32_t i = 0; i < nitems; ++i) {
    cache.Replace(keys[i], values[i]);
  }

  for (auto _ : state) {
    int64_t nfinds = 0;
    for (const auto& key : keys) {
      nfinds += (cache.Find(key) != nullptr);
    }
    benchmark::DoNotOptimize(nfinds);
    ARROW_CHECK_EQ(nfinds, nitems);
  }
  state.SetItemsProcessed(state.iterations() * nitems);
}

static void LruCacheLookup(benchmark::State& state) {
  const auto keys = MakeStrings(kCacheSize, state.range(0));
  const auto values = MakeStrings(kCacheSize, state.range(1));
  BenchmarkCacheLookups<LruCache<std::string, std::string>>(state, keys, values);
}

static void SetCacheArgs(benchmark::internal::Benchmark* bench) {
  bench->Args({kSmallKeyLength, kSmallValueLength});
  bench->Args({kSmallKeyLength, kLargeValueLength});
  bench->Args({kLargeKeyLength, kSmallValueLength});
  bench->Args({kLargeKeyLength, kLargeValueLength});
}

BENCHMARK(LruCacheLookup)->Apply(SetCacheArgs);

struct Callable {
  explicit Callable(std::vector<std::string> values)
      : index_(0), values_(std::move(values)) {}

  std::string operator()(const std::string& key) {
    // Return a value unrelated to the key
    if (++index_ >= static_cast<int64_t>(values_.size())) {
      index_ = 0;
    }
    return values_[index_];
  }

 private:
  int64_t index_;
  std::vector<std::string> values_;
};

template <typename Memoized>
static void BenchmarkMemoize(benchmark::State& state, Memoized&& mem,
                             const std::vector<std::string>& keys) {
  // Prime memoization cache
  for (const auto& key : keys) {
    mem(key);
  }

  for (auto _ : state) {
    int64_t nbytes = 0;
    for (const auto& key : keys) {
      nbytes += static_cast<int64_t>(mem(key).length());
    }
    benchmark::DoNotOptimize(nbytes);
  }
  state.SetItemsProcessed(state.iterations() * keys.size());
}

static void MemoizeLruCached(benchmark::State& state) {
  const auto keys = MakeStrings(kCacheSize, state.range(0));
  const auto values = MakeStrings(kCacheSize, state.range(1));
  auto mem = MemoizeLru(Callable(values), kCacheSize);
  BenchmarkMemoize(state, mem, keys);
}

static void MemoizeLruCachedThreadUnsafe(benchmark::State& state) {
  const auto keys = MakeStrings(kCacheSize, state.range(0));
  const auto values = MakeStrings(kCacheSize, state.range(1));
  // Emulate recommended usage of MemoizeLruCachedThreadUnsafe
  // (the compiler is probably able to cache the TLS-looked up value, though)
  thread_local auto mem = MemoizeLruThreadUnsafe(Callable(values), kCacheSize);
  BenchmarkMemoize(state, mem, keys);
}

BENCHMARK(MemoizeLruCached)->Apply(SetCacheArgs);
BENCHMARK(MemoizeLruCachedThreadUnsafe)->Apply(SetCacheArgs);

}  // namespace internal
}  // namespace arrow
