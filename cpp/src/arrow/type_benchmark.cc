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
#include <exception>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/macros.h"

namespace arrow {

static void TypeEqualsSimple(benchmark::State& state) {  // NOLINT non-const reference
  auto a = uint8();
  auto b = uint8();
  auto c = float64();

  int64_t total = 0;
  for (auto _ : state) {
    total += a->Equals(*b);
    total += a->Equals(*c);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

static void TypeEqualsComplex(benchmark::State& state) {  // NOLINT non-const reference
  auto fa1 = field("as", list(float16()));
  auto fa2 = field("as", list(float16()));
  auto fb1 = field("bs", utf8());
  auto fb2 = field("bs", utf8());
  auto fc1 = field("cs", list(fixed_size_binary(10)));
  auto fc2 = field("cs", list(fixed_size_binary(10)));
  auto fc3 = field("cs", list(fixed_size_binary(11)));

  auto a = struct_({fa1, fb1, fc1});
  auto b = struct_({fa2, fb2, fc2});
  auto c = struct_({fa2, fb2, fc3});

  int64_t total = 0;
  for (auto _ : state) {
    total += a->Equals(*b);
    total += a->Equals(*c);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

static void TypeEqualsWithMetadata(
    benchmark::State& state) {  // NOLINT non-const reference
  auto md1 = key_value_metadata({"k1", "k2"}, {"some value1", "some value2"});
  auto md2 = key_value_metadata({"k1", "k2"}, {"some value1", "some value2"});
  auto md3 = key_value_metadata({"k2", "k1"}, {"some value2", "some value1"});

  auto fa1 = field("as", list(float16()));
  auto fa2 = field("as", list(float16()));
  auto fb1 = field("bs", utf8(), /*nullable=*/true, md1);
  auto fb2 = field("bs", utf8(), /*nullable=*/true, md2);
  auto fb3 = field("bs", utf8(), /*nullable=*/true, md3);

  auto a = struct_({fa1, fb1});
  auto b = struct_({fa2, fb2});
  auto c = struct_({fa2, fb3});

  int64_t total = 0;
  for (auto _ : state) {
    total += a->Equals(*b);
    total += a->Equals(*c);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

static std::vector<std::shared_ptr<Schema>> SampleSchemas() {
  auto fa1 = field("as", list(float16()));
  auto fa2 = field("as", list(float16()));
  auto fb1 = field("bs", utf8());
  auto fb2 = field("bs", utf8());
  auto fc1 = field("cs", list(fixed_size_binary(10)));
  auto fc2 = field("cs", list(fixed_size_binary(10)));
  auto fd1 = field("ds", decimal(19, 5));
  auto fd2 = field("ds", decimal(19, 5));
  auto fe1 = field("es", map(utf8(), int32()));
  auto fe2 = field("es", map(utf8(), int32()));
  auto ff1 = field("fs", dictionary(int8(), binary()));
  auto ff2 = field("fs", dictionary(int8(), binary()));
  auto fg1 = field(
      "gs", struct_({field("A", int8()), field("B", int16()), field("C", float32())}));
  auto fg2 = field(
      "gs", struct_({field("A", int8()), field("B", int16()), field("C", float32())}));
  auto fh1 = field("hs", large_binary());
  auto fh2 = field("hs", large_binary());

  auto fz1 = field("zs", duration(TimeUnit::MICRO));
  auto fz2 = field("zs", duration(TimeUnit::MICRO));
  auto fz3 = field("zs", duration(TimeUnit::NANO));

  auto schema1 = ::arrow::schema({fa1, fb1, fc1, fd1, fe1, ff1, fg1, fh1, fz1});
  auto schema2 = ::arrow::schema({fa2, fb2, fc2, fd2, fe2, ff2, fg2, fh2, fz2});
  auto schema3 = ::arrow::schema({fa2, fb2, fc2, fd2, fe2, ff2, fg2, fh2, fz3});

  return {schema1, schema2, schema3};
}

static void SchemaEquals(benchmark::State& state) {  // NOLINT non-const reference
  auto schemas = SampleSchemas();

  auto schema1 = schemas[0];
  auto schema2 = schemas[1];
  auto schema3 = schemas[2];

  int64_t total = 0;
  for (auto _ : state) {
    total += schema1->Equals(*schema2, /*check_metadata =*/false);
    total += schema1->Equals(*schema3, /*check_metadata =*/false);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

static void SchemaEqualsWithMetadata(
    benchmark::State& state) {  // NOLINT non-const reference
  auto schemas = SampleSchemas();

  auto schema1 = schemas[0];
  auto schema2 = schemas[1];
  auto schema3 = schemas[2];

  auto md1 = key_value_metadata({"k1", "k2"}, {"some value1", "some value2"});
  auto md2 = key_value_metadata({"k1", "k2"}, {"some value1", "some value2"});
  auto md3 = key_value_metadata({"k2", "k1"}, {"some value2", "some value1"});

  schema1 = schema1->WithMetadata(md1);
  schema2 = schema1->WithMetadata(md2);
  schema3 = schema1->WithMetadata(md3);

  int64_t total = 0;
  for (auto _ : state) {
    total += schema1->Equals(*schema2);
    total += schema1->Equals(*schema3);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

// ------------------------------------------------------------------------
// Micro-benchmark various error reporting schemes

#if (defined(__GNUC__) || defined(__APPLE__))
#define ARROW_NO_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define ARROW_NO_INLINE __declspec(noinline)
#else
#define ARROW_NO_INLINE
#warning Missing "noinline" attribute, no-inline benchmarks may be bogus
#endif

inline int64_t Accumulate(int64_t partial, int32_t value) {
  // Something non-trivial to avoid vectorization
  return partial + value + (partial >> 5) * value;
}

std::vector<int32_t> RandomIntegers() {
  std::default_random_engine gen(42);
  // Make 42 extremely unlikely (to make error Status allocation negligible)
  std::uniform_int_distribution<int32_t> dist(0, 100000);

  std::vector<int32_t> integers(6000);
  std::generate(integers.begin(), integers.end(), [&]() { return dist(gen); });
  return integers;
}

inline int32_t NoError(int32_t v) { return v + 1; }

ARROW_NO_INLINE int32_t NoErrorNoInline(int32_t v) { return v + 1; }

inline std::pair<bool, int32_t> ErrorAsBool(int32_t v) {
  return {ARROW_PREDICT_FALSE(v == 42), v + 1};
}

ARROW_NO_INLINE std::pair<bool, int32_t> ErrorAsBoolNoInline(int32_t v) {
  return {ARROW_PREDICT_FALSE(v == 42), v + 1};
}

inline Status ErrorAsStatus(int32_t v, int32_t* out) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    return Status::Invalid("42");
  }
  *out = v + 1;
  return Status::OK();
}

ARROW_NO_INLINE Status ErrorAsStatusNoInline(int32_t v, int32_t* out) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    return Status::Invalid("42");
  }
  *out = v + 1;
  return Status::OK();
}

inline Result<int32_t> ErrorAsResult(int32_t v) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    return Status::Invalid("42");
  }
  return v + 1;
}

ARROW_NO_INLINE Result<int32_t> ErrorAsResultNoInline(int32_t v) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    return Status::Invalid("42");
  }
  return v + 1;
}

inline int32_t ErrorAsException(int32_t v) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    throw std::invalid_argument("42");
  }
  return v + 1;
}

ARROW_NO_INLINE int32_t ErrorAsExceptionNoInline(int32_t v) {
  if (ARROW_PREDICT_FALSE(v == 42)) {
    throw std::invalid_argument("42");
  }
  return v + 1;
}

static void ErrorSchemeNoError(benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      total = Accumulate(total, NoError(v));
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeNoErrorNoInline(
    benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      total = Accumulate(total, NoErrorNoInline(v));
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeBool(benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      auto pair = ErrorAsBool(v);
      if (!ARROW_PREDICT_FALSE(pair.first)) {
        total = Accumulate(total, pair.second);
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeBoolNoInline(
    benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      auto pair = ErrorAsBoolNoInline(v);
      if (!ARROW_PREDICT_FALSE(pair.first)) {
        total = Accumulate(total, pair.second);
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeStatus(benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      int32_t value = 0;
      if (ARROW_PREDICT_TRUE(ErrorAsStatus(v, &value).ok())) {
        total = Accumulate(total, value);
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeStatusNoInline(
    benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      int32_t value;
      if (ARROW_PREDICT_TRUE(ErrorAsStatusNoInline(v, &value).ok())) {
        total = Accumulate(total, value);
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeResult(benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      auto maybe_value = ErrorAsResult(v);
      if (ARROW_PREDICT_TRUE(maybe_value.ok())) {
        total = Accumulate(total, *std::move(maybe_value));
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeResultNoInline(
    benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      auto maybe_value = ErrorAsResultNoInline(v);
      if (ARROW_PREDICT_TRUE(maybe_value.ok())) {
        total = Accumulate(total, *std::move(maybe_value));
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeException(benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      try {
        total = Accumulate(total, ErrorAsException(v));
      } catch (const std::exception&) {
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

static void ErrorSchemeExceptionNoInline(
    benchmark::State& state) {  // NOLINT non-const reference
  auto integers = RandomIntegers();

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto v : integers) {
      try {
        total = Accumulate(total, ErrorAsExceptionNoInline(v));
      } catch (const std::exception&) {
      }
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * integers.size());
}

BENCHMARK(TypeEqualsSimple);
BENCHMARK(TypeEqualsComplex);
BENCHMARK(TypeEqualsWithMetadata);
BENCHMARK(SchemaEquals);
BENCHMARK(SchemaEqualsWithMetadata);

BENCHMARK(ErrorSchemeNoError);
BENCHMARK(ErrorSchemeBool);
BENCHMARK(ErrorSchemeStatus);
BENCHMARK(ErrorSchemeResult);
BENCHMARK(ErrorSchemeException);

BENCHMARK(ErrorSchemeNoErrorNoInline);
BENCHMARK(ErrorSchemeBoolNoInline);
BENCHMARK(ErrorSchemeStatusNoInline);
BENCHMARK(ErrorSchemeResultNoInline);
BENCHMARK(ErrorSchemeExceptionNoInline);

}  // namespace arrow
