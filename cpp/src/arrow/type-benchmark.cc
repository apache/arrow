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

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

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

  schema1 = schema1->AddMetadata(md1);
  schema2 = schema1->AddMetadata(md2);
  schema3 = schema1->AddMetadata(md3);

  int64_t total = 0;
  for (auto _ : state) {
    total += schema1->Equals(*schema2);
    total += schema1->Equals(*schema3);
  }
  benchmark::DoNotOptimize(total);
  state.SetItemsProcessed(state.iterations() * 2);
}

BENCHMARK(TypeEqualsSimple);
BENCHMARK(TypeEqualsComplex);
BENCHMARK(TypeEqualsWithMetadata);
BENCHMARK(SchemaEquals);
BENCHMARK(SchemaEqualsWithMetadata);

}  // namespace arrow
