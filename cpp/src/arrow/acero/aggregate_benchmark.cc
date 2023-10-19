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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/exec.h"
#include "benchmark/benchmark.h"

#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/array/array_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/string.h"

namespace arrow::acero {

#include <cassert>
#include <cmath>
#include <iostream>
#include <random>

using arrow::internal::ToChars;

//
// GroupBy
//

std::shared_ptr<RecordBatch> RecordBatchFromArrays(
    const std::vector<std::shared_ptr<Array>>& arguments,
    const std::vector<std::shared_ptr<Array>>& keys) {
  std::vector<std::shared_ptr<Field>> fields;
  std::vector<std::shared_ptr<Array>> all_arrays;
  int64_t length = -1;
  if (arguments.empty()) {
    DCHECK(!keys.empty());
    length = keys[0]->length();
  } else {
    length = arguments[0]->length();
  }
  for (std::size_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
    const auto& arg = arguments[arg_idx];
    DCHECK_EQ(arg->length(), length);
    fields.push_back(field("arg" + ToChars(arg_idx), arg->type()));
    all_arrays.push_back(arg);
  }
  for (std::size_t key_idx = 0; key_idx < keys.size(); key_idx++) {
    const auto& key = keys[key_idx];
    DCHECK_EQ(key->length(), length);
    fields.push_back(field("key" + ToChars(key_idx), key->type()));
    all_arrays.push_back(key);
  }
  return RecordBatch::Make(schema(std::move(fields)), length, std::move(all_arrays));
}

Result<std::shared_ptr<Table>> BatchGroupBy(
    std::shared_ptr<RecordBatch> batch, std::vector<Aggregate> aggregates,
    std::vector<FieldRef> keys, bool use_threads = false,
    MemoryPool* memory_pool = default_memory_pool()) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                        Table::FromRecordBatches({std::move(batch)}));
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(table))},
       {"aggregate", AggregateNodeOptions(std::move(aggregates), std::move(keys))}});
  return DeclarationToTable(std::move(plan), use_threads, memory_pool);
}

static void BenchmarkGroupBy(benchmark::State& state, std::vector<Aggregate> aggregates,
                             const std::vector<std::shared_ptr<Array>>& arguments,
                             const std::vector<std::shared_ptr<Array>>& keys) {
  std::shared_ptr<RecordBatch> batch = RecordBatchFromArrays(arguments, keys);
  std::vector<FieldRef> key_refs;
  for (std::size_t key_idx = 0; key_idx < keys.size(); key_idx++) {
    key_refs.emplace_back(static_cast<int>(key_idx + arguments.size()));
  }
  for (std::size_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
    aggregates[arg_idx].target = {FieldRef(static_cast<int>(arg_idx))};
  }
  for (auto _ : state) {
    ABORT_NOT_OK(BatchGroupBy(batch, aggregates, key_refs));
  }
}

#define GROUP_BY_BENCHMARK(Name, Impl)                               \
  static void Name(benchmark::State& state) {                        \
    RegressionArgs args(state, false);                               \
    auto rng = random::RandomArrayGenerator(1923);                   \
    (Impl)();                                                        \
  }                                                                  \
  BENCHMARK(Name)->Apply([](benchmark::internal::Benchmark* bench) { \
    BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});             \
  })

// Grouped Sum

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/16,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/256,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/4096,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/15);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/255);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/4095);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/4);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/4,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/15);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/16,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/63);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/64,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

// Grouped MinMax

GROUP_BY_BENCHMARK(MinMaxDoublesGroupedByMediumInt, [&] {
  auto input = rng.Float64(args.size,
                           /*min=*/0.0,
                           /*max=*/1.0e14,
                           /*null_probability=*/args.null_proportion,
                           /*nan_probability=*/args.null_proportion / 10);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

GROUP_BY_BENCHMARK(MinMaxShortStringsGroupedByMediumInt, [&] {
  auto input = rng.String(args.size,
                          /*min_length=*/0,
                          /*max_length=*/64,
                          /*null_probability=*/args.null_proportion);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

GROUP_BY_BENCHMARK(MinMaxLongStringsGroupedByMediumInt, [&] {
  auto input = rng.String(args.size,
                          /*min_length=*/0,
                          /*max_length=*/512,
                          /*null_probability=*/args.null_proportion);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

}  // namespace arrow::acero
