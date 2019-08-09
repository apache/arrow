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
#include "arrow/colfmt/api.h"
#include "arrow/colfmt/test-util.h"

namespace arrow {
namespace colfmt {

constexpr uint32_t kSeed = 23999;

template<typename G>
static void BenchmarkShred(benchmark::State& state) {  // NOLINT non-const reference
  auto pool = default_memory_pool();
  Dataset ds = G(pool, kSeed)();

  while (state.KeepRunning()) {
    std::shared_ptr<Shredder> shredder = Shredder::Create(ds.schema(), pool).ValueOrDie();
    ABORT_NOT_OK(shredder->Shred(*ds.array()));
    ColumnMap res = shredder->Finish().ValueOrDie();
    benchmark::DoNotOptimize(res);
  }
  state.counters["NDocs"] = benchmark::Counter(ds.array()->length() * state.iterations(),
                                               benchmark::Counter::kIsRate);
}

template<typename G>
static void BenchmarkStitch(benchmark::State& state) {  // NOLINT non-const reference
  auto pool = default_memory_pool();
  Dataset ds = G(pool, kSeed)();

  std::shared_ptr<Shredder> shredder = Shredder::Create(ds.schema(), pool).ValueOrDie();
  ABORT_NOT_OK(shredder->Shred(*ds.array()));
  ColumnMap colmap = shredder->Finish().ValueOrDie();

  while (state.KeepRunning()) {
    std::shared_ptr<Stitcher> stitcher = Stitcher::Create(ds.schema(), pool).ValueOrDie();
    ABORT_NOT_OK(stitcher->Stitch(colmap));
    std::shared_ptr<arrow::Array> res = stitcher->Finish().ValueOrDie();
    benchmark::DoNotOptimize(res);
  }
  state.counters["NDocs"] = benchmark::Counter(ds.array()->length() * state.iterations(),
                                               benchmark::Counter::kIsRate);
}

class NestedStruct : public DatasetGenerator {
 public:
  using DatasetGenerator::DatasetGenerator;
  Dataset operator()() {
    return GenerateNestedStruct(5000,  /* num_docs */
                                5,     /* num_structs */
                                10,    /* num_fields */
                                0.25,  /* pr_struct_null */
                                0.25); /* pr_field_null */
  }
};
BENCHMARK_TEMPLATE(BenchmarkShred,  NestedStruct);
BENCHMARK_TEMPLATE(BenchmarkStitch, NestedStruct);

class ShallowList : public DatasetGenerator {
 public:
  using DatasetGenerator::DatasetGenerator;
  Dataset operator()() {
    return GenerateNestedList(5000, /* num_docs */
                              1,    /* num_levels */
                              100,  /* num_nodes */
                              0.2); /* pr_null */
  }
};
BENCHMARK_TEMPLATE(BenchmarkShred,  ShallowList);
BENCHMARK_TEMPLATE(BenchmarkStitch, ShallowList);

class DeepList : public DatasetGenerator {
 public:
  using DatasetGenerator::DatasetGenerator;
  Dataset operator()() {
    return GenerateNestedList(5000, /* num_docs */
                              5,    /* num_levels */
                              100,  /* num_nodes */
                              0.2); /* pr_null */
  }
};
BENCHMARK_TEMPLATE(BenchmarkShred,  DeepList);
BENCHMARK_TEMPLATE(BenchmarkStitch, DeepList);


}  // namespace colfmt
}  // namespace arrow
