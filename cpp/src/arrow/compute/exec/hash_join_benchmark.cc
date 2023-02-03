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

#include "arrow/api.h"
#include "arrow/compute/exec/hash_join.h"
#include "arrow/compute/exec/hash_join_node.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder_internal.h"
#include "arrow/testing/random.h"
#include "arrow/util/thread_pool.h"

#include <cstdint>
#include <cstdio>
#include <memory>

#include <omp.h>

namespace arrow {
namespace compute {
struct BenchmarkSettings {
  int num_threads = 1;
  JoinType join_type = JoinType::INNER;
  // Change to 'true' to benchmark alternative, non-default and less optimized version of
  // a hash join node implementation.
  bool use_basic_implementation = false;
  int batch_size = 1024;
  int num_build_batches = 32;
  int num_probe_batches = 32 * 16;
  std::vector<std::shared_ptr<DataType>> key_types = {int32()};
  std::vector<std::shared_ptr<DataType>> build_payload_types = {};
  std::vector<std::shared_ptr<DataType>> probe_payload_types = {};

  double null_percentage = 0.0;
  double cardinality = 1.0;  // Proportion of distinct keys in build side
  double selectivity = 1.0;  // Probability of a match for a given row
};

class JoinBenchmark {
 public:
  explicit JoinBenchmark(BenchmarkSettings& settings) {
    SchemaBuilder l_schema_builder, r_schema_builder;
    std::vector<FieldRef> left_keys, right_keys;
    std::vector<JoinKeyCmp> key_cmp;
    for (size_t i = 0; i < settings.key_types.size(); i++) {
      std::string l_name = "lk" + std::to_string(i);
      std::string r_name = "rk" + std::to_string(i);

      // For integers, selectivity is the proportion of the build interval that overlaps
      // with the probe interval
      uint64_t num_build_rows = settings.num_build_batches * settings.batch_size;

      uint64_t min_build_value = 0;
      uint64_t max_build_value =
          static_cast<uint64_t>(num_build_rows * settings.cardinality);

      uint64_t min_probe_value =
          static_cast<uint64_t>((1.0 - settings.selectivity) * max_build_value);
      uint64_t max_probe_value = min_probe_value + max_build_value;

      std::unordered_map<std::string, std::string> build_metadata;
      build_metadata["null_probability"] = std::to_string(settings.null_percentage);
      build_metadata["min"] = std::to_string(min_build_value);
      build_metadata["max"] = std::to_string(max_build_value);
      build_metadata["min_length"] = "2";
      build_metadata["max_length"] = "20";

      std::unordered_map<std::string, std::string> probe_metadata;
      probe_metadata["null_probability"] = std::to_string(settings.null_percentage);
      probe_metadata["min"] = std::to_string(min_probe_value);
      probe_metadata["max"] = std::to_string(max_probe_value);

      auto l_field =
          field(l_name, settings.key_types[i], key_value_metadata(probe_metadata));
      auto r_field =
          field(r_name, settings.key_types[i], key_value_metadata(build_metadata));

      DCHECK_OK(l_schema_builder.AddField(l_field));
      DCHECK_OK(r_schema_builder.AddField(r_field));

      left_keys.push_back(FieldRef(l_name));
      right_keys.push_back(FieldRef(r_name));
      key_cmp.push_back(JoinKeyCmp::EQ);
    }

    for (size_t i = 0; i < settings.build_payload_types.size(); i++) {
      std::string name = "lp" + std::to_string(i);
      DCHECK_OK(l_schema_builder.AddField(field(name, settings.probe_payload_types[i])));
    }

    for (size_t i = 0; i < settings.build_payload_types.size(); i++) {
      std::string name = "rp" + std::to_string(i);
      DCHECK_OK(r_schema_builder.AddField(field(name, settings.build_payload_types[i])));
    }

    auto l_schema = *l_schema_builder.Finish();
    auto r_schema = *r_schema_builder.Finish();

    BatchesWithSchema l_batches_with_schema =
        MakeRandomBatches(l_schema, settings.num_probe_batches, settings.batch_size);
    BatchesWithSchema r_batches_with_schema =
        MakeRandomBatches(r_schema, settings.num_build_batches, settings.batch_size);

    for (ExecBatch& batch : l_batches_with_schema.batches)
      l_batches_.InsertBatch(std::move(batch));
    for (ExecBatch& batch : r_batches_with_schema.batches)
      r_batches_.InsertBatch(std::move(batch));

    stats_.num_probe_rows = settings.num_probe_batches * settings.batch_size;

    schema_mgr_ = std::make_unique<HashJoinSchema>();
    Expression filter = literal(true);
    DCHECK_OK(schema_mgr_->Init(settings.join_type, *l_batches_with_schema.schema,
                                left_keys, *r_batches_with_schema.schema, right_keys,
                                filter, "l_", "r_"));

    if (settings.use_basic_implementation) {
      join_ = *HashJoinImpl::MakeBasic();
    } else {
      join_ = *HashJoinImpl::MakeSwiss();
    }

    omp_set_num_threads(settings.num_threads);
    auto schedule_callback = [](std::function<Status(size_t)> func) -> Status {
#pragma omp task
      { DCHECK_OK(func(omp_get_thread_num())); }
      return Status::OK();
    };

    scheduler_ = TaskScheduler::Make();
    DCHECK_OK(ctx_.Init(settings.num_threads, nullptr));

    auto register_task_group_callback = [&](std::function<Status(size_t, int64_t)> task,
                                            std::function<Status(size_t)> cont) {
      return scheduler_->RegisterTaskGroup(std::move(task), std::move(cont));
    };

    auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
      return scheduler_->StartTaskGroup(omp_get_thread_num(), task_group_id, num_tasks);
    };

    DCHECK_OK(join_->Init(
        &ctx_, settings.join_type, settings.num_threads, &(schema_mgr_->proj_maps[0]),
        &(schema_mgr_->proj_maps[1]), std::move(key_cmp), std::move(filter),
        std::move(register_task_group_callback), std::move(start_task_group_callback),
        [](int64_t, ExecBatch) { return Status::OK(); },
        [](int64_t) { return Status::OK(); }));

    task_group_probe_ = scheduler_->RegisterTaskGroup(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return join_->ProbeSingleBatch(thread_index, std::move(l_batches_[task_id]));
        },
        [this](size_t thread_index) -> Status {
          return join_->ProbingFinished(thread_index);
        });

    scheduler_->RegisterEnd();

    DCHECK_OK(scheduler_->StartScheduling(
        0 /*thread index*/, std::move(schedule_callback),
        static_cast<int>(2 * settings.num_threads) /*concurrent tasks*/,
        settings.num_threads == 1));
  }

  void RunJoin() {
#pragma omp parallel
    {
      int tid = omp_get_thread_num();
#pragma omp single
      DCHECK_OK(
          join_->BuildHashTable(tid, std::move(r_batches_), [this](size_t thread_index) {
            return scheduler_->StartTaskGroup(thread_index, task_group_probe_,
                                              l_batches_.batch_count());
          }));
    }
  }

  std::unique_ptr<TaskScheduler> scheduler_;
  AccumulationQueue l_batches_;
  AccumulationQueue r_batches_;
  std::unique_ptr<HashJoinSchema> schema_mgr_;
  std::unique_ptr<HashJoinImpl> join_;
  QueryContext ctx_;
  int task_group_probe_;

  struct {
    uint64_t num_probe_rows;
  } stats_;
};

static void HashJoinBasicBenchmarkImpl(benchmark::State& st,
                                       BenchmarkSettings& settings) {
  uint64_t total_rows = 0;
  for (auto _ : st) {
    st.PauseTiming();
    {
      JoinBenchmark bm(settings);
      st.ResumeTiming();
      bm.RunJoin();
      st.PauseTiming();
      total_rows += bm.stats_.num_probe_rows;
    }
    st.ResumeTiming();
  }
  st.counters["rows/sec"] = benchmark::Counter(total_rows, benchmark::Counter::kIsRate);
}

template <typename... Args>
static void BM_HashJoinBasic_KeyTypes(benchmark::State& st,
                                      std::vector<std::shared_ptr<DataType>> key_types,
                                      Args&&...) {
  BenchmarkSettings settings;
  settings.num_build_batches = static_cast<int>(st.range(0));
  settings.num_probe_batches = settings.num_build_batches;
  settings.key_types = std::move(key_types);

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_ProbeParallelism(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.num_threads = static_cast<int>(st.range(0));
  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_build_batches * 8;

  HashJoinBasicBenchmarkImpl(st, settings);
}

#ifdef ARROW_BUILD_DETAILED_BENCHMARKS  // Necessary to suppress warnings
template <typename... Args>
static void BM_HashJoinBasic_Selectivity(benchmark::State& st,
                                         std::vector<std::shared_ptr<DataType>> key_types,
                                         Args&&...) {
  BenchmarkSettings settings;
  settings.selectivity = static_cast<double>(st.range(0)) / 100.0;

  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_build_batches;
  settings.key_types = std::move(key_types);

  HashJoinBasicBenchmarkImpl(st, settings);
}

template <typename... Args>
static void BM_HashJoinBasic_JoinType(benchmark::State& st, JoinType join_type,
                                      Args&&...) {
  BenchmarkSettings settings;
  settings.selectivity = static_cast<double>(st.range(0)) / 100.0;

  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_build_batches;
  settings.join_type = join_type;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_MatchesPerRow(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.cardinality = 1.0 / static_cast<double>(st.range(0));

  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_probe_batches;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_PayloadSize(benchmark::State& st) {
  BenchmarkSettings settings;
  int32_t payload_size = static_cast<int32_t>(st.range(0));
  settings.probe_payload_types = {fixed_size_binary(payload_size)};
  settings.cardinality = 1.0 / static_cast<double>(st.range(1));

  settings.num_build_batches = static_cast<int>(st.range(2));
  settings.num_probe_batches = settings.num_probe_batches;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_BuildParallelism(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.num_threads = static_cast<int>(st.range(0));
  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_threads;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_NullPercentage(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.null_percentage = static_cast<double>(st.range(0)) / 100.0;

  HashJoinBasicBenchmarkImpl(st, settings);
}
#endif

std::vector<int64_t> hashtable_krows = benchmark::CreateRange(1, 4096, 8);

std::vector<std::string> keytypes_argnames = {"Hashtable krows"};
#ifdef ARROW_BUILD_DETAILED_BENCHMARKS
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int32}", {int32()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64}", {int64()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64}", {int64(), int64()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64,int64}",
                  {int64(), int64(), int64()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64,int64,int64}",
                  {int64(), int64(), int64(), int64()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{utf8}", {utf8()})
    ->ArgNames(keytypes_argnames)
    ->RangeMultiplier(4)
    ->Range(1, 64);
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(4)}",
                  {fixed_size_binary(4)})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(8)}",
                  {fixed_size_binary(8)})
    ->ArgNames(keytypes_argnames)
    ->RangeMultiplier(4)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(16)}",
                  {fixed_size_binary(16)})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(24)}",
                  {fixed_size_binary(24)})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});
BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(32)}",
                  {fixed_size_binary(32)})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});

std::vector<std::string> selectivity_argnames = {"Selectivity", "HashTable krows"};
std::vector<std::vector<int64_t>> selectivity_args = {
    benchmark::CreateDenseRange(0, 100, 20), hashtable_krows};

BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{int32}", {int32()})
    ->ArgNames(selectivity_argnames)
    ->ArgsProduct(selectivity_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{int64}", {int64()})
    ->ArgNames(selectivity_argnames)
    ->ArgsProduct(selectivity_args);

// Joins on UTF8 are currently really slow, so anything above 64 doesn't finished within
// a reasonable amount of time.
BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{utf8}", {utf8()})
    ->ArgNames(selectivity_argnames)
    ->ArgsProduct({benchmark::CreateDenseRange(0, 100, 20),
                   benchmark::CreateRange(1, 64, 8)});

BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{fixed_size_binary(16)}",
                  {fixed_size_binary(16)})
    ->ArgNames(selectivity_argnames)
    ->ArgsProduct(selectivity_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{fixed_size_binary(32)}",
                  {fixed_size_binary(32)})
    ->ArgNames(selectivity_argnames)
    ->ArgsProduct(selectivity_args);

std::vector<std::string> jointype_argnames = {"Selectivity", "HashTable krows"};
std::vector<std::vector<int64_t>> jointype_args = {
    benchmark::CreateDenseRange(0, 100, 20), hashtable_krows};

BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Inner", JoinType::INNER)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Semi", JoinType::LEFT_SEMI)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Semi", JoinType::RIGHT_SEMI)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Anti", JoinType::LEFT_ANTI)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Anti", JoinType::RIGHT_ANTI)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Outer", JoinType::LEFT_OUTER)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Outer", JoinType::RIGHT_OUTER)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);
BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Full Outer", JoinType::FULL_OUTER)
    ->ArgNames(jointype_argnames)
    ->ArgsProduct(jointype_args);

BENCHMARK(BM_HashJoinBasic_MatchesPerRow)
    ->ArgNames({"Matches Per Row", "HashTable krows"})
    ->ArgsProduct({benchmark::CreateRange(1, 16, 2), hashtable_krows});

BENCHMARK(BM_HashJoinBasic_PayloadSize)
    ->ArgNames({"Payload Size", "Matches Per Row", "HashTable krows"})
    ->ArgsProduct({benchmark::CreateRange(1, 128, 4), benchmark::CreateRange(1, 16, 2),
                   hashtable_krows});

BENCHMARK(BM_HashJoinBasic_ProbeParallelism)
    ->ArgNames({"Threads", "HashTable krows"})
    ->ArgsProduct({benchmark::CreateDenseRange(1, 16, 1), hashtable_krows})
    ->MeasureProcessCPUTime();

BENCHMARK(BM_HashJoinBasic_BuildParallelism)
    ->ArgNames({"Threads", "HashTable krows"})
    ->ArgsProduct({benchmark::CreateDenseRange(1, 16, 1), hashtable_krows})
    ->MeasureProcessCPUTime();

BENCHMARK(BM_HashJoinBasic_NullPercentage)
    ->ArgNames({"Null Percentage"})
    ->DenseRange(0, 100, 10);
#else

BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int32}", {int32()})
    ->ArgNames(keytypes_argnames)
    ->ArgsProduct({hashtable_krows});

BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{utf8}", {utf8()})
    ->ArgNames(keytypes_argnames)
    ->RangeMultiplier(4)
    ->Range(1, 64);

BENCHMARK(BM_HashJoinBasic_ProbeParallelism)
    ->ArgNames({"Threads", "HashTable krows"})
    ->ArgsProduct({benchmark::CreateDenseRange(1, 16, 1), hashtable_krows})
    ->MeasureProcessCPUTime();

#endif  // ARROW_BUILD_DETAILED_BENCHMARKS

}  // namespace compute
}  // namespace arrow
