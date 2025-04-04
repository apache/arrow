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

#include "arrow/acero/hash_join.h"
#include "arrow/acero/hash_join_node.h"
#include "arrow/acero/options.h"
#include "arrow/acero/swiss_join_internal.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/api.h"
#include "arrow/compute/row/row_encoder_internal.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/thread_pool.h"

#include <cstdint>
#include <cstdio>
#include <memory>

namespace arrow {
namespace acero {
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
  int var_length_min = 2;    // Minimal length of any var length types
  int var_length_max = 20;   // Maximum length of any var length types

  Expression residual_filter = literal(true);

  bool stats_probe_rows = true;
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
      build_metadata["min_length"] = std::to_string(settings.var_length_min);
      build_metadata["max_length"] = std::to_string(settings.var_length_max);

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

    for (size_t i = 0; i < settings.probe_payload_types.size(); i++) {
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

    stats_.num_build_rows = settings.num_build_batches * settings.batch_size;
    stats_.num_probe_rows = settings.num_probe_batches * settings.batch_size;

    schema_mgr_ = std::make_unique<HashJoinSchema>();
    DCHECK_OK(schema_mgr_->Init(settings.join_type, *l_batches_with_schema.schema,
                                left_keys, *r_batches_with_schema.schema, right_keys,
                                settings.residual_filter, "l_", "r_"));

    if (settings.use_basic_implementation) {
      join_ = *HashJoinImpl::MakeBasic();
    } else {
      join_ = *HashJoinImpl::MakeSwiss();
    }

    scheduler_ = TaskScheduler::Make();
    thread_pool_ = arrow::internal::GetCpuThreadPool();
    DCHECK_OK(thread_pool_->SetCapacity(settings.num_threads));
    DCHECK_OK(ctx_.Init(nullptr));

    auto register_task_group_callback = [&](std::function<Status(size_t, int64_t)> task,
                                            std::function<Status(size_t)> cont) {
      return scheduler_->RegisterTaskGroup(std::move(task), std::move(cont));
    };

    auto start_task_group_callback = [&](int task_group_id, int64_t num_tasks) {
      return scheduler_->StartTaskGroup(/*thread_id=*/0, task_group_id, num_tasks);
    };

    DCHECK_OK(join_->Init(
        &ctx_, settings.join_type, settings.num_threads, &(schema_mgr_->proj_maps[0]),
        &(schema_mgr_->proj_maps[1]), std::move(key_cmp), settings.residual_filter,
        std::move(register_task_group_callback), std::move(start_task_group_callback),
        [](int64_t, ExecBatch) { return Status::OK(); },
        [&](int64_t) { return Status::OK(); }));

    task_group_probe_ = scheduler_->RegisterTaskGroup(
        [this](size_t thread_index, int64_t task_id) -> Status {
          return join_->ProbeSingleBatch(thread_index, std::move(l_batches_[task_id]));
        },
        [this](size_t thread_index) -> Status {
          return join_->ProbingFinished(thread_index);
        });

    scheduler_->RegisterEnd();

    DCHECK_OK(scheduler_->StartScheduling(
        /*thread_id=*/0,
        [&](std::function<Status(size_t)> task) -> Status {
          return thread_pool_->Spawn([&, task]() { DCHECK_OK(task(thread_indexer_())); });
        },
        thread_pool_->GetCapacity(), settings.num_threads == 1));
  }

  void RunJoin() {
    DCHECK_OK(join_->BuildHashTable(
        /*thread_id=*/0, std::move(r_batches_), [this](size_t thread_index) {
          return scheduler_->StartTaskGroup(thread_index, task_group_probe_,
                                            l_batches_.batch_count());
        }));

    thread_pool_->WaitForIdle();
  }

  std::unique_ptr<TaskScheduler> scheduler_;
  ThreadIndexer thread_indexer_;
  arrow::internal::ThreadPool* thread_pool_;

  AccumulationQueue l_batches_;
  AccumulationQueue r_batches_;
  std::unique_ptr<HashJoinSchema> schema_mgr_;
  std::unique_ptr<HashJoinImpl> join_;
  QueryContext ctx_;
  int task_group_probe_;

  struct {
    uint64_t num_build_rows;
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
      total_rows += (settings.stats_probe_rows ? bm.stats_.num_probe_rows
                                               : bm.stats_.num_build_rows);
    }
    st.ResumeTiming();
  }
  st.counters["rows/sec"] =
      benchmark::Counter(static_cast<double>(total_rows), benchmark::Counter::kIsRate);
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
  settings.num_probe_batches = settings.num_build_batches;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_PayloadSize(benchmark::State& st) {
  BenchmarkSettings settings;
  int32_t payload_size = static_cast<int32_t>(st.range(0));
  settings.probe_payload_types = {fixed_size_binary(payload_size)};
  settings.cardinality = 1.0 / static_cast<double>(st.range(1));

  settings.num_build_batches = static_cast<int>(st.range(2));
  settings.num_probe_batches = settings.num_build_batches;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_BuildParallelism(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.num_threads = static_cast<int>(st.range(0));
  settings.num_build_batches = static_cast<int>(st.range(1));
  settings.num_probe_batches = settings.num_threads;
  settings.stats_probe_rows = false;

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_NullPercentage(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.null_percentage = static_cast<double>(st.range(0)) / 100.0;

  HashJoinBasicBenchmarkImpl(st, settings);
}

template <typename... Args>
static void BM_HashJoinBasic_TrivialResidualFilter(benchmark::State& st,
                                                   JoinType join_type,
                                                   Expression residual_filter,
                                                   Args&&...) {
  BenchmarkSettings settings;
  settings.join_type = join_type;
  settings.build_payload_types = {binary()};
  settings.probe_payload_types = {binary()};

  settings.use_basic_implementation = st.range(0);

  settings.num_build_batches = 1024;
  settings.num_probe_batches = 1024;

  // Let payload column length from 1 to 100.
  settings.var_length_min = 1;
  settings.var_length_max = 100;

  settings.residual_filter = std::move(residual_filter);

  HashJoinBasicBenchmarkImpl(st, settings);
}

template <typename... Args>
static void BM_HashJoinBasic_ComplexResidualFilter(benchmark::State& st,
                                                   JoinType join_type, Args&&...) {
  BenchmarkSettings settings;
  settings.join_type = join_type;
  settings.build_payload_types = {binary()};
  settings.probe_payload_types = {binary()};

  settings.use_basic_implementation = st.range(0);

  settings.num_build_batches = 1024;
  settings.num_probe_batches = 1024;

  // Let payload column length from 1 to 100.
  settings.var_length_min = 1;
  settings.var_length_max = 100;

  // Create filter referring payload columns from both sides.
  // binary_length(probe_payload) + binary_length(build_payload) <= 2 * selectivity
  settings.selectivity = static_cast<double>(st.range(1)) / 100.0;
  using arrow::compute::call;
  using arrow::compute::field_ref;
  settings.residual_filter =
      call("less_equal", {call("plus", {call("binary_length", {field_ref("lp0")}),
                                        call("binary_length", {field_ref("rp0")})}),
                          literal(2 * settings.selectivity)});

  HashJoinBasicBenchmarkImpl(st, settings);
}

static void BM_HashJoinBasic_HeavyBuildPayload(benchmark::State& st) {
  BenchmarkSettings settings;
  settings.build_payload_types = {boolean(), fixed_size_binary(64), utf8(),
                                  boolean(), fixed_size_binary(64), utf8()};
  settings.probe_payload_types = {int32()};
  settings.null_percentage = 0.5;
  settings.cardinality = 1.0 / 16.0;
  settings.num_build_batches = static_cast<int>(st.range(0));
  settings.num_probe_batches = settings.num_build_batches;
  settings.var_length_min = 64;
  settings.var_length_max = 128;

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

const char* use_basic_argname = "Use basic";
std::vector<int64_t> use_basic_arg = benchmark::CreateDenseRange(0, 1, 1);

std::vector<std::string> trivial_residual_filter_argnames = {use_basic_argname};
std::vector<std::vector<int64_t>> trivial_residual_filter_args = {use_basic_arg};

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Inner/Literal(true)",
                  JoinType::INNER, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Semi/Literal(true)",
                  JoinType::LEFT_SEMI, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Semi/Literal(true)",
                  JoinType::RIGHT_SEMI, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Anti/Literal(true)",
                  JoinType::LEFT_ANTI, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Anti/Literal(true)",
                  JoinType::RIGHT_ANTI, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Outer/Literal(true)",
                  JoinType::LEFT_OUTER, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Outer/Literal(true)",
                  JoinType::RIGHT_OUTER, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Full Outer/Literal(true)",
                  JoinType::FULL_OUTER, literal(true))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Inner/Literal(false)",
                  JoinType::INNER, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Semi/Literal(false)",
                  JoinType::LEFT_SEMI, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Semi/Literal(false)",
                  JoinType::RIGHT_SEMI, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Anti/Literal(false)",
                  JoinType::LEFT_ANTI, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Anti/Literal(false)",
                  JoinType::RIGHT_ANTI, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Left Outer/Literal(false)",
                  JoinType::LEFT_OUTER, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Right Outer/Literal(false)",
                  JoinType::RIGHT_OUTER, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_TrivialResidualFilter, "Full Outer/Literal(false)",
                  JoinType::FULL_OUTER, literal(false))
    ->ArgNames(trivial_residual_filter_argnames)
    ->ArgsProduct(trivial_residual_filter_args);

std::vector<std::string> complex_residual_filter_argnames = {use_basic_argname,
                                                             "Selectivity"};
std::vector<std::vector<int64_t>> complex_residual_filter_args = {
    use_basic_arg, benchmark::CreateDenseRange(0, 100, 20)};

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Inner", JoinType::INNER)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Left Semi",
                  JoinType::LEFT_SEMI)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Right Semi",
                  JoinType::RIGHT_SEMI)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Left Anti",
                  JoinType::LEFT_ANTI)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Right Anti",
                  JoinType::RIGHT_ANTI)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Left Outer",
                  JoinType::LEFT_OUTER)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Right Outer",
                  JoinType::RIGHT_OUTER)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK_CAPTURE(BM_HashJoinBasic_ComplexResidualFilter, "Full Outer",
                  JoinType::FULL_OUTER)
    ->ArgNames(complex_residual_filter_argnames)
    ->ArgsProduct(complex_residual_filter_args);

BENCHMARK(BM_HashJoinBasic_HeavyBuildPayload)
    ->ArgNames({"HashTable krows"})
    ->ArgsProduct({benchmark::CreateRange(1, 512, 8)});
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

void RowArrayDecodeBenchmark(benchmark::State& st, const std::shared_ptr<Schema>& schema,
                             int column_to_decode) {
  auto batches = MakeRandomBatches(schema, 1, std::numeric_limits<uint16_t>::max());
  const auto& batch = batches.batches[0];
  RowArray rows;
  std::vector<uint16_t> row_ids_encode(batch.length);
  std::iota(row_ids_encode.begin(), row_ids_encode.end(), 0);
  std::vector<KeyColumnArray> temp_column_arrays;
  DCHECK_OK(rows.AppendBatchSelection(
      default_memory_pool(), internal::CpuInfo::GetInstance()->hardware_flags(), batch, 0,
      static_cast<int>(batch.length), static_cast<int>(batch.length),
      row_ids_encode.data(), temp_column_arrays));
  std::vector<uint32_t> row_ids_decode(batch.length);
  // Create a random access pattern to simulate hash join.
  std::default_random_engine gen(42);
  std::uniform_int_distribution<uint32_t> dist(0,
                                               static_cast<uint32_t>(batch.length - 1));
  std::transform(row_ids_decode.begin(), row_ids_decode.end(), row_ids_decode.begin(),
                 [&](uint32_t) { return dist(gen); });

  for (auto _ : st) {
    ResizableArrayData column;
    // Allocate at least 8 rows for the convenience of SIMD decoding.
    int log_num_rows_min = std::max(3, bit_util::Log2(batch.length));
    DCHECK_OK(column.Init(batch[column_to_decode].type(), default_memory_pool(),
                          log_num_rows_min));
    DCHECK_OK(rows.DecodeSelected(&column, column_to_decode,
                                  static_cast<int>(batch.length), row_ids_decode.data(),
                                  default_memory_pool()));
  }
  st.SetItemsProcessed(st.iterations() * batch.length);
}

static void BM_RowArray_Decode(benchmark::State& st,
                               const std::shared_ptr<DataType>& type) {
  SchemaBuilder schema_builder;
  DCHECK_OK(schema_builder.AddField(field("", type)));
  auto schema = *schema_builder.Finish();
  RowArrayDecodeBenchmark(st, schema, 0);
}

BENCHMARK_CAPTURE(BM_RowArray_Decode, "boolean", boolean());
BENCHMARK_CAPTURE(BM_RowArray_Decode, "int8", int8());
BENCHMARK_CAPTURE(BM_RowArray_Decode, "int16", int16());
BENCHMARK_CAPTURE(BM_RowArray_Decode, "int32", int32());
BENCHMARK_CAPTURE(BM_RowArray_Decode, "int64", int64());

static void BM_RowArray_DecodeFixedSizeBinary(benchmark::State& st) {
  int fixed_size = static_cast<int>(st.range(0));
  SchemaBuilder schema_builder;
  DCHECK_OK(schema_builder.AddField(field("", fixed_size_binary(fixed_size))));
  auto schema = *schema_builder.Finish();
  RowArrayDecodeBenchmark(st, schema, 0);
}

BENCHMARK(BM_RowArray_DecodeFixedSizeBinary)
    ->ArgNames({"fixed_size"})
    ->ArgsProduct({{3, 5, 6, 7, 9, 16, 42}});

static void BM_RowArray_DecodeBinary(benchmark::State& st) {
  int max_length = static_cast<int>(st.range(0));
  std::unordered_map<std::string, std::string> metadata;
  metadata["max_length"] = std::to_string(max_length);
  SchemaBuilder schema_builder;
  DCHECK_OK(schema_builder.AddField(field("", utf8(), key_value_metadata(metadata))));
  auto schema = *schema_builder.Finish();
  RowArrayDecodeBenchmark(st, schema, 0);
}

BENCHMARK(BM_RowArray_DecodeBinary)
    ->ArgNames({"max_length"})
    ->ArgsProduct({{32, 64, 128}});

static void BM_RowArray_DecodeOneOfColumns(benchmark::State& st,
                                           std::vector<std::shared_ptr<DataType>> types) {
  SchemaBuilder schema_builder;
  for (const auto& type : types) {
    DCHECK_OK(schema_builder.AddField(field("", type)));
  }
  auto schema = *schema_builder.Finish();
  int column_to_decode = static_cast<int>(st.range(0));
  RowArrayDecodeBenchmark(st, schema, column_to_decode);
}

const std::vector<std::shared_ptr<DataType>> fixed_length_row_column_types{
    boolean(), int32(), fixed_size_binary(64)};
BENCHMARK_CAPTURE(BM_RowArray_DecodeOneOfColumns,
                  "fixed_length_row:{boolean,int32,fixed_size_binary(64)}",
                  fixed_length_row_column_types)
    ->ArgNames({"column"})
    ->ArgsProduct(
        {benchmark::CreateDenseRange(0, fixed_length_row_column_types.size() - 1, 1)});

const std::vector<std::shared_ptr<DataType>> var_length_row_column_types{
    boolean(), int32(), utf8(), utf8()};
BENCHMARK_CAPTURE(BM_RowArray_DecodeOneOfColumns,
                  "var_length_row:{boolean,int32,utf8,utf8}", var_length_row_column_types)
    ->ArgNames({"column"})
    ->ArgsProduct({benchmark::CreateDenseRange(0, var_length_row_column_types.size() - 1,
                                               1)});

}  // namespace acero
}  // namespace arrow
