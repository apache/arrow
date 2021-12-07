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
#include "arrow/testing/gtest_util.h"

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/hash_join.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/thread_pool.h"

#include <cstdio>
#include <cstdint>
#include <memory>

#include <omp.h>

namespace arrow
{
namespace compute
{
    struct BenchmarkSettings
    {
        int num_threads = 1;
        JoinType join_type = JoinType::INNER;
        int batch_size = 1024;
        int num_build_batches = 32;
        int num_probe_batches = 32 * 16;
        std::vector<std::shared_ptr<DataType>> key_types = { int32() };
        std::vector<std::shared_ptr<DataType>> build_payload_types = {};
        std::vector<std::shared_ptr<DataType>> probe_payload_types = {};

        double null_percentage = 0.0;
        double cardinality = 1.0; // Proportion of distinct keys in build side
        double selectivity = 1.0; // Probability of a match for a given row
    };

    class JoinBenchmark
    {
    public:
        JoinBenchmark(BenchmarkSettings &settings)
        {
            bool is_parallel = settings.num_threads != 1;

            SchemaBuilder l_schema_builder, r_schema_builder;
            std::vector<FieldRef> left_keys, right_keys;
            for(size_t i = 0; i < settings.key_types.size(); i++)
            {
                std::string l_name = "lk" + std::to_string(i);
                std::string r_name = "rk" + std::to_string(i);

                // For integers, selectivity is the proportion of the build interval that overlaps with
                // the probe interval
                uint64_t num_build_rows = settings.num_build_batches * settings.batch_size;

                uint64_t min_build_value = 0;
                uint64_t max_build_value = static_cast<uint64_t>(num_build_rows * settings.cardinality);

                uint64_t min_probe_value = static_cast<uint64_t>((1.0 - settings.selectivity) * max_build_value);
                uint64_t max_probe_value = min_probe_value + max_build_value;

                std::unordered_map<std::string, std::string> build_metadata;
                build_metadata["null_probability"] = std::to_string(settings.null_percentage);
                build_metadata["min"] = std::to_string(min_build_value);
                build_metadata["max"] = std::to_string(max_build_value);

                std::unordered_map<std::string, std::string> probe_metadata;
                probe_metadata["null_probability"] = std::to_string(settings.null_percentage);
                probe_metadata["min"] = std::to_string(min_probe_value);
                probe_metadata["max"] = std::to_string(max_probe_value);

                auto l_field = field(l_name, settings.key_types[i], key_value_metadata(probe_metadata));
                auto r_field = field(r_name, settings.key_types[i], key_value_metadata(build_metadata));

                DCHECK_OK(l_schema_builder.AddField(std::move(l_field)));
                DCHECK_OK(r_schema_builder.AddField(std::move(r_field)));

                left_keys.push_back(FieldRef(l_name));
                right_keys.push_back(FieldRef(r_name));
            }

            for(size_t i = 0; i < settings.build_payload_types.size(); i++)
            {
                std::string name = "lp" + std::to_string(i);
                DCHECK_OK(l_schema_builder.AddField(field(name, settings.probe_payload_types[i])));
            }

            for(size_t i = 0; i < settings.build_payload_types.size(); i++)
            {
                std::string name = "rp" + std::to_string(i);
                DCHECK_OK(r_schema_builder.AddField(field(name, settings.build_payload_types[i])));
            }

            auto l_schema = *l_schema_builder.Finish();
            auto r_schema = *r_schema_builder.Finish();

            l_batches_ = MakeRandomBatches(l_schema, settings.num_probe_batches, settings.batch_size);
            r_batches_ = MakeRandomBatches(r_schema, settings.num_build_batches, settings.batch_size);

            stats_.num_probe_rows = settings.num_probe_batches * settings.batch_size;

            ctx_ = arrow::internal::make_unique<ExecContext>(
                default_memory_pool(), is_parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

            schema_mgr_ = arrow::internal::make_unique<HashJoinSchema>();
            Expression filter = literal(true);
            DCHECK_OK(schema_mgr_->Init(
                          settings.join_type,
                          *l_batches_.schema,
                          left_keys,
                          *r_batches_.schema,
                          right_keys,
                          filter,
                          "l_",
                          "r_"));

            join_ = *HashJoinImpl::MakeBasic();

            omp_set_num_threads(settings.num_threads);
            auto schedule_callback = [](std::function<Status(size_t)> func) -> Status
            {
                #pragma omp task
                { DCHECK_OK(func(omp_get_thread_num())); }
                return Status::OK();
            };


            DCHECK_OK(join_->Init(
                          ctx_.get(), settings.join_type, !is_parallel /* use_sync_execution*/, settings.num_threads,
                          schema_mgr_.get(), {JoinKeyCmp::EQ}, std::move(filter),
                          [](ExecBatch) {},
                          [](int64_t x) {},
                          schedule_callback));
            
        }

        void RunJoin()
        {
            double nanos = 0;
            #pragma omp parallel reduction(+:nanos)
            {
                auto start = std::chrono::high_resolution_clock::now();
                int tid = omp_get_thread_num();
                #pragma omp for nowait
                for(auto batch : r_batches_.batches)
                    DCHECK_OK(join_->InputReceived(tid, 1 /* side */, batch));
                #pragma omp for nowait
                for(auto batch : l_batches_.batches)
                    DCHECK_OK(join_->InputReceived(tid, 0 /* side */, batch));

                #pragma omp barrier

                #pragma omp single nowait
                { DCHECK_OK(join_->InputFinished(tid, /* side */ 1)); }

                #pragma omp single nowait
                { DCHECK_OK(join_->InputFinished(tid, /* side */ 0)); }
                std::chrono::duration<double, std::nano> elapsed = std::chrono::high_resolution_clock::now() - start;
                nanos += elapsed.count();
            }
            stats_.total_nanoseconds = nanos;
        }

        ThreadIndexer thread_indexer_;
        BatchesWithSchema l_batches_;
        BatchesWithSchema r_batches_;
        std::unique_ptr<HashJoinSchema> schema_mgr_;
        std::unique_ptr<HashJoinImpl> join_;
        std::unique_ptr<ExecContext> ctx_;

        struct
        {
            double total_nanoseconds;
            uint64_t num_probe_rows;
        } stats_;
    };

    static void HashJoinBasicBenchmarkImpl(benchmark::State &st, BenchmarkSettings &settings)
    {
        JoinBenchmark bm(settings);
        double total_nanos = 0;
        uint64_t total_rows = 0;
        for(auto _ : st)
        {
            bm.RunJoin();
            total_nanos += bm.stats_.total_nanoseconds;
            total_rows += bm.stats_.num_probe_rows;
        }
        st.counters["ns/row"] = total_nanos / total_rows;
    }

    template <typename ...Args>
    static void BM_HashJoinBasic_KeyTypes(benchmark::State &st, std::vector<std::shared_ptr<DataType>> key_types, Args&& ...)
    {
        BenchmarkSettings settings;
        settings.num_build_batches = static_cast<int>(st.range(0));
        settings.num_probe_batches = settings.num_build_batches;
        settings.key_types = key_types;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    template <typename ...Args>
    static void BM_HashJoinBasic_Selectivity(benchmark::State &st, std::vector<std::shared_ptr<DataType>> key_types, Args&& ...)
    {
        BenchmarkSettings settings;
        settings.selectivity = static_cast<double>(st.range(0)) / 100.0;

        settings.num_build_batches = static_cast<int>(st.range(1));
        settings.num_probe_batches = settings.num_build_batches;
        settings.key_types = key_types;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    template <typename ...Args>
    static void BM_HashJoinBasic_JoinType(benchmark::State &st, JoinType join_type, Args&& ...)
    {
        BenchmarkSettings settings;
        settings.selectivity = static_cast<double>(st.range(0)) / 100.0;

        settings.num_build_batches = static_cast<int>(st.range(1));
        settings.num_probe_batches = settings.num_build_batches;
        settings.join_type = join_type;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_MatchesPerRow(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.cardinality = 1.0 / static_cast<double>(st.range(0));

        settings.num_build_batches = static_cast<int>(st.range(1));
        settings.num_probe_batches = settings.num_probe_batches;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_PayloadSize(benchmark::State &st)
    {
        BenchmarkSettings settings;
        int32_t payload_size = static_cast<int32_t>(st.range(0));
        settings.probe_payload_types = { fixed_size_binary(payload_size) };
        settings.cardinality = 1.0 / static_cast<double>(st.range(1));

        settings.num_build_batches = static_cast<int>(st.range(2));
        settings.num_probe_batches = settings.num_probe_batches;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_ProbeParallelism(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.num_threads = static_cast<int>(st.range(0));
        settings.num_build_batches = static_cast<int>(st.range(1));
        settings.num_probe_batches = settings.num_build_batches * 8;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_BuildParallelism(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.num_threads = static_cast<int>(st.range(0));
        settings.num_build_batches = static_cast<int>(st.range(1));
        settings.num_probe_batches = settings.num_threads;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_NullPercentage(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.null_percentage = static_cast<double>(st.range(0)) / 100.0;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    std::vector<std::string> keytypes_argnames = { "Hashtable krows" };
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int32}", {int32()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64}", {int64()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64}", {int64(), int64()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64,int64}", {int64(), int64(), int64()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{int64,int64,int64,int64}", {int64(), int64(), int64(), int64()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{utf8}", {utf8()})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 256);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(4)}", {fixed_size_binary(4)})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(8)}", {fixed_size_binary(8)})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(16)}", {fixed_size_binary(16)})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(24)}", {fixed_size_binary(24)})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);
    BENCHMARK_CAPTURE(BM_HashJoinBasic_KeyTypes, "{fixed_size_binary(32)}", {fixed_size_binary(32)})->ArgNames(keytypes_argnames)->RangeMultiplier(4)->Range(1, 4 * 1024);

    std::vector<std::string> selectivity_argnames = { "Selectivity", "HashTable krows" };
    BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{int32}", {int32()})->ArgNames(selectivity_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{int64}", {int64()})->ArgNames(selectivity_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });

    // Joins on UTF8 are currently really slow, so anything above 512 doesn't finished within a reasonable amount of time. 
    BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{utf8}", {utf8()})->ArgNames(selectivity_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 512, 8) });

    BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{fixed_size_binary(16)}", {fixed_size_binary(16)})->ArgNames(selectivity_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_Selectivity, "{fixed_size_binary(32)}", {fixed_size_binary(32)})->ArgNames(selectivity_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });

    std::vector<std::string> jointype_argnames = { "Selectivity", "HashTable krows" };
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Inner", JoinType::INNER)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Semi", JoinType::LEFT_SEMI)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Semi", JoinType::RIGHT_SEMI)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Anti", JoinType::LEFT_ANTI)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Anti", JoinType::RIGHT_ANTI)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Left Outer", JoinType::LEFT_OUTER)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Right Outer", JoinType::RIGHT_OUTER)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });
    BENCHMARK_CAPTURE(BM_HashJoinBasic_JoinType, "Full Outer", JoinType::FULL_OUTER)->ArgNames(jointype_argnames)->ArgsProduct(
        { benchmark::CreateDenseRange(0, 100, 10), benchmark::CreateRange(1, 4 * 1024, 8) });

    BENCHMARK(BM_HashJoinBasic_MatchesPerRow)->ArgNames({"MatchesPerRow", "HashTable krows"})->ArgsProduct(
        { benchmark::CreateRange(1, 16, 2), benchmark::CreateRange(1, 4 * 1024, 8) });

    BENCHMARK(BM_HashJoinBasic_PayloadSize)->ArgNames({"Payload Size", "MatchesPerRow", "HashTable krows"})->ArgsProduct(
        { benchmark::CreateRange(1, 128, 4), benchmark::CreateRange(1, 16, 2), benchmark::CreateRange(1, 4 * 1024, 8) });

    BENCHMARK(BM_HashJoinBasic_ProbeParallelism)->ArgNames({"Threads", "HashTable krows"})->ArgsProduct(
        { benchmark::CreateDenseRange(1, 16, 1), benchmark::CreateRange(1, 4 * 1024, 8) });

    BENCHMARK(BM_HashJoinBasic_BuildParallelism)->ArgNames({"Threads", "HashTable krows"})->ArgsProduct(
        { benchmark::CreateDenseRange(1, 16, 1), benchmark::CreateRange(1, 4 * 1024, 8) });

    BENCHMARK(BM_HashJoinBasic_NullPercentage)->ArgNames({"NullPercentage"})->DenseRange(0, 100, 10);
} // namespace compute
} // namespace arrow
