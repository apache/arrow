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
        double build_to_probe_proportion = 0.1;
        int batch_size = 1024;
        int num_build_batches = 32;
        std::vector<std::shared_ptr<DataType>> key_types = { int32() };
        std::vector<std::shared_ptr<DataType>> build_payload_types = {};
        std::vector<std::shared_ptr<DataType>> probe_payload_types = {};

        double null_percentage = 0.0;
        double build_cardinality = 0.6;
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

                uint64_t num_build_rows = settings.num_build_batches * settings.batch_size;
                uint64_t max_int_value = static_cast<uint64_t>(num_build_rows * settings.build_cardinality);

                std::unordered_map<std::string, std::string> metadata;
                metadata["null_probability"] = std::to_string(settings.null_percentage);
                metadata["min"] = "0";
                metadata["max"] = std::to_string(max_int_value);
                auto l_field = field(l_name, settings.key_types[i], key_value_metadata(metadata));
                auto r_field = field(r_name, settings.key_types[i], key_value_metadata(metadata));

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

            int num_probe_batches = static_cast<int>(settings.num_build_batches / settings.build_to_probe_proportion);
            l_batches_ = MakeRandomBatches(l_schema, num_probe_batches, settings.batch_size);
            r_batches_ = MakeRandomBatches(r_schema, settings.num_build_batches, settings.batch_size);

            stats_.num_probe_rows = num_probe_batches * settings.batch_size;

            ctx_ = arrow::internal::make_unique<ExecContext>(
                default_memory_pool(), is_parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

            schema_mgr_ = arrow::internal::make_unique<HashJoinSchema>();
            DCHECK_OK(schema_mgr_->Init(
                          settings.join_type,
                          *l_batches_.schema,
                          left_keys,
                          *r_batches_.schema,
                          right_keys,
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
                          schema_mgr_.get(), {JoinKeyCmp::EQ},
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

    static void BM_HashJoinBasic_Threads(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.num_threads = static_cast<int>(st.range(0));

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_RelativeBuildProbe(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.build_to_probe_proportion = static_cast<double>(st.range(0)) / 100.0;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_NumKeyColumns(benchmark::State &st)
    {
        BenchmarkSettings settings;
        for(int i = 0; i < st.range(0); i++)
            settings.key_types.push_back(int32());

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    static void BM_HashJoinBasic_NullPercentage(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.null_percentage = static_cast<double>(st.range(0)) / 100.0;

        HashJoinBasicBenchmarkImpl(st, settings);
    }
    
    static void BM_HashJoinBasic_BuildCardinality(benchmark::State &st)
    {
        BenchmarkSettings settings;
        settings.build_cardinality = static_cast<double>(st.range(0)) / 100.0;

        HashJoinBasicBenchmarkImpl(st, settings);
    }

    BENCHMARK(BM_HashJoinBasic_Threads)->ArgNames({"Threads"})->DenseRange(1, 16);
    BENCHMARK(BM_HashJoinBasic_RelativeBuildProbe)->ArgNames({"RelativeBuildProbePercentage"})->DenseRange(1, 200, 20);
    BENCHMARK(BM_HashJoinBasic_NumKeyColumns)->ArgNames({"NumKeyColumns"})->RangeMultiplier(2)->Range(1, 32);
    BENCHMARK(BM_HashJoinBasic_NullPercentage)->ArgNames({"NullPercentage"})->DenseRange(0, 100, 10);
    BENCHMARK(BM_HashJoinBasic_BuildCardinality)->ArgNames({"BuildCardinality"})->DenseRange(10, 100, 10);
} // namespace compute
} // namespace arrow
