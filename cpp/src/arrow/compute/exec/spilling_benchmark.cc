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

#include <thread>
#include "benchmark/benchmark.h"
#include "arrow/util/checked_cast.h"
#include "arrow/compute/exec/accumulation_queue.h"
#include "arrow/compute/exec/spilling_util.h"
#include "arrow/compute/exec/test_util.h"

namespace arrow
{
    namespace compute
    {
        struct SpillingBenchmarkSettings
        {
            int64_t num_files = 4;
            int64_t num_threads = -1;
        };

        static void SpillingWrite_Impl(benchmark::State &st, SpillingBenchmarkSettings &settings)
        {
            constexpr int num_batches = 1024;
            constexpr int batch_size = 32000;
            int64_t num_files = settings.num_files;
            std::shared_ptr<Schema> bm_schema = schema({ field("f1", int32()), field("f2", int32()) });
            Random64Bit rng(42);
            for(auto _ : st)
            {
                st.PauseTiming();
                {
                    QueryContext ctx;
                    std::vector<SpillFile> file(num_files);
                    Future<> fut = util::AsyncTaskScheduler::Make(
                        [&](util::AsyncTaskScheduler *sched)
                        {
                            RETURN_NOT_OK(ctx.Init(settings.num_threads, sched));
                            if(settings.num_threads != -1)
                                RETURN_NOT_OK(
                                    arrow::internal::checked_cast<arrow::internal::ThreadPool *>(ctx.io_context()->executor())->
                                    SetCapacity(static_cast<int>(settings.num_threads)));
                            BatchesWithSchema batches = MakeRandomBatches(
                                bm_schema,
                                num_batches,
                                batch_size,
                                SpillFile::kAlignment,
                                ctx.memory_pool());
                            st.ResumeTiming();

                            for(ExecBatch &b : batches.batches)
                            {
                                int64_t idx = rng.from_range(static_cast<int64_t>(0), num_files - 1);
                                RETURN_NOT_OK(file[idx].SpillBatch(&ctx, std::move(b)));
                            }
                            return Status::OK();
                        });
                    fut.Wait();
                    st.PauseTiming();
                    for(SpillFile &f : file)
                        DCHECK_OK(f.Cleanup());
                }
                st.ResumeTiming();
            }
            st.counters["BytesProcessed"] =
                benchmark::Counter(num_batches * batch_size * sizeof(int32_t) * 2,
                        benchmark::Counter::kIsIterationInvariantRate,
                        benchmark::Counter::OneK::kIs1024);
        }

        static void BM_SpillingWrite(benchmark::State &st)
        {
            SpillingBenchmarkSettings settings;
            settings.num_files = st.range(0);
            SpillingWrite_Impl(st, settings);
        }

        static void BM_SpillingRead(benchmark::State &st)
        {
            constexpr int num_batches = 1024;
            constexpr int batch_size = 32000;
            std::shared_ptr<Schema> bm_schema = schema({ field("f1", int32()), field("f2", int32()) });
            for(auto _ : st)
            {
                st.PauseTiming();
                {
                    SpillFile file;
                    QueryContext ctx;
                    Future<> fut = util::AsyncTaskScheduler::Make(
                        [&](util::AsyncTaskScheduler *sched)
                        {
                            RETURN_NOT_OK(ctx.Init(std::thread::hardware_concurrency(), sched));
                            BatchesWithSchema batches = MakeRandomBatches(
                                bm_schema,
                                num_batches,
                                batch_size,
                                SpillFile::kAlignment,
                                ctx.memory_pool());

                            std::vector<ExecBatch> accum(num_batches);
                            for(ExecBatch &b : batches.batches)
                                DCHECK_OK(file.SpillBatch(&ctx, std::move(b)));

                            while(file.batches_written() < num_batches)
                                std::this_thread::yield();

                            RETURN_NOT_OK(file.PreallocateBatches(ctx.memory_pool()));
                            st.ResumeTiming();

                            RETURN_NOT_OK(file.ReadBackBatches(
                                          &ctx,
                                          [&](size_t, size_t idx, ExecBatch batch)
                                          {
                                              accum[idx] = std::move(batch);
                                              return Status::OK();
                                          },
                                          [&](size_t)
                                          {
                                              return Status::OK();
                                          }));
                            return Status::OK();
                        });
                    fut.Wait();
                    st.PauseTiming();
                    DCHECK_OK(file.Cleanup());
                }
                st.ResumeTiming();
            }
            st.counters["BytesProcessed"] =
                benchmark::Counter(num_batches * batch_size * sizeof(int32_t) * 2,
                        benchmark::Counter::kIsIterationInvariantRate,
                        benchmark::Counter::OneK::kIs1024);
        }


        static void BM_SpillingNumThreads(benchmark::State &st)
        {
            SpillingBenchmarkSettings settings;
            settings.num_threads = st.range(0);
            SpillingWrite_Impl(st, settings);
        }

        BENCHMARK(BM_SpillingWrite)->UseRealTime()->ArgNames({"NumFiles"})->RangeMultiplier(4)->Range(1, SpillingAccumulationQueue::kNumPartitions);
        BENCHMARK(BM_SpillingRead)->UseRealTime();
        BENCHMARK(BM_SpillingNumThreads)->UseRealTime()->ArgNames({"NumThreads"})->RangeMultiplier(2)->Range(1, 2 * std::thread::hardware_concurrency());
    }
}
