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

#include <gmock/gmock-matchers.h>

#include <condition_variable>
#include <mutex>

#include "arrow/api.h"
#include "arrow/compute/exec/accumulation_queue.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/spilling_util.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/light_array.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {
namespace internal {

enum class SpillingTestParam {
  None,
  Values,
  ValuesAndHashes,
};

void TestSpillingAccumulationQueue(SpillingTestParam param) {
  QueryContext ctx;
  SpillingAccumulationQueue queue;

  Future<> fut = util::AsyncTaskScheduler::Make([&](util::AsyncTaskScheduler* sched) {
    RETURN_NOT_OK(ctx.Init(ctx.max_concurrency(), sched));
    RETURN_NOT_OK(queue.Init(&ctx));
    ctx.scheduler()->RegisterEnd();
    RETURN_NOT_OK(ctx.scheduler()->StartScheduling(
        /*thread_index=*/0,
        [&ctx](std::function<Status(size_t)> fn) {
          return ctx.ScheduleTask(std::move(fn));
        },
        /*concurrent_tasks=*/static_cast<int>(ctx.max_concurrency()), false));

    size_t num_batches = 4 * SpillingAccumulationQueue::kNumPartitions;
    size_t rows_per_batch = ExecBatchBuilder::num_rows_max();
    std::vector<ExecBatch> batches;

    size_t spill_every_n_batches = 0;
    switch (param) {
      case SpillingTestParam::None:
        spill_every_n_batches = num_batches;
        break;
      case SpillingTestParam::Values:
        spill_every_n_batches = 32;
        break;
      case SpillingTestParam::ValuesAndHashes:
        spill_every_n_batches = 3;
        break;
      default:
        DCHECK(false);
    }

    int num_vals_spilled = 0;
    int num_hashes_spilled = 0;
    for (size_t i = 0; i < num_batches; i++) {
      if (i % spill_every_n_batches == 0) {
        ARROW_ASSIGN_OR_RAISE(bool advanced, queue.AdvanceSpillCursor());
        if (num_vals_spilled < SpillingAccumulationQueue::kNumPartitions) {
          ARROW_CHECK(advanced);
        }
        num_vals_spilled++;

        if (!advanced) {
          ARROW_ASSIGN_OR_RAISE(bool advanced_hash, queue.AdvanceHashCursor());
          if (num_hashes_spilled < SpillingAccumulationQueue::kNumPartitions) {
            ARROW_CHECK(advanced_hash);
          }
          num_hashes_spilled++;
        }
      }

      ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> vals_buf,
                            AllocateBuffer(sizeof(uint64_t) * rows_per_batch));
      ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> hashes_buf,
                            AllocateBuffer(sizeof(uint64_t) * rows_per_batch));

      uint64_t* vals = reinterpret_cast<uint64_t*>(vals_buf->mutable_data());
      uint64_t* hashes = reinterpret_cast<uint64_t*>(hashes_buf->mutable_data());
      for (size_t j = 0; j < rows_per_batch; j++) {
        vals[j] = j;
        hashes[j] = (j % SpillingAccumulationQueue::kNumPartitions);
      }

      ArrayData vals_data(uint64(), rows_per_batch, {nullptr, std::move(vals_buf)});
      ArrayData hashes_data(uint64(), rows_per_batch, {nullptr, std::move(hashes_buf)});
      ExecBatch batch({std::move(vals_data), std::move(hashes_data)}, rows_per_batch);
      ARROW_CHECK_OK(queue.InsertBatch(/*thread_index=*/0, std::move(batch)));
    }

    for (size_t ipart = 0; ipart < SpillingAccumulationQueue::kNumPartitions; ipart++) {
      Future<> fut = Future<>::Make();
      AccumulationQueue ac;
      ac.Resize(queue.batch_count(ipart));
      ARROW_CHECK_OK(queue.GetPartition(
          /*thread_index=*/0,
          /*partition=*/ipart,
          [&](size_t, size_t batch_idx, ExecBatch batch) {
            ac[batch_idx] = std::move(batch);
            return Status::OK();
          },
          [&](size_t) {
            fut.MarkFinished();
            return Status::OK();
          }));
      ARROW_CHECK_OK(fut.status());
      ARROW_CHECK_EQ(ac.batch_count(),
                     num_batches / SpillingAccumulationQueue::kNumPartitions);
      for (size_t ibatch = 0; ibatch < ac.batch_count(); ibatch++) {
        ARROW_CHECK_EQ(ac[ibatch].num_values(), 1);
        ARROW_CHECK_EQ(ac[ibatch].length, ExecBatchBuilder::num_rows_max());
        const uint64_t* vals =
            reinterpret_cast<const uint64_t*>(ac[ibatch][0].array()->buffers[1]->data());
        for (int64_t irow = 0; irow < ac[ibatch].length; irow++)
          ARROW_CHECK_EQ(vals[irow] % SpillingAccumulationQueue::kNumPartitions, ipart);
      }
    }
    return Status::OK();
  });
  ASSERT_FINISHES_OK(fut);
}

TEST(Spilling, SpillingAccumulationQueue_NoSpill) {
  TestSpillingAccumulationQueue(SpillingTestParam::None);
}

TEST(Spilling, SpillingAccumulationQueue_SpillValues) {
  TestSpillingAccumulationQueue(SpillingTestParam::Values);
}

TEST(Spilling, SpillingAccumulationQueue_SpillValuesAndHashes) {
  TestSpillingAccumulationQueue(SpillingTestParam::ValuesAndHashes);
}

TEST(Spilling, ReadWriteBasicBatches) {
  QueryContext ctx;
  SpillFile file;
  BatchesWithSchema batches = MakeBasicBatches();
  std::vector<ExecBatch> read_batches(batches.batches.size());

  Future<> fut = util::AsyncTaskScheduler::Make([&](util::AsyncTaskScheduler* sched) {
    ARROW_CHECK_OK(ctx.Init(ctx.max_concurrency(), sched));
    for (ExecBatch& b : batches.batches) {
      ExecBatchBuilder builder;
      std::vector<uint16_t> row_ids(b.length);
      std::iota(row_ids.begin(), row_ids.end(), 0);
      ARROW_CHECK_OK(builder.AppendSelected(ctx.memory_pool(), b,
                                            static_cast<int>(b.length), row_ids.data(),
                                            b.num_values()));
      ARROW_CHECK_OK(file.SpillBatch(&ctx, builder.Flush()));
    }

    ARROW_CHECK_OK(file.ReadBackBatches(
        &ctx,
        [&read_batches](size_t, size_t batch_idx, ExecBatch batch) {
          read_batches[batch_idx] = std::move(batch);
          return Status::OK();
        },
        [&](size_t) {
          AssertExecBatchesEqualIgnoringOrder(batches.schema, batches.batches,
                                              read_batches);
          return Status::OK();
        }));
    return Status::OK();
  });
  ASSERT_FINISHES_OK(fut);
}

TEST(Spilling, HashJoin) {
  constexpr int kNumTests = 10;
  Random64Bit rng(42);

  // 50% chance to get a string column, 50% chance to get an integer
  std::vector<std::shared_ptr<DataType>> possible_types = {
      int8(), int16(), int32(), int64(), utf8(), utf8(), utf8(), utf8(),
  };

  std::unordered_map<std::string, std::string> key_metadata;
  key_metadata["min"] = "0";
  key_metadata["max"] = "1000";

  for (int itest = 0; itest < kNumTests; itest++) {
    int left_cols = rng.from_range(1, 4);
    std::vector<std::shared_ptr<Field>> left_fields = {
        field("l0", int32(), key_value_metadata(key_metadata))};
    for (int i = 1; i < left_cols; i++) {
      std::string name = std::string("l") + std::to_string(i);
      size_t type = rng.from_range(static_cast<size_t>(0), possible_types.size() - 1);
      left_fields.push_back(field(std::move(name), possible_types[type]));
    }

    int right_cols = rng.from_range(1, 4);
    std::vector<std::shared_ptr<Field>> right_fields = {
        field("r0", int32(), key_value_metadata(key_metadata))};
    for (int i = 1; i < right_cols; i++) {
      std::string name = std::string("r") + std::to_string(i);
      size_t type = rng.from_range(static_cast<size_t>(0), possible_types.size() - 1);
      right_fields.push_back(field(std::move(name), possible_types[type]));
    }

    std::vector<JoinKeyCmp> key_cmp = {JoinKeyCmp::EQ};
    std::vector<FieldRef> left_keys = {FieldRef{0}};
    std::vector<FieldRef> right_keys = {FieldRef{0}};

    std::shared_ptr<Schema> l_schema = schema(std::move(left_fields));
    std::shared_ptr<Schema> r_schema = schema(std::move(right_fields));

    BatchesWithSchema l_batches = MakeRandomBatches(
        l_schema, 10, 1024, kDefaultBufferAlignment, default_memory_pool());
    BatchesWithSchema r_batches = MakeRandomBatches(
        r_schema, 10, 1024, kDefaultBufferAlignment, default_memory_pool());

    std::vector<ExecBatch> reference;
    for (bool spilling : {false, true}) {
      QueryOptions options;
      if (spilling) options.max_memory_bytes = 1024;
      ExecContext ctx(default_memory_pool(), ::arrow::internal::GetCpuThreadPool());
      Declaration l_source{
          "source", SourceNodeOptions{l_batches.schema,
                                      l_batches.gen(/*parallel=*/true, /*slow=*/false)}};
      Declaration r_source{
          "source", SourceNodeOptions{r_batches.schema,
                                      r_batches.gen(/*parallel=*/true, /*slow=*/false)}};

      HashJoinNodeOptions join_options;
      join_options.left_keys = left_keys;
      join_options.right_keys = right_keys;
      join_options.output_all = true;
      join_options.key_cmp = key_cmp;
      Declaration join{"hashjoin", {l_source, r_source}, join_options};

      ASSERT_FINISHES_OK_AND_ASSIGN(auto result,
                                    DeclarationToExecBatchesAsync(join, ctx, options));
      if (!spilling)
        reference = std::move(result.batches);
      else
        AssertExecBatchesEqualIgnoringOrder(result.schema, reference, result.batches);
    }
  }
}
}  // namespace internal
}  // namespace compute
}  // namespace arrow
