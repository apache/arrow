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

#include <random>

namespace arrow
{
namespace compute
{

    class JoinImplFixture : public benchmark::Fixture
    {
    public:
        JoinImplFixture()
        {
            printf("Constructor\n");
        }

        void SetUp(const benchmark::State &state)
        {
            bool parallel = false;
            JoinType join_type = JoinType::INNER;
            int num_batches = 10;
            int batch_size = 1024;

            auto l_schema = schema({field("l1", int32())});
            auto r_schema = schema({field("r1", int32())});

            l_batches_ = MakeRandomBatches(l_schema, num_batches, batch_size);
            r_batches_ = MakeRandomBatches(r_schema, num_batches, batch_size);

            std::vector<FieldRef> left_keys{"l1"};
            std::vector<FieldRef> right_keys{"r1"};

            ctx_ = arrow::internal::make_unique<ExecContext>(
                default_memory_pool(), parallel ? arrow::internal::GetCpuThreadPool() : nullptr);

            schema_mgr_ = arrow::internal::make_unique<HashJoinSchema>();
            DCHECK_OK(schema_mgr_->Init(
                          join_type,
                          *l_batches_.schema,
                          left_keys,
                          *r_batches_.schema,
                          right_keys,
                          "l_",
                          "r_"));

            join_ = *HashJoinImpl::MakeBasic();
            DCHECK_OK(join_->Init(
                          ctx_.get(), join_type, true, 1,
                          schema_mgr_.get(), {JoinKeyCmp::EQ},
                          [](ExecBatch) {},
                          [](int64_t) {},
                          [this](std::function<Status(size_t)> func) -> Status
                          {
                              auto executor = this->ctx_->executor();
                              if (executor)
                              {
                                  RETURN_NOT_OK(executor->Spawn([this, func]
                                  {
                                      size_t thread_index = thread_indexer_();
                                      Status status = func(thread_index);
                                      if (!status.ok())
                                      {
                                          ARROW_DCHECK(false);
                                          return;
                                      }
                                  }));
                              }
                              else
                              {
                                  // We should not get here in serial execution mode
                                  ARROW_DCHECK(false);
                              }
                              return Status::OK();
                          }));
        }

        void TearDown(const benchmark::State &state)
        {
        }

        void RunJoin()
        {
            for(auto batch : r_batches_.batches)
                DCHECK_OK(join_->InputReceived(0, 1, batch));
            DCHECK_OK(join_->InputFinished(0, 1));

            for(auto batch : r_batches_.batches)
                DCHECK_OK(join_->InputReceived(0, 0, batch));
            DCHECK_OK(join_->InputFinished(0, 0));
        }

        ~JoinImplFixture()
        {
            printf("Destructor\n");
        }
        ThreadIndexer thread_indexer_;
        BatchesWithSchema l_batches_;
        BatchesWithSchema r_batches_;
        std::unique_ptr<HashJoinSchema> schema_mgr_;
        std::unique_ptr<HashJoinImpl> join_;
        std::unique_ptr<ExecContext> ctx_;
    };

    BENCHMARK_F(JoinImplFixture, Basic)(benchmark::State &st)
    {
        for(auto _ : st)
        {
            RunJoin();
        }
    }

    BENCHMARK_F(JoinImplFixture, Basic2)(benchmark::State &st)
    {
        for(auto _ : st)
        {
            RunJoin();
        }
    }

} // namespace compute
} // namespace arrow
