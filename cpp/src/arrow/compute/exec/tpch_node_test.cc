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

#include "arrow/api.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/exec/util.h"
#include "arrow/compute/kernels/row_encoder.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/exec/tpch_node.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/thread_pool.h"
#include "arrow/array/validate.h"

namespace arrow
{
    namespace compute
    {
        void ValidateBatch(const ExecBatch &batch)
        {
            for(const Datum &d : batch.values)
                ASSERT_OK(arrow::internal::ValidateArray(*d.array()));
        }

        TEST(TpchNode, Supplier)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Supplier();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                std::cout << batch.ToString() << std::endl;
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 10000);
        }

        TEST(TpchNode, Part)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Part();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 200000);
        }

        TEST(TpchNode, PartSupp)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.PartSupp();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 800000);
        }

        TEST(TpchNode, Customer)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Customer();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 150000);
        }

        TEST(TpchNode, Orders)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Orders();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 1500000);
        }

        TEST(TpchNode, Lineitem)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Lineitem();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            for(auto &batch : res)
            {
                ValidateBatch(batch);
            }
        }

        TEST(TpchNode, Nation)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Nation();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 25);
        }

        TEST(TpchNode, Region)
        {
            ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
            std::shared_ptr<ExecPlan> plan = *ExecPlan::Make(&ctx);
            TpchGen gen = *TpchGen::Make(plan.get());
            ExecNode *table = *gen.Region();
            AsyncGenerator<util::optional<ExecBatch>> sink_gen;
            Declaration sink("sink", { Declaration::Input(table) }, SinkNodeOptions{&sink_gen});
            std::ignore = *sink.AddToPlan(plan.get());
            auto fut = StartAndCollect(plan.get(), sink_gen);
            auto res = *fut.MoveResult();
            int64_t num_rows = 0;
            for(auto &batch : res)
            {
                ValidateBatch(batch);
                num_rows += batch.length;
            }
            ASSERT_EQ(num_rows, 5);
        }
    }
}
