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

#include "arrow/dataset/test_util_internal.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function_internal.h"
#include "arrow/datum.h"
#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/vector.h"

namespace arrow {

using acero::TableFromExecBatches;
using compute::SortKey;
using compute::SortOptions;
using compute::Take;
using internal::Executor;

namespace dataset {
namespace {

struct DummyNode : ExecNode {
  DummyNode(ExecPlan* plan, NodeVector inputs, bool is_sink,
            StartProducingFunc start_producing, StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(inputs), {}, (is_sink) ? nullptr : dummy_schema()),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    input_labels_.resize(inputs_.size());
    for (size_t i = 0; i < input_labels_.size(); ++i) {
      input_labels_[i] = std::to_string(i);
    }
  }

  const char* kind_name() const override { return "Dummy"; }

  Status InputReceived(ExecNode* input, ExecBatch batch) override { return Status::OK(); }

  Status InputFinished(ExecNode* input, int total_batches) override {
    return Status::OK();
  }

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void PauseProducing(ExecNode* output, int32_t counter) override {
    ASSERT_NE(output_, nullptr) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void ResumeProducing(ExecNode* output, int32_t counter) override {
    ASSERT_NE(output_, nullptr) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  Status StopProducingImpl() override {
    if (stop_producing_) {
      stop_producing_(this);
    }
    return Status::OK();
  }

 private:
  void AssertIsOutput(ExecNode* output) { ASSERT_EQ(output->output(), nullptr); }

  std::shared_ptr<Schema> dummy_schema() const {
    return schema({field("dummy", null())});
  }

  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  std::unordered_set<ExecNode*> requested_stop_;
  bool started_ = false;
};

}  // namespace

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types, std::string_view json) {
  auto fields = ::arrow::internal::MapVector(
      [](const TypeHolder& th) { return field("", th.GetSharedPtr()); }, types);

  ExecBatch batch{*RecordBatchFromJSON(schema(std::move(fields)), json)};

  return batch;
}

ExecBatch ExecBatchFromJSON(const std::vector<TypeHolder>& types,
                            const std::vector<ArgShape>& shapes, std::string_view json) {
  DCHECK_EQ(types.size(), shapes.size());

  ExecBatch batch = ExecBatchFromJSON(types, json);

  auto value_it = batch.values.begin();
  for (ArgShape shape : shapes) {
    if (shape == ArgShape::SCALAR) {
      if (batch.length == 0) {
        *value_it = MakeNullScalar(value_it->type());
      } else {
        *value_it = value_it->make_array()->GetScalar(0).ValueOrDie();
      }
    }
    ++value_it;
  }

  return batch;
}

Future<> StartAndFinish(ExecPlan* plan) {
  RETURN_NOT_OK(plan->Validate());
  plan->StartProducing();
  return plan->finished();
}

Future<std::vector<ExecBatch>> StartAndCollect(
    ExecPlan* plan, AsyncGenerator<std::optional<ExecBatch>> gen) {
  RETURN_NOT_OK(plan->Validate());
  plan->StartProducing();

  auto collected_fut = CollectAsyncGenerator(gen);

  return AllFinished({plan->finished(), Future<>(collected_fut)})
      .Then([collected_fut]() -> Result<std::vector<ExecBatch>> {
        ARROW_ASSIGN_OR_RAISE(auto collected, collected_fut.result());
        return ::arrow::internal::MapVector(
            [](std::optional<ExecBatch> batch) { return batch.value_or(ExecBatch()); },
            std::move(collected));
      });
}

Result<std::shared_ptr<Table>> SortTableOnAllFields(const std::shared_ptr<Table>& tab) {
  std::vector<SortKey> sort_keys;
  for (int i = 0; i < tab->num_columns(); i++) {
    sort_keys.emplace_back(i);
  }
  ARROW_ASSIGN_OR_RAISE(auto sort_ids, SortIndices(tab, SortOptions(sort_keys)));
  ARROW_ASSIGN_OR_RAISE(auto tab_sorted, Take(tab, sort_ids));
  return tab_sorted.table();
}

void AssertTablesEqualIgnoringOrder(const std::shared_ptr<Table>& exp,
                                    const std::shared_ptr<Table>& act) {
  ASSERT_EQ(exp->num_columns(), act->num_columns());
  if (exp->num_rows() == 0) {
    ASSERT_EQ(exp->num_rows(), act->num_rows());
  } else {
    ASSERT_OK_AND_ASSIGN(auto exp_sorted, SortTableOnAllFields(exp));
    ASSERT_OK_AND_ASSIGN(auto act_sorted, SortTableOnAllFields(act));

    AssertTablesEqual(*exp_sorted, *act_sorted,
                      /*same_chunk_layout=*/false, /*flatten=*/true);
  }
}

void AssertExecBatchesEqualIgnoringOrder(const std::shared_ptr<Schema>& schema,
                                         const std::vector<ExecBatch>& exp,
                                         const std::vector<ExecBatch>& act) {
  ASSERT_OK_AND_ASSIGN(auto exp_tab, TableFromExecBatches(schema, exp));
  ASSERT_OK_AND_ASSIGN(auto act_tab, TableFromExecBatches(schema, act));
  AssertTablesEqualIgnoringOrder(exp_tab, act_tab);
}

void AssertExecBatchesEqual(const std::shared_ptr<Schema>& schema,
                            const std::vector<ExecBatch>& exp,
                            const std::vector<ExecBatch>& act) {
  ASSERT_OK_AND_ASSIGN(auto exp_tab, TableFromExecBatches(schema, exp));
  ASSERT_OK_AND_ASSIGN(auto act_tab, TableFromExecBatches(schema, act));
  AssertTablesEqual(*exp_tab, *act_tab);
}

}  // namespace dataset
}  // namespace arrow
