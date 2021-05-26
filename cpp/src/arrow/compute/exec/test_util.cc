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

#include "arrow/compute/exec/test_util.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/datum.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"

namespace arrow {

using internal::Executor;

namespace compute {

void AssertBatchesEqual(const ExecBatch& expected, const ExecBatch& actual) {
  ASSERT_THAT(actual.values, testing::ElementsAreArray(expected.values));
}

namespace {

// TODO expose this as `static ValueDescr::FromSchemaColumns`?
std::vector<ValueDescr> DescrFromSchemaColumns(const Schema& schema) {
  std::vector<ValueDescr> descr(schema.num_fields());
  std::transform(schema.fields().begin(), schema.fields().end(), descr.begin(),
                 [](const std::shared_ptr<Field>& field) {
                   return ValueDescr::Array(field->type());
                 });
  return descr;
}

struct DummyNode : ExecNode {
  DummyNode(ExecPlan* plan, std::string label, NodeVector inputs, int num_outputs,
            StartProducingFunc start_producing, StopProducingFunc stop_producing)
      : ExecNode(plan, std::move(label), std::move(inputs), {}, descr(), num_outputs),
        start_producing_(std::move(start_producing)),
        stop_producing_(std::move(stop_producing)) {
    input_labels_.resize(inputs_.size());
    for (size_t i = 0; i < input_labels_.size(); ++i) {
      input_labels_[i] = std::to_string(i);
    }
  }

  const char* kind_name() override { return "Dummy"; }

  void InputReceived(ExecNode* input, int seq_num, ExecBatch batch) override {}

  void ErrorReceived(ExecNode* input, Status error) override {}

  void InputFinished(ExecNode* input, int seq_stop) override {}

  Status StartProducing() override {
    if (start_producing_) {
      RETURN_NOT_OK(start_producing_(this));
    }
    started_ = true;
    return Status::OK();
  }

  void PauseProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void ResumeProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
  }

  void StopProducing(ExecNode* output) override {
    ASSERT_GE(num_outputs(), 0) << "Sink nodes should not experience backpressure";
    AssertIsOutput(output);
    StopProducing();
  }

  void StopProducing() override {
    if (started_) {
      started_ = false;
      for (const auto& input : inputs_) {
        input->StopProducing(this);
      }
      if (stop_producing_) {
        stop_producing_(this);
      }
    }
  }

 private:
  void AssertIsOutput(ExecNode* output) {
    ASSERT_NE(std::find(outputs_.begin(), outputs_.end(), output), outputs_.end());
  }

  BatchDescr descr() const { return std::vector<ValueDescr>{ValueDescr(null())}; }

  StartProducingFunc start_producing_;
  StopProducingFunc stop_producing_;
  bool started_ = false;
};

AsyncGenerator<util::optional<ExecBatch>> Wrap(RecordBatchGenerator gen,
                                               ::arrow::internal::Executor* io_executor) {
  return MakeMappedGenerator(
      MakeTransferredGenerator(std::move(gen), io_executor),
      [](const std::shared_ptr<RecordBatch>& batch) -> util::optional<ExecBatch> {
        return ExecBatch(*batch);
      });
}

}  // namespace

ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    const std::shared_ptr<Schema>& schema,
                                    RecordBatchGenerator generator,
                                    ::arrow::internal::Executor* io_executor) {
  return MakeSourceNode(plan, std::move(label), DescrFromSchemaColumns(*schema),
                        Wrap(std::move(generator), io_executor));
}

ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    const std::shared_ptr<RecordBatchReader>& reader,
                                    Executor* io_executor) {
  auto gen =
      MakeBackgroundGenerator(MakeIteratorFromReader(reader), io_executor).ValueOrDie();

  return MakeRecordBatchReaderNode(plan, std::move(label), reader->schema(),
                                   std::move(gen), io_executor);
}

ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, std::vector<ExecNode*> inputs,
                        int num_outputs, StartProducingFunc start_producing,
                        StopProducingFunc stop_producing) {
  return plan->EmplaceNode<DummyNode>(plan, std::move(label), std::move(inputs),
                                      num_outputs, std::move(start_producing),
                                      std::move(stop_producing));
}

}  // namespace compute
}  // namespace arrow
