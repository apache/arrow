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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/type_fwd.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"
#include "arrow/util/optional.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace engine {

class ExecNode;
class QueryNode;
class QueryPlan;

class ARROW_EXPORT ExecPlan {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecPlan() = default;

  /// The query plan this ExecPlan is an instance of
  const QueryPlan& query_plan() const { return *query_plan_; }

  compute::ExecContext* context() { return context_; }

 protected:
  friend class QueryPlan;

  ExecPlan() = default;

  const std::shared_ptr<const QueryPlan> query_plan_;
  compute::ExecContext* context_;
};

class ARROW_EXPORT ExecNode {
 public:
  using NodeVector = std::vector<ExecNode*>;

  virtual ~ExecNode();

  /// The query node this ExecNode is an instance of
  const QueryNode& query_node() const { return *query_node_; }

  /// This node's exec plan
  ExecPlan* plan() { return plan_; }

  /// Transfer input batch to ExecNode
  ///
  /// When all inputs are received for a given batch_index, the batch is ready
  /// for execution.
  Status InputReceived(int32_t input_index, int32_t batch_index,
                       compute::ExecBatch batch);

  /// Mark the inputs finished after the given number of batches.
  ///
  /// This may be called before all inputs are received.  This simply fixes
  /// the total number of incoming batches so that the ExecNode knows when
  /// it has received all input.
  Status InputFinished(int32_t num_batches);

  /// Schedule batch execution once all inputs are received for the given batch_index
  ///
  /// The returned Future is finished once execution of the batch is finished.
  /// Note that execution doesn't necessarily mean that any outputs are produced.
  /// Depending on the ExecNode type, outputs may be produced on the fly,
  /// or only at the end when all inputs have been received.
  Future<> RunAsync(int32_t batch_index, internal::Executor* executor);

  /// Schedule finalization once all batches are executed
  ///
  /// Return a Future that will be marked finished once all inputs are received
  /// and all computation has finished.
  ///
  /// RunAsync and FinishAsync can be though of as exposing a map/reduce pipeline.
  Future<> FinishAsync(internal::Executor* executor);

 protected:
  struct InputBatch {
    std::vector<util::optional<compute::ExecBatch>> batches;
    int32_t num_ready = 0;
    Future<> ready_fut;

    explicit InputBatch(std::vector<util::optional<compute::ExecBatch>> batches);
  };

  ExecNode(ExecPlan* plan, const QueryNode* query_node);

  virtual Status RunSyncInternal(int32_t batch_index) = 0;
  virtual Status FinishSyncInternal() = 0;

  InputBatch* EnsureBatch(int32_t batch_index);
  void ReserveBatches(int32_t num_batches);

  ExecPlan* plan_;
  const QueryNode* const query_node_;
  const int32_t num_inputs_;

  // XXX also use a per-input batch mutex?
  util::Mutex mutex_;
  std::vector<std::unique_ptr<InputBatch>> input_batches_;
  int32_t finish_at_ = -1;
  Future<> finished_fut_;
};

}  // namespace engine
}  // namespace arrow
