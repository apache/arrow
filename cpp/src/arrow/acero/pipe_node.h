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
#include <mutex>
#include <string>
#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/visibility.h"

namespace arrow {

using internal::checked_cast;

using compute::Ordering;

namespace acero {
class Pipe;
class PipeSource {
 public:
  PipeSource();
  virtual ~PipeSource() {}
  void Pause(int32_t counter);
  void Resume(int32_t counter);
  Status StopProducing();
  Status Validate(const Ordering& ordering);

 private:
  friend class Pipe;
  Status Initialize(Pipe* pipe);

  virtual Status HandleInputReceived(ExecBatch batch) = 0;
  virtual Status HandleInputFinished(int total_batches) = 0;

  Pipe* pipe_{nullptr};
};

/// @brief Provides pipe like infastructure for Acero. It isa center element for
/// pipe_sink/pipe_tee and pipe_source infrastructure. Can also be used to create
/// auxiliarty outputs(pipes) for ExecNodes.
class ARROW_ACERO_EXPORT Pipe {
 public:
  Pipe(ExecPlan* plan, std::string pipe_name, std::unique_ptr<BackpressureControl> ctrl,
       std::function<Status()> stopProducing, Ordering ordering = Ordering::Unordered(),
       bool pause_on_any = true, bool stop_on_any = false);

  const Ordering& ordering() const;

  // Scan current exec plan to find all pipe_source nodes matching pipe name.
  // All matching pipe_sources will be added to async sources list.
  // If may_be_sync is true first added source will be added to as sync source.
  // This mthod sould be called from ExecNode::Init() containing this pipe
  Status Init(const std::shared_ptr<Schema> schema, bool may_be_sync = false);

  /// @brief Adds asynchronous PipeSource
  /// @param source - pipe source to be added
  /// Batches will be delivered in separate tasks for each
  Status addAsyncSource(PipeSource* source, bool may_be_sync = false);

  /// @brief Adds synchronous PipeSource
  /// @param source - pipe source to be added
  /// Batches will be delivered synchronousely in InputReceived. Only one synchronous
  /// source is supported - this should be last operation within task.
  Status addSyncSource(PipeSource* source);

  /// @brief Transfer batch to pipe
  Status InputReceived(ExecBatch batch);
  /// @brief Mark the inputs finished after the given number of batches.
  Status InputFinished(int total_batches);

  /// @brief Check for pipe_sources connected
  /// Can be used to skip production of exec batches when there are no consumers.
  bool HasSources() const;

  /// @brief Check for number of pipe_sources connected
  size_t CountSources() const;

  /// @brief Get pipe_name
  std::string PipeName() const { return pipe_name_; }

 private:
  friend class PipeSource;
  // Backpresurre interface for PipeSource
  void Pause(PipeSource* output, int counter);
  // Backpresurre interface for PipeSource
  void Resume(PipeSource* output, int counter);
  //
  Status StopProducing(PipeSource* output);

 private:
  ExecPlan* plan_;
  Ordering ordering_;
  std::string pipe_name_;
  std::vector<PipeSource*> async_nodes_;
  PipeSource* sync_node_{nullptr};
  // backpressure
  std::unordered_map<PipeSource*, bool> paused_;
  std::mutex mutex_;
  std::atomic_size_t paused_count_;
  std::unique_ptr<BackpressureControl> ctrl_;
  // stopProducing
  std::atomic_size_t stopped_count_;
  std::function<Status()> stopProducing_;

  const bool pause_on_any_;
  const bool stop_on_any_;
};

}  // namespace acero
}  // namespace arrow
