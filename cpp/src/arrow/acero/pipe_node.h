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
  Status Validate(const Ordering& ordering);

 private:
  friend class Pipe;
  Status Initialize(Pipe* pipe);

  virtual Status HandleInputReceived(ExecBatch batch) = 0;
  virtual Status HandleInputFinished(int total_batches) = 0;

  Pipe* pipe_{nullptr};
};

class ARROW_ACERO_EXPORT Pipe {
 public:
  Pipe(ExecPlan* plan, std::string pipe_name, std::unique_ptr<BackpressureControl> ctrl,
       Ordering ordering = Ordering::Unordered());

  const Ordering& ordering() const;

  // Called from pipe_source nodes
  void Pause(PipeSource* output, int counter);

  // Called from pipe_source nodes
  void Resume(PipeSource* output, int counter);

  // Called from pipe_sink
  Status InputReceived(ExecBatch batch);
  // Called from pipe_sink
  Status InputFinished(int total_batches);

  Status addSource(PipeSource* source);

  // Called from pipe_sink Init
  Status Init(const std::shared_ptr<Schema> schema);

  bool HasSources() const;

  std::string PipeName() const { return pipe_name_; }

 private:
  // pipe
  ExecPlan* plan_;
  Ordering ordering_;
  std::string pipe_name_;
  std::vector<PipeSource*> source_nodes_;
  PipeSource* last_source_node_{nullptr};
  // backpressure
  std::unordered_map<PipeSource*, bool> paused_;
  std::mutex mutex_;
  std::atomic<int32_t> paused_count_;
  std::unique_ptr<BackpressureControl> ctrl_;
};

}  // namespace acero
}  // namespace arrow
