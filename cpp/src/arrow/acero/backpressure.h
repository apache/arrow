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
#include "arrow/acero/options.h"

#include <mutex>
namespace arrow::acero {

// Generic backpressure controller for ExecNode
class ARROW_ACERO_EXPORT BackpressureController : public BackpressureControl {
 public:
  BackpressureController(ExecNode* node, ExecNode* output,
                         std::atomic<int32_t>& backpressure_counter);

  void Pause() override;
  void Resume() override;

 private:
  ExecNode* node_;
  ExecNode* output_;
  std::atomic<int32_t>& backpressure_counter_;
};

template <typename T>
class BackpressureControlWrapper : public BackpressureControl {
 public:
  explicit BackpressureControlWrapper(T* obj) : obj_(obj) {}

  void Pause() override { obj_->Pause(); }
  void Resume() override { obj_->Resume(); }

 private:
  T* obj_;
};

// Provides infrastructure of combining multiple backpressure sources and propagate the
// result into BackpressureControl There are two logic scheme of backpressure:
// 1. Default pause_on_any=true - pause on any source is propagated - OR logic
// 2. pause_on_any=false - pause is propagated only when all sources are paused - AND
// logic
class ARROW_ACERO_EXPORT BackpressureCombiner {
 public:
  explicit BackpressureCombiner(std::unique_ptr<BackpressureControl> backpressure_control,
                                bool pause_on_any = true);

  // Instances of Source can be used as usual BackpresureControl.
  // That means that BackpressureCombiner::Source can use another BackpressureCombiner
  // as backpressure_control
  // This enabled building more complex backpressure logic using AND/OR operations.
  // Source can also be connected with more BackpressureCombiners to facilitate
  // propagation of backpressure to multiple inputs.
  class ARROW_ACERO_EXPORT Source : public BackpressureControl {
   public:
    // strong - strong_connection=true
    // weak - strong_connection=false
    explicit Source(BackpressureCombiner* ctrl = nullptr);
    void AddController(BackpressureCombiner* ctrl);
    void Pause() override;
    void Resume() override;

   private:
    std::vector<BackpressureCombiner*> connections_;
  };

 private:
  friend class Source;
  void Pause(Source* output);
  void Resume(Source* output);

  void UpdatePauseStateUnlocked();
  bool pause_on_any_;
  std::unique_ptr<BackpressureControl> backpressure_control_;
  std::mutex mutex_;
  std::unordered_map<Source*, bool> paused_;
  size_t paused_count_{0};
  bool paused{false};
};

}  // namespace arrow::acero
