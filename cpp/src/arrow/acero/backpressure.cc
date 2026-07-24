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

#include "arrow/acero/backpressure.h"
#include "arrow/acero/exec_plan.h"

namespace arrow::acero {

BackpressureController::BackpressureController(ExecNode* node, ExecNode* output,
                                               std::atomic<int32_t>& backpressure_counter)
    : node_(node), output_(output), backpressure_counter_(backpressure_counter) {}
void BackpressureController::Pause() {
  node_->PauseProducing(output_, ++backpressure_counter_);
}
void BackpressureController::Resume() {
  node_->ResumeProducing(output_, ++backpressure_counter_);
}

BackpressureCombiner::BackpressureCombiner(
    std::unique_ptr<BackpressureControl> backpressure_control, bool pause_on_any)
    : pause_on_any_(pause_on_any),
      backpressure_control_(std::move(backpressure_control)) {}

// Called from Source nodes
void BackpressureCombiner::Pause(Source* output) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (!paused_[output]) {
    paused_[output] = true;
    paused_count_++;
    UpdatePauseStateUnlocked();
  }
}

// Called from Source nodes
void BackpressureCombiner::Resume(Source* output) {
  std::lock_guard<std::mutex> lg(mutex_);
  if (paused_.find(output) == paused_.end()) {
    paused_[output] = false;
    UpdatePauseStateUnlocked();
  } else if (paused_[output]) {
    paused_[output] = false;
    paused_count_--;
    UpdatePauseStateUnlocked();
  }
}

void BackpressureCombiner::Stop() {
  std::lock_guard<std::mutex> lg(mutex_);
  stopped = true;
  backpressure_control_->Resume();
  paused = false;
}

void BackpressureCombiner::UpdatePauseStateUnlocked() {
  if (stopped) return;
  bool should_be_paused = (paused_count_ > 0);
  if (!pause_on_any_) {
    should_be_paused = should_be_paused && (paused_count_ == paused_.size());
  }
  if (should_be_paused) {
    if (!paused) {
      backpressure_control_->Pause();
      paused = true;
    }
  } else {
    if (paused) {
      backpressure_control_->Resume();
      paused = false;
    }
  }
}

BackpressureCombiner::Source::Source(BackpressureCombiner* ctrl) {
  if (ctrl) {
    AddController(ctrl);
  }
}

void BackpressureCombiner::Source::AddController(BackpressureCombiner* ctrl) {
  ctrl->Resume(this);  // populate map in controller
  connections_.push_back(ctrl);
}
void BackpressureCombiner::Source::Pause() {
  for (auto& conn_ : connections_) {
    conn_->Pause(this);
  }
}
void BackpressureCombiner::Source::Resume() {
  for (auto& conn_ : connections_) {
    conn_->Resume(this);
  }
}

}  // namespace arrow::acero
