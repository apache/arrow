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
namespace arrow::acero {
BackpressureCombiner::BackpressureCombiner(
    std::unique_ptr<BackpressureControl> backpressure_control)
    : backpressure_control_(std::move(backpressure_control)) {}

// Called from Source nodes
void BackpressureCombiner::Pause(Source* output, bool strong_connection) {
  std::lock_guard<std::mutex> lg(mutex_);
  auto& paused_ = strong_connection ? strong_paused_ : weak_paused_;
  auto& paused_count_ = strong_connection ? strong_paused_count_ : weak_paused_count_;

  if (!paused_[output]) {
    paused_[output] = true;
    paused_count_++;
    UpdatePauseStateUnlocked();
  }
}

// Called from Source nodes
void BackpressureCombiner::Resume(Source* output, bool strong_connection) {
  std::lock_guard<std::mutex> lg(mutex_);
  auto& paused_ = strong_connection ? strong_paused_ : weak_paused_;
  auto& paused_count_ = strong_connection ? strong_paused_count_ : weak_paused_count_;
  if (paused_.find(output) == paused_.end()) {
    paused_[output] = false;
    UpdatePauseStateUnlocked();
  }
  if (paused_[output]) {
    paused_[output] = false;
    paused_count_--;
    UpdatePauseStateUnlocked();
  }
}

void BackpressureCombiner::UpdatePauseStateUnlocked() {
  bool should_be_paused =
      strong_paused_count_ > 0 || weak_paused_count_ == weak_paused_.size();
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

BackpressureCombiner::Source::Source(BackpressureCombiner* ctrl, bool strong_connection) {
  if (ctrl) {
    AddController(ctrl, strong_connection);
  }
}

void BackpressureCombiner::Source::AddController(BackpressureCombiner* ctrl,
                                                 bool strong_connection) {
  ctrl->Resume(this, strong_connection);  // populate map in controller
  connections_.push_back(Connection{ctrl, strong_connection});
}
void BackpressureCombiner::Source::Pause() {
  for (auto& conn_ : connections_) {
    conn_.ctrl->Pause(this, conn_.strong);
  }
}
void BackpressureCombiner::Source::Resume() {
  for (auto& conn_ : connections_) {
    conn_.ctrl->Resume(this, conn_.strong);
  }
}

}  // namespace arrow::acero
