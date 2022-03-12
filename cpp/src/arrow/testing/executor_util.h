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

#include "arrow/util/io_util.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

/// An executor which synchronously runs the task as part of the SpawnReal call.
class MockExecutor : public internal::Executor {
 public:
  int GetCapacity() override { return 0; }

  Status SpawnReal(internal::TaskHints hints, internal::FnOnce<void()> task, StopToken,
                   StopCallback&&) override {
    spawn_count_++;
    current_thread_id_ = internal::GetThreadId();
    std::move(task)();
    current_thread_id_ = internal::kUnlikelyThreadId;
    return Status::OK();
  }

  bool OwnsThisThread() const override {
    return internal::GetThreadId() == current_thread_id_;
  }

  int GetThreadIndex() const override { return OwnsThisThread() ? 0 : -1; }
  int spawn_count() const { return spawn_count_; }

 private:
  int spawn_count_ = 0;
  uint64_t current_thread_id_ = -1;
};

/// An executor which does not actually run the task.  Can be used to simulate situations
/// where the executor schedules a task in a long queue and doesn't get around to running
/// it for a while
class DelayedExecutor : public internal::Executor {
 public:
  int GetCapacity() override { return 0; }

  bool OwnsThisThread() const override { return false; }

  int GetThreadIndex() const override { return -1; }

  Status SpawnReal(internal::TaskHints hints, internal::FnOnce<void()> task, StopToken,
                   StopCallback&&) override {
    captured_tasks_.push_back(std::move(task));
    return Status::OK();
  }

  std::vector<internal::FnOnce<void()>>* captured_tasks() { return &captured_tasks_; }

 private:
  std::vector<internal::FnOnce<void()>> captured_tasks_;
};

}  // namespace arrow
