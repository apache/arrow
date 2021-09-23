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

#include "arrow/util/task_scheduler.h"
#include <cstring>
#include <utility>
#include "arrow/util/thread_pool.h"

namespace arrow {

namespace internal {

TaskScheduler::TaskScheduler() : Executor() {}

TaskScheduler::~TaskScheduler() {}

int TaskScheduler::GetCapacity() { return thread_pool_->GetCapacity(); }

bool TaskScheduler::OwnsThisThread() { return thread_pool_->OwnsThisThread(); }

Status TaskScheduler::SpawnReal(TaskHints hints, FnOnce<void()> task,
                                StopToken stop_token, StopCallback&& stop_callback) {
  auto status = thread_pool_->SpawnReal(hints, std::forward<FnOnce<void()>>(task),
                                        std::move(stop_token), std::move(stop_callback));
  return status;
}

Result<std::shared_ptr<TaskScheduler>> TaskScheduler::Make(int threads) {
  auto scheduler = std::shared_ptr<TaskScheduler>(new TaskScheduler());
  auto maybe_pool = ThreadPool::Make(threads);
  if (!maybe_pool.ok()) {
    maybe_pool.status().Abort("Failed to create global CPU thread pool");
  }
  scheduler->thread_pool_ = maybe_pool.ValueOrDie();

  return scheduler;
}

Result<std::shared_ptr<TaskScheduler>> TaskScheduler::MakeEternal(int threads) {
  auto scheduler = std::shared_ptr<TaskScheduler>(new TaskScheduler());
  auto maybe_pool = ThreadPool::MakeEternal(ThreadPool::DefaultCapacity());
  if (!maybe_pool.ok()) {
    maybe_pool.status().Abort("Failed to create global CPU thread pool");
  }
  scheduler->thread_pool_ = maybe_pool.ValueOrDie();
  return scheduler;
}

std::shared_ptr<TaskScheduler> TaskScheduler::MakeCpuTaskScheduler() {
  auto maybe_scheduler = TaskScheduler::MakeEternal(ThreadPool::DefaultCapacity());
  if (!maybe_scheduler.ok()) {
    maybe_scheduler.status().Abort("Failed to create global CPU thread pool");
  }
  return *std::move(maybe_scheduler);
}

TaskScheduler* GetCpuTaskScheduler() {
  static std::shared_ptr<TaskScheduler> singleton = TaskScheduler::MakeCpuTaskScheduler();
  return singleton.get();
}

}  // namespace internal
}  // namespace arrow
