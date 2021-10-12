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

#include "arrow/util/async_util.h"

#include "arrow/util/future.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {

AsyncDestroyable::AsyncDestroyable() : on_closed_(Future<>::Make()) {}

#ifndef NDEBUG
AsyncDestroyable::~AsyncDestroyable() {
  DCHECK(constructed_correctly_) << "An instance of AsyncDestroyable must be created by "
                                    "MakeSharedAsync or MakeUniqueAsync";
}
#else
AsyncDestroyable::~AsyncDestroyable() = default;
#endif

void AsyncDestroyable::Destroy() {
  DoDestroy().AddCallback([this](const Status& st) {
    on_closed_.MarkFinished(st);
    delete this;
  });
}

Status AsyncTaskGroup::AddTask(std::function<Result<Future<>>()> task) {
  auto guard = mutex_.Lock();
  if (all_tasks_done_.is_finished()) {
    return Status::Invalid("Attempt to add a task after the task group has completed");
  }
  if (!err_.ok()) {
    return err_;
  }
  Result<Future<>> maybe_task_fut = task();
  if (!maybe_task_fut.ok()) {
    err_ = maybe_task_fut.status();
    return err_;
  }
  return AddTaskUnlocked(*maybe_task_fut, std::move(guard));
}

Status AsyncTaskGroup::AddTaskUnlocked(const Future<>& task_fut,
                                       util::Mutex::Guard guard) {
  // If the task is already finished there is nothing to track so lets save
  // some work and return early
  if (task_fut.is_finished()) {
    err_ &= task_fut.status();
    return err_;
  }
  running_tasks_++;
  guard.Unlock();
  task_fut.AddCallback([this](const Status& st) {
    auto guard = mutex_.Lock();
    err_ &= st;
    if (--running_tasks_ == 0 && finished_adding_) {
      guard.Unlock();
      all_tasks_done_.MarkFinished(err_);
    }
  });
  return Status::OK();
}

Status AsyncTaskGroup::AddTask(const Future<>& task_fut) {
  auto guard = mutex_.Lock();
  if (all_tasks_done_.is_finished()) {
    return Status::Invalid("Attempt to add a task after the task group has completed");
  }
  if (!err_.ok()) {
    return err_;
  }
  return AddTaskUnlocked(task_fut, std::move(guard));
}

Future<> AsyncTaskGroup::End() {
  auto guard = mutex_.Lock();
  finished_adding_ = true;
  if (running_tasks_ == 0) {
    all_tasks_done_.MarkFinished(err_);
    return all_tasks_done_;
  }
  return all_tasks_done_;
}

Future<> AsyncTaskGroup::OnFinished() const { return all_tasks_done_; }

SerializedAsyncTaskGroup::SerializedAsyncTaskGroup() : on_finished_(Future<>::Make()) {}

Status SerializedAsyncTaskGroup::AddTask(std::function<Result<Future<>>()> task) {
  util::Mutex::Guard guard = mutex_.Lock();
  ARROW_RETURN_NOT_OK(err_);
  if (on_finished_.is_finished()) {
    return Status::Invalid("Attempt to add a task after a task group has finished");
  }
  tasks_.push(std::move(task));
  if (!processing_.is_valid()) {
    ConsumeAsMuchAsPossibleUnlocked(std::move(guard));
  }
  return err_;
}

Future<> SerializedAsyncTaskGroup::End() {
  util::Mutex::Guard guard = mutex_.Lock();
  ended_ = true;
  if (!processing_.is_valid()) {
    guard.Unlock();
    on_finished_.MarkFinished(err_);
  }
  return on_finished_;
}

void SerializedAsyncTaskGroup::ConsumeAsMuchAsPossibleUnlocked(
    util::Mutex::Guard&& guard) {
  while (err_.ok() && !tasks_.empty() && TryDrainUnlocked()) {
  }
  if (ended_ && tasks_.empty() && !processing_.is_valid()) {
    guard.Unlock();
    on_finished_.MarkFinished(err_);
  }
}

bool SerializedAsyncTaskGroup::TryDrainUnlocked() {
  if (processing_.is_valid()) {
    return false;
  }
  std::function<Result<Future<>>()> next_task = std::move(tasks_.front());
  tasks_.pop();
  Result<Future<>> maybe_next_fut = next_task();
  if (!maybe_next_fut.ok()) {
    err_ &= maybe_next_fut.status();
    return true;
  }
  Future<> next_fut = maybe_next_fut.MoveValueUnsafe();
  if (next_fut.is_finished()) {
    err_ &= next_fut.status();
    return true;
  }
  processing_ = std::move(next_fut);
  processing_.AddCallback([this](const Status& st) {
    util::Mutex::Guard guard = mutex_.Lock();
    processing_ = Future<>();
    err_ &= st;
    ConsumeAsMuchAsPossibleUnlocked(std::move(guard));
  });
  return false;
}

}  // namespace util
}  // namespace arrow
