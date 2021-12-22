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
  if (finished_adding_) {
    return Status::Cancelled("Ignoring task added after the task group has been ended");
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
  if (finished_adding_) {
    return Status::Cancelled("Ignoring task added after the task group has been ended");
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
  if (ended_) {
    return Status::Cancelled("Ignoring task added after the task group has been ended");
  }
  tasks_.push(std::move(task));
  if (!processing_.is_valid()) {
    ConsumeAsMuchAsPossibleUnlocked(std::move(guard));
  }
  return err_;
}

Future<> SerializedAsyncTaskGroup::EndUnlocked(util::Mutex::Guard&& guard) {
  ended_ = true;
  if (!processing_.is_valid()) {
    guard.Unlock();
    on_finished_.MarkFinished(err_);
  }
  return on_finished_;
}

Future<> SerializedAsyncTaskGroup::End() { return EndUnlocked(mutex_.Lock()); }

Future<> SerializedAsyncTaskGroup::Abort(Status err) {
  util::Mutex::Guard guard = mutex_.Lock();
  err_ = std::move(err);
  tasks_ = std::queue<std::function<Result<Future<>>()>>();
  return EndUnlocked(std::move(guard));
}

void SerializedAsyncTaskGroup::ConsumeAsMuchAsPossibleUnlocked(
    util::Mutex::Guard&& guard) {
  while (err_.ok() && !tasks_.empty() && TryDrainUnlocked()) {
  }
  if (ended_ && (!err_.ok() || tasks_.empty()) && !processing_.is_valid()) {
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
  if (!next_fut.TryAddCallback([this] {
        return [this](const Status& st) {
          util::Mutex::Guard guard = mutex_.Lock();
          processing_ = Future<>();
          err_ &= st;
          ConsumeAsMuchAsPossibleUnlocked(std::move(guard));
        };
      })) {
    // Didn't add callback, future already finished
    err_ &= next_fut.status();
    return true;
  }
  processing_ = std::move(next_fut);
  return false;
}

Future<> AsyncToggle::WhenOpen() {
  util::Mutex::Guard guard = mutex_.Lock();
  return when_open_;
}

void AsyncToggle::Open() {
  util::Mutex::Guard guard = mutex_.Lock();
  if (!closed_) {
    return;
  }
  closed_ = false;
  Future<> to_finish = when_open_;
  guard.Unlock();
  to_finish.MarkFinished();
}

void AsyncToggle::Close() {
  util::Mutex::Guard guard = mutex_.Lock();
  if (closed_) {
    return;
  }
  closed_ = true;
  when_open_ = Future<>::Make();
}

bool AsyncToggle::IsOpen() {
  util::Mutex::Guard guard = mutex_.Lock();
  return !closed_;
}

BackpressureOptions BackpressureOptions::Make(uint32_t resume_if_below,
                                              uint32_t pause_if_above) {
  auto toggle = std::make_shared<util::AsyncToggle>();
  return BackpressureOptions{std::move(toggle), resume_if_below, pause_if_above};
}

BackpressureOptions BackpressureOptions::NoBackpressure() {
  return BackpressureOptions();
}

}  // namespace util
}  // namespace arrow
