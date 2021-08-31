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
#elif
AsyncDestroyable::~AsyncDestroyable() = default;
#endif

void AsyncDestroyable::Destroy() {
  DoDestroy().AddCallback([this](const Status& st) {
    on_closed_.MarkFinished(st);
    delete this;
  });
}

Status AsyncTaskGroup::AddTask(const Future<>& task) {
  auto guard = mutex_.Lock();
  if (finished_adding_) {
    return Status::Invalid("Attempt to add a task after StopAddingAndWait");
  }
  if (!err_.ok()) {
    return err_;
  }
  running_tasks_++;
  guard.Unlock();
  task.AddCallback([this](const Status& st) {
    auto guard = mutex_.Lock();
    err_ &= st;
    if (--running_tasks_ == 0 && finished_adding_) {
      guard.Unlock();
      all_tasks_done_.MarkFinished(err_);
    }
  });
  return Status::OK();
}

Future<> AsyncTaskGroup::StopAddingAndWait() {
  auto guard = mutex_.Lock();
  finished_adding_ = true;
  if (running_tasks_ == 0) {
    return err_;
  }
  return all_tasks_done_;
}

}  // namespace util
}  // namespace arrow
