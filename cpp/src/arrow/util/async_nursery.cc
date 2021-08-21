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

#include "arrow/util/async_nursery.h"

#include "arrow/util/logging.h"

namespace arrow {
namespace util {

AsyncCloseable::AsyncCloseable() = default;
AsyncCloseable::AsyncCloseable(AsyncCloseable* parent) {
  AddDependentTask(parent->OnClosed());
}

AsyncCloseable::~AsyncCloseable() {
  // FIXME - Would be awesome if there were a way to enforce this at compile time
  DCHECK_NE(nursery_, nullptr) << "An AsyncCloseable must be created with a nursery "
                                  "using MakeSharedCloseable or MakeUniqueCloseable";
}

const Future<>& AsyncCloseable::OnClosed() {
  // Lazily create the future to save effort if we don't need it
  if (!on_closed_.is_valid()) {
    on_closed_ = Future<>::Make();
  }
  return on_closed_;
}

void AsyncCloseable::AddDependentTask(const Future<>& task) {
  DCHECK(!closed_);
  if (num_tasks_outstanding_.fetch_add(1) == 1) {
    tasks_finished_ = Future<>::Make();
  };
  task.AddCallback([this](const Status& st) {
    if (num_tasks_outstanding_.fetch_sub(1) == 1 && closed_.load()) {
      tasks_finished_.MarkFinished(st);
    }
  });
}

void AsyncCloseable::SetNursery(Nursery* nursery) { nursery_ = nursery; }

void AsyncCloseable::Destroy() {
  DCHECK_NE(nursery_, nullptr);
  closed_ = true;
  nursery_->num_closeables_destroyed_.fetch_add(1);
  Future<> finish_fut;
  if (tasks_finished_.is_valid()) {
    if (num_tasks_outstanding_.fetch_sub(1) > 1) {
      finish_fut = AllComplete({DoClose(), tasks_finished_});
    } else {
      // Any added tasks have already finished so there is nothing to wait for
      finish_fut = DoClose();
    }
  } else {
    // No dependent tasks were added
    finish_fut = DoClose();
  }
  finish_fut.AddCallback([this](const Status& st) {
    if (on_closed_.is_valid()) {
      on_closed_.MarkFinished(st);
    }
    nursery_->OnTaskFinished(st);
    delete this;
  });
}

Status AsyncCloseable::CheckClosed() const {
  if (closed_.load()) {
    return Status::Invalid("Invalid operation after Close");
  }
  return Status::OK();
}

void AsyncCloseablePimpl::Init(AsyncCloseable* impl) { impl_ = impl; }
void AsyncCloseablePimpl::Destroy() { impl_->Destroy(); }
void AsyncCloseablePimpl::SetNursery(Nursery* nursery) { impl_->SetNursery(nursery); }

Nursery::Nursery() : finished_(Future<>::Make()){};

Status Nursery::WaitForFinish() {
  if (num_closeables_destroyed_.load() != num_closeables_created_.load()) {
    return Status::UnknownError(
        "Not all closeables that were created during the nursery were destroyed.  "
        "Something must be holding onto a shared_ptr/unique_ptr reference.");
  }
  if (num_tasks_outstanding_.fetch_sub(1) == 1) {
    // All tasks done, nothing to wait for
    return Status::OK();
  }
  return finished_.status();
}

void Nursery::OnTaskFinished(Status st) {
  if (num_tasks_outstanding_.fetch_sub(1) == 1) {
    finished_.MarkFinished(std::move(st));
  }
}

Status Nursery::RunInNursery(std::function<void(Nursery*)> task) {
  Nursery nursery;
  task(&nursery);
  return nursery.WaitForFinish();
}

Status Nursery::RunInNurserySt(std::function<Status(Nursery*)> task) {
  Nursery nursery;
  Status task_st = task(&nursery);
  // Need to wait for everything to finish, even if invalid status
  Status close_st = nursery.WaitForFinish();
  return task_st & close_st;
}

}  // namespace util
}  // namespace arrow
