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

AsyncCloseable::AsyncCloseable(AsyncCloseable* parent) : on_closed_(Future<>::Make()) {
  if (parent) {
    Mutex::Guard guard = mutex_.Lock();
    parent->children_.push_back(this);
    self_itr_ = --parent->children_.end();
  }
}

AsyncCloseable::~AsyncCloseable() { DCHECK(close_complete_.load()); }

Future<> AsyncCloseable::OnClosed() { return on_closed_; }

Future<> AsyncCloseable::Close() {
  {
    Mutex::Guard guard = mutex_.Lock();
    if (closed_.load()) {
      return Future<>::MakeFinished();
    }
    closed_.store(true);
  }
  return DoClose()
      .Then([this] {
        close_complete_.store(true);
        on_closed_.MarkFinished();
        return CloseChildren();
      })
      .Then([] {},
            [this](const Status& err) {
              close_complete_.store(true);
              on_closed_.MarkFinished(err);
              return err;
            });
}

Future<> AsyncCloseable::CloseChildren() {
  for (auto& child : children_) {
    tasks_.push_back(child->Close());
  }
  return AllComplete(tasks_);
}

Status AsyncCloseable::CheckClosed() const {
  if (closed_.load()) {
    return Status::Invalid("Invalid operation after Close");
  }
  return Status::OK();
}

void AsyncCloseable::AssertNotCloseComplete() const { DCHECK(!close_complete_); }

void AsyncCloseable::AddDependentTask(Future<> task) {
  tasks_.push_back(std::move(task));
}

OwnedAsyncCloseable::OwnedAsyncCloseable(AsyncCloseable* parent)
    : AsyncCloseable(parent) {
  parent_ = parent;
}

void OwnedAsyncCloseable::Init() {
  Mutex::Guard lock = parent_->mutex_.Lock();
  parent_->owned_children_.push_back(shared_from_this());
  owned_self_itr_ = --parent_->owned_children_.end();
}

void OwnedAsyncCloseable::Evict() {
  {
    Mutex::Guard lock = parent_->mutex_.Lock();
    if (parent_->closed_) {
      // Parent is already closing, no need to do anything, the parent will call close on
      // this instance eventually
      return;
    }
    parent_->children_.erase(self_itr_);
  }
  // We need to add a dependent task to make sure our parent does not close itself
  // while we are shutting down.
  parent_->AddDependentTask(Close().Then([this] {
    Mutex::Guard lock = parent_->mutex_.Lock();
    parent_->owned_children_.erase(owned_self_itr_);
  }));
}

NurseryPimpl::~NurseryPimpl() = default;

Nursery::Nursery() : AsyncCloseable(nullptr) {}
Future<> Nursery::DoClose() { return Future<>::MakeFinished(); }

Status Nursery::RunInNursery(std::function<void(Nursery*)> task) {
  Nursery nursery;
  task(&nursery);
  return nursery.Close().status();
}

Status Nursery::RunInNurserySt(std::function<Status(Nursery*)> task) {
  Nursery nursery;
  Status task_st = task(&nursery);
  Status close_st = nursery.Close().status();
  return task_st & close_st;
}

}  // namespace util
}  // namespace arrow
