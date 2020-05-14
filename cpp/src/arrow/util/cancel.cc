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

#include <atomic>
#include <mutex>
#include <utility>

#include "arrow/util/cancel.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"

namespace arrow {

StopCallback::StopCallback(StopToken* token, Callable cb)
    : token_(token), cb_(std::move(cb)) {
  if (token_ != nullptr) {
    DCHECK(cb_);
    // May call *this
    token_->SetCallback(this);
  }
}

StopCallback::~StopCallback() {
  if (token_ != nullptr) {
    token_->RemoveCallback(this);
  }
}

StopCallback::StopCallback(StopCallback&& other) { *this = std::move(other); }

StopCallback& StopCallback::operator=(StopCallback&& other) {
  token_ = other.token_;
  if (token_ != nullptr) {
    other.token_ = nullptr;
    token_->RemoveCallback(&other);
  }
  cb_ = std::move(other.cb_);
  if (token_ != nullptr) {
    // May call *this
    token_->SetCallback(this);
  }
  return *this;
}

void StopCallback::Call(const Status& st) {
  if (cb_) {
    // Forget callable after calling it
    Callable local_cb;
    cb_.swap(local_cb);
    local_cb(st);
  }
}

// NOTE: We care mainly about the making the common case (not cancelled) fast.

struct StopToken::Impl {
  std::atomic<bool> requested_{false};
  std::mutex mutex_;
  StopCallback* cb_{nullptr};
  Status cancel_error_;
};

StopToken::StopToken() : impl_(new Impl()) {}

StopToken::~StopToken() {}

Status StopToken::Poll() {
  if (impl_->requested_) {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    return impl_->cancel_error_;
  }
  return Status::OK();
}

bool StopToken::IsStopRequested() { return impl_->requested_; }

void StopToken::RequestStop() { RequestStop(Status::Cancelled("Operation cancelled")); }

void StopToken::RequestStop(Status st) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  DCHECK(!st.ok());
  if (!impl_->requested_) {
    impl_->requested_ = true;
    impl_->cancel_error_ = std::move(st);
    if (impl_->cb_) {
      impl_->cb_->Call(impl_->cancel_error_);
    }
  }
}

StopCallback StopToken::SetCallback(StopCallback::Callable cb) {
  return StopCallback(this, std::move(cb));
}

void StopToken::SetCallback(StopCallback* cb) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  DCHECK_EQ(impl_->cb_, nullptr);
  impl_->cb_ = cb;
  if (impl_->requested_) {
    impl_->cb_->Call(impl_->cancel_error_);
  }
}

void StopToken::RemoveCallback(StopCallback* cb) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  DCHECK_EQ(impl_->cb_, cb);
  impl_->cb_ = nullptr;
}

}  // namespace arrow
