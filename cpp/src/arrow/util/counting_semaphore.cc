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

#include "arrow/util/counting_semaphore_internal.h"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>

#include "arrow/status.h"

namespace arrow {
namespace util {

class CountingSemaphore::Impl {
 public:
  Impl(uint32_t initial_avail, double timeout_seconds)
      : num_permits_(initial_avail), timeout_seconds_(timeout_seconds) {}

  Status Acquire(uint32_t num_permits) {
    std::unique_lock<std::mutex> lk(mutex_);
    RETURN_NOT_OK(CheckClosed());
    num_waiters_ += num_permits;
    waiter_cv_.notify_all();
    bool timed_out = !acquirer_cv_.wait_for(
        lk, std::chrono::nanoseconds(static_cast<int64_t>(timeout_seconds_ * 1e9)),
        [&] { return closed_ || num_permits <= num_permits_; });
    num_waiters_ -= num_permits;
    if (timed_out) {
      return Status::Invalid("Timed out waiting for semaphore to release ", num_permits,
                             " permits.");
    }
    if (closed_) {
      return Status::Invalid("Semaphore closed while acquiring");
    }
    num_permits_ -= num_permits;
    return Status::OK();
  }

  Status Release(uint32_t num_permits) {
    std::lock_guard<std::mutex> lg(mutex_);
    RETURN_NOT_OK(CheckClosed());
    num_permits_ += num_permits;
    acquirer_cv_.notify_all();
    return Status::OK();
  }

  Status WaitForWaiters(uint32_t num_waiters) {
    std::unique_lock<std::mutex> lk(mutex_);
    RETURN_NOT_OK(CheckClosed());
    if (waiter_cv_.wait_for(
            lk, std::chrono::nanoseconds(static_cast<int64_t>(timeout_seconds_ * 1e9)),
            [&] { return closed_ || num_waiters <= num_waiters_; })) {
      if (closed_) {
        return Status::Invalid("Semaphore closed while waiting for waiters");
      }
      return Status::OK();
    }
    return Status::Invalid("Timed out waiting for ", num_waiters,
                           " to start waiting on semaphore");
  }

  Status Close() {
    std::lock_guard<std::mutex> lg(mutex_);
    RETURN_NOT_OK(CheckClosed());
    closed_ = true;
    if (num_waiters_ > 0) {
      waiter_cv_.notify_all();
      acquirer_cv_.notify_all();
      return Status::Invalid(
          "There were one or more threads waiting on a semaphore when it was closed");
    }
    return Status::OK();
  }

 private:
  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Invalid operation on closed semaphore");
    }
    return Status::OK();
  }

  uint32_t num_permits_;
  double timeout_seconds_;
  uint32_t num_waiters_ = 0;
  bool closed_ = false;
  std::mutex mutex_;
  std::condition_variable acquirer_cv_;
  std::condition_variable waiter_cv_;
};

CountingSemaphore::CountingSemaphore(uint32_t initial_avail, double timeout_seconds)
    : impl_(new Impl(initial_avail, timeout_seconds)) {}

CountingSemaphore::~CountingSemaphore() = default;

Status CountingSemaphore::Acquire(uint32_t num_permits) {
  return impl_->Acquire(num_permits);
}
Status CountingSemaphore::Release(uint32_t num_permits) {
  return impl_->Release(num_permits);
}
Status CountingSemaphore::WaitForWaiters(uint32_t num_waiters) {
  return impl_->WaitForWaiters(num_waiters);
}
Status CountingSemaphore::Close() { return impl_->Close(); }

}  // namespace util
}  // namespace arrow
