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

#include "arrow/util/iterator.h"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>

#include "arrow/util/logging.h"

namespace arrow {
namespace detail {

ReadaheadPromise::~ReadaheadPromise() {}

class ReadaheadQueue::Impl : public std::enable_shared_from_this<ReadaheadQueue::Impl> {
 public:
  explicit Impl(int64_t readahead_queue_size) : max_readahead_(readahead_queue_size) {}

  ~Impl() { EnsureShutdownOrDie(false); }

  void Start() {
    // Cannot do this in constructor as shared_from_this() would throw
    DCHECK(!thread_.joinable());
    auto self = shared_from_this();
    thread_ = std::thread([self]() { self->DoWork(); });
    DCHECK(thread_.joinable());
  }

  void EnsureShutdownOrDie(bool wait = true) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!please_shutdown_) {
      ARROW_CHECK_OK(ShutdownUnlocked(std::move(lock), wait));
    }
    DCHECK(!thread_.joinable());
  }

  Status Append(std::unique_ptr<ReadaheadPromise> promise) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (please_shutdown_) {
      return Status::Invalid("Shutdown requested");
    }
    todo_.push_back(std::move(promise));
    if (static_cast<int64_t>(todo_.size()) == 1) {
      // Signal there's more work to do
      lock.unlock();
      worker_wakeup_.notify_one();
    }
    return Status::OK();
  }

  Status PopDone(std::unique_ptr<ReadaheadPromise>* out) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (please_shutdown_) {
      return Status::Invalid("Shutdown requested");
    }
    work_done_.wait(lock, [this]() { return done_.size() > 0; });
    *out = std::move(done_.front());
    done_.pop_front();
    if (static_cast<int64_t>(done_.size()) < max_readahead_) {
      // Signal there's more work to do
      lock.unlock();
      worker_wakeup_.notify_one();
    }
    return Status::OK();
  }

  Status Pump(std::function<std::unique_ptr<ReadaheadPromise>()> factory) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (please_shutdown_) {
      return Status::Invalid("Shutdown requested");
    }
    while (static_cast<int64_t>(done_.size() + todo_.size()) < max_readahead_) {
      todo_.push_back(factory());
    }
    // Signal there's more work to do
    lock.unlock();
    worker_wakeup_.notify_one();
    return Status::OK();
  }

  Status Shutdown(bool wait = true) {
    return ShutdownUnlocked(std::unique_lock<std::mutex>(mutex_), wait);
  }

  Status ShutdownUnlocked(std::unique_lock<std::mutex> lock, bool wait = true) {
    if (please_shutdown_) {
      return Status::Invalid("Shutdown already requested");
    }
    DCHECK(thread_.joinable());
    please_shutdown_ = true;
    lock.unlock();
    worker_wakeup_.notify_one();
    if (wait) {
      thread_.join();
    } else {
      thread_.detach();
    }
    return Status::OK();
  }

  void DoWork() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!please_shutdown_) {
      while (static_cast<int64_t>(done_.size()) < max_readahead_ && todo_.size() > 0) {
        auto promise = std::move(todo_.front());
        todo_.pop_front();
        lock.unlock();
        promise->Call();
        lock.lock();
        done_.push_back(std::move(promise));
        work_done_.notify_one();
        // Exit eagerly
        if (please_shutdown_) {
          return;
        }
      }
      // Wait for more work to do
      worker_wakeup_.wait(lock);
    }
  }

  std::deque<std::unique_ptr<ReadaheadPromise>> todo_;
  std::deque<std::unique_ptr<ReadaheadPromise>> done_;
  int64_t max_readahead_;
  bool please_shutdown_ = false;

  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable worker_wakeup_;
  std::condition_variable work_done_;
};

ReadaheadQueue::ReadaheadQueue(int readahead_queue_size)
    : impl_(new Impl(readahead_queue_size)) {
  impl_->Start();
}

ReadaheadQueue::~ReadaheadQueue() {}

Status ReadaheadQueue::Append(std::unique_ptr<ReadaheadPromise> promise) {
  return impl_->Append(std::move(promise));
}

Status ReadaheadQueue::PopDone(std::unique_ptr<ReadaheadPromise>* out) {
  return impl_->PopDone(out);
}

Status ReadaheadQueue::Pump(std::function<std::unique_ptr<ReadaheadPromise>()> factory) {
  return impl_->Pump(std::move(factory));
}

Status ReadaheadQueue::Shutdown() { return impl_->Shutdown(); }

void ReadaheadQueue::EnsureShutdownOrDie() { return impl_->EnsureShutdownOrDie(); }

}  // namespace detail
}  // namespace arrow
