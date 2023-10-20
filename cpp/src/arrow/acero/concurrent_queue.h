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

#include <mutex>
#include <queue>
#include "arrow/acero/backpressure_handler.h"

namespace arrow::acero {

/**
 * Simple implementation for an unbound concurrent queue
 */
template <class T>
class ConcurrentQueue {
 public:
  T Pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [&] { return !queue_.empty(); });
    return PopUnlocked();
  }

  T PopUnlocked() {
    auto item = queue_.front();
    queue_.pop();
    return item;
  }

  void Push(const T& item) {
    std::unique_lock<std::mutex> lock(mutex_);
    return PushUnlocked(item);
  }

  void PushUnlocked(const T& item) {
    queue_.push(item);
    cond_.notify_one();
  }

  void Clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    ClearUnlocked();
  }

  void ClearUnlocked() { queue_ = std::queue<T>(); }

  std::optional<T> TryPop() {
    std::unique_lock<std::mutex> lock(mutex_);
    return TryPopUnlocked();
  }

  std::optional<T> TryPopUnlocked() {
    // Try to pop the oldest value from the queue (or return nullopt if none)
    if (queue_.empty()) {
      return std::nullopt;
    } else {
      auto item = queue_.front();
      queue_.pop();
      return item;
    }
  }

  bool Empty() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  // Un-synchronized access to front
  // For this to be "safe":
  // 1) the caller logically guarantees that queue is not empty
  // 2) pop/try_pop cannot be called concurrently with this
  const T& UnsyncFront() const { return queue_.front(); }

  size_t UnsyncSize() const { return queue_.size(); }

 protected:
  std::mutex& GetMutex() { return mutex_; }

 private:
  std::queue<T> queue_;
  mutable std::mutex mutex_;
  std::condition_variable cond_;
};

template <typename T>
class BackpressureConcurrentQueue : public ConcurrentQueue<T> {
 private:
  struct DoHandle {
    explicit DoHandle(BackpressureConcurrentQueue& queue)
        : queue_(queue), start_size_(queue_.UnsyncSize()) {}

    ~DoHandle() {
      size_t end_size = queue_.UnsyncSize();
      queue_.handler_.Handle(start_size_, end_size);
    }

    BackpressureConcurrentQueue& queue_;
    size_t start_size_;
  };

 public:
  explicit BackpressureConcurrentQueue(BackpressureHandler handler)
      : handler_(std::move(handler)) {}

  T Pop() {
    std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
    DoHandle do_handle(*this);
    return ConcurrentQueue<T>::PopUnlocked();
  }

  void Push(const T& item) {
    std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
    DoHandle do_handle(*this);
    ConcurrentQueue<T>::PushUnlocked(item);
  }

  void Clear() {
    std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
    DoHandle do_handle(*this);
    ConcurrentQueue<T>::ClearUnlocked();
  }

  std::optional<T> TryPop() {
    std::unique_lock<std::mutex> lock(ConcurrentQueue<T>::GetMutex());
    DoHandle do_handle(*this);
    return ConcurrentQueue<T>::TryPopUnlocked();
  }

 private:
  BackpressureHandler handler_;
};

}  // namespace arrow::acero
