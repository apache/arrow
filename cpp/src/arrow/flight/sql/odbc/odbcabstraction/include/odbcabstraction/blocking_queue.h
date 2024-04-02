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

#include <atomic>
#include <boost/optional.hpp>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

namespace driver {
namespace odbcabstraction {

template <typename T>
class BlockingQueue {
  size_t capacity_;
  std::vector<T> buffer_;
  size_t buffer_size_{0};
  size_t left_{0};   // index where variables are put inside of buffer (produced)
  size_t right_{0};  // index where variables are removed from buffer (consumed)

  std::mutex mtx_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;

  std::vector<std::thread> threads_;
  std::atomic<size_t> active_threads_{0};
  std::atomic<bool> closed_{false};

 public:
  typedef std::function<boost::optional<T>(void)> Supplier;

  explicit BlockingQueue(size_t capacity) : capacity_(capacity), buffer_(capacity) {}

  void AddProducer(Supplier supplier) {
    active_threads_++;
    threads_.emplace_back([=] {
      while (!closed_) {
        // Block while queue is full
        std::unique_lock<std::mutex> unique_lock(mtx_);
        if (!WaitUntilCanPushOrClosed(unique_lock)) break;
        unique_lock.unlock();

        // Only one thread at a time be notified and call supplier
        auto item = supplier();
        if (!item) break;

        Push(*item);
      }

      std::unique_lock<std::mutex> unique_lock(mtx_);
      active_threads_--;
      not_empty_.notify_all();
    });
  }

  void Push(T item) {
    std::unique_lock<std::mutex> unique_lock(mtx_);
    if (!WaitUntilCanPushOrClosed(unique_lock)) return;

    buffer_[right_] = std::move(item);

    right_ = (right_ + 1) % capacity_;
    buffer_size_++;

    not_empty_.notify_one();
  }

  bool Pop(T* result) {
    std::unique_lock<std::mutex> unique_lock(mtx_);
    if (!WaitUntilCanPopOrClosed(unique_lock)) return false;

    *result = std::move(buffer_[left_]);

    left_ = (left_ + 1) % capacity_;
    buffer_size_--;

    not_full_.notify_one();

    return true;
  }

  void Close() {
    std::unique_lock<std::mutex> unique_lock(mtx_);

    if (closed_) return;
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();

    unique_lock.unlock();

    for (auto& item : threads_) {
      item.join();
    }
  }

 private:
  bool WaitUntilCanPushOrClosed(std::unique_lock<std::mutex>& unique_lock) {
    not_full_.wait(unique_lock,
                   [this]() { return closed_ || buffer_size_ != capacity_; });
    return !closed_;
  }

  bool WaitUntilCanPopOrClosed(std::unique_lock<std::mutex>& unique_lock) {
    not_empty_.wait(unique_lock, [this]() {
      return closed_ || buffer_size_ != 0 || active_threads_ == 0;
    });

    return !closed_ && buffer_size_ > 0;
  }
};

}  // namespace odbcabstraction
}  // namespace driver
