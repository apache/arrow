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

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/mutex.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

/// A thread safe set of values which can be borrowed (for example by tasks running in a
/// ThreadPool). Borrowing a value will either lazily construct it or pop from a cache.
/// Calls to the constructor and destructor are not synchronized.
template <typename T>
class BorrowSet : public std::enable_shared_from_this<BorrowSet<T>> {
 public:
  /// Construct a BorrowSet with static capacity.
  static std::shared_ptr<BorrowSet> Make(int capacity) {
    std::shared_ptr<BorrowSet> set(new BorrowSet);
    set->SetCapacity(capacity);
    return set;
  }

  /// Construct a BorrowSet whose capacity tracks that of an Executor.
  static std::shared_ptr<BorrowSet> Make(Executor* executor) {
    std::shared_ptr<BorrowSet> set(new BorrowSet);

    std::weak_ptr<BorrowSet> weak_set(set);
    executor->OnCapacityChanged([weak_set](int capacity) {
      if (auto set = weak_set.lock()) {
        set->SetCapacity(capacity);
        return true;
      }
      // set was discarded, delete this callback
      return false;
    });

    return set;
  }

  ~BorrowSet() { SetCapacity(0); }

  /// Add a callback which will be invoked whenever the BorrowSet is done with an instance
  /// of T. This will be called in ~BorrowSet() and whenever capacity is decreased. This
  /// method is not thread safe, invoke it before multiple threads are accessing the
  /// BorrowSet. Only one callback is supported; multiple calls to this function will
  /// overwrite the previous callback.
  void OnDone(std::function<void(T&&)> on_done) { on_done_ = std::move(on_done); }

  struct Value {
   public:
    Value(T value, BorrowSet* set)
        : value_(std::move(value)), set_(set->shared_from_this()) {}

    ARROW_DEFAULT_MOVE_AND_ASSIGN(Value);
    ARROW_DISALLOW_COPY_AND_ASSIGN(Value);

    ~Value() {
      if (set_) {
        set_->ReturnOne(std::move(value_));
      }
    }

    T* get() { return &value_; }
    T& operator*() { return value_; }
    T* operator->() { return &value_; }

   private:
    T value_;
    std::shared_ptr<BorrowSet> set_;
  };

  Value BorrowOne() {
    auto lock = mutex_.Lock();
    // NB: can't assert that we haven't exceeded capacity because we might
    // have multiple still-borrowed instances after a capacity change

    ++borrowed_count_;

    if (cache_.empty()) {
      // create a new value and borrow that
      lock.Unlock();
      return Value(T{}, this);
    }

    // pop a value from the cache
    auto cached = std::move(cache_.back());
    cache_.pop_back();
    return Value(std::move(cached), this);
  }

 private:
  BorrowSet() = default;

  void ReturnOne(T returned) {
    auto lock = mutex_.Lock();

    --borrowed_count_;

    if (GetInstanceCountUnlocked() < capacity_) {
      return cache_.push_back(std::move(returned));
    }

    // returning an instance after capacity has been decreased;
    // just destroy this extra now
    lock.Unlock();
    on_done_(std::move(returned));
  }

  int GetInstanceCountUnlocked() const {
    return borrowed_count_ + static_cast<int>(cache_.size());
  }

  void SetCapacity(int capacity) {
    auto lock = mutex_.Lock();

    capacity_ = capacity;
    cache_.reserve(capacity_);

    // If capacity has been reduced there may be cached instances which we
    // can be done with immediately.
    int extra_count = GetInstanceCountUnlocked() - capacity_;
    if (extra_count <= 0) return;

    extra_count = std::min(static_cast<int>(cache_.size()), extra_count);

    std::vector<T> done_now(extra_count);
    std::move(cache_.end() - extra_count, cache_.end(), done_now.begin());
    cache_.erase(cache_.end() - extra_count, cache_.end());
    lock.Unlock();

    for (auto&& extra : done_now) {
      on_done_(std::move(extra));
    }
  }

  util::Mutex mutex_;
  std::function<void(T&&)> on_done_ = [](T&&) {};
  std::vector<T> cache_;
  int borrowed_count_ = 0, capacity_ = 0;
};

}  // namespace internal
}  // namespace arrow
