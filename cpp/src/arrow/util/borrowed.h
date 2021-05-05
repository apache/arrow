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

#include "arrow/util/functional.h"
#include "arrow/util/mutex.h"

namespace arrow {
namespace internal {

/// A wrapper for a value borrowed from another context.
/// Includes a callback which returns the value to that context in ~Borrowed.
template <typename T>
class Borrowed {
 public:
  Borrowed(T value, FnOnce<void(T)> ret)
      : borrowed_(std::move(value)), return_(std::move(ret)) {}

  Borrowed(Borrowed&&) = default;
  Borrowed& operator=(Borrowed&&) = default;

  ~Borrowed() { std::move(return_)(std::move(borrowed_)); }

  T& get() { return borrowed_; }

 private:
  T borrowed_;
  FnOnce<void(T)> return_;
};

/// A thread safe set of values which can be borrowed (for example by tasks running in a
/// ThreadPool). Borrowing a value will either lazily construct or pop from a cache.
/// Calls to the constructor and destructor are not synchronized. In addition to releasing
/// any resources not handled by T::~T(), the destructor has the responsibility of
/// finalizing any associated accumulations for each instance.
template <typename T>
class BorrowSet : public std::enable_shared_from_this<BorrowSet<T>> {
 public:
  static std::shared_ptr<BorrowSet> Make(std::function<T()> construct,
                                         std::function<void(T&&)> destroy) {
    std::shared_ptr<BorrowSet> set(new BorrowSet);
    set->construct_ = std::move(construct);
    set->destroy_ = std::move(destroy);
    return set;
  }

  ~BorrowSet() { SetCapacity(0); }

  void SetCapacity(int capacity) {
    auto lock = mutex_.Lock();

    capacity_ = capacity;
    cache_.reserve(capacity_);

    // If capacity has been reduced there may be cached instances which we
    // can destroy immediately.
    std::vector<T> destroy_now;
    while (GetInstanceCountUnlocked() > capacity_) {
      if (cache_.empty()) break;
      destroy_now.push_back(std::move(cache_.back()));
      cache_.pop_back();
    }
    lock.Unlock();

    for (auto&& extra : destroy_now) {
      destroy_(std::move(extra));
    }
  }

  Borrowed<T> BorrowOne() {
    auto lock = mutex_.Lock();
    // NB: can't assert that we haven't exceeded capacity because we might
    // have multiple still-borrowed instances after a capacity change

    ++borrowed_count_;

    if (cache_.empty()) {
      // create a new value and borrow that
      lock.Unlock();
      return Borrow(construct_());
    }

    // pop a value from the cache
    auto cached = std::move(cache_.back());
    cache_.pop_back();
    return Borrow(std::move(cached));
  }

 private:
  BorrowSet() = default;

  int GetInstanceCountUnlocked() const {
    return borrowed_count_ + static_cast<int>(cache_.size());
  }

  Borrowed<T> Borrow(T value) {
    auto self = this->shared_from_this();
    return Borrowed<T>(std::move(value),
                       [self](T returned) { self->ReturnOne(std::move(returned)); });
  }

  void ReturnOne(T returned) {
    auto lock = mutex_.Lock();

    --borrowed_count_;

    if (GetInstanceCountUnlocked() < capacity_) {
      return cache_.push_back(std::move(returned));
    }

    // returning an instance after capacity has been decreased;
    // just destroy this extra now
    lock.Unlock();
    destroy_(std::move(returned));
  }

  util::Mutex mutex_;
  std::function<T()> construct_;
  std::function<void(T&&)> destroy_;
  std::vector<T> cache_;
  int borrowed_count_ = 0, capacity_ = 0;
};

}  // namespace internal
}  // namespace arrow
