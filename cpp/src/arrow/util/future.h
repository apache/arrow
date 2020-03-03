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
#include <cmath>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

enum class FutureState : int8_t { PENDING, SUCCESS, FAILURE };

inline bool IsFutureFinished(FutureState state) { return state != FutureState::PENDING; }

// ---------------------------------------------------------------------
// Type-erased helpers

class FutureWaiter;
template <typename T>
class Future;

class ARROW_EXPORT FutureImpl {
 public:
  static constexpr double kInfinity = HUGE_VAL;

  virtual ~FutureImpl() = default;

  FutureState state() { return state_.load(); }

  static std::unique_ptr<FutureImpl> Make();

 protected:
  FutureImpl();
  ARROW_DISALLOW_COPY_AND_ASSIGN(FutureImpl);

  // Future API
  void MarkFinished();
  void MarkFailed();
  void Wait();
  bool Wait(double seconds);

  // Waiter API
  inline FutureState SetWaiter(FutureWaiter* w, int future_num);
  inline void RemoveWaiter(FutureWaiter* w);

  std::atomic<FutureState> state_;

  template <typename T>
  friend class Future;
  template <typename T>
  friend class FutureStorage;
  friend class FutureWaiter;
  friend class FutureWaiterImpl;
};

// An object that waits on multiple futures at once.  Only one waiter
// can be registered for each future at any time.
class ARROW_EXPORT FutureWaiter {
 public:
  enum Kind : int8_t { ANY, ALL, ALL_OR_FIRST_FAILED, ITERATE };

  static constexpr double kInfinity = FutureImpl::kInfinity;

  static std::unique_ptr<FutureWaiter> Make(Kind kind, std::vector<FutureImpl*> futures);

  template <typename FutureType>
  static std::unique_ptr<FutureWaiter> Make(Kind kind,
                                            const std::vector<FutureType>& futures) {
    return Make(kind, ExtractFutures(futures));
  }

  virtual ~FutureWaiter();

  bool Wait(double seconds = kInfinity);
  int WaitAndFetchOne();

  std::vector<int> MoveFinishedFutures();

 protected:
  // Extract FutureImpls from Futures
  template <typename FutureType,
            typename Enable = std::enable_if<!std::is_pointer<FutureType>::value>>
  static std::vector<FutureImpl*> ExtractFutures(const std::vector<FutureType>& futures) {
    std::vector<FutureImpl*> base_futures(futures.size());
    for (int i = 0; i < static_cast<int>(futures.size()); ++i) {
      base_futures[i] = futures[i].impl_;
    }
    return base_futures;
  }

  // Extract FutureImpls from Future pointers
  template <typename FutureType>
  static std::vector<FutureImpl*> ExtractFutures(
      const std::vector<FutureType*>& futures) {
    std::vector<FutureImpl*> base_futures(futures.size());
    for (int i = 0; i < static_cast<int>(futures.size()); ++i) {
      base_futures[i] = futures[i]->impl_;
    }
    return base_futures;
  }

  FutureWaiter();
  ARROW_DISALLOW_COPY_AND_ASSIGN(FutureWaiter);

  inline void MarkFutureFinishedUnlocked(int future_num, FutureState state);

  friend class FutureImpl;
  friend class ConcreteFutureImpl;
};

// ---------------------------------------------------------------------
// An intermediate class for storing Future results

class FutureStorageBase {
 public:
  FutureStorageBase() : impl_(FutureImpl::Make()) {}

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(FutureStorageBase);
  std::unique_ptr<FutureImpl> impl_;

  template <typename T>
  friend class Future;
};

template <typename T>
class FutureStorage : public FutureStorageBase {
 public:
  static constexpr bool HasValue = true;

  Status status() const { return result_.status(); }

  template <typename U>
  void MarkFinished(U&& value) {
    result_ = std::forward<U>(value);
    if (ARROW_PREDICT_TRUE(result_.ok())) {
      impl_->MarkFinished();
    } else {
      impl_->MarkFailed();
    }
  }

  template <typename Func>
  void ExecuteAndMarkFinished(Func&& func) {
    MarkFinished(func());
  }

 protected:
  Result<T> result_;
  friend class Future<T>;
};

// A Future<void> just stores a Status (always ok for now, but that could change
// if we implement cancellation).
template <>
class FutureStorage<void> : public FutureStorageBase {
 public:
  static constexpr bool HasValue = false;

  Status status() const { return status_; }

  void MarkFinished(Status st = Status::OK()) {
    status_ = std::move(st);
    impl_->MarkFinished();
  }

  template <typename Func>
  void ExecuteAndMarkFinished(Func&& func) {
    func();
    MarkFinished();
  }

 protected:
  Status status_;
};

// A Future<Status> just stores a Status.
template <>
class FutureStorage<Status> : public FutureStorageBase {
 public:
  static constexpr bool HasValue = false;

  Status status() const { return status_; }

  void MarkFinished(Status st) {
    status_ = std::move(st);
    if (ARROW_PREDICT_TRUE(status_.ok())) {
      impl_->MarkFinished();
    } else {
      impl_->MarkFailed();
    }
  }

  template <typename Func>
  void ExecuteAndMarkFinished(Func&& func) {
    MarkFinished(func());
  }

 protected:
  Status status_;
};

// ---------------------------------------------------------------------
// Public API

template <typename T>
class Future {
  static constexpr bool HasValue = FutureStorage<T>::HasValue;
  template <typename U>
  using EnableResult = typename std::enable_if<HasValue, Result<U>>::type;

 public:
  static constexpr double kInfinity = FutureImpl::kInfinity;

  // Default constructor creates an invalid Future.  Use Future::Make()
  // for a valid Future.  This constructor is mostly for the convenience
  // of being able to presize a vector of Futures.
  Future() : impl_(NULLPTR) {}

  // Consumer API

  bool is_valid() const { return storage_ != NULLPTR; }

  FutureState state() const {
    CheckValid();
    return impl_->state();
  }

  template <typename U = T>
  const Result<T>& result(EnableResult<U>* = NULLPTR) const& {
    CheckValid();
    Wait();
    return storage_->result_;
  }

  template <typename U = T>
  Result<T> result(EnableResult<U>* = NULLPTR) && {
    CheckValid();
    Wait();
    return std::move(storage_->result_);
  }

  Status status() const {
    CheckValid();
    Wait();
    return storage_->status();
  }

  void Wait() const {
    CheckValid();
    if (!IsFutureFinished(impl_->state())) {
      impl_->Wait();
    }
  }

  bool Wait(double seconds) const {
    CheckValid();
    if (IsFutureFinished(impl_->state())) {
      return true;
    }
    return impl_->Wait(seconds);
  }

  // Producer API

  template <typename Func>
  void ExecuteAndMarkFinished(Func&& func) {
    storage_->ExecuteAndMarkFinished(std::forward<Func>(func));
  }

  template <typename... Args>
  void MarkFinished(Args&&... args) {
    storage_->MarkFinished(std::forward<Args>(args)...);
  }

  static Future Make() {
    Future fut;
    fut.storage_ = std::make_shared<FutureStorage<T>>();
    fut.impl_ = fut.storage_->impl_.get();
    return fut;
  }

 protected:
  void CheckValid() const {
#ifndef NDEBUG
    if (!is_valid()) {
      Status::Invalid("Invalid Future (default-initialized?)").Abort();
    }
#endif
  }

  std::shared_ptr<FutureStorage<T>> storage_;
  FutureImpl* impl_;

  friend class FutureWaiter;
};

template <typename T>
inline bool WaitForAll(const std::vector<Future<T>>& futures,
                       double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ALL, futures);
  return waiter->Wait(seconds);
}

template <typename T>
inline bool WaitForAll(const std::vector<Future<T>*>& futures,
                       double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ALL, futures);
  return waiter->Wait(seconds);
}

template <typename T>
inline std::vector<int> WaitForAny(const std::vector<Future<T>>& futures,
                                   double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ANY, futures);
  waiter->Wait(seconds);
  return waiter->MoveFinishedFutures();
}

template <typename T>
inline std::vector<int> WaitForAny(const std::vector<Future<T>*>& futures,
                                   double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ANY, futures);
  waiter->Wait(seconds);
  return waiter->MoveFinishedFutures();
}

}  // namespace arrow
