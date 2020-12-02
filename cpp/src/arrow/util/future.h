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

/// A Future's execution or completion status
enum class FutureState : int8_t { PENDING, SUCCESS, FAILURE };

inline bool IsFutureFinished(FutureState state) { return state != FutureState::PENDING; }

// ---------------------------------------------------------------------
// Type-erased helpers

class FutureWaiter;

class ARROW_EXPORT FutureImpl {
 public:
  static constexpr double kInfinity = HUGE_VAL;

  virtual ~FutureImpl() = default;

  FutureState state() { return state_.load(); }

  static std::unique_ptr<FutureImpl> Make();
  static std::unique_ptr<FutureImpl> MakeFinished(FutureState state);

 protected:
  FutureImpl();

  // Future API
  void MarkFinished();
  void MarkFailed();
  void Wait();
  bool Wait(double seconds);

  // Waiter API
  inline FutureState SetWaiter(FutureWaiter* w, int future_num);
  inline void RemoveWaiter(FutureWaiter* w);

  std::atomic<FutureState> state_{FutureState::PENDING};

  // Type erased storage for arbitrary results
  // XXX small objects could be stored alongside state_ instead of boxed in a pointer
  using Storage = std::unique_ptr<void, void (*)(void*)>;
  Storage result_{NULLPTR, NULLPTR};

  template <typename T>
  friend class Future;
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
      base_futures[i] = futures[i].impl_.get();
    }
    return base_futures;
  }

  // Extract FutureImpls from Future pointers
  template <typename FutureType>
  static std::vector<FutureImpl*> ExtractFutures(
      const std::vector<FutureType*>& futures) {
    std::vector<FutureImpl*> base_futures(futures.size());
    for (int i = 0; i < static_cast<int>(futures.size()); ++i) {
      base_futures[i] = futures[i]->impl_.get();
    }
    return base_futures;
  }

  FutureWaiter();
  ARROW_DISALLOW_COPY_AND_ASSIGN(FutureWaiter);

  inline void MarkFutureFinishedUnlocked(int future_num, FutureState state);

  friend class FutureImpl;
  friend class ConcreteFutureImpl;
};

namespace detail {

struct Empty {
  static Result<Empty> ToResult(Status s) {
    if (ARROW_PREDICT_TRUE(s.ok())) {
      return Empty{};
    }
    return s;
  }

  template <typename T>
  using EnableIfSame = typename std::enable_if<std::is_same<Empty, T>::value>::type;
};

}  // namespace detail

// ---------------------------------------------------------------------
// Public API

/// \brief EXPERIMENTAL A std::future-like class with more functionality.
///
/// A Future represents the results of a past or future computation.
/// The Future API has two sides: a producer side and a consumer side.
///
/// The producer API allows creating a Future and setting its result or
/// status, possibly after running a computation function.
///
/// The consumer API allows querying a Future's current state, wait for it
/// to complete, or wait on multiple Futures at once (using WaitForAll,
/// WaitForAny or AsCompletedIterator).
template <typename T = detail::Empty>
class Future {
 public:
  using ValueType = T;
  static constexpr double kInfinity = FutureImpl::kInfinity;

  // The default constructor creates an invalid Future.  Use Future::Make()
  // for a valid Future.  This constructor is mostly for the convenience
  // of being able to presize a vector of Futures.
  Future() = default;

  // Consumer API

  bool is_valid() const { return impl_ != NULLPTR; }

  /// \brief Return the Future's current state
  ///
  /// A return value of PENDING is only indicative, as the Future can complete
  /// concurrently.  A return value of FAILURE or SUCCESS is definitive, though.
  FutureState state() const {
    CheckValid();
    return impl_->state();
  }

  /// \brief Whether the Future is finished
  ///
  /// A false return value is only indicative, as the Future can complete
  /// concurrently.  A true return value is definitive, though.
  bool is_finished() const {
    CheckValid();
    return IsFutureFinished(impl_->state());
  }

  /// \brief Wait for the Future to complete and return its Result
  const Result<ValueType>& result() const& {
    Wait();
    return *GetResult();
  }
  Result<ValueType>&& result() && {
    Wait();
    return std::move(*GetResult());
  }

  /// \brief Wait for the Future to complete and return its Status
  const Status& status() const { return result().status(); }

  /// \brief Future<T> is convertible to Future<>, which views only the
  /// Status of the original. Marking the returned Future Finished is not supported.
  explicit operator Future<>() const {
    Future<> status_future;
    status_future.impl_ = impl_;
    return status_future;
  }

  /// \brief Wait for the Future to complete
  void Wait() const {
    CheckValid();
    if (!IsFutureFinished(impl_->state())) {
      impl_->Wait();
    }
  }

  /// \brief Wait for the Future to complete, or for the timeout to expire
  ///
  /// `true` is returned if the Future completed, `false` if the timeout expired.
  /// Note a `false` value is only indicative, as the Future can complete
  /// concurrently.
  bool Wait(double seconds) const {
    CheckValid();
    if (IsFutureFinished(impl_->state())) {
      return true;
    }
    return impl_->Wait(seconds);
  }

  // Producer API

  /// \brief Producer API: mark Future finished
  ///
  /// The Future's result is set to `res`.
  void MarkFinished(Result<ValueType> res) { DoMarkFinished(std::move(res)); }

  /// \brief Mark a Future<> completed with the provided Status.
  template <typename E = ValueType>
  detail::Empty::EnableIfSame<E> MarkFinished(Status s = Status::OK()) {
    return DoMarkFinished(E::ToResult(std::move(s)));
  }

  /// \brief Producer API: instantiate a valid Future
  ///
  /// The Future's state is initialized with PENDING.
  static Future Make() {
    Future fut;
    fut.impl_ = FutureImpl::Make();
    return fut;
  }

  /// \brief Producer API: instantiate a finished Future
  static Future MakeFinished(Result<ValueType> res) {
    Future fut;
    if (ARROW_PREDICT_TRUE(res.ok())) {
      fut.impl_ = FutureImpl::MakeFinished(FutureState::SUCCESS);
    } else {
      fut.impl_ = FutureImpl::MakeFinished(FutureState::FAILURE);
    }
    fut.SetResult(std::move(res));
    return fut;
  }

  /// \brief Make a finished Future<> with the provided Status.
  template <typename E = ValueType, typename = detail::Empty::EnableIfSame<E>>
  static Future<> MakeFinished(Status s = Status::OK()) {
    return MakeFinished(E::ToResult(std::move(s)));
  }

 protected:
  Result<ValueType>* GetResult() const {
    return static_cast<Result<ValueType>*>(impl_->result_.get());
  }

  void SetResult(Result<ValueType> res) {
    impl_->result_ = {new Result<ValueType>(std::move(res)),
                      [](void* p) { delete static_cast<Result<ValueType>*>(p); }};
  }

  void DoMarkFinished(Result<ValueType> res) {
    SetResult(std::move(res));

    if (ARROW_PREDICT_TRUE(GetResult()->ok())) {
      impl_->MarkFinished();
    } else {
      impl_->MarkFailed();
    }
  }

  void CheckValid() const {
#ifndef NDEBUG
    if (!is_valid()) {
      Status::Invalid("Invalid Future (default-initialized?)").Abort();
    }
#endif
  }

  std::shared_ptr<FutureImpl> impl_;

  friend class FutureWaiter;

  template <typename U>
  friend class Future;
};

/// If a Result<Future> holds an error instead of a Future, construct a finished Future
/// holding that error.
template <typename T>
static Future<T> DeferNotOk(Result<Future<T>> maybe_future) {
  if (ARROW_PREDICT_FALSE(!maybe_future.ok())) {
    return Future<T>::MakeFinished(std::move(maybe_future).status());
  }
  return std::move(maybe_future).MoveValueUnsafe();
}

/// \brief Wait for all the futures to end, or for the given timeout to expire.
///
/// `true` is returned if all the futures completed before the timeout was reached,
/// `false` otherwise.
template <typename T>
inline bool WaitForAll(const std::vector<Future<T>>& futures,
                       double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ALL, futures);
  return waiter->Wait(seconds);
}

/// \brief Wait for all the futures to end, or for the given timeout to expire.
///
/// `true` is returned if all the futures completed before the timeout was reached,
/// `false` otherwise.
template <typename T>
inline bool WaitForAll(const std::vector<Future<T>*>& futures,
                       double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ALL, futures);
  return waiter->Wait(seconds);
}

/// \brief Wait for one of the futures to end, or for the given timeout to expire.
///
/// The indices of all completed futures are returned.  Note that some futures
/// may not be in the returned set, but still complete concurrently.
template <typename T>
inline std::vector<int> WaitForAny(const std::vector<Future<T>>& futures,
                                   double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ANY, futures);
  waiter->Wait(seconds);
  return waiter->MoveFinishedFutures();
}

/// \brief Wait for one of the futures to end, or for the given timeout to expire.
///
/// The indices of all completed futures are returned.  Note that some futures
/// may not be in the returned set, but still complete concurrently.
template <typename T>
inline std::vector<int> WaitForAny(const std::vector<Future<T>*>& futures,
                                   double seconds = FutureWaiter::kInfinity) {
  auto waiter = FutureWaiter::Make(FutureWaiter::ANY, futures);
  waiter->Wait(seconds);
  return waiter->MoveFinishedFutures();
}

}  // namespace arrow
