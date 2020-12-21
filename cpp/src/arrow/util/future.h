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
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace detail {

struct Empty {
  static Result<Empty> ToResult(Status s) {
    if (ARROW_PREDICT_TRUE(s.ok())) {
      return Empty{};
    }
    return s;
  }
};

template <typename>
struct is_future : std::false_type {};

template <typename T>
struct is_future<Future<T>> : std::true_type {};

template <typename Signature>
using result_of_t = typename std::result_of<Signature>::type;

constexpr struct ContinueFuture {
  template <typename Return>
  struct ForReturnImpl;

  template <typename Return>
  using ForReturn = typename ForReturnImpl<Return>::type;

  template <typename Signature>
  using ForSignature = ForReturn<result_of_t<Signature>>;

  template <typename ContinueFunc, typename... Args,
            typename ContinueResult = result_of_t<ContinueFunc && (Args && ...)>,
            typename NextFuture = ForReturn<ContinueResult>>
  typename std::enable_if<std::is_void<ContinueResult>::value>::type operator()(
      NextFuture next, ContinueFunc&& f, Args&&... a) const {
    std::forward<ContinueFunc>(f)(std::forward<Args>(a)...);
    next.MarkFinished();
  }

  template <typename ContinueFunc, typename... Args,
            typename ContinueResult = result_of_t<ContinueFunc && (Args && ...)>,
            typename NextFuture = ForReturn<ContinueResult>>
  typename std::enable_if<!std::is_void<ContinueResult>::value &&
                          !is_future<ContinueResult>::value>::type
  operator()(NextFuture next, ContinueFunc&& f, Args&&... a) const {
    next.MarkFinished(std::forward<ContinueFunc>(f)(std::forward<Args>(a)...));
  }

  template <typename ContinueFunc, typename... Args,
            typename ContinueResult = result_of_t<ContinueFunc && (Args && ...)>,
            typename NextFuture = ForReturn<ContinueResult>>
  typename std::enable_if<is_future<ContinueResult>::value>::type operator()(
      NextFuture next, ContinueFunc&& f, Args&&... a) const {
    ContinueResult signal_to_complete_next =
        std::forward<ContinueFunc>(f)(std::forward<Args>(a)...);

    struct MarkNextFinished {
      void operator()(const Result<typename ContinueResult::ValueType>& result) && {
        next.MarkFinished(result);
      }
      NextFuture next;
    };

    signal_to_complete_next.AddCallback(MarkNextFinished{std::move(next)});
  }
} Continue;

template <>
struct ContinueFuture::ForReturnImpl<void> {
  using type = Future<>;
};

template <>
struct ContinueFuture::ForReturnImpl<Status> {
  using type = Future<>;
};

template <typename R>
struct ContinueFuture::ForReturnImpl {
  using type = Future<R>;
};

template <typename T>
struct ContinueFuture::ForReturnImpl<Result<T>> {
  using type = Future<T>;
};

template <typename T>
struct ContinueFuture::ForReturnImpl<Future<T>> {
  using type = Future<T>;
};

}  // namespace detail

/// A Future's execution or completion status
enum class FutureState : int8_t { PENDING, SUCCESS, FAILURE };

inline bool IsFutureFinished(FutureState state) { return state != FutureState::PENDING; }

// Untyped private implementation
class ARROW_EXPORT FutureImpl {
 public:
  FutureImpl();
  virtual ~FutureImpl() = default;

  FutureState state() { return state_.load(); }

  static std::unique_ptr<FutureImpl> Make();
  static std::unique_ptr<FutureImpl> MakeFinished(FutureState state);

  // Future API
  void MarkFinished();
  void MarkFailed();
  void Wait();
  bool Wait(double seconds);

  using Callback = internal::FnOnce<void()>;
  void AddCallback(Callback callback);

  // Waiter API
  inline FutureState SetWaiter(FutureWaiter* w, int future_num);
  inline void RemoveWaiter(FutureWaiter* w);

  std::atomic<FutureState> state_{FutureState::PENDING};

  // Type erased storage for arbitrary results
  // XXX small objects could be stored inline instead of boxed in a pointer
  using Storage = std::unique_ptr<void, void (*)(void*)>;
  Storage result_{NULLPTR, NULLPTR};

  std::vector<Callback> callbacks_;
};

// An object that waits on multiple futures at once.  Only one waiter
// can be registered for each future at any time.
class ARROW_EXPORT FutureWaiter {
 public:
  enum Kind : int8_t { ANY, ALL, ALL_OR_FIRST_FAILED, ITERATE };

  static constexpr double kInfinity = HUGE_VAL;

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
template <typename T>
class ARROW_MUST_USE_TYPE Future {
 public:
  using ValueType = T;

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
  template <typename E = ValueType, typename = typename std::enable_if<
                                        std::is_same<E, detail::Empty>::value>::type>
  void MarkFinished(Status s = Status::OK()) {
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
  template <typename E = ValueType, typename = typename std::enable_if<
                                        std::is_same<E, detail::Empty>::value>::type>
  static Future<> MakeFinished(Status s = Status::OK()) {
    return MakeFinished(E::ToResult(std::move(s)));
  }

  /// \brief Consumer API: Register a callback to run when this future completes
  ///
  /// The callback should receive the result of the future (const Result<T>&)
  /// For a void or statusy future this should be
  /// (const Result<detail::Empty>& result)
  ///
  /// There is no guarantee to the order in which callbacks will run.  In
  /// particular, callbacks added while the future is being marked complete
  /// may be executed immediately, ahead of, or even the same time as, other
  /// callbacks that have been previously added.
  ///
  /// WARNING: callbacks may hold arbitrary references, including cyclic references.
  /// Since callbacks will only be destroyed after they are invoked, this can lead to
  /// memory leaks if a Future is never marked finished (abandoned):
  ///
  /// {
  ///     auto fut = Future<>::Make();
  ///     fut.AddCallback([fut](...) {});
  /// }
  ///
  /// In this example `fut` falls out of scope but is not destroyed because it holds a
  /// cyclic reference to itself through the callback.
  template <typename OnComplete>
  void AddCallback(OnComplete&& on_complete) const {
    struct Callback {
      void operator()() && {
        auto self = weak_self.get();
        std::move(on_complete)(*self.GetResult());
      }

      WeakFuture<T> weak_self;
      OnComplete on_complete;
    };

    // We know impl_ will not be dangling when invoking callbacks because at least one
    // thread will be waiting for MarkFinished to return. Thus it's safe to keep a
    // weak reference to impl_ here
    impl_->AddCallback(
        Callback{WeakFuture<T>(*this), std::forward<OnComplete>(on_complete)});
  }

  /// \brief Consumer API: Register a continuation to run when this future completes
  ///
  /// The continuation will run in the same thread that called MarkFinished (whatever
  /// callback is registered with this function will run before MarkFinished returns).
  /// Avoid long-running callbacks in favor of submitting a task to an Executor and
  /// returning the future.
  ///
  /// Two callbacks are supported:
  /// - OnSuccess, called against the result (const ValueType&) on successul completion.
  /// - OnFailure, called against the error (const Status&) on failed completion.
  ///
  /// Then() returns a Future whose ValueType is derived from the return type of the
  /// callbacks. If a callback returns:
  /// - void, a Future<> will be returned which will completes successully as soon
  ///   as the callback runs.
  /// - Status, a Future<> will be returned which will complete with the returned Status
  ///   as soon as the callback runs.
  /// - V or Result<V>, a Future<V> will be returned which will complete with the result
  ///   of invoking the callback as soon as the callback runs.
  /// - Future<V>, a Future<V> will be returned which will be marked complete when the
  ///   future returned by the callback completes (and will complete with the same
  ///   result).
  ///
  /// The continued Future type must be the same for both callbacks.
  ///
  /// Note that OnFailure can swallow errors, allowing continued Futures to successully
  /// complete even if this Future fails.
  ///
  /// If this future is already completed then the callback will be run immediately
  /// and the returned future may already be marked complete.
  ///
  /// See AddCallback for general considerations when writing callbacks.
  template <typename OnSuccess, typename OnFailure,
            typename ContinuedFuture =
                detail::ContinueFuture::ForSignature<OnSuccess && (const T&)>>
  ContinuedFuture Then(OnSuccess&& on_success, OnFailure&& on_failure) const {
    static_assert(
        std::is_same<detail::ContinueFuture::ForSignature<OnFailure && (const Status&)>,
                     ContinuedFuture>::value,
        "OnSuccess and OnFailure must continue with the same future type");

    auto next = ContinuedFuture::Make();

    struct Callback {
      void operator()(const Result<T>& result) && {
        if (ARROW_PREDICT_TRUE(result.ok())) {
          // move on_failure to a(n immediately destroyed) temporary to free its resources
          ARROW_UNUSED(OnFailure(std::move(on_failure)));
          detail::Continue(std::move(next), std::move(on_success), result.ValueOrDie());
        } else {
          ARROW_UNUSED(OnSuccess(std::move(on_success)));
          detail::Continue(std::move(next), std::move(on_failure), result.status());
        }
      }

      OnSuccess on_success;
      OnFailure on_failure;
      ContinuedFuture next;
    };

    AddCallback(Callback{std::forward<OnSuccess>(on_success),
                         std::forward<OnFailure>(on_failure), next});

    return next;
  }

  /// \brief Overload without OnFailure. Failures will be passed through unchanged.
  template <typename OnSuccess,
            typename ContinuedFuture =
                detail::ContinueFuture::ForSignature<OnSuccess && (const T&)>>
  ContinuedFuture Then(OnSuccess&& on_success) const {
    return Then(std::forward<OnSuccess>(on_success), [](const Status& s) {
      return Result<typename ContinuedFuture::ValueType>(s);
    });
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

  explicit Future(std::shared_ptr<FutureImpl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<FutureImpl> impl_;

  friend class FutureWaiter;
  friend struct detail::ContinueFuture;

  template <typename U>
  friend class Future;
  friend class WeakFuture<T>;

  FRIEND_TEST(FutureRefTest, ChainRemoved);
  FRIEND_TEST(FutureRefTest, TailRemoved);
  FRIEND_TEST(FutureRefTest, HeadRemoved);
};

template <typename T>
class WeakFuture {
 public:
  explicit WeakFuture(const Future<T>& future) : impl_(future.impl_) {}

  Future<T> get() { return Future<T>{impl_.lock()}; }

 private:
  std::weak_ptr<FutureImpl> impl_;
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
