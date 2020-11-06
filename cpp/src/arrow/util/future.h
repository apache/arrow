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
#include "arrow/util/macros.h"
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

  template <typename T>
  using EnableIfSame = typename std::enable_if<std::is_same<Empty, T>::value>::type;
};

}  // namespace detail
class FutureWaiter;
template <typename T = detail::Empty>
class Future;
template <typename T>
class FutureStorage;

/// A Future's execution or completion status
enum class FutureState : int8_t { PENDING, SUCCESS, FAILURE };

using Callback = std::function<void()>;

inline bool IsFutureFinished(FutureState state) { return state != FutureState::PENDING; }

template <typename>
struct is_future : std::false_type {};

template <typename T>
struct is_future<Future<T>> : std::true_type {};

template <typename Signature>
using result_of_t = typename std::result_of<Signature>::type;

namespace detail {

template <typename ContinuationReturn>
struct ContinuedFutureImpl;

template <>
struct ContinuedFutureImpl<void> {
  using type = Future<>;
};

template <>
struct ContinuedFutureImpl<Status> {
  using type = Future<>;
};

template <typename R>
struct ContinuedFutureImpl {
  using type = Future<R>;
};

template <typename T>
struct ContinuedFutureImpl<Result<T>> {
  using type = Future<T>;
};

template <typename T>
struct ContinuedFutureImpl<Future<T>> {
  using type = Future<T>;
};

template <typename Signature>
using ContinuedFuture = typename ContinuedFutureImpl<result_of_t<Signature>>::type;

}  // namespace detail

// ---------------------------------------------------------------------
// Type-erased helpers

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
  void AddCallback(Callback callback);

  // Waiter API
  inline FutureState SetWaiter(FutureWaiter* w, int future_num);
  inline void RemoveWaiter(FutureWaiter* w);

  std::atomic<FutureState> state_{FutureState::PENDING};

  // Type erased storage for arbitrary results
  // XXX small objects could be stored alongside state_ instead of boxed in a pointer
  using Storage = std::unique_ptr<void, void (*)(void*)>;
  Storage result_{NULLPTR, NULLPTR};

  template <typename Finished, typename ContinuedFuture, typename Continuation,
            typename Return = result_of_t<Continuation && (const Result<Finished>&)>>
  typename std::enable_if<std::is_void<Return>::value>::type Continue(
      ContinuedFuture next, Continuation&& continuation, bool run_on_failure) {
    static_assert(std::is_same<ContinuedFuture, Future<>>::value, "");
    const auto& this_result = *static_cast<Result<Finished>*>(result_.get());

    if (this_result.ok() || run_on_failure) {
      std::forward<Continuation>(continuation)(this_result);
      next.MarkFinished(Status::OK());
    } else {
      next.MarkFinished(this_result.status());
    }
  }

  template <typename Finished, typename ContinuedFuture, typename Continuation,
            typename Return = result_of_t<Continuation && (const Result<Finished>&)>>
  typename std::enable_if<std::is_same<Return, Status>::value>::type Continue(
      ContinuedFuture next, Continuation&& continuation, bool run_on_failure) {
    static_assert(std::is_same<ContinuedFuture, Future<>>::value, "");
    const auto& this_result = *static_cast<Result<Finished>*>(result_.get());

    if (this_result.ok() || run_on_failure) {
      Status next_status = std::forward<Continuation>(continuation)(this_result);
      next.MarkFinished(std::move(next_status));
    } else {
      next.MarkFinished(this_result.status());
    }
  }

  template <typename Finished, typename ContinuedFuture, typename Continuation,
            typename Return = result_of_t<Continuation && (const Result<Finished>&)>>
  typename std::enable_if<!std::is_void<Return>::value &&
                          !std::is_same<Return, Status>::value &&
                          !is_future<Return>::value>::type
  Continue(ContinuedFuture next, Continuation&& continuation, bool run_on_failure) {
    static_assert(!std::is_same<ContinuedFuture, Future<>>::value, "");
    const auto& this_result = *static_cast<Result<Finished>*>(result_.get());

    if (this_result.ok() || run_on_failure) {
      Result<typename ContinuedFuture::ValueType> next_result =
          std::forward<Continuation>(continuation)(this_result);
      next.MarkFinished(std::move(next_result));
    } else {
      next.MarkFinished(this_result.status());
    }
  }

  template <typename Finished, typename ContinuedFuture, typename Continuation,
            typename Return = result_of_t<Continuation && (const Result<Finished>&)>>
  typename std::enable_if<is_future<Return>::value>::type Continue(
      ContinuedFuture next, Continuation&& continuation, bool run_on_failure) {
    const auto& this_result = *static_cast<Result<Finished>*>(result_.get());

    if (this_result.ok() || run_on_failure) {
      auto next_future = std::forward<Continuation>(continuation)(this_result);
      next_future.AddCallback(
          [next_future, next]() mutable { next.MarkFinished(next_future.result()); });
    } else {
      next.MarkFinished(this_result.status());
    }
  }

  std::vector<Callback> callbacks_;

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

  /// \brief Wait for the Future to complete and return its Result
  const Result<ValueType>& result() const& {
    Wait();
    return *GetResult();
  }
  Result<ValueType>&& result() && {
    Wait();
    return std::move(*GetResult());
  }

  /// \brief In general, Then should be preferred over AddCallback.
  void AddCallback(Callback callback) const {
    // TODO: Get rid of this method or at least make protected somehow?
    impl_->AddCallback(callback);
  }

  /// \brief Wait for the Future to complete and return its Status
  const Status& status() const { return result().status(); }

  /// \brief Future<T> is convertible to Future<>, which views only the
  /// Status of the original. Marking this Future Finished is not supported.
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

  /// \brief Mark a Future<> or Future<> completed with the provided Status.
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

  /// \brief Make a finished Future<> or Future<> with the provided Status.
  template <typename E = ValueType, typename = detail::Empty::EnableIfSame<E>>
  static Future<> MakeFinished(Status s = Status::OK()) {
    return MakeFinished(E::ToResult(std::move(s)));
  }

  /// \brief Consumer API: Register a continuation to run when this future completes
  ///
  /// The continuation will run in the same thread that called MarkFinished (whatever
  /// callback is registered with this function will run before MarkFinished returns).
  /// If your callback is lengthy then it is generally best to spawn a task so that the
  /// callback can immediately return a future.
  ///
  /// The callback should receive (const Result<T> &).
  ///
  /// This method returns a future which will be completed after the callback (and
  /// potentially the callback task) have finished.
  ///
  /// If the callback returns:
  /// - void, a Future<> will be produced which will complete successully as soon
  ///   as the callback runs.
  /// - Status, a Future<> will be produced which will complete with the returned Status
  ///   as soon as the callback runs.
  /// - V or Result<V>, a Future<V> will be produced which will complete with the result
  ///   of the callback as soon as the callback runs.
  /// - Future<V>, a Future<V> will be produced which will be marked complete when the
  ///   future returned by the callback completes (and will complete with the same
  ///   result).
  ///
  /// If this future fails (results in a non-ok status) then by default the callback will
  /// not run.  Instead the future that this method returns will be marked failed with the
  /// same status (the failure will propagate).  This is analagous to throwing an
  /// exception in sequential code, the failure propagates upwards, skipping the following
  /// code.
  ///
  /// However, if run_on_failure is set to true then the callback will be run even if this
  /// future fails.  The callback will receive the failed status (or failed result) and
  /// can attempt to salvage or continue despite the failure.  As long as the callback
  /// doesn't return a failed status (or failed future) then the final future (the one
  /// returned by this method) will be marked with the successful result of the callback.
  /// This is analagous to catching an exception in sequential code.
  ///
  /// If this future is already completed then the callback will be run immediately
  /// (before this method returns) and the returned future may already be marked complete
  /// (it will definitely be marked complete if the callback returns a non-future or a
  /// completed future).
  ///
  /// Care should be taken when creating continuation callbacks.  The callback may not run
  /// for some time.  If the callback is a lambda function then capture by reference is
  /// generally not advised as references are probably not going to still be in scope when
  /// the callback runs.  Capture by value or passing in a shared_ptr should generally be
  /// safe.  Capturing this may or may not be safe, you will need to ensure that the this
  /// object is going to still exist when the callback is run.  This can be tricky because
  /// even though the "semantic instance" may still exist if the instance was moved then
  /// it will no longer exist.
  ///
  /// Example:
  ///
  /// // This approach is NOT SAFE.  Even though the task objects are kept around until
  /// after they are finished it is not the SAME task object.
  /// // The task object is created, the callback lambda is created, a copy of this is
  /// copied into the callback, and then the task object is
  /// // immediately moved into the vector.  This move creates a new task object (with a
  /// different this pointer).  The this pointer that exists
  /// // when the callback actually runs will be pointing to an invalid MyTask object.
  /// class MyTask {
  ///   public:
  ///     MyTask(std::shared_ptr<MyItem> item, Future<Block> block_to_process) :
  ///     item_(item) {
  ///       task_future_ = block_to_process.Then([this] {return Process();});
  ///     }
  ///     ARROW_DEFAULT_MOVE_AND_ASSIGN(MyTask);
  ///   private:
  ///     Status Process();
  ///     std::shared_ptr<MyItem> item_;
  ///     Future<> task_future_;
  /// };
  ///
  /// std::vector<MyTask> tasks;
  /// for (auto && block_future : block_futures) {
  ///   for (auto && item : items) {
  ///     tasks.push_back(MyTask(item, block_future));
  ///   }
  /// }
  /// AwaitAllTasks(tasks);
  struct LessPrefer {};
  struct Prefer : LessPrefer {};

  template <typename Continuation>
  detail::ContinuedFuture<Continuation && (const Result<T>&)> ThenImpl(
      Continuation&& continuation, bool run_on_failure, Prefer) const {
    auto future = detail::ContinuedFuture < Continuation && (const Result<T>&) > ::Make();

    // We know impl_ will be valid when invoking callbacks because at least one thread
    // will be waiting for MarkFinished to return. Thus it's safe to keep a non-owning
    // reference to impl_ here (but *not* to `this`!)
    FutureImpl* impl = impl_.get();
    impl_->AddCallback([impl, future, continuation, run_on_failure]() mutable {
      impl->Continue<T>(std::move(future), std::move(continuation), run_on_failure);
    });

    return future;
  }

  template <typename Continuation>
  detail::ContinuedFuture<Continuation && (const Status&)> ThenImpl(
      Continuation&& continuation, bool run_on_failure, LessPrefer) const {
    return ThenImpl(
        [continuation](const Result<T>& result) mutable {
          return std::move(continuation)(result.status());
        },
        run_on_failure, Prefer{});
  }

  template <typename Continuation>
  auto Then(Continuation&& continuation, bool run_on_failure = false) const
      -> decltype(ThenImpl(std::forward<Continuation>(continuation), run_on_failure,
                           Prefer{})) {
    return ThenImpl(std::forward<Continuation>(continuation), run_on_failure, Prefer{});
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

  FRIEND_TEST(FutureRefTest, ChainRemoved);
  FRIEND_TEST(FutureRefTest, TailRemoved);
  FRIEND_TEST(FutureRefTest, HeadRemoved);
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
