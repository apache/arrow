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

#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

namespace detail {

template <typename Fn, typename T>
class AsyncFunctionIterator {
 public:
  explicit AsyncFunctionIterator(Fn fn) : fn_(std::move(fn)) {}

  Future<T> Next() { return fn_(); }

 private:
  Fn fn_;
};

}  // namespace detail

/// \brief An asynchronous Iterator that can return errors
template <typename T>
class AsyncIterator : public util::EqualityComparable<AsyncIterator<T>> {
 public:
  /// \brief Iterator may be constructed from any type which has a member function
  /// with signature Future<T> Next();
  ///
  /// See comment on Iterator for details about the Wrapped/Delete mechanisms in this
  /// class.
  template <typename Wrapped>
  explicit AsyncIterator(Wrapped has_next)
      : ptr_(new Wrapped(std::move(has_next)), Delete<Wrapped>), next_(Next<Wrapped>) {}

  AsyncIterator() : ptr_(NULLPTR, [](void*) {}) {}

  /// \brief Return the next element of the sequence.  The future will complete with
  /// IterationTraits<T>::End() when the iteration is completed. Calling this on a default
  /// constructed AsyncIterator will result in undefined behavior.
  ///
  /// Note, unlike Iterator, care should be taken to provide back-pressure and not call
  /// Next() repeatedly.
  Future<T> Next() { return next_(ptr_.get()); }

  /// Pass each element of the sequence to a visitor. Will return any error status
  /// returned by the visitor, terminating iteration.
  template <typename Visitor>
  Future<> Visit(Visitor visitor) {
    // TODO: Capturing this, do we need to ensure lifetime?
    auto loop_body = [this, visitor] {
      auto next = Next();
      return next.Then([visitor](const T& result) -> Result<ControlFlow<detail::Empty>> {
        if (result == IterationTraits<T>::End()) {
          return Break(detail::Empty());
        } else {
          auto visited = visitor(result);
          if (visited.ok()) {
            return Continue();
          } else {
            return visited;
          }
        }
      });
    };
    return Loop(loop_body);
  }

  /// AsyncIterators will only compare equal if they are both null.
  /// Equality comparability is required to make an Iterator of Iterators
  /// (to check for the end condition).
  /// TODO: Is this still needed?  Will there be an Iterator<AsyncIterator> or
  /// AsyncIterator<AsyncIterator>?
  bool Equals(const AsyncIterator& other) const { return ptr_ == other.ptr_; }

  explicit operator bool() const { return ptr_ != NULLPTR; }

  /// \brief Move every element of this iterator into a vector.
  Future<std::shared_ptr<std::vector<T>>> ToVector() {
    auto vec = std::make_shared<std::vector<T>>();
    auto loop_body = [this, vec] {
      auto next = Next();
      return next.Then(
          [vec](const T& result) -> Result<ControlFlow<std::shared_ptr<std::vector<T>>>> {
            if (result == IterationTraits<T>::End()) {
              return Break(vec);
            } else {
              vec->push_back(result);
              return Continue();
            }
          });
    };
    return Loop(loop_body);
  }

  /// \brief Construct an Iterator which invokes a callable on Next()
  template <typename Fn,
            typename Ret = typename internal::call_traits::return_type<Fn>::ValueType>
  static AsyncIterator<Ret> MakeFunctionIterator(Fn fn) {
    return AsyncIterator<Ret>(detail::AsyncFunctionIterator<Fn, Ret>(std::move(fn)));
  }

  static AsyncIterator<T> MakeEmpty() {
    return AsyncIterator<T>::MakeFunctionIterator(
        [] { return Future<T>::MakeFinished(IterationTraits<T>::End()); });
  }

 private:
  /// Implementation of deleter for ptr_: Casts from void* to the wrapped type and
  /// deletes that.
  template <typename HasNext>
  static void Delete(void* ptr) {
    delete static_cast<HasNext*>(ptr);
  }

  /// Implementation of Next: Casts from void* to the wrapped type and invokes that
  /// type's Next member function.
  template <typename HasNext>
  static Future<T> Next(void* ptr) {
    return static_cast<HasNext*>(ptr)->Next();
  }

  /// ptr_ is a unique_ptr to void with a custom deleter: a function pointer which first
  /// casts from void* to a pointer to the wrapped type then deletes that.
  std::unique_ptr<void, void (*)(void*)> ptr_;

  /// next_ is a function pointer which first casts from void* to a pointer to the wrapped
  /// type then invokes its Next member function.
  Future<T> (*next_)(void*) = NULLPTR;
};

namespace async {

/// Iterates through a generator of futures, visiting the result of each one and returning
/// a future that completes when all have been visited
template <typename T>
Future<> VisitAsyncGenerator(std::function<Future<T>()> generator,
                             std::function<Status(T)> visitor) {
  auto loop_body = [generator, visitor] {
    auto next = generator();
    return next.Then([visitor](const T& result) -> Result<ControlFlow<detail::Empty>> {
      if (result == IterationTraits<T>::End()) {
        return Break(detail::Empty());
      } else {
        auto visited = visitor(result);
        if (visited.ok()) {
          return Continue();
        } else {
          return visited;
        }
      }
    });
  };
  return Loop(loop_body);
}

template <typename T>
Future<std::vector<T>> CollectAsyncGenerator(std::function<Future<T>()> generator) {
  auto vec = std::make_shared<std::vector<T>>();
  auto loop_body = [generator, vec] {
    auto next = generator();
    return next.Then([vec](const T& result) -> Result<ControlFlow<std::vector<T>>> {
      if (result == IterationTraits<T>::End()) {
        return Break(*vec);
      } else {
        vec->push_back(result);
        return Continue();
      }
    });
  };
  return Loop(loop_body);
}

template <typename T = detail::Empty>
struct TransformFlow {
  using YieldValueType = T;

  bool HasValue() const { return yield_value_.has_value(); }
  bool Finished() const { return !yield_value_.has_value(); }
  bool ReadyForNext() const { return ready_for_next_; }

  static Result<YieldValueType> MoveYieldValue(const TransformFlow& cf) {
    return std::move(*cf.yield_value_);
  }

  mutable util::optional<YieldValueType> yield_value_;
  bool finished_;
  bool ready_for_next_;
};

struct Finish {
  template <typename T>
  operator TransformFlow<T>() && {  // NOLINT explicit
    return {true, true};
  }
};

struct Skip {
  template <typename T>
  operator TransformFlow<T>() && {  // NOLINT explicit
    return {false, true};
  }
};

template <typename T = detail::Empty>
TransformFlow<T> Yield(T value = {}, bool ready_for_next = true) {
  return TransformFlow<T>{std::move(value), false, ready_for_next};
}

template <typename T, typename V>
std::function<Future<V>()> Transform(
    std::function<Future<T>()> generator,
    std::function<TransformFlow<V>(T value)> transformer) {
  auto finished = std::make_shared<bool>();
  auto last_value = std::make_shared<util::optional<T>>();

  std::function<util::optional<V>> pump =
      [transformer](std::shared_ptr<bool>& finished,
                    std::shared_ptr<util::optional<T>>& last_value) {
        while (!*finished && last_value->has_value()) {
          TransformFlow<V> next = transformer(**last_value);
          if (next.ReadyForNext()) {
            last_value->reset();
          }
          if (next.Finished()) {
            *finished = true;
          }
          if (next.HasValue()) {
            return next.Value();
          }
        }
        if (*finished) {
          return IterationTraits<V>::End();
        }
        return util::optional<V>();
      };

  std::function<Future<V>()> result;
  result = [finished, last_value, generator, result]() {
    auto maybe_next = pump(finished, last_value);
    if (maybe_next->has_value()) {
      return Future<V>::MakeFinished(maybe_next);
    }
    return generator().Then([result, last_value](const Result<T>& next_result) {
      if (next_result.ok()) {
        *last_value = *next_result;
        return result();
      } else {
        return Future<V>::MakeFinished(next_result.status());
      }
    });
  };
}

}  // namespace async

namespace detail {

// TODO: Should Operator here just be std::function<Status(T, Emitter&)> for self
// documenting & type erasure purposes?
// TODO: Lambdas are capturing this, do we need to do some work to make sure this doesn't
// die until the lambdas have had a chance to run?  Maybe capture weak reference to this
template <typename T, typename V, typename Operator>
class AsyncOperatorIterator {
 public:
  explicit AsyncOperatorIterator(AsyncIterator<T> it, Operator&& op)
      : it_(std::move(it)), op_(op) {}

  Future<> PumpUntilReady() {
    if (!emitter_.finished_ && emitter_.item_buffer_.empty()) {
      return it_.Next().Then([this](const T& next) -> Future<> {
        auto finished = (next == IterationTraits<T>::End());
        // TODO: Clean up with futuristic ARROW_RETURN_NOT_OK
        auto op_status = op_(std::move(next), emitter_);
        if (!op_status.ok()) {
          return Future<>::MakeFinished(op_status);
        }
        if (finished) {
          emitter_.finished_ = true;
        }
        // TODO: Recursing here, stack overflow possible?
        return PumpUntilReady();
      });
    } else {
      return Future<>::MakeFinished();
    }
  }

  // Note: it is not safe to call Next again until the previous iteration is finished
  // should not iterate over this in a parallel fashion.  This is even more dangerous
  // here.
  Future<V> Next() {
    return PumpUntilReady().Then([this](const detail::Empty&) -> Result<V> {
      if (emitter_.finished_ && emitter_.item_buffer_.empty()) {
        return IterationTraits<V>::End();
      }
      auto result = emitter_.item_buffer_.front();
      emitter_.item_buffer_.pop();
      return result;
    });
  }

 private:
  AsyncIterator<T> it_;
  Operator op_;
  Emitter<V> emitter_;
};

template <typename T>
struct AsyncIteratorWrapperPromise : ReadaheadPromise {
  ~AsyncIteratorWrapperPromise() override {}

  explicit AsyncIteratorWrapperPromise(Iterator<T>* it) : it_(it) {}

  void Call() override {
    assert(!called_);
    out_.MarkFinished(it_->Next());
    called_ = true;
  }

  Iterator<T>* it_;
  Future<T> out_ = Future<T>::Make();
  bool called_ = false;
};

}  // namespace detail

// Should this be a member function of Iterator<T>?
template <typename T, typename V, typename Operator>
AsyncIterator<V> MakeAsyncOperatorIterator(AsyncIterator<T> it, Operator op) {
  return AsyncIterator<V>(
      detail::AsyncOperatorIterator<T, V, Operator>(std::move(it), std::move(op)));
}

/// \brief Async iterator that iterates on the underlying iterator in a
/// separate thread.
/// TODO: AFter sleeping on it I should add limit back into readahead to avoid
/// memory exhaustion.  Item is "consumed" as soon as future is created.
template <typename T>
class AsyncIteratorWrapper {
  using PromiseType = typename detail::AsyncIteratorWrapperPromise<T>;

 public:
  // Public default constructor creates an empty iterator
  AsyncIteratorWrapper(internal::Executor* executor) : executor_(executor), done_(true) {}

  ~AsyncIteratorWrapper() {
    if (queue_) {
      // Make sure the queue doesn't call any promises after this object
      // is destroyed.
      queue_->EnsureShutdownOrDie();
    }
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(AsyncIteratorWrapper);
  ARROW_DISALLOW_COPY_AND_ASSIGN(AsyncIteratorWrapper);

  Future<T> Next() {
    if (done_) {
      return Future<T>::MakeFinished(IterationTraits<T>::End());
    }
    auto promise = std::unique_ptr<PromiseType>(new PromiseType{it_.get()});
    auto result = Future<T>(promise->out_);
    // TODO: Need a futuristic version of ARROW_RETURN_NOT_OK
    auto append_status = queue_->Append(
        static_cast<std::unique_ptr<detail::ReadaheadPromise>>(std::move(promise)));
    if (!append_status.ok()) {
      return Future<T>::MakeFinished(append_status);
    }

    result.AddCallback([this](const Result<T>& result) {
      if (!result.ok() || result.ValueUnsafe() == IterationTraits<T>::End()) {
        done_ = true;
      }
    });

    return executor_->Transfer(result);
  }

  static Result<AsyncIterator<T>> Make(Iterator<T> it) {
    return AsyncIterator<T>(AsyncIteratorWrapper(std::move(it)));
  }

 private:
  explicit AsyncIteratorWrapper(Iterator<T> it)
      : it_(new Iterator<T>(std::move(it))), queue_(new detail::ReadaheadQueue(0)) {}

  // The underlying iterator is referenced by pointer in ReadaheadPromise,
  // so make sure it doesn't move.
  std::unique_ptr<Iterator<T>> it_;
  std::unique_ptr<detail::ReadaheadQueue> queue_;
  internal::Executor* executor_;
  bool done_ = false;
};

/// \brief Async generator that iterates on an underlying iterator in a
/// separate thread.
/// TODO: After sleeping on it I should add limit back into readahead to avoid
/// memory exhaustion.  Item is "consumed" as soon as future is created.
template <typename T>
class BackgroundIterator {
  using PromiseType = typename detail::AsyncIteratorWrapperPromise<T>;

 public:
  explicit BackgroundIterator(Iterator<T> it, internal::Executor* executor)
      : it_(new Iterator<T>(std::move(it))),
        queue_(new detail::ReadaheadQueue(0)),
        executor_(executor) {}

  ~BackgroundIterator() {
    if (queue_) {
      // Make sure the queue doesn't call any promises after this object
      // is destroyed.
      queue_->EnsureShutdownOrDie();
    }
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(BackgroundIterator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(BackgroundIterator);

  Future<T> operator()() {
    if (done_) {
      return Future<T>::MakeFinished(IterationTraits<T>::End());
    }
    auto promise = std::unique_ptr<PromiseType>(new PromiseType{it_.get()});
    auto result = Future<T>(promise->out_);
    // TODO: Need a futuristic version of ARROW_RETURN_NOT_OK
    auto append_status = queue_->Append(
        static_cast<std::unique_ptr<detail::ReadaheadPromise>>(std::move(promise)));
    if (!append_status.ok()) {
      return Future<T>::MakeFinished(append_status);
    }

    result.AddCallback([this](const Result<T>& result) {
      if (!result.ok() || result.ValueUnsafe() == IterationTraits<T>::End()) {
        done_ = true;
      }
    });

    return executor_->Transfer(result);
  }

 protected:
  // The underlying iterator is referenced by pointer in ReadaheadPromise,
  // so make sure it doesn't move.
  std::unique_ptr<Iterator<T>> it_;
  std::unique_ptr<detail::ReadaheadQueue> queue_;
  internal::Executor* executor_;
  bool done_ = false;
};

template <typename T>
struct BackgroundIteratorWrapper {
  explicit BackgroundIteratorWrapper(std::shared_ptr<BackgroundIterator<T>> target)
      : target_(std::move(target)) {}

  Future<T> operator()() { return (*target_)(); }

  std::shared_ptr<BackgroundIterator<T>> target_;
};

/// \brief Construct an Iterator which invokes a callable on Next()
template <typename T>
static Result<std::function<Future<T>()>> MakeBackgroundIterator(
    Iterator<T> iterator, internal::ThreadPool* executor) {
  auto background_iterator =
      std::make_shared<BackgroundIterator<T>>(std::move(iterator), executor);
  return static_cast<std::function<Future<T>()>>(
      BackgroundIteratorWrapper<T>(std::move(background_iterator)));
}

}  // namespace arrow