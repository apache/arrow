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
#include <cassert>
#include <cstring>
#include <deque>
#include <limits>
#include <queue>

#include "arrow/util/async_util.h"
#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/io_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/mutex.h"
#include "arrow/util/optional.h"
#include "arrow/util/queue.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

// The methods in this file create, modify, and utilize AsyncGenerator which is an
// iterator of futures.  This allows an asynchronous source (like file input) to be run
// through a pipeline in the same way that iterators can be used to create pipelined
// workflows.
//
// In order to support pipeline parallelism we introduce the concept of asynchronous
// reentrancy. This is different than synchronous reentrancy.  With synchronous code a
// function is reentrant if the function can be called again while a previous call to that
// function is still running.  Unless otherwise specified none of these generators are
// synchronously reentrant.  Care should be taken to avoid calling them in such a way (and
// the utilities Visit/Collect/Await take care to do this).
//
// Asynchronous reentrancy on the other hand means the function is called again before the
// future returned by the function is marked finished (but after the call to get the
// future returns).  Some of these generators are async-reentrant while others (e.g.
// those that depend on ordered processing like decompression) are not.  Read the MakeXYZ
// function comments to determine which generators support async reentrancy.
//
// Note: Generators that are not asynchronously reentrant can still support readahead
// (\see MakeSerialReadaheadGenerator).
//
// Readahead operators, and some other operators, may introduce queueing.  Any operators
// that introduce buffering should detail the amount of buffering they introduce in their
// MakeXYZ function comments.
template <typename T>
using AsyncGenerator = std::function<Future<T>()>;

template <typename T>
struct IterationTraits<AsyncGenerator<T>> {
  /// \brief by default when iterating through a sequence of AsyncGenerator<T>,
  /// an empty function indicates the end of iteration.
  static AsyncGenerator<T> End() { return AsyncGenerator<T>(); }

  static bool IsEnd(const AsyncGenerator<T>& val) { return !val; }
};

template <typename T>
Future<T> AsyncGeneratorEnd() {
  return Future<T>::MakeFinished(IterationTraits<T>::End());
}

/// returning a future that completes when all have been visited
template <typename T, typename Visitor>
Future<> VisitAsyncGenerator(AsyncGenerator<T> generator, Visitor visitor) {
  struct LoopBody {
    struct Callback {
      Result<ControlFlow<>> operator()(const T& next) {
        if (IsIterationEnd(next)) {
          return Break();
        } else {
          auto visited = visitor(next);
          if (visited.ok()) {
            return Continue();
          } else {
            return visited;
          }
        }
      }

      Visitor visitor;
    };

    Future<ControlFlow<>> operator()() {
      Callback callback{visitor};
      auto next = generator();
      return next.Then(std::move(callback));
    }

    AsyncGenerator<T> generator;
    Visitor visitor;
  };

  return Loop(LoopBody{std::move(generator), std::move(visitor)});
}

/// \brief Wait for an async generator to complete, discarding results.
template <typename T>
Future<> DiscardAllFromAsyncGenerator(AsyncGenerator<T> generator) {
  std::function<Status(T)> visitor = [](const T&) { return Status::OK(); };
  return VisitAsyncGenerator(generator, visitor);
}

/// \brief Collect the results of an async generator into a vector
template <typename T>
Future<std::vector<T>> CollectAsyncGenerator(AsyncGenerator<T> generator) {
  auto vec = std::make_shared<std::vector<T>>();
  struct LoopBody {
    Future<ControlFlow<std::vector<T>>> operator()() {
      auto next = generator_();
      auto vec = vec_;
      return next.Then([vec](const T& result) -> Result<ControlFlow<std::vector<T>>> {
        if (IsIterationEnd(result)) {
          return Break(*vec);
        } else {
          vec->push_back(result);
          return Continue();
        }
      });
    }
    AsyncGenerator<T> generator_;
    std::shared_ptr<std::vector<T>> vec_;
  };
  return Loop(LoopBody{std::move(generator), std::move(vec)});
}

/// \see MakeMappedGenerator
template <typename T, typename V>
class MappingGenerator {
 public:
  MappingGenerator(AsyncGenerator<T> source, std::function<Future<V>(const T&)> map)
      : state_(std::make_shared<State>(std::move(source), std::move(map))) {}

  Future<V> operator()() {
    auto future = Future<V>::Make();
    bool should_trigger;
    {
      auto guard = state_->mutex.Lock();
      if (state_->finished) {
        return AsyncGeneratorEnd<V>();
      }
      should_trigger = state_->waiting_jobs.empty();
      state_->waiting_jobs.push_back(future);
    }
    if (should_trigger) {
      state_->source().AddCallback(Callback{state_});
    }
    return future;
  }

 private:
  struct State {
    State(AsyncGenerator<T> source, std::function<Future<V>(const T&)> map)
        : source(std::move(source)),
          map(std::move(map)),
          waiting_jobs(),
          mutex(),
          finished(false) {}

    void Purge() {
      // This might be called by an original callback (if the source iterator fails or
      // ends) or by a mapped callback (if the map function fails or ends prematurely).
      // Either way it should only be called once and after finished is set so there is no
      // need to guard access to `waiting_jobs`.
      while (!waiting_jobs.empty()) {
        waiting_jobs.front().MarkFinished(IterationTraits<V>::End());
        waiting_jobs.pop_front();
      }
    }

    AsyncGenerator<T> source;
    std::function<Future<V>(const T&)> map;
    std::deque<Future<V>> waiting_jobs;
    util::Mutex mutex;
    bool finished;
  };

  struct Callback;

  struct MappedCallback {
    void operator()(const Result<V>& maybe_next) {
      bool end = !maybe_next.ok() || IsIterationEnd(*maybe_next);
      bool should_purge = false;
      if (end) {
        {
          auto guard = state->mutex.Lock();
          should_purge = !state->finished;
          state->finished = true;
        }
      }
      sink.MarkFinished(maybe_next);
      if (should_purge) {
        state->Purge();
      }
    }
    std::shared_ptr<State> state;
    Future<V> sink;
  };

  struct Callback {
    void operator()(const Result<T>& maybe_next) {
      Future<V> sink;
      bool end = !maybe_next.ok() || IsIterationEnd(*maybe_next);
      bool should_purge = false;
      bool should_trigger;
      {
        auto guard = state->mutex.Lock();
        // A MappedCallback may have purged or be purging the queue;
        // we shouldn't do anything here.
        if (state->finished) return;
        if (end) {
          should_purge = !state->finished;
          state->finished = true;
        }
        sink = state->waiting_jobs.front();
        state->waiting_jobs.pop_front();
        should_trigger = !end && !state->waiting_jobs.empty();
      }
      if (should_purge) {
        state->Purge();
      }
      if (should_trigger) {
        state->source().AddCallback(Callback{state});
      }
      if (maybe_next.ok()) {
        const T& val = maybe_next.ValueUnsafe();
        if (IsIterationEnd(val)) {
          sink.MarkFinished(IterationTraits<V>::End());
        } else {
          Future<V> mapped_fut = state->map(val);
          mapped_fut.AddCallback(MappedCallback{std::move(state), std::move(sink)});
        }
      } else {
        sink.MarkFinished(maybe_next.status());
      }
    }

    std::shared_ptr<State> state;
  };

  std::shared_ptr<State> state_;
};

/// \brief Create a generator that will apply the map function to each element of
/// source.  The map function is not called on the end token.
///
/// Note: This function makes a copy of `map` for each item
/// Note: Errors returned from the `map` function will be propagated
///
/// If the source generator is async-reentrant then this generator will be also
template <typename T, typename MapFn,
          typename Mapped = detail::result_of_t<MapFn(const T&)>,
          typename V = typename EnsureFuture<Mapped>::type::ValueType>
AsyncGenerator<V> MakeMappedGenerator(AsyncGenerator<T> source_generator, MapFn map) {
  struct MapCallback {
    MapFn map_;

    Future<V> operator()(const T& val) { return ToFuture(map_(val)); }
  };

  return MappingGenerator<T, V>(std::move(source_generator), MapCallback{std::move(map)});
}

/// \brief Create a generator that will apply the map function to
/// each element of source.  The map function is not called on the end
/// token.  The result of the map function should be another
/// generator; all these generators will then be flattened to produce
/// a single stream of items.
///
/// Note: This function makes a copy of `map` for each item
/// Note: Errors returned from the `map` function will be propagated
///
/// If the source generator is async-reentrant then this generator will be also
template <typename T, typename MapFn,
          typename Mapped = detail::result_of_t<MapFn(const T&)>,
          typename V = typename EnsureFuture<Mapped>::type::ValueType>
AsyncGenerator<T> MakeFlatMappedGenerator(AsyncGenerator<T> source_generator, MapFn map) {
  return MakeConcatenatedGenerator(
      MakeMappedGenerator(std::move(source_generator), std::move(map)));
}

/// \see MakeSequencingGenerator
template <typename T, typename ComesAfter, typename IsNext>
class SequencingGenerator {
 public:
  SequencingGenerator(AsyncGenerator<T> source, ComesAfter compare, IsNext is_next,
                      T initial_value)
      : state_(std::make_shared<State>(std::move(source), std::move(compare),
                                       std::move(is_next), std::move(initial_value))) {}

  Future<T> operator()() {
    {
      auto guard = state_->mutex.Lock();
      // We can send a result immediately if the top of the queue is either an
      // error or the next item
      if (!state_->queue.empty() &&
          (!state_->queue.top().ok() ||
           state_->is_next(state_->previous_value, *state_->queue.top()))) {
        auto result = std::move(state_->queue.top());
        if (result.ok()) {
          state_->previous_value = *result;
        }
        state_->queue.pop();
        return Future<T>::MakeFinished(result);
      }
      if (state_->finished) {
        return AsyncGeneratorEnd<T>();
      }
      // The next item is not in the queue so we will need to wait
      auto new_waiting_fut = Future<T>::Make();
      state_->waiting_future = new_waiting_fut;
      guard.Unlock();
      state_->source().AddCallback(Callback{state_});
      return new_waiting_fut;
    }
  }

 private:
  struct WrappedComesAfter {
    bool operator()(const Result<T>& left, const Result<T>& right) {
      if (!left.ok() || !right.ok()) {
        // Should never happen
        return false;
      }
      return compare(*left, *right);
    }
    ComesAfter compare;
  };

  struct State {
    State(AsyncGenerator<T> source, ComesAfter compare, IsNext is_next, T initial_value)
        : source(std::move(source)),
          is_next(std::move(is_next)),
          previous_value(std::move(initial_value)),
          waiting_future(),
          queue(WrappedComesAfter{compare}),
          finished(false),
          mutex() {}

    AsyncGenerator<T> source;
    IsNext is_next;
    T previous_value;
    Future<T> waiting_future;
    std::priority_queue<Result<T>, std::vector<Result<T>>, WrappedComesAfter> queue;
    bool finished;
    util::Mutex mutex;
  };

  class Callback {
   public:
    explicit Callback(std::shared_ptr<State> state) : state_(std::move(state)) {}

    void operator()(const Result<T> result) {
      Future<T> to_deliver;
      bool finished;
      {
        auto guard = state_->mutex.Lock();
        bool ready_to_deliver = false;
        if (!result.ok()) {
          // Clear any cached results
          while (!state_->queue.empty()) {
            state_->queue.pop();
          }
          ready_to_deliver = true;
          state_->finished = true;
        } else if (IsIterationEnd<T>(result.ValueUnsafe())) {
          ready_to_deliver = state_->queue.empty();
          state_->finished = true;
        } else {
          ready_to_deliver = state_->is_next(state_->previous_value, *result);
        }

        if (ready_to_deliver && state_->waiting_future.is_valid()) {
          to_deliver = state_->waiting_future;
          if (result.ok()) {
            state_->previous_value = *result;
          }
        } else {
          state_->queue.push(result);
        }
        // Capture state_->finished so we can access it outside the mutex
        finished = state_->finished;
      }
      // Must deliver result outside of the mutex
      if (to_deliver.is_valid()) {
        to_deliver.MarkFinished(result);
      } else {
        // Otherwise, if we didn't get the next item (or a terminal item), we
        // need to keep looking
        if (!finished) {
          state_->source().AddCallback(Callback{state_});
        }
      }
    }

   private:
    const std::shared_ptr<State> state_;
  };

  const std::shared_ptr<State> state_;
};

/// \brief Buffer an AsyncGenerator to return values in sequence order  ComesAfter
/// and IsNext determine the sequence order.
///
/// ComesAfter should be a BinaryPredicate that only returns true if a comes after b
///
/// IsNext should be a BinaryPredicate that returns true, given `a` and `b`, only if
/// `b` follows immediately after `a`.  It should return true given `initial_value` and
/// `b` if `b` is the first item in the sequence.
///
/// This operator will queue unboundedly while waiting for the next item.  It is intended
/// for jittery sources that might scatter an ordered sequence.  It is NOT intended to
/// sort.  Using it to try and sort could result in excessive RAM usage.  This generator
/// will queue up to N blocks where N is the max "out of order"ness of the source.
///
/// For example, if the source is 1,6,2,5,4,3 it will queue 3 blocks because 3 is 3
/// blocks beyond where it belongs.
///
/// This generator is not async-reentrant but it consists only of a simple log(n)
/// insertion into a priority queue.
template <typename T, typename ComesAfter, typename IsNext>
AsyncGenerator<T> MakeSequencingGenerator(AsyncGenerator<T> source_generator,
                                          ComesAfter compare, IsNext is_next,
                                          T initial_value) {
  return SequencingGenerator<T, ComesAfter, IsNext>(
      std::move(source_generator), std::move(compare), std::move(is_next),
      std::move(initial_value));
}

/// \see MakeTransformedGenerator
template <typename T, typename V>
class TransformingGenerator {
  // The transforming generator state will be referenced as an async generator but will
  // also be referenced via callback to various futures.  If the async generator owner
  // moves it around we need the state to be consistent for future callbacks.
  struct TransformingGeneratorState
      : std::enable_shared_from_this<TransformingGeneratorState> {
    TransformingGeneratorState(AsyncGenerator<T> generator, Transformer<T, V> transformer)
        : generator_(std::move(generator)),
          transformer_(std::move(transformer)),
          last_value_(),
          finished_() {}

    Future<V> operator()() {
      while (true) {
        auto maybe_next_result = Pump();
        if (!maybe_next_result.ok()) {
          return Future<V>::MakeFinished(maybe_next_result.status());
        }
        auto maybe_next = std::move(maybe_next_result).ValueUnsafe();
        if (maybe_next.has_value()) {
          return Future<V>::MakeFinished(*std::move(maybe_next));
        }

        auto next_fut = generator_();
        // If finished already, process results immediately inside the loop to avoid
        // stack overflow
        if (next_fut.is_finished()) {
          auto next_result = next_fut.result();
          if (next_result.ok()) {
            last_value_ = *next_result;
          } else {
            return Future<V>::MakeFinished(next_result.status());
          }
          // Otherwise, if not finished immediately, add callback to process results
        } else {
          auto self = this->shared_from_this();
          return next_fut.Then([self](const T& next_result) {
            self->last_value_ = next_result;
            return (*self)();
          });
        }
      }
    }

    // See comment on TransformingIterator::Pump
    Result<util::optional<V>> Pump() {
      if (!finished_ && last_value_.has_value()) {
        ARROW_ASSIGN_OR_RAISE(TransformFlow<V> next, transformer_(*last_value_));
        if (next.ReadyForNext()) {
          if (IsIterationEnd(*last_value_)) {
            finished_ = true;
          }
          last_value_.reset();
        }
        if (next.Finished()) {
          finished_ = true;
        }
        if (next.HasValue()) {
          return next.Value();
        }
      }
      if (finished_) {
        return IterationTraits<V>::End();
      }
      return util::nullopt;
    }

    AsyncGenerator<T> generator_;
    Transformer<T, V> transformer_;
    util::optional<T> last_value_;
    bool finished_;
  };

 public:
  explicit TransformingGenerator(AsyncGenerator<T> generator,
                                 Transformer<T, V> transformer)
      : state_(std::make_shared<TransformingGeneratorState>(std::move(generator),
                                                            std::move(transformer))) {}

  Future<V> operator()() { return (*state_)(); }

 protected:
  std::shared_ptr<TransformingGeneratorState> state_;
};

/// \brief Transform an async generator using a transformer function returning a new
/// AsyncGenerator
///
/// The transform function here behaves exactly the same as the transform function in
/// MakeTransformedIterator and you can safely use the same transform function to
/// transform both synchronous and asynchronous streams.
///
/// This generator is not async-reentrant
///
/// This generator may queue up to 1 instance of T but will not delay
template <typename T, typename V>
AsyncGenerator<V> MakeTransformedGenerator(AsyncGenerator<T> generator,
                                           Transformer<T, V> transformer) {
  return TransformingGenerator<T, V>(generator, transformer);
}

/// \see MakeSerialReadaheadGenerator
template <typename T>
class SerialReadaheadGenerator {
 public:
  SerialReadaheadGenerator(AsyncGenerator<T> source_generator, int max_readahead)
      : state_(std::make_shared<State>(std::move(source_generator), max_readahead)) {}

  Future<T> operator()() {
    if (state_->first_) {
      // Lazy generator, need to wait for the first ask to prime the pump
      state_->first_ = false;
      auto next = state_->source_();
      return next.Then(Callback{state_}, ErrCallback{state_});
    }

    // This generator is not async-reentrant.  We won't be called until the last
    // future finished so we know there is something in the queue
    auto finished = state_->finished_.load();
    if (finished && state_->readahead_queue_.IsEmpty()) {
      return AsyncGeneratorEnd<T>();
    }

    std::shared_ptr<Future<T>> next;
    if (!state_->readahead_queue_.Read(next)) {
      return Status::UnknownError("Could not read from readahead_queue");
    }

    auto last_available = state_->spaces_available_.fetch_add(1);
    if (last_available == 0 && !finished) {
      // Reader idled out, we need to restart it
      ARROW_RETURN_NOT_OK(state_->Pump(state_));
    }
    return *next;
  }

 private:
  struct State {
    State(AsyncGenerator<T> source, int max_readahead)
        : first_(true),
          source_(std::move(source)),
          finished_(false),
          // There is one extra "space" for the in-flight request
          spaces_available_(max_readahead + 1),
          // The SPSC queue has size-1 "usable" slots so we need to overallocate 1
          readahead_queue_(max_readahead + 1) {}

    Status Pump(const std::shared_ptr<State>& self) {
      // Can't do readahead_queue.write(source().Then(...)) because then the
      // callback might run immediately and add itself to the queue before this gets added
      // to the queue messing up the order.
      auto next_slot = std::make_shared<Future<T>>();
      auto written = readahead_queue_.Write(next_slot);
      if (!written) {
        return Status::UnknownError("Could not write to readahead_queue");
      }
      // If this Pump is being called from a callback it is possible for the source to
      // poll and read from the queue between the Write and this spot where we fill the
      // value in. However, it is not possible for the future to read this value we are
      // writing.  That is because this callback (the callback for future X) must be
      // finished before future X is marked complete and this source is not pulled
      // reentrantly so it will not poll for future X+1 until this callback has completed.
      *next_slot = source_().Then(Callback{self}, ErrCallback{self});
      return Status::OK();
    }

    // Only accessed by the consumer end
    bool first_;
    // Accessed by both threads
    AsyncGenerator<T> source_;
    std::atomic<bool> finished_;
    // The queue has a size but it is not atomic.  We keep track of how many spaces are
    // left in the queue here so we know if we've just written the last value and we need
    // to stop reading ahead or if we've just read from a full queue and we need to
    // restart reading ahead
    std::atomic<uint32_t> spaces_available_;
    // Needs to be a queue of shared_ptr and not Future because we set the value of the
    // future after we add it to the queue
    util::SpscQueue<std::shared_ptr<Future<T>>> readahead_queue_;
  };

  struct Callback {
    Result<T> operator()(const T& next) {
      if (IsIterationEnd(next)) {
        state_->finished_.store(true);
        return next;
      }
      auto last_available = state_->spaces_available_.fetch_sub(1);
      if (last_available > 1) {
        ARROW_RETURN_NOT_OK(state_->Pump(state_));
      }
      return next;
    }

    std::shared_ptr<State> state_;
  };

  struct ErrCallback {
    Result<T> operator()(const Status& st) {
      state_->finished_.store(true);
      return st;
    }

    std::shared_ptr<State> state_;
  };

  std::shared_ptr<State> state_;
};

/// \see MakeFromFuture
template <typename T>
class FutureFirstGenerator {
 public:
  explicit FutureFirstGenerator(Future<AsyncGenerator<T>> future)
      : state_(std::make_shared<State>(std::move(future))) {}

  Future<T> operator()() {
    if (state_->source_) {
      return state_->source_();
    } else {
      auto state = state_;
      return state_->future_.Then([state](const AsyncGenerator<T>& source) {
        state->source_ = source;
        return state->source_();
      });
    }
  }

 private:
  struct State {
    explicit State(Future<AsyncGenerator<T>> future) : future_(future), source_() {}

    Future<AsyncGenerator<T>> future_;
    AsyncGenerator<T> source_;
  };

  std::shared_ptr<State> state_;
};

/// \brief Transform a Future<AsyncGenerator<T>> into an AsyncGenerator<T>
/// that waits for the future to complete as part of the first item.
///
/// This generator is not async-reentrant (even if the generator yielded by future is)
///
/// This generator does not queue
template <typename T>
AsyncGenerator<T> MakeFromFuture(Future<AsyncGenerator<T>> future) {
  return FutureFirstGenerator<T>(std::move(future));
}

/// \brief Create a generator that will pull from the source into a queue.  Unlike
/// MakeReadaheadGenerator this will not pull reentrantly from the source.
///
/// The source generator does not need to be async-reentrant
///
/// This generator is not async-reentrant (even if the source is)
///
/// This generator may queue up to max_readahead additional instances of T
template <typename T>
AsyncGenerator<T> MakeSerialReadaheadGenerator(AsyncGenerator<T> source_generator,
                                               int max_readahead) {
  return SerialReadaheadGenerator<T>(std::move(source_generator), max_readahead);
}

/// \brief Create a generator that immediately pulls from the source
///
/// Typical generators do not pull from their source until they themselves
/// are pulled.  This generator does not follow that convention and will call
/// generator() once before it returns.  The returned generator will otherwise
/// mirror the source.
///
/// This generator forwards aysnc-reentrant pressure to the source
/// This generator buffers one item (the first result) until it is delivered.
template <typename T>
AsyncGenerator<T> MakeAutoStartingGenerator(AsyncGenerator<T> generator) {
  struct AutostartGenerator {
    Future<T> operator()() {
      if (first_future->is_valid()) {
        Future<T> result = *first_future;
        *first_future = Future<T>();
        return result;
      }
      return source();
    }

    std::shared_ptr<Future<T>> first_future;
    AsyncGenerator<T> source;
  };

  std::shared_ptr<Future<T>> first_future = std::make_shared<Future<T>>(generator());
  return AutostartGenerator{std::move(first_future), std::move(generator)};
}

/// \see MakeReadaheadGenerator
template <typename T>
class ReadaheadGenerator {
 public:
  ReadaheadGenerator(AsyncGenerator<T> source_generator, int max_readahead)
      : state_(std::make_shared<State>(std::move(source_generator), max_readahead)) {}

  Future<T> AddMarkFinishedContinuation(Future<T> fut) {
    auto state = state_;
    return fut.Then(
        [state](const T& result) -> Result<T> {
          state->MarkFinishedIfDone(result);
          return result;
        },
        [state](const Status& err) -> Result<T> {
          state->finished.store(true);
          return err;
        });
  }

  Future<T> operator()() {
    if (state_->readahead_queue.empty()) {
      // This is the first request, let's pump the underlying queue
      for (int i = 0; i < state_->max_readahead; i++) {
        auto next = state_->source_generator();
        auto next_after_check = AddMarkFinishedContinuation(std::move(next));
        state_->readahead_queue.push(std::move(next_after_check));
      }
    }
    // Pop one and add one
    auto result = state_->readahead_queue.front();
    state_->readahead_queue.pop();
    if (state_->finished.load()) {
      state_->readahead_queue.push(AsyncGeneratorEnd<T>());
    } else {
      auto back_of_queue = state_->source_generator();
      auto back_of_queue_after_check =
          AddMarkFinishedContinuation(std::move(back_of_queue));
      state_->readahead_queue.push(std::move(back_of_queue_after_check));
    }
    return result;
  }

 private:
  struct State {
    State(AsyncGenerator<T> source_generator, int max_readahead)
        : source_generator(std::move(source_generator)), max_readahead(max_readahead) {
      finished.store(false);
    }

    void MarkFinishedIfDone(const T& next_result) {
      if (IsIterationEnd(next_result)) {
        finished.store(true);
      }
    }

    AsyncGenerator<T> source_generator;
    int max_readahead;
    std::atomic<bool> finished;
    std::queue<Future<T>> readahead_queue;
  };

  std::shared_ptr<State> state_;
};

/// \brief A generator where the producer pushes items on a queue.
///
/// No back-pressure is applied, so this generator is mostly useful when
/// producing the values is neither CPU- nor memory-expensive (e.g. fetching
/// filesystem metadata).
///
/// This generator is not async-reentrant.
template <typename T>
class PushGenerator {
  struct State {
    explicit State(util::BackpressureOptions backpressure)
        : backpressure(std::move(backpressure)) {}

    void OpenBackpressureIfFreeUnlocked(util::Mutex::Guard&& guard) {
      if (backpressure.toggle && result_q.size() < backpressure.resume_if_below) {
        // Open might trigger callbacks so release the lock first
        guard.Unlock();
        backpressure.toggle->Open();
      }
    }

    void CloseBackpressureIfFullUnlocked() {
      if (backpressure.toggle && result_q.size() > backpressure.pause_if_above) {
        backpressure.toggle->Close();
      }
    }

    util::BackpressureOptions backpressure;
    util::Mutex mutex;
    std::deque<Result<T>> result_q;
    util::optional<Future<T>> consumer_fut;
    bool finished = false;
  };

 public:
  /// Producer API for PushGenerator
  class Producer {
   public:
    explicit Producer(const std::shared_ptr<State>& state) : weak_state_(state) {}

    /// \brief Push a value on the queue
    ///
    /// True is returned if the value was pushed, false if the generator is
    /// already closed or destroyed.  If the latter, it is recommended to stop
    /// producing any further values.
    bool Push(Result<T> result) {
      auto state = weak_state_.lock();
      if (!state) {
        // Generator was destroyed
        return false;
      }
      auto lock = state->mutex.Lock();
      if (state->finished) {
        // Closed early
        return false;
      }
      if (state->consumer_fut.has_value()) {
        auto fut = std::move(state->consumer_fut.value());
        state->consumer_fut.reset();
        lock.Unlock();  // unlock before potentially invoking a callback
        fut.MarkFinished(std::move(result));
      } else {
        state->result_q.push_back(std::move(result));
        state->CloseBackpressureIfFullUnlocked();
      }
      return true;
    }

    /// \brief Tell the consumer we have finished producing
    ///
    /// It is allowed to call this and later call Push() again ("early close").
    /// In this case, calls to Push() after the queue is closed are silently
    /// ignored.  This can help implementing non-trivial cancellation cases.
    ///
    /// True is returned on success, false if the generator is already closed
    /// or destroyed.
    bool Close() {
      auto state = weak_state_.lock();
      if (!state) {
        // Generator was destroyed
        return false;
      }
      auto lock = state->mutex.Lock();
      if (state->finished) {
        // Already closed
        return false;
      }
      state->finished = true;
      if (state->consumer_fut.has_value()) {
        auto fut = std::move(state->consumer_fut.value());
        state->consumer_fut.reset();
        lock.Unlock();  // unlock before potentially invoking a callback
        fut.MarkFinished(IterationTraits<T>::End());
      }
      return true;
    }

    /// Return whether the generator was closed or destroyed.
    bool is_closed() const {
      auto state = weak_state_.lock();
      if (!state) {
        // Generator was destroyed
        return true;
      }
      auto lock = state->mutex.Lock();
      return state->finished;
    }

   private:
    const std::weak_ptr<State> weak_state_;
  };

  explicit PushGenerator(util::BackpressureOptions backpressure = {})
      : state_(std::make_shared<State>(std::move(backpressure))) {}

  /// Read an item from the queue
  Future<T> operator()() const {
    auto lock = state_->mutex.Lock();
    assert(!state_->consumer_fut.has_value());  // Non-reentrant
    if (!state_->result_q.empty()) {
      auto fut = Future<T>::MakeFinished(std::move(state_->result_q.front()));
      state_->result_q.pop_front();
      state_->OpenBackpressureIfFreeUnlocked(std::move(lock));
      return fut;
    }
    if (state_->finished) {
      return AsyncGeneratorEnd<T>();
    }
    auto fut = Future<T>::Make();
    state_->consumer_fut = fut;
    return fut;
  }

  /// \brief Return producer-side interface
  ///
  /// The returned object must be used by the producer to push values on the queue.
  /// Only a single Producer object should be instantiated.
  Producer producer() { return Producer{state_}; }

 private:
  const std::shared_ptr<State> state_;
};

/// \brief Create a generator that pulls reentrantly from a source
/// This generator will pull reentrantly from a source, ensuring that max_readahead
/// requests are active at any given time.
///
/// The source generator must be async-reentrant
///
/// This generator itself is async-reentrant.
///
/// This generator may queue up to max_readahead instances of T
template <typename T>
AsyncGenerator<T> MakeReadaheadGenerator(AsyncGenerator<T> source_generator,
                                         int max_readahead) {
  return ReadaheadGenerator<T>(std::move(source_generator), max_readahead);
}

/// \brief Creates a generator that will yield finished futures from a vector
///
/// This generator is async-reentrant
template <typename T>
AsyncGenerator<T> MakeVectorGenerator(std::vector<T> vec) {
  struct State {
    explicit State(std::vector<T> vec_) : vec(std::move(vec_)), vec_idx(0) {}

    std::vector<T> vec;
    std::atomic<std::size_t> vec_idx;
  };

  auto state = std::make_shared<State>(std::move(vec));
  return [state]() {
    auto idx = state->vec_idx.fetch_add(1);
    if (idx >= state->vec.size()) {
      // Eagerly return memory
      state->vec.clear();
      return AsyncGeneratorEnd<T>();
    }
    return Future<T>::MakeFinished(state->vec[idx]);
  };
}

/// \see MakeMergedGenerator
template <typename T>
class MergedGenerator {
 public:
  explicit MergedGenerator(AsyncGenerator<AsyncGenerator<T>> source,
                           int max_subscriptions)
      : state_(std::make_shared<State>(std::move(source), max_subscriptions)) {}

  Future<T> operator()() {
    Future<T> waiting_future;
    std::shared_ptr<DeliveredJob> delivered_job;
    {
      auto guard = state_->mutex.Lock();
      if (!state_->delivered_jobs.empty()) {
        delivered_job = std::move(state_->delivered_jobs.front());
        state_->delivered_jobs.pop_front();
      } else if (state_->finished) {
        return IterationTraits<T>::End();
      } else {
        waiting_future = Future<T>::Make();
        state_->waiting_jobs.push_back(std::make_shared<Future<T>>(waiting_future));
      }
    }
    if (delivered_job) {
      // deliverer will be invalid if outer callback encounters an error and delivers a
      // failed result
      if (delivered_job->deliverer) {
        delivered_job->deliverer().AddCallback(
            InnerCallback{state_, delivered_job->index});
      }
      return std::move(delivered_job->value);
    }
    if (state_->first) {
      state_->first = false;
      for (std::size_t i = 0; i < state_->active_subscriptions.size(); i++) {
        state_->PullSource().AddCallback(OuterCallback{state_, i});
      }
    }
    return waiting_future;
  }

 private:
  struct DeliveredJob {
    explicit DeliveredJob(AsyncGenerator<T> deliverer_, Result<T> value_,
                          std::size_t index_)
        : deliverer(deliverer_), value(std::move(value_)), index(index_) {}

    AsyncGenerator<T> deliverer;
    Result<T> value;
    std::size_t index;
  };

  struct State {
    State(AsyncGenerator<AsyncGenerator<T>> source, int max_subscriptions)
        : source(std::move(source)),
          active_subscriptions(max_subscriptions),
          delivered_jobs(),
          waiting_jobs(),
          mutex(),
          first(true),
          source_exhausted(false),
          finished(false),
          num_active_subscriptions(max_subscriptions) {}

    Future<AsyncGenerator<T>> PullSource() {
      // Need to guard access to source() so we don't pull sync-reentrantly which
      // is never valid.
      auto lock = mutex.Lock();
      return source();
    }

    AsyncGenerator<AsyncGenerator<T>> source;
    // active_subscriptions and delivered_jobs will be bounded by max_subscriptions
    std::vector<AsyncGenerator<T>> active_subscriptions;
    std::deque<std::shared_ptr<DeliveredJob>> delivered_jobs;
    // waiting_jobs is unbounded, reentrant pulls (e.g. AddReadahead) will provide the
    // backpressure
    std::deque<std::shared_ptr<Future<T>>> waiting_jobs;
    util::Mutex mutex;
    bool first;
    bool source_exhausted;
    bool finished;
    int num_active_subscriptions;
  };

  struct InnerCallback {
    void operator()(const Result<T>& maybe_next_ref) {
      Future<T> next_fut;
      const Result<T>* maybe_next = &maybe_next_ref;

      while (true) {
        Future<T> sink;
        bool sub_finished = maybe_next->ok() && IsIterationEnd(**maybe_next);
        {
          auto guard = state->mutex.Lock();
          if (state->finished) {
            // We've errored out so just ignore this result and don't keep pumping
            return;
          }
          if (!sub_finished) {
            if (state->waiting_jobs.empty()) {
              state->delivered_jobs.push_back(std::make_shared<DeliveredJob>(
                  state->active_subscriptions[index], *maybe_next, index));
            } else {
              sink = std::move(*state->waiting_jobs.front());
              state->waiting_jobs.pop_front();
            }
          }
        }
        if (sub_finished) {
          state->PullSource().AddCallback(OuterCallback{state, index});
        } else if (sink.is_valid()) {
          sink.MarkFinished(*maybe_next);
          if (!maybe_next->ok()) return;

          next_fut = state->active_subscriptions[index]();
          if (next_fut.TryAddCallback([this]() { return *this; })) {
            return;
          }
          // Already completed. Avoid very deep recursion by looping
          // here instead of relying on the callback.
          maybe_next = &next_fut.result();
          continue;
        }
        return;
      }
    }
    std::shared_ptr<State> state;
    std::size_t index;
  };

  struct OuterCallback {
    void operator()(const Result<AsyncGenerator<T>>& maybe_next) {
      bool should_purge = false;
      bool should_continue = false;
      Future<T> error_sink;
      {
        auto guard = state->mutex.Lock();
        if (!maybe_next.ok() || IsIterationEnd(*maybe_next)) {
          state->source_exhausted = true;
          if (!maybe_next.ok() || --state->num_active_subscriptions == 0) {
            state->finished = true;
            should_purge = true;
          }
          if (!maybe_next.ok()) {
            if (state->waiting_jobs.empty()) {
              state->delivered_jobs.push_back(std::make_shared<DeliveredJob>(
                  AsyncGenerator<T>(), maybe_next.status(), index));
            } else {
              error_sink = std::move(*state->waiting_jobs.front());
              state->waiting_jobs.pop_front();
            }
          }
        } else {
          state->active_subscriptions[index] = *maybe_next;
          should_continue = true;
        }
      }
      if (error_sink.is_valid()) {
        error_sink.MarkFinished(maybe_next.status());
      }
      if (should_continue) {
        (*maybe_next)().AddCallback(InnerCallback{state, index});
      } else if (should_purge) {
        // At this point state->finished has been marked true so no one else
        // will be interacting with waiting_jobs and we can iterate outside lock
        while (!state->waiting_jobs.empty()) {
          state->waiting_jobs.front()->MarkFinished(IterationTraits<T>::End());
          state->waiting_jobs.pop_front();
        }
      }
    }
    std::shared_ptr<State> state;
    std::size_t index;
  };

  std::shared_ptr<State> state_;
};

/// \brief Create a generator that takes in a stream of generators and pulls from up to
/// max_subscriptions at a time
///
/// Note: This may deliver items out of sequence. For example, items from the third
/// AsyncGenerator generated by the source may be emitted before some items from the first
/// AsyncGenerator generated by the source.
///
/// This generator will pull from source async-reentrantly unless max_subscriptions is 1
/// This generator will not pull from the individual subscriptions reentrantly.  Add
/// readahead to the individual subscriptions if that is desired.
/// This generator is async-reentrant
///
/// This generator may queue up to max_subscriptions instances of T
template <typename T>
AsyncGenerator<T> MakeMergedGenerator(AsyncGenerator<AsyncGenerator<T>> source,
                                      int max_subscriptions) {
  return MergedGenerator<T>(std::move(source), max_subscriptions);
}

template <typename T>
Result<AsyncGenerator<T>> MakeSequencedMergedGenerator(
    AsyncGenerator<AsyncGenerator<T>> source, int max_subscriptions) {
  if (max_subscriptions < 0) {
    return Status::Invalid("max_subscriptions must be a positive integer");
  }
  if (max_subscriptions == 1) {
    return Status::Invalid("Use MakeConcatenatedGenerator if max_subscriptions is 1");
  }
  AsyncGenerator<AsyncGenerator<T>> autostarting_source = MakeMappedGenerator(
      std::move(source),
      [](const AsyncGenerator<T>& sub) { return MakeAutoStartingGenerator(sub); });
  AsyncGenerator<AsyncGenerator<T>> sub_readahead =
      MakeSerialReadaheadGenerator(std::move(autostarting_source), max_subscriptions - 1);
  return MakeConcatenatedGenerator(std::move(sub_readahead));
}

/// \brief Create a generator that takes in a stream of generators and pulls from each
/// one in sequence.
///
/// This generator is async-reentrant but will never pull from source reentrantly and
/// will never pull from any subscription reentrantly.
///
/// This generator may queue 1 instance of T
///
/// TODO: Could potentially make a bespoke implementation instead of MergedGenerator that
/// forwards async-reentrant requests instead of buffering them (which is what
/// MergedGenerator does)
template <typename T>
AsyncGenerator<T> MakeConcatenatedGenerator(AsyncGenerator<AsyncGenerator<T>> source) {
  return MergedGenerator<T>(std::move(source), 1);
}

template <typename T>
struct Enumerated {
  T value;
  int index;
  bool last;
};

template <typename T>
struct IterationTraits<Enumerated<T>> {
  static Enumerated<T> End() { return Enumerated<T>{IterationEnd<T>(), -1, false}; }
  static bool IsEnd(const Enumerated<T>& val) { return val.index < 0; }
};

/// \see MakeEnumeratedGenerator
template <typename T>
class EnumeratingGenerator {
 public:
  EnumeratingGenerator(AsyncGenerator<T> source, T initial_value)
      : state_(std::make_shared<State>(std::move(source), std::move(initial_value))) {}

  Future<Enumerated<T>> operator()() {
    if (state_->finished) {
      return AsyncGeneratorEnd<Enumerated<T>>();
    } else {
      auto state = state_;
      return state->source().Then([state](const T& next) {
        auto finished = IsIterationEnd<T>(next);
        auto prev = Enumerated<T>{state->prev_value, state->prev_index, finished};
        state->prev_value = next;
        state->prev_index++;
        state->finished = finished;
        return prev;
      });
    }
  }

 private:
  struct State {
    State(AsyncGenerator<T> source, T initial_value)
        : source(std::move(source)), prev_value(std::move(initial_value)), prev_index(0) {
      finished = IsIterationEnd<T>(prev_value);
    }

    AsyncGenerator<T> source;
    T prev_value;
    int prev_index;
    bool finished;
  };

  std::shared_ptr<State> state_;
};

/// Wrap items from a source generator with positional information
///
/// When used with MakeMergedGenerator and MakeSequencingGenerator this allows items to be
/// processed in a "first-available" fashion and later resequenced which can reduce the
/// impact of sources with erratic performance (e.g. a filesystem where some items may
/// take longer to read than others).
///
/// TODO(ARROW-12371) Would require this generator be async-reentrant
///
/// \see MakeSequencingGenerator for an example of putting items back in order
///
/// This generator is not async-reentrant
///
/// This generator buffers one item (so it knows which item is the last item)
template <typename T>
AsyncGenerator<Enumerated<T>> MakeEnumeratedGenerator(AsyncGenerator<T> source) {
  return FutureFirstGenerator<Enumerated<T>>(
      source().Then([source](const T& initial_value) -> AsyncGenerator<Enumerated<T>> {
        return EnumeratingGenerator<T>(std::move(source), initial_value);
      }));
}

/// \see MakeTransferredGenerator
template <typename T>
class TransferringGenerator {
 public:
  explicit TransferringGenerator(AsyncGenerator<T> source, internal::Executor* executor)
      : source_(std::move(source)), executor_(executor) {}

  Future<T> operator()() { return executor_->Transfer(source_()); }

 private:
  AsyncGenerator<T> source_;
  internal::Executor* executor_;
};

/// \brief Transfer a future to an underlying executor.
///
/// Continuations run on the returned future will be run on the given executor
/// if they cannot be run synchronously.
///
/// This is often needed to move computation off I/O threads or other external
/// completion sources and back on to the CPU executor so the I/O thread can
/// stay busy and focused on I/O
///
/// Keep in mind that continuations called on an already completed future will
/// always be run synchronously and so no transfer will happen in that case.
///
/// This generator is async reentrant if the source is
///
/// This generator will not queue
template <typename T>
AsyncGenerator<T> MakeTransferredGenerator(AsyncGenerator<T> source,
                                           internal::Executor* executor) {
  return TransferringGenerator<T>(std::move(source), executor);
}

/// \see MakeBackgroundGenerator
template <typename T>
class BackgroundGenerator {
 public:
  explicit BackgroundGenerator(Iterator<T> it, internal::Executor* io_executor, int max_q,
                               int q_restart)
      : state_(std::make_shared<State>(io_executor, std::move(it), max_q, q_restart)),
        cleanup_(std::make_shared<Cleanup>(state_.get())) {}

  Future<T> operator()() {
    auto guard = state_->mutex.Lock();
    Future<T> waiting_future;
    if (state_->queue.empty()) {
      if (state_->finished) {
        return AsyncGeneratorEnd<T>();
      } else {
        waiting_future = Future<T>::Make();
        state_->waiting_future = waiting_future;
      }
    } else {
      auto next = Future<T>::MakeFinished(std::move(state_->queue.front()));
      state_->queue.pop();
      if (state_->NeedsRestart()) {
        return state_->RestartTask(state_, std::move(guard), std::move(next));
      }
      return next;
    }
    // This should only trigger the very first time this method is called
    if (state_->NeedsRestart()) {
      return state_->RestartTask(state_, std::move(guard), std::move(waiting_future));
    }
    return waiting_future;
  }

 protected:
  static constexpr uint64_t kUnlikelyThreadId{std::numeric_limits<uint64_t>::max()};

  struct State {
    State(internal::Executor* io_executor, Iterator<T> it, int max_q, int q_restart)
        : io_executor(io_executor),
          max_q(max_q),
          q_restart(q_restart),
          it(std::move(it)),
          reading(false),
          finished(false),
          should_shutdown(false) {}

    void ClearQueue() {
      while (!queue.empty()) {
        queue.pop();
      }
    }

    bool TaskIsRunning() const { return task_finished.is_valid(); }

    bool NeedsRestart() const {
      return !finished && !reading && static_cast<int>(queue.size()) <= q_restart;
    }

    void DoRestartTask(std::shared_ptr<State> state, util::Mutex::Guard guard) {
      // If we get here we are actually going to start a new task so let's create a
      // task_finished future for it
      state->task_finished = Future<>::Make();
      state->reading = true;
      auto spawn_status = io_executor->Spawn(
          [state]() { BackgroundGenerator::WorkerTask(std::move(state)); });
      if (!spawn_status.ok()) {
        // If we can't spawn a new task then send an error to the consumer (either via a
        // waiting future or the queue) and mark ourselves finished
        state->finished = true;
        state->task_finished = Future<>();
        if (waiting_future.has_value()) {
          auto to_deliver = std::move(waiting_future.value());
          waiting_future.reset();
          guard.Unlock();
          to_deliver.MarkFinished(spawn_status);
        } else {
          ClearQueue();
          queue.push(spawn_status);
        }
      }
    }

    Future<T> RestartTask(std::shared_ptr<State> state, util::Mutex::Guard guard,
                          Future<T> next) {
      if (TaskIsRunning()) {
        // If the task is still cleaning up we need to wait for it to finish before
        // restarting.  We also want to block the consumer until we've restarted the
        // reader to avoid multiple restarts
        return task_finished.Then([state, next]() {
          // This may appear dangerous (recursive mutex) but we should be guaranteed the
          // outer guard has been released by this point.  We know...
          // * task_finished is not already finished (it would be invalid in that case)
          // * task_finished will not be marked complete until we've given up the mutex
          auto guard_ = state->mutex.Lock();
          state->DoRestartTask(state, std::move(guard_));
          return next;
        });
      }
      // Otherwise we can restart immediately
      DoRestartTask(std::move(state), std::move(guard));
      return next;
    }

    internal::Executor* io_executor;
    const int max_q;
    const int q_restart;
    Iterator<T> it;
    std::atomic<uint64_t> worker_thread_id{kUnlikelyThreadId};

    // If true, the task is actively pumping items from the queue and does not need a
    // restart
    bool reading;
    // Set to true when a terminal item arrives
    bool finished;
    // Signal to the background task to end early because consumers have given up on it
    bool should_shutdown;
    // If the queue is empty, the consumer will create a waiting future and wait for it
    std::queue<Result<T>> queue;
    util::optional<Future<T>> waiting_future;
    // Every background task is given a future to complete when it is entirely finished
    // processing and ready for the next task to start or for State to be destroyed
    Future<> task_finished;
    util::Mutex mutex;
  };

  // Cleanup task that will be run when all consumer references to the generator are lost
  struct Cleanup {
    explicit Cleanup(State* state) : state(state) {}
    ~Cleanup() {
      /// TODO: Once ARROW-13109 is available then we can be force consumers to spawn and
      /// there is no need to perform this check.
      ///
      /// It's a deadlock if we enter cleanup from
      /// the worker thread but it can happen if the consumer doesn't transfer away
      assert(state->worker_thread_id.load() != ::arrow::internal::GetThreadId());
      Future<> finish_fut;
      {
        auto lock = state->mutex.Lock();
        if (!state->TaskIsRunning()) {
          return;
        }
        // Signal the current task to stop and wait for it to finish
        state->should_shutdown = true;
        finish_fut = state->task_finished;
      }
      // Using future as a condition variable here
      Status st = finish_fut.status();
      ARROW_UNUSED(st);
    }
    State* state;
  };

  static void WorkerTask(std::shared_ptr<State> state) {
    state->worker_thread_id.store(::arrow::internal::GetThreadId());
    // We need to capture the state to read while outside the mutex
    bool reading = true;
    while (reading) {
      auto next = state->it.Next();
      // Need to capture state->waiting_future inside the mutex to mark finished outside
      Future<T> waiting_future;
      {
        auto guard = state->mutex.Lock();

        if (state->should_shutdown) {
          state->finished = true;
          break;
        }

        if (!next.ok() || IsIterationEnd<T>(*next)) {
          // Terminal item.  Mark finished to true, send this last item, and quit
          state->finished = true;
          if (!next.ok()) {
            state->ClearQueue();
          }
        }
        // At this point we are going to send an item.  Either we will add it to the
        // queue or deliver it to a waiting future.
        if (state->waiting_future.has_value()) {
          waiting_future = std::move(state->waiting_future.value());
          state->waiting_future.reset();
        } else {
          state->queue.push(std::move(next));
          // We just filled up the queue so it is time to quit.  We may need to notify
          // a cleanup task so we transition to Quitting
          if (static_cast<int>(state->queue.size()) >= state->max_q) {
            state->reading = false;
          }
        }
        reading = state->reading && !state->finished;
      }
      // This should happen outside the mutex.  Presumably there is a
      // transferring generator on the other end that will quickly transfer any
      // callbacks off of this thread so we can continue looping.  Still, best not to
      // rely on that
      if (waiting_future.is_valid()) {
        waiting_future.MarkFinished(next);
      }
    }
    // Once we've sent our last item we can notify any waiters that we are done and so
    // either state can be cleaned up or a new background task can be started
    Future<> task_finished;
    {
      auto guard = state->mutex.Lock();
      // After we give up the mutex state can be safely deleted.  We will no longer
      // reference it.  We can safely transition to idle now.
      task_finished = state->task_finished;
      state->task_finished = Future<>();
      state->worker_thread_id.store(kUnlikelyThreadId);
    }
    task_finished.MarkFinished();
  }

  std::shared_ptr<State> state_;
  // state_ is held by both the generator and the background thread so it won't be cleaned
  // up when all consumer references are relinquished.  cleanup_ is only held by the
  // generator so it will be destructed when the last consumer reference is gone.  We use
  // this to cleanup / stop the background generator in case the consuming end stops
  // listening (e.g. due to a downstream error)
  std::shared_ptr<Cleanup> cleanup_;
};

constexpr int kDefaultBackgroundMaxQ = 32;
constexpr int kDefaultBackgroundQRestart = 16;

/// \brief Create an AsyncGenerator<T> by iterating over an Iterator<T> on a background
/// thread
///
/// The parameter max_q and q_restart control queue size and background thread task
/// management. If the background task is fast you typically don't want it creating a
/// thread task for every item.  Instead the background thread will run until it fills
/// up a readahead queue.
///
/// Once the queue has filled up the background thread task will terminate (allowing other
/// I/O tasks to use the thread).  Once the queue has been drained enough (specified by
/// q_restart) then the background thread task will be restarted.  If q_restart is too low
/// then you may exhaust the queue waiting for the background thread task to start running
/// again.  If it is too high then it will be constantly stopping and restarting the
/// background queue task
///
/// The "background thread" is a logical thread and will run as tasks on the io_executor.
/// This thread may stop and start when the queue fills up but there will only be one
/// active background thread task at any given time.  You MUST transfer away from this
/// background generator.  Otherwise there could be a race condition if a callback on the
/// background thread deletes the last consumer reference to the background generator. You
/// can transfer onto the same executor as the background thread, it is only neccesary to
/// create a new thread task, not to switch executors.
///
/// This generator is not async-reentrant
///
/// This generator will queue up to max_q blocks
template <typename T>
static Result<AsyncGenerator<T>> MakeBackgroundGenerator(
    Iterator<T> iterator, internal::Executor* io_executor,
    int max_q = kDefaultBackgroundMaxQ, int q_restart = kDefaultBackgroundQRestart) {
  if (max_q < q_restart) {
    return Status::Invalid("max_q must be >= q_restart");
  }
  return BackgroundGenerator<T>(std::move(iterator), io_executor, max_q, q_restart);
}

/// \see MakeGeneratorIterator
template <typename T>
class GeneratorIterator {
 public:
  explicit GeneratorIterator(AsyncGenerator<T> source) : source_(std::move(source)) {}

  Result<T> Next() { return source_().result(); }

 private:
  AsyncGenerator<T> source_;
};

/// \brief Convert an AsyncGenerator<T> to an Iterator<T> which blocks until each future
/// is finished
template <typename T>
Iterator<T> MakeGeneratorIterator(AsyncGenerator<T> source) {
  return Iterator<T>(GeneratorIterator<T>(std::move(source)));
}

/// \brief Add readahead to an iterator using a background thread.
///
/// Under the hood this is converting the iterator to a generator using
/// MakeBackgroundGenerator, adding readahead to the converted generator with
/// MakeReadaheadGenerator, and then converting back to an iterator using
/// MakeGeneratorIterator.
template <typename T>
Result<Iterator<T>> MakeReadaheadIterator(Iterator<T> it, int readahead_queue_size) {
  ARROW_ASSIGN_OR_RAISE(auto io_executor, internal::ThreadPool::Make(1));
  auto max_q = readahead_queue_size;
  auto q_restart = std::max(1, max_q / 2);
  ARROW_ASSIGN_OR_RAISE(
      auto background_generator,
      MakeBackgroundGenerator(std::move(it), io_executor.get(), max_q, q_restart));
  // Capture io_executor to keep it alive as long as owned_bg_generator is still
  // referenced
  AsyncGenerator<T> owned_bg_generator = [io_executor, background_generator]() {
    return background_generator();
  };
  return MakeGeneratorIterator(std::move(owned_bg_generator));
}

/// \brief Make a generator that returns a single pre-generated future
///
/// This generator is async-reentrant.
template <typename T>
std::function<Future<T>()> MakeSingleFutureGenerator(Future<T> future) {
  assert(future.is_valid());
  auto state = std::make_shared<Future<T>>(std::move(future));
  return [state]() -> Future<T> {
    auto fut = std::move(*state);
    if (fut.is_valid()) {
      return fut;
    } else {
      return AsyncGeneratorEnd<T>();
    }
  };
}

/// \brief Make a generator that immediately ends.
///
/// This generator is async-reentrant.
template <typename T>
std::function<Future<T>()> MakeEmptyGenerator() {
  return []() -> Future<T> { return AsyncGeneratorEnd<T>(); };
}

/// \brief Make a generator that always fails with a given error
///
/// This generator is async-reentrant.
template <typename T>
AsyncGenerator<T> MakeFailingGenerator(Status st) {
  assert(!st.ok());
  auto state = std::make_shared<Status>(std::move(st));
  return [state]() -> Future<T> {
    auto st = std::move(*state);
    if (!st.ok()) {
      return std::move(st);
    } else {
      return AsyncGeneratorEnd<T>();
    }
  };
}

/// \brief Make a generator that always fails with a given error
///
/// This overload allows inferring the return type from the argument.
template <typename T>
AsyncGenerator<T> MakeFailingGenerator(const Result<T>& result) {
  return MakeFailingGenerator<T>(result.status());
}

/// \brief Prepend initial_values onto a generator
///
/// This generator is async-reentrant but will buffer requests and will not
/// pull from following_values async-reentrantly.
template <typename T>
AsyncGenerator<T> MakeGeneratorStartsWith(std::vector<T> initial_values,
                                          AsyncGenerator<T> following_values) {
  auto initial_values_vec_gen = MakeVectorGenerator(std::move(initial_values));
  auto gen_gen = MakeVectorGenerator<AsyncGenerator<T>>(
      {std::move(initial_values_vec_gen), std::move(following_values)});
  return MakeConcatenatedGenerator(std::move(gen_gen));
}

template <typename T>
struct CancellableGenerator {
  Future<T> operator()() {
    if (stop_token.IsStopRequested()) {
      return stop_token.Poll();
    }
    return source();
  }

  AsyncGenerator<T> source;
  StopToken stop_token;
};

/// \brief Allow an async generator to be cancelled
///
/// This generator is async-reentrant
template <typename T>
AsyncGenerator<T> MakeCancellable(AsyncGenerator<T> source, StopToken stop_token) {
  return CancellableGenerator<T>{std::move(source), std::move(stop_token)};
}

template <typename T>
struct PauseableGenerator {
 public:
  PauseableGenerator(AsyncGenerator<T> source, std::shared_ptr<util::AsyncToggle> toggle)
      : state_(std::make_shared<PauseableGeneratorState>(std::move(source),
                                                         std::move(toggle))) {}

  Future<T> operator()() { return (*state_)(); }

 private:
  struct PauseableGeneratorState
      : public std::enable_shared_from_this<PauseableGeneratorState> {
    PauseableGeneratorState(AsyncGenerator<T> source,
                            std::shared_ptr<util::AsyncToggle> toggle)
        : source_(std::move(source)), toggle_(std::move(toggle)) {}

    Future<T> operator()() {
      std::shared_ptr<PauseableGeneratorState> self = this->shared_from_this();
      return toggle_->WhenOpen().Then([self] {
        util::Mutex::Guard guard = self->mutex_.Lock();
        return self->source_();
      });
    }

    AsyncGenerator<T> source_;
    std::shared_ptr<util::AsyncToggle> toggle_;
    util::Mutex mutex_;
  };
  std::shared_ptr<PauseableGeneratorState> state_;
};

/// \brief Allow an async generator to be paused
///
/// This generator is NOT async-reentrant and calling it in an async-reentrant fashion
/// may lead to items getting reordered (and potentially truncated if the end token is
/// reordered ahead of valid items)
///
/// This generator forwards async-reentrant pressure
template <typename T>
AsyncGenerator<T> MakePauseable(AsyncGenerator<T> source,
                                std::shared_ptr<util::AsyncToggle> toggle) {
  return PauseableGenerator<T>(std::move(source), std::move(toggle));
}

template <typename T>
class DefaultIfEmptyGenerator {
 public:
  DefaultIfEmptyGenerator(AsyncGenerator<T> source, T or_value)
      : state_(std::make_shared<State>(std::move(source), std::move(or_value))) {}

  Future<T> operator()() {
    if (state_->first) {
      state_->first = false;
      struct {
        T or_value;

        Result<T> operator()(const T& value) {
          if (IterationTraits<T>::IsEnd(value)) {
            return std::move(or_value);
          }
          return value;
        }
      } Continuation;
      Continuation.or_value = std::move(state_->or_value);
      return state_->source().Then(std::move(Continuation));
    }
    return state_->source();
  }

 private:
  struct State {
    AsyncGenerator<T> source;
    T or_value;
    bool first;
    State(AsyncGenerator<T> source_, T or_value_)
        : source(std::move(source_)), or_value(std::move(or_value_)), first(true) {}
  };
  std::shared_ptr<State> state_;
};

/// \brief If the generator is empty, return the given value, else
/// forward the values from the generator.
///
/// This generator is async-reentrant.
template <typename T>
AsyncGenerator<T> MakeDefaultIfEmptyGenerator(AsyncGenerator<T> source, T or_value) {
  return DefaultIfEmptyGenerator<T>(std::move(source), std::move(or_value));
}
}  // namespace arrow
