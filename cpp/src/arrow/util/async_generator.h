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

#include <cassert>
#include <deque>
#include <queue>

#include "arrow/util/functional.h"
#include "arrow/util/future.h"
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
// future returned by the function is marekd finished (but after the call to get the
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
template <typename T>
Future<> VisitAsyncGenerator(AsyncGenerator<T> generator,
                             std::function<Status(T)> visitor) {
  struct LoopBody {
    struct Callback {
      Result<ControlFlow<detail::Empty>> operator()(const T& result) {
        if (IsIterationEnd(result)) {
          return Break(detail::Empty());
        } else {
          auto visited = visitor(result);
          if (visited.ok()) {
            return Continue();
          } else {
            return visited;
          }
        }
      }

      std::function<Status(T)> visitor;
    };

    Future<ControlFlow<detail::Empty>> operator()() {
      Callback callback{visitor};
      auto next = generator();
      return next.Then(std::move(callback));
    }

    AsyncGenerator<T> generator;
    std::function<Status(T)> visitor;
  };

  return Loop(LoopBody{std::move(generator), std::move(visitor)});
}

/// \brief Waits for an async generator to complete, discarding results.
template <typename T>
Future<> DiscardAllFromAsyncGenerator(AsyncGenerator<T> generator) {
  std::function<Status(T)> visitor = [](...) { return Status::OK(); };
  return VisitAsyncGenerator(generator, visitor);
}

/// \brief Collects the results of an async generator into a vector
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

/// \brief Creates a generator that will apply the map function to each element of
/// source.  The map function is not called on the end token.
///
/// Note: This function makes a copy of `map` for each item
/// Note: Errors returned from the `map` function will be propagated
///
/// If the source generator is async-reentrant then this generator will be also
template <typename T, typename V>
AsyncGenerator<V> MakeMappedGenerator(AsyncGenerator<T> source_generator,
                                      std::function<Result<V>(const T&)> map) {
  std::function<Future<V>(const T&)> future_map = [map](const T& val) -> Future<V> {
    return Future<V>::MakeFinished(map(val));
  };
  return MappingGenerator<T, V>(std::move(source_generator), std::move(future_map));
}
template <typename T, typename V>
AsyncGenerator<V> MakeMappedGenerator(AsyncGenerator<T> source_generator,
                                      std::function<V(const T&)> map) {
  std::function<Future<V>(const T&)> maybe_future_map = [map](const T& val) -> Future<V> {
    return Future<V>::MakeFinished(map(val));
  };
  return MappingGenerator<T, V>(std::move(source_generator), std::move(maybe_future_map));
}
template <typename T, typename V>
AsyncGenerator<V> MakeMappedGenerator(AsyncGenerator<T> source_generator,
                                      std::function<Future<V>(const T&)> map) {
  return MappingGenerator<T, V>(std::move(source_generator), std::move(map));
}

/// \see MakeAsyncGenerator
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
          return next_fut.Then([self](const Result<T>& next_result) {
            if (next_result.ok()) {
              self->last_value_ = *next_result;
              return (*self)();
            } else {
              return Future<V>::MakeFinished(next_result.status());
            }
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

/// \brief Transforms an async generator using a transformer function returning a new
/// AsyncGenerator
///
/// The transform function here behaves exactly the same as the transform function in
/// MakeTransformedIterator and you can safely use the same transform function to
/// transform both synchronous and asynchronous streams.
///
/// This generator is not async-reentrant
///
/// This generator may queue up to 1 instance of T
template <typename T, typename V>
AsyncGenerator<V> MakeAsyncGenerator(AsyncGenerator<T> generator,
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
      return next.Then(Callback{state_});
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
      // Can't do readahead_queue.write(source().Then(Callback{self})) because then the
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
      *next_slot = source_().Then(Callback{self});
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
    Result<T> operator()(const Result<T>& maybe_next) {
      if (!maybe_next.ok()) {
        state_->finished_.store(true);
        return maybe_next;
      }
      const auto& next = *maybe_next;
      if (IsIterationEnd(next)) {
        state_->finished_.store(true);
        return maybe_next;
      }
      auto last_available = state_->spaces_available_.fetch_sub(1);
      if (last_available > 1) {
        ARROW_RETURN_NOT_OK(state_->Pump(state_));
      }
      return maybe_next;
    }

    std::shared_ptr<State> state_;
  };

  std::shared_ptr<State> state_;
};

/// \brief Creates a generator that will pull from the source into a queue.  Unlike
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

/// \see MakeReadaheadGenerator
template <typename T>
class ReadaheadGenerator {
 public:
  ReadaheadGenerator(AsyncGenerator<T> source_generator, int max_readahead)
      : source_generator_(std::move(source_generator)), max_readahead_(max_readahead) {
    auto finished = std::make_shared<std::atomic<bool>>(false);
    mark_finished_if_done_ = [finished](const Result<T>& next_result) {
      if (!next_result.ok()) {
        finished->store(true);
      } else {
        if (IsIterationEnd(*next_result)) {
          *finished = true;
        }
      }
    };
    finished_ = std::move(finished);
  }

  Future<T> operator()() {
    if (readahead_queue_.empty()) {
      // This is the first request, let's pump the underlying queue
      for (int i = 0; i < max_readahead_; i++) {
        auto next = source_generator_();
        next.AddCallback(mark_finished_if_done_);
        readahead_queue_.push(std::move(next));
      }
    }
    // Pop one and add one
    auto result = readahead_queue_.front();
    readahead_queue_.pop();
    if (finished_->load()) {
      readahead_queue_.push(AsyncGeneratorEnd<T>());
    } else {
      auto back_of_queue = source_generator_();
      back_of_queue.AddCallback(mark_finished_if_done_);
      readahead_queue_.push(std::move(back_of_queue));
    }
    return result;
  }

 private:
  AsyncGenerator<T> source_generator_;
  int max_readahead_;
  std::function<void(const Result<T>&)> mark_finished_if_done_;
  // Can't use a bool here because finished may be referenced by callbacks that
  // outlive this class
  std::shared_ptr<std::atomic<bool>> finished_;
  std::queue<Future<T>> readahead_queue_;
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
    util::Mutex mutex;
    std::deque<Result<T>> result_q;
    util::optional<Future<T>> consumer_fut;
    bool finished = false;
  };

 public:
  /// Producer API for PushGenerator
  class Producer {
   public:
    explicit Producer(std::shared_ptr<State> state) : state_(std::move(state)) {}

    /// Push a value on the queue
    void Push(Result<T> result) {
      auto lock = state_->mutex.Lock();
      if (state_->finished) {
        // Closed early
        return;
      }
      if (state_->consumer_fut.has_value()) {
        auto fut = std::move(state_->consumer_fut.value());
        state_->consumer_fut.reset();
        lock.Unlock();  // unlock before potentially invoking a callback
        fut.MarkFinished(std::move(result));
        return;
      }
      state_->result_q.push_back(std::move(result));
    }

    /// \brief Tell the consumer we have finished producing
    ///
    /// It is allowed to call this and later call Push() again ("early close").
    /// In this case, calls to Push() after the queue is closed are silently
    /// ignored.  This can help implementing non-trivial cancellation cases.
    void Close() {
      auto lock = state_->mutex.Lock();
      if (state_->finished) {
        // Already closed
        return;
      }
      state_->finished = true;
      if (state_->consumer_fut.has_value()) {
        auto fut = std::move(state_->consumer_fut.value());
        state_->consumer_fut.reset();
        lock.Unlock();  // unlock before potentially invoking a callback
        fut.MarkFinished(IterationTraits<T>::End());
      }
    }

    bool is_closed() const {
      auto lock = state_->mutex.Lock();
      return state_->finished;
    }

   private:
    const std::shared_ptr<State> state_;
  };

  PushGenerator() : state_(std::make_shared<State>()) {}

  /// Read an item from the queue
  Future<T> operator()() {
    auto lock = state_->mutex.Lock();
    assert(!state_->consumer_fut.has_value());  // Non-reentrant
    if (!state_->result_q.empty()) {
      auto fut = Future<T>::MakeFinished(std::move(state_->result_q.front()));
      state_->result_q.pop_front();
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

/// \brief Creates a generator that pulls reentrantly from a source
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
        state_->source().AddCallback(OuterCallback{state_, i});
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
    void operator()(const Result<T>& maybe_next) {
      Future<T> sink;
      bool sub_finished = maybe_next.ok() && IsIterationEnd(*maybe_next);
      {
        auto guard = state->mutex.Lock();
        if (state->finished) {
          // We've errored out so just ignore this result and don't keep pumping
          return;
        }
        if (!sub_finished) {
          if (state->waiting_jobs.empty()) {
            state->delivered_jobs.push_back(std::make_shared<DeliveredJob>(
                state->active_subscriptions[index], maybe_next, index));
          } else {
            sink = std::move(*state->waiting_jobs.front());
            state->waiting_jobs.pop_front();
          }
        }
      }
      if (sub_finished) {
        state->source().AddCallback(OuterCallback{state, index});
      } else if (sink.is_valid()) {
        sink.MarkFinished(maybe_next);
        if (maybe_next.ok()) {
          state->active_subscriptions[index]().AddCallback(*this);
        }
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

/// \brief Creates a generator that takes in a stream of generators and pulls from up to
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

/// \brief Creates a generator that takes in a stream of generators and pulls from each
/// one in sequence.
///
/// This generator is async-reentrant but will never pull from source reentrantly and
/// will never pull from any subscription reentrantly.
///
/// This generator may queue 1 instance of T
template <typename T>
AsyncGenerator<T> MakeConcatenatedGenerator(AsyncGenerator<AsyncGenerator<T>> source) {
  return MergedGenerator<T>(std::move(source), 1);
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

/// \brief Transfers a future to an underlying executor.
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
/// \see MakeIteratorGenerator
template <typename T>
class IteratorGenerator {
 public:
  explicit IteratorGenerator(Iterator<T> it) : it_(std::move(it)) {}

  Future<T> operator()() { return Future<T>::MakeFinished(it_.Next()); }

 private:
  Iterator<T> it_;
};

/// \brief Constructs a generator that yields futures from an iterator.
///
/// Note: Do not use this if you can avoid it.  This blocks in an async
/// context which is a bad idea.  If you're converting sync-I/O to async
/// then use MakeBackgroundGenerator.  Otherwise, convert the underlying
/// source to async.  This function is only around until we can conver the
/// remaining table readers to async.  Once all uses of this generator have
/// been removed it should be removed(ARROW-11909).
///
/// This generator is not async-reentrant
///
/// This generator will not queue
template <typename T>
AsyncGenerator<T> MakeIteratorGenerator(Iterator<T> it) {
  return IteratorGenerator<T>(std::move(it));
}

/// \see MakeBackgroundGenerator
template <typename T>
class BackgroundGenerator {
 public:
  explicit BackgroundGenerator(Iterator<T> it, internal::Executor* io_executor)
      : io_executor_(io_executor) {
    task_ = Task{std::make_shared<Iterator<T>>(std::move(it)),
                 std::make_shared<std::atomic<bool>>(false)};
  }

  ~BackgroundGenerator() {
    // The thread pool will be disposed of automatically.  By default it will not wait
    // so the background thread may outlive this object.  That should be ok.  Any task
    // objects in the thread pool are copies of task_ and have their own shared_ptr to
    // the iterator.
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(BackgroundGenerator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(BackgroundGenerator);

  Future<T> operator()() {
    auto submitted_future = io_executor_->Submit(task_);
    if (!submitted_future.ok()) {
      return Future<T>::MakeFinished(submitted_future.status());
    }
    return std::move(*submitted_future);
  }

 protected:
  struct Task {
    Result<T> operator()() {
      if (*done_) {
        return IterationTraits<T>::End();
      }
      auto next = it_->Next();
      if (!next.ok() || IsIterationEnd(*next)) {
        *done_ = true;
      }
      return next;
    }
    // This task is going to be copied so we need to convert the iterator ptr to
    // a shared ptr.  This should be safe however because the background executor only
    // has a single thread so it can't access it_ across multiple threads.
    std::shared_ptr<Iterator<T>> it_;
    std::shared_ptr<std::atomic<bool>> done_;
  };

  Task task_;
  internal::Executor* io_executor_;
};

/// \brief Creates an AsyncGenerator<T> by iterating over an Iterator<T> on a background
/// thread
///
/// This generator is async-reentrant
///
/// This generator will not queue
template <typename T>
static Result<AsyncGenerator<T>> MakeBackgroundGenerator(
    Iterator<T> iterator, internal::Executor* io_executor) {
  auto background_iterator = std::make_shared<BackgroundGenerator<T>>(
      std::move(iterator), std::move(io_executor));
  return [background_iterator]() { return (*background_iterator)(); };
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

/// \brief Converts an AsyncGenerator<T> to an Iterator<T> by blocking until each future
/// is finished
template <typename T>
Result<Iterator<T>> MakeGeneratorIterator(AsyncGenerator<T> source) {
  return Iterator<T>(GeneratorIterator<T>(std::move(source)));
}

/// \brief Adds readahead to an iterator using a background thread.
///
/// Under the hood this is converting the iterator to a generator using
/// MakeBackgroundGenerator, adding readahead to the converted generator with
/// MakeReadaheadGenerator, and then converting back to an iterator using
/// MakeGeneratorIterator.
template <typename T>
Result<Iterator<T>> MakeReadaheadIterator(Iterator<T> it, int readahead_queue_size) {
  ARROW_ASSIGN_OR_RAISE(auto io_executor, internal::ThreadPool::Make(1));
  ARROW_ASSIGN_OR_RAISE(auto background_generator,
                        MakeBackgroundGenerator(std::move(it), io_executor.get()));
  // Capture io_executor to keep it alive as long as owned_bg_generator is still
  // referenced
  AsyncGenerator<T> owned_bg_generator = [io_executor, background_generator]() {
    return background_generator();
  };
  auto readahead_generator =
      MakeReadaheadGenerator(std::move(owned_bg_generator), readahead_queue_size);
  return MakeGeneratorIterator(std::move(readahead_generator));
}

}  // namespace arrow
