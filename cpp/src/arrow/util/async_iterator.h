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
#include <iostream>
#include <queue>

#include "arrow/util/functional.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

template <typename T>
using AsyncGenerator = std::function<Future<T>()>;

/// Iterates through a generator of futures, visiting the result of each one and
/// returning a future that completes when all have been visited
template <typename T>
Future<> VisitAsyncGenerator(AsyncGenerator<T> generator,
                             std::function<Status(T)> visitor) {
  struct LoopBody {
    struct Callback {
      Result<ControlFlow<detail::Empty>> operator()(const T& result) {
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

template <typename T>
Future<std::vector<T>> CollectAsyncGenerator(AsyncGenerator<T> generator) {
  auto vec = std::make_shared<std::vector<T>>();
  struct LoopBody {
    Future<ControlFlow<std::vector<T>>> operator()() {
      auto next = generator();
      auto vec = vec_;
      return next.Then([vec](const T& result) -> Result<ControlFlow<std::vector<T>>> {
        if (result == IterationTraits<T>::End()) {
          return Break(*vec);
        } else {
          vec->push_back(result);
          return Continue();
        }
      });
    }
    AsyncGenerator<T> generator;
    std::shared_ptr<std::vector<T>> vec_;
  };
  return Loop(LoopBody{std::move(generator), std::move(vec)});
}

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
        // If finished already, process results immediately inside the loop to avoid stack
        // overflow
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
          if (*last_value_ == IterationTraits<T>::End()) {
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

template <typename T>
class ReadaheadGenerator {
 public:
  ReadaheadGenerator(AsyncGenerator<T> source_generator, int max_readahead)
      : source_generator_(std::move(source_generator)), max_readahead_(max_readahead) {
    auto finished = std::make_shared<std::atomic<bool>>();
    mark_finished_if_done_ = [finished](const Result<T>& next_result) {
      if (!next_result.ok()) {
        finished->store(true);
      } else {
        const auto& next = *next_result;
        if (next == IterationTraits<T>::End()) {
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
      readahead_queue_.push(Future<T>::MakeFinished(IterationTraits<T>::End()));
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

/// \brief Creates a generator that pulls reentrantly from a source
/// This generator will pull reentrantly from a source, ensuring that max_readahead
/// requests are active at any given time.
///
/// The source generator must be async-reentrant
///
/// This generator itself is async-reentrant.
template <typename T>
AsyncGenerator<T> AddReadahead(AsyncGenerator<T> source_generator, int max_readahead) {
  return ReadaheadGenerator<T>(std::move(source_generator), max_readahead);
}

/// \brief Transforms an async generator using a transformer function returning a new
/// AsyncGenerator
///
/// The transform function here behaves exactly the same as the transform function in
/// MakeTransformedIterator and you can safely use the same transform function to
/// transform both synchronous and asynchronous streams.
///
/// This generator is not async-reentrant
template <typename T, typename V>
AsyncGenerator<V> TransformAsyncGenerator(AsyncGenerator<T> generator,
                                          Transformer<T, V> transformer) {
  return TransformingGenerator<T, V>(generator, transformer);
}

/// \brief Transfers execution of the generator onto the given executor
///
/// This generator is async-reentrant if the source generator is async-reentrant
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
template <typename T>
AsyncGenerator<T> TransferGenerator(AsyncGenerator<T> source,
                                    internal::Executor* executor) {
  return TransferringGenerator<T>(std::move(source), executor);
}

/// \brief Async generator that iterates on an underlying iterator in a
/// separate executor.
///
/// This generator is async-reentrant
template <typename T>
class BackgroundGenerator {
 public:
  explicit BackgroundGenerator(Iterator<T> it,
                               std::shared_ptr<internal::ThreadPool> background_executor)
      : background_executor_(std::move(background_executor)) {
    task_ =
        Task{std::make_shared<Iterator<T>>(std::move(it)), std::make_shared<bool>(false)};
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
    auto submitted_future = background_executor_->Submit(task_);
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
      if (!next.ok() || *next == IterationTraits<T>::End()) {
        *done_ = true;
      }
      return next;
    }
    // This task is going to be copied so we need to convert the iterator ptr to
    // a shared ptr.  This should be safe however because the background executor only
    // has a single thread so it can't access it_ across multiple threads.
    std::shared_ptr<Iterator<T>> it_;
    std::shared_ptr<bool> done_;
  };

  Task task_;
  std::shared_ptr<internal::ThreadPool> background_executor_;
};

/// \brief Creates an AsyncGenerator<T> by iterating over an Iterator<T> on a background
/// thread
template <typename T>
static Result<AsyncGenerator<T>> MakeBackgroundGenerator(Iterator<T> iterator) {
  ARROW_ASSIGN_OR_RAISE(auto background_executor, internal::ThreadPool::Make(1));
  auto background_iterator = std::make_shared<BackgroundGenerator<T>>(
      std::move(iterator), std::move(background_executor));
  return [background_iterator]() { return (*background_iterator)(); };
}

/// \brief Converts an AsyncGenerator<T> to an Iterator<T> by blocking until each future
/// is finished
template <typename T>
class GeneratorIterator {
 public:
  explicit GeneratorIterator(AsyncGenerator<T> source) : source_(std::move(source)) {}

  Result<T> Next() { return source_().result(); }

 private:
  AsyncGenerator<T> source_;
};

template <typename T>
Result<Iterator<T>> MakeGeneratorIterator(AsyncGenerator<T> source) {
  return Iterator<T>(GeneratorIterator<T>(std::move(source)));
}

template <typename T>
Result<Iterator<T>> MakeReadaheadIterator(Iterator<T> it, int readahead_queue_size) {
  ARROW_ASSIGN_OR_RAISE(auto background_generator,
                        MakeBackgroundGenerator(std::move(it)));
  auto readahead_generator =
      AddReadahead(std::move(background_generator), readahead_queue_size);
  return MakeGeneratorIterator(std::move(readahead_generator));
}

}  // namespace arrow
