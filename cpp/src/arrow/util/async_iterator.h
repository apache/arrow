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
 public:
  explicit TransformingGenerator(AsyncGenerator<T> generator,
                                 Transformer<T, V> transformer)
      : finished_(),
        last_value_(),
        generator_(std::move(generator)),
        transformer_(std::move(transformer)) {}

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
          last_value_ = *std::move(next_result);
        } else {
          return Future<V>::MakeFinished(next_result.status());
        }
        // Otherwise, if not finished immediately, add callback to process results
      } else {
        return next_fut.Then([this](const Result<T>& next_result) {
          if (next_result.ok()) {
            last_value_ = *std::move(next_result);
            return (*this)();
          } else {
            return Future<V>::MakeFinished(next_result.status());
          }
        });
      }
    }
  }

 protected:
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

  bool finished_;
  util::optional<T> last_value_;
  AsyncGenerator<T> generator_;
  Transformer<T, V> transformer_;
};

template <typename T>
class ReadaheadGenerator {
 public:
  ReadaheadGenerator(AsyncGenerator<T> source_generator, int max_readahead)
      : source_generator_(std::move(source_generator)), max_readahead_(max_readahead) {
    auto finished = std::make_shared<bool>();
    mark_finished_if_done_ = [finished](const Result<T>& next_result) {
      if (!next_result.ok()) {
        *finished = true;
      } else {
        const auto& next = *next_result;
        *finished = (next == IterationTraits<T>::End());
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
    if (*finished_) {
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
  std::shared_ptr<bool> finished_;
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

namespace detail {

template <typename T>
struct BackgroundGeneratorPromise : ReadaheadPromise {
  ~BackgroundGeneratorPromise() override {}

  explicit BackgroundGeneratorPromise(Iterator<T>* it) : it_(it) {}

  bool Call() override {
    auto next = it_->Next();
    auto finished = next == IterationTraits<T>::End();
    out_.MarkFinished(std::move(next));
    return finished;
  }

  void End() override { out_.MarkFinished(IterationTraits<T>::End()); }

  Iterator<T>* it_;
  Future<T> out_ = Future<T>::Make();
};

}  // namespace detail

/// \brief Async generator that iterates on an underlying iterator in a
/// separate thread.
///
/// This generator is async-reentrant
template <typename T>
class BackgroundGenerator {
  using PromiseType = typename detail::BackgroundGeneratorPromise<T>;

 public:
  explicit BackgroundGenerator(Iterator<T> it, internal::Executor* executor)
      : it_(new Iterator<T>(std::move(it))),
        queue_(new detail::ReadaheadQueue(0)),
        executor_(executor),
        done_() {}

  ~BackgroundGenerator() {
    if (queue_) {
      // Make sure the queue doesn't call any promises after this object
      // is destroyed.
      queue_->EnsureShutdownOrDie();
    }
  }

  ARROW_DEFAULT_MOVE_AND_ASSIGN(BackgroundGenerator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(BackgroundGenerator);

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
  bool done_;
};

/// \brief Creates an AsyncGenerator<T> by iterating over an Iterator<T> on a background
/// thread
template <typename T>
static Result<AsyncGenerator<T>> MakeBackgroundGenerator(Iterator<T> iterator,
                                                         internal::Executor* executor) {
  auto background_iterator =
      std::make_shared<BackgroundGenerator<T>>(std::move(iterator), executor);
  return [background_iterator]() { return (*background_iterator)(); };
}

}  // namespace arrow
