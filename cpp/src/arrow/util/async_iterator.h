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

template <typename T>
using AsyncGenerator = std::function<Future<T>()>;

/// Iterates through a generator of futures, visiting the result of each one and
/// returning a future that completes when all have been visited
template <typename T>
Future<> VisitAsyncGenerator(AsyncGenerator<T> generator,
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
Future<std::vector<T>> CollectAsyncGenerator(AsyncGenerator<T> generator) {
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

template <typename T, typename V>
class TransformingGenerator {
 public:
  explicit TransformingGenerator(AsyncGenerator<T> generator,
                                 std::function<TransformFlow<V>(T)> transformer)
      : finished_(), last_value_(), generator_(generator), transformer_(transformer) {}

  util::optional<V> Pump() {
    while (!finished_ && last_value_.has_value()) {
      TransformFlow<V> next = transformer_(*last_value_);
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
    return util::optional<V>();
  }

  Future<V> operator()() {
    while (true) {
      auto maybe_next = Pump();
      if (maybe_next.has_value()) {
        return Future<V>::MakeFinished(*maybe_next);
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
        return next_fut.Then([this](const Result<T>& next_result) {
          if (next_result.ok()) {
            last_value_ = *next_result;
            return (*this)();
          } else {
            return Future<V>::MakeFinished(next_result.status());
          }
        });
      }
    }
  }

 protected:
  bool finished_;
  util::optional<T> last_value_;
  AsyncGenerator<T> generator_;
  Transformer<T, V> transformer_;
};

template <typename T, typename V>
AsyncGenerator<V> TransformAsyncGenerator(AsyncGenerator<T> generator,
                                          Transformer<T, V> transformer) {
  return TransformingGenerator<T, V>(generator, transformer);
}

namespace detail {

template <typename T>
struct BackgroundIteratorPromise : ReadaheadPromise {
  ~BackgroundIteratorPromise() override {}

  explicit BackgroundIteratorPromise(Iterator<T>* it) : it_(it) {}

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

/// \brief Async generator that iterates on an underlying iterator in a
/// separate thread.
template <typename T>
class BackgroundIterator {
  using PromiseType = typename detail::BackgroundIteratorPromise<T>;

 public:
  explicit BackgroundIterator(Iterator<T> it, internal::Executor* executor)
      : it_(new Iterator<T>(std::move(it))),
        queue_(new detail::ReadaheadQueue(0)),
        executor_(executor),
        done_() {}

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
  bool done_;
};

/// \brief Creates an AsyncGenerator<T> by iterating over an Iterator<T> on a background
/// thread
template <typename T>
static Result<AsyncGenerator<T>> MakeBackgroundIterator(Iterator<T> iterator,
                                                        internal::ThreadPool* executor) {
  auto background_iterator =
      std::make_shared<BackgroundIterator<T>>(std::move(iterator), executor);
  return [background_iterator]() { return (*background_iterator)(); };
}

}  // namespace arrow
