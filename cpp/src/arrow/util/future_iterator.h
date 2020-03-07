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
#include <memory>
#include <utility>
#include <vector>

#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// An iterator that takes a set of futures, and yields their results as
/// they are completed, in any order.
template <typename T>
class AsCompletedIterator {
 public:
  // Public default constructor creates an empty iterator
  AsCompletedIterator();

  explicit AsCompletedIterator(std::vector<Future<T>> futures)
      : futures_(std::move(futures)),
        waiter_(FutureWaiter::Make(FutureWaiter::ITERATE, futures_)) {}

  ARROW_DEFAULT_MOVE_AND_ASSIGN(AsCompletedIterator);
  ARROW_DISALLOW_COPY_AND_ASSIGN(AsCompletedIterator);

  /// Return the results of the first completed, not-yet-returned Future.
  ///
  /// The result can be successful or not, depending on the Future's underlying
  /// task's result.  Even if a Future returns a failed Result, you can still
  /// call Next() to get further results.
  Result<T> Next() {
    if (n_fetched_ == futures_.size()) {
      return IterationTraits<T>::End();
    }
    auto index = waiter_->WaitAndFetchOne();
    ++n_fetched_;
    assert(index >= 0 && static_cast<size_t>(index) < futures_.size());
    auto& fut = futures_[index];
    assert(IsFutureFinished(fut.state()));
    return std::move(fut).result();
  }

 private:
  size_t n_fetched_ = 0;
  std::vector<Future<T>> futures_;
  std::unique_ptr<FutureWaiter> waiter_;
};

template <typename T>
Iterator<T> MakeAsCompletedIterator(std::vector<Future<T>> futures) {
  return Iterator<T>(AsCompletedIterator<T>(std::move(futures)));
}

}  // namespace arrow
