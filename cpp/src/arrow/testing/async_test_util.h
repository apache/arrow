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
#include <memory>

#include "arrow/util/future.h"
#include "arrow/util/async_generator.h"

namespace arrow {
namespace util {

template <typename T>
class TrackingGenerator {
 public:
  explicit TrackingGenerator(AsyncGenerator<T> source)
      : state_(std::make_shared<State>(std::move(source))) {}

  Future<T> operator()() {
    state_->num_read++;
    return state_->source();
  }

  int num_read() { return state_->num_read.load(); }

 private:
  struct State {
    explicit State(AsyncGenerator<T> source) : source(std::move(source)), num_read(0) {}

    AsyncGenerator<T> source;
    std::atomic<int> num_read;
  };

  std::shared_ptr<State> state_;
};

}
}