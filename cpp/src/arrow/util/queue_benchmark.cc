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

#include <algorithm>
#include <iterator>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/queue.h"

namespace arrow {

namespace util {

static constexpr int64_t kSize = 100000;

void SpscQueueThroughput(benchmark::State& state) {
  SpscQueue<std::shared_ptr<Buffer>> queue(16);

  std::vector<std::shared_ptr<Buffer>> source;
  std::vector<std::shared_ptr<Buffer>> sink;
  source.reserve(kSize);
  sink.resize(kSize);
  const uint8_t data[1] = {0};
  for (int64_t i = 0; i < kSize; i++) {
    source.push_back(std::make_shared<Buffer>(data, 1));
  }

  for (auto _ : state) {
    std::thread producer([&] {
      auto itr = std::make_move_iterator(source.begin());
      auto end = std::make_move_iterator(source.end());
      while (itr != end) {
        while (!queue.Write(*itr)) {
        }
        itr++;
      }
    });

    std::thread consumer([&] {
      auto itr = sink.begin();
      auto end = sink.end();
      while (itr != end) {
        auto next = queue.FrontPtr();
        if (next != nullptr) {
          (*itr).swap(*next);
          queue.PopFront();
          itr++;
        }
      }
    });

    producer.join();
    consumer.join();
    std::swap(source, sink);
  }

  for (const auto& buf : source) {
    ARROW_CHECK(buf && buf->size() == 1);
  }
  state.SetItemsProcessed(state.iterations() * kSize);
}

BENCHMARK(SpscQueueThroughput)->UseRealTime();

}  // namespace util
}  // namespace arrow
