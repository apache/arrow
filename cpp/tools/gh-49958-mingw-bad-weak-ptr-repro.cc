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

// Minimal standalone repro for GH-49958 (MinGW gcc 16.1)
//
//   Stress weak_ptr -> shared_ptr promotion plus shared_from_this() on an object
//   that stays alive for the whole run. Any std::bad_weak_ptr throw or impossible
//   weak_ptr expiration while keep_alive is true indicates toolchain/runtime breakage.
//
// Build (MSYS2 MINGW64):
//   g++ -std=gnu++20 -O2 -g -pthread gh-49958-mingw-bad-weak-ptr-repro.cc -o repro
//
// Run:
//   ./repro 30
//   (argument is runtime seconds; default 10)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace {

struct State : public std::enable_shared_from_this<State> {
  void Push(int value) {
    // This should always succeed while the owning shared_ptr is alive.
    auto self = shared_from_this();
    (void)self;

    std::lock_guard<std::mutex> guard(mu);
    queue.push_back(value);
  }

  bool Pop(int* out) {
    std::lock_guard<std::mutex> guard(mu);
    if (queue.empty()) {
      return false;
    }
    *out = queue.front();
    queue.pop_front();
    return true;
  }

  std::mutex mu;
  std::deque<int> queue;
};

}  // namespace

int main(int argc, char** argv) {
  int run_seconds = 10;
  if (argc > 1) {
    run_seconds = std::max(1, std::atoi(argv[1]));
  }

  constexpr int kProducerThreads = 20;
  constexpr int kLockOnlyThreads = 8;

  auto state = std::make_shared<State>();
  std::weak_ptr<State> weak = state;

  std::atomic<bool> stop{false};
  std::atomic<bool> should_be_alive{true};
  std::atomic<bool> saw_bad_weak_ptr{false};
  std::atomic<bool> saw_impossible_expired{false};

  std::atomic<uint64_t> pushes{0};
  std::atomic<uint64_t> pops{0};
  std::atomic<uint64_t> lock_ok{0};
  std::atomic<uint64_t> lock_fail{0};

  auto producer = [&] {
    int value = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      auto sp = weak.lock();
      if (!sp) {
        lock_fail.fetch_add(1, std::memory_order_relaxed);
        if (should_be_alive.load(std::memory_order_relaxed)) {
          saw_impossible_expired.store(true, std::memory_order_relaxed);
        }
        continue;
      }
      lock_ok.fetch_add(1, std::memory_order_relaxed);

      try {
        sp->Push(value++);
        pushes.fetch_add(1, std::memory_order_relaxed);
      } catch (const std::bad_weak_ptr&) {
        saw_bad_weak_ptr.store(true, std::memory_order_relaxed);
        stop.store(true, std::memory_order_relaxed);
        return;
      }

      // Extra allocator churn to make races/miscompiles show up faster.
      std::vector<std::shared_ptr<int>> churn;
      churn.reserve(32);
      for (int i = 0; i < 32; ++i) {
        churn.push_back(std::make_shared<int>(value + i));
      }
    }
  };

  auto lock_only = [&] {
    while (!stop.load(std::memory_order_relaxed)) {
      auto sp = weak.lock();
      if (sp) {
        lock_ok.fetch_add(1, std::memory_order_relaxed);
      } else {
        lock_fail.fetch_add(1, std::memory_order_relaxed);
        if (should_be_alive.load(std::memory_order_relaxed)) {
          saw_impossible_expired.store(true, std::memory_order_relaxed);
        }
      }
    }
  };

  auto consumer = [&] {
    int out = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      if (state->Pop(&out)) {
        (void)out;
        pops.fetch_add(1, std::memory_order_relaxed);
      } else {
        std::this_thread::yield();
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kProducerThreads + kLockOnlyThreads + 1);
  for (int i = 0; i < kProducerThreads; ++i) {
    threads.emplace_back(producer);
  }
  for (int i = 0; i < kLockOnlyThreads; ++i) {
    threads.emplace_back(lock_only);
  }
  threads.emplace_back(consumer);

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(run_seconds);
  while (std::chrono::steady_clock::now() < deadline &&
         !stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  stop.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  should_be_alive.store(false, std::memory_order_relaxed);
  state.reset();

  std::fprintf(stderr,
               "done: pushes=%" PRIu64 " pops=%" PRIu64 " lock_ok=%" PRIu64
               " lock_fail=%" PRIu64 " bad_weak_ptr=%d impossible_expired=%d\n",
               pushes.load(), pops.load(), lock_ok.load(), lock_fail.load(),
               saw_bad_weak_ptr.load() ? 1 : 0, saw_impossible_expired.load() ? 1 : 0);

  if (saw_bad_weak_ptr.load()) {
    std::fprintf(stderr, "REPRODUCED: unexpected std::bad_weak_ptr while object alive\n");
    return 3;
  }
  if (saw_impossible_expired.load()) {
    std::fprintf(
        stderr, "REPRODUCED: weak_ptr.lock() failed while owning shared_ptr was alive\n");
    return 2;
  }

  std::fprintf(stderr, "No failure observed in this run\n");
  return 0;
}
