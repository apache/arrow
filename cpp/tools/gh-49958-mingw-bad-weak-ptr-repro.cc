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

// Minimal standalone repro for MinGW gcc 16.1 shared_ptr/weak_ptr regression.
// Build (MSYS2 MINGW64):
//   g++ -std=gnu++20 -O2 -g -pthread gh-49958-mingw-bad-weak-ptr-repro.cc -o repro
// Run (argument in seconds):
//   ./repro 30

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

namespace {

struct State : public std::enable_shared_from_this<State> {
  void Check() {
    check_enter.fetch_add(1, std::memory_order_relaxed);
    stage.store(5, std::memory_order_relaxed);  // in shared_from_this
    auto self = shared_from_this();
    (void)self;
    check_ok.fetch_add(1, std::memory_order_relaxed);
    stage.store(6, std::memory_order_relaxed);  // shared_from_this returned
  }

  static std::atomic<uint64_t> check_enter;
  static std::atomic<uint64_t> check_ok;
  static std::atomic<int> stage;
};

std::atomic<uint64_t> State::check_enter{0};
std::atomic<uint64_t> State::check_ok{0};
std::atomic<int> State::stage{0};

std::atomic<uint64_t> lock_ok{0};
std::atomic<uint64_t> lock_fail{0};
std::atomic<uint64_t> checks{0};
std::atomic<bool> saw_bad_weak_ptr{false};
std::atomic<bool> saw_impossible_expired{false};

void Dump(const char* tag) {
  std::fprintf(stderr,
               "%s: stage=%d checks=%" PRIu64 " lock_ok=%" PRIu64 " lock_fail=%" PRIu64
               " check_enter=%" PRIu64 " check_ok=%" PRIu64
               " bad_weak_ptr=%d impossible_expired=%d\n",
               tag, State::stage.load(std::memory_order_relaxed),
               checks.load(std::memory_order_relaxed),
               lock_ok.load(std::memory_order_relaxed),
               lock_fail.load(std::memory_order_relaxed),
               State::check_enter.load(std::memory_order_relaxed),
               State::check_ok.load(std::memory_order_relaxed),
               saw_bad_weak_ptr.load(std::memory_order_relaxed) ? 1 : 0,
               saw_impossible_expired.load(std::memory_order_relaxed) ? 1 : 0);
}

void OnSignal(int sig) {
  std::fprintf(stderr, "caught signal %d\n", sig);
  Dump("crash_state");
  std::_Exit(128 + sig);
}

}  // namespace

int main(int argc, char** argv) {
  std::setvbuf(stderr, nullptr, _IONBF, 0);
  std::signal(SIGSEGV, OnSignal);
  std::signal(SIGABRT, OnSignal);

  int run_seconds = 10;
  if (argc > 1) {
    run_seconds = std::max(1, std::atoi(argv[1]));
  }

  constexpr int kThreads = 32;

  auto owner = std::make_shared<State>();
  std::weak_ptr<State> weak = owner;

  std::atomic<bool> stop{false};
  std::atomic<bool> owner_is_alive{true};

  auto worker = [&] {
    auto weak_local = weak;
    while (!stop.load(std::memory_order_relaxed)) {
      State::stage.store(1, std::memory_order_relaxed);  // loop
      auto sp = weak_local.lock();
      if (!sp) {
        State::stage.store(3, std::memory_order_relaxed);  // lock failed
        lock_fail.fetch_add(1, std::memory_order_relaxed);
        if (owner_is_alive.load(std::memory_order_relaxed)) {
          saw_impossible_expired.store(true, std::memory_order_relaxed);
          stop.store(true, std::memory_order_relaxed);
        }
        continue;
      }
      lock_ok.fetch_add(1, std::memory_order_relaxed);
      State::stage.store(2, std::memory_order_relaxed);  // sp obtained

      try {
        State::stage.store(4, std::memory_order_relaxed);  // before Check
        sp->Check();
        checks.fetch_add(1, std::memory_order_relaxed);
      } catch (const std::bad_weak_ptr&) {
        saw_bad_weak_ptr.store(true, std::memory_order_relaxed);
        stop.store(true, std::memory_order_relaxed);
        return;
      }

      std::shared_ptr<State> copies[8] = {sp, sp, sp, sp, sp, sp, sp, sp};
      (void)copies;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back(worker);
  }

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(run_seconds);
  while (std::chrono::steady_clock::now() < deadline &&
         !stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop.store(true, std::memory_order_relaxed);
  for (auto& t : threads) {
    t.join();
  }

  owner_is_alive.store(false, std::memory_order_relaxed);
  owner.reset();

  Dump("done");

  if (saw_bad_weak_ptr.load(std::memory_order_relaxed)) {
    std::fprintf(stderr, "REPRODUCED: unexpected std::bad_weak_ptr while object alive\n");
    return 3;
  }
  if (saw_impossible_expired.load(std::memory_order_relaxed)) {
    std::fprintf(
        stderr, "REPRODUCED: weak_ptr.lock() failed while owning shared_ptr was alive\n");
    return 2;
  }

  std::fprintf(stderr, "No failure observed in this run\n");
  return 0;
}
