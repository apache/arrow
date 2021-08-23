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

#include "arrow/util/async_nursery.h"

#include <gtest/gtest.h>

#include <thread>

#include "arrow/result.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace util {

class GatingDoClose : public AsyncCloseable {
 public:
  GatingDoClose(AsyncCloseable* parent, Future<> close_future)
      : AsyncCloseable(parent), close_future_(std::move(close_future)) {}
  GatingDoClose(Future<> close_future) : close_future_(std::move(close_future)) {}

  Future<> DoClose() override { return close_future_; }

  Future<> close_future_;
};

class GatingDoCloseWithUniqueChild : public AsyncCloseable {
 public:
  GatingDoCloseWithUniqueChild(Nursery* nursery, Future<> close_future)
      : child_(
            nursery->MakeUniqueCloseable<GatingDoClose>(this, std::move(close_future))) {}

  Future<> DoClose() override {
    child_.reset();
    return Future<>::MakeFinished();
  }

  std::unique_ptr<GatingDoClose, DestroyingDeleter<GatingDoClose>> child_;
};

class GatingDoCloseWithSharedChild : public AsyncCloseable {
 public:
  GatingDoCloseWithSharedChild(Nursery* nursery, Future<> close_future)
      : child_(
            nursery->MakeSharedCloseable<GatingDoClose>(this, std::move(close_future))) {}

  Future<> DoClose() override {
    child_.reset();
    return Future<>::MakeFinished();
  }

  std::shared_ptr<GatingDoClose> child_;
};

class GatingDoCloseAsDependentTask : public AsyncCloseable {
 public:
  GatingDoCloseAsDependentTask(Future<> close_future) {
    AddDependentTask(std::move(close_future));
  }

  Future<> DoClose() override { return Future<>::MakeFinished(); }
};

class MarkWhenDestroyed : public GatingDoClose {
 public:
  MarkWhenDestroyed(Future<> close_future, bool* destroyed)
      : GatingDoClose(std::move(close_future)), destroyed_(destroyed) {}
  ~MarkWhenDestroyed() { *destroyed_ = true; }

 private:
  bool* destroyed_;
};

class EvictsChild : public AsyncCloseable {
 public:
  EvictsChild(Nursery* nursery, bool* child_destroyed, Future<> child_future,
              Future<> final_future)
      : final_close_future_(std::move(final_future)) {
    owned_child_ = nursery->MakeSharedCloseable<MarkWhenDestroyed>(
        std::move(child_future), child_destroyed);
  }

  void EvictChild() { owned_child_.reset(); }

  Future<> DoClose() override { return final_close_future_; }

 private:
  Future<> final_close_future_;
  std::shared_ptr<GatingDoClose> owned_child_;
};

template <typename T>
void AssertDoesNotCloseEarly() {
  Future<> gate = Future<>::Make();
  std::atomic<bool> finished{false};
  std::thread thread([&] {
    ASSERT_OK(Nursery::RunInNursery(
        [&](Nursery* nursery) { nursery->MakeSharedCloseable<T>(gate); }));
    finished.store(true);
  });

  SleepABit();
  ASSERT_FALSE(finished.load());
  gate.MarkFinished();
  BusyWait(10, [&] { return finished.load(); });
  thread.join();
};

template <typename T>
void AssertDoesNotCloseEarlyWithChild() {
  Future<> gate = Future<>::Make();
  std::atomic<bool> finished{false};
  std::thread thread([&] {
    ASSERT_OK(Nursery::RunInNursery(
        [&](Nursery* nursery) { nursery->MakeSharedCloseable<T>(nursery, gate); }));
    finished.store(true);
  });

  SleepABit();
  ASSERT_FALSE(finished.load());
  gate.MarkFinished();
  BusyWait(10, [&] { return finished.load(); });
  thread.join();
};

TEST(AsyncNursery, DoClose) { AssertDoesNotCloseEarly<GatingDoClose>(); }

TEST(AsyncNursery, SharedChildDoClose) {
  AssertDoesNotCloseEarlyWithChild<GatingDoCloseWithSharedChild>();
}

TEST(AsyncNursery, UniqueChildDoClose) {
  AssertDoesNotCloseEarlyWithChild<GatingDoCloseWithUniqueChild>();
}

TEST(AsyncNursery, DependentTask) {
  AssertDoesNotCloseEarly<GatingDoCloseAsDependentTask>();
}

TEST(AsyncNursery, EvictedChild) {
  Future<> child_future = Future<>::Make();
  Future<> final_future = Future<>::Make();
  std::atomic<bool> finished{false};
  std::thread thread([&] {
    ASSERT_OK(Nursery::RunInNursery([&](Nursery* nursery) {
      bool child_destroyed = false;
      std::shared_ptr<EvictsChild> evicts_child =
          nursery->MakeSharedCloseable<EvictsChild>(nursery, &child_destroyed,
                                                    child_future, final_future);
      evicts_child->EvictChild();
      // Owner no longer has reference to child here but it's kept alive by nursery
      // because it isn't done
      ASSERT_FALSE(child_destroyed);
      child_future.MarkFinished();
      ASSERT_TRUE(child_destroyed);
    }));
    finished.store(true);
  });

  SleepABit();
  ASSERT_FALSE(finished.load());
  final_future.MarkFinished();
  BusyWait(10, [&] { return finished.load(); });
  thread.join();
}

}  // namespace util
}  // namespace arrow