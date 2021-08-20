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

  Future<> DoClose() override { return close_future_; }

  Future<> close_future_;
};

class GatingDoCloseWithChild : public AsyncCloseable {
 public:
  GatingDoCloseWithChild(AsyncCloseable* parent, Future<> close_future)
      : AsyncCloseable(parent), child_(this, std::move(close_future)) {}

  Future<> DoClose() override { return Future<>::MakeFinished(); }

  GatingDoClose child_;
};

class GatingDoCloseAsDependentTask : public AsyncCloseable {
 public:
  GatingDoCloseAsDependentTask(AsyncCloseable* parent, Future<> close_future)
      : AsyncCloseable(parent) {
    AddDependentTask(std::move(close_future));
  }

  Future<> DoClose() override { return Future<>::MakeFinished(); }
};

class EvictableChild : public OwnedAsyncCloseable {
 public:
  EvictableChild(AsyncCloseable* parent, Future<> close_future)
      : OwnedAsyncCloseable(parent), close_future_(std::move(close_future)) {}

  Future<> DoClose() override { return close_future_; }

 private:
  Future<> close_future_;
};

class EvictsChild : public AsyncCloseable {
 public:
  EvictsChild(AsyncCloseable* parent, Future<> child_future, Future<> final_future)
      : AsyncCloseable(parent), final_close_future_(std::move(final_future)) {
    owned_child_ = std::make_shared<EvictableChild>(this, std::move(child_future));
    owned_child_->Init();
    child_ = owned_child_;
  }

  void EvictChild() {
    owned_child_->Evict();
    owned_child_.reset();
  }

  bool ChildIsExpired() { return child_.expired(); }

  Future<> DoClose() override { return final_close_future_; }

 private:
  Future<> final_close_future_;
  std::shared_ptr<EvictableChild> owned_child_;
  std::weak_ptr<EvictableChild> child_;
};

template <typename T>
void AssertDoesNotCloseEarly() {
  Future<> gate = Future<>::Make();
  std::atomic<bool> finished{false};
  std::thread thread([&] {
    ASSERT_OK(Nursery::RunInNursery([&](Nursery* nursery) {
      nursery->AddRoot<T>(
          [&](AsyncCloseable* parent) { return std::make_shared<T>(parent, gate); });
    }));
    finished.store(true);
  });

  SleepABit();
  ASSERT_FALSE(finished.load());
  gate.MarkFinished();
  BusyWait(10, [&] { return finished.load(); });
  thread.join();
};

TEST(AsyncNursery, DoClose) { AssertDoesNotCloseEarly<GatingDoClose>(); }

TEST(AsyncNursery, ChildDoClose) { AssertDoesNotCloseEarly<GatingDoCloseWithChild>(); }

TEST(AsyncNursery, DependentTask) {
  AssertDoesNotCloseEarly<GatingDoCloseAsDependentTask>();
}

TEST(AsyncNursery, EvictedChild) {
  Future<> child_future = Future<>::Make();
  Future<> final_future = Future<>::Make();
  std::atomic<bool> finished{false};
  std::thread thread([&] {
    ASSERT_OK(Nursery::RunInNursery([&](Nursery* nursery) {
      std::shared_ptr<EvictsChild> evicts_child =
          nursery->AddRoot<EvictsChild>([&](AsyncCloseable* parent) {
            return std::make_shared<EvictsChild>(parent, child_future, final_future);
          });
      evicts_child->EvictChild();
      // Owner no longer has reference to child here but it's kept alive by nursery
      // because it isn't done
      ASSERT_FALSE(evicts_child->ChildIsExpired());
      child_future.MarkFinished();
      ASSERT_TRUE(evicts_child->ChildIsExpired());
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