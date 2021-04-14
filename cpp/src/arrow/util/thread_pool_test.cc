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

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

template <typename T>
static void task_add(T x, T y, T* out) {
  *out = x + y;
}

template <typename T>
struct task_slow_add {
  void operator()(T x, T y, T* out) {
    SleepFor(seconds_);
    *out = x + y;
  }

  const double seconds_;
};

typedef std::function<void(int, int, int*)> AddTaskFunc;

template <typename T>
static T add(T x, T y) {
  return x + y;
}

template <typename T>
static T slow_add(double seconds, T x, T y) {
  SleepFor(seconds);
  return x + y;
}

template <typename T>
static T inplace_add(T& x, T y) {
  return x += y;
}

// A class to spawn "add" tasks to a pool and check the results when done

class AddTester {
 public:
  explicit AddTester(int nadds, StopToken stop_token = StopToken::Unstoppable())
      : nadds_(nadds), stop_token_(stop_token), xs_(nadds), ys_(nadds), outs_(nadds, -1) {
    int x = 0, y = 0;
    std::generate(xs_.begin(), xs_.end(), [&] {
      ++x;
      return x;
    });
    std::generate(ys_.begin(), ys_.end(), [&] {
      y += 10;
      return y;
    });
  }

  AddTester(AddTester&&) = default;

  void SpawnTasks(ThreadPool* pool, AddTaskFunc add_func) {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_OK(pool->Spawn([=] { add_func(xs_[i], ys_[i], &outs_[i]); }, stop_token_));
    }
  }

  void CheckResults() {
    for (int i = 0; i < nadds_; ++i) {
      ASSERT_EQ(outs_[i], (i + 1) * 11);
    }
  }

  void CheckNotAllComputed() {
    for (int i = 0; i < nadds_; ++i) {
      if (outs_[i] == -1) {
        return;
      }
    }
    ASSERT_TRUE(0) << "all values were computed";
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(AddTester);

  int nadds_;
  StopToken stop_token_;
  std::vector<int> xs_;
  std::vector<int> ys_;
  std::vector<int> outs_;
};

class TestRunSynchronously : public testing::TestWithParam<bool> {
 public:
  bool UseThreads() { return GetParam(); }

  template <typename T>
  Result<T> Run(FnOnce<Future<T>(Executor*)> top_level_task) {
    return RunSynchronously(std::move(top_level_task), UseThreads());
  }

  Status RunVoid(FnOnce<Future<>(Executor*)> top_level_task) {
    return RunSynchronouslyVoid(std::move(top_level_task), UseThreads());
  }

  void TestContinueAfterExternal(bool transfer_to_main_thread) {
    bool continuation_ran = false;
    EXPECT_OK_AND_ASSIGN(auto external_pool, ThreadPool::Make(1));
    auto top_level_task = [&](Executor* executor) {
      struct Callback {
        Status operator()(...) {
          *continuation_ran = true;
          return Status::OK();
        }
        bool* continuation_ran;
      };
      auto fut = DeferNotOk(external_pool->Submit([&] {
        SleepABit();
        return Status::OK();
      }));
      if (transfer_to_main_thread) {
        fut = executor->Transfer(fut);
      }
      return fut.Then(Callback{&continuation_ran});
    };
    ASSERT_OK(RunVoid(std::move(top_level_task)));
    EXPECT_TRUE(continuation_ran);
  }
};

TEST_P(TestRunSynchronously, SimpleRun) {
  bool task_ran = false;
  auto task = [&](Executor* executor) {
    EXPECT_NE(executor, nullptr);
    task_ran = true;
    return Future<>::MakeFinished(Status::OK());
  };
  ASSERT_OK(RunVoid(std::move(task)));
  EXPECT_TRUE(task_ran);
}

TEST_P(TestRunSynchronously, SpawnNested) {
  bool nested_ran = false;
  auto top_level_task = [&](Executor* executor) {
    return DeferNotOk(executor->Submit([&] {
      nested_ran = true;
      return Status::OK();
    }));
  };
  ASSERT_OK(RunVoid(std::move(top_level_task)));
  EXPECT_TRUE(nested_ran);
}

TEST_P(TestRunSynchronously, SpawnMoreNested) {
  std::atomic<int> nested_ran{0};
  auto top_level_task = [&](Executor* executor) -> Future<> {
    auto fut_a = DeferNotOk(executor->Submit([&] { nested_ran++; }));
    auto fut_b = DeferNotOk(executor->Submit([&] { nested_ran++; }));
    return AllComplete({fut_a, fut_b})
        .Then([&](const Result<arrow::detail::Empty>& result) {
          nested_ran++;
          return result;
        });
  };
  ASSERT_OK(RunVoid(std::move(top_level_task)));
  EXPECT_EQ(nested_ran, 3);
}

TEST_P(TestRunSynchronously, WithResult) {
  auto top_level_task = [&](Executor* executor) {
    return DeferNotOk(executor->Submit([] { return 42; }));
  };
  ASSERT_OK_AND_EQ(42, Run<int>(std::move(top_level_task)));
}

TEST_P(TestRunSynchronously, StopTokenSpawn) {
  bool nested_ran = false;
  StopSource stop_source;
  auto top_level_task = [&](Executor* executor) -> Future<> {
    stop_source.RequestStop(Status::Invalid("XYZ"));
    RETURN_NOT_OK(executor->Spawn([&] { nested_ran = true; }, stop_source.token()));
    return Future<>::MakeFinished();
  };
  ASSERT_OK(RunVoid(std::move(top_level_task)));
  EXPECT_FALSE(nested_ran);
}

TEST_P(TestRunSynchronously, StopTokenSubmit) {
  bool nested_ran = false;
  StopSource stop_source;
  auto top_level_task = [&](Executor* executor) -> Future<> {
    stop_source.RequestStop();
    return DeferNotOk(executor->Submit(stop_source.token(), [&] {
      nested_ran = true;
      return Status::OK();
    }));
  };
  ASSERT_RAISES(Cancelled, RunVoid(std::move(top_level_task)));
  EXPECT_FALSE(nested_ran);
}

TEST_P(TestRunSynchronously, ContinueAfterExternal) {
  // The future returned by the top-level task completes on another thread.
  // This can trigger delicate race conditions in the SerialExecutor code,
  // especially destruction.
  this->TestContinueAfterExternal(/*transfer_to_main_thread=*/false);
}

TEST_P(TestRunSynchronously, ContinueAfterExternalTransferred) {
  // Like above, but the future is transferred back to the serial executor
  // after completion on an external thread.
  this->TestContinueAfterExternal(/*transfer_to_main_thread=*/true);
}

TEST_P(TestRunSynchronously, SchedulerAbort) {
  auto top_level_task = [&](Executor* executor) { return Status::Invalid("XYZ"); };
  ASSERT_RAISES(Invalid, RunVoid(std::move(top_level_task)));
}

TEST_P(TestRunSynchronously, PropagatedError) {
  auto top_level_task = [&](Executor* executor) {
    return DeferNotOk(executor->Submit([] { return Status::Invalid("XYZ"); }));
  };
  ASSERT_RAISES(Invalid, RunVoid(std::move(top_level_task)));
}

INSTANTIATE_TEST_SUITE_P(TestRunSynchronously, TestRunSynchronously,
                         ::testing::Values(false, true));

class TestThreadPool : public ::testing::Test {
 public:
  void TearDown() override {
    fflush(stdout);
    fflush(stderr);
  }

  std::shared_ptr<ThreadPool> MakeThreadPool() { return MakeThreadPool(4); }

  std::shared_ptr<ThreadPool> MakeThreadPool(int threads) {
    return *ThreadPool::Make(threads);
  }

  void DoSpawnAdds(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                   StopToken stop_token = StopToken::Unstoppable(),
                   StopSource* stop_source = nullptr) {
    AddTester add_tester(nadds, stop_token);
    add_tester.SpawnTasks(pool, add_func);
    if (stop_source) {
      stop_source->RequestStop();
    }
    ASSERT_OK(pool->Shutdown());
    if (stop_source) {
      add_tester.CheckNotAllComputed();
    } else {
      add_tester.CheckResults();
    }
  }

  void SpawnAdds(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                 StopToken stop_token = StopToken::Unstoppable()) {
    DoSpawnAdds(pool, nadds, std::move(add_func), std::move(stop_token));
  }

  void SpawnAddsAndCancel(ThreadPool* pool, int nadds, AddTaskFunc add_func,
                          StopSource* stop_source) {
    DoSpawnAdds(pool, nadds, std::move(add_func), stop_source->token(), stop_source);
  }

  void DoSpawnAddsThreaded(ThreadPool* pool, int nthreads, int nadds,
                           AddTaskFunc add_func,
                           StopToken stop_token = StopToken::Unstoppable(),
                           StopSource* stop_source = nullptr) {
    // Same as SpawnAdds, but do the task spawning from multiple threads
    std::vector<AddTester> add_testers;
    std::vector<std::thread> threads;
    for (int i = 0; i < nthreads; ++i) {
      add_testers.emplace_back(nadds, stop_token);
    }
    for (auto& add_tester : add_testers) {
      threads.emplace_back([&] { add_tester.SpawnTasks(pool, add_func); });
    }
    if (stop_source) {
      stop_source->RequestStop();
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_OK(pool->Shutdown());
    for (auto& add_tester : add_testers) {
      if (stop_source) {
        add_tester.CheckNotAllComputed();
      } else {
        add_tester.CheckResults();
      }
    }
  }

  void SpawnAddsThreaded(ThreadPool* pool, int nthreads, int nadds, AddTaskFunc add_func,
                         StopToken stop_token = StopToken::Unstoppable()) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func),
                        std::move(stop_token));
  }

  void SpawnAddsThreadedAndCancel(ThreadPool* pool, int nthreads, int nadds,
                                  AddTaskFunc add_func, StopSource* stop_source) {
    DoSpawnAddsThreaded(pool, nthreads, nadds, std::move(add_func), stop_source->token(),
                        stop_source);
  }
};

TEST_F(TestThreadPool, ConstructDestruct) {
  // Stress shutdown-at-destruction logic
  for (int threads : {1, 2, 3, 8, 32, 70}) {
    auto pool = this->MakeThreadPool(threads);
  }
}

// Correctness and stress tests using Spawn() and Shutdown()

TEST_F(TestThreadPool, Spawn) {
  auto pool = this->MakeThreadPool(3);
  SpawnAdds(pool.get(), 7, task_add<int>);
}

TEST_F(TestThreadPool, StressSpawn) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, task_add<int>);
}

TEST_F(TestThreadPool, StressSpawnThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_add<int>);
}

TEST_F(TestThreadPool, SpawnSlow) {
  // This checks that Shutdown() waits for all tasks to finish
  auto pool = this->MakeThreadPool(2);
  SpawnAdds(pool.get(), 7, task_slow_add<int>{/*seconds=*/0.02});
}

TEST_F(TestThreadPool, StressSpawnSlow) {
  auto pool = this->MakeThreadPool(30);
  SpawnAdds(pool.get(), 1000, task_slow_add<int>{/*seconds=*/0.002});
}

TEST_F(TestThreadPool, StressSpawnSlowThreaded) {
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_slow_add<int>{/*seconds=*/0.002});
}

TEST_F(TestThreadPool, SpawnWithStopToken) {
  StopSource stop_source;
  auto pool = this->MakeThreadPool(3);
  SpawnAdds(pool.get(), 7, task_add<int>, stop_source.token());
}

TEST_F(TestThreadPool, StressSpawnThreadedWithStopToken) {
  StopSource stop_source;
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreaded(pool.get(), 20, 100, task_add<int>, stop_source.token());
}

TEST_F(TestThreadPool, SpawnWithStopTokenCancelled) {
  StopSource stop_source;
  auto pool = this->MakeThreadPool(3);
  SpawnAddsAndCancel(pool.get(), 100, task_slow_add<int>{/*seconds=*/0.02}, &stop_source);
}

TEST_F(TestThreadPool, StressSpawnThreadedWithStopTokenCancelled) {
  StopSource stop_source;
  auto pool = this->MakeThreadPool(30);
  SpawnAddsThreadedAndCancel(pool.get(), 20, 100, task_slow_add<int>{/*seconds=*/0.02},
                             &stop_source);
}

TEST_F(TestThreadPool, QuickShutdown) {
  AddTester add_tester(100);
  {
    auto pool = this->MakeThreadPool(3);
    add_tester.SpawnTasks(pool.get(), task_slow_add<int>{/*seconds=*/0.02});
    ASSERT_OK(pool->Shutdown(false /* wait */));
    add_tester.CheckNotAllComputed();
  }
  add_tester.CheckNotAllComputed();
}

TEST_F(TestThreadPool, SetCapacity) {
  auto pool = this->MakeThreadPool(5);

  // Thread spawning is on-demand
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 0);

  ASSERT_OK(pool->SetCapacity(3));
  ASSERT_EQ(pool->GetCapacity(), 3);
  ASSERT_EQ(pool->GetActualCapacity(), 0);

  auto gating_task = GatingTask::Make();

  ASSERT_OK(pool->Spawn(gating_task->Task()));
  ASSERT_OK(gating_task->WaitForRunning(1));
  ASSERT_EQ(pool->GetActualCapacity(), 1);
  ASSERT_OK(gating_task->Unlock());

  gating_task = GatingTask::Make();
  // Spawn more tasks than the pool capacity
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(pool->Spawn(gating_task->Task()));
  }
  ASSERT_OK(gating_task->WaitForRunning(3));
  SleepFor(0.001);  // Sleep a bit just to make sure it isn't making any threads
  ASSERT_EQ(pool->GetActualCapacity(), 3);  // maxxed out

  // The tasks have not finished yet, increasing the desired capacity
  // should spawn threads immediately.
  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  // Thread reaping is eager (but asynchronous)
  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);

  // Wait for workers to wake up and secede
  ASSERT_OK(gating_task->Unlock());
  BusyWait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  // Downsize while tasks are pending
  ASSERT_OK(pool->SetCapacity(5));
  ASSERT_EQ(pool->GetCapacity(), 5);
  gating_task = GatingTask::Make();
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(pool->Spawn(gating_task->Task()));
  }
  ASSERT_OK(gating_task->WaitForRunning(5));
  ASSERT_EQ(pool->GetActualCapacity(), 5);

  ASSERT_OK(pool->SetCapacity(2));
  ASSERT_EQ(pool->GetCapacity(), 2);
  ASSERT_OK(gating_task->Unlock());
  BusyWait(0.5, [&] { return pool->GetActualCapacity() == 2; });
  ASSERT_EQ(pool->GetActualCapacity(), 2);

  // Ensure nothing got stuck
  ASSERT_OK(pool->Shutdown());
}

// Test Submit() functionality

TEST_F(TestThreadPool, Submit) {
  auto pool = this->MakeThreadPool(3);
  {
    ASSERT_OK_AND_ASSIGN(Future<int> fut, pool->Submit(add<int>, 4, 5));
    Result<int> res = fut.result();
    ASSERT_OK_AND_EQ(9, res);
  }
  {
    ASSERT_OK_AND_ASSIGN(Future<std::string> fut,
                         pool->Submit(add<std::string>, "foo", "bar"));
    ASSERT_OK_AND_EQ("foobar", fut.result());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit(slow_add<int>, /*seconds=*/0.01, 4, 5));
    ASSERT_OK_AND_EQ(9, fut.result());
  }
  {
    // Reference passing
    std::string s = "foo";
    ASSERT_OK_AND_ASSIGN(auto fut,
                         pool->Submit(inplace_add<std::string>, std::ref(s), "bar"));
    ASSERT_OK_AND_EQ("foobar", fut.result());
    ASSERT_EQ(s, "foobar");
  }
  {
    // `void` return type
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit(SleepFor, 0.001));
    ASSERT_OK(fut.status());
  }
}

TEST_F(TestThreadPool, SubmitWithStopToken) {
  auto pool = this->MakeThreadPool(3);
  {
    StopSource stop_source;
    ASSERT_OK_AND_ASSIGN(Future<int> fut,
                         pool->Submit(stop_source.token(), add<int>, 4, 5));
    Result<int> res = fut.result();
    ASSERT_OK_AND_EQ(9, res);
  }
}

TEST_F(TestThreadPool, SubmitWithStopTokenCancelled) {
  auto pool = this->MakeThreadPool(3);
  {
    const int n_futures = 100;
    StopSource stop_source;
    StopToken stop_token = stop_source.token();
    std::vector<Future<int>> futures;
    for (int i = 0; i < n_futures; ++i) {
      ASSERT_OK_AND_ASSIGN(
          auto fut, pool->Submit(stop_token, slow_add<int>, 0.01 /*seconds*/, i, 1));
      futures.push_back(std::move(fut));
    }
    SleepFor(0.05);  // Let some work finish
    stop_source.RequestStop();
    int n_success = 0;
    int n_cancelled = 0;
    for (int i = 0; i < n_futures; ++i) {
      Result<int> res = futures[i].result();
      if (res.ok()) {
        ASSERT_EQ(i + 1, *res);
        ++n_success;
      } else {
        ASSERT_RAISES(Cancelled, res);
        ++n_cancelled;
      }
    }
    ASSERT_GT(n_success, 0);
    ASSERT_GT(n_cancelled, 0);
  }
}

// Test fork safety on Unix

#if !(defined(_WIN32) || defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || \
      defined(THREAD_SANITIZER))
TEST_F(TestThreadPool, ForkSafety) {
  pid_t child_pid;
  int child_status;

  {
    // Fork after task submission
    auto pool = this->MakeThreadPool(3);
    ASSERT_OK_AND_ASSIGN(auto fut, pool->Submit(add<int>, 4, 5));
    ASSERT_OK_AND_EQ(9, fut.result());

    child_pid = fork();
    if (child_pid == 0) {
      // Child: thread pool should be usable
      ASSERT_OK_AND_ASSIGN(fut, pool->Submit(add<int>, 3, 4));
      if (*fut.result() != 7) {
        std::exit(1);
      }
      // Shutting down shouldn't hang or fail
      Status st = pool->Shutdown();
      std::exit(st.ok() ? 0 : 2);
    } else {
      // Parent
      ASSERT_GT(child_pid, 0);
      ASSERT_GT(waitpid(child_pid, &child_status, 0), 0);
      ASSERT_TRUE(WIFEXITED(child_status));
      ASSERT_EQ(WEXITSTATUS(child_status), 0);
      ASSERT_OK(pool->Shutdown());
    }
  }
  {
    // Fork after shutdown
    auto pool = this->MakeThreadPool(3);
    ASSERT_OK(pool->Shutdown());

    child_pid = fork();
    if (child_pid == 0) {
      // Child
      // Spawning a task should return with error (pool was shutdown)
      Status st = pool->Spawn([] {});
      if (!st.IsInvalid()) {
        std::exit(1);
      }
      // Trigger destructor
      pool.reset();
      std::exit(0);
    } else {
      // Parent
      ASSERT_GT(child_pid, 0);
      ASSERT_GT(waitpid(child_pid, &child_status, 0), 0);
      ASSERT_TRUE(WIFEXITED(child_status));
      ASSERT_EQ(WEXITSTATUS(child_status), 0);
    }
  }
}
#endif

TEST(TestGlobalThreadPool, Capacity) {
  // Sanity check
  auto pool = GetCpuThreadPool();
  int capacity = pool->GetCapacity();
  ASSERT_GT(capacity, 0);
  ASSERT_EQ(GetCpuThreadPoolCapacity(), capacity);

  // This value depends on whether any tasks were launched previously
  ASSERT_GE(pool->GetActualCapacity(), 0);
  ASSERT_LE(pool->GetActualCapacity(), capacity);

  // Exercise default capacity heuristic
  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));
  ASSERT_OK(DelEnvVar("OMP_THREAD_LIMIT"));
  int hw_capacity = std::thread::hardware_concurrency();
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 13);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "7,5,13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 7);
  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));

  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "1"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 1);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "999"));
  if (hw_capacity <= 999) {
    ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  }
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "6,5,13"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 6);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "2"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), 2);

  // Invalid env values
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "0"));
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "0"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "zzz"));
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "x"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);
  ASSERT_OK(SetEnvVar("OMP_THREAD_LIMIT", "-1"));
  ASSERT_OK(SetEnvVar("OMP_NUM_THREADS", "99999999999999999999999999"));
  ASSERT_EQ(ThreadPool::DefaultCapacity(), hw_capacity);

  ASSERT_OK(DelEnvVar("OMP_NUM_THREADS"));
  ASSERT_OK(DelEnvVar("OMP_THREAD_LIMIT"));
}

}  // namespace internal
}  // namespace arrow
