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

#ifndef _WIN32
#include <unistd.h>
#endif

#include <cstdint>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#if defined(_MSC_VER)
// Disable harmless warning for decorated name length limit
#pragma warning(disable : 4503)
#endif

namespace arrow {

/// \brief Get the capacity of the global thread pool
///
/// Return the number of worker threads in the thread pool to which
/// Arrow dispatches various CPU-bound tasks.  This is an ideal number,
/// not necessarily the exact number of threads at a given point in time.
///
/// You can change this number using SetCpuThreadPoolCapacity().
ARROW_EXPORT int GetCpuThreadPoolCapacity();

/// \brief Set the capacity of the global thread pool
///
/// Set the number of worker threads int the thread pool to which
/// Arrow dispatches various CPU-bound tasks.
///
/// The current number is returned by GetCpuThreadPoolCapacity().
ARROW_EXPORT Status SetCpuThreadPoolCapacity(int threads);

namespace internal {

namespace detail {

// Make sure that both functions returning T and Result<T> can be called
// through Executor::Submit(), and that a Future<T> is returned for both.
template <typename T>
struct ExecutorResultTraits {
  using ValueType = T;
};

template <typename T>
struct ExecutorResultTraits<Result<T>> {
  using ValueType = T;
};

}  // namespace detail

// Hints about a task that may be used by an Executor.
// They are ignored by the provided ThreadPool implementation.
struct TaskHints {
  // The lower, the more urgent
  int32_t priority = 0;
  // The IO transfer size in bytes
  int64_t io_size = -1;
  // The approximate CPU cost in number of instructions
  int64_t cpu_cost = -1;
  // An application-specific ID
  int64_t external_id = -1;
};

class ARROW_EXPORT Executor {
 public:
  virtual ~Executor();

  // Spawn a fire-and-forget task.
  template <typename Function>
  Status Spawn(Function&& func) {
    return SpawnReal(TaskHints{}, std::forward<Function>(func));
  }

  template <typename Function>
  Status Spawn(TaskHints hints, Function&& func) {
    return SpawnReal(std::move(hints), std::forward<Function>(func));
  }

  // Submit a callable and arguments for execution.  Return a future that
  // will return the callable's result value once.
  // The callable's arguments are copied before execution.
  template <
      typename Function, typename... Args,
      typename FunctionRetType = typename std::result_of<Function && (Args && ...)>::type,
      typename RT = typename detail::ExecutorResultTraits<FunctionRetType>,
      typename ValueType = typename RT::ValueType>
  Result<Future<ValueType>> Submit(Function&& func, Args&&... args) {
    return Submit(TaskHints{}, std::forward<Function>(func), std::forward<Args>(args)...);
  }

  template <
      typename Function, typename... Args,
      typename FunctionRetType = typename std::result_of<Function && (Args && ...)>::type,
      typename RT = typename detail::ExecutorResultTraits<FunctionRetType>,
      typename ValueType = typename RT::ValueType>
  Result<Future<ValueType>> Submit(TaskHints hints, Function&& func, Args&&... args) {
    auto bound_func =
        std::bind(std::forward<Function>(func), std::forward<Args>(args)...);
    using BoundFuncType = decltype(bound_func);

    struct Task {
      BoundFuncType bound_func;
      Future<ValueType> future;

      void operator()() { future.ExecuteAndMarkFinished(std::move(bound_func)); }
    };
    auto future = Future<ValueType>::Make();
    ARROW_RETURN_NOT_OK(SpawnReal(std::move(hints), Task{std::move(bound_func), future}));
    return future;
  }

  // Like Submit(), but also returns a (failed) Future when submission fails
  template <
      typename Function, typename... Args,
      typename FunctionRetType = typename std::result_of<Function && (Args && ...)>::type,
      typename RT = typename detail::ExecutorResultTraits<FunctionRetType>,
      typename ValueType = typename RT::ValueType>
  Future<ValueType> SubmitAsFuture(Function&& func, Args&&... args) {
    ARROW_ASSIGN_OR_RETURN_FUTURE(
        auto future, ValueType,
        Submit(std::forward<Function>(func), std::forward<Args>(args)...));
    return future;
  }

  // Return the level of parallelism (the number of tasks that may be executed
  // concurrently).  This may be an approximate number.
  virtual int GetCapacity() = 0;

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Executor);

  Executor() = default;

  // Subclassing API
  virtual Status SpawnReal(TaskHints hints, std::function<void()> task) = 0;
};

// An Executor implementation spawning tasks in FIFO manner on a fixed-size
// pool of worker threads.
class ARROW_EXPORT ThreadPool : public Executor {
 public:
  // Construct a thread pool with the given number of worker threads
  static Result<std::shared_ptr<ThreadPool>> Make(int threads);

  // Like Make(), but takes care that the returned ThreadPool is compatible
  // with destruction late at process exit.
  static Result<std::shared_ptr<ThreadPool>> MakeEternal(int threads);

  // Destroy thread pool; the pool will first be shut down
  ~ThreadPool();

  // Return the desired number of worker threads.
  // The actual number of workers may lag a bit before being adjusted to
  // match this value.
  int GetCapacity() override;

  // Dynamically change the number of worker threads.
  // This function returns quickly, but it may take more time before the
  // thread count is fully adjusted.
  Status SetCapacity(int threads);

  // Heuristic for the default capacity of a thread pool for CPU-bound tasks.
  // This is exposed as a static method to help with testing.
  static int DefaultCapacity();

  // Shutdown the pool.  Once the pool starts shutting down, new tasks
  // cannot be submitted anymore.
  // If "wait" is true, shutdown waits for all pending tasks to be finished.
  // If "wait" is false, workers are stopped as soon as currently executing
  // tasks are finished.
  Status Shutdown(bool wait = true);

  struct State;

 protected:
  FRIEND_TEST(TestThreadPool, SetCapacity);
  FRIEND_TEST(TestGlobalThreadPool, Capacity);
  friend ARROW_EXPORT ThreadPool* GetCpuThreadPool();

  ThreadPool();

  Status SpawnReal(TaskHints hints, std::function<void()> task) override;

  // Collect finished worker threads, making sure the OS threads have exited
  void CollectFinishedWorkersUnlocked();
  // Launch a given number of additional workers
  void LaunchWorkersUnlocked(int threads);
  // Get the current actual capacity
  int GetActualCapacity();
  // Reinitialize the thread pool if the pid changed
  void ProtectAgainstFork();

  static std::shared_ptr<ThreadPool> MakeCpuThreadPool();

  std::shared_ptr<State> sp_state_;
  State* state_;
  bool shutdown_on_destroy_;
#ifndef _WIN32
  pid_t pid_;
#endif
};

// Return the process-global thread pool for CPU-bound tasks.
ARROW_EXPORT ThreadPool* GetCpuThreadPool();

}  // namespace internal
}  // namespace arrow
