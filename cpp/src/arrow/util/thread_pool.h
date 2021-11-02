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
#include <memory>
#include <queue>
#include <type_traits>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
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
  using StopCallback = internal::FnOnce<void(const Status&)>;

  virtual ~Executor();

  // Spawn a fire-and-forget task.
  template <typename Function>
  Status Spawn(Function&& func) {
    return SpawnReal(TaskHints{}, std::forward<Function>(func), StopToken::Unstoppable(),
                     StopCallback{});
  }
  template <typename Function>
  Status Spawn(Function&& func, StopToken stop_token) {
    return SpawnReal(TaskHints{}, std::forward<Function>(func), std::move(stop_token),
                     StopCallback{});
  }
  template <typename Function>
  Status Spawn(TaskHints hints, Function&& func) {
    return SpawnReal(hints, std::forward<Function>(func), StopToken::Unstoppable(),
                     StopCallback{});
  }
  template <typename Function>
  Status Spawn(TaskHints hints, Function&& func, StopToken stop_token) {
    return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                     StopCallback{});
  }
  template <typename Function>
  Status Spawn(TaskHints hints, Function&& func, StopToken stop_token,
               StopCallback stop_callback) {
    return SpawnReal(hints, std::forward<Function>(func), std::move(stop_token),
                     std::move(stop_callback));
  }

  // Transfers a future to this executor.  Any continuations added to the
  // returned future will run in this executor.  Otherwise they would run
  // on the same thread that called MarkFinished.
  //
  // This is necessary when (for example) an I/O task is completing a future.
  // The continuations of that future should run on the CPU thread pool keeping
  // CPU heavy work off the I/O thread pool.  So the I/O task should transfer
  // the future to the CPU executor before returning.
  //
  // By default this method will only transfer if the future is not already completed.  If
  // the future is already completed then any callback would be run synchronously and so
  // no transfer is typically necessary.  However, in cases where you want to force a
  // transfer (e.g. to help the scheduler break up units of work across multiple cores)
  // then you can override this behavior with `always_transfer`.
  template <typename T>
  Future<T> Transfer(Future<T> future) {
    return DoTransfer(std::move(future), false);
  }

  // Overload of Transfer which will always schedule callbacks on new threads even if the
  // future is finished when the callback is added.
  //
  // This can be useful in cases where you want to ensure parallelism
  template <typename T>
  Future<T> TransferAlways(Future<T> future) {
    return DoTransfer(std::move(future), true);
  }

  // Submit a callable and arguments for execution.  Return a future that
  // will return the callable's result value once.
  // The callable's arguments are copied before execution.
  template <typename Function, typename... Args,
            typename FutureType = typename ::arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
  Result<FutureType> Submit(TaskHints hints, StopToken stop_token, Function&& func,
                            Args&&... args) {
    using ValueType = typename FutureType::ValueType;

    auto future = FutureType::Make();
    auto task = std::bind(::arrow::detail::ContinueFuture{}, future,
                          std::forward<Function>(func), std::forward<Args>(args)...);
    struct {
      WeakFuture<ValueType> weak_fut;

      void operator()(const Status& st) {
        auto fut = weak_fut.get();
        if (fut.is_valid()) {
          fut.MarkFinished(st);
        }
      }
    } stop_callback{WeakFuture<ValueType>(future)};
    ARROW_RETURN_NOT_OK(SpawnReal(hints, std::move(task), std::move(stop_token),
                                  std::move(stop_callback)));

    return future;
  }

  template <typename Function, typename... Args,
            typename FutureType = typename ::arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
  Result<FutureType> Submit(StopToken stop_token, Function&& func, Args&&... args) {
    return Submit(TaskHints{}, stop_token, std::forward<Function>(func),
                  std::forward<Args>(args)...);
  }

  template <typename Function, typename... Args,
            typename FutureType = typename ::arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
  Result<FutureType> Submit(TaskHints hints, Function&& func, Args&&... args) {
    return Submit(std::move(hints), StopToken::Unstoppable(),
                  std::forward<Function>(func), std::forward<Args>(args)...);
  }

  template <typename Function, typename... Args,
            typename FutureType = typename ::arrow::detail::ContinueFuture::ForSignature<
                Function && (Args && ...)>>
  Result<FutureType> Submit(Function&& func, Args&&... args) {
    return Submit(TaskHints{}, StopToken::Unstoppable(), std::forward<Function>(func),
                  std::forward<Args>(args)...);
  }

  // Return the level of parallelism (the number of tasks that may be executed
  // concurrently).  This may be an approximate number.
  virtual int GetCapacity() = 0;

  // Return true if the thread from which this function is called is owned by this
  // Executor. Returns false if this Executor does not support this property.
  virtual bool OwnsThisThread() { return false; }

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Executor);

  Executor() = default;

  template <typename T, typename FT = Future<T>, typename FTSync = typename FT::SyncType>
  Future<T> DoTransfer(Future<T> future, bool always_transfer = false) {
    auto transferred = Future<T>::Make();
    if (always_transfer) {
      CallbackOptions callback_options = CallbackOptions::Defaults();
      callback_options.should_schedule = ShouldSchedule::Always;
      callback_options.executor = this;
      auto sync_callback = [transferred](const FTSync& result) mutable {
        transferred.MarkFinished(result);
      };
      future.AddCallback(sync_callback, callback_options);
      return transferred;
    }

    // We could use AddCallback's ShouldSchedule::IfUnfinished but we can save a bit of
    // work by doing the test here.
    auto callback = [this, transferred](const FTSync& result) mutable {
      auto spawn_status =
          Spawn([transferred, result]() mutable { transferred.MarkFinished(result); });
      if (!spawn_status.ok()) {
        transferred.MarkFinished(spawn_status);
      }
    };
    auto callback_factory = [&callback]() { return callback; };
    if (future.TryAddCallback(callback_factory)) {
      return transferred;
    }
    // If the future is already finished and we aren't going to force spawn a thread
    // then we don't need to add another layer of callback and can return the original
    // future
    return future;
  }

  // Subclassing API
  virtual Status SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken,
                           StopCallback&&) = 0;
};

/// \brief An executor implementation that runs all tasks on a single thread using an
/// event loop.
///
/// Note: Any sort of nested parallelism will deadlock this executor.  Blocking waits are
/// fine but if one task needs to wait for another task it must be expressed as an
/// asynchronous continuation.
class ARROW_EXPORT SerialExecutor : public Executor {
 public:
  template <typename T = ::arrow::internal::Empty>
  using TopLevelTask = internal::FnOnce<Future<T>(Executor*)>;

  ~SerialExecutor() override;

  int GetCapacity() override { return 1; };
  Status SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken,
                   StopCallback&&) override;

  /// \brief Runs the TopLevelTask and any scheduled tasks
  ///
  /// The TopLevelTask (or one of the tasks it schedules) must either return an invalid
  /// status or call the finish signal. Failure to do this will result in a deadlock.  For
  /// this reason it is preferable (if possible) to use the helper methods (below)
  /// RunSynchronously/RunSerially which delegates the responsiblity onto a Future
  /// producer's existing responsibility to always mark a future finished (which can
  /// someday be aided by ARROW-12207).
  template <typename T = internal::Empty, typename FT = Future<T>,
            typename FTSync = typename FT::SyncType>
  static FTSync RunInSerialExecutor(TopLevelTask<T> initial_task) {
    Future<T> fut = SerialExecutor().Run<T>(std::move(initial_task));
    return FutureToSync(fut);
  }

 private:
  SerialExecutor();

  // State uses mutex
  struct State;
  std::shared_ptr<State> state_;

  template <typename T, typename FTSync = typename Future<T>::SyncType>
  Future<T> Run(TopLevelTask<T> initial_task) {
    auto final_fut = std::move(initial_task)(this);
    if (final_fut.is_finished()) {
      return final_fut;
    }
    final_fut.AddCallback([this](const FTSync&) { MarkFinished(); });
    RunLoop();
    return final_fut;
  }
  void RunLoop();
  void MarkFinished();
};

/// An Executor implementation spawning tasks in FIFO manner on a fixed-size
/// pool of worker threads.
///
/// Note: Any sort of nested parallelism will deadlock this executor.  Blocking waits are
/// fine but if one task needs to wait for another task it must be expressed as an
/// asynchronous continuation.
class ARROW_EXPORT ThreadPool : public Executor {
 public:
  // Construct a thread pool with the given number of worker threads
  static Result<std::shared_ptr<ThreadPool>> Make(int threads);

  // Like Make(), but takes care that the returned ThreadPool is compatible
  // with destruction late at process exit.
  static Result<std::shared_ptr<ThreadPool>> MakeEternal(int threads);

  // Destroy thread pool; the pool will first be shut down
  ~ThreadPool() override;

  // Return the desired number of worker threads.
  // The actual number of workers may lag a bit before being adjusted to
  // match this value.
  int GetCapacity() override;

  bool OwnsThisThread() override;

  // Return the number of tasks either running or in the queue.
  int GetNumTasks();

  // Dynamically change the number of worker threads.
  //
  // This function always returns immediately.
  // If fewer threads are running than this number, new threads are spawned
  // on-demand when needed for task execution.
  // If more threads are running than this number, excess threads are reaped
  // as soon as possible.
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

  // Wait for the thread pool to become idle
  //
  // This is useful for sequencing tests
  void WaitForIdle();

  struct State;

 protected:
  FRIEND_TEST(TestThreadPool, SetCapacity);
  FRIEND_TEST(TestGlobalThreadPool, Capacity);
  friend ARROW_EXPORT ThreadPool* GetCpuThreadPool();

  ThreadPool();

  Status SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken,
                   StopCallback&&) override;

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

/// \brief Potentially run an async operation serially (if use_threads is false)
/// \see RunSerially
///
/// If `use_threads` is true, the global CPU executor is used.
/// If `use_threads` is false, a temporary SerialExecutor is used.
/// `get_future` is called (from this thread) with the chosen executor and must
/// return a future that will eventually finish. This function returns once the
/// future has finished.
template <typename Fut, typename ValueType = typename Fut::ValueType>
typename Fut::SyncType RunSynchronously(FnOnce<Fut(Executor*)> get_future,
                                        bool use_threads) {
  if (use_threads) {
    auto fut = std::move(get_future)(GetCpuThreadPool());
    return FutureToSync(fut);
  } else {
    return SerialExecutor::RunInSerialExecutor<ValueType>(std::move(get_future));
  }
}

}  // namespace internal
}  // namespace arrow
