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
#include <functional>
#include <list>
#include <memory>
#include <thread>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

struct Task {
  FnOnce<void()> callable;
  StopToken stop_token;
  Executor::StopCallback stop_callback;
};

using ThreadIt = std::list<std::thread>::iterator;

/// Contains most of the logic needed to manage a pool of threads.  This is separate from
/// ThreadPoolBase because it needs to be referenced by the worker loop.  The worker loop
/// cannot have a shared_ptr to ThreadPoolBase or else the eternal pools will never
/// shutdown (they only shutdown now when all references are lost)
class WorkerControl {
 public:
  explicit WorkerControl(std::function<std::thread(ThreadIt)> thread_factory);

  /// Called after a fork to copy the state of the parent thread pool.  For the most part
  /// we want to reset the state but we will steal a few things
  void Reset(const WorkerControl& other);

  /// These methods are called by ThreadPoolBase
  Result<bool> SetCapacity(int desired_capacity);
  /// Marks the thread pool as shutting down.  ThreadPoolBase should notify all workers
  /// between this call and WaitForShutdownComplete so that they can wakeup and shutdown
  Status BeginShutdown(bool wait);
  /// Waits for all worker threads to finish
  Status WaitForShutdownComplete();
  /// Called when a new task has been added
  Status RecordTaskAdded();

  /// \see ThreadPool::NumTasksRunningOrQueued
  uint64_t NumTasksRunningOrQueued() const;
  /// \see ThreadPool::MaxTasksQueued
  uint64_t MaxTasksQueued() const;
  /// \see ThreadPool::TotalTasksQueued
  uint64_t TotalTasksQueued() const;

  /// These methods are called by worker loops

  /// This must be called when a worker finishes a task so that the controller can
  /// keep track of when there is enough work to launch new threads
  void RecordFinishedTask();
  /// This should be called when a worker thread is out of work to see if the thread pool
  /// has shutdown.  Unlike ShouldWorkerQuitNow this should only be called when a thread
  /// is out of work and would otherwise sleep.
  bool ShouldWorkerQuit(ThreadIt* thread_it);
  /// This should be called often to check if a worker thread should quit early (before it
  /// has finished all of its available work).
  bool ShouldWorkerQuitNow(ThreadIt* thread_it);
  /// Should be called first by a worker thread as soon as it starts up.  Until this
  /// call finishes `thread_it` will not have a valid value
  void WaitForReady();

  // Get the current actual capacity
  int GetActualCapacity() const;

 private:
  void LaunchWorkersUnlocked(int to_launch);
  // Marks a thread finished and removes it from the workers list
  void MarkThreadFinishedUnlocked(ThreadIt* thread_it);
  // Collect finished worker threads, making sure the OS threads have exited
  void CollectFinishedWorkersUnlocked();
  // Get the amount of threads we could still launch based on capacity and # of tasks
  int GetAdditionalThreadsNeeded() const;
  void RecordTaskSubmitted();

  std::atomic<uint64_t> num_tasks_running_;
  std::atomic<uint64_t> total_tasks_;
  std::atomic<uint64_t> max_tasks_;

  std::list<std::thread> workers_;
  // Trashcan for finished threads
  std::vector<std::thread> finished_workers_;
  // Desired number of threads
  int desired_capacity_ = 0;

  // Are we shutting down?
  bool please_shutdown_ = false;
  bool quick_shutdown_ = false;

  std::function<std::thread(ThreadIt)> thread_factory_;

  std::mutex mx;
  // Condition variable that the thread pool waits on when it is waiting for all worker
  // threads to finish while shutting down
  std::condition_variable cv_shutdown;

  friend class ThreadPoolBase;
};

class ARROW_EXPORT ThreadPoolBase : public ThreadPool {
 public:
  virtual ~ThreadPoolBase();

  // -------------- Management API -------------
  // These methods must all be guarded with ProtectAgainstFork

  int GetCapacity() override;
  int GetActualCapacity() const override;
  Status SetCapacity(int threads) override;
  Status Shutdown(bool wait = true) override;

  // ------------- Statistics API ---------------

  uint64_t NumTasksRunningOrQueued() const override;
  uint64_t MaxTasksQueued() const override;
  uint64_t TotalTasksQueued() const override;

 protected:
  FRIEND_TEST(TestThreadPool, DestroyWithoutShutdown);
  FRIEND_TEST(TestThreadPool, SetCapacity);
  FRIEND_TEST(TestGlobalThreadPool, Capacity);
  friend ARROW_EXPORT ThreadPool* GetCpuThreadPool();

  explicit ThreadPoolBase(bool eternal = false);

  Status SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken,
                   StopCallback&&) override;
  /// Must be called by child threads on shutdown to ensure the thread pool is stopped
  /// (unless shutdown_on_destroy_ is false)
  void MaybeShutdownOnDestroy();

  // Reinitialize the thread pool if the pid changed
  void ProtectAgainstFork();
  /// Called on the child process after a fork.  After a fork all threads will have ceased
  /// running in the child process.  This method should clean up the thread pool state and
  /// restart any previously running threads.
  ///
  /// The behavior is somewhat ill-defined if tasks are running when the fork happened.
  /// For more details see ARROW-12879
  virtual void ResetAfterFork();

  /// Launch a worker thread
  virtual std::thread LaunchWorker(std::shared_ptr<WorkerControl> control,
                                   ThreadIt thread_it) = 0;
  /// Add a task to the task queue(s)
  virtual void DoSubmitTask(TaskHints hints, Task task) = 0;
  /// After a capacity change this is called so idle workers can shutdown
  virtual void WakeupWorkersToCheckShutdown() = 0;

#ifndef _WIN32
  pid_t pid_;
#endif
  bool shutdown_on_destroy_ = false;
  std::shared_ptr<WorkerControl> control_;
};

}  // namespace internal
}  // namespace arrow
