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

#ifndef ARROW_UTIL_THREAD_POOL_H
#define ARROW_UTIL_THREAD_POOL_H

#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <future>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow {

// Set the number of worker threads used by the process-global thread pool
// for CPU-bound tasks.
ARROW_EXPORT Status SetCPUThreadPoolCapacity(size_t threads);

namespace internal {

namespace detail {

// Needed because std::packaged_task is not copyable and hence not convertible
// to std::function.
template <typename R, typename... Args>
struct packaged_task_wrapper {
  using PackagedTask = std::packaged_task<R(Args...)>;

  explicit packaged_task_wrapper(PackagedTask&& task)
      : task_(std::make_shared<PackagedTask>(std::forward<PackagedTask>(task))) {}

  void operator()(Args&&... args) { return (*task_)(std::forward<Args>(args)...); }
  std::shared_ptr<PackagedTask> task_;
};

}  // namespace detail

class ThreadPool {
 public:
  // Construct a thread pool with the given number of worker threads
  static Status Make(size_t threads, std::shared_ptr<ThreadPool>* out);

  // Destroy thread pool; the pool will first be shut down
  ~ThreadPool();

  // Dynamically change the number of worker threads.
  // This function returns quickly, but it may take more time before the
  // thread count is fully adjusted.
  Status SetCapacity(size_t threads);

  // Shutdown the pool.  Once the pool starts shutting down, new tasks
  // cannot be submitted anymore.
  // If "wait" is true, shutdown waits for all pending tasks to be finished.
  // If "wait" is false, workers are stopped as soon as currently executing
  // tasks are finished.
  Status Shutdown(bool wait = true);

  // Spawn a fire-and-forget task on one of the workers.
  template <typename Function>
  Status Spawn(Function&& func) {
    return SpawnReal(std::forward<Function>(func));
  }

  // Submit a callable and arguments for execution.  Return a future that
  // will return the callable's result value once.
  // The callable's arguments are copied before execution.
  // Since the function is variadic and needs to return a result (the future),
  // an exception is raised if the task fails spawning (which currently
  // only occurs if the ThreadPool is shutting down).
  template <typename Function, typename... Args,
            typename Result = typename std::result_of<Function && (Args && ...)>::type>
  std::future<Result> Submit(Function&& func, Args&&... args) {
    // Trying to templatize std::packaged_task with Function doesn't seem
    // to work, so go through std::bind to simplify the packaged signature
    using PackagedTask = std::packaged_task<Result()>;
    auto task = PackagedTask(std::bind(std::forward<Function>(func), args...));
    auto fut = task.get_future();

    Status st = SpawnReal(detail::packaged_task_wrapper<Result>(std::move(task)));
    if (!st.ok()) {
      throw std::runtime_error(st.ToString());
    }
    return fut;
  }

 protected:
  FRIEND_TEST(TestThreadPool, SetCapacity);

  ThreadPool();

  ARROW_DISALLOW_COPY_AND_ASSIGN(ThreadPool);

  Status SpawnReal(std::function<void()> task);
  // Collect finished worker threads, making sure the OS threads have exited
  void CollectFinishedWorkersUnlocked();
  // Launch a given number of additional workers
  void LaunchWorkersUnlocked(size_t threads);
  void WorkerLoop(std::list<std::thread>::iterator it);
  size_t GetCapacity();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::condition_variable cv_shutdown_;

  std::list<std::thread> workers_;
  // Trashcan for finished threads
  std::vector<std::thread> finished_workers_;
  std::deque<std::function<void()>> pending_tasks_;

  // Desired number of threads
  size_t desired_capacity_;
  // Are we shutting down?
  bool please_shutdown_;
  bool quick_shutdown_;
};

// Return the process-global thread pool for CPU-bound tasks.
ThreadPool* CPUThreadPool();

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_THREAD_POOL_H
