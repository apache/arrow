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

#include "arrow/util/thread-pool.h"
#include "arrow/util/io-util.h"
#include "arrow/util/logging.h"

#include <algorithm>
#include <string>

namespace arrow {
namespace internal {

ThreadPool::ThreadPool()
    : desired_capacity_(0), please_shutdown_(false), quick_shutdown_(false) {}

ThreadPool::~ThreadPool() { ARROW_UNUSED(Shutdown(false /* wait */)); }

Status ThreadPool::SetCapacity(size_t threads) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (please_shutdown_) {
    return Status::Invalid("operation forbidden during or after shutdown");
  }
  if (threads <= 0) {
    return Status::Invalid("ThreadPool capacity must be > 0");
  }
  CollectFinishedWorkersUnlocked();

  desired_capacity_ = threads;
  int64_t diff = desired_capacity_ - workers_.size();
  if (diff > 0) {
    LaunchWorkersUnlocked(static_cast<size_t>(diff));
  } else if (diff < 0) {
    // Wake threads to ask them to stop
    cv_.notify_all();
  }
  return Status::OK();
}

size_t ThreadPool::GetCapacity() {
  std::unique_lock<std::mutex> lock(mutex_);
  return workers_.size();
}

Status ThreadPool::Shutdown(bool wait) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (please_shutdown_) {
    return Status::Invalid("Shutdown() already called");
  }
  please_shutdown_ = true;
  quick_shutdown_ = !wait;
  cv_.notify_all();
  cv_shutdown_.wait(lock, [this] { return workers_.empty(); });
  if (!quick_shutdown_) {
    DCHECK_EQ(pending_tasks_.size(), 0);
  } else {
    pending_tasks_.clear();
  }
  CollectFinishedWorkersUnlocked();
  return Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
  for (auto& thread : finished_workers_) {
    // Make sure OS thread has exited
    thread.join();
  }
  finished_workers_.clear();
}

void ThreadPool::LaunchWorkersUnlocked(size_t threads) {
  for (size_t i = 0; i < threads; i++) {
    workers_.emplace_back();
    auto it = --workers_.end();
    *it = std::thread([this, it] { WorkerLoop(it); });
  }
}

void ThreadPool::WorkerLoop(std::list<std::thread>::iterator it) {
  std::unique_lock<std::mutex> lock(mutex_);

  // Since we hold the lock, `it` now points to the correct thread object
  // (LaunchWorkersUnlocked has exited)
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    // Execute pending tasks if any
    while (!pending_tasks_.empty() && !quick_shutdown_) {
      // If too many threads, secede from the pool.
      // We check this opportunistically at each loop iteration since
      // it releases the lock below.
      if (workers_.size() > desired_capacity_) {
        break;
      }
      {
        std::function<void()> task = std::move(pending_tasks_.front());
        pending_tasks_.pop_front();
        lock.unlock();
        task();
      }
      lock.lock();
    }
    // Now either the queue is empty *or* a quick shutdown was requested
    if (please_shutdown_ || workers_.size() > desired_capacity_) {
      break;
    }
    // Wait for next wakeup
    cv_.wait(lock);
  }

  // We're done.  Move our thread object to the trashcan of finished
  // workers.  This has two motivations:
  // 1) the thread object doesn't get destroyed before this function finishes
  //    (but we could call thread::detach() instead)
  // 2) we can explicitly join() the trashcan threads to make sure all OS threads
  //    are exited before the ThreadPool is destroyed.  Otherwise subtle
  //    timing conditions can lead to false positives with Valgrind.
  DCHECK_EQ(std::this_thread::get_id(), it->get_id());
  finished_workers_.push_back(std::move(*it));
  workers_.erase(it);
  if (please_shutdown_) {
    // Notify the function waiting in Shutdown().
    cv_shutdown_.notify_one();
  }
}

Status ThreadPool::SpawnReal(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (please_shutdown_) {
      return Status::Invalid("operation forbidden during or after shutdown");
    }
    CollectFinishedWorkersUnlocked();
    pending_tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
  return Status::OK();
}

Status ThreadPool::Make(size_t threads, std::shared_ptr<ThreadPool>* out) {
  auto pool = std::shared_ptr<ThreadPool>(new ThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  *out = std::move(pool);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Global thread pool

static size_t ParseOMPEnvVar(const char* name) {
  // OMP_NUM_THREADS is a comma-separated list of positive integers.
  // We are only interested in the first (top-level) number.
  std::string str;
  if (!GetEnvVar(name, &str).ok()) {
    return 0;
  }
  auto first_comma = str.find_first_of(',');
  if (first_comma != std::string::npos) {
    str = str.substr(0, first_comma);
  }
  try {
    return static_cast<size_t>(std::max(0LL, std::stoll(str)));
  } catch (...) {
    return 0;
  }
}

size_t ThreadPool::DefaultCapacity() {
  size_t capacity, limit;
  capacity = ParseOMPEnvVar("OMP_NUM_THREADS");
  if (capacity == 0) {
    capacity = std::thread::hardware_concurrency();
  }
  limit = ParseOMPEnvVar("OMP_THREAD_LIMIT");
  if (limit > 0) {
    capacity = std::min(limit, capacity);
  }
  if (capacity == 0) {
    ARROW_LOG(WARNING) << "Failed to determine the number of available threads, "
                          "using a hardcoded arbitrary value";
    capacity = 4;
  }
  return capacity;
}

// Helper for the singleton pattern
static std::shared_ptr<ThreadPool> MakePoolWithDefaultCapacity() {
  std::shared_ptr<ThreadPool> pool;
  DCHECK_OK(ThreadPool::Make(ThreadPool::DefaultCapacity(), &pool));
  return pool;
}

ThreadPool* CPUThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = MakePoolWithDefaultCapacity();
  return singleton.get();
}

}  // namespace internal

Status SetCPUThreadPoolCapacity(size_t threads) {
  return internal::CPUThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
