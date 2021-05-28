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

#include "arrow/util/ws_thread_pool.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <sstream>
#include <thread>

#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"

constexpr bool PRINT_DEBUG = false;

namespace arrow {
namespace internal {

ResizableRingBuffer::ResizableRingBuffer(std::size_t size)
    : size_(size), mask_(size - 1), arr_(size) {
  // Confirm size is a power of 2
  DCHECK_EQ(size & (size - 1), 0);
}

std::size_t ResizableRingBuffer::size() const { return size_; }

ThreadPool::Task* ResizableRingBuffer::Get(std::size_t i) {
  return arr_[i & mask_].load(std::memory_order_relaxed);
}

void ResizableRingBuffer::Put(std::size_t i, ThreadPool::Task* task) {
  arr_[i & mask_].store(task, std::memory_order_relaxed);
}

ResizableRingBuffer ResizableRingBuffer::Resize(std::size_t top, std::size_t bottom) {
  std::size_t new_size = size_ << 1;
  ResizableRingBuffer new_buf(new_size);
  for (std::size_t i = top; i < bottom; i++) {
    new_buf.Put(i, Get(i));
  }
  return new_buf;
}

WorkQueue::WorkQueue(std::size_t initial_capacity)
    : top_(0),
      bottom_(0),
      tasks_(new ResizableRingBuffer(initial_capacity)),
      to_delete_(32) {}

WorkQueue::~WorkQueue() {
  for (auto& item : to_delete_) {
    delete item;
  }
  delete tasks_.load();
}

bool WorkQueue::Empty() const {
  std::size_t bottom = bottom_.load(std::memory_order_relaxed);
  std::size_t top = top_.load(std::memory_order_relaxed);
  return bottom <= top;
}

std::size_t WorkQueue::Size() const {
  std::size_t bottom = bottom_.load(std::memory_order_relaxed);
  std::size_t top = top_.load(std::memory_order_relaxed);
  return std::max(bottom - top, static_cast<std::size_t>(0));
}

std::size_t WorkQueue::Capacity() const {
  return tasks_.load(std::memory_order_relaxed)->size();
}

void WorkQueue::Push(ThreadPool::Task* task) {
  DCHECK(task->callable);
  std::size_t bottom = bottom_.load(std::memory_order_relaxed);
  std::size_t top = top_.load(std::memory_order_acquire);
  ResizableRingBuffer* tasks = tasks_.load(std::memory_order_relaxed);
  std::size_t count_existing = bottom - top;

  // The queue is full so we need to resize.  There is only one producer so only one
  // thread can be in resize at once.  Other threads will only see the new queue once it
  // is fully initialized
  if (tasks->size() < count_existing + 1) {
    ResizableRingBuffer* bigger = new ResizableRingBuffer(tasks->Resize(top, bottom));
    // Collect to delete later
    to_delete_.push_back(tasks);
    tasks = bigger;
    // FIXME - Should be relaxed
    tasks_.store(tasks, std::memory_order_relaxed);
  }

  tasks->Put(bottom, std::move(task));
  std::atomic_thread_fence(std::memory_order_release);
  bottom_.store(bottom + 1, std::memory_order_relaxed);
}

ThreadPool::Task* WorkQueue::Pop() {
  std::size_t new_bottom = bottom_.load(std::memory_order_relaxed) - 1;
  ResizableRingBuffer* tasks = tasks_.load(std::memory_order_relaxed);

  bottom_.store(new_bottom, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  std::size_t top = top_.load(std::memory_order_relaxed);

  ThreadPool::Task* task = nullptr;
  if (top <= new_bottom) {
    task = tasks->Get(new_bottom);
    if (top == new_bottom) {
      // If we are removing the last item we need to do a double-check to handle the
      // case where a stealing thread comes in and steals the item at the same time we are
      // trying to grab it.
      if (!top_.compare_exchange_strong(top, top + 1, std::memory_order_seq_cst,
                                        std::memory_order_relaxed)) {
        task = nullptr;
      }
      bottom_.store(new_bottom + 1, std::memory_order_relaxed);
    }
  } else {
    // Empty, return null and put the bottom back
    bottom_.store(new_bottom + 1, std::memory_order_relaxed);
  }

  DCHECK(!task || task->callable);

  return task;
}

ThreadPool::Task* WorkQueue::Steal() {
  std::size_t top = top_.load(std::memory_order_acquire);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  std::size_t bottom = bottom_.load(std::memory_order_acquire);

  ThreadPool::Task* task = nullptr;

  if (top < bottom) {
    ResizableRingBuffer* tasks = tasks_.load(std::memory_order_consume);
    task = tasks->Get(top);

    // Need to check and see if the owning thread grabbed the item before we could
    // mark it claimed.
    if (!top_.compare_exchange_strong(top, top + 1, std::memory_order_seq_cst,
                                      std::memory_order_relaxed)) {
      return nullptr;
    }
  }

  return task;
}

void WorkQueue::Clear() {
  for (auto& item : to_delete_) {
    delete item;
  }
  bottom_.store(0);
  top_.store(0);
}

Result<std::shared_ptr<ThreadPool>> WorkStealingThreadPool::Make(int threads) {
  auto pool = std::shared_ptr<ThreadPool>(new WorkStealingThreadPool(threads));
  RETURN_NOT_OK(pool->SetCapacity(threads));
  return pool;
}

Result<std::shared_ptr<ThreadPool>> WorkStealingThreadPool::MakeEternal(int threads) {
  ARROW_ASSIGN_OR_RAISE(auto pool, Make(threads));
  // On Windows, the ThreadPool destructor may be called after non-main threads
  // have been killed by the OS, and hang in a condition variable.
  // On Unix, we want to avoid leak reports by Valgrind.
#ifdef _WIN32
  pool->shutdown_on_destroy_ = false;
#endif
  return pool;
}

WorkStealingThreadPool::WorkStealingThreadPool(int capacity)
    : task_queues_(capacity), next_thread_index_(0), searching_(0){};

WorkStealingThreadPool::~WorkStealingThreadPool() {
  // In the event of a quick shutdown we may have extra tasks lying around
  while (!unaffiliated_queue_.Empty()) {
    delete unaffiliated_queue_.Pop();
  }
  for (auto& thread_q : task_queues_) {
    while (!thread_q.Empty()) {
      delete thread_q.Pop();
    }
  }
}

void WorkStealingThreadPool::ResetAfterFork() {
  unaffiliated_queue_.Clear();
  for (auto& task_queue : task_queues_) {
    task_queue.Clear();
  }
}

constexpr std::size_t NOT_IN_POOL = -1;
thread_local std::size_t tls_thread_index = NOT_IN_POOL;

struct StlThread : public Thread {
  StlThread(std::thread* thread) : thread(thread) {}
  ~StlThread() {
    if (thread) {
      delete thread;
    }
  }
  void Join() { thread->join(); }
  bool IsCurrentThread() const { return std::this_thread::get_id() == thread->get_id(); }
  // This is called on the child process.  The actual pthread is no longer valid.  We
  // cannot delete it any longer.  Simply drop the reference.  This is a bit of a leak
  // so feel free to replace with something more clever.
  void ResetAfterFork() { thread = nullptr; }
  std::thread* thread = nullptr;
};

std::shared_ptr<Thread> WorkStealingThreadPool::LaunchWorker(ThreadPool::Control* control,
                                                             ThreadIt thread_it) {
  // FIXME: Not safe for capacity resize
  std::size_t worker_index = next_thread_index_.fetch_add(1, std::memory_order_relaxed);
  auto self = shared_from_this();
  std::thread* thread = new std::thread([self, control, thread_it, worker_index] {
    WorkStealingThreadPool::WorkerLoop(self, control, thread_it, worker_index);
  });
  return std::make_shared<StlThread>(thread);
}

void WorkStealingThreadPool::DoSubmitTask(TaskHints hints, ThreadPool::Task task) {
  auto task_ptr = new ThreadPool::Task(std::move(task));
  if (tls_thread_index == NOT_IN_POOL) {
    // FIXME: Not safe as multiple threads may be pushing to the unaffiliated queue
    unaffiliated_queue_.Push(task_ptr);
  } else {
    DCHECK_LT(tls_thread_index, task_queues_.size());
    task_queues_[tls_thread_index].Push(task_ptr);
  }
  // If we are already searching then the searcher will notify when done
  if (!searching_.load()) {
    NotifyIdleWorker();
  }
}

namespace {

void RunTask(ThreadPool::Task* task, const std::shared_ptr<ThreadPool>& thread_pool) {
  StopToken* stop_token = &task->stop_token;
  if (!stop_token->IsStopRequested()) {
    std::move(task->callable)();
  } else {
    if (task->stop_callback) {
      std::move(task->stop_callback)(stop_token->Poll());
    }
  }
  ARROW_UNUSED(std::move(task));  // release resources before waiting for lock
  delete task;
  thread_pool->RecordFinishedTask();
}

}  // namespace

bool WorkStealingThreadPool::Empty() { return unaffiliated_queue_.Empty(); }

void WorkStealingThreadPool::WorkerLoop(
    std::shared_ptr<WorkStealingThreadPool> thread_pool, Control* tp_control,
    ThreadIt thread_it, int thread_index) {
  thread_pool->NotifyWorkerStarted();
  tls_thread_index = thread_index;
  DCHECK_NE(tls_thread_index, NOT_IN_POOL);
  DCHECK_LT(tls_thread_index, thread_pool->task_queues_.size());
  DCHECK((*thread_it)->IsCurrentThread());

  auto& my_queue = thread_pool->task_queues_[thread_index];

  while (true) {
    bool stopped = false;
    // First, execute tasks belonging to this thread
    if (PRINT_DEBUG) {
      std::cout << tls_thread_index << ": Loop start" << std::endl;
    }
    while (!my_queue.Empty() && !thread_pool->ShouldWorkerQuitNow(thread_it, &stopped)) {
      {
        ThreadPool::Task* task = my_queue.Pop();
        // Need to check since task may have been stolen
        if (task) {
          if (PRINT_DEBUG) {
            std::cout << tls_thread_index << ": Found my task" << std::endl;
          }
          RunTask(task, thread_pool);
        }
      }
    }

    if (!stopped) {
      thread_pool->ShouldWorkerQuitNow(thread_it, &stopped);
    }
    if (stopped) {
      break;
    }

    // Second, try and grab a task from the unaffiliated queue
    ThreadPool::Task* unassigned_task = thread_pool->unaffiliated_queue_.Steal();
    if (unassigned_task) {
      if (PRINT_DEBUG) {
        std::cout << tls_thread_index << ": Stole unassigned task" << std::endl;
      }
      RunTask(unassigned_task, thread_pool);
      // Since we ran a task lets jump back to the start and see if we ended up with any
      // child tasks
      continue;
    }

    auto task_stolen = false;
    // Try and steal if no one else is
    auto was_searching = thread_pool->searching_.fetch_or(1);
    if (!was_searching) {
      // FIXME: Should pick a random starting point
      ThreadPool::Task* stolen_task = nullptr;
      for (auto& task_queue : thread_pool->task_queues_) {
        stolen_task = task_queue.Steal();
        if (stolen_task) {
          break;
        }
      }
      thread_pool->searching_.store(0);
      // If we were able to find a task it seems likely there is work to do so wake up
      // sibling
      if (stolen_task) {
        if (PRINT_DEBUG) {
          std::cout << tls_thread_index << ": Running stolen task" << std::endl;
        }
        thread_pool->NotifyIdleWorker();
        RunTask(stolen_task, thread_pool);
      }
    }

    // If we stole a task then we can go through the loop again before we go idle
    if (!task_stolen) {
      // Now either the queue is empty *or* a quick shutdown was requested
      if (thread_pool->ShouldWorkerQuit(thread_it)) {
        if (PRINT_DEBUG) {
          std::cout << tls_thread_index << ": Quitting" << std::endl;
        }
        break;
      }

      // Wait for next wakeup
      thread_pool->WaitForWork();
    }
  }

  // FIXME: Do I need to worry about this?
  tls_thread_index = NOT_IN_POOL;
}

}  // namespace internal
}  // namespace arrow