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

#include "arrow/util/thread_pool.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool_internal.h"

namespace arrow {
namespace internal {

Executor::~Executor() = default;

struct SerialExecutor::State {
  std::deque<Task> task_queue;
  std::mutex mutex;
  std::condition_variable wait_for_tasks;
  bool finished{false};
};

SerialExecutor::SerialExecutor() : state_(std::make_shared<State>()) {}

SerialExecutor::~SerialExecutor() = default;

Status SerialExecutor::SpawnReal(TaskHints hints, FnOnce<void()> task,
                                 StopToken stop_token, StopCallback&& stop_callback) {
  // While the SerialExecutor runs tasks synchronously on its main thread,
  // SpawnReal may be called from external threads (e.g. when transferring back
  // from blocking I/O threads), so we need to keep the state alive *and* to
  // lock its contents.
  //
  // Note that holding the lock while notifying the condition variable may
  // not be sufficient, as some exit paths in the main thread are unlocked.
  auto state = state_;
  {
    std::lock_guard<std::mutex> lk(state->mutex);
    state->task_queue.push_back(
        Task{std::move(task), std::move(stop_token), std::move(stop_callback)});
  }
  state->wait_for_tasks.notify_one();
  return Status::OK();
}

void SerialExecutor::MarkFinished() {
  // Same comment as SpawnReal above
  auto state = state_;
  {
    std::lock_guard<std::mutex> lk(state->mutex);
    state->finished = true;
  }
  state->wait_for_tasks.notify_one();
}

void SerialExecutor::RunLoop() {
  // This is called from the SerialExecutor's main thread, so the
  // state is guaranteed to be kept alive.
  std::unique_lock<std::mutex> lk(state_->mutex);

  while (!state_->finished) {
    while (!state_->task_queue.empty()) {
      Task task = std::move(state_->task_queue.front());
      state_->task_queue.pop_front();
      lk.unlock();
      if (!task.stop_token.IsStopRequested()) {
        std::move(task.callable)();
      } else {
        if (task.stop_callback) {
          std::move(task.stop_callback)(task.stop_token.Poll());
        }
        // Can't break here because there may be cleanup tasks down the chain we still
        // need to run.
      }
      lk.lock();
    }
    // In this case we must be waiting on work from external (e.g. I/O) executors.  Wait
    // for tasks to arrive (typically via transferred futures).
    state_->wait_for_tasks.wait(
        lk, [&] { return state_->finished || !state_->task_queue.empty(); });
  }
}

using ThreadIt = std::list<std::thread>::iterator;

WorkerControl::WorkerControl(std::function<std::thread(ThreadIt)> thread_factory)
    : num_tasks_running_(0),
      total_tasks_(0),
      max_tasks_(0),
      thread_factory_(std::move(thread_factory)) {}

void WorkerControl::Reset(const WorkerControl& other) {
  please_shutdown_ = other.please_shutdown_;
  quick_shutdown_ = other.quick_shutdown_;
  // Launch worker threads anew
  if (!please_shutdown_) {
    ARROW_UNUSED(SetCapacity(other.desired_capacity_));
  }
}

Result<bool> WorkerControl::SetCapacity(int desired_capacity) {
  std::lock_guard<std::mutex> lock(mx);
  if (please_shutdown_) {
    return Status::Invalid("operation forbidden during or after shutdown");
  }
  if (desired_capacity <= 0) {
    return Status::Invalid("ThreadPool capacity must be > 0");
  }
  CollectFinishedWorkersUnlocked();

  desired_capacity_ = desired_capacity;
  // See if we need to increase or decrease the number of running threads.  There is a
  // bit of finesse here.  NumTasksRunningOrQueued can change outside the mutex.
  //
  // It's possible we spawn workers when we didn't strictly need to (i.e.
  // NumTasksRunningOrQueued goes down after we check).  However, that should be ok.
  //
  // It should not be possible we fail to spawn tasks when needed.  Any call to increase
  // NumTasksRunningOrQueued will have its own accompanying call to launch workers.
  int required = GetAdditionalThreadsNeeded();

  if (required > 0) {
    // Some tasks are pending, spawn the number of needed threads immediately
    LaunchWorkersUnlocked(required);
  } else if (required < 0) {
    return true;
  }
  return false;
}

void WorkerControl::LaunchWorkersUnlocked(int to_launch) {
  for (int i = 0; i < to_launch; i++) {
    workers_.emplace_back();
    DCHECK_LE(static_cast<int>(workers_.size()), desired_capacity_);
    auto it = --(workers_.end());
    *it = thread_factory_(it);
  }
}

Status WorkerControl::BeginShutdown(bool wait) {
  std::unique_lock<std::mutex> lock(mx);

  if (please_shutdown_) {
    return Status::Invalid("Shutdown() already called");
  }
  please_shutdown_ = true;
  quick_shutdown_ = !wait;
  return Status::OK();
}

Status WorkerControl::WaitForShutdownComplete() {
  std::unique_lock<std::mutex> lock(mx);
  cv_shutdown.wait(lock, [this] { return workers_.empty(); });
  DCHECK(workers_.empty());
  CollectFinishedWorkersUnlocked();
  if (!quick_shutdown_) {
    DCHECK_EQ(NumTasksRunningOrQueued(), 0);
  }
  return Status::OK();
}

Status WorkerControl::RecordTaskAdded() {
  {
    if (please_shutdown_) {
      return Status::Invalid("operation forbidden during or after shutdown");
    }
    if (!finished_workers_.empty()) {
      std::lock_guard<std::mutex> lock(mx);
      // Maybe someone snuck in and cleared this out while we were grabbing the lock
      // but it should be harmless to do it again.
      CollectFinishedWorkersUnlocked();
    }
    RecordTaskSubmitted();
    if (GetAdditionalThreadsNeeded() > 0) {
      std::lock_guard<std::mutex> lock(mx);
      // We avoided locking on the hot path unless we needed to so we have to double
      // check to ensure we still have spare capacity
      if (!please_shutdown_ && GetAdditionalThreadsNeeded() > 0) {
        // We can still spin up more workers so spin up a new worker
        LaunchWorkersUnlocked(/*to_launch=*/1);
      }
    }
  }
  return Status::OK();
}

void WorkerControl::RecordFinishedTask() {
  num_tasks_running_.fetch_sub(1, std::memory_order_relaxed);
}

bool WorkerControl::ShouldWorkerQuit(ThreadIt* thread_it) {
  if (*thread_it == workers_.end()) {
    return true;
  }
  // At this point we have run out of work and are off the hot path so it is safe to
  // lock
  std::lock_guard<std::mutex> lock(mx);
  if (please_shutdown_) {
    MarkThreadFinishedUnlocked(thread_it);
    return true;
  }
  return false;
}

bool WorkerControl::ShouldWorkerQuitNow(ThreadIt* thread_it) {
  if (*thread_it == workers_.end()) {
    return true;
  }
  // This method is called often but returns true very rarely so we only grab
  // the mutex if it looks like we are going to be quitting.  As a result we need
  // to do a bit of a double-check
  if (quick_shutdown_ || desired_capacity_ < GetActualCapacity()) {
    std::lock_guard<std::mutex> lock(mx);
    if (quick_shutdown_ || desired_capacity_ < GetActualCapacity()) {
      MarkThreadFinishedUnlocked(thread_it);
      return true;
    } else {
      return false;
    }
  }
  return false;
}

void WorkerControl::MarkThreadFinishedUnlocked(ThreadIt* thread_it) {
  finished_workers_.push_back(std::move(**thread_it));
  workers_.erase(*thread_it);
  *thread_it = workers_.end();
  cv_shutdown.notify_one();
}

void WorkerControl::WaitForReady() {
  // Grab the lock (at least briefly) when the thread starts, to make sure the
  // launching task is finished before we start work.
  std::lock_guard<std::mutex> lock(mx);
}

uint64_t WorkerControl::NumTasksRunningOrQueued() const {
  return num_tasks_running_.load(std::memory_order_acquire);
}
uint64_t WorkerControl::MaxTasksQueued() const {
  return max_tasks_.load(std::memory_order_relaxed);
}
uint64_t WorkerControl::TotalTasksQueued() const {
  // This may lag behind the actual value a bit
  return total_tasks_.load(std::memory_order_relaxed);
}

int WorkerControl::GetActualCapacity() const { return static_cast<int>(workers_.size()); }

void WorkerControl::CollectFinishedWorkersUnlocked() {
  for (auto& thread : finished_workers_) {
    thread.join();
  }
  finished_workers_.clear();
}
int WorkerControl::GetAdditionalThreadsNeeded() const {
  int unallocated_tasks =
      static_cast<int>(NumTasksRunningOrQueued()) - GetActualCapacity();
  int unused_capacity = desired_capacity_ - GetActualCapacity();
  return std::min(unallocated_tasks, unused_capacity);
}

void WorkerControl::RecordTaskSubmitted() {
  uint64_t num_tasks_running =
      num_tasks_running_.fetch_add(1, std::memory_order_release) + 1;
  // This is incorrect if multiple threads are submitting tasks at the same time
  // but correctness is not worth introducing the cost of cross-thread
  // synchronization
  if (num_tasks_running > max_tasks_.load(std::memory_order_relaxed)) {
    max_tasks_.store(num_tasks_running_, std::memory_order_relaxed);
  }
  total_tasks_.fetch_add(1, std::memory_order_relaxed);
}

/// A ThreadPool implementation which uses one lock-protected task queue which
/// the workers all share.
class ARROW_EXPORT SimpleThreadPool
    : public ThreadPoolBase,
      public std::enable_shared_from_this<SimpleThreadPool> {
 public:
  // Destroy thread pool; the pool will first be shut down
  ~SimpleThreadPool() override;

  class SimpleTaskQueue;

 protected:
  explicit SimpleThreadPool(bool eternal = false);
  void ResetAfterFork() override;
  std::thread LaunchWorker(std::shared_ptr<WorkerControl> control,
                           ThreadIt thread_it) override;
  static void WorkerLoop(std::shared_ptr<WorkerControl> thread_pool,
                         std::shared_ptr<SimpleTaskQueue> task_queue, ThreadIt self);

  void DoSubmitTask(TaskHints hints, Task task) override;
  void WakeupWorkersToCheckShutdown() override;
  util::optional<Task> PopTask();

  std::shared_ptr<SimpleTaskQueue> task_queue_;

  friend Result<std::shared_ptr<ThreadPool>> MakeSimpleThreadPool(int num_threads);
  friend Result<std::shared_ptr<ThreadPool>> MakeEternalSimpleThreadPool(int num_threads);
};

void ThreadPoolBase::ResetAfterFork() {
  // We need to reinitialize the control block because any threads holding a mutex when
  // the fork happened will never release those mutexes (this is true of all other
  // mutexes too.  See ARROW-12879 for follow-up)
  //
  // The proper way to do this is to use pthread_atfork to obtain the mutex before
  // forking.  Improvements welcome, this approach leaks memory because we do not delete
  // the old instance.  We cannot delete the old instance because mutexes cannot be
  // deleted while locked (and they will never be unlocked if they were held by any
  // threads at the time of fork).
  auto old_control = control_;
  control_ = std::make_shared<WorkerControl>(
      [this](ThreadIt it) { return this->LaunchWorker(control_, it); });
  control_->Reset(*old_control);
}

uint64_t ThreadPoolBase::NumTasksRunningOrQueued() const {
  return control_->NumTasksRunningOrQueued();
}
uint64_t ThreadPoolBase::MaxTasksQueued() const { return control_->MaxTasksQueued(); }
uint64_t ThreadPoolBase::TotalTasksQueued() const { return control_->TotalTasksQueued(); }
int ThreadPoolBase::GetActualCapacity() const { return control_->GetActualCapacity(); }

/// A simple task FIFO task queue that can be shared by multiple consumers and producers
class SimpleThreadPool::SimpleTaskQueue {
 public:
  bool Empty() { return task_count_.load(std::memory_order_acquire) == 0; }
  void NotifyIdleWorker() { waiting_for_work_.notify_one(); }
  void WakeupForShutdown() {
    std::lock_guard<std::mutex> lock(mx_);
    waiting_for_work_.notify_all();
  }
  void Clear() { pending_tasks_.clear(); }
  bool WaitForWork(std::function<bool()> should_shutdown) {
    std::unique_lock<std::mutex> lock(mx_);
    waiting_for_work_.wait(lock, [&] { return !Empty() || should_shutdown(); });
    return !Empty() || !should_shutdown();
  }

  util::optional<Task> PopTask() {
    std::lock_guard<std::mutex> lock(mx_);
    if (pending_tasks_.empty()) {
      return util::nullopt;
    }
    // Not a big deal if workers think there are tasks when there aren't so don't
    // impose memory order here.
    task_count_.fetch_sub(1, std::memory_order_relaxed);
    Task task = std::move(pending_tasks_.front());
    pending_tasks_.pop_front();
    return std::move(task);
  }

  void PushTask(TaskHints hints, Task task) {
    std::unique_lock<std::mutex> lock(mx_);
    pending_tasks_.push_back(std::move(task));
    task_count_.fetch_add(1, std::memory_order_release);
    lock.unlock();
    NotifyIdleWorker();
  }

 private:
  std::deque<Task> pending_tasks_;
  // Store task count separately so we can quickly check if pending_tasks_ is empty
  // without grabbing the lock
  std::atomic<std::size_t> task_count_;
  std::mutex mx_;
  std::condition_variable waiting_for_work_;
};

// The worker loop must be capable of running after all other references to
// `thread_pool` have been lost so we capture a shared_ptr
void SimpleThreadPool::WorkerLoop(std::shared_ptr<WorkerControl> control,
                                  std::shared_ptr<SimpleTaskQueue> task_queue,
                                  ThreadIt self) {
  // Inform the caller we started
  control->WaitForReady();
  // thread_it is our reference to the thread object's position in the worker list,
  // needed for notifying the pool when finished.
  DCHECK(std::this_thread::get_id() == (*self).get_id());

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    // Execute pending tasks if any, check after each task for a quick shutdown
    while (!control->ShouldWorkerQuitNow(&self) && !task_queue->Empty()) {
      auto maybe_task = task_queue->PopTask();
      if (!maybe_task.has_value()) {
        break;
      }
      StopToken* stop_token = &maybe_task->stop_token;
      if (!stop_token->IsStopRequested()) {
        std::move(maybe_task->callable)();
      } else {
        if (maybe_task->stop_callback) {
          std::move(maybe_task->stop_callback)(stop_token->Poll());
        }
      }
      control->RecordFinishedTask();
    }

    // Now either the queue is empty *or* a quick shutdown was requested
    if (control->ShouldWorkerQuitNow(&self)) {
      break;
    }
    if (!task_queue->WaitForWork([&] { return control->ShouldWorkerQuit(&self); })) {
      // If WaitForWork returns false we must've been woken up to shut down
      break;
    }
  }
  DCHECK_GE(control->NumTasksRunningOrQueued(), 0);
}

ThreadPoolBase::ThreadPoolBase(bool eternal)
    : shutdown_on_destroy_(true),
      control_(std::make_shared<WorkerControl>(
          [this](ThreadIt it) { return LaunchWorker(control_, it); })) {
#ifndef _WIN32
  pid_ = getpid();
#else
  // On Windows, the ThreadPool destructor may be called after non-main threads
  // have been killed by the OS, and hang in a condition variable.
  // On Unix, we want to avoid leak reports by Valgrind.
  shutdown_on_destroy_ = !eternal;
#endif
}

ThreadPoolBase::~ThreadPoolBase() {
  if (shutdown_on_destroy_) {
    // Child implementations MUST call MaybeShutdownOnDestroy()
    DCHECK_EQ(control_->workers_.size(), 0);
  }
}

void ThreadPoolBase::MaybeShutdownOnDestroy() {
  if (shutdown_on_destroy_) {
    ARROW_UNUSED(Shutdown(false /* wait */));
  }
}

void ThreadPoolBase::ProtectAgainstFork() {
#ifndef _WIN32
  pid_t current_pid = getpid();
  if (pid_ != current_pid) {
    // Reinitialize internal state in child process after fork()
    // Ideally we would use pthread_at_fork(), but that doesn't allow
    // storing an argument, hence we'd need to maintain a list of all
    // existing ThreadPools.
    ResetAfterFork();
    pid_ = current_pid;
  }
#endif
}

Status ThreadPoolBase::SetCapacity(int desired_capacity) {
  ProtectAgainstFork();
  ARROW_ASSIGN_OR_RAISE(auto should_notify, control_->SetCapacity(desired_capacity));
  if (should_notify) {
    // Excess threads are running, wake some up to withdraw themselves from the pool
    WakeupWorkersToCheckShutdown();
  }
  return Status::OK();
}

int ThreadPoolBase::GetCapacity() {
  ProtectAgainstFork();
  return control_->desired_capacity_;
}

Status ThreadPoolBase::Shutdown(bool wait) {
  ProtectAgainstFork();
  RETURN_NOT_OK(control_->BeginShutdown(wait));
  WakeupWorkersToCheckShutdown();
  return control_->WaitForShutdownComplete();
}

Status ThreadPoolBase::SpawnReal(TaskHints hints, FnOnce<void()> task,
                                 StopToken stop_token, StopCallback&& stop_callback) {
  {
    ProtectAgainstFork();
    // Update statistics and potentially launch a new thread if we have more work than
    // active threads
    RETURN_NOT_OK(control_->RecordTaskAdded());
    Task task_wrapper{std::move(task), std::move(stop_token), std::move(stop_callback)};
    DoSubmitTask(std::move(hints), std::move(task_wrapper));
  }
  return Status::OK();
}

Result<std::shared_ptr<ThreadPool>> MakeSimpleThreadPool(int threads) {
  auto pool = std::shared_ptr<ThreadPool>(new SimpleThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  return pool;
}

Result<std::shared_ptr<ThreadPool>> MakeEternalSimpleThreadPool(int threads) {
  auto pool = std::shared_ptr<ThreadPool>(new SimpleThreadPool(/*eternal=*/true));
  RETURN_NOT_OK(pool->SetCapacity(threads));
  return pool;
}

SimpleThreadPool::~SimpleThreadPool() { MaybeShutdownOnDestroy(); }

SimpleThreadPool::SimpleThreadPool(bool eternal)
    : ThreadPoolBase(eternal), task_queue_(std::make_shared<SimpleTaskQueue>()) {}

void SimpleThreadPool::WakeupWorkersToCheckShutdown() {
  task_queue_->WakeupForShutdown();
}

void SimpleThreadPool::ResetAfterFork() {
  ThreadPoolBase::ResetAfterFork();
  // Might as well clean up what we can but the dead threads will have a strong ref to
  // this so it will leak
  task_queue_->Clear();
  // Old task_queue will be leaked (contains mutexes)
  task_queue_ = std::make_shared<SimpleTaskQueue>();
}

void SimpleThreadPool::DoSubmitTask(TaskHints hints, Task task) {
  task_queue_->PushTask(std::move(hints), std::move(task));
}

util::optional<Task> SimpleThreadPool::PopTask() { return task_queue_->PopTask(); }

std::thread SimpleThreadPool::LaunchWorker(std::shared_ptr<WorkerControl> control,
                                           ThreadIt thread_it) {
  auto task_queue = task_queue_;
  std::thread thread(
      [=] { SimpleThreadPool::WorkerLoop(control, task_queue, thread_it); });
  return thread;
}

// ----------------------------------------------------------------------
// Global thread pool

static int ParseOMPEnvVar(const char* name) {
  // OMP_NUM_THREADS is a comma-separated list of positive integers.
  // We are only interested in the first (top-level) number.
  auto result = GetEnvVar(name);
  if (!result.ok()) {
    return 0;
  }
  auto str = *std::move(result);
  auto first_comma = str.find_first_of(',');
  if (first_comma != std::string::npos) {
    str = str.substr(0, first_comma);
  }
  try {
    return std::max(0, std::stoi(str));
  } catch (...) {
    return 0;
  }
}

int ThreadPool::DefaultCapacity() {
  int capacity, limit;
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
std::shared_ptr<ThreadPool> MakeCpuThreadPool() {
  auto maybe_pool = MakeEternalSimpleThreadPool(ThreadPool::DefaultCapacity());
  if (!maybe_pool.ok()) {
    maybe_pool.status().Abort("Failed to create global CPU thread pool");
  }
  return *std::move(maybe_pool);
}

ThreadPool* GetCpuThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = MakeCpuThreadPool();
  return singleton.get();
}

}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
