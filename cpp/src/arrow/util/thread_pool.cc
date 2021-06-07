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

namespace arrow {
namespace internal {

Executor::~Executor() = default;

struct SerialExecutor::State {
  std::deque<ThreadPool::Task> task_queue;
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
    state->task_queue.push_back(ThreadPool::Task{std::move(task), std::move(stop_token),
                                                 std::move(stop_callback)});
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
      ThreadPool::Task task = std::move(state_->task_queue.front());
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

Thread::~Thread() = default;

struct ThreadPool::Control {
  std::mutex mx;
  // Condition variable that workers wait on when there is no work to do.  This should be
  // signalled whenever new work arrives or when threads need to shut down
  std::condition_variable cv_idle_workers;
  // Condition variable that the thread pool waits on when it is waiting for all worker
  // threads to finish while shutting down
  std::condition_variable cv_shutdown;
};

void ThreadPool::ResetAfterFork() {
  for (auto worker : workers_) {
    worker->ResetAfterFork();
  }
  workers_.clear();
  finished_workers_.clear();
  num_tasks_running_.store(0);
  total_tasks_.store(0);
  max_tasks_.store(0);
  desired_capacity_ = 0;
  // We need to reinitialize the control block because any threads holding a mutex when
  // the fork happened will never release those mutexes (this is true of all other mutexes
  // too.  See ARROW-12879 for follow-up)
  //
  // The proper way to do this is to use pthread_atfork to obtain the mutex before
  // forking.  Improvements welcome, this approach leaks memory because we do not delete
  // the old instance.  We cannot delete the old instance because mutexes cannot be delete
  // while locked (and they will never be unlocked).
  control_ = new Control();
}

void ThreadPool::RecordTaskSubmitted() {
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

void ThreadPool::RecordFinishedTask() {
  num_tasks_running_.fetch_sub(1, std::memory_order_relaxed);
}

uint64_t ThreadPool::NumTasksRunningOrQueued() const {
  return num_tasks_running_.load(std::memory_order_acquire);
}
uint64_t ThreadPool::MaxTasksQueued() const {
  return max_tasks_.load(std::memory_order_relaxed);
}
uint64_t ThreadPool::TotalTasksQueued() const {
  // This may lag behind the actual value a bit
  return total_tasks_.load(std::memory_order_relaxed);
}

// The worker loop must be capable of running after all other references to `thread_pool`
// have been lost so we capture a shared_ptr
void SimpleThreadPool::WorkerLoop(std::shared_ptr<SimpleThreadPool> thread_pool,
                                  ThreadIt self) {
  // Inform the caller we started
  thread_pool->WaitForReady();
  // thread_it is our reference to the thread object's position in the worker list, needed
  // for notifying the pool when finished.
  DCHECK((*self)->IsCurrentThread());

  while (true) {
    // By the time this thread is started, some tasks may have been pushed
    // or shutdown could even have been requested.  So we only wait on the
    // condition variable at the end of the loop.

    bool stopped = false;
    // Execute pending tasks if any, check after each task for a quick shutdown
    while (!thread_pool->ShouldWorkerQuitNow(&self) && !thread_pool->Empty()) {
      util::optional<Task> maybe_task = thread_pool->PopTask();
      if (!maybe_task.has_value()) {
        break;
      }
      auto task = *std::move(maybe_task);
      StopToken* stop_token = &task.stop_token;
      if (!stop_token->IsStopRequested()) {
        std::move(task.callable)();
      } else {
        if (task.stop_callback) {
          std::move(task.stop_callback)(stop_token->Poll());
        }
      }
      thread_pool->RecordFinishedTask();
    }

    // Now either the queue is empty *or* a quick shutdown was requested
    if (stopped || thread_pool->ShouldWorkerQuit(&self)) {
      break;
    }
    thread_pool->WaitForWork();
  }
  DCHECK_GE(thread_pool->NumTasksRunningOrQueued(), 0);
}

ThreadPool::ThreadPool(bool eternal)
    : num_tasks_running_(0),
      total_tasks_(0),
      max_tasks_(0),
      shutdown_on_destroy_(true),
      control_(new Control()) {
#ifndef _WIN32
  pid_ = getpid();
#else
  // On Windows, the ThreadPool destructor may be called after non-main threads
  // have been killed by the OS, and hang in a condition variable.
  // On Unix, we want to avoid leak reports by Valgrind.
  shutdown_on_destroy_ = !eternal;
#endif
}

ThreadPool::~ThreadPool() {
  if (shutdown_on_destroy_) {
    ARROW_UNUSED(Shutdown(false /* wait */));
  }
  delete control_;
}

void ThreadPool::ProtectAgainstFork() {
#ifndef _WIN32
  pid_t current_pid = getpid();
  if (pid_ != current_pid) {
    // Reinitialize internal state in child process after fork()
    // Ideally we would use pthread_at_fork(), but that doesn't allow
    // storing an argument, hence we'd need to maintain a list of all
    // existing ThreadPools.
    int capacity = desired_capacity_;

    ResetAfterFork();

    pid_ = current_pid;

    // Launch worker threads anew
    if (!please_shutdown_) {
      ARROW_UNUSED(SetCapacity(capacity));
    }
  }
#endif
}

Status ThreadPool::SetCapacity(int desired_capacity) {
  ProtectAgainstFork();
  std::lock_guard<std::mutex> lock(control_->mx);
  if (please_shutdown_) {
    return Status::Invalid("operation forbidden during or after shutdown");
  }
  if (desired_capacity <= 0) {
    return Status::Invalid("ThreadPool capacity must be > 0");
  }
  CollectFinishedWorkersUnlocked();

  desired_capacity_ = desired_capacity;
  // See if we need to increase or decrease the number of running threads.  There is a bit
  // of finesse here.  NumTasksRunningOrQueued can change outside the mutex.
  //
  // It's possible we spawn workers when we didn't strictly need to (i.e.
  // NumTasksRunningOrQueued goes down after we check).  However, that should be ok.
  //
  // It should not be possible we fail to spawn tasks when needed.  Any call to increase
  // NumTasksRunningOrQueued will have its own accompanying call to launch workers.
  const int required = GetAdditionalThreadsNeeded();

  if (required > 0) {
    // Some tasks are pending, spawn the number of needed threads immediately
    LaunchWorkersUnlocked(required);
  } else if (required < 0) {
    // Excess threads are running, wake them so that they stop
    control_->cv_idle_workers.notify_all();
  }
  return Status::OK();
}

int ThreadPool::GetCapacity() {
  ProtectAgainstFork();
  return desired_capacity_;
}

int ThreadPool::GetActualCapacity() const { return workers_.size(); }

Status ThreadPool::Shutdown(bool wait) {
  ProtectAgainstFork();
  std::unique_lock<std::mutex> lock(control_->mx);

  if (please_shutdown_) {
    return Status::Invalid("Shutdown() already called");
  }
  please_shutdown_ = true;
  quick_shutdown_ = !wait;
  control_->cv_idle_workers.notify_all();
  control_->cv_shutdown.wait(lock, [this] { return workers_.empty(); });
  DCHECK(workers_.empty());
  CollectFinishedWorkersUnlocked();
  if (!quick_shutdown_) {
    DCHECK_EQ(NumTasksRunningOrQueued(), 0);
  }
  return Status::OK();
}

void ThreadPool::CollectFinishedWorkersUnlocked() {
  for (auto& thread : finished_workers_) {
    // Allow thread to do any neccesary cleanup (e.g. OS-level join)
    thread->Join();
  }
  finished_workers_.clear();
}

bool ThreadPool::ShouldWorkerQuit(ThreadIt* thread_it) {
  if (*thread_it == workers_.end()) {
    return true;
  }
  // At this point we have run out of work and are off the hot path so it is safe to lock
  std::lock_guard<std::mutex> lock(control_->mx);
  if (please_shutdown_ && Empty()) {
    MarkThreadFinishedUnlocked(thread_it);
    return true;
  }
  return false;
}

bool ThreadPool::ShouldWorkerQuitNow(ThreadIt* thread_it) {
  if (*thread_it == workers_.end()) {
    return true;
  }
  // This method is called often but returns true very rarely so we only grab
  // the mutex if it looks like we are going to be quitting.  As a result we need
  // to do a bit of a double-check
  if (quick_shutdown_ || desired_capacity_ < GetActualCapacity()) {
    std::lock_guard<std::mutex> lock(control_->mx);
    if (quick_shutdown_ || desired_capacity_ < GetActualCapacity()) {
      MarkThreadFinishedUnlocked(thread_it);
      return true;
    } else {
      return false;
    }
  }
  return false;
}

void ThreadPool::MarkThreadFinishedUnlocked(ThreadIt* thread_it) {
  finished_workers_.push_back(std::move(**thread_it));
  workers_.erase(*thread_it);
  *thread_it = workers_.end();
  control_->cv_shutdown.notify_one();
}

void ThreadPool::LaunchWorkersUnlocked(int threads) {
  for (int i = 0; i < threads; i++) {
    workers_.emplace_back();
    DCHECK_LE(static_cast<int>(workers_.size()), desired_capacity_);
    auto it = --(workers_.end());
    *it = LaunchWorker(it);
  }
}

int ThreadPool::GetAdditionalThreadsNeeded() const {
  int unallocated_tasks =
      static_cast<int>(NumTasksRunningOrQueued()) - GetActualCapacity();
  int unused_capacity = desired_capacity_ - GetActualCapacity();
  return std::min(unallocated_tasks, unused_capacity);
}

Status ThreadPool::SpawnReal(TaskHints hints, FnOnce<void()> task, StopToken stop_token,
                             StopCallback&& stop_callback) {
  {
    ProtectAgainstFork();
    if (please_shutdown_) {
      return Status::Invalid("operation forbidden during or after shutdown");
    }
    if (!finished_workers_.empty()) {
      std::lock_guard<std::mutex> lock(control_->mx);
      // Maybe someone snuck in and cleared this out while we were grabbing the lock
      // but it should be harmless to do it again.
      CollectFinishedWorkersUnlocked();
    }
    RecordTaskSubmitted();
    if (GetAdditionalThreadsNeeded() > 0) {
      std::lock_guard<std::mutex> lock(control_->mx);
      // We avoided locking on the hot path unless we needed to so we have to double
      // check to ensure we still have spare capacity
      if (!please_shutdown_ && GetAdditionalThreadsNeeded() > 0) {
        // We can still spin up more workers so spin up a new worker
        LaunchWorkersUnlocked(/*threads=*/1);
      }
    }
    Task task_wrapper{std::move(task), std::move(stop_token), std::move(stop_callback)};
    DoSubmitTask(std::move(hints), std::move(task_wrapper));
  }
  return Status::OK();
}

void ThreadPool::WaitForReady() {
  // Grab the lock (at least briefly) when the thread starts, to make sure the launching
  // task is finished before we start work.
  std::lock_guard<std::mutex> lock(control_->mx);
}

void ThreadPool::NotifyIdleWorker() { control_->cv_idle_workers.notify_one(); }

void ThreadPool::WaitForWork() {
  std::unique_lock<std::mutex> lock(control_->mx);
  control_->cv_idle_workers.wait(lock, [this] { return !Empty() || please_shutdown_; });
}

Result<std::shared_ptr<ThreadPool>> SimpleThreadPool::Make(int threads) {
  auto pool = std::shared_ptr<ThreadPool>(new SimpleThreadPool());
  RETURN_NOT_OK(pool->SetCapacity(threads));
  return pool;
}

Result<std::shared_ptr<ThreadPool>> SimpleThreadPool::MakeEternal(int threads) {
  auto pool = std::shared_ptr<ThreadPool>(new SimpleThreadPool(/*eternal=*/true));
  RETURN_NOT_OK(pool->SetCapacity(threads));
  return pool;
}

SimpleThreadPool::~SimpleThreadPool() = default;
SimpleThreadPool::SimpleThreadPool(bool eternal) : ThreadPool(eternal), task_count_(0) {}

void SimpleThreadPool::ResetAfterFork() {
  ThreadPool::ResetAfterFork();
  pending_tasks_.clear();
}

void SimpleThreadPool::DoSubmitTask(TaskHints hints, Task task) {
  std::unique_lock<std::mutex> lock(control_->mx);
  pending_tasks_.push_back(std::move(task));
  task_count_.fetch_add(1, std::memory_order_release);
  lock.unlock();
  NotifyIdleWorker();
}

util::optional<ThreadPool::Task> SimpleThreadPool::PopTask() {
  std::lock_guard<std::mutex> lock(control_->mx);
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

bool SimpleThreadPool::Empty() {
  return task_count_.load(std::memory_order_acquire) == 0;
}

struct StlThread : public Thread {
  explicit StlThread(std::thread* thread) : thread(thread) {}
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

std::shared_ptr<Thread> SimpleThreadPool::LaunchWorker(ThreadIt thread_it) {
  auto self = shared_from_this();
  std::thread* thread = new std::thread(
      [self, thread_it] { SimpleThreadPool::WorkerLoop(self, thread_it); });
  return std::make_shared<StlThread>(thread);
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
std::shared_ptr<ThreadPool> ThreadPool::MakeCpuThreadPool() {
  auto maybe_pool = SimpleThreadPool::MakeEternal(ThreadPool::DefaultCapacity());
  if (!maybe_pool.ok()) {
    maybe_pool.status().Abort("Failed to create global CPU thread pool");
  }
  return *std::move(maybe_pool);
}

ThreadPool* GetCpuThreadPool() {
  static std::shared_ptr<ThreadPool> singleton = ThreadPool::MakeCpuThreadPool();
  return singleton.get();
}

}  // namespace internal

int GetCpuThreadPoolCapacity() { return internal::GetCpuThreadPool()->GetCapacity(); }

Status SetCpuThreadPoolCapacity(int threads) {
  return internal::GetCpuThreadPool()->SetCapacity(threads);
}

}  // namespace arrow
