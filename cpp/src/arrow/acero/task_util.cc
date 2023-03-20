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

#include "arrow/acero/task_util.h"

#include <algorithm>
#include <mutex>

#include "arrow/util/logging.h"

namespace arrow {
namespace acero {

class TaskSchedulerImpl : public TaskScheduler {
 public:
  TaskSchedulerImpl();
  int RegisterTaskGroup(TaskImpl task_impl, TaskGroupContinuationImpl cont_impl) override;
  void RegisterEnd() override;
  Status StartTaskGroup(size_t thread_id, int group_id, int64_t total_num_tasks) override;
  Status ExecuteMore(size_t thread_id, int num_tasks_to_execute,
                     bool execute_all) override;
  Status StartScheduling(size_t thread_id, ScheduleImpl schedule_impl,
                         int num_concurrent_tasks, bool use_sync_execution) override;
  void Abort(AbortContinuationImpl impl) override;

 private:
  // Task group state transitions progress one way.
  // Seeing an old version of the state by a thread is a valid situation.
  //
  enum class TaskGroupState : int {
    NOT_READY,
    READY,
    ALL_TASKS_STARTED,
    ALL_TASKS_FINISHED
  };

  struct TaskGroup {
    TaskGroup(TaskImpl task_impl, TaskGroupContinuationImpl cont_impl)
        : task_impl_(std::move(task_impl)),
          cont_impl_(std::move(cont_impl)),
          state_(TaskGroupState::NOT_READY),
          num_tasks_present_(0) {
      num_tasks_started_.value.store(0);
      num_tasks_finished_.value.store(0);
    }
    TaskGroup(const TaskGroup& src)
        : task_impl_(src.task_impl_),
          cont_impl_(src.cont_impl_),
          state_(TaskGroupState::NOT_READY),
          num_tasks_present_(0) {
      ARROW_DCHECK(src.state_ == TaskGroupState::NOT_READY);
      num_tasks_started_.value.store(0);
      num_tasks_finished_.value.store(0);
    }
    TaskImpl task_impl_;
    TaskGroupContinuationImpl cont_impl_;

    TaskGroupState state_;
    int64_t num_tasks_present_;

    AtomicWithPadding<int64_t> num_tasks_started_;
    AtomicWithPadding<int64_t> num_tasks_finished_;
  };

  std::vector<std::pair<int, int64_t>> PickTasks(int num_tasks, int start_task_group = 0);
  Status ExecuteTask(size_t thread_id, int group_id, int64_t task_id,
                     bool* task_group_finished);
  bool PostExecuteTask(size_t thread_id, int group_id);
  Status OnTaskGroupFinished(size_t thread_id, int group_id,
                             bool* all_task_groups_finished);
  Status ScheduleMore(size_t thread_id, int num_tasks_finished = 0);

  bool use_sync_execution_;
  int num_concurrent_tasks_;
  ScheduleImpl schedule_impl_;
  AbortContinuationImpl abort_cont_impl_;

  std::vector<TaskGroup> task_groups_;
  bool aborted_;
  bool register_finished_;
  std::mutex mutex_;  // Mutex protecting task_groups_ (state_ and num_tasks_present_
                      // fields), aborted_ flag and register_finished_ flag

  AtomicWithPadding<int> num_tasks_to_schedule_;
  // If a task group adds tasks it's possible for a thread inside
  // ScheduleMore to miss this fact.  This serves as a flag to
  // notify the scheduling thread that it might need to make
  // another pass through the scheduler
  AtomicWithPadding<bool> tasks_added_recently_;
};

TaskSchedulerImpl::TaskSchedulerImpl()
    : use_sync_execution_(false),
      num_concurrent_tasks_(0),
      aborted_(false),
      register_finished_(false) {
  num_tasks_to_schedule_.value.store(0);
  tasks_added_recently_.value.store(false);
}

int TaskSchedulerImpl::RegisterTaskGroup(TaskImpl task_impl,
                                         TaskGroupContinuationImpl cont_impl) {
  int result = static_cast<int>(task_groups_.size());
  task_groups_.emplace_back(std::move(task_impl), std::move(cont_impl));
  return result;
}

void TaskSchedulerImpl::RegisterEnd() {
  std::lock_guard<std::mutex> lock(mutex_);

  register_finished_ = true;
}

Status TaskSchedulerImpl::StartTaskGroup(size_t thread_id, int group_id,
                                         int64_t total_num_tasks) {
  ARROW_DCHECK(group_id >= 0 && group_id < static_cast<int>(task_groups_.size()));
  TaskGroup& task_group = task_groups_[group_id];

  bool aborted = false;
  bool all_tasks_finished = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    aborted = aborted_;

    if (task_group.state_ == TaskGroupState::NOT_READY) {
      task_group.num_tasks_present_ = total_num_tasks;
      if (total_num_tasks == 0) {
        task_group.state_ = TaskGroupState::ALL_TASKS_FINISHED;
        all_tasks_finished = true;
      }
      task_group.state_ = TaskGroupState::READY;
    }
  }

  if (!aborted && all_tasks_finished) {
    bool all_task_groups_finished = false;
    RETURN_NOT_OK(OnTaskGroupFinished(thread_id, group_id, &all_task_groups_finished));
    if (all_task_groups_finished) {
      return Status::OK();
    }
  }

  if (!aborted) {
    tasks_added_recently_.value.store(true);
    return ScheduleMore(thread_id);
  } else {
    return Status::Cancelled("Scheduler cancelled");
  }
}

std::vector<std::pair<int, int64_t>> TaskSchedulerImpl::PickTasks(int num_tasks,
                                                                  int start_task_group) {
  std::vector<std::pair<int, int64_t>> result;
  for (size_t i = 0; i < task_groups_.size(); ++i) {
    int task_group_id = static_cast<int>((start_task_group + i) % (task_groups_.size()));
    TaskGroup& task_group = task_groups_[task_group_id];

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (task_group.state_ != TaskGroupState::READY) {
        continue;
      }
    }

    int num_tasks_remaining = num_tasks - static_cast<int>(result.size());
    int64_t start_task =
        task_group.num_tasks_started_.value.fetch_add(num_tasks_remaining);
    if (start_task >= task_group.num_tasks_present_) {
      continue;
    }

    int num_tasks_current_group = num_tasks_remaining;
    if (start_task + num_tasks_current_group >= task_group.num_tasks_present_) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (task_group.state_ == TaskGroupState::READY) {
          task_group.state_ = TaskGroupState::ALL_TASKS_STARTED;
        }
      }
      num_tasks_current_group =
          static_cast<int>(task_group.num_tasks_present_ - start_task);
    }

    for (int64_t task_id = start_task; task_id < start_task + num_tasks_current_group;
         ++task_id) {
      result.push_back(std::make_pair(task_group_id, task_id));
    }

    if (static_cast<int>(result.size()) == num_tasks) {
      break;
    }
  }

  return result;
}

Status TaskSchedulerImpl::ExecuteTask(size_t thread_id, int group_id, int64_t task_id,
                                      bool* task_group_finished) {
  if (!aborted_) {
    RETURN_NOT_OK(task_groups_[group_id].task_impl_(thread_id, task_id));
  }
  *task_group_finished = PostExecuteTask(thread_id, group_id);
  return Status::OK();
}

bool TaskSchedulerImpl::PostExecuteTask(size_t thread_id, int group_id) {
  int64_t total = task_groups_[group_id].num_tasks_present_;
  int64_t prev_finished = task_groups_[group_id].num_tasks_finished_.value.fetch_add(1);
  bool all_tasks_finished = (prev_finished + 1 == total);
  return all_tasks_finished;
}

Status TaskSchedulerImpl::OnTaskGroupFinished(size_t thread_id, int group_id,
                                              bool* all_task_groups_finished) {
  bool aborted = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    aborted = aborted_;
    TaskGroup& task_group = task_groups_[group_id];
    task_group.state_ = TaskGroupState::ALL_TASKS_FINISHED;
    *all_task_groups_finished = true;
    for (size_t i = 0; i < task_groups_.size(); ++i) {
      if (task_groups_[i].state_ != TaskGroupState::ALL_TASKS_FINISHED) {
        *all_task_groups_finished = false;
        break;
      }
    }
  }

  if (aborted && *all_task_groups_finished) {
    abort_cont_impl_();
    return Status::Cancelled("Scheduler cancelled");
  }
  if (!aborted) {
    RETURN_NOT_OK(task_groups_[group_id].cont_impl_(thread_id));
  }
  return Status::OK();
}

Status TaskSchedulerImpl::ExecuteMore(size_t thread_id, int num_tasks_to_execute,
                                      bool execute_all) {
  num_tasks_to_execute = std::max(1, num_tasks_to_execute);

  int last_id = 0;
  for (;;) {
    if (aborted_) {
      return Status::Cancelled("Scheduler cancelled");
    }

    // Pick next bundle of tasks
    const auto& tasks = PickTasks(num_tasks_to_execute, last_id);
    if (tasks.empty()) {
      break;
    }
    last_id = tasks.back().first;

    // Execute picked tasks immediately
    for (size_t i = 0; i < tasks.size(); ++i) {
      int group_id = tasks[i].first;
      int64_t task_id = tasks[i].second;
      bool task_group_finished = false;
      Status status = ExecuteTask(thread_id, group_id, task_id, &task_group_finished);
      if (!status.ok()) {
        // Mark the remaining picked tasks as finished
        for (size_t j = i + 1; j < tasks.size(); ++j) {
          if (PostExecuteTask(thread_id, tasks[j].first)) {
            bool all_task_groups_finished = false;
            RETURN_NOT_OK(
                OnTaskGroupFinished(thread_id, group_id, &all_task_groups_finished));
            if (all_task_groups_finished) {
              return Status::OK();
            }
          }
        }
        return status;
      } else {
        if (task_group_finished) {
          bool all_task_groups_finished = false;
          RETURN_NOT_OK(
              OnTaskGroupFinished(thread_id, group_id, &all_task_groups_finished));
          if (all_task_groups_finished) {
            return Status::OK();
          }
        }
      }
    }

    if (!execute_all) {
      num_tasks_to_execute -= static_cast<int>(tasks.size());
      if (num_tasks_to_execute == 0) {
        break;
      }
    }
  }

  return Status::OK();
}

Status TaskSchedulerImpl::StartScheduling(size_t thread_id, ScheduleImpl schedule_impl,
                                          int num_concurrent_tasks,
                                          bool use_sync_execution) {
  schedule_impl_ = std::move(schedule_impl);
  use_sync_execution_ = use_sync_execution;
  num_concurrent_tasks_ = num_concurrent_tasks;
  num_tasks_to_schedule_.value += num_concurrent_tasks;
  return ScheduleMore(thread_id);
}

Status TaskSchedulerImpl::ScheduleMore(size_t thread_id, int num_tasks_finished) {
  if (aborted_) {
    return Status::Cancelled("Scheduler cancelled");
  }

  ARROW_DCHECK(register_finished_);

  if (use_sync_execution_) {
    return ExecuteMore(thread_id, 1, true);
  }

  int num_new_tasks = num_tasks_finished;
  for (;;) {
    int expected = num_tasks_to_schedule_.value.load();
    if (num_tasks_to_schedule_.value.compare_exchange_strong(expected, 0)) {
      num_new_tasks += expected;
      break;
    }
  }
  if (num_new_tasks == 0) {
    return Status::OK();
  }

  const auto& tasks = PickTasks(num_new_tasks);
  if (static_cast<int>(tasks.size()) < num_new_tasks) {
    num_tasks_to_schedule_.value += num_new_tasks - static_cast<int>(tasks.size());
  }

  bool expected_might_have_missed_tasks = true;
  if (tasks_added_recently_.value.compare_exchange_strong(
          expected_might_have_missed_tasks, false)) {
    if (tasks.empty()) {
      // num_tasks_finished has already been added to num_tasks_to_schedule so
      // pass 0 here.
      return ScheduleMore(thread_id);
    }
  }

  for (size_t i = 0; i < tasks.size(); ++i) {
    int group_id = tasks[i].first;
    int64_t task_id = tasks[i].second;
    RETURN_NOT_OK(schedule_impl_([this, group_id, task_id](size_t thread_id) -> Status {
      RETURN_NOT_OK(ScheduleMore(thread_id, 1));

      bool task_group_finished = false;
      RETURN_NOT_OK(ExecuteTask(thread_id, group_id, task_id, &task_group_finished));

      if (task_group_finished) {
        bool all_task_groups_finished = false;
        return OnTaskGroupFinished(thread_id, group_id, &all_task_groups_finished);
      }

      return Status::OK();
    }));
  }

  return Status::OK();
}

void TaskSchedulerImpl::Abort(AbortContinuationImpl impl) {
  bool all_finished = true;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    aborted_ = true;
    abort_cont_impl_ = std::move(impl);
    if (register_finished_) {
      for (size_t i = 0; i < task_groups_.size(); ++i) {
        TaskGroup& task_group = task_groups_[i];
        if (task_group.state_ == TaskGroupState::NOT_READY) {
          task_group.state_ = TaskGroupState::ALL_TASKS_FINISHED;
        } else if (task_group.state_ == TaskGroupState::READY) {
          int64_t expected = task_group.num_tasks_started_.value.load();
          for (;;) {
            if (task_group.num_tasks_started_.value.compare_exchange_strong(
                    expected, task_group.num_tasks_present_)) {
              break;
            }
          }
          int64_t before_add = task_group.num_tasks_finished_.value.fetch_add(
              task_group.num_tasks_present_ - expected);
          if (before_add >= expected) {
            task_group.state_ = TaskGroupState::ALL_TASKS_FINISHED;
          } else {
            all_finished = false;
            task_group.state_ = TaskGroupState::ALL_TASKS_STARTED;
          }
        }
      }
    }
  }
  if (all_finished) {
    abort_cont_impl_();
  }
}

std::unique_ptr<TaskScheduler> TaskScheduler::Make() {
  std::unique_ptr<TaskSchedulerImpl> impl{new TaskSchedulerImpl()};
  return std::move(impl);
}

}  // namespace acero
}  // namespace arrow
