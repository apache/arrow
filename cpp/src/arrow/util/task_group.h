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

#include <functional>
#include <memory>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

/// \brief A group of related tasks
///
/// A TaskGroup executes tasks with the signature `Status()`.
/// Execution can be serial or parallel, depending on the TaskGroup
/// implementation.  When Finish() returns, it is guaranteed that all
/// tasks have finished, or at least one has errored.
///
/// Once an error has occurred any tasks that are submitted to the task group
/// will not run.  The call to Append will simply return without scheduling the
/// task.
///
/// If the task group is parallel it is possible that multiple tasks could be
/// running at the same time and one of those tasks fails.  This will put the
/// task group in a failure state (so additional tasks cannot be run) however
/// it will not interrupt running tasks.  Finish and FinishAsync will not complete
/// until all running tasks have finished, even if one task fails.
///
/// If you wish to nest task groups then you can do so by obtaining a Future
/// from your child task groups (using FinishAsync) and adding this future
/// as a task to your parent task group.
///
/// Once a task group has finished new tasks should not be added to it.  This
/// will lead to a runtime error.  If you need to start a new batch of work then
/// you should create a new task group.  Keep in mind that you can nest task groups.
class ARROW_EXPORT TaskGroup : public std::enable_shared_from_this<TaskGroup> {
 public:
  /// Add a Status-returning function to execute.  Execution order is
  /// undefined.  The function may be executed immediately or later.
  template <typename Function>
  void Append(Function&& func) {
    return AppendReal(std::forward<Function>(func));
  }

  /// Adds a future to the list of tasks to wait for.
  template <typename T>
  void Append(const Future<T>& future) {
    return AppendReal(Future<>(future));
  }

  /// Wait for execution of all tasks (and subgroups) to be finished,
  /// or for at least one task (or subgroup) to error out.
  /// The returned Status propagates the error status of the first failing
  /// task (or subgroup).
  virtual Status Finish() = 0;

  /// Returns a future that will complete when all tasks in the task group
  /// are finished.
  ///
  /// If any task fails then this future will be marked failed with the failing
  /// status.  However, the future will not be marked complete until all other
  /// running tasks have been completed.  Tasks that have not started running will
  /// not start once the task group enters an error state so this possibility of
  /// "other tasks" only exists with a threaded task group.
  virtual Future<> FinishAsync() = 0;

  /// The current aggregate error Status.  Non-blocking, useful for stopping early.
  virtual Status current_status() = 0;

  /// Whether some tasks have already failed.  Non-blocking , useful for stopping early.
  virtual bool ok() = 0;

  /// How many tasks can typically be executed in parallel.
  /// This is only a hint, useful for testing or debugging.
  virtual int parallelism() = 0;

  static std::shared_ptr<TaskGroup> MakeSerial();
  static std::shared_ptr<TaskGroup> MakeThreaded(internal::Executor*);

  virtual ~TaskGroup() = default;

 protected:
  TaskGroup() = default;
  ARROW_DISALLOW_COPY_AND_ASSIGN(TaskGroup);

  virtual void AppendReal(std::function<Status()> task) = 0;
  virtual void AppendReal(Future<> task) = 0;
};

}  // namespace internal
}  // namespace arrow
