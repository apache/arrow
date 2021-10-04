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

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include "arrow/util/mutex.h"

namespace arrow {
namespace util {

/// Custom deleter for AsyncDestroyable objects
template <typename T>
struct DestroyingDeleter {
  void operator()(T* p) {
    if (p) {
      p->Destroy();
    }
  }
};

/// An object which should be asynchronously closed before it is destroyed
///
/// Classes can extend this to ensure that the close method is called and completed
/// before the instance is deleted.  This provides smart_ptr / delete semantics for
/// objects with an asynchronous destructor.
///
/// Classes which extend this must be constructed using MakeSharedAsync or MakeUniqueAsync
class ARROW_EXPORT AsyncDestroyable {
 public:
  AsyncDestroyable();
  virtual ~AsyncDestroyable();

  /// A future which will complete when the AsyncDestroyable has finished and is ready
  /// to be deleted.
  ///
  /// This can be used to ensure all work done by this object has been completed before
  /// proceeding.
  Future<> on_closed() { return on_closed_; }

 protected:
  /// Subclasses should override this and perform any cleanup.  Once the future returned
  /// by this method finishes then this object is eligible for destruction and any
  /// reference to `this` will be invalid
  virtual Future<> DoDestroy() = 0;

 private:
  void Destroy();

  Future<> on_closed_;
#ifndef NDEBUG
  bool constructed_correctly_ = false;
#endif

  template <typename T>
  friend struct DestroyingDeleter;
  template <typename T, typename... Args>
  friend std::shared_ptr<T> MakeSharedAsync(Args&&... args);
  template <typename T, typename... Args>
  friend std::unique_ptr<T, DestroyingDeleter<T>> MakeUniqueAsync(Args&&... args);
};

template <typename T, typename... Args>
std::shared_ptr<T> MakeSharedAsync(Args&&... args) {
  static_assert(std::is_base_of<AsyncDestroyable, T>::value,
                "Nursery::MakeSharedCloseable only works with AsyncDestroyable types");
  std::shared_ptr<T> ptr(new T(std::forward<Args&&>(args)...), DestroyingDeleter<T>());
#ifndef NDEBUG
  ptr->constructed_correctly_ = true;
#endif
  return ptr;
}

template <typename T, typename... Args>
std::unique_ptr<T, DestroyingDeleter<T>> MakeUniqueAsync(Args&&... args) {
  static_assert(std::is_base_of<AsyncDestroyable, T>::value,
                "Nursery::MakeUniqueCloseable only works with AsyncDestroyable types");
  std::unique_ptr<T, DestroyingDeleter<T>> ptr(new T(std::forward<Args>(args)...),
                                               DestroyingDeleter<T>());
#ifndef NDEBUG
  ptr->constructed_correctly_ = true;
#endif
  return ptr;
}

/// A utility which keeps track of a collection of asynchronous tasks
///
/// This can be used to provide structured concurrency for asynchronous development.
/// A task group created at a high level can be distributed amongst low level components
/// which register work to be completed.  The high level job can then wait for all work
/// to be completed before cleaning up.
class ARROW_EXPORT AsyncTaskGroup {
 public:
  /// Add a task to be tracked by this task group
  ///
  /// If a previous task has failed then adding a task will fail
  ///
  /// If WaitForTasksToFinish has been called and the returned future has been marked
  /// completed then adding a task will fail.
  Status AddTask(const Future<>& task);
  /// A future that will be completed when all running tasks are finished.
  ///
  /// It is allowed for tasks to be added after this call provided the future has not yet
  /// completed.  This should be safe as long as the tasks being added are added as part
  /// of a task that is tracked.  As soon as the count of running tasks reaches 0 this
  /// future will be marked complete.
  ///
  /// Any attempt to add a task after the returned future has completed will fail.
  Future<> WaitForTasksToFinish();

 private:
  bool finished_adding_ = false;
  int running_tasks_ = 0;
  Status err_;
  Future<> all_tasks_done_ = Future<>::Make();
  util::Mutex mutex_;
};

}  // namespace util
}  // namespace arrow
