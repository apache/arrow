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

#include <queue>

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
  Status AddTask(std::function<Result<Future<>>()> task);
  /// Add a task that has already been started
  Status AddTask(const Future<>& task);
  /// Signal that top level tasks are done being added
  ///
  /// It is allowed for tasks to be added after this call provided the future has not yet
  /// completed.  This should be safe as long as the tasks being added are added as part
  /// of a task that is tracked.  As soon as the count of running tasks reaches 0 this
  /// future will be marked complete.
  ///
  /// Any attempt to add a task after the returned future has completed will fail.
  ///
  /// The returned future that will finish when all running tasks have finished.
  Future<> End();
  /// A future that will be finished after End is called and all tasks have completed
  ///
  /// This is the same future that is returned by End() but calling this method does
  /// not indicate that top level tasks are done being added.  End() must still be called
  /// at some point or the future returned will never finish.
  ///
  /// This is a utility method for workflows where the finish future needs to be
  /// referenced before all top level tasks have been queued.
  Future<> OnFinished() const;

 private:
  Status AddTaskUnlocked(const Future<>& task, util::Mutex::Guard guard);

  bool finished_adding_ = false;
  int running_tasks_ = 0;
  Status err_;
  Future<> all_tasks_done_ = Future<>::Make();
  util::Mutex mutex_;
};

/// A task group which serializes asynchronous tasks in a push-based workflow
///
/// Tasks will be executed in the order they are added
///
/// This will buffer results in an unlimited fashion so it should be combined
/// with some kind of backpressure
class ARROW_EXPORT SerializedAsyncTaskGroup {
 public:
  SerializedAsyncTaskGroup();
  /// Push an item into the serializer and (eventually) into the consumer
  ///
  /// The item will not be delivered to the consumer until all previous items have been
  /// consumed.
  ///
  /// If the consumer returns an error then this serializer will go into an error state
  /// and all subsequent pushes will fail with that error.  Pushes that have been queued
  /// but not delivered will be silently dropped.
  ///
  /// \return True if the item was pushed immediately to the consumer, false if it was
  /// queued
  Status AddTask(std::function<Result<Future<>>()> task);

  /// Signal that all top level tasks have been added
  ///
  /// The returned future that will finish when all tasks have been consumed.
  Future<> End();

  /// Abort a task group
  ///
  /// Tasks that have not been started will be discarded
  ///
  /// The returned future will finish when all running tasks have finished.
  Future<> Abort(Status err);

  /// A future that finishes when all queued items have been delivered.
  ///
  /// This will return the same future returned by End but will not signal
  /// that all tasks have been finished.  End must be called at some point in order for
  /// this future to finish.
  Future<> OnFinished() const { return on_finished_; }

 private:
  void ConsumeAsMuchAsPossibleUnlocked(util::Mutex::Guard&& guard);
  Future<> EndUnlocked(util::Mutex::Guard&& guard);
  bool TryDrainUnlocked();

  Future<> on_finished_;
  std::queue<std::function<Result<Future<>>()>> tasks_;
  util::Mutex mutex_;
  bool ended_ = false;
  Status err_;
  Future<> processing_;
};

class ARROW_EXPORT AsyncToggle {
 public:
  /// Get a future that will complete when the toggle next becomes open
  ///
  /// If the toggle is open this returns immediately
  /// If the toggle is closed this future will be unfinished until the next call to Open
  Future<> WhenOpen();
  /// \brief Close the toggle
  ///
  /// After this call any call to WhenOpen will be delayed until the next open
  void Close();
  /// \brief Open the toggle
  ///
  /// Note: This call may complete a future, triggering any callbacks, and generally
  /// should not be done while holding any locks.
  ///
  /// Note: If Open is called from multiple threads it could lead to a situation where
  /// callbacks from the second open finish before callbacks on the first open.
  ///
  /// All current waiters will be released to enter, even if another close call
  /// quickly follows
  void Open();

  /// \brief Return true if the toggle is currently open
  bool IsOpen();

 private:
  Future<> when_open_ = Future<>::MakeFinished();
  bool closed_ = false;
  util::Mutex mutex_;
};

/// \brief Options to control backpressure behavior
struct ARROW_EXPORT BackpressureOptions {
  /// \brief Create default options that perform no backpressure
  BackpressureOptions() : toggle(NULLPTR), resume_if_below(0), pause_if_above(0) {}
  /// \brief Create options that will perform backpressure
  ///
  /// \param toggle A toggle to be shared between the producer and consumer
  /// \param resume_if_below The producer should resume producing if the backpressure
  ///                        queue has fewer than resume_if_below items.
  /// \param pause_if_above The producer should pause producing if the backpressure
  ///                       queue has more than pause_if_above items
  BackpressureOptions(std::shared_ptr<util::AsyncToggle> toggle, uint32_t resume_if_below,
                      uint32_t pause_if_above)
      : toggle(std::move(toggle)),
        resume_if_below(resume_if_below),
        pause_if_above(pause_if_above) {}

  static BackpressureOptions Make(uint32_t resume_if_below = 32,
                                  uint32_t pause_if_above = 64);

  static BackpressureOptions NoBackpressure();

  std::shared_ptr<util::AsyncToggle> toggle;
  uint32_t resume_if_below;
  uint32_t pause_if_above;
};

}  // namespace util
}  // namespace arrow
