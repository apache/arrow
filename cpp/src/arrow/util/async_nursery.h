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

#ifndef ARROW_ASYNC_NURSERY_H
#define ARROW_ASYNC_NURSERY_H

#include <list>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/future.h"
#include "arrow/util/mutex.h"

namespace arrow {
namespace util {

class OwnedAsyncCloseable;

/// An object which should be asynchronously closed before it is destroyed
///
/// Any AsyncCloseable must be kept alive until its parent is destroyed (this is a given
/// if the parent is a nursery).  For shorter lived tasks/objects consider
/// OwnedAsyncCloseable adding a dependent task.
class AsyncCloseable {
 public:
  /// \brief Construct an AsyncCloseable as a child of `parent`
  explicit AsyncCloseable(AsyncCloseable* parent);
  virtual ~AsyncCloseable();

  /// Returns a future that is completed when this object is finished closing
  Future<> OnClosed();

 protected:
  /// Closes the AsyncCloseable
  /// This will first call DoClose and then simultaneously close all children and
  /// tasks_
  virtual Future<> Close();
  /// Subclasses should override this and perform any cleanup that is not captured by
  /// tasks_.  Once the future returned by this method finishes then this object is
  /// eligible for destruction and any reference to `this` may be invalid
  virtual Future<> DoClose() = 0;

  /// This method is called by subclasses to add tasks which must complete before the
  /// object can be safely deleted
  void AddDependentTask(Future<> task);
  /// This method can be called by subclasses for error checking purposes.  It will
  /// return an invalid status if this object has started closing
  Status CheckClosed() const;
  /// This can be used for sanity checking that a callback is not run after close has
  /// been finished.  It will assert if the object has been fully closed (and `this`
  /// references are unsafe)
  void AssertNotCloseComplete() const;

 private:
  Future<> CloseChildren();

  std::list<AsyncCloseable*> children_;
  std::list<AsyncCloseable*>::iterator self_itr_;
  std::list<std::shared_ptr<AsyncCloseable>> owned_children_;
  std::vector<Future<>> tasks_;
  Future<> on_closed_;
  std::atomic<bool> closed_{false};
  std::atomic<bool> close_complete_{false};
  util::Mutex mutex_;

  friend OwnedAsyncCloseable;
};

/// An override of AsyncCloseable which is eligible for eviction.  Instances must be
/// created as shared pointers as this utilizes enable_shared_from_this.
class OwnedAsyncCloseable : public AsyncCloseable,
                            public std::enable_shared_from_this<OwnedAsyncCloseable> {
 public:
  explicit OwnedAsyncCloseable(AsyncCloseable* parent);

  void Init();
  /// \brief Marks this instance as eligible for removal.  This will start the close
  /// process and, when it is finished, the instance will be deleted.
  void Evict();

 protected:
  AsyncCloseable* parent_;
  std::list<std::shared_ptr<AsyncCloseable>>::iterator owned_self_itr_;
};

/// Helper base class which allows classes that implement the pimpl pattern to
/// participate in a nursery.  This is only needed if the type is going to be
/// used as a nursery root.
class NurseryPimpl {
 public:
  virtual ~NurseryPimpl();
  /// Transfers ownership of the impl to the nursery
  virtual std::shared_ptr<AsyncCloseable> TransferOwnership() = 0;
};

class Nursery : private AsyncCloseable {
 public:
  template <typename T>
  std::shared_ptr<T> AddRoot(std::function<std::shared_ptr<T>(AsyncCloseable*)> factory) {
    std::shared_ptr<T> root = factory(this);
    roots_.push_back(std::static_pointer_cast<AsyncCloseable>(root));
    return root;
  }

  template <typename T>
  std::shared_ptr<T> AddPimplRoot(
      std::function<std::shared_ptr<T>(AsyncCloseable*)> factory) {
    std::shared_ptr<T> pimpl = factory(this);
    roots_.push_back(pimpl->TransferOwnership());
    return pimpl;
  }

  /// Runs `task` within a nursery.  This method will not return until
  /// all roots added by the task have been closed and destroyed.
  static Status RunInNursery(std::function<void(Nursery*)> task);
  // FIXME template hackery to callable that can return void/Status/Result
  static Status RunInNurserySt(std::function<Status(Nursery*)> task);

 protected:
  Future<> DoClose() override;

  Nursery();

  std::vector<std::shared_ptr<AsyncCloseable>> roots_;
};

}  // namespace util
}  // namespace arrow

#endif  // ARROW_ASYNC_NURSERY_H
