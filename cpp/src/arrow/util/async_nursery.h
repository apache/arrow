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

class Nursery;
class AsyncCloseablePimpl;

template <typename T>
struct ARROW_EXPORT DestroyingDeleter {
  void operator()(T* p) { p->Destroy(); }
};

/// An object which should be asynchronously closed before it is destroyed
///
/// Any AsyncCloseable must be kept alive until its parent is destroyed (this is a given
/// if the parent is a nursery).  For shorter lived tasks/objects consider
/// OwnedAsyncCloseable adding a dependent task.
class ARROW_EXPORT AsyncCloseable : public std::enable_shared_from_this<AsyncCloseable> {
 public:
  AsyncCloseable();
  explicit AsyncCloseable(AsyncCloseable* parent);
  virtual ~AsyncCloseable();

  /// Returns a future that is completed when this object is finished closing
  const Future<>& OnClosed();

 protected:
  /// Subclasses should override this and perform any cleanup.  Once the future returned
  /// by this method finishes then this object is eligible for destruction and any
  /// reference to `this` will be invalid
  virtual Future<> DoClose() = 0;

  /// This method is called by subclasses to add tasks which must complete before the
  /// object can be safely deleted
  void AddDependentTask(const Future<>& task);
  /// This method can be called by subclasses for error checking purposes.  It will
  /// return an invalid status if this object has started closing
  Status CheckClosed() const;

  Nursery* nursery_;

 private:
  void SetNursery(Nursery* nursery);
  void Destroy();

  Future<> on_closed_;
  Future<> tasks_finished_;
  std::atomic<bool> closed_{false};
  std::atomic<uint32_t> num_tasks_outstanding_{1};

  friend Nursery;
  template <typename T>
  friend struct DestroyingDeleter;
  friend AsyncCloseablePimpl;
};

class ARROW_EXPORT AsyncCloseablePimpl {
 protected:
  void Init(AsyncCloseable* impl);

 private:
  void SetNursery(Nursery* nursery);
  void Destroy();

  AsyncCloseable* impl_;

  friend Nursery;
  template <typename T>
  friend struct DestroyingDeleter;
};

class ARROW_EXPORT Nursery {
 public:
  // FIXME: Add static_assert that T extends AsyncCloseable for friendlier error message
  template <typename T, typename... Args>
  typename std::enable_if<!std::is_array<T>::value, std::shared_ptr<T>>::type
  MakeSharedCloseable(Args&&... args) {
    num_closeables_created_.fetch_add(1);
    num_tasks_outstanding_.fetch_add(1);
    std::shared_ptr<T> shared_closeable(new T(std::forward<Args&&>(args)...),
                                        DestroyingDeleter<T>());
    shared_closeable->SetNursery(this);
    return shared_closeable;
  }

  template <typename T, typename... Args>
  typename std::enable_if<!std::is_array<T>::value,
                          std::unique_ptr<T, DestroyingDeleter<T>>>::type
  MakeUniqueCloseable(Args&&... args) {
    num_closeables_created_.fetch_add(1);
    num_tasks_outstanding_.fetch_add(1);
    auto unique_closeable = std::unique_ptr<T, DestroyingDeleter<T>>(
        new T(std::forward<Args>(args)...), DestroyingDeleter<T>());
    unique_closeable->SetNursery(this);
    return unique_closeable;
  }

  template <typename T>
  void AddDependentTask(const Future<T>& task) {
    num_tasks_outstanding_.fetch_add(1);
    task.AddCallback([this](const Result<T>& res) { OnTaskFinished(res.status()); });
  }

  /// Runs `task` within a nursery.  This method will not return until
  /// all roots added by the task have been closed and destroyed.
  static Status RunInNursery(std::function<void(Nursery*)> task);
  // FIXME template hackery to callable that can return void/Status/Result
  // to work correctly with PIMPL objects this may just mean making sure the
  // Destroy method works.  Or maybe we want a marker class for PIMPL?
  static Status RunInNurserySt(std::function<Status(Nursery*)> task);

 protected:
  Status WaitForFinish();
  void OnTaskFinished(Status st);

  Nursery();

  // Rather than keep a separate closing flag (and requiring mutex) we treat
  // "closing" as a default task
  std::atomic<uint32_t> num_tasks_outstanding_{1};
  std::atomic<uint32_t> num_closeables_created_{0};
  std::atomic<uint32_t> num_closeables_destroyed_{0};
  Future<> finished_;

  friend AsyncCloseable;
};

}  // namespace util
}  // namespace arrow

#endif  // ARROW_ASYNC_NURSERY_H
