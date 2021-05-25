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

#include <cstddef>

#include "arrow/util/cancel.h"
#include "arrow/util/functional.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace internal {

struct Task {
  FnOnce<void()> callable;
  StopToken stop_token;
  Executor::StopCallback stop_callback;
};

/// An implementation of a Chase-Lev deque as described in
/// https://www.dre.vanderbilt.edu/~schmidt/PDF/work-stealing-dequeue.pdf
///
/// Implementations to look at:
///  * https://github.com/taskflow/taskflow/blob/master/taskflow/core/tsq.hpp
///
/// It is lock-free and supports multiple consumers with a single producer
///
/// The owning thread pushes tasks to the bottom of the stack
/// The owning thread pulls tasks from the bottom of the stack (LIFO)
///
/// Thieving threads pull tasks from the top of the stack (FIFO)
///
/// The memory ordering is designed such that the hot path (owning thread pulls its own
/// items) is fastest.  It is based on a paper "Correct and Efficient Work-Stealing for
/// Weak Memory Models" here: https://fzn.fr/readings/ppopp13.pdf
///
/// Potential future optimizations
/// * The original paper (see above) describes how to shrink the array to help conserve
///   memory after a burst  of tasks.
/// * When the buffer resizes the original buffer is destroyed immediately.  The taskflow
///   implementation throws it in a garbage buffer to be destroyed later at destruction
///   time

class ResizableRingBuffer {
 public:
  /// Creates a ring buffer with a fixed capacity
  /// NB: size must be a power of 2
  ResizableRingBuffer(std::size_t size);
  /// Size of the buffer
  std::size_t size() const;
  /// Gets an item from the buffer
  Task* Get(std::size_t i);
  /// Inserts an item into the buffer
  void Put(std::size_t i, Task* task);
  /// Creates a new buffer with 2x the capacity, needs to know where top and
  /// bottom are to copy into the new buffer in the correct order.
  ResizableRingBuffer Resize(std::size_t bottom, std::size_t top);

 private:
  std::size_t size_;
  // The original paper has to regularly do (i % size).  Since size is always a power
  // of 2 we can do this more efficiently with (i & (size - 1)).   mask_ is size_ - 1
  std::size_t mask_;
  std::vector<std::atomic<Task*>> arr_;
};

class WorkQueue {
 public:
  /// Creates a work queue with a given initial capacity, the queue can grow beyond this
  /// capacity if needed
  WorkQueue(std::size_t initial_capacity);
  ~WorkQueue();

  bool Empty() const;
  std::size_t Size() const;
  std::size_t Capacity() const;

  /// Adds an item to the bottom of the stack
  /// NB: Must only be called by the owning thread
  void Push(Task* task);
  /// Pulls an item off the bottom of the stack
  /// NB: Must only be called by the owning thread
  Task* Pop();
  /// Steals an item off the top of the stack
  Task* Steal();

 private:
  std::atomic<std::size_t> top_;
  std::atomic<std::size_t> bottom_;
  std::atomic<ResizableRingBuffer*> tasks_;
  // We can't delete a buffer immediately when we resize it because there may be threads
  // in the middle of a steal operation.  So instead we just keep track of all the buffers
  // here and delete them at the end.  Each resize doubles the queue so this shouldn't
  // happen all that often.
  std::vector<ResizableRingBuffer*> to_delete_;
};

}  // namespace internal
}  // namespace arrow