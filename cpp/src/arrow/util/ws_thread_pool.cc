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
#include <iostream>

#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

ResizableRingBuffer::ResizableRingBuffer(std::size_t size)
    : size_(size), mask_(size - 1), arr_(size) {
  // Confirm size is a power of 2
  DCHECK_EQ(size & (size - 1), 0);
}

std::size_t ResizableRingBuffer::size() const { return size_; }

Task* ResizableRingBuffer::Get(std::size_t i) {
  return arr_[i & mask_].load(std::memory_order_relaxed);
}

void ResizableRingBuffer::Put(std::size_t i, Task* task) {
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

void WorkQueue::Push(Task* task) {
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

Task* WorkQueue::Pop() {
  std::size_t new_bottom = bottom_.load(std::memory_order_relaxed) - 1;
  ResizableRingBuffer* tasks = tasks_.load(std::memory_order_relaxed);

  bottom_.store(new_bottom, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  std::size_t top = top_.load(std::memory_order_relaxed);

  Task* task = nullptr;
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

Task* WorkQueue::Steal() {
  std::size_t top = top_.load(std::memory_order_acquire);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  std::size_t bottom = bottom_.load(std::memory_order_acquire);

  Task* task = nullptr;

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

}  // namespace internal
}  // namespace arrow