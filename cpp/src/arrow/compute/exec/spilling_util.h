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

#include <atomic>
#include <functional>
#include <memory>
#include <vector>
#include "arrow/compute/exec/query_context.h"

namespace arrow {
namespace compute {
#ifdef _WIN32
using FileHandle = void*;
ARROW_EXPORT extern const FileHandle kInvalidHandle;
#else
using FileHandle = int;
constexpr FileHandle kInvalidHandle = -1;
#endif

// A temporary file meant for spilling data to disk. It can spill a batch to
// disk and read it back into memory. This class is designed to fully utilize
// disk bandwidth and for removing batches from memory as quickly as possible.
// Note that dictionaries are not spilled! They are expected to be very small,
// and so retaining them in memory is considered to be fine.
// One other note: Access to this class is expected to be exclusive from the
// perspective of the CPU thread pool. There may be concurrent accesses from
// the IO thread pool by tasks scheduled by this class itself (in other words,
// this class is not thread-safe from the user's point of view).
class ARROW_EXPORT SpillFile {
 public:
  static constexpr size_t kAlignment = 512;

  ~SpillFile();
  // To spill a batch the following must be true:
  // - Row offset for each column must be 0.
  // - Column buffers must be aligned to 512 bytes
  // - No column can be a scalar
  // These assumptions aren't as inconvenient as it seems because
  // typically batches will be partitioned before being spilled,
  // meaning the batches will come from ExecBatchBuilder, which
  // ensures these assumptions hold.
  // It is a bug to spill a batch after ReadBackBatches.
  Status SpillBatch(QueryContext* ctx, ExecBatch batch);

  // Reads back all of the batches from the disk, invoking `fn`
  // on each batch, and invoking `on_finished` when `fn` has finished
  // on all batches. Both will be run on the CPU thread pool.
  // Do NOT insert any batches after invoking this function.
  Status ReadBackBatches(QueryContext* ctx,
                         std::function<Status(size_t, size_t, ExecBatch)> fn,
                         std::function<Status(size_t)> on_finished);
  Status Cleanup();
  size_t num_batches() const { return batches_.size(); }
  size_t batches_written() const { return batches_written_.load(); }

  // Used for benchmarking only!
  Status PreallocateBatches(MemoryPool* memory_pool);

  struct BatchInfo;

 private:
  Status ScheduleReadbackTasks(QueryContext* ctx);
  Status OnBatchRead(size_t thread_index, size_t batch_index, ExecBatch batch);

  bool preallocated_ = false;

  FileHandle handle_ = kInvalidHandle;
  size_t size_ = 0;

  std::vector<BatchInfo*> batches_;

  std::atomic<size_t> batches_written_{0};
  std::atomic<bool> read_requested_{false};
  std::atomic<bool> read_started_{false};
  std::atomic<size_t> batches_read_{0};

  std::function<Status(size_t, size_t, ExecBatch)>
      readback_fn_;  // thread_index, batch_index, batch
  std::function<Status(size_t)> on_readback_finished_;
};
}  // namespace compute
}  // namespace arrow
