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
#include <cstdint>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/type_fwd.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/mutex.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/type_fwd.h"

namespace arrow {
namespace compute {

ARROW_EXPORT
Status ValidateExecNodeInputs(ExecPlan* plan, const std::vector<ExecNode*>& inputs,
                              int expected_num_inputs, const char* kind_name);

ARROW_EXPORT
Result<std::shared_ptr<Table>> TableFromExecBatches(
    const std::shared_ptr<Schema>& schema, const std::vector<ExecBatch>& exec_batches);

class ARROW_EXPORT AtomicCounter {
 public:
  AtomicCounter() = default;

  int count() const { return count_.load(); }

  std::optional<int> total() const {
    int total = total_.load();
    if (total == -1) return {};
    return total;
  }

  // return true if the counter is complete
  bool Increment() {
    DCHECK_NE(count_.load(), total_.load());
    int count = count_.fetch_add(1) + 1;
    if (count != total_.load()) return false;
    return DoneOnce();
  }

  // return true if the counter is complete
  bool SetTotal(int total) {
    total_.store(total);
    if (count_.load() != total) return false;
    return DoneOnce();
  }

  // return true if the counter has not already been completed
  bool Cancel() { return DoneOnce(); }

  // return true if the counter has finished or been cancelled
  bool Completed() { return complete_.load(); }

 private:
  // ensure there is only one true return from Increment(), SetTotal(), or Cancel()
  bool DoneOnce() {
    bool expected = false;
    return complete_.compare_exchange_strong(expected, true);
  }

  std::atomic<int> count_{0}, total_{-1};
  std::atomic<bool> complete_{false};
};

class ARROW_EXPORT ThreadIndexer {
 public:
  size_t operator()();

  static size_t Capacity();

 private:
  static size_t Check(size_t thread_index);

  util::Mutex mutex_;
  std::unordered_map<std::thread::id, size_t> id_to_index_;
};

// Helper class to calculate the modified number of rows to process using SIMD.
//
// Some array elements at the end will be skipped in order to avoid buffer
// overrun, when doing memory loads and stores using larger word size than a
// single array element.
//
class TailSkipForSIMD {
 public:
  static int64_t FixBitAccess(int num_bytes_accessed_together, int64_t num_rows,
                              int bit_offset) {
    int64_t num_bytes = bit_util::BytesForBits(num_rows + bit_offset);
    int64_t num_bytes_safe =
        std::max(static_cast<int64_t>(0LL), num_bytes - num_bytes_accessed_together + 1);
    int64_t num_rows_safe =
        std::max(static_cast<int64_t>(0LL), 8 * num_bytes_safe - bit_offset);
    return std::min(num_rows_safe, num_rows);
  }
  static int64_t FixBinaryAccess(int num_bytes_accessed_together, int64_t num_rows,
                                 int64_t length) {
    int64_t num_rows_to_skip = bit_util::CeilDiv(length, num_bytes_accessed_together);
    int64_t num_rows_safe =
        std::max(static_cast<int64_t>(0LL), num_rows - num_rows_to_skip);
    return num_rows_safe;
  }
  static int64_t FixVarBinaryAccess(int num_bytes_accessed_together, int64_t num_rows,
                                    const uint32_t* offsets) {
    // Do not process rows that could read past the end of the buffer using N
    // byte loads/stores.
    //
    int64_t num_rows_safe = num_rows;
    while (num_rows_safe > 0 &&
           offsets[num_rows_safe] + num_bytes_accessed_together > offsets[num_rows]) {
      --num_rows_safe;
    }
    return num_rows_safe;
  }
  static int FixSelection(int64_t num_rows_safe, int num_selected,
                          const uint16_t* selection) {
    int num_selected_safe = num_selected;
    while (num_selected_safe > 0 && selection[num_selected_safe - 1] >= num_rows_safe) {
      --num_selected_safe;
    }
    return num_selected_safe;
  }
};

/// \brief A consumer that collects results into an in-memory table
struct ARROW_EXPORT TableSinkNodeConsumer : public SinkNodeConsumer {
 public:
  TableSinkNodeConsumer(std::shared_ptr<Table>* out, MemoryPool* pool)
      : out_(out), pool_(pool) {}
  Status Init(const std::shared_ptr<Schema>& schema,
              BackpressureControl* backpressure_control, ExecPlan* plan) override;
  Status Consume(ExecBatch batch) override;
  Future<> Finish() override;

 private:
  std::shared_ptr<Table>* out_;
  MemoryPool* pool_;
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  util::Mutex consume_mutex_;
};

class ARROW_EXPORT NullSinkNodeConsumer : public SinkNodeConsumer {
 public:
  Status Init(const std::shared_ptr<Schema>&, BackpressureControl*,
              ExecPlan* plan) override {
    return Status::OK();
  }
  Status Consume(ExecBatch exec_batch) override { return Status::OK(); }
  Future<> Finish() override { return Status::OK(); }

 public:
  static std::shared_ptr<NullSinkNodeConsumer> Make() {
    return std::make_shared<NullSinkNodeConsumer>();
  }
};

/// CRTP helper for tracing helper functions

class ARROW_EXPORT TracedNode {
 public:
  // All nodes should call TraceStartProducing or NoteStartProducing exactly once
  // Most nodes will be fine with a call to NoteStartProducing since the StartProducing
  // call is usually fairly cheap and simply schedules tasks to fetch the actual data.

  explicit TracedNode(ExecNode* node) : node_(node) {}

  // Create a span to record the StartProducing work
  [[nodiscard]] ::arrow::internal::tracing::Scope TraceStartProducing(
      std::string extra_details) const;

  // Record a call to StartProducing without creating with a span
  void NoteStartProducing(std::string extra_details) const;

  // All nodes should call TraceInputReceived for each batch they receive.  This call
  // should track the time spent processing the batch.  NoteInputReceived is available
  // but usually won't be used unless a node is simply adding batches to a trivial queue.

  // Create a span to record the InputReceived work
  [[nodiscard]] ::arrow::internal::tracing::Scope TraceInputReceived(
      const ExecBatch& batch) const;

  // Record a call to InputReceived without creating with a span
  void NoteInputReceived(const ExecBatch& batch) const;

  // Create a span to record any "finish" work.  This should NOT be called as part of
  // InputFinished and many nodes may not need to call this at all.  This should be used
  // when a node has some extra work that has to be done once it has received all of its
  // data.  For example, an aggregation node calculating aggregations.  This will
  // typically be called as a result of InputFinished OR InputReceived.
  [[nodiscard]] ::arrow::internal::tracing::Scope TraceFinish() const;

 private:
  ExecNode* node_;
};

}  // namespace compute
}  // namespace arrow
