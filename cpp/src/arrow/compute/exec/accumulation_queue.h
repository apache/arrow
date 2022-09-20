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

#include <cstdint>
#include <vector>

#include "arrow/compute/exec.h"

namespace arrow {
namespace util {
using arrow::compute::ExecBatch;

/// \brief A container that accumulates batches until they are ready to
///        be processed.
class AccumulationQueue {
 public:
  AccumulationQueue() : row_count_(0) {}
  ~AccumulationQueue() = default;

  // We should never be copying ExecBatch around
  AccumulationQueue(const AccumulationQueue&) = delete;
  AccumulationQueue& operator=(const AccumulationQueue&) = delete;

  AccumulationQueue(AccumulationQueue&& that);
  AccumulationQueue& operator=(AccumulationQueue&& that);

  void Concatenate(AccumulationQueue&& that);
  void InsertBatch(ExecBatch batch);
  int64_t row_count() { return row_count_; }
  size_t batch_count() { return batches_.size(); }
  bool empty() const { return batches_.empty(); }
  void Clear();
  ExecBatch& operator[](size_t i);

 private:
  int64_t row_count_;
  std::vector<ExecBatch> batches_;
};

/// \brief Sequences data and allows for algorithms relying on ordered execution
///
/// The ordered execution queue will buffer data.  Typically, it is used when
/// there is an ordering in place, and we can assume the stream is roughly in
/// order, even though it may be quite jittery.  For example, if we are scanning
/// a dataset that is ordered by some column then the ordered accumulation queue
/// can be used even though a parallel dataset scan wouldn't neccesarily produce
/// a perfectly ordered stream due to jittery I/O.
///
/// The downstream side of the queue is broken into two parts.  The first part,
/// which should be relatively fast, runs serially, and creates tasks.  The second
/// part, which can be slower, will run these tasks in parallel.
///
/// For example, if we are doing an ordered group by operation then the serial part
/// will scan the batches to find group boundaries (places where the key value changes)
/// and will slice the input into groups.  The second part will actually run the
/// aggregations on the groups and then call the downstream nodes.
///
/// This node is currently implemented as a pipeline breaker in the sense that it creates
/// new thread tasks. Each downstream task (the slower part) will be run as a new thread
/// task (submitted via the scheduling callback).  A more sophisticated implementation
/// could probably be created that only breaks the pipeline when a batch arrives out of
/// order.
class OrderedAccumulationQueue {
 public:
  using Task = std::function<Status()>;
  using TaskFactoryCallback = std::function<Result<Task>(std::vector<ExecBatch>)>;
  using ScheduleCallback = std::function<Status(Task)>;

  virtual ~OrderedAccumulationQueue() = default;

  /// \brief Insert a new batch into the queue
  /// \param batch The batch to insert
  ///
  /// If the batch is the next batch in the sequence then a new task will be created from
  /// all available batches and submitted to the scheduler.
  virtual Status InsertBatch(ExecBatch batch) = 0;
  /// \brief Ensure the queue has been fully drained
  ///
  /// If a caller expects to process all data (e.g. not something like a fetch node) then
  /// the caller should call this to ensure that all batches have been processed.  This is
  /// a sanity check to help detect bugs which produce streams of batches with gaps in the
  /// sequencing index and is not strictly needed.
  virtual Status CheckDrained() const = 0;

  /// \brief Create a new ordered accumulation queue
  /// \param create_task The callback to use when a new task needs to be created
  ///
  /// This callback will run serially and will never be called reentrantly.  It will
  /// be given a vector of batches and those batches will be in sequence-order.
  ///
  /// Ideally this callback should be as quick as possible, doing only the work that
  /// needs to be truly serialized.  The returned task will then be scheduled.
  /// \param schedule The callback to use to schedule a new task
  static std::unique_ptr<OrderedAccumulationQueue> Make(TaskFactoryCallback create_task,
                                                        ScheduleCallback schedule);
};

}  // namespace util
}  // namespace arrow
