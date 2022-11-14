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

#include <memory>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

/// \brief A segment of contiguous rows for grouping
struct ARROW_EXPORT GroupingSegment {
  int64_t offset;
  int64_t length;
  bool is_open;
};

inline bool operator==(const GroupingSegment& segment1, const GroupingSegment& segment2) {
  return segment1.offset == segment2.offset && segment1.length == segment2.length &&
         segment1.is_open == segment2.is_open;
}
inline bool operator!=(const GroupingSegment& segment1, const GroupingSegment& segment2) {
  return !(segment1 == segment2);
}

/// \brief Computes grouping segments for a batch. Each segment covers rows with identical
/// values in the batch. The values in the batch are often selected as keys from a larger
/// batch.
class ARROW_EXPORT GroupingSegmenter {
 public:
  virtual ~GroupingSegmenter() = default;

  /// \brief Construct a GroupingSegmenter which receives the specified key types
  static Result<std::unique_ptr<GroupingSegmenter>> Make(
      std::vector<TypeHolder> key_types, ExecContext* ctx = default_exec_context());

  /// \brief Reset this grouping segmenter
  virtual Status Reset() = 0;

  /// \brief Get the next segment for the given batch starting from the given offset
  virtual Result<GroupingSegment> GetNextSegment(const ExecSpan& batch,
                                                 int64_t offset) = 0;

  /// \brief Get the next segment for the given batch starting from the given offset
  virtual Result<GroupingSegment> GetNextSegment(const ExecBatch& batch,
                                                 int64_t offset) = 0;
};

/// Consumes batches of keys and yields batches of the group ids.
class ARROW_EXPORT Grouper {
 public:
  virtual ~Grouper() = default;

  /// Construct a Grouper which receives the specified key types
  static Result<std::unique_ptr<Grouper>> Make(const std::vector<TypeHolder>& key_types,
                                               ExecContext* ctx = default_exec_context());

  /// Consume a batch of keys, producing the corresponding group ids as an integer array,
  /// over a slice defined by an offset and length, which defaults to the batch length.
  /// Currently only uint32 indices will be produced, eventually the bit width will only
  /// be as wide as necessary.
  virtual Result<Datum> Consume(const ExecSpan& batch, int64_t consume_offset = 0,
                                int64_t consume_length = -1) = 0;

  /// Consume a batch of keys, producing the corresponding group ids as an integer array,
  /// over a slice defined by an offset and length, which defaults to the batch length.
  /// Currently only uint32 indices will be produced, eventually the bit width will only
  /// be as wide as necessary.
  virtual Result<Datum> Consume(const ExecBatch& batch, int64_t consume_offset = 0,
                                int64_t consume_length = -1) = 0;

  /// Get current unique keys. May be called multiple times.
  virtual Result<ExecBatch> GetUniques() = 0;

  /// Get the current number of groups.
  virtual uint32_t num_groups() const = 0;

  /// \brief Assemble lists of indices of identical elements.
  ///
  /// \param[in] ids An unsigned, all-valid integral array which will be
  ///                used as grouping criteria.
  /// \param[in] num_groups An upper bound for the elements of ids
  /// \param[in] ctx Execution context to use during the operation
  /// \return A num_groups-long ListArray where the slot at i contains a
  ///         list of indices where i appears in ids.
  ///
  ///   MakeGroupings([
  ///       2,
  ///       2,
  ///       5,
  ///       5,
  ///       2,
  ///       3
  ///   ], 8) == [
  ///       [],
  ///       [],
  ///       [0, 1, 4],
  ///       [5],
  ///       [],
  ///       [2, 3],
  ///       [],
  ///       []
  ///   ]
  static Result<std::shared_ptr<ListArray>> MakeGroupings(
      const UInt32Array& ids, uint32_t num_groups,
      ExecContext* ctx = default_exec_context());

  /// \brief Produce a ListArray whose slots are selections of `array` which correspond to
  /// the provided groupings.
  ///
  /// For example,
  ///   ApplyGroupings([
  ///       [],
  ///       [],
  ///       [0, 1, 4],
  ///       [5],
  ///       [],
  ///       [2, 3],
  ///       [],
  ///       []
  ///   ], [2, 2, 5, 5, 2, 3]) == [
  ///       [],
  ///       [],
  ///       [2, 2, 2],
  ///       [3],
  ///       [],
  ///       [5, 5],
  ///       [],
  ///       []
  ///   ]
  static Result<std::shared_ptr<ListArray>> ApplyGroupings(
      const ListArray& groupings, const Array& array,
      ExecContext* ctx = default_exec_context());
};

}  // namespace compute
}  // namespace arrow
