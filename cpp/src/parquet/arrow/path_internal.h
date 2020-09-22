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
#include <functional>
#include <memory>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"

#include "parquet/platform.h"

namespace arrow {

class Array;

}  // namespace arrow

namespace parquet {

struct ArrowWriteContext;

namespace arrow {

// This files contain internal implementation details and should not be considered
// part of the public API.

// The MultipathLevelBuilder is intended to fully support all Arrow nested types that
// map to parquet types (i.e. Everything but Unions).
//

/// \brief Half open range of elements in an array.
struct ElementRange {
  /// Upper bound of range (inclusive)
  int64_t start;
  /// Upper bound of range (exclusive)
  int64_t end;

  bool Empty() const { return start == end; }

  int64_t Size() const { return end - start; }
};

/// \brief Result for a single leaf array when running the builder on the
/// its root.
struct MultipathLevelBuilderResult {
  /// \brief The Array containing only the values to write (after all nesting has
  /// been processed.
  ///
  /// No additional processing is done on this array (it is copied as is when
  /// visited via a DFS).
  std::shared_ptr<::arrow::Array> leaf_array;

  /// \brief Might be null.
  const int16_t* def_levels = nullptr;

  /// \brief  Might be null.
  const int16_t* rep_levels = nullptr;

  /// \brief Number of items (int16_t) contained in def/rep_levels when present.
  int64_t def_rep_level_count = 0;

  /// \brief Contains element ranges of the required visiting on the
  /// descendants of the final list ancestor for any leaf node.
  ///
  /// The algorithm will attempt to consolidate visited ranges into
  /// the smallest number possible.
  ///
  /// This data is necessary to pass along because after producing
  /// def-rep levels for each leaf array it is impossible to determine
  /// which values have to be sent to parquet when a null list value
  /// in a nullable ListArray is non-empty.
  ///
  /// This allows for the parquet writing to determine which values ultimately
  /// needs to be written.
  std::vector<ElementRange> post_list_visited_elements;

  /// Whether the leaf array is nullable.
  bool leaf_is_nullable;
};

/// \brief Logic for being able to write out nesting (rep/def level) data that is
/// needed for writing to parquet.
class PARQUET_EXPORT MultipathLevelBuilder {
 public:
  /// \brief A callback function that will receive results from the call to
  /// Write(...) below.  The MultipathLevelBuilderResult passed in will
  /// only remain valid for the function call (i.e. storing it and relying
  /// for its data to be consistent afterwards will result in undefined
  /// behavior.
  using CallbackFunction =
      std::function<::arrow::Status(const MultipathLevelBuilderResult&)>;

  /// \brief Determine rep/def level information for the array.
  ///
  /// The callback will be invoked for each leaf Array that is a
  /// descendant of array.  Each leaf array is processed in a depth
  /// first traversal-order.
  ///
  /// \param[in] array The array to process.
  /// \param[in] array_field_nullable Whether the algorithm should consider
  ///   the the array column as nullable (as determined by its type's parent
  ///   field).
  /// \param[in, out] context for use when allocating memory, etc.
  /// \param[out] write_leaf_callback Callback to receive results.
  /// There will be one call to the write_leaf_callback for each leaf node.
  static ::arrow::Status Write(const ::arrow::Array& array, bool array_field_nullable,
                               ArrowWriteContext* context,
                               CallbackFunction write_leaf_callback);

  /// \brief Construct a new instance of the builder.
  ///
  /// \param[in] array The array to process.
  /// \param[in] array_field_nullable Whether the algorithm should consider
  ///   the the array column as nullable (as determined by its type's parent
  ///   field).
  static ::arrow::Result<std::unique_ptr<MultipathLevelBuilder>> Make(
      const ::arrow::Array& array, bool array_field_nullable);

  virtual ~MultipathLevelBuilder() = default;

  /// \brief Returns the number of leaf columns that need to be written
  /// to Parquet.
  virtual int GetLeafCount() const = 0;

  /// \brief Calls write_leaf_callback with the MultipathLevelBuilderResult corresponding
  /// to |leaf_index|.
  ///
  /// \param[in] leaf_index The index of the leaf column to write.  Must be in the range
  /// [0, GetLeafCount()].
  /// \param[in, out] context for use when allocating memory, etc.
  /// \param[out] write_leaf_callback Callback to receive the result.
  virtual ::arrow::Status Write(int leaf_index, ArrowWriteContext* context,
                                CallbackFunction write_leaf_callback) = 0;
};

}  // namespace arrow
}  // namespace parquet
