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
#include "arrow/compute/exec/options.h"
#include "arrow/compute/light_array.h"
#include "arrow/compute/row/encode_internal.h"
#include "arrow/compute/row/row_internal.h"

namespace arrow {
namespace compute {

// Write operations (appending batch rows) must not be called by more than one
// thread at the same time.
//
// Read operations (row comparison, column decoding)
// can be called by multiple threads concurrently.
//
struct RowTable {
  RowTable() : is_initialized_(false) {}

  Status InitIfNeeded(MemoryPool* pool, const ExecBatch& batch);
  Status InitIfNeeded(MemoryPool* pool, const RowTableMetadata& row_metadata);

  Status AppendBatchSelection(MemoryPool* pool, const ExecBatch& batch, int begin_row_id,
                              int end_row_id, int num_row_ids, const uint16_t* row_ids,
                              std::vector<KeyColumnArray>& temp_column_arrays);

  // This can only be called for a minibatch.
  //
  void Compare(const ExecBatch& batch, int begin_row_id, int end_row_id, int num_selected,
               const uint16_t* batch_selection_maybe_null, const uint32_t* array_row_ids,
               uint32_t* out_num_not_equal, uint16_t* out_not_equal_selection,
               int64_t hardware_flags, util::TempVectorStack* temp_stack,
               std::vector<KeyColumnArray>& temp_column_arrays,
               uint8_t* out_match_bitvector_maybe_null = NULLPTR);

  // TODO: add AVX2 version
  //
  Status DecodeSelected(ResizableArrayData* target, int column_id, int num_rows_to_append,
                        const uint32_t* row_ids, MemoryPool* pool) const;

  void DebugPrintToFile(const char* filename, bool print_sorted) const;

  int64_t num_rows() const { return is_initialized_ ? rows_.length() : 0; }

  bool is_initialized_;
  RowTableEncoder encoder_;
  RowTableImpl rows_;
  RowTableImpl rows_temp_;
};

// Implements concatenating multiple row tables into a single one, using
// potentially multiple threads, each processing a single input row array.
//
class RowTableMerge {
 public:
  // Calculate total number of rows and size in bytes for merged sequence of
  // rows and allocate memory for it.
  //
  // If the rows are of varying length, initialize in the offset array the first
  // entry for the write area for each input row array. Leave all other
  // offsets and buffers uninitialized.
  //
  // All input sources must be initialized, but they can contain zero rows.
  //
  // Output in vector the first target row id for each source (exclusive
  // cummulative sum of number of rows in sources).
  //
  static Status PrepareForMerge(RowTable* target, const std::vector<RowTable*>& sources,
                                std::vector<int64_t>* first_target_row_id,
                                MemoryPool* pool);

  // Copy rows from source array to target array.
  // Both arrays must have the same row metadata.
  // Target array must already have the memory reserved in all internal buffers
  // for the copy of the rows.
  //
  // Copy of the rows will occupy the same amount of space in the target array
  // buffers as in the source array, but in the target array we pick at what row
  // position and offset we start writing.
  //
  // Optionally, the rows may be reordered during copy according to the
  // provided permutation, which represents some sorting order of source rows.
  // Nth element of the permutation array is the source row index for the Nth
  // row written into target array. If permutation is missing (null), then the
  // order of source rows will remain unchanged.
  //
  // In case of varying length rows, we purposefully skip outputting of N+1 (one
  // after last) offset, to allow concurrent copies of rows done to adjacent
  // ranges in the target array. This offset should already contain the right
  // value after calling the method preparing target array for merge (which
  // initializes boundary offsets for target row ranges for each source).
  //
  static void MergeSingle(RowTable* target, const RowTable& source,
                          int64_t first_target_row_id,
                          const int64_t* source_rows_permutation);

 private:
  // Copy rows from source array to a region of the target array.
  // This implementation is for fixed length rows.
  // Null information needs to be handled separately.
  //
  static void CopyFixedLength(RowTableImpl* target, const RowTableImpl& source,
                              int64_t first_target_row_id,
                              const int64_t* source_rows_permutation);

  // Copy rows from source array to a region of the target array.
  // This implementation is for varying length rows.
  // Null information needs to be handled separately.
  //
  static void CopyVaryingLength(RowTableImpl* target, const RowTableImpl& source,
                                int64_t first_target_row_id,
                                int64_t first_target_row_offset,
                                const int64_t* source_rows_permutation);

  // Copy null information from rows from source array to a region of the target
  // array.
  //
  static void CopyNulls(RowTableImpl* target, const RowTableImpl& source,
                        int64_t first_target_row_id,
                        const int64_t* source_rows_permutation);
};

}  // namespace compute
}  // namespace arrow
