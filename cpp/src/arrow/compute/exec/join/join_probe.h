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
#include <memory>
#include <vector>

#include "arrow/compute/exec/join/join_type.h"
#include "arrow/compute/exec/util.h"
#include "arrow/util/bit_util.h"

/*
  This file implements specific row processing strategies related to different types of
  join (left semi-join, full outer join, ...).
*/

namespace arrow {
namespace compute {
/*
class MatchingPairsBatchIterator {
 public:
  /// \param num_batch_rows number of rows in input batch to iterate over
  /// \param opt_batch_row_ids row ids in input batch. Null means the entire sequence
  /// starting from 0.
  /// \param opt_key_to_payload_map maps key id to a range of payload ids. Element indexed
  /// by key id gives start of the range, element right after gives end of range (one
  /// after the last element). Null means 1-1 mapping from key id to payload id, in which
  /// case computing payload ids is redundant and will be skipped.
  /// \param payload_is_present false means that there is no payload and therefore
  /// computing payload ids is useless and will be skipped
  MatchingPairsBatchIterator(int num_batch_rows, const uint16_t* opt_batch_row_ids,
                             const key_id_type* key_ids,
                             const key_id_type* opt_key_to_payload_map,
                             bool payload_is_present);

  bool PayloadIdsSameAsKeyIds() const {
    return !(payload_is_present_ && opt_key_to_payload_map_);
  }

  /// The caller is responsible for pre-allocating output arrays with at least
  /// num_rows_max elements. Payload id array may be null if payload ids are always the
  /// same as key ids and therefore not populated by this method.
  int GetNextBatch(int num_rows_max, uint16_t* out_batch_row_ids,
                   key_id_type* out_key_ids, key_id_type* out_opt_payload_ids,
                   bool* out_payload_ids_same_as_key_ids);

 private:
  int num_batch_rows_;
  const uint16_t* opt_batch_row_ids_;
  const key_id_type* key_ids_;
  const key_id_type* opt_key_to_payload_map_;
  bool payload_is_present_;
  int64_t num_processed_batch_rows_;
  int64_t num_processed_matches_for_last_batch_row_;
};

void MarkHashTableMatch();

// Refer to flow diagram
void HashTableProbe_SemiOrAntiSemi(const ExecBatch& batch) {}

// Refer to flow diagram
// TODO: in a while loop keep returning batches or take a lambda for outputting batch
// (non-template)
void HashTableProbe_InnerOrOuter(HashTable* hash_table, int side,
                                 const ExecBatch& batch) {
  auto join_key_batch = ProjectBatch(batch, input_schema[side], join_key_schema[side]);
  if (!batch_filtered) {
    // Compute hash
    // Execute early filters
  }
  auto has_match_bitvector;
  auto match_iterator_begin;
  auto match_iterator_end;
  hash_table->Find(has_match_bitvector, match_iterator_begin);
  if (multiple_matches_possible) {
    // Remap match iterator begin
    // Load match iterator end
  } else {
    // Set match iterator end to match iterator begin plus one
  }
  // For each batch of matching pairs
  std::vector<int, int> matching_pairs;
  for (auto batch = matching_pairs_batch_iterator.begin();
       batch != matching_pairs_batch_iterator.end(); ++matching_pairs_batch_iterator) {
    if (has_residual_filter) {
      // For each column in residual filter definition
      // Is the column coming from batch side or hash table side?
      for (;;) {
        if (auto result = Find(residual_filter_input_schema[column],
                               input_schema[side]) != kNotFound) {
          batch.AppendColumnShuffle(matching_pairs_batch_iterator.first, result);
        } else {
          auto result =
              Find(residual_filter_input_schema[column], join_payload_schema[side]);
          ARROW_DCHECK(result != kNotFound);
          join_sides[other_side(side)].payload().AppendColumnShuffle(
              matching_pairs_batch_iterator.second, result);
        }
      }
      EvaluateResidualFilter(residual_filter_batch);
      // Update match bit vector
      // Update matching pairs
    }
    // Update hash table match vector if needed
    // Assemble and output next result batch
  }
}

void HashTableScan() {}
*/
}  // namespace compute
}  // namespace arrow