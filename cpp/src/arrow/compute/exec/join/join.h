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

namespace arrow {
namespace compute {

/*
  The basis for the future implementation of main hash join interface - its exec node.
  TODO: Implement missing ExecNode class
*/

/*
class HashJoin {
  enum class BatchSource { INPUT, SAVED, SAVED_FILTERED, HASH_TABLE };

  void Make(Schema left_keys, Schema right_keys, Schema left_output, Schema right_output,
            std::string output_field_name_prefix, Schema left_filter_input,
            Schema right_filter_input,
            // residual filter callback or template
            );

// Refer to state diagram
void ProcessInputBatch(int side, BatchSource source,
                       std::shared_ptr<std::vector<KeyColumnArray>>& batch) {
  ARROW_DCHECK(side == 0 || side == 1);
  int other_side = 1 - side;

  auto state = input_state_mgr.get(side);
  ARROW_DCHECK(state == HAS_MORE_ROWS);
  auto other_state = input_state_mgr.get(other_side);
  switch (other_state) {
    case READING_INPUT:
    case BUILDING_EARLY_FILTER:
      input_data[side].AppendBatch(batch);
      break;
    case EARLY_FILTER_READY:
    case BUILDING_HASH_TABLE:
      auto join_key_batch =
          ProjectBatch(batch, input_schema[side], join_key_schema[side]);
      auto hash = ComputeHash(join_key_batch);
      auto filter_result = EarlyFilter(hash, input_data[other_side].early_filter);
      // TODO: Depending on the join type either output or remove rows with no match
      input_data[side].AppendFiltered(batch, hash, filter_result);
      break;
    case HASH_TABLE_READY:
      HashTableProbe(batch);
      break;
    default:
      ARROW_DCHECK(false);
  }
}

// List of tasks:
// building early filter
// building hash table
// filtering using early filter
// probing hash table using
void ExecuteInternalTask() {
  // Messsage processing for (;;) loop
  // returns false when finished
  // can be called multiple times after returning false and will consistently keep
  // returning false can result in an empty call that does not return false if one of
  // the join inputs is still streaming
}

void SaveBatch() {}

void FilterAndSaveBatch() {}

void ProbeHashTable() {}

void BuildEarlyFilter() {}

void BuildHashTable() {}

void ScanHashTable() {}

void OnFinishedProcessingInputBatch(int side, BatchSource source) {}

void OnFinishedReadingInput() {
  // Check if this was the last batch
  // State transition
  // Build early filter
}

void OnSizeLimitReached() {}

void OnFinishedBuildingEarlyFilter() {}

void OnFinishedBuildingHashTable() {}

void OnFinishedHashTableProbing() {}

void OnFinishedHashTableScan() {}
}
;

*/

}  // namespace compute
}  // namespace arrow