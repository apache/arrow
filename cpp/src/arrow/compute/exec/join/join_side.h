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

#include "arrow/compute/exec/join/join_batch.h"
#include "arrow/compute/exec/join/join_filter.h"

/*
  This file contains declarations of data that is stored for each of two sides of the hash
  join. Classes here represent the state of processing of each side during execution of
  each hash join.
*/

namespace arrow {
namespace compute {

// READING_INPUT is a start state and FINISHED is an end state.
// Transition graph is acyclic. The end state can be reached from any state.
// All other states have only one valid input state (their predecessor on the list below).
enum class JoinSideState : uint8_t {
  READING_INPUT,
  SAVED_INPUT_READY,
  EARLY_FILTER_READY,
  HASH_TABLE_READY,
  FINISHED,
};

class JoinState {
  JoinState() { states_[0] = states_[1] = JoinSideState::READING_INPUT; }

  // Return true if transition was successful, false if transition has already been
  bool StateTransition(int side, JoinSideState new_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    JoinSideState old_state = states_[side];
    switch (new_state) {
      case JoinSideState::READING_INPUT:
        return false;
      case JoinSideState::SAVED_INPUT_READY:
        if (old_state == JoinSideState::READING_INPUT) {
          states_[side] = new_state;
          return true;
        } else {
          return false;
        }
      case JoinSideState::EARLY_FILTER_READY:
        if (old_state == JoinSideState::SAVED_INPUT_READY) {
          states_[side] = new_state;
          return true;
        } else {
          return false;
        }
      case JoinSideState::HASH_TABLE_READY:
        if (old_state == JoinSideState::EARLY_FILTER_READY) {
          states_[side] = new_state;
          return true;
        } else {
          return false;
        }
      case JoinSideState::FINISHED:
        if (old_state != JoinSideState::FINISHED) {
          states_[side] = new_state;
          return true;
        } else {
          return false;
        }
      default:
        return false;
    }
  }

  JoinSideState state(int side) const { return states_[side]; }

 private:
  JoinSideState states_[2];
  std::mutex mutex_;
};

struct JoinSideData {
  void SaveBatch(const ExecBatch& batch, bool is_filtered) {
    std::lock_guard<std::mutex> lock(mutex);
    if (is_filtered) {
      saved_filtered.push_back(BatchWithJoinData(batch));
    } else {
      saved.push_back(BatchWithJoinData(batch));
    }
  }
  std::vector<BatchWithJoinData> saved;
  std::vector<BatchWithJoinData> saved_filtered;
  ApproximateMembershipTest early_filter;
  std::mutex mutex;
};

static inline int other_side(int side) {
  ARROW_DCHECK(side == 0 || side == 1);
  return 1 - side;
}

}  // namespace compute
}  // namespace arrow