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

#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

enum class JoinType {
  LEFT_SEMI,
  RIGHT_SEMI,
  LEFT_ANTI,
  RIGHT_ANTI,
  INNER,
  LEFT_OUTER,
  RIGHT_OUTER,
  FULL_OUTER
};

class ReversibleJoinType {
  ReversibleJoinType(JoinType join_type_when_first_child_is_probe);
  JoinType get(int probe_side) {
    if (probe_side == 0) {
      return join_type_when_first_child_is_probe_;
    } else {
      ARROW_DCHECK(probe_side == 1);
      switch (join_type_when_first_child_is_probe_) {
        case JoinType::LEFT_SEMI:
          return JoinType::RIGHT_SEMI;
          break;
        case JoinType::RIGHT_SEMI:
          return JoinType::LEFT_SEMI;
          break;
        case JoinType::LEFT_ANTI:
          return JoinType::RIGHT_ANTI;
          break;
        case JoinType::RIGHT_ANTI:
          return JoinType::LEFT_ANTI;
          break;
        case JoinType::INNER:
          return JoinType::INNER;
          break;
        case JoinType::LEFT_OUTER:
          return JoinType::RIGHT_OUTER;
          break;
        case JoinType::RIGHT_OUTER:
          return JoinType::LEFT_OUTER;
          break;
        case JoinType::FULL_OUTER:
          return JoinType::FULL_OUTER;
          break;
      }
    }
  }

 private:
  JoinType join_type_when_first_child_is_probe_;
};

using hash_type = uint32_t;
using key_id_type = uint32_t;

}  // namespace compute
}  // namespace arrow