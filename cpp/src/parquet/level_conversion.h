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

#include "parquet/platform.h"
#include "parquet/schema.h"

namespace parquet {
namespace internal {

struct PARQUET_EXPORT LevelInfo {
  LevelInfo()
      : null_slot_usage(1), def_level(0), rep_level(0), repeated_ancestor_def_level(0) {}
  LevelInfo(int32_t null_slots, int32_t definition_level, int32_t repetition_level,
            int32_t repeated_ancestor_definition_level)
      : null_slot_usage(null_slots),
        def_level(definition_level),
        rep_level(repetition_level),
        repeated_ancestor_def_level(repeated_ancestor_definition_level) {}

  bool operator==(const LevelInfo& b) const {
    return null_slot_usage == b.null_slot_usage && def_level == b.def_level &&
           rep_level == b.rep_level &&
           repeated_ancestor_def_level == b.repeated_ancestor_def_level;
  }

  // How many slots an undefined but present (i.e. null) element in
  // parquet consumes when decoding to Arrow.
  // "Slot" is used in the same context as the Arrow specification
  // (i.e. a value holder).
  // This is only ever >1 for descendents of FixedSizeList.
  int32_t null_slot_usage = 1;

  // The definition level at which the value for the field
  // is considered not null (definition levels greater than
  // or equal to this value indicate a not-null
  // value for the field). For list fields definition levels
  // greater than or equal to this field indicate a present,
  // possibly null, child value.
  int16_t def_level = 0;

  // The repetition level corresponding to this element
  // or the closest repeated ancestor.  Any repetition
  // level less than this indicates either a new list OR
  // an empty list (which is determined in conjunction
  // with definition levels).
  int16_t rep_level = 0;

  // The definition level indicating the level at which the closest
  // repeated ancestor is not empty.  This is used to discriminate
  // between a value less than |def_level| being null or excluded entirely.
  // For instance if we have an arrow schema like:
  // list(struct(f0: int)).  Then then there are the following
  // definition levels:
  //   0 = null list
  //   1 = present but empty list.
  //   2 = a null value in the list
  //   3 = a non null struct but null integer.
  //   4 = a present integer.
  // When reconstructing, the struct and integer arrays'
  // repeated_ancestor_def_level would be 2.  Any
  // def_level < 2 indicates that there isn't a corresponding
  // child value in the list.
  // i.e. [null, [], [null], [{f0: null}], [{f0: 1}]]
  // has the def levels [0, 1, 2, 3, 4].  The actual
  // struct array is only of length 3: [not-set, set, set] and
  // the int array is also of length 3: [N/A, null, 1].
  //
  int16_t repeated_ancestor_def_level = 0;

  /// Increments levels according to the cardinality of node.
  void Increment(const schema::Node& node) {
    if (node.is_repeated()) {
      IncrementRepeated();
      return;
    }
    if (node.is_optional()) {
      IncrementOptional();
      return;
    }
  }

  /// Incremetns level for a optional node.
  void IncrementOptional() { def_level++; }

  /// Increments levels for the repeated node.  Returns
  /// the previous ancestor_list_def_level.
  int16_t IncrementRepeated() {
    int16_t last_repeated_ancestor = repeated_ancestor_def_level;

    // Repeated fields add both a repetition and definition level. This is used
    // to distinguish between an empty list and a list with an item in it.
    ++rep_level;
    ++def_level;
    // For levels >= repeated_ancenstor_def_level it indicates the list was
    // non-null and had at least one element.  This is important
    // for later decoding because we need to add a slot for these
    // values.  for levels < current_def_level no slots are added
    // to arrays.
    repeated_ancestor_def_level = def_level;
    return last_repeated_ancestor;
  }

  friend std::ostream& operator<<(std::ostream& os, const LevelInfo& levels) {
    // This print method is to silence valgrind issues.  What's printed
    // is not important because all asserts happen directly on
    // members.
    os << "{def=" << levels.def_level << ", rep=" << levels.rep_level
       << ", repeated_ancestor_def=" << levels.repeated_ancestor_def_level;
    if (levels.null_slot_usage > 1) {
      os << ", null_slot_usage=" << levels.null_slot_usage;
    }
    os << "}";
    return os;
  }
};

void PARQUET_EXPORT DefinitionLevelsToBitmap(
    const int16_t* def_levels, int64_t num_def_levels, const int16_t max_definition_level,
    const int16_t max_repetition_level, int64_t* values_read, int64_t* null_count,
    uint8_t* valid_bits, int64_t valid_bits_offset);

// These APIs are likely to be revised as part of ARROW-8494 to reduce duplicate code.
// They currently represent minimal functionality for vectorized computation of definition
// levels.

#if defined(ARROW_LITTLE_ENDIAN)
/// Builds a bitmap by applying predicate to the level vector provided.
///
/// \param[in] levels Rep or def level array.
/// \param[in] num_levels The number of levels to process (must be [0, 64])
/// \param[in] predicate The predicate to apply (must have the signature `bool
/// predicate(int16_t)`.
/// \returns The bitmap using least significant "bit" ordering.
///
/// N.B. Correct byte ordering is dependent on little-endian architectures.
///
template <typename Predicate>
uint64_t LevelsToBitmap(const int16_t* levels, int64_t num_levels, Predicate predicate) {
  // Both clang and GCC can vectorize this automatically with SSE4/AVX2.
  uint64_t mask = 0;
  for (int x = 0; x < num_levels; x++) {
    mask |= static_cast<uint64_t>(predicate(levels[x]) ? 1 : 0) << x;
  }
  return mask;
}

/// Builds a  bitmap where each set bit indicates the corresponding level is greater
/// than rhs.
static inline uint64_t GreaterThanBitmap(const int16_t* levels, int64_t num_levels,
                                         int16_t rhs) {
  return LevelsToBitmap(levels, num_levels, [rhs](int16_t value) { return value > rhs; });
}

#endif

}  // namespace internal
}  // namespace parquet
