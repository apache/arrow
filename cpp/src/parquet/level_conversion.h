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

#include "arrow/util/bitmap.h"
#include "arrow/util/optional.h"
#include "arrow/util/variant.h"
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

  bool HasNullableValues() const { return repeated_ancestor_def_level < def_level; }

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

/// Converts def_levels to validity bitmaps for non-list arrays.
/// TODO: use input/output parameter below instead of individual calls.
void PARQUET_EXPORT DefinitionLevelsToBitmap(const int16_t* def_levels,
                                             int64_t num_def_levels, LevelInfo level_info,
                                             int64_t* values_read, int64_t* null_count,
                                             uint8_t* valid_bits,
                                             int64_t valid_bits_offset);

/// Input/Output structure for reconstructed validity bitmaps.
struct PARQUET_EXPORT ValidityBitmapInputOutput {
  /// The number of values added to the bitmap.
  int64_t values_read = 0;
  /// The number of nulls encountered.
  int64_t null_count = 0;
  // The validity bitmp to populate. Can only be null
  // for DefRepLevelsToListInfo (if all that is needed is list lengths).
  uint8_t* valid_bits = nullptr;
  /// Input only, offset into valid_bits to start at.
  int64_t valid_bits_offset = 0;
};

/// Reconstructs a validity bitmap and list lengths for a ListArray based on
/// def/rep levels.
void PARQUET_EXPORT ConvertDefRepLevelsToList(
    const int16_t* def_levels, const int16_t* rep_levels, int64_t num_def_levels,
    LevelInfo level_info, ValidityBitmapInputOutput* output,
    ::arrow::util::variant<int32_t*, int64_t*> lengths);

/// Bitmaps needed to resolve validity bitmaps for Arrow arrays with nested
/// parquet data.
struct PARQUET_EXPORT NestedValidityBitmaps {
  /// >= Bitmap for definition levels that indicate an element is defined (not-null).
  ///
  /// N.B. For nullable lists the definition level LevelInfo.def_level - 1, because
  /// LevelInfo.def_level is the definition level indicating an element in the list
  /// ist present.
  ::arrow::internal::Bitmap ge_def_level;
  /// >= Bitmap for repetition level a target nullable list.
  ::arrow::util::optional<::arrow::internal::Bitmap> ge_rep_level;
  /// >= Bitmap for the definition level that indicates the immediately prior
  /// list has present elements (not missing).
  ///
  /// Needed for arrays (leaf, structs, lists) nested within another list.
  ::arrow::util::optional<::arrow::internal::Bitmap> ge_rep_ancestor_def_level;
};

/// \brief Outputs a new validity bitmap for fields nested below a list.
void PARQUET_EXPORT ResolveNestedValidityBitmap(const NestedValidityBitmaps& bitmaps,
                                                int null_slot_count,
                                                ValidityBitmapInputOutput* output);

/// Bitmaps necessary to calculate list lengths.
struct PARQUET_EXPORT ListLengthBitmaps {
  /// >= bitmap for the repetition level for list that lengths needed to be extract for.
  ::arrow::internal::Bitmap ge_rep_level;
  /// >= bitmap for definition level that indicates a element is in the list.
  ::arrow::internal::Bitmap ge_def_level;
  /// > bitmap for the target repetition level.
  ///
  /// Only needed for intermediate repeated elements
  /// when there are two more more nested repeated fields.
  ::arrow::util::optional<::arrow::internal::Bitmap> gt_rep_level;
  /// >= bitmap for the definition level that indicates an element is present (not empty)
  /// in a the last list before this one.
  ///
  /// Only needed for lists that are nested within other lists.
  ::arrow::util::optional<::arrow::internal::Bitmap> ge_rep_ancestor_def_level;
};

::arrow::util::variant<int32_t*, int64_t*> PARQUET_EXPORT PopulateListLengths(
    const ListLengthBitmaps& bitmaps, ::arrow::util::variant<int32_t*, int64_t*> lengths);

uint64_t RunBasedExtract(uint64_t bitmap, uint64_t selection);
#if defined(ARROW_HAVE_RUNTIME_BMI2)
void PARQUET_EXPORT DefinitionLevelsToBitmapBmi2WithRepeatedParent(
    const int16_t* def_levels, int64_t num_def_levels, LevelInfo level_info,
    int64_t* values_read, int64_t* null_count, uint8_t* valid_bits,
    int64_t valid_bits_offset);
void PARQUET_EXPORT ResolveNestedValidityBitmapBmi2(const NestedValidityBitmaps& bitmaps,
                                                    int null_slot_count,
                                                    ValidityBitmapInputOutput* output);

::arrow::util::variant<int32_t*, int64_t*> PARQUET_EXPORT PopulateListLengthsBmi2(
    const ListLengthBitmaps& bitmaps, ::arrow::util::variant<int32_t*, int64_t*> lengths);
#endif

}  // namespace internal
}  // namespace parquet
