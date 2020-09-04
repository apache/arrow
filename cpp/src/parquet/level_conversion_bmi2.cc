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
#include "parquet/level_conversion.h"

#define BMI_RUNTIME_VERSION bmi2
#include "parquet/level_conversion_inc.h"
#undef BMI_RUNTIME_VERSION

namespace parquet {
namespace internal {
void DefinitionLevelsToBitmapBmi2WithRepeatedParent(
    const int16_t* def_levels, int64_t num_def_levels, LevelInfo level_info,
    int64_t* values_read, int64_t* null_count, uint8_t* valid_bits,
    int64_t valid_bits_offset) {
  bmi2::DefinitionLevelsToBitmapSimd</*has_repeated_parent=*/true>(
      def_levels, num_def_levels, level_info, values_read, null_count, valid_bits,
      valid_bits_offset);
}

void ResolveNestedValidityBitmapBmi2(const NestedValidityBitmaps& bitmaps,
                                     int null_slot_count,
                                     ValidityBitmapInputOutput* output) {
  bmi2::ResolveNestedValidityBitmap(bitmaps, null_slot_count, output);
}

::arrow::util::variant<int32_t*, int64_t*> PopulateListLengthsBmi2(
    const ListLengthBitmaps& bitmaps,
    ::arrow::util::variant<int32_t*, int64_t*> lengths) {
  return bmi2::PopulateListLengthsListTypeDispatch(bitmaps, lengths);
}

}  // namespace internal
}  // namespace parquet
