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

#define PARQUET_IMPL_NAMESPACE bmi2
#include "parquet/level_conversion_inc.h"
#undef PARQUET_IMPL_NAMESPACE

namespace parquet {
namespace internal {
void DefLevelsToBitmapBmi2WithRepeatedParent(const int16_t* def_levels,
                                             int64_t num_def_levels, LevelInfo level_info,
                                             ValidityBitmapInputOutput* output) {
  bmi2::DefLevelsToBitmapSimd</*has_repeated_parent=*/true>(def_levels, num_def_levels,
                                                            level_info, output);
}

}  // namespace internal
}  // namespace parquet
