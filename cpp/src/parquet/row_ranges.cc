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

#include "row_ranges.h"

namespace parquet {

bool RowRanges::IsOverlapping(int64_t from, int64_t to) const {
  int low = 0, high = static_cast<int>(ranges_.size() - 1);
  while (low <= high) {
    const int mid = low + (high - low) / 2;
    if (ranges_[mid].to < from) {
      low = mid + 1;
    } else if (ranges_[mid].from > to) {
      high = mid - 1;
    } else {
      return true;  // Overlapping
    }
  }
  return false;  // No overlap
}

} // namespace parquet
