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
#include <vector>

#include "parquet/platform.h"

namespace parquet {

enum class PARQUET_EXPORT IndexKind : uint8_t {
  kColumnIndex,
  kOffsetIndex,
  kBloomFilter,
};

/// \brief Location to a Parquet index (page index or bloom filter).
struct PARQUET_EXPORT IndexLocation {
  // File offset of the given index, in bytes
  int64_t offset;
  // Length of the given index, in bytes
  int32_t length;
};

/// \brief Identifier for a column chunk.
struct PARQUET_EXPORT ColumnChunkId {
  int32_t row_group_index;
  int32_t column_index;
};

using IndexLocations = std::vector<std::pair<ColumnChunkId, IndexLocation>>;

}  // namespace parquet
