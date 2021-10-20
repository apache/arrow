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

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"

namespace arrow {

namespace util {

/// \brief Estimate the size (in bytes) of all buffers
///
/// Note: This will overestimate if there is an offset.
///       See ARROW-14356
/// Note: This will overestimate if any buffers are shared.
///       See ARROW-14357
int64_t EstimateBufferSize(const ArrayData& array_data);
/// \brief Estimate the size (in bytes) of all buffers
/// Note: The caveats on the ArrayData overload apply here as well
int64_t EstimateBufferSize(const Array& array);
/// \brief Estimate the size (in bytes) of all buffers
/// Note: The caveats on the ArrayData overload apply here as well
int64_t EstimateBufferSize(const ChunkedArray& chunked_array);
/// \brief Estimate the size (in bytes) of all buffers
/// Note: The caveats on the ArrayData overload apply here as well
int64_t EstimateBufferSize(const RecordBatch& record_batch);
/// \brief Estimate the size (in bytes) of all buffers
/// Note: The caveats on the ArrayData overload apply here as well
int64_t EstimateBufferSize(const Table& table);

}  // namespace util

}  // namespace arrow
