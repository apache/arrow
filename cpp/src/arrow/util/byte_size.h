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

/// \brief The sum of bytes in each buffer referenced by the array
///
/// Note: An array may only reference a portion of a buffer.
///       This method will overestimate in this case and return the
///       byte size of the entire buffer.
/// Note: If a buffer is referenced multiple times then it will
///       only be counted once.
int64_t ARROW_EXPORT TotalBufferSize(const ArrayData& array_data);
/// \brief The sum of bytes in each buffer referenced by the array
/// Note: The caveats on the ArrayData overload apply here as well
int64_t ARROW_EXPORT TotalBufferSize(const Array& array);
/// \brief The sum of bytes in each buffer referenced by the array
/// Note: The caveats on the ArrayData overload apply here as well
int64_t ARROW_EXPORT TotalBufferSize(const ChunkedArray& chunked_array);
/// \brief The sum of bytes in each buffer referenced by the batch
/// Note: The caveats on the ArrayData overload apply here as well
int64_t ARROW_EXPORT TotalBufferSize(const RecordBatch& record_batch);
/// \brief The sum of bytes in each buffer referenced by the table
/// Note: The caveats on the ArrayData overload apply here as well
int64_t ARROW_EXPORT TotalBufferSize(const Table& table);

}  // namespace util

}  // namespace arrow
