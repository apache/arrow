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

#include <memory>

#include "arrow/array/data.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace array {

/// \brief Truncate the buffers of an ArrayData to only include the bytes
/// required to represent the slice indicated by (offset, length).
///
/// This function walks the ArrayData tree recursively and, for each node,
/// replaces over-sized buffers with sliced (or copied, when bit-offset
/// conversion is required) equivalents.  The ArrayData itself is not mutated;
/// a new ArrayData is returned that shares no oversized backing memory with
/// the input.
///
/// Uses include:
///   - Pickle serialization of sliced Array / ChunkedArray objects (GH-26685)
///   - Any other path that would otherwise serialize the full backing buffer
///     of a sliced array
///
/// The logic is adapted from the IPC writer (RecordBatchSerializer::Visit)
/// and handles validity bitmaps, fixed-width data buffers, variable-width
/// offsets + value buffers, list offsets + child arrays, struct children,
/// union type codes + offsets + children, dictionary indices, and run-end
/// encoded arrays.
///
/// Non-CPU device buffers are returned unmodified (we cannot safely slice
/// generic device memory from this utility).
ARROW_EXPORT
Result<std::shared_ptr<ArrayData>> TruncateBuffers(
    std::shared_ptr<ArrayData> data,
    MemoryPool* pool = default_memory_pool());

}  // namespace array
}  // namespace arrow
