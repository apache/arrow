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

#include "arrow/compute/kernels/chunked_internal.h"

#include <algorithm>

#include "arrow/record_batch.h"
#include "arrow/util/logging.h"

namespace arrow::compute::internal {

std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays) {
  std::vector<const Array*> pointers(arrays.size());
  std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                 [&](const std::shared_ptr<Array>& array) { return array.get(); });
  return pointers;
}

std::vector<int64_t> ChunkedIndexMapper::GetChunkLengths(
    util::span<const Array* const> chunks) {
  std::vector<int64_t> chunk_lengths(chunks.size());
  for (int64_t i = 0; i < static_cast<int64_t>(chunks.size()); ++i) {
    chunk_lengths[i] = chunks[i]->length();
  }
  return chunk_lengths;
}

std::vector<int64_t> ChunkedIndexMapper::GetChunkLengths(
    const RecordBatchVector& chunks) {
  std::vector<int64_t> chunk_lengths(chunks.size());
  for (int64_t i = 0; i < static_cast<int64_t>(chunks.size()); ++i) {
    chunk_lengths[i] = chunks[i]->num_rows();
  }
  return chunk_lengths;
}

Result<std::pair<CompressedChunkLocation*, CompressedChunkLocation*>>
ChunkedIndexMapper::LogicalToPhysical() {
  // Check that indices would fall in bounds for CompressedChunkLocation
  if (ARROW_PREDICT_FALSE(chunk_lengths_.size() >
                          CompressedChunkLocation::kMaxChunkIndex + 1)) {
    return Status::NotImplemented("Chunked array has more than ",
                                  CompressedChunkLocation::kMaxChunkIndex + 1, " chunks");
  }
  for (int64_t chunk_length : chunk_lengths_) {
    if (ARROW_PREDICT_FALSE(static_cast<uint64_t>(chunk_length) >
                            CompressedChunkLocation::kMaxIndexInChunk + 1)) {
      return Status::NotImplemented("Individual chunk in chunked array has more than ",
                                    CompressedChunkLocation::kMaxIndexInChunk + 1,
                                    " elements");
    }
  }

  const int64_t num_indices = static_cast<int64_t>(indices_end_ - indices_begin_);
  DCHECK_EQ(num_indices, std::accumulate(chunk_lengths_.begin(), chunk_lengths_.end(),
                                         static_cast<int64_t>(0)));
  CompressedChunkLocation* physical_begin =
      reinterpret_cast<CompressedChunkLocation*>(indices_begin_);
  DCHECK_EQ(physical_begin + num_indices,
            reinterpret_cast<CompressedChunkLocation*>(indices_end_));

  int64_t chunk_offset = 0;
  for (int64_t chunk_index = 0; chunk_index < static_cast<int64_t>(chunk_lengths_.size());
       ++chunk_index) {
    const int64_t chunk_length = chunk_lengths_[chunk_index];
    for (int64_t i = 0; i < chunk_length; ++i) {
      // Logical indices are expected to be chunk-partitioned, which avoids costly
      // chunked index resolution.
      DCHECK_GE(indices_begin_[chunk_offset + i], static_cast<uint64_t>(chunk_offset));
      DCHECK_LT(indices_begin_[chunk_offset + i],
                static_cast<uint64_t>(chunk_offset + chunk_length));
      physical_begin[chunk_offset + i] = CompressedChunkLocation{
          static_cast<uint64_t>(chunk_index),
          indices_begin_[chunk_offset + i] - static_cast<uint64_t>(chunk_offset)};
    }
    chunk_offset += chunk_length;
  }

  return std::pair{physical_begin, physical_begin + num_indices};
}

Status ChunkedIndexMapper::PhysicalToLogical() {
  std::vector<int64_t> chunk_offsets(chunk_lengths_.size());
  {
    int64_t offset = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(chunk_lengths_.size()); ++i) {
      chunk_offsets[i] = offset;
      offset += chunk_lengths_[i];
    }
  }

  const int64_t num_indices = static_cast<int64_t>(indices_end_ - indices_begin_);
  CompressedChunkLocation* physical_begin =
      reinterpret_cast<CompressedChunkLocation*>(indices_begin_);
  for (int64_t i = 0; i < num_indices; ++i) {
    const auto loc = physical_begin[i];
    DCHECK_LT(loc.chunk_index(), chunk_offsets.size());
    DCHECK_LT(loc.index_in_chunk(),
              static_cast<uint64_t>(chunk_lengths_[loc.chunk_index()]));
    indices_begin_[i] =
        chunk_offsets[loc.chunk_index()] + static_cast<int64_t>(loc.index_in_chunk());
  }

  return Status::OK();
}

}  // namespace arrow::compute::internal
