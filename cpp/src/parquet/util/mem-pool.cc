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

// Initially imported from Apache Impala on 2016-02-23, and has been modified
// since for parquet-cpp

#include "parquet/util/mem-pool.h"

#include <stdio.h>

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>

#include "parquet/util/bit-util.h"

namespace parquet_cpp {

const int MemPool::INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

MemPool::MemPool()
  : current_chunk_idx_(-1),
    next_chunk_size_(INITIAL_CHUNK_SIZE),
    total_allocated_bytes_(0),
    peak_allocated_bytes_(0),
    total_reserved_bytes_(0) {}

MemPool::ChunkInfo::ChunkInfo(int64_t size, uint8_t* buf)
  : data(buf),
    size(size),
    allocated_bytes(0) {
}

MemPool::~MemPool() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    free(chunks_[i].data);
  }

  DCHECK(chunks_.empty()) << "Must call FreeAll() or AcquireData() for this pool";
}

void MemPool::Clear() {
  current_chunk_idx_ = -1;
  for (auto chunk = chunks_.begin(); chunk != chunks_.end(); ++chunk) {
    chunk->allocated_bytes = 0;
  }
  total_allocated_bytes_ = 0;
  DCHECK(CheckIntegrity(false));
}

void MemPool::FreeAll() {
  int64_t total_bytes_released = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    total_bytes_released += chunks_[i].size;
    free(chunks_[i].data);
  }
  chunks_.clear();
  next_chunk_size_ = INITIAL_CHUNK_SIZE;
  current_chunk_idx_ = -1;
  total_allocated_bytes_ = 0;
  total_reserved_bytes_ = 0;
}

bool MemPool::FindChunk(int64_t min_size) {
  // Try to allocate from a free chunk. The first free chunk, if any, will be immediately
  // after the current chunk.
  int first_free_idx = current_chunk_idx_ + 1;
  // (cast size() to signed int in order to avoid everything else being cast to
  // unsigned long, in particular -1)
  while (++current_chunk_idx_  < static_cast<int>(chunks_.size())) {
    // we found a free chunk
    DCHECK_EQ(chunks_[current_chunk_idx_].allocated_bytes, 0);

    if (chunks_[current_chunk_idx_].size >= min_size) {
      // This chunk is big enough.  Move it before the other free chunks.
      if (current_chunk_idx_ != first_free_idx) {
        std::swap(chunks_[current_chunk_idx_], chunks_[first_free_idx]);
        current_chunk_idx_ = first_free_idx;
      }
      break;
    }
  }

  if (current_chunk_idx_ == static_cast<int>(chunks_.size())) {
    // need to allocate new chunk.
    int64_t chunk_size;
    DCHECK_GE(next_chunk_size_, INITIAL_CHUNK_SIZE);
    DCHECK_LE(next_chunk_size_, MAX_CHUNK_SIZE);

    chunk_size = std::max<int64_t>(min_size, next_chunk_size_);

    // Allocate a new chunk. Return early if malloc fails.
    uint8_t* buf = reinterpret_cast<uint8_t*>(malloc(chunk_size));
    if (UNLIKELY(buf == NULL)) {
      DCHECK_EQ(current_chunk_idx_, static_cast<int>(chunks_.size()));
      current_chunk_idx_ = static_cast<int>(chunks_.size()) - 1;
      return false;
    }

    // If there are no free chunks put it at the end, otherwise before the first free.
    if (first_free_idx == static_cast<int>(chunks_.size())) {
      chunks_.push_back(ChunkInfo(chunk_size, buf));
    } else {
      current_chunk_idx_ = first_free_idx;
      auto insert_chunk = chunks_.begin() + current_chunk_idx_;
      chunks_.insert(insert_chunk, ChunkInfo(chunk_size, buf));
    }
    total_reserved_bytes_ += chunk_size;
    // Don't increment the chunk size until the allocation succeeds: if an attempted
    // large allocation fails we don't want to increase the chunk size further.
    next_chunk_size_ = static_cast<int>(std::min<int64_t>(
            chunk_size * 2, MAX_CHUNK_SIZE));
  }

  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  DCHECK(CheckIntegrity(true));
  return true;
}

void MemPool::AcquireData(MemPool* src, bool keep_current) {
  DCHECK(src->CheckIntegrity(false));
  int num_acquired_chunks;
  if (keep_current) {
    num_acquired_chunks = src->current_chunk_idx_;
  } else if (src->GetFreeOffset() == 0) {
    // nothing in the last chunk
    num_acquired_chunks = src->current_chunk_idx_;
  } else {
    num_acquired_chunks = src->current_chunk_idx_ + 1;
  }

  if (num_acquired_chunks <= 0) {
    if (!keep_current) src->FreeAll();
    return;
  }

  auto end_chunk = src->chunks_.begin() + num_acquired_chunks;
  int64_t total_transfered_bytes = 0;
  for (auto i = src->chunks_.begin(); i != end_chunk; ++i) {
    total_transfered_bytes += i->size;
  }
  src->total_reserved_bytes_ -= total_transfered_bytes;
  total_reserved_bytes_ += total_transfered_bytes;

  // insert new chunks after current_chunk_idx_
  auto insert_chunk = chunks_.begin() + current_chunk_idx_ + 1;
  chunks_.insert(insert_chunk, src->chunks_.begin(), end_chunk);
  src->chunks_.erase(src->chunks_.begin(), end_chunk);
  current_chunk_idx_ += num_acquired_chunks;

  if (keep_current) {
    src->current_chunk_idx_ = 0;
    DCHECK(src->chunks_.size() == 1 || src->chunks_[1].allocated_bytes == 0);
    total_allocated_bytes_ += src->total_allocated_bytes_ - src->GetFreeOffset();
    src->total_allocated_bytes_ = src->GetFreeOffset();
  } else {
    src->current_chunk_idx_ = -1;
    total_allocated_bytes_ += src->total_allocated_bytes_;
    src->total_allocated_bytes_ = 0;
  }
  peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);

  if (!keep_current) src->FreeAll();
  DCHECK(CheckIntegrity(false));
}

std::string MemPool::DebugString() {
  std::stringstream out;
  char str[16];
  out << "MemPool(#chunks=" << chunks_.size() << " [";
  for (size_t i = 0; i < chunks_.size(); ++i) {
    sprintf(str, "0x%lx=", reinterpret_cast<size_t>(chunks_[i].data)); // NOLINT
    out << (i > 0 ? " " : "")
        << str
        << chunks_[i].size
        << "/" << chunks_[i].allocated_bytes;
  }
  out << "] current_chunk=" << current_chunk_idx_
      << " total_sizes=" << GetTotalChunkSizes()
      << " total_alloc=" << total_allocated_bytes_
      << ")";
  return out.str();
}

int64_t MemPool::GetTotalChunkSizes() const {
  int64_t result = 0;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    result += chunks_[i].size;
  }
  return result;
}

bool MemPool::CheckIntegrity(bool current_chunk_empty) {
  // check that current_chunk_idx_ points to the last chunk with allocated data
  DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));
  int64_t total_allocated = 0;
  for (int i = 0; i < static_cast<int>(chunks_.size()); ++i) {
    DCHECK_GT(chunks_[i].size, 0);
    if (i < current_chunk_idx_) {
      DCHECK_GT(chunks_[i].allocated_bytes, 0);
    } else if (i == current_chunk_idx_) {
      if (current_chunk_empty) {
        DCHECK_EQ(chunks_[i].allocated_bytes, 0);
      } else {
        DCHECK_GT(chunks_[i].allocated_bytes, 0);
      }
    } else {
      DCHECK_EQ(chunks_[i].allocated_bytes, 0);
    }
    total_allocated += chunks_[i].allocated_bytes;
  }
  DCHECK_EQ(total_allocated, total_allocated_bytes_);
  return true;
}

} // namespace parquet_cpp
