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

#ifndef PARQUET_UTIL_MEM_POOL_H
#define PARQUET_UTIL_MEM_POOL_H

#include <stdio.h>
#include <algorithm>
#include <cstdint>
#include <vector>
#include <string>

#include "parquet/util/logging.h"
#include "parquet/util/bit-util.h"

namespace parquet_cpp {

/// A MemPool maintains a list of memory chunks from which it allocates memory in
/// response to Allocate() calls;
/// Chunks stay around for the lifetime of the mempool or until they are passed on to
/// another mempool.
//
/// An Allocate() call will attempt to allocate memory from the chunk that was most
/// recently added; if that chunk doesn't have enough memory to
/// satisfy the allocation request, the free chunks are searched for one that is
/// big enough otherwise a new chunk is added to the list.
/// The current_chunk_idx_ always points to the last chunk with allocated memory.
/// In order to keep allocation overhead low, chunk sizes double with each new one
/// added, until they hit a maximum size.
//
///     Example:
///     MemPool* p = new MemPool();
///     for (int i = 0; i < 1024; ++i) {
/// returns 8-byte aligned memory (effectively 24 bytes):
///       .. = p->Allocate(17);
///     }
/// at this point, 17K have been handed out in response to Allocate() calls and
/// 28K of chunks have been allocated (chunk sizes: 4K, 8K, 16K)
/// We track total and peak allocated bytes. At this point they would be the same:
/// 28k bytes.  A call to Clear will return the allocated memory so
/// total_allocate_bytes_
/// becomes 0 while peak_allocate_bytes_ remains at 28k.
///     p->Clear();
/// the entire 1st chunk is returned:
///     .. = p->Allocate(4 * 1024);
/// 4K of the 2nd chunk are returned:
///     .. = p->Allocate(4 * 1024);
/// a new 20K chunk is created
///     .. = p->Allocate(20 * 1024);
//
///      MemPool* p2 = new MemPool();
/// the new mempool receives all chunks containing data from p
///      p2->AcquireData(p, false);
/// At this point p.total_allocated_bytes_ would be 0 while p.peak_allocated_bytes_
/// remains unchanged.
/// The one remaining (empty) chunk is released:
///    delete p;

class MemPool {
 public:
  MemPool();

  /// Frees all chunks of memory and subtracts the total allocated bytes
  /// from the registered limits.
  ~MemPool();

  /// Allocates 8-byte aligned section of memory of 'size' bytes at the end
  /// of the the current chunk. Creates a new chunk if there aren't any chunks
  /// with enough capacity.
  uint8_t* Allocate(int size) {
    return Allocate<false>(size);
  }

  /// Returns 'byte_size' to the current chunk back to the mem pool. This can
  /// only be used to return either all or part of the previous allocation returned
  /// by Allocate().
  void ReturnPartialAllocation(int byte_size) {
    DCHECK_GE(byte_size, 0);
    DCHECK(current_chunk_idx_ != -1);
    ChunkInfo& info = chunks_[current_chunk_idx_];
    DCHECK_GE(info.allocated_bytes, byte_size);
    info.allocated_bytes -= byte_size;
    total_allocated_bytes_ -= byte_size;
  }

  /// Makes all allocated chunks available for re-use, but doesn't delete any chunks.
  void Clear();

  /// Deletes all allocated chunks. FreeAll() or AcquireData() must be called for
  /// each mem pool
  void FreeAll();

  /// Absorb all chunks that hold data from src. If keep_current is true, let src hold on
  /// to its last allocated chunk that contains data.
  /// All offsets handed out by calls to GetCurrentOffset() for 'src' become invalid.
  void AcquireData(MemPool* src, bool keep_current);

  std::string DebugString();

  int64_t total_allocated_bytes() const { return total_allocated_bytes_; }
  int64_t peak_allocated_bytes() const { return peak_allocated_bytes_; }
  int64_t total_reserved_bytes() const { return total_reserved_bytes_; }

  /// Return sum of chunk_sizes_.
  int64_t GetTotalChunkSizes() const;

 private:
  friend class MemPoolTest;
  static const int INITIAL_CHUNK_SIZE = 4 * 1024;

  /// The maximum size of chunk that should be allocated. Allocations larger than this
  /// size will get their own individual chunk.
  static const int MAX_CHUNK_SIZE = 1024 * 1024;

  struct ChunkInfo {
    uint8_t* data; // Owned by the ChunkInfo.
    int64_t size;  // in bytes

    /// bytes allocated via Allocate() in this chunk
    int64_t allocated_bytes;

    explicit ChunkInfo(int64_t size, uint8_t* buf);

    ChunkInfo()
      : data(NULL),
        size(0),
        allocated_bytes(0) {}
  };

  /// chunk from which we served the last Allocate() call;
  /// always points to the last chunk that contains allocated data;
  /// chunks 0..current_chunk_idx_ are guaranteed to contain data
  /// (chunks_[i].allocated_bytes > 0 for i: 0..current_chunk_idx_);
  /// -1 if no chunks present
  int current_chunk_idx_;

  /// The size of the next chunk to allocate.
  int64_t next_chunk_size_;

  /// sum of allocated_bytes_
  int64_t total_allocated_bytes_;

  /// Maximum number of bytes allocated from this pool at one time.
  int64_t peak_allocated_bytes_;

  /// sum of all bytes allocated in chunks_
  int64_t total_reserved_bytes_;

  std::vector<ChunkInfo> chunks_;

  /// Find or allocated a chunk with at least min_size spare capacity and update
  /// current_chunk_idx_. Also updates chunks_, chunk_sizes_ and allocated_bytes_
  /// if a new chunk needs to be created.
  bool FindChunk(int64_t min_size);

  /// Check integrity of the supporting data structures; always returns true but DCHECKs
  /// all invariants.
  /// If 'current_chunk_empty' is false, checks that the current chunk contains data.
  bool CheckIntegrity(bool current_chunk_empty);

  /// Return offset to unoccpied space in current chunk.
  int GetFreeOffset() const {
    if (current_chunk_idx_ == -1) return 0;
    return chunks_[current_chunk_idx_].allocated_bytes;
  }

  template <bool CHECK_LIMIT_FIRST>
  uint8_t* Allocate(int size) {
    if (size == 0) return NULL;

    int64_t num_bytes = BitUtil::RoundUp(size, 8);
    if (current_chunk_idx_ == -1
        || num_bytes + chunks_[current_chunk_idx_].allocated_bytes
          > chunks_[current_chunk_idx_].size) {
      // If we couldn't allocate a new chunk, return NULL.
      if (UNLIKELY(!FindChunk(num_bytes))) return NULL;
    }
    ChunkInfo& info = chunks_[current_chunk_idx_];
    uint8_t* result = info.data + info.allocated_bytes;
    DCHECK_LE(info.allocated_bytes + num_bytes, info.size);
    info.allocated_bytes += num_bytes;
    total_allocated_bytes_ += num_bytes;
    DCHECK_LE(current_chunk_idx_, static_cast<int>(chunks_.size()) - 1);
    peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);
    return result;
  }
};

} // namespace parquet_cpp

#endif // PARQUET_UTIL_MEM_POOL_H
