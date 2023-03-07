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

#include <atomic>
#include <cstdint>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/mutex.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/type_fwd.h"

#if defined(__clang__) || defined(__GNUC__)
#define BYTESWAP(x) __builtin_bswap64(x)
#define ROTL(x, n) (((x) << (n)) | ((x) >> ((-n) & 31)))
#define ROTL64(x, n) (((x) << (n)) | ((x) >> ((-n) & 63)))
#define PREFETCH(ptr) __builtin_prefetch((ptr), 0 /* rw==read */, 3 /* locality */)
#elif defined(_MSC_VER)
#include <intrin.h>
#define BYTESWAP(x) _byteswap_uint64(x)
#define ROTL(x, n) _rotl((x), (n))
#define ROTL64(x, n) _rotl64((x), (n))
#if defined(_M_X64) || defined(_M_I86)
#include <mmintrin.h>  // https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx
#define PREFETCH(ptr) _mm_prefetch((const char*)(ptr), _MM_HINT_T0)
#else
#define PREFETCH(ptr) (void)(ptr) /* disabled */
#endif
#endif

namespace arrow {
namespace util {

template<typename T>
inline void CheckAlignment(const void *ptr) {
      ARROW_DCHECK(reinterpret_cast<uint64_t>(ptr) % sizeof(T) == 0);
}

/// Storage used to allocate temporary vectors of a batch size.
/// Temporary vectors should resemble allocating temporary variables on the stack
/// but in the context of vectorized processing where we need to store a vector of
/// temporaries instead of a single value.
class TempVectorStack {
  template<typename>
  friend
  class TempVectorHolder;

 public:
  Status Init(MemoryPool *pool, int64_t size) {
    num_vectors_ = 0;
    top_ = 0;
    buffer_size_ = PaddedAllocationSize(size) + kPadding + 2 * sizeof(uint64_t);
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(size, pool));
    // Ensure later operations don't accidentally read uninitialized memory.
    std::memset(buffer->mutable_data(), 0xFF, size);
    buffer_ = std::move(buffer);
    return Status::OK();
  }

 private:
  int64_t PaddedAllocationSize(int64_t num_bytes) {
    // Round up allocation size to multiple of 8 bytes
    // to avoid returning temp vectors with unaligned address.
    //
    // Also add padding at the end to facilitate loads and stores
    // using SIMD when number of vector elements is not divisible
    // by the number of SIMD lanes.
    //
    return ::arrow::bit_util::RoundUp(num_bytes, sizeof(int64_t)) + kPadding;
  }
  void alloc(uint32_t num_bytes, uint8_t **data, int *id) {
    int64_t old_top = top_;
    top_ += PaddedAllocationSize(num_bytes) + 2 * sizeof(uint64_t);
    // Stack overflow check
        ARROW_DCHECK(top_ <= buffer_size_);
    *data = buffer_->mutable_data() + old_top + sizeof(uint64_t);
    // We set 8 bytes before the beginning of the allocated range and
    // 8 bytes after the end to check for stack overflow (which would
    // result in those known bytes being corrupted).
    reinterpret_cast<uint64_t *>(buffer_->mutable_data() + old_top)[0] = kGuard1;
    reinterpret_cast<uint64_t *>(buffer_->mutable_data() + top_)[-1] = kGuard2;
    *id = num_vectors_++;
  }
  void release(int id, uint32_t num_bytes) {
        ARROW_DCHECK(num_vectors_ == id + 1);
    int64_t size = PaddedAllocationSize(num_bytes) + 2 * sizeof(uint64_t);
        ARROW_DCHECK(reinterpret_cast<const uint64_t *>(buffer_->mutable_data() + top_)[-1] ==
        kGuard2);
        ARROW_DCHECK(top_ >= size);
    top_ -= size;
        ARROW_DCHECK(reinterpret_cast<const uint64_t *>(buffer_->mutable_data() + top_)[0] ==
        kGuard1);
    --num_vectors_;
  }
  static constexpr uint64_t kGuard1 = 0x3141592653589793ULL;
  static constexpr uint64_t kGuard2 = 0x0577215664901532ULL;
  static constexpr int64_t kPadding = 64;
  int num_vectors_;
  int64_t top_;
  std::unique_ptr<Buffer> buffer_;
  int64_t buffer_size_;
};

}
}