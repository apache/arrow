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

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

#if defined(__clang__) || defined(__GNUC__)
#define BYTESWAP(x) __builtin_bswap64(x)
#define ROTL(x, n) (((x) << (n)) | ((x) >> (32 - (n))))
#elif defined(_MSC_VER)
#include <intrin.h>
#define BYTESWAP(x) _byteswap_uint64(x)
#define ROTL(x, n) _rotl((x), (n))
#endif

namespace arrow {
namespace util {

// Some platforms typedef int64_t as long int instead of long long int,
// which breaks the _mm256_i64gather_epi64 and _mm256_i32gather_epi64 intrinsics
// which need long long.
// We use the cast to the type below in these intrinsics to make the code
// compile in all cases.
//
using int64_for_gather_t = const long long int;  // NOLINT runtime-int

/// Storage used to allocate temporary vectors of a batch size.
/// Temporary vectors should resemble allocating temporary variables on the stack
/// but in the context of vectorized processing where we need to store a vector of
/// temporaries instead of a single value.
class TempVectorStack {
  template <typename>
  friend class TempVectorHolder;

 public:
  Status Init(MemoryPool* pool, int64_t size) {
    num_vectors_ = 0;
    top_ = 0;
    buffer_size_ = size;
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(size, pool));
    buffer_ = std::move(buffer);
    return Status::OK();
  }

 private:
  void alloc(uint32_t num_bytes, uint8_t** data, int* id) {
    int64_t old_top = top_;
    top_ += num_bytes + padding;
    // Stack overflow check
    ARROW_DCHECK(top_ <= buffer_size_);
    *data = buffer_->mutable_data() + old_top;
    *id = num_vectors_++;
  }
  void release(int id, uint32_t num_bytes) {
    ARROW_DCHECK(num_vectors_ == id + 1);
    int64_t size = num_bytes + padding;
    ARROW_DCHECK(top_ >= size);
    top_ -= size;
    --num_vectors_;
  }
  static constexpr int64_t padding = 64;
  int num_vectors_;
  int64_t top_;
  std::unique_ptr<Buffer> buffer_;
  int64_t buffer_size_;
};

template <typename T>
class TempVectorHolder {
  friend class TempVectorStack;

 public:
  ~TempVectorHolder() { stack_->release(id_, num_elements_ * sizeof(T)); }
  T* mutable_data() { return reinterpret_cast<T*>(data_); }
  TempVectorHolder(TempVectorStack* stack, uint32_t num_elements) {
    stack_ = stack;
    num_elements_ = num_elements;
    stack_->alloc(num_elements * sizeof(T), &data_, &id_);
  }

 private:
  TempVectorStack* stack_;
  uint8_t* data_;
  int id_;
  uint32_t num_elements_;
};

class BitUtil {
 public:
  static void bits_to_indexes(int bit_to_search, int64_t hardware_flags,
                              const int num_bits, const uint8_t* bits, int* num_indexes,
                              uint16_t* indexes, int bit_offset = 0);

  static void bits_filter_indexes(int bit_to_search, int64_t hardware_flags,
                                  const int num_bits, const uint8_t* bits,
                                  const uint16_t* input_indexes, int* num_indexes,
                                  uint16_t* indexes, int bit_offset = 0);

  // Input and output indexes may be pointing to the same data (in-place filtering).
  static void bits_split_indexes(int64_t hardware_flags, const int num_bits,
                                 const uint8_t* bits, int* num_indexes_bit0,
                                 uint16_t* indexes_bit0, uint16_t* indexes_bit1,
                                 int bit_offset = 0);

  // Bit 1 is replaced with byte 0xFF.
  static void bits_to_bytes(int64_t hardware_flags, const int num_bits,
                            const uint8_t* bits, uint8_t* bytes, int bit_offset = 0);

  // Return highest bit of each byte.
  static void bytes_to_bits(int64_t hardware_flags, const int num_bits,
                            const uint8_t* bytes, uint8_t* bits, int bit_offset = 0);

  static bool are_all_bytes_zero(int64_t hardware_flags, const uint8_t* bytes,
                                 uint32_t num_bytes);

 private:
  inline static void bits_to_indexes_helper(uint64_t word, uint16_t base_index,
                                            int* num_indexes, uint16_t* indexes);
  inline static void bits_filter_indexes_helper(uint64_t word,
                                                const uint16_t* input_indexes,
                                                int* num_indexes, uint16_t* indexes);
  template <int bit_to_search, bool filter_input_indexes>
  static void bits_to_indexes_internal(int64_t hardware_flags, const int num_bits,
                                       const uint8_t* bits, const uint16_t* input_indexes,
                                       int* num_indexes, uint16_t* indexes);

#if defined(ARROW_HAVE_AVX2)
  static void bits_to_indexes_avx2(int bit_to_search, const int num_bits,
                                   const uint8_t* bits, int* num_indexes,
                                   uint16_t* indexes);
  static void bits_filter_indexes_avx2(int bit_to_search, const int num_bits,
                                       const uint8_t* bits, const uint16_t* input_indexes,
                                       int* num_indexes, uint16_t* indexes);
  template <int bit_to_search>
  static void bits_to_indexes_imp_avx2(const int num_bits, const uint8_t* bits,
                                       int* num_indexes, uint16_t* indexes);
  template <int bit_to_search>
  static void bits_filter_indexes_imp_avx2(const int num_bits, const uint8_t* bits,
                                           const uint16_t* input_indexes,
                                           int* num_indexes, uint16_t* indexes);
  static void bits_to_bytes_avx2(const int num_bits, const uint8_t* bits, uint8_t* bytes);
  static void bytes_to_bits_avx2(const int num_bits, const uint8_t* bytes, uint8_t* bits);
  static bool are_all_bytes_zero_avx2(const uint8_t* bytes, uint32_t num_bytes);
#endif
};

}  // namespace util
}  // namespace arrow
