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
#include "arrow/util/optional.h"
#include "arrow/util/thread_pool.h"

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

template <typename T>
inline void CheckAlignment(const void* ptr) {
  ARROW_DCHECK(reinterpret_cast<uint64_t>(ptr) % sizeof(T) == 0);
}

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
  void alloc(uint32_t num_bytes, uint8_t** data, int* id) {
    int64_t old_top = top_;
    top_ += PaddedAllocationSize(num_bytes) + 2 * sizeof(uint64_t);
    // Stack overflow check
    ARROW_DCHECK(top_ <= buffer_size_);
    *data = buffer_->mutable_data() + old_top + sizeof(uint64_t);
    // We set 8 bytes before the beginning of the allocated range and
    // 8 bytes after the end to check for stack overflow (which would
    // result in those known bytes being corrupted).
    reinterpret_cast<uint64_t*>(buffer_->mutable_data() + old_top)[0] = kGuard1;
    reinterpret_cast<uint64_t*>(buffer_->mutable_data() + top_)[-1] = kGuard2;
    *id = num_vectors_++;
  }
  void release(int id, uint32_t num_bytes) {
    ARROW_DCHECK(num_vectors_ == id + 1);
    int64_t size = PaddedAllocationSize(num_bytes) + 2 * sizeof(uint64_t);
    ARROW_DCHECK(reinterpret_cast<const uint64_t*>(buffer_->mutable_data() + top_)[-1] ==
                 kGuard2);
    ARROW_DCHECK(top_ >= size);
    top_ -= size;
    ARROW_DCHECK(reinterpret_cast<const uint64_t*>(buffer_->mutable_data() + top_)[0] ==
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

class bit_util {
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
                                       int* num_indexes, uint16_t* indexes,
                                       uint16_t base_index = 0);

#if defined(ARROW_HAVE_AVX2)
  static void bits_to_indexes_avx2(int bit_to_search, const int num_bits,
                                   const uint8_t* bits, int* num_indexes,
                                   uint16_t* indexes, uint16_t base_index = 0);
  static void bits_filter_indexes_avx2(int bit_to_search, const int num_bits,
                                       const uint8_t* bits, const uint16_t* input_indexes,
                                       int* num_indexes, uint16_t* indexes);
  template <int bit_to_search>
  static void bits_to_indexes_imp_avx2(const int num_bits, const uint8_t* bits,
                                       int* num_indexes, uint16_t* indexes,
                                       uint16_t base_index = 0);
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
namespace compute {

ARROW_EXPORT
Status ValidateExecNodeInputs(ExecPlan* plan, const std::vector<ExecNode*>& inputs,
                              int expected_num_inputs, const char* kind_name);

ARROW_EXPORT
Result<std::shared_ptr<Table>> TableFromExecBatches(
    const std::shared_ptr<Schema>& schema, const std::vector<ExecBatch>& exec_batches);

class AtomicCounter {
 public:
  AtomicCounter() = default;

  int count() const { return count_.load(); }

  util::optional<int> total() const {
    int total = total_.load();
    if (total == -1) return {};
    return total;
  }

  // return true if the counter is complete
  bool Increment() {
    DCHECK_NE(count_.load(), total_.load());
    int count = count_.fetch_add(1) + 1;
    if (count != total_.load()) return false;
    return DoneOnce();
  }

  // return true if the counter is complete
  bool SetTotal(int total) {
    total_.store(total);
    if (count_.load() != total) return false;
    return DoneOnce();
  }

  // return true if the counter has not already been completed
  bool Cancel() { return DoneOnce(); }

  // return true if the counter has finished or been cancelled
  bool Completed() { return complete_.load(); }

 private:
  // ensure there is only one true return from Increment(), SetTotal(), or Cancel()
  bool DoneOnce() {
    bool expected = false;
    return complete_.compare_exchange_strong(expected, true);
  }

  std::atomic<int> count_{0}, total_{-1};
  std::atomic<bool> complete_{false};
};

class ThreadIndexer {
 public:
  size_t operator()();

  static size_t Capacity();

 private:
  static size_t Check(size_t thread_index);

  util::Mutex mutex_;
  std::unordered_map<std::thread::id, size_t> id_to_index_;
};

}  // namespace compute
}  // namespace arrow
