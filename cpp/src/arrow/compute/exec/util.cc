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

#include "arrow/compute/exec/util.h"

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/table.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/tracing_internal.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using bit_util::CountTrailingZeros;

namespace util {

inline uint64_t bit_util::SafeLoadUpTo8Bytes(const uint8_t* bytes, int num_bytes) {
  // This will not be correct on big-endian architectures.
#if !ARROW_LITTLE_ENDIAN
  ARROW_DCHECK(false);
#endif
  ARROW_DCHECK(num_bytes >= 0 && num_bytes <= 8);
  if (num_bytes == 8) {
    return util::SafeLoad(reinterpret_cast<const uint64_t*>(bytes));
  } else {
    uint64_t word = 0;
    for (int i = 0; i < num_bytes; ++i) {
      word |= static_cast<uint64_t>(bytes[i]) << (8 * i);
    }
    return word;
  }
}

inline void bit_util::SafeStoreUpTo8Bytes(uint8_t* bytes, int num_bytes, uint64_t value) {
  // This will not be correct on big-endian architectures.
#if !ARROW_LITTLE_ENDIAN
  ARROW_DCHECK(false);
#endif
  ARROW_DCHECK(num_bytes >= 0 && num_bytes <= 8);
  if (num_bytes == 8) {
    util::SafeStore(reinterpret_cast<uint64_t*>(bytes), value);
  } else {
    for (int i = 0; i < num_bytes; ++i) {
      bytes[i] = static_cast<uint8_t>(value >> (8 * i));
    }
  }
}

inline void bit_util::bits_to_indexes_helper(uint64_t word, uint16_t base_index,
                                             int* num_indexes, uint16_t* indexes) {
  int n = *num_indexes;
  while (word) {
    indexes[n++] = base_index + static_cast<uint16_t>(CountTrailingZeros(word));
    word &= word - 1;
  }
  *num_indexes = n;
}

inline void bit_util::bits_filter_indexes_helper(uint64_t word,
                                                 const uint16_t* input_indexes,
                                                 int* num_indexes, uint16_t* indexes) {
  int n = *num_indexes;
  while (word) {
    indexes[n++] = input_indexes[CountTrailingZeros(word)];
    word &= word - 1;
  }
  *num_indexes = n;
}

template <int bit_to_search, bool filter_input_indexes>
void bit_util::bits_to_indexes_internal(int64_t hardware_flags, const int num_bits,
                                        const uint8_t* bits,
                                        const uint16_t* input_indexes, int* num_indexes,
                                        uint16_t* indexes, uint16_t base_index) {
  // 64 bits at a time
  constexpr int unroll = 64;
  int tail = num_bits % unroll;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    if (filter_input_indexes) {
      bits_filter_indexes_avx2(bit_to_search, num_bits - tail, bits, input_indexes,
                               num_indexes, indexes);
    } else {
      bits_to_indexes_avx2(bit_to_search, num_bits - tail, bits, num_indexes, indexes,
                           base_index);
    }
  } else {
#endif
    *num_indexes = 0;
    for (int i = 0; i < num_bits / unroll; ++i) {
      uint64_t word = util::SafeLoad(&reinterpret_cast<const uint64_t*>(bits)[i]);
      if (bit_to_search == 0) {
        word = ~word;
      }
      if (filter_input_indexes) {
        bits_filter_indexes_helper(word, input_indexes + i * 64, num_indexes, indexes);
      } else {
        bits_to_indexes_helper(word, i * 64 + base_index, num_indexes, indexes);
      }
    }
#if defined(ARROW_HAVE_AVX2)
  }
#endif
  // Optionally process the last partial word with masking out bits outside range
  if (tail) {
    const uint8_t* bits_tail = bits + (num_bits - tail) / 8;
    uint64_t word = SafeLoadUpTo8Bytes(bits_tail, (tail + 7) / 8);
    if (bit_to_search == 0) {
      word = ~word;
    }
    word &= ~0ULL >> (64 - tail);
    if (filter_input_indexes) {
      bits_filter_indexes_helper(word, input_indexes + num_bits - tail, num_indexes,
                                 indexes);
    } else {
      bits_to_indexes_helper(word, num_bits - tail + base_index, num_indexes, indexes);
    }
  }
}

void bit_util::bits_to_indexes(int bit_to_search, int64_t hardware_flags, int num_bits,
                               const uint8_t* bits, int* num_indexes, uint16_t* indexes,
                               int bit_offset) {
  bits += bit_offset / 8;
  bit_offset %= 8;
  *num_indexes = 0;
  uint16_t base_index = 0;
  if (bit_offset != 0) {
    uint64_t bits_head = bits[0] >> bit_offset;
    int bits_in_first_byte = std::min(num_bits, 8 - bit_offset);
    bits_to_indexes(bit_to_search, hardware_flags, bits_in_first_byte,
                    reinterpret_cast<const uint8_t*>(&bits_head), num_indexes, indexes);
    if (num_bits <= bits_in_first_byte) {
      return;
    }
    num_bits -= bits_in_first_byte;
    indexes += *num_indexes;
    bits += 1;
    base_index = bits_in_first_byte;
  }

  int num_indexes_new = 0;
  if (bit_to_search == 0) {
    bits_to_indexes_internal<0, false>(hardware_flags, num_bits, bits, nullptr,
                                       &num_indexes_new, indexes, base_index);
  } else {
    ARROW_DCHECK(bit_to_search == 1);
    bits_to_indexes_internal<1, false>(hardware_flags, num_bits, bits, nullptr,
                                       &num_indexes_new, indexes, base_index);
  }
  *num_indexes += num_indexes_new;
}

void bit_util::bits_filter_indexes(int bit_to_search, int64_t hardware_flags,
                                   const int num_bits, const uint8_t* bits,
                                   const uint16_t* input_indexes, int* num_indexes,
                                   uint16_t* indexes, int bit_offset) {
  bits += bit_offset / 8;
  bit_offset %= 8;
  if (bit_offset != 0) {
    int num_indexes_head = 0;
    uint64_t bits_head = bits[0] >> bit_offset;
    int bits_in_first_byte = std::min(num_bits, 8 - bit_offset);
    bits_filter_indexes(bit_to_search, hardware_flags, bits_in_first_byte,
                        reinterpret_cast<const uint8_t*>(&bits_head), input_indexes,
                        &num_indexes_head, indexes);
    int num_indexes_tail = 0;
    if (num_bits > bits_in_first_byte) {
      bits_filter_indexes(bit_to_search, hardware_flags, num_bits - bits_in_first_byte,
                          bits + 1, input_indexes + bits_in_first_byte, &num_indexes_tail,
                          indexes + num_indexes_head);
    }
    *num_indexes = num_indexes_head + num_indexes_tail;
    return;
  }

  if (bit_to_search == 0) {
    bits_to_indexes_internal<0, true>(hardware_flags, num_bits, bits, input_indexes,
                                      num_indexes, indexes);
  } else {
    ARROW_DCHECK(bit_to_search == 1);
    bits_to_indexes_internal<1, true>(hardware_flags, num_bits, bits, input_indexes,
                                      num_indexes, indexes);
  }
}

void bit_util::bits_split_indexes(int64_t hardware_flags, const int num_bits,
                                  const uint8_t* bits, int* num_indexes_bit0,
                                  uint16_t* indexes_bit0, uint16_t* indexes_bit1,
                                  int bit_offset) {
  bits_to_indexes(0, hardware_flags, num_bits, bits, num_indexes_bit0, indexes_bit0,
                  bit_offset);
  int num_indexes_bit1;
  bits_to_indexes(1, hardware_flags, num_bits, bits, &num_indexes_bit1, indexes_bit1,
                  bit_offset);
}

void bit_util::bits_to_bytes(int64_t hardware_flags, const int num_bits,
                             const uint8_t* bits, uint8_t* bytes, int bit_offset) {
  bits += bit_offset / 8;
  bit_offset %= 8;
  if (bit_offset != 0) {
    uint64_t bits_head = bits[0] >> bit_offset;
    int bits_in_first_byte = std::min(num_bits, 8 - bit_offset);
    bits_to_bytes(hardware_flags, bits_in_first_byte,
                  reinterpret_cast<const uint8_t*>(&bits_head), bytes);
    if (num_bits > bits_in_first_byte) {
      bits_to_bytes(hardware_flags, num_bits - bits_in_first_byte, bits + 1,
                    bytes + bits_in_first_byte);
    }
    return;
  }

  int num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    // The function call below processes whole 32 bit chunks together.
    num_processed = num_bits - (num_bits % 32);
    bits_to_bytes_avx2(num_processed, bits, bytes);
  }
#endif
  // Processing 8 bits at a time
  constexpr int unroll = 8;
  for (int i = num_processed / unroll; i < num_bits / unroll; ++i) {
    uint8_t bits_next = bits[i];
    // Clear the lowest bit and then make 8 copies of remaining 7 bits, each 7 bits apart
    // from the previous.
    uint64_t unpacked = static_cast<uint64_t>(bits_next & 0xfe) *
                        ((1ULL << 7) | (1ULL << 14) | (1ULL << 21) | (1ULL << 28) |
                         (1ULL << 35) | (1ULL << 42) | (1ULL << 49));
    unpacked |= (bits_next & 1);
    unpacked &= 0x0101010101010101ULL;
    unpacked *= 255;
    util::SafeStore(&reinterpret_cast<uint64_t*>(bytes)[i], unpacked);
  }
  int tail = num_bits % unroll;
  if (tail) {
    uint8_t bits_next = bits[(num_bits - tail) / unroll];
    // Clear the lowest bit and then make 8 copies of remaining 7 bits, each 7 bits apart
    // from the previous.
    uint64_t unpacked = static_cast<uint64_t>(bits_next & 0xfe) *
                        ((1ULL << 7) | (1ULL << 14) | (1ULL << 21) | (1ULL << 28) |
                         (1ULL << 35) | (1ULL << 42) | (1ULL << 49));
    unpacked |= (bits_next & 1);
    unpacked &= 0x0101010101010101ULL;
    unpacked *= 255;
    SafeStoreUpTo8Bytes(bytes + num_bits - tail, tail, unpacked);
  }
}

void bit_util::bytes_to_bits(int64_t hardware_flags, const int num_bits,
                             const uint8_t* bytes, uint8_t* bits, int bit_offset) {
  bits += bit_offset / 8;
  bit_offset %= 8;
  if (bit_offset != 0) {
    uint64_t bits_head;
    int bits_in_first_byte = std::min(num_bits, 8 - bit_offset);
    bytes_to_bits(hardware_flags, bits_in_first_byte, bytes,
                  reinterpret_cast<uint8_t*>(&bits_head));
    uint8_t mask = (1 << bit_offset) - 1;
    *bits = static_cast<uint8_t>((*bits & mask) | (bits_head << bit_offset));

    if (num_bits > bits_in_first_byte) {
      bytes_to_bits(hardware_flags, num_bits - bits_in_first_byte,
                    bytes + bits_in_first_byte, bits + 1);
    }
    return;
  }

  int num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    // The function call below processes whole 32 bit chunks together.
    num_processed = num_bits - (num_bits % 32);
    bytes_to_bits_avx2(num_processed, bytes, bits);
  }
#endif
  // Process 8 bits at a time
  constexpr int unroll = 8;
  for (int i = num_processed / unroll; i < num_bits / unroll; ++i) {
    uint64_t bytes_next = util::SafeLoad(&reinterpret_cast<const uint64_t*>(bytes)[i]);
    bytes_next &= 0x0101010101010101ULL;
    bytes_next |= (bytes_next >> 7);  // Pairs of adjacent output bits in individual bytes
    bytes_next |= (bytes_next >> 14);  // 4 adjacent output bits in individual bytes
    bytes_next |= (bytes_next >> 28);  // All 8 output bits in the lowest byte
    bits[i] = static_cast<uint8_t>(bytes_next & 0xff);
  }
  int tail = num_bits % unroll;
  if (tail) {
    uint64_t bytes_next = SafeLoadUpTo8Bytes(bytes + num_bits - tail, tail);
    bytes_next &= 0x0101010101010101ULL;
    bytes_next |= (bytes_next >> 7);  // Pairs of adjacent output bits in individual bytes
    bytes_next |= (bytes_next >> 14);  // 4 adjacent output bits in individual bytes
    bytes_next |= (bytes_next >> 28);  // All 8 output bits in the lowest byte
    bits[num_bits / 8] = static_cast<uint8_t>(bytes_next & 0xff);
  }
}

bool bit_util::are_all_bytes_zero(int64_t hardware_flags, const uint8_t* bytes,
                                  uint32_t num_bytes) {
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    return are_all_bytes_zero_avx2(bytes, num_bytes);
  }
#endif
  uint64_t result_or = 0;
  uint32_t i;
  for (i = 0; i < num_bytes / 8; ++i) {
    uint64_t x = util::SafeLoad(&reinterpret_cast<const uint64_t*>(bytes)[i]);
    result_or |= x;
  }
  if (num_bytes % 8 > 0) {
    uint64_t tail = 0;
    result_or |= memcmp(bytes + i * 8, &tail, num_bytes % 8);
  }
  return result_or == 0;
}

}  // namespace util

namespace compute {

Status ValidateExecNodeInputs(ExecPlan* plan, const std::vector<ExecNode*>& inputs,
                              int expected_num_inputs, const char* kind_name) {
  if (static_cast<int>(inputs.size()) != expected_num_inputs) {
    return Status::Invalid(kind_name, " requires ", expected_num_inputs,
                           " inputs but got ", inputs.size());
  }

  for (auto input : inputs) {
    if (input->plan() != plan) {
      return Status::Invalid("Constructing a ", kind_name,
                             " node in a different plan from its input");
    }
  }

  return Status::OK();
}

Result<std::shared_ptr<Table>> TableFromExecBatches(
    const std::shared_ptr<Schema>& schema, const std::vector<ExecBatch>& exec_batches) {
  RecordBatchVector batches;
  for (const auto& batch : exec_batches) {
    ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema));
    batches.push_back(std::move(rb));
  }
  return Table::FromRecordBatches(schema, batches);
}

size_t ThreadIndexer::operator()() {
  auto id = std::this_thread::get_id();

  auto guard = mutex_.Lock();  // acquire the lock
  const auto& id_index = *id_to_index_.emplace(id, id_to_index_.size()).first;

  return Check(id_index.second);
}

size_t ThreadIndexer::Capacity() {
  static size_t max_size = GetCpuThreadPoolCapacity() + io::GetIOThreadPoolCapacity() + 1;
  return max_size;
}

size_t ThreadIndexer::Check(size_t thread_index) {
  DCHECK_LT(thread_index, Capacity())
      << "thread index " << thread_index << " is out of range [0, " << Capacity() << ")";

  return thread_index;
}

Status TableSinkNodeConsumer::Init(const std::shared_ptr<Schema>& schema,
                                   BackpressureControl* backpressure_control,
                                   ExecPlan* plan) {
  // If the user is collecting into a table then backpressure is meaningless
  ARROW_UNUSED(backpressure_control);
  schema_ = schema;
  return Status::OK();
}

Status TableSinkNodeConsumer::Consume(ExecBatch batch) {
  auto guard = consume_mutex_.Lock();
  ARROW_ASSIGN_OR_RAISE(auto rb, batch.ToRecordBatch(schema_, pool_));
  batches_.push_back(std::move(rb));
  return Status::OK();
}

Future<> TableSinkNodeConsumer::Finish() {
  ARROW_ASSIGN_OR_RAISE(*out_, Table::FromRecordBatches(schema_, batches_));
  return Status::OK();
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceStartProducing(
    std::string extra_details) const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(
      span, node_kind + "::StartProducing",
      {{"node.details", extra_details}, {"node.label", node_->label()}});
}

void TracedNode::NoteStartProducing(std::string extra_details) const {
  std::string node_kind(node_->kind_name());
  EVENT_ON_CURRENT_SPAN(node_kind + "::StartProducing", {{"node.details", extra_details},
                                                         {"node.label", node_->label()}});
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceInputReceived(
    const ExecBatch& batch) const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(
      span, node_kind + "::InputReceived",
      {{"node.label", node_->label()}, {"node.batch_length", batch.length}});
}

void TracedNode::NoteInputReceived(const ExecBatch& batch) const {
  std::string node_kind(node_->kind_name());
  EVENT_ON_CURRENT_SPAN(
      node_kind + "::InputReceived",
      {{"node.label", node_->label()}, {"node.batch_length", batch.length}});
}

[[nodiscard]] ::arrow::internal::tracing::Scope TracedNode::TraceFinish() const {
  std::string node_kind(node_->kind_name());
  util::tracing::Span span;
  return START_SCOPED_SPAN(span, node_kind + "::Finish",
                           {{"node.label", node_->label()}});
}

}  // namespace compute
}  // namespace arrow
