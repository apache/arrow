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

// Core PFOR (Patched Frame of Reference) compression implementation
//
// Adapted from the Snowflake PFOR encoder (PforEncoder.{hpp,cpp}).
// Key differences from the Snowflake implementation:
//   - Vector size: 1024 (not 2048)
//   - Max exceptions: uint16 (not uint8)
//   - Exception values: original integers (not FOR offsets)
//   - Bit packing: Arrow's BitWriter/unpack (not Snowflake's BitPacker)

#include "arrow/util/pfor/pfor.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {
namespace pfor {

// ----------------------------------------------------------------------
// FindOptimalBitWidth: histogram-based cost model

template <typename T>
BitWidthResult PforCompression<T>::FindOptimalBitWidth(const UnsignedT* deltas,
                                                       uint32_t num_elements) {
  constexpr uint8_t max_bits = PforTypeTraits<T>::kMaxBitWidth;
  constexpr uint8_t position_bits = 16;  // uint16_t for exception position
  constexpr uint8_t value_bits = sizeof(T) * 8;

  // Build histogram: histogram[b] = count of deltas requiring exactly b bits
  std::array<uint32_t, 65> histogram{};  // Support up to 64 bits
  for (uint32_t i = 0; i < num_elements; ++i) {
    uint8_t bits = PforTypeTraits<T>::BitsRequired(deltas[i]);
    histogram[bits]++;
  }

  // Evaluate each candidate bit width
  uint64_t best_cost = std::numeric_limits<uint64_t>::max();
  uint8_t best_bit_width = max_bits;
  uint16_t best_num_exceptions = 0;

  uint64_t exceptions_above = num_elements;  // All start as potential exceptions

  for (uint8_t b = 0; b <= max_bits; ++b) {
    exceptions_above -= histogram[b];

    if (exceptions_above > PforConstants::kMaxExceptions) {
      continue;
    }

    uint64_t packing_cost = static_cast<uint64_t>(num_elements) * b;
    uint64_t exception_cost = exceptions_above * (position_bits + value_bits);
    uint64_t total_cost = packing_cost + exception_cost;

    if (total_cost < best_cost) {
      best_cost = total_cost;
      best_bit_width = b;
      best_num_exceptions = static_cast<uint16_t>(exceptions_above);
    }
  }

  return {best_bit_width, best_num_exceptions};
}

// ----------------------------------------------------------------------
// EncodeVector

template <typename T>
PforEncodedVector<T> PforCompression<T>::EncodeVector(const T* values,
                                                      uint32_t num_elements) {
  ARROW_DCHECK(num_elements > 0);

  // Step 1: Find min (frame of reference)
  T min_val = values[0];
  for (uint32_t i = 1; i < num_elements; ++i) {
    if (values[i] < min_val) min_val = values[i];
  }

  // Step 2: Compute unsigned deltas
  const auto unsigned_min = static_cast<UnsignedT>(min_val);
  std::vector<UnsignedT> deltas(num_elements);
  for (uint32_t i = 0; i < num_elements; ++i) {
    deltas[i] = static_cast<UnsignedT>(values[i]) - unsigned_min;
  }

  // Step 3: Find optimal bit width
  auto [bit_width, num_exceptions] =
      FindOptimalBitWidth(deltas.data(), num_elements);

  // Step 4: Collect exceptions and replace with placeholder (0)
  PforEncodedVector<T> result;
  result.info.frame_of_reference = min_val;
  result.info.bit_width = bit_width;
  result.info.num_exceptions = num_exceptions;

  if (num_exceptions > 0) {
    result.exception_positions.reserve(num_exceptions);
    result.exception_values.reserve(num_exceptions);

    UnsignedT mask = (bit_width >= PforTypeTraits<T>::kMaxBitWidth)
                         ? static_cast<UnsignedT>(-1)
                         : (static_cast<UnsignedT>(1) << bit_width) - 1;

    for (uint32_t i = 0; i < num_elements; ++i) {
      if (deltas[i] > mask) {
        result.exception_positions.push_back(static_cast<uint16_t>(i));
        result.exception_values.push_back(values[i]);  // Store ORIGINAL value
        deltas[i] = 0;  // Placeholder
      }
    }
  }

  // Step 5: Bit-pack the deltas
  if (bit_width > 0) {
    size_t packed_size = static_cast<size_t>(
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * bit_width));
    result.packed_values.resize(packed_size, 0);

    bit_util::BitWriter writer(result.packed_values.data(),
                               static_cast<int>(packed_size));
    for (uint32_t i = 0; i < num_elements; ++i) {
      writer.PutValue(static_cast<uint64_t>(deltas[i]), bit_width);
    }
    writer.Flush();
  }

  return result;
}

// ----------------------------------------------------------------------
// DecodeVector

template <typename T>
size_t PforCompression<T>::DecodeVector(T* values, const uint8_t* data,
                                        uint32_t num_elements) {
  // Step 1: Read vector info
  auto info = PforVectorInfo<T>::Load(data);
  const uint8_t* read_ptr = data + PforVectorInfo<T>::kSerializedSize;

  // Step 2: Handle constant data (bit_width == 0, no exceptions)
  if (info.bit_width == 0 && info.num_exceptions == 0) {
    std::fill(values, values + num_elements, info.frame_of_reference);
    return PforVectorInfo<T>::kSerializedSize;
  }

  // Step 3: Unpack bit-packed deltas and add FOR
  if (info.bit_width > 0) {
    // Use SIMD-optimized unpack for batches of 32 (uint32) or 32 (uint64)
    constexpr int kBatchSize = 32;
    uint32_t full_batches = num_elements / kBatchSize;
    uint32_t remainder = num_elements % kBatchSize;

    UnsignedT* unsigned_values = reinterpret_cast<UnsignedT*>(values);
    const auto unsigned_for = static_cast<UnsignedT>(info.frame_of_reference);

    // Unpack full batches using SIMD
    for (uint32_t batch = 0; batch < full_batches; ++batch) {
      arrow::internal::unpack(read_ptr, unsigned_values + batch * kBatchSize,
                              kBatchSize, info.bit_width,
                              batch * kBatchSize * info.bit_width);
    }

    // Unpack remainder using BitReader
    if (remainder > 0) {
      size_t packed_size = static_cast<size_t>(
          bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width));
      bit_util::BitReader reader(read_ptr, static_cast<int>(packed_size));
      // Skip past the full batches
      for (uint32_t i = 0; i < full_batches * kBatchSize; ++i) {
        uint64_t val;
        reader.GetValue(info.bit_width, &val);
      }
      for (uint32_t i = full_batches * kBatchSize; i < num_elements; ++i) {
        uint64_t val;
        reader.GetValue(info.bit_width, &val);
        unsigned_values[i] = static_cast<UnsignedT>(val);
      }
    }

    // Add FOR to all values
    for (uint32_t i = 0; i < num_elements; ++i) {
      unsigned_values[i] += unsigned_for;
    }

    size_t packed_size = static_cast<size_t>(
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * info.bit_width));
    read_ptr += packed_size;
  } else {
    // bit_width == 0 but has exceptions - fill with FOR
    std::fill(values, values + num_elements, info.frame_of_reference);
  }

  // Step 4: Patch exceptions (stored as original values)
  if (info.num_exceptions > 0) {
    const uint8_t* positions_ptr = read_ptr;
    read_ptr += info.num_exceptions * sizeof(uint16_t);

    const uint8_t* values_ptr = read_ptr;
    read_ptr += info.num_exceptions * sizeof(T);

    for (uint16_t i = 0; i < info.num_exceptions; ++i) {
      uint16_t pos;
      std::memcpy(&pos, positions_ptr + i * sizeof(uint16_t), sizeof(uint16_t));

      T value;
      std::memcpy(&value, values_ptr + i * sizeof(T), sizeof(T));

      values[pos] = value;
    }
  }

  return static_cast<size_t>(read_ptr - data);
}

// ----------------------------------------------------------------------
// Serialization helpers

template <typename T>
size_t PforCompression<T>::SerializedVectorSize(const PforEncodedVector<T>& vec,
                                                uint32_t num_elements) {
  size_t size = PforVectorInfo<T>::kSerializedSize;
  if (vec.info.bit_width > 0) {
    size += static_cast<size_t>(
        bit_util::BytesForBits(static_cast<int64_t>(num_elements) * vec.info.bit_width));
  }
  size += vec.info.num_exceptions * sizeof(uint16_t);  // positions
  size += vec.info.num_exceptions * sizeof(T);          // values
  return size;
}

template <typename T>
size_t PforCompression<T>::SerializeVector(const PforEncodedVector<T>& vec,
                                           uint32_t num_elements,
                                           uint8_t* dest) {
  uint8_t* write_ptr = dest;

  // Write vector info
  vec.info.Store(write_ptr);
  write_ptr += PforVectorInfo<T>::kSerializedSize;

  // Write packed values
  if (vec.info.bit_width > 0 && !vec.packed_values.empty()) {
    std::memcpy(write_ptr, vec.packed_values.data(), vec.packed_values.size());
    write_ptr += vec.packed_values.size();
  }

  // Write exception positions
  if (vec.info.num_exceptions > 0) {
    std::memcpy(write_ptr, vec.exception_positions.data(),
                vec.info.num_exceptions * sizeof(uint16_t));
    write_ptr += vec.info.num_exceptions * sizeof(uint16_t);

    // Write exception values (original integers)
    std::memcpy(write_ptr, vec.exception_values.data(),
                vec.info.num_exceptions * sizeof(T));
    write_ptr += vec.info.num_exceptions * sizeof(T);
  }

  return static_cast<size_t>(write_ptr - dest);
}

// Explicit template instantiations
template class PforCompression<int32_t>;
template class PforCompression<int64_t>;

}  // namespace pfor
}  // namespace util
}  // namespace arrow
