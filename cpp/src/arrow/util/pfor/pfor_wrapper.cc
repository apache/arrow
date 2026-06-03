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

// PFOR page-level wrapper implementation
//
// Page layout:
//   [Header 7B] [Offset Array: numVectors * 4B] [Vector 0] [Vector 1] ...
//
// Each vector:
//   [PforVectorInfo] [PackedValues] [ExceptionPositions] [ExceptionValues]

#include "arrow/util/pfor/pfor_wrapper.h"

#include <algorithm>
#include <cstring>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace pfor {

// ----------------------------------------------------------------------
// Header serialization

template <typename T>
void PforWrapper<T>::StoreHeader(arrow::util::span<uint8_t> dest,
                                 const PforHeader& header) {
  uint8_t* ptr = dest.data();
  ptr[0] = header.packing_mode;
  ptr[1] = header.log_vector_size;
  ptr[2] = header.value_byte_width;
  std::memcpy(ptr + 3, &header.num_elements, sizeof(int32_t));
}

template <typename T>
typename PforWrapper<T>::PforHeader PforWrapper<T>::LoadHeader(
    arrow::util::span<const uint8_t> src) {
  PforHeader header;
  const uint8_t* ptr = src.data();
  header.packing_mode = ptr[0];
  header.log_vector_size = ptr[1];
  header.value_byte_width = ptr[2];
  std::memcpy(&header.num_elements, ptr + 3, sizeof(int32_t));
  return header;
}

// ----------------------------------------------------------------------
// Encode

template <typename T>
void PforWrapper<T>::Encode(const T* values, int32_t num_values, char* comp,
                            int64_t* comp_size) {
  ARROW_DCHECK(num_values > 0);
  ARROW_DCHECK(comp != nullptr);
  ARROW_DCHECK(comp_size != nullptr);

  const int32_t vector_size = kVectorSize;
  const int32_t num_vectors =
      (num_values + vector_size - 1) / vector_size;

  auto* dest = reinterpret_cast<uint8_t*>(comp);

  // Step 1: Write header
  PforHeader header;
  header.packing_mode = PforConstants::kPackingModeForBitPack;
  header.log_vector_size = PforConstants::kDefaultLogVectorSize;
  header.value_byte_width = sizeof(T);
  header.num_elements = num_values;
  StoreHeader(arrow::util::span<uint8_t>(dest, PforConstants::kHeaderSize), header);
  uint8_t* write_ptr = dest + PforConstants::kHeaderSize;

  // Step 2: Reserve space for offset array
  uint8_t* offset_array_start = write_ptr;
  write_ptr += num_vectors * sizeof(uint32_t);

  // Step 3: Encode each vector and build offset array
  const uint8_t* data_start = offset_array_start;

  for (int32_t v = 0; v < num_vectors; ++v) {
    // Record offset (from start of offset array)
    uint32_t offset = static_cast<uint32_t>(write_ptr - data_start);
    std::memcpy(offset_array_start + v * sizeof(uint32_t), &offset, sizeof(uint32_t));

    // Determine elements in this vector
    int32_t start_idx = v * vector_size;
    int32_t elements_in_vector =
        std::min(vector_size, num_values - start_idx);

    // Encode vector
    auto encoded = PforCompression<T>::EncodeVector(
        values + start_idx, elements_in_vector);

    // Serialize to output
    int64_t bytes_written = PforCompression<T>::SerializeVector(
        encoded, elements_in_vector,
        arrow::util::span<uint8_t>(write_ptr, dest + *comp_size - write_ptr));
    write_ptr += bytes_written;
  }

  *comp_size = static_cast<int64_t>(write_ptr - dest);
}

// ----------------------------------------------------------------------
// Decode

template <typename T>
void PforWrapper<T>::Decode(T* values, int32_t num_values, const char* comp,
                            int64_t comp_size) {
  ARROW_DCHECK(num_values > 0);
  ARROW_DCHECK(comp != nullptr);

  const auto* src = reinterpret_cast<const uint8_t*>(comp);

  // Step 1: Read header
  PforHeader header = LoadHeader(
      arrow::util::span<const uint8_t>(src, PforConstants::kHeaderSize));
  ARROW_DCHECK(header.packing_mode == PforConstants::kPackingModeForBitPack);
  ARROW_DCHECK(header.value_byte_width == sizeof(T));

  const int32_t vector_size = 1 << header.log_vector_size;
  const int32_t num_vectors =
      (header.num_elements + vector_size - 1) / vector_size;

  // Step 2: Read offset array
  const uint8_t* offset_array_start = src + PforConstants::kHeaderSize;

  // Step 3: Decode each vector
  for (int32_t v = 0; v < num_vectors; ++v) {
    uint32_t offset;
    std::memcpy(&offset, offset_array_start + v * sizeof(uint32_t),
                sizeof(uint32_t));

    const uint8_t* vector_data = offset_array_start + offset;

    int32_t start_idx = v * vector_size;
    int32_t elements_in_vector =
        std::min(vector_size, header.num_elements - start_idx);

    PforCompression<T>::DecodeVector(
        values + start_idx,
        arrow::util::span<const uint8_t>(vector_data, src + comp_size - vector_data),
        elements_in_vector);
  }
}

// ----------------------------------------------------------------------
// GetMaxCompressedSize

template <typename T>
int64_t PforWrapper<T>::GetMaxCompressedSize(int32_t num_values) {
  const int32_t vector_size = kVectorSize;
  const int32_t num_vectors =
      (num_values + vector_size - 1) / vector_size;

  // Header + offset array
  int64_t size = PforConstants::kHeaderSize +
                 num_vectors * static_cast<int64_t>(sizeof(uint32_t));

  // Worst case per vector: full bit width + all exceptions
  int64_t max_vector_size = PforVectorInfo<T>::kStoredSize
      + vector_size * static_cast<int64_t>(sizeof(T))       // packed at full width
      + vector_size * static_cast<int64_t>(sizeof(int16_t)) // exception positions
      + vector_size * static_cast<int64_t>(sizeof(T));      // exception values

  size += num_vectors * max_vector_size;
  return size;
}

// Explicit template instantiations
template class PforWrapper<int32_t>;
template class PforWrapper<int64_t>;

}  // namespace pfor
}  // namespace util
}  // namespace arrow
