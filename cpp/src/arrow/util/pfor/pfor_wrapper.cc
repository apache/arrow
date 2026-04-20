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

namespace arrow {
namespace util {
namespace pfor {

// ----------------------------------------------------------------------
// Header serialization

template <typename T>
void PforWrapper<T>::StoreHeader(uint8_t* dest, const PforHeader& header) {
  dest[0] = header.packing_mode;
  dest[1] = header.log_vector_size;
  dest[2] = header.value_byte_width;
  uint32_t le_num = header.num_elements;  // Assume LE platform
  std::memcpy(dest + 3, &le_num, sizeof(uint32_t));
}

template <typename T>
typename PforWrapper<T>::PforHeader PforWrapper<T>::LoadHeader(const uint8_t* src) {
  PforHeader header;
  header.packing_mode = src[0];
  header.log_vector_size = src[1];
  header.value_byte_width = src[2];
  std::memcpy(&header.num_elements, src + 3, sizeof(uint32_t));
  return header;
}

// ----------------------------------------------------------------------
// Encode

template <typename T>
void PforWrapper<T>::Encode(const T* values, uint32_t num_values, char* comp,
                            size_t* comp_size) {
  ARROW_DCHECK(num_values > 0);
  ARROW_DCHECK(comp != nullptr);
  ARROW_DCHECK(comp_size != nullptr);

  const uint32_t vector_size = kVectorSize;
  const uint32_t num_vectors =
      (num_values + vector_size - 1) / vector_size;

  auto* dest = reinterpret_cast<uint8_t*>(comp);

  // Step 1: Write header
  PforHeader header;
  header.packing_mode = PforConstants::kPackingModeForBitPack;
  header.log_vector_size = PforConstants::kDefaultLogVectorSize;
  header.value_byte_width = sizeof(T);
  header.num_elements = num_values;
  StoreHeader(dest, header);
  uint8_t* write_ptr = dest + PforConstants::kHeaderSize;

  // Step 2: Reserve space for offset array
  uint8_t* offset_array_start = write_ptr;
  write_ptr += num_vectors * sizeof(uint32_t);

  // Step 3: Encode each vector and build offset array
  const uint8_t* data_start = offset_array_start;  // Offsets relative to offset array start

  for (uint32_t v = 0; v < num_vectors; ++v) {
    // Record offset (from start of offset array)
    uint32_t offset = static_cast<uint32_t>(write_ptr - data_start);
    std::memcpy(offset_array_start + v * sizeof(uint32_t), &offset, sizeof(uint32_t));

    // Determine elements in this vector
    uint32_t start_idx = v * vector_size;
    uint32_t elements_in_vector =
        std::min(vector_size, num_values - start_idx);

    // Encode vector
    auto encoded = PforCompression<T>::EncodeVector(
        values + start_idx, elements_in_vector);

    // Serialize to output
    size_t bytes_written = PforCompression<T>::SerializeVector(
        encoded, elements_in_vector, write_ptr);
    write_ptr += bytes_written;
  }

  *comp_size = static_cast<size_t>(write_ptr - dest);
}

// ----------------------------------------------------------------------
// Decode

template <typename T>
void PforWrapper<T>::Decode(T* values, uint32_t num_values, const char* comp,
                            size_t comp_size) {
  ARROW_DCHECK(num_values > 0);
  ARROW_DCHECK(comp != nullptr);

  const auto* src = reinterpret_cast<const uint8_t*>(comp);

  // Step 1: Read header
  PforHeader header = LoadHeader(src);
  ARROW_DCHECK(header.packing_mode == PforConstants::kPackingModeForBitPack);
  ARROW_DCHECK(header.value_byte_width == sizeof(T));

  const uint32_t vector_size = 1u << header.log_vector_size;
  const uint32_t num_vectors =
      (header.num_elements + vector_size - 1) / vector_size;

  // Step 2: Read offset array
  const uint8_t* offset_array_start = src + PforConstants::kHeaderSize;

  // Step 3: Decode each vector
  for (uint32_t v = 0; v < num_vectors; ++v) {
    uint32_t offset;
    std::memcpy(&offset, offset_array_start + v * sizeof(uint32_t),
                sizeof(uint32_t));

    const uint8_t* vector_data = offset_array_start + offset;

    uint32_t start_idx = v * vector_size;
    uint32_t elements_in_vector =
        std::min(vector_size, header.num_elements - start_idx);

    PforCompression<T>::DecodeVector(
        values + start_idx, vector_data, elements_in_vector);
  }
}

// ----------------------------------------------------------------------
// GetMaxCompressedSize

template <typename T>
size_t PforWrapper<T>::GetMaxCompressedSize(uint32_t num_values) {
  const uint32_t vector_size = kVectorSize;
  const uint32_t num_vectors =
      (num_values + vector_size - 1) / vector_size;

  // Header + offset array
  size_t size = PforConstants::kHeaderSize + num_vectors * sizeof(uint32_t);

  // Worst case per vector: full bit width + all exceptions
  size_t max_vector_size = PforVectorInfo<T>::kSerializedSize
      + vector_size * sizeof(T)               // packed at full width
      + vector_size * sizeof(uint16_t)        // exception positions
      + vector_size * sizeof(T);              // exception values

  size += num_vectors * max_vector_size;
  return size;
}

// Explicit template instantiations
template class PforWrapper<int32_t>;
template class PforWrapper<int64_t>;

}  // namespace pfor
}  // namespace util
}  // namespace arrow
