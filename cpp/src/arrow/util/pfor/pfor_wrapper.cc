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
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace pfor {

// ----------------------------------------------------------------------
// Header serialization

template <typename T>
void PforWrapper<T>::StoreHeader(arrow::util::span<uint8_t> dest,
                                 const PforHeader& header) {
  uint8_t* ptr = dest.data();
  util::SafeStore(ptr + 0, header.packing_mode);
  util::SafeStore(ptr + 1, header.log_vector_size);
  util::SafeStore(ptr + 2, header.value_byte_width);
  util::SafeStore(ptr + 3, header.num_elements);
}

template <typename T>
Result<typename PforWrapper<T>::PforHeader> PforWrapper<T>::LoadHeader(
    arrow::util::span<const uint8_t> src) {
  if (src.size() < static_cast<size_t>(PforConstants::kHeaderSize)) {
    return Status::Invalid("PFOR compressed buffer too small for header: ",
                           src.size(), " < ", PforConstants::kHeaderSize);
  }
  PforHeader header;
  const uint8_t* ptr = src.data();
  header.packing_mode = util::SafeLoadAs<uint8_t>(ptr + 0);
  header.log_vector_size = util::SafeLoadAs<uint8_t>(ptr + 1);
  header.value_byte_width = util::SafeLoadAs<uint8_t>(ptr + 2);
  header.num_elements = util::SafeLoadAs<int32_t>(ptr + 3);

  if (header.packing_mode != PforConstants::kPackingModeForBitPack) {
    return Status::Invalid("PFOR unsupported packing mode: ",
                           static_cast<int>(header.packing_mode));
  }
  if (header.value_byte_width != sizeof(T)) {
    return Status::Invalid("PFOR value_byte_width mismatch: ",
                           static_cast<int>(header.value_byte_width),
                           " vs expected ", sizeof(T));
  }
  if (header.log_vector_size < PforConstants::kMinLogVectorSize ||
      header.log_vector_size > PforConstants::kMaxLogVectorSize) {
    return Status::Invalid("PFOR invalid log_vector_size: ",
                           static_cast<int>(header.log_vector_size));
  }
  if (header.num_elements < 0) {
    return Status::Invalid("PFOR invalid num_elements: ", header.num_elements);
  }
  return header;
}

// ----------------------------------------------------------------------
// Encode

template <typename T>
void PforWrapper<T>::Encode(const T* values, int32_t num_values, int32_t vector_size,
                            char* comp, int64_t* comp_size) {
  ARROW_DCHECK(num_values > 0);
  ARROW_DCHECK(comp != nullptr);
  ARROW_DCHECK(comp_size != nullptr);
  ARROW_DCHECK((vector_size & (vector_size - 1)) == 0);

  const int32_t num_vectors =
      (num_values + vector_size - 1) / vector_size;

  // Compute log2(vector_size)
  uint8_t log_vector_size = 0;
  for (int32_t v = vector_size; v > 1; v >>= 1) ++log_vector_size;

  auto* dest = reinterpret_cast<uint8_t*>(comp);

  // Step 1: Write header
  PforHeader header;
  header.packing_mode = PforConstants::kPackingModeForBitPack;
  header.log_vector_size = log_vector_size;
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
    util::SafeStore(offset_array_start + v * sizeof(uint32_t), offset);

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

template <typename T>
void PforWrapper<T>::Encode(const T* values, int32_t num_values, char* comp,
                            int64_t* comp_size) {
  Encode(values, num_values, kVectorSize, comp, comp_size);
}

// ----------------------------------------------------------------------
// Decode

template <typename T>
Status PforWrapper<T>::Decode(T* values, int32_t num_values, const char* comp,
                              int64_t comp_size) {
  if (num_values <= 0) {
    return Status::Invalid("PFOR num_values must be positive: ", num_values);
  }
  if (comp == nullptr) {
    return Status::Invalid("PFOR compressed data pointer is null");
  }

  const auto* src = reinterpret_cast<const uint8_t*>(comp);

  // Step 1: Read header
  ARROW_ASSIGN_OR_RAISE(
      PforHeader header,
      LoadHeader(arrow::util::span<const uint8_t>(src, comp_size)));

  const int32_t vector_size = 1 << header.log_vector_size;
  const int32_t num_vectors =
      (header.num_elements + vector_size - 1) / vector_size;

  // Step 2: Read offset array
  const uint8_t* offset_array_start = src + PforConstants::kHeaderSize;

  // Step 3: Decode each vector
  for (int32_t v = 0; v < num_vectors; ++v) {
    uint32_t offset =
        util::SafeLoadAs<uint32_t>(offset_array_start + v * sizeof(uint32_t));

    const uint8_t* vector_data = offset_array_start + offset;

    int32_t start_idx = v * vector_size;
    int32_t elements_in_vector =
        std::min(vector_size, header.num_elements - start_idx);

    ARROW_RETURN_NOT_OK(PforCompression<T>::DecodeVector(
        values + start_idx,
        arrow::util::span<const uint8_t>(vector_data, src + comp_size - vector_data),
        elements_in_vector));
  }

  return Status::OK();
}

// ----------------------------------------------------------------------
// GetMaxCompressedSize

template <typename T>
int64_t PforWrapper<T>::GetMaxCompressedSize(int32_t num_values, int32_t vector_size) {
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
