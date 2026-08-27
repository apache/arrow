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
#include <bit>
#include <cstring>
#include <span>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace pfor {

namespace {

/// Width of one entry in the per-page offset array.
constexpr int64_t kOffsetSize = sizeof(PforConstants::OffsetType);

/// \brief Accept only the vector sizes the page header can describe
///
/// A size outside this range would encode a page that Decode then rejects, so
/// refuse it up front.
Status ValidateVectorSize(int32_t vector_size) {
  if (!std::has_single_bit(static_cast<uint32_t>(vector_size)) ||
      vector_size < (1 << PforConstants::kMinLogVectorSize) ||
      vector_size > PforConstants::kMaxVectorSize) {
    return Status::Invalid("PFOR vector_size must be a power of two in [",
                           1 << PforConstants::kMinLogVectorSize, ", ",
                           PforConstants::kMaxVectorSize, "]: ", vector_size);
  }
  return Status::OK();
}

}  // namespace

// ----------------------------------------------------------------------
// Header serialization

template <typename T>
void PforWrapper<T>::StoreHeader(std::span<uint8_t> dest, const PforHeader& header) {
  static_assert(PforConstants::kHeaderSize == sizeof(PforHeader::packing_mode) +
                                                  sizeof(PforHeader::log_vector_size) +
                                                  sizeof(PforHeader::value_byte_width) +
                                                  sizeof(PforHeader::num_elements),
                "kHeaderSize must match the fields StoreHeader and LoadHeader move");
  uint8_t* ptr = dest.data();
  util::SafeStore(ptr + 0, header.packing_mode);
  util::SafeStore(ptr + 1, header.log_vector_size);
  util::SafeStore(ptr + 2, header.value_byte_width);
  util::SafeStore(ptr + 3, header.num_elements);
}

template <typename T>
Result<typename PforWrapper<T>::PforHeader> PforWrapper<T>::LoadHeader(
    std::span<const uint8_t> src) {
  if (src.size() < static_cast<size_t>(PforConstants::kHeaderSize)) {
    return Status::Invalid("PFOR compressed buffer too small for header: ", src.size(),
                           " < ", PforConstants::kHeaderSize);
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
    return Status::Invalid(
        "PFOR value_byte_width mismatch: ", static_cast<int>(header.value_byte_width),
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
Status PforWrapper<T>::Encode(const T* values, int32_t num_values, int32_t vector_size,
                              uint8_t* comp, int64_t* comp_size) {
  if (num_values <= 0) {
    return Status::Invalid("PFOR num_values must be positive: ", num_values);
  }
  if (values == nullptr) {
    return Status::Invalid("PFOR input pointer is null");
  }
  if (comp == nullptr) {
    return Status::Invalid("PFOR output pointer is null");
  }
  if (comp_size == nullptr) {
    return Status::Invalid("PFOR output size pointer is null");
  }
  RETURN_NOT_OK(ValidateVectorSize(vector_size));

  // Everything below writes into `comp` without re-checking, including the
  // header and the offset array, so confirm once that the caller gave us the
  // buffer the API asks for.
  ARROW_ASSIGN_OR_RAISE(const int64_t max_size,
                        GetMaxCompressedSize(num_values, vector_size));
  if (*comp_size < max_size) {
    return Status::Invalid("PFOR output buffer of ", *comp_size,
                           " bytes is smaller than the ", max_size, " bytes ", num_values,
                           " values may need");
  }

  const int32_t num_vectors =
      static_cast<int32_t>(bit_util::CeilDiv(num_values, vector_size));

  // ValidateVectorSize has already established that vector_size is a power of
  // two, so its trailing-zero count is log2 of it.
  const auto log_vector_size =
      static_cast<uint8_t>(std::countr_zero(static_cast<uint32_t>(vector_size)));

  uint8_t* dest = comp;

  // Step 1: Write header
  PforHeader header;
  header.packing_mode = PforConstants::kPackingModeForBitPack;
  header.log_vector_size = log_vector_size;
  header.value_byte_width = sizeof(T);
  header.num_elements = num_values;
  StoreHeader(std::span<uint8_t>(dest, PforConstants::kHeaderSize), header);
  uint8_t* write_ptr = dest + PforConstants::kHeaderSize;

  // Step 2: Reserve space for offset array
  uint8_t* offset_array_start = write_ptr;
  write_ptr += num_vectors * kOffsetSize;

  // Step 3: Encode each vector and build offset array
  const uint8_t* data_start = offset_array_start;

  for (int32_t v = 0; v < num_vectors; ++v) {
    // Record offset (from start of offset array)
    const auto offset = static_cast<PforConstants::OffsetType>(write_ptr - data_start);
    util::SafeStore(offset_array_start + v * kOffsetSize, offset);

    // Determine elements in this vector
    int32_t start_idx = v * vector_size;
    int32_t elements_in_vector = std::min(vector_size, num_values - start_idx);

    auto encoded =
        PforCompression<T>::EncodeVector(values + start_idx, elements_in_vector);

    // Serialize to output
    ARROW_ASSIGN_OR_RAISE(
        const int64_t bytes_written,
        PforCompression<T>::SerializeVector(
            encoded, elements_in_vector,
            std::span<uint8_t>(write_ptr, dest + *comp_size - write_ptr)));
    write_ptr += bytes_written;
  }

  *comp_size = static_cast<int64_t>(write_ptr - dest);
  return Status::OK();
}

template <typename T>
Status PforWrapper<T>::Encode(const T* values, int32_t num_values, uint8_t* comp,
                              int64_t* comp_size) {
  return Encode(values, num_values, kVectorSize, comp, comp_size);
}

// ----------------------------------------------------------------------
// Decode

template <typename T>
Status PforWrapper<T>::Decode(const uint8_t* comp, int64_t comp_size, int32_t num_values,
                              T* values) {
  if (num_values <= 0) {
    return Status::Invalid("PFOR num_values must be positive: ", num_values);
  }
  if (comp == nullptr) {
    return Status::Invalid("PFOR compressed data pointer is null");
  }
  // Every bound below is expressed against `comp_size`, and it also becomes the
  // size of a std::span, where a negative value would convert to a huge size_t
  // and read past the end of the buffer.
  if (comp_size < PforConstants::kHeaderSize) {
    return Status::Invalid("PFOR compressed buffer too small for header: ", comp_size,
                           " < ", PforConstants::kHeaderSize);
  }

  const uint8_t* src = comp;

  // Step 1: Read header
  ARROW_ASSIGN_OR_RAISE(PforHeader header,
                        LoadHeader(std::span<const uint8_t>(src, comp_size)));

  // The caller sized `values` for num_values, so a header claiming more than
  // that would have us write past the end of it.
  if (header.num_elements > num_values) {
    return Status::Invalid("PFOR header element count ", header.num_elements,
                           " exceeds output capacity ", num_values);
  }

  const int32_t vector_size = 1 << header.log_vector_size;
  const int32_t num_vectors =
      static_cast<int32_t>(bit_util::CeilDiv(header.num_elements, vector_size));

  // Step 2: Read offset array
  const uint8_t* offset_array_start = src + PforConstants::kHeaderSize;
  const int64_t offset_array_size = num_vectors * kOffsetSize;
  if (PforConstants::kHeaderSize + offset_array_size > comp_size) {
    return Status::Invalid("PFOR offset array for ", num_vectors,
                           " vectors does not fit in ", comp_size, " bytes");
  }

  // Step 3: Decode each vector
  for (int32_t v = 0; v < num_vectors; ++v) {
    const auto offset =
        util::SafeLoadAs<PforConstants::OffsetType>(offset_array_start + v * kOffsetSize);
    if (PforConstants::kHeaderSize + static_cast<int64_t>(offset) >= comp_size) {
      return Status::Invalid("PFOR vector ", v, " offset ", offset,
                             " is past the end of the ", comp_size, " byte buffer");
    }

    const uint8_t* vector_data = offset_array_start + offset;

    int32_t start_idx = v * vector_size;
    int32_t elements_in_vector = std::min(vector_size, header.num_elements - start_idx);

    ARROW_RETURN_NOT_OK(PforCompression<T>::DecodeVector(
        std::span<const uint8_t>(vector_data, src + comp_size - vector_data),
        elements_in_vector, values + start_idx));
  }

  return Status::OK();
}

// ----------------------------------------------------------------------
// GetMaxCompressedSize

template <typename T>
Result<int64_t> PforWrapper<T>::GetMaxCompressedSize(int32_t num_values,
                                                     int32_t vector_size) {
  if (num_values < 0) {
    return Status::Invalid("PFOR num_values must be non-negative: ", num_values);
  }
  RETURN_NOT_OK(ValidateVectorSize(vector_size));

  const int64_t num_vectors = bit_util::CeilDiv(num_values, vector_size);

  // A vector never serializes to more than its values occupy unpacked.
  // FindOptimalBitWidth minimises `num_elements * bit_width + num_exceptions *
  // (16 + 8 * sizeof(T))` bits, and the full-width candidate scores
  // `num_elements * 8 * sizeof(T)` bits with no exceptions at all, so whatever
  // width it does pick costs no more than that. Bit packing rounds the packed
  // section up to a whole byte, hence the trailing byte.
  const int64_t max_vector_size =
      PforVectorInfo<T>::kStoredSize + vector_size * static_cast<int64_t>(sizeof(T)) + 1;

  return PforConstants::kHeaderSize + num_vectors * (kOffsetSize + max_vector_size);
}

// Explicit template instantiations
template class PforWrapper<int32_t>;
template class PforWrapper<int64_t>;

}  // namespace pfor
}  // namespace util
}  // namespace arrow
