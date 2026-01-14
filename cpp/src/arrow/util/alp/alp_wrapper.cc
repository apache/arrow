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

#include "arrow/util/alp/alp_wrapper.h"

#include <cmath>
#include <optional>

#include "arrow/util/alp/alp.h"
#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/alp/alp_sampler.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

namespace {

// ----------------------------------------------------------------------
// AlpHeader

/// \brief Header structure for ALP compression blocks
///
/// Contains page-level metadata for ALP compression. The num_elements field
/// stores the total element count for the page, allowing per-vector element
/// counts to be inferred (all vectors except the last have vector_size elements).
///
/// Note: num_elements is uint32_t because Parquet page headers use i32 for num_values.
/// See: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
///
/// Serialization format (version 1):
///
///   +---------------------------------------------------+
///   |  AlpHeader (12 bytes)                             |
///   +---------------------------------------------------+
///   |  Offset |  Field              |  Size             |
///   +---------+---------------------+-------------------+
///   |    0    |  version            |  1 byte (uint8)   |
///   |    1    |  compression_mode   |  1 byte (uint8)   |
///   |    2    |  integer_encoding    |  1 byte (uint8)   |
///   |    3    |  reserved           |  1 byte (uint8)   |
///   |    4    |  vector_size        |  4 bytes (uint32) |
///   |    8    |  num_elements       |  4 bytes (uint32) |
///   +---------------------------------------------------+
///
/// \note version must remain the first field to allow reading the rest
///       of the header based on version number.
struct AlpHeader {
  /// Version number. Must remain the first field for version-based parsing.
  uint8_t version = 0;
  /// Compression mode (currently only kAlp is supported).
  uint8_t compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  /// Bit packing layout used for bitpacking.
  uint8_t integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kBitPack);
  /// Reserved for future use (also ensures 4-byte alignment for vector_size).
  uint8_t reserved = 0;
  /// Vector size used for compression.
  /// Must be AlpConstants::kAlpVectorSize for decompression.
  uint32_t vector_size = 0;
  /// Total number of elements in the page (uint32_t since Parquet uses i32).
  /// Per-vector element count is inferred: vector_size for all but the last vector.
  uint32_t num_elements = 0;

  /// \brief Get the size in bytes of the AlpHeader for a version
  ///
  /// \param[in] v the version number
  /// \return the size in bytes
  static constexpr size_t GetSizeForVersion(uint8_t v) {
    // Version 1 header is 12 bytes
    return (v == 1) ? 12 : 0;
  }

  /// \brief Check whether the given version is valid
  ///
  /// \param[in] v the version to check
  /// \return the version if valid, otherwise asserts
  static uint8_t IsValidVersion(uint8_t v) {
    ARROW_CHECK(v == 1) << "invalid_version: " << static_cast<int>(v);
    return v;
  }

  /// \brief Calculate the number of elements for a given vector index
  ///
  /// \param[in] vector_index the 0-based index of the vector
  /// \return the number of elements in this vector
  uint16_t GetVectorNumElements(uint64_t vector_index) const {
    const uint64_t num_full_vectors = num_elements / vector_size;
    const uint64_t remainder = num_elements % vector_size;
    if (vector_index < num_full_vectors) {
      return static_cast<uint16_t>(vector_size);  // Full vector
    } else if (vector_index == num_full_vectors && remainder > 0) {
      return static_cast<uint16_t>(remainder);  // Last partial vector
    }
    return 0;  // Invalid index
  }

  /// \brief Get the AlpMode enum from the stored uint8_t
  AlpMode GetCompressionMode() const {
    return static_cast<AlpMode>(compression_mode);
  }

  /// \brief Get the AlpIntegerEncoding enum from the stored uint8_t
  AlpIntegerEncoding GetIntegerEncoding() const {
    return static_cast<AlpIntegerEncoding>(integer_encoding);
  }
};

}  // namespace

// ----------------------------------------------------------------------
// AlpWrapper::AlpHeader definition

template <typename T>
struct AlpWrapper<T>::AlpHeader : public ::arrow::util::alp::AlpHeader {
};

// ----------------------------------------------------------------------
// AlpWrapper implementation

template <typename T>
typename AlpWrapper<T>::AlpHeader AlpWrapper<T>::LoadHeader(
    const char* comp, size_t comp_size) {
  ARROW_CHECK(comp_size >= 1) << "alp_loadHeader_compSize_too_small_for_version";
  uint8_t version;
  std::memcpy(&version, comp, sizeof(version));
  AlpHeader::IsValidVersion(version);
  const size_t header_size = AlpHeader::GetSizeForVersion(version);
  ARROW_CHECK(comp_size >= header_size) << "alp_loadHeader_compSize_too_small";
  AlpHeader header{};
  std::memcpy(&header, comp, header_size);
  return header;
}

template <typename T>
void AlpWrapper<T>::Encode(const T* decomp, size_t decomp_size, char* comp,
                           size_t* comp_size, std::optional<AlpMode> enforce_mode) {
  ARROW_CHECK(decomp_size % sizeof(T) == 0) << "alp_encode_input_must_be_multiple_of_T";
  const uint64_t element_count = decomp_size / sizeof(T);
  const uint8_t version =
      AlpHeader::IsValidVersion(AlpConstants::kAlpVersion);

  AlpSampler<T> sampler;
  sampler.AddSample({decomp, element_count});
  auto sampling_result = sampler.Finalize();

  // Make room to store header afterwards.
  char* encoded_header = comp;
  const size_t header_size = AlpHeader::GetSizeForVersion(version);
  comp += header_size;
  const uint64_t remaining_compressed_size = *comp_size - header_size;

  const CompressionProgress compression_progress =
      EncodeAlp(decomp, element_count, comp, remaining_compressed_size,
                sampling_result.alp_preset);

  AlpHeader header{};
  header.version = version;
  header.compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  header.integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kBitPack);
  header.vector_size = AlpConstants::kAlpVectorSize;
  header.num_elements = static_cast<uint32_t>(element_count);

  std::memcpy(encoded_header, &header, header_size);
  *comp_size = header_size + compression_progress.num_compressed_bytes_produced;
}

template <typename T>
template <typename TargetType>
void AlpWrapper<T>::Decode(TargetType* decomp, uint32_t num_elements, const char* comp,
                           size_t comp_size) {
  const AlpHeader header = LoadHeader(comp, comp_size);
  ARROW_CHECK(header.vector_size == AlpConstants::kAlpVectorSize)
      << "unsupported_vector_size: " << header.vector_size;

  const size_t header_size = AlpHeader::GetSizeForVersion(header.version);
  const char* compression_body = comp + header_size;
  const uint64_t compression_body_size = comp_size - header_size;

  ARROW_CHECK(header.GetCompressionMode() == AlpMode::kAlp)
      << "alp_decode_unsupported_mode";

  DecodeAlp<TargetType>(decomp, num_elements, compression_body, compression_body_size,
                        header.GetIntegerEncoding(), header.vector_size,
                        header.num_elements);
}

template void AlpWrapper<float>::Decode(float* decomp, uint32_t num_elements,
                                        const char* comp, size_t comp_size);
template void AlpWrapper<float>::Decode(double* decomp, uint32_t num_elements,
                                        const char* comp, size_t comp_size);
template void AlpWrapper<double>::Decode(double* decomp, uint32_t num_elements,
                                         const char* comp, size_t comp_size);

template <typename T>
uint64_t AlpWrapper<T>::GetMaxCompressedSize(uint64_t decomp_size) {
  ARROW_CHECK(decomp_size % sizeof(T) == 0)
      << "alp_decompressed_size_not_multiple_of_T";
  const uint64_t element_count = decomp_size / sizeof(T);
  const uint8_t version =
      AlpHeader::IsValidVersion(AlpConstants::kAlpVersion);
  uint64_t max_alp_size = AlpHeader::GetSizeForVersion(version);
  // Add per-vector header sizes (10 bytes for float, 14 bytes for double).
  max_alp_size +=
      AlpEncodedVectorInfo<T>::kStoredSize *
      std::ceil(static_cast<double>(element_count) / AlpConstants::kAlpVectorSize);
  // Worst case: everything is an exception, except two values that are chosen
  // with large difference to make FOR encoding for placeholders impossible.
  // Values/placeholders.
  max_alp_size += element_count * sizeof(T);
  // Exceptions.
  max_alp_size += element_count * sizeof(T);
  // Exception positions.
  max_alp_size += element_count * sizeof(AlpConstants::PositionType);

  return max_alp_size;
}

template <typename T>
auto AlpWrapper<T>::EncodeAlp(const T* decomp, uint64_t element_count, char* comp,
                              size_t comp_size, const AlpEncodingPreset& combinations)
    -> CompressionProgress {
  uint64_t output_offset = 0;
  uint64_t input_offset = 0;
  uint64_t remaining_output_size = comp_size;

  for (uint64_t remaining_elements = element_count; remaining_elements > 0;
       remaining_elements -= std::min(AlpConstants::kAlpVectorSize, remaining_elements)) {
    const uint64_t elements_to_encode =
        std::min(AlpConstants::kAlpVectorSize, remaining_elements);
    const AlpEncodedVector<T> encoded_vector = AlpCompression<T>::CompressVector(
        decomp + input_offset, elements_to_encode, combinations);

    const uint64_t compressed_vector_size = encoded_vector.GetStoredSize();
    if (compressed_vector_size == 0 || compressed_vector_size > remaining_output_size) {
      return CompressionProgress{0, 0};
    }

    ARROW_CHECK(encoded_vector.GetStoredSize() <= remaining_output_size)
        << "alp_encode_cannot_store_compressed_vector";

    encoded_vector.Store({comp + output_offset, remaining_output_size});

    remaining_output_size -= compressed_vector_size;
    output_offset += compressed_vector_size;
    input_offset += elements_to_encode;
  }
  return CompressionProgress{output_offset, input_offset};
}

template <typename T>
template <typename TargetType>
auto AlpWrapper<T>::DecodeAlp(TargetType* decomp, size_t decomp_element_count,
                              const char* comp, size_t comp_size,
                              AlpIntegerEncoding integer_encoding,
                              uint32_t vector_size, uint32_t total_elements)
    -> DecompressionProgress {
  uint64_t input_offset = 0;
  uint64_t output_offset = 0;
  uint64_t vector_index = 0;

  // Calculate per-vector element count from total and position
  const uint64_t num_full_vectors = total_elements / vector_size;
  const uint64_t remainder = total_elements % vector_size;

  while (input_offset < comp_size && output_offset < decomp_element_count) {
    // Calculate this vector's element count
    uint16_t this_vector_elements;
    if (vector_index < num_full_vectors) {
      this_vector_elements = static_cast<uint16_t>(vector_size);
    } else if (vector_index == num_full_vectors && remainder > 0) {
      this_vector_elements = static_cast<uint16_t>(remainder);
    } else {
      break;  // No more vectors
    }

    // Use zero-copy view to avoid memory allocation and copying
    const AlpEncodedVectorView<T> encoded_view = AlpEncodedVectorView<T>::LoadView(
        {comp + input_offset, comp_size - input_offset}, this_vector_elements);
    const uint64_t compressed_size = encoded_view.GetStoredSize();

    ARROW_CHECK(output_offset + this_vector_elements <= decomp_element_count)
        << "alp_decode_output_too_small: " << output_offset << " vs "
        << this_vector_elements << " vs " << decomp_element_count;

    AlpCompression<T>::DecompressVectorView(encoded_view, integer_encoding,
                                            decomp + output_offset);

    input_offset += compressed_size;
    output_offset += this_vector_elements;
    vector_index++;
  }

  return DecompressionProgress{output_offset, input_offset};
}

// ----------------------------------------------------------------------
// Template instantiations

template class AlpWrapper<float>;
template class AlpWrapper<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
