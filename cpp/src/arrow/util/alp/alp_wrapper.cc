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
// CompressionBlockHeader

/// \brief Header structure for ALP compression blocks
///
/// Contains metadata required to decompress the data. Note that compressed_size
/// and num_elements are NOT stored in the header - they are available from the
/// page header and passed to the Decode() function.
///
/// Serialization format (version 1):
///
///   +---------------------------------------------------+
///   |  CompressionBlockHeader (24 bytes)                |
///   +---------------------------------------------------+
///   |  Offset |  Field              |  Size             |
///   +---------+---------------------+-------------------+
///   |    0    |  version            |  8 bytes (uint64) |
///   |    8    |  vector_size        |  8 bytes (uint64) |
///   |   16    |  compression_mode   |  4 bytes (enum)   |
///   |   20    |  bit_pack_layout    |  4 bytes (enum)   |
///   +---------------------------------------------------+
///
/// \note version must remain the first field to allow reading the rest
///       of the header based on version number.
struct CompressionBlockHeader {
  /// Version number. Must remain the first field for version-based parsing.
  uint64_t version = 0;
  /// Vector size used for compression.
  /// Must be AlpConstants::kAlpVectorSize for decompression.
  uint64_t vector_size = 0;
  /// Compression mode (currently only kAlp is supported).
  AlpMode compression_mode = AlpMode::kAlp;
  /// Bit packing layout used for bitpacking.
  AlpBitPackLayout bit_pack_layout = AlpBitPackLayout::kNormal;

  /// \brief Get the size in bytes of the CompressionBlockHeader for a version
  ///
  /// \param[in] v the version number
  /// \return the size in bytes
  static size_t GetSizeForVersion(uint64_t v) {
    size_t size;
    if (v == 1) {
      size = sizeof(version) + sizeof(vector_size) + sizeof(compression_mode) +
             sizeof(bit_pack_layout);
    } else {
      ARROW_CHECK(false) << "unknown_version: " << v;
    }
    return size;
  }

  /// \brief Check whether the given version is valid
  ///
  /// \param[in] v the version to check
  /// \return the version if valid, otherwise asserts
  static uint64_t IsValidVersion(uint64_t v) {
    if (v == 1) {
      return v;
    }
    ARROW_CHECK(false) << "invalid_version: " << v;
    return 0;  // Unreachable, but silences warning.
  }
};

}  // namespace

// ----------------------------------------------------------------------
// AlpWrapper::CompressionBlockHeader definition

template <typename T>
struct AlpWrapper<T>::CompressionBlockHeader : public ::arrow::util::alp::CompressionBlockHeader {
};

// ----------------------------------------------------------------------
// AlpWrapper implementation

template <typename T>
typename AlpWrapper<T>::CompressionBlockHeader AlpWrapper<T>::LoadHeader(
    const char* comp, size_t comp_size) {
  CompressionBlockHeader header{};
  ARROW_CHECK(comp_size > sizeof(header.version))
      << "alp_loadHeader_compSize_too_small_for_header_version";
  uint64_t version;
  std::memcpy(&version, comp, sizeof(header.version));
  ::arrow::util::alp::CompressionBlockHeader::IsValidVersion(version);
  ARROW_CHECK(comp_size >= ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version))
      << "alp_loadHeader_compSize_too_small";
  std::memcpy(&header, comp,
              ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version));
  return header;
}

template <typename T>
void AlpWrapper<T>::Encode(const T* decomp, size_t decomp_size, char* comp,
                           size_t* comp_size, std::optional<AlpMode> enforce_mode) {
  ARROW_CHECK(decomp_size % sizeof(T) == 0) << "alp_encode_input_must_be_multiple_of_T";
  const uint64_t element_count = decomp_size / sizeof(T);
  const uint64_t version = ::arrow::util::alp::CompressionBlockHeader::IsValidVersion(
      AlpConstants::kAlpVersion);

  AlpSampler<T> sampler;
  sampler.AddSample({decomp, element_count});
  auto sampling_result = sampler.Finalize();

  // Make room to store header afterwards.
  char* encoded_header = comp;
  comp += ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version);
  const uint64_t remaining_compressed_size =
      *comp_size - ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version);

  const CompressionProgress compression_progress =
      EncodeAlp(decomp, element_count, comp, remaining_compressed_size,
                sampling_result.alp_preset);

  CompressionBlockHeader header{};
  header.version = version;
  header.vector_size = AlpConstants::kAlpVectorSize;
  header.compression_mode = AlpMode::kAlp;
  header.bit_pack_layout = AlpBitPackLayout::kNormal;

  std::memcpy(encoded_header, &header,
              ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version));
  *comp_size = ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version) +
               compression_progress.num_compressed_bytes_produced;
}

template <typename T>
template <typename TargetType>
void AlpWrapper<T>::Decode(TargetType* decomp, uint64_t num_elements, const char* comp,
                           size_t comp_size) {
  const CompressionBlockHeader header = LoadHeader(comp, comp_size);
  ARROW_CHECK(header.vector_size == AlpConstants::kAlpVectorSize)
      << "unsupported_vector_size: " << header.vector_size;

  const char* compression_body =
      comp + ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(header.version);
  const uint64_t compression_body_size =
      comp_size -
      ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(header.version);

  ARROW_CHECK(header.compression_mode == AlpMode::kAlp) << "alp_decode_unsupported_mode";

  DecodeAlp<TargetType>(decomp, num_elements, compression_body, compression_body_size,
                        header.bit_pack_layout);
}

template void AlpWrapper<float>::Decode(float* decomp, uint64_t num_elements,
                                        const char* comp, size_t comp_size);
template void AlpWrapper<float>::Decode(double* decomp, uint64_t num_elements,
                                        const char* comp, size_t comp_size);
template void AlpWrapper<double>::Decode(double* decomp, uint64_t num_elements,
                                         const char* comp, size_t comp_size);

template <typename T>
uint64_t AlpWrapper<T>::GetMaxCompressedSize(uint64_t decomp_size) {
  ARROW_CHECK(decomp_size % sizeof(T) == 0)
      << "alp_decompressed_size_not_multiple_of_T";
  const uint64_t element_count = decomp_size / sizeof(T);
  const uint64_t version = ::arrow::util::alp::CompressionBlockHeader::IsValidVersion(
      AlpConstants::kAlpVersion);
  uint64_t max_alp_size =
      ::arrow::util::alp::CompressionBlockHeader::GetSizeForVersion(version);
  // Add header sizes.
  max_alp_size +=
      sizeof(AlpEncodedVectorInfo) *
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
                              AlpBitPackLayout bit_pack_layout) -> DecompressionProgress {
  uint64_t input_offset = 0;
  uint64_t output_offset = 0;
  while (input_offset < comp_size && output_offset < decomp_element_count) {
    // Use zero-copy view to avoid memory allocation and copying
    const AlpEncodedVectorView<T> encoded_view =
        AlpEncodedVectorView<T>::LoadView({comp + input_offset, comp_size - input_offset});
    const uint64_t compressed_size = encoded_view.GetStoredSize();
    const uint64_t element_count = encoded_view.vector_info.num_elements;

    ARROW_CHECK(output_offset + element_count <= decomp_element_count)
        << "alp_decode_output_too_small: " << output_offset << " vs " << element_count
        << " vs " << decomp_element_count;

    AlpCompression<T>::DecompressVectorView(encoded_view, bit_pack_layout,
                                            decomp + output_offset);

    input_offset += compressed_size;
    output_offset += element_count;
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
