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
/// Note: log_vector_size stores the base-2 logarithm of the vector size.
/// The actual vector size is computed as: 1u << log_vector_size (i.e., 2^log_vector_size).
/// For example, log_vector_size=10 means vector_size=1024.
/// This allows representing any power-of-2 vector size up to 2^255 in a single byte.
///
/// Header format (version 1):
///
///   +---------------------------------------------------+
///   |  AlpHeader (8 bytes)                              |
///   +---------------------------------------------------+
///   |  Offset |  Field              |  Size             |
///   +---------+---------------------+-------------------+
///   |    0    |  version            |  1 byte (uint8)   |
///   |    1    |  compression_mode   |  1 byte (uint8)   |
///   |    2    |  integer_encoding   |  1 byte (uint8)   |
///   |    3    |  log_vector_size    |  1 byte (uint8)   |
///   |    4    |  num_elements       |  4 bytes (uint32) |
///   +---------------------------------------------------+
///
/// Page-level layout (metadata-at-start for efficient random access):
///
///   +-------------------------------------------------------------------+
///   |  [AlpHeader (8B)]                                                 |
///   |  [VectorInfo₀ | VectorInfo₁ | ... | VectorInfoₙ]  ← Metadata      |
///   |  [Data₀ | Data₁ | ... | Dataₙ]                    ← Data sections |
///   +-------------------------------------------------------------------+
///
/// This layout enables O(1) random access to any vector by:
/// 1. Reading all VectorInfo first (contiguous, cache-friendly)
/// 2. Computing data offsets from VectorInfo
/// 3. Seeking directly to the target vector's data
///
/// \note version must remain the first field to allow reading the rest
///       of the header based on version number.
struct AlpHeader {
  /// Version number. Must remain the first field for version-based parsing.
  uint8_t version = 0;
  /// Compression mode (currently only kAlp is supported).
  uint8_t compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  /// Integer encoding method used (currently only kForBitPack is supported).
  uint8_t integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  /// Log base 2 of vector size. Actual vector size = 1u << log_vector_size.
  /// For example: 10 means 2^10 = 1024 elements per vector.
  uint8_t log_vector_size = 0;
  /// Total number of elements in the page (uint32_t since Parquet uses i32).
  /// Per-vector element count is inferred: vector_size for all but the last vector.
  uint32_t num_elements = 0;

  /// \brief Get the size in bytes of the AlpHeader for a version
  ///
  /// \param[in] v the version number
  /// \return the size in bytes
  static constexpr size_t GetSizeForVersion(uint8_t v) {
    // Version 1 header is 8 bytes
    return (v == 1) ? 8 : 0;
  }

  /// \brief Check whether the given version is valid
  ///
  /// \param[in] v the version to check
  /// \return the version if valid, otherwise asserts
  static uint8_t IsValidVersion(uint8_t v) {
    ARROW_CHECK(v == 1) << "invalid_version: " << static_cast<int>(v);
    return v;
  }

  /// \brief Compute the actual vector size from log_vector_size
  ///
  /// \return the vector size (2^log_vector_size)
  uint32_t GetVectorSize() const { return 1u << log_vector_size; }

  /// \brief Compute log base 2 of a power-of-2 value
  ///
  /// \param[in] value a power-of-2 value
  /// \return the log base 2 of value
  static uint8_t Log2(uint32_t value) {
    ARROW_CHECK(value > 0 && (value & (value - 1)) == 0)
        << "value_must_be_power_of_2: " << value;
    uint8_t log = 0;
    while ((1u << log) < value) {
      ++log;
    }
    return log;
  }

  /// \brief Calculate the number of elements for a given vector index
  ///
  /// \param[in] vector_index the 0-based index of the vector
  /// \return the number of elements in this vector
  uint16_t GetVectorNumElements(uint64_t vector_index) const {
    const uint32_t vector_size = GetVectorSize();
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
  header.integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  header.log_vector_size = AlpHeader::Log2(AlpConstants::kAlpVectorSize);
  header.num_elements = static_cast<uint32_t>(element_count);

  std::memcpy(encoded_header, &header, header_size);
  *comp_size = header_size + compression_progress.num_compressed_bytes_produced;
}

template <typename T>
template <typename TargetType>
void AlpWrapper<T>::Decode(TargetType* decomp, uint32_t num_elements, const char* comp,
                           size_t comp_size) {
  const AlpHeader header = LoadHeader(comp, comp_size);
  const uint32_t vector_size = header.GetVectorSize();
  ARROW_CHECK(vector_size == AlpConstants::kAlpVectorSize)
      << "unsupported_vector_size: " << vector_size;

  const size_t header_size = AlpHeader::GetSizeForVersion(header.version);
  const char* compression_body = comp + header_size;
  const uint64_t compression_body_size = comp_size - header_size;

  ARROW_CHECK(header.GetCompressionMode() == AlpMode::kAlp)
      << "alp_decode_unsupported_mode";

  DecodeAlp<TargetType>(decomp, num_elements, compression_body, compression_body_size,
                        header.GetIntegerEncoding(), vector_size,
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
  // Add per-vector metadata sizes: AlpInfo (4 bytes) + ForInfo (6/10 bytes)
  const uint64_t vectors_count =
      static_cast<uint64_t>(std::ceil(static_cast<double>(element_count) / AlpConstants::kAlpVectorSize));
  max_alp_size +=
      (AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize) * vectors_count;
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
  // GROUPED METADATA LAYOUT:
  // [AlpInfo₀ | AlpInfo₁ | ... | AlpInfoₙ]     ← All ALP metadata (4B each)
  // [ForInfo₀ | ForInfo₁ | ... | ForInfoₙ]     ← All FOR metadata (6/10B each)
  // [Data₀ | Data₁ | ... | Dataₙ]               ← All data sections

  // Phase 1: Compress all vectors and collect them
  std::vector<AlpEncodedVector<T>> encoded_vectors;
  const uint64_t num_vectors =
      (element_count + AlpConstants::kAlpVectorSize - 1) / AlpConstants::kAlpVectorSize;
  encoded_vectors.reserve(num_vectors);

  uint64_t input_offset = 0;
  for (uint64_t remaining_elements = element_count; remaining_elements > 0;
       remaining_elements -= std::min(AlpConstants::kAlpVectorSize, remaining_elements)) {
    const uint64_t elements_to_encode =
        std::min(AlpConstants::kAlpVectorSize, remaining_elements);
    encoded_vectors.push_back(AlpCompression<T>::CompressVector(
        decomp + input_offset, static_cast<uint16_t>(elements_to_encode), combinations));
    input_offset += elements_to_encode;
  }

  // Calculate total size needed
  const uint64_t total_alp_info_size =
      encoded_vectors.size() * AlpEncodedVectorInfo::kStoredSize;
  const uint64_t total_for_info_size =
      encoded_vectors.size() * AlpEncodedForVectorInfo<T>::kStoredSize;
  uint64_t total_data_size = 0;
  for (const auto& vec : encoded_vectors) {
    total_data_size += vec.GetDataStoredSize();
  }
  const uint64_t total_size = total_alp_info_size + total_for_info_size + total_data_size;

  if (total_size > comp_size) {
    return CompressionProgress{0, 0};
  }

  // Phase 2: Write all AlpInfo first (ALP metadata section)
  uint64_t alp_info_offset = 0;
  for (const auto& vec : encoded_vectors) {
    vec.alp_info.Store({comp + alp_info_offset, AlpEncodedVectorInfo::kStoredSize});
    alp_info_offset += AlpEncodedVectorInfo::kStoredSize;
  }

  // Phase 3: Write all ForInfo next (FOR metadata section)
  uint64_t for_info_offset = total_alp_info_size;
  for (const auto& vec : encoded_vectors) {
    vec.for_info.Store({comp + for_info_offset, AlpEncodedForVectorInfo<T>::kStoredSize});
    for_info_offset += AlpEncodedForVectorInfo<T>::kStoredSize;
  }

  // Phase 4: Write all data sections consecutively
  uint64_t data_offset = total_alp_info_size + total_for_info_size;
  for (const auto& vec : encoded_vectors) {
    const uint64_t data_size = vec.GetDataStoredSize();
    vec.StoreDataOnly({comp + data_offset, data_size});
    data_offset += data_size;
  }

  ARROW_CHECK(data_offset == total_size)
      << "alp_encode_size_mismatch: " << data_offset << " vs " << total_size;

  return CompressionProgress{total_size, element_count};
}

template <typename T>
template <typename TargetType>
auto AlpWrapper<T>::DecodeAlp(TargetType* decomp, size_t decomp_element_count,
                              const char* comp, size_t comp_size,
                              AlpIntegerEncoding integer_encoding,
                              uint32_t vector_size, uint32_t total_elements)
    -> DecompressionProgress {
  // GROUPED METADATA LAYOUT:
  // [AlpInfo₀ | AlpInfo₁ | ... | AlpInfoₙ]     ← All ALP metadata (4B each)
  // [ForInfo₀ | ForInfo₁ | ... | ForInfoₙ]     ← All FOR metadata (6/10B each)
  // [Data₀ | Data₁ | ... | Dataₙ]               ← All data sections

  // Calculate number of vectors
  const uint32_t num_vectors =
      (total_elements + vector_size - 1) / vector_size;

  if (num_vectors == 0) {
    return DecompressionProgress{0, 0};
  }

  const uint64_t total_alp_info_size =
      static_cast<uint64_t>(num_vectors) * AlpEncodedVectorInfo::kStoredSize;
  const uint64_t total_for_info_size =
      static_cast<uint64_t>(num_vectors) * AlpEncodedForVectorInfo<T>::kStoredSize;
  const uint64_t total_metadata_size = total_alp_info_size + total_for_info_size;

  ARROW_CHECK(comp_size >= total_metadata_size)
      << "alp_decode_comp_size_too_small_for_metadata: " << comp_size << " vs "
      << total_metadata_size;

  // Load all metadata into cache (precomputes data offsets for O(1) random access)
  const AlpMetadataCache<T> cache = AlpMetadataCache<T>::Load(
      num_vectors, vector_size, total_elements,
      {comp, total_alp_info_size},
      {comp + total_alp_info_size, total_for_info_size});

  // Pointer to start of data section
  const char* data_section = comp + total_metadata_size;
  const size_t data_section_size = comp_size - total_metadata_size;

  // Decode each vector using the cache
  uint64_t output_offset = 0;
  for (uint32_t vector_index = 0; vector_index < cache.GetNumVectors(); vector_index++) {
    const uint16_t this_vector_elements = cache.GetVectorNumElements(vector_index);

    ARROW_CHECK(output_offset + this_vector_elements <= decomp_element_count)
        << "alp_decode_output_too_small: " << output_offset << " vs "
        << this_vector_elements << " vs " << decomp_element_count;

    // Use LoadViewDataOnly since AlpInfo and ForInfo are stored separately in the cache
    const uint64_t data_offset = cache.GetVectorDataOffset(vector_index);
    const AlpEncodedVectorView<T> encoded_view = AlpEncodedVectorView<T>::LoadViewDataOnly(
        {data_section + data_offset, data_section_size - data_offset},
        cache.GetAlpInfo(vector_index), cache.GetForInfo(vector_index), this_vector_elements);

    AlpCompression<T>::DecompressVectorView(encoded_view, integer_encoding,
                                            decomp + output_offset);

    output_offset += this_vector_elements;
  }

  return DecompressionProgress{output_offset, total_metadata_size + cache.GetTotalDataSize()};
}

// ----------------------------------------------------------------------
// Template instantiations

template class AlpWrapper<float>;
template class AlpWrapper<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
