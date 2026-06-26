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

#include "arrow/util/alp/alp_codec.h"

#include <cmath>
#include <limits>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp.h"
#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/alp/alp_sampler.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

// ALP serialization uses memcpy for multi-byte integers (header fields,
// offsets, frame_of_reference) and assumes little-endian byte order on disk.
static_assert(ARROW_LITTLE_ENDIAN,
              "ALP serialization assumes little-endian byte order");

namespace {

// ----------------------------------------------------------------------
// AlpHeader

/// \brief Header structure for ALP compression blocks
///
/// Contains page-level metadata for ALP compression. The num_elements field
/// stores the total element count for the page, allowing per-vector element
/// counts to be inferred (all vectors except the last have vector_size elements).
///
/// Note: num_elements is int32_t to match Parquet page headers (i32 for num_values).
/// See: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
///
/// Note: log_vector_size stores the base-2 logarithm of the vector size.
/// The actual vector size is computed as: 1u << log_vector_size (i.e., 2^log_vector_size).
/// For example, log_vector_size=10 means vector_size=1024.
/// This allows representing any power-of-2 vector size up to 2^255 in a single byte.
///
/// Header format (7 bytes):
///
///   +---------------------------------------------------+
///   |  AlpHeader (7 bytes)                              |
///   +---------------------------------------------------+
///   |  Offset |  Field              |  Size             |
///   +---------+---------------------+-------------------+
///   |    0    |  compression_mode   |  1 byte (uint8)   |
///   |    1    |  integer_encoding   |  1 byte (uint8)   |
///   |    2    |  log_vector_size    |  1 byte (uint8)   |
///   |    3    |  num_elements       |  4 bytes (int32)  |
///   +---------------------------------------------------+
///
/// Page-level layout (offset-based interleaved for O(1) random access):
///
///   +-------------------------------------------------------------------+
///   |  [AlpHeader (7B)]                                                 |
///   |  [Offset₀ | Offset₁ | ... | Offsetₙ₋₁]       ← Vector offsets     |
///   |  [Vector₀][Vector₁]...[Vectorₙ₋₁]            ← Interleaved data   |
///   +-------------------------------------------------------------------+
///   where each Vector = [AlpInfo | ForInfo | Data]
///
/// This layout enables O(1) random access to any vector by:
/// 1. Reading the offset for target vector (direct lookup)
/// 2. Jumping to that offset to read metadata + data together
struct AlpHeader {
  /// Compression mode (currently only kAlp is supported).
  uint8_t compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  /// Integer encoding method used (currently only kForBitPack is supported).
  uint8_t integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  /// Log base 2 of vector size. Actual vector size = 1u << log_vector_size.
  /// For example: 10 means 2^10 = 1024 elements per vector.
  uint8_t log_vector_size = 0;
  /// Total number of elements in the page (int32_t to match Parquet's i32 num_values).
  /// Per-vector element count is inferred: vector_size for all but the last vector.
  int32_t num_elements = 0;

  /// Size of the serialized header in bytes.
  static constexpr size_t kSize = 7;

  /// \brief Calculate the number of vectors from total elements and vector size
  ///
  /// \return number of vectors (full + partial if any)
  int32_t GetNumVectors() const {
    const int32_t vector_size = GetVectorSize();
    return static_cast<int32_t>(::arrow::bit_util::CeilDiv(num_elements, vector_size));
  }

  /// \brief Get the size of the offsets section
  ///
  /// \return size in bytes of the offsets array (num_vectors * sizeof(OffsetType))
  int64_t GetOffsetsSectionSize() const {
    return static_cast<int64_t>(GetNumVectors()) * sizeof(AlpConstants::OffsetType);
  }

  /// \brief Compute the actual vector size from log_vector_size
  ///
  /// \return the vector size (2^log_vector_size)
  int32_t GetVectorSize() const { return 1 << log_vector_size; }

  /// \brief Compute log base 2 of a power-of-2 value
  ///
  /// \param[in] value a power-of-2 value
  /// \return the log base 2 of value
  static uint8_t Log2(int32_t value) {
    ARROW_CHECK(value > 0 && (value & (value - 1)) == 0)
        << "value_must_be_power_of_2: " << value;
    return static_cast<uint8_t>(__builtin_ctz(static_cast<unsigned>(value)));
  }

  /// \brief Calculate the number of elements for a given vector index
  ///
  /// \param[in] vector_index the 0-based index of the vector
  /// \return the number of elements in this vector, or error if index is out of range
  Result<int32_t> GetVectorNumElements(int32_t vector_index) const {
    const int32_t vector_size = GetVectorSize();
    const int32_t num_full_vectors = num_elements / vector_size;
    const int32_t remainder = num_elements % vector_size;
    if (vector_index < num_full_vectors) {
      return vector_size;  // Full vector
    } else if (vector_index == num_full_vectors && remainder > 0) {
      return remainder;  // Last partial vector
    }
    return Status::Invalid("ALP invalid vector index: ", vector_index,
                           " (num_vectors=", GetNumVectors(), ")");
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
// AlpCodec::AlpHeader definition

template <typename T>
struct AlpCodec<T>::AlpHeader : public ::arrow::util::alp::AlpHeader {
};

// ----------------------------------------------------------------------
// AlpCodec implementation

template <typename T>
Result<typename AlpCodec<T>::AlpHeader> AlpCodec<T>::LoadHeader(
    const uint8_t* input, int64_t input_size) {
  if (input_size < static_cast<int64_t>(AlpHeader::kSize)) {
    return Status::Invalid("ALP compressed buffer too small for header: ", input_size,
                           " < ", AlpHeader::kSize);
  }
  AlpHeader header{};
  header.compression_mode = util::SafeLoadAs<uint8_t>(input);
  header.integer_encoding = util::SafeLoadAs<uint8_t>(input + 1);
  header.log_vector_size = util::SafeLoadAs<uint8_t>(input + 2);
  header.num_elements = util::SafeLoadAs<int32_t>(input + 3);

  if (header.compression_mode != static_cast<uint8_t>(AlpMode::kAlp)) {
    return Status::Invalid("ALP unsupported compression mode: ",
                           static_cast<int>(header.compression_mode));
  }
  if (header.integer_encoding !=
      static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack)) {
    return Status::Invalid("ALP unsupported integer encoding: ",
                           static_cast<int>(header.integer_encoding));
  }
  if (header.log_vector_size == 0 ||
      header.log_vector_size > AlpConstants::kMaxLogVectorSize) {
    return Status::Invalid("ALP invalid log_vector_size: ",
                           static_cast<int>(header.log_vector_size));
  }
  if (header.num_elements < 0) {
    return Status::Invalid("ALP invalid num_elements: ", header.num_elements);
  }
  return header;
}

template <typename T>
typename AlpCodec<T>::AlpSamplerResult AlpCodec<T>::CreateSamplingPreset(
    const T* input, int64_t num_elements) {
  ARROW_CHECK(num_elements >= 0)
      << "alp_encode_num_elements_must_be_non_negative";

  AlpSampler<T> sampler;
  sampler.AddSample({input, static_cast<size_t>(num_elements)});
  return sampler.Finalize();
}

template <typename T>
void AlpCodec<T>::EncodeWithPreset(const T* input, int64_t num_elements,
                                     const AlpSamplerResult& preset,
                                     int32_t vector_size,
                                     uint8_t* output, int64_t* output_size) {
  ARROW_CHECK(num_elements >= 0)
      << "alp_encode_num_elements_must_be_non_negative";
  ARROW_CHECK(num_elements <= std::numeric_limits<int32_t>::max())
      << "alp_num_elements_exceeds_int32_max: " << num_elements;
  ARROW_CHECK(vector_size > 0 && (vector_size & (vector_size - 1)) == 0)
      << "alp_vector_size_must_be_power_of_2: " << vector_size;
  ARROW_CHECK(vector_size <= (1 << AlpConstants::kMaxLogVectorSize))
      << "alp_vector_size_exceeds_max: " << vector_size;

  // Make room to store header afterwards.
  uint8_t* encoded_header = output;
  uint8_t* body = output + AlpHeader::kSize;
  const int64_t remaining_output_size = *output_size - static_cast<int64_t>(AlpHeader::kSize);

  const CompressionProgress compression_progress =
      EncodeAlp(input, num_elements, body, remaining_output_size,
                preset.alp_parameters, vector_size);

  AlpHeader header{};
  header.compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  header.integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  header.log_vector_size = AlpHeader::Log2(vector_size);
  header.num_elements = static_cast<int32_t>(num_elements);

  util::SafeStore(encoded_header + 0, header.compression_mode);
  util::SafeStore(encoded_header + 1, header.integer_encoding);
  util::SafeStore(encoded_header + 2, header.log_vector_size);
  util::SafeStore(encoded_header + 3, header.num_elements);
  *output_size = static_cast<int64_t>(AlpHeader::kSize) +
                 compression_progress.num_compressed_bytes_produced;
}

template <typename T>
void AlpCodec<T>::Encode(const T* input, int64_t num_elements,
                           int32_t vector_size,
                           uint8_t* output, int64_t* output_size) {
  auto sampling_result = CreateSamplingPreset(input, num_elements);
  EncodeWithPreset(input, num_elements, sampling_result, vector_size, output, output_size);
}

template <typename T>
void AlpCodec<T>::Encode(const T* input, int64_t num_elements,
                           uint8_t* output, int64_t* output_size) {
  Encode(input, num_elements, AlpConstants::kAlpVectorSize, output, output_size);
}

template <typename T>
template <typename TargetType>
Status AlpCodec<T>::Decode(int32_t num_elements, const uint8_t* input, int64_t input_size,
                             TargetType* output) {
  ARROW_ASSIGN_OR_RAISE(const AlpHeader header, LoadHeader(input, input_size));
  const int32_t vector_size = header.GetVectorSize();

  const uint8_t* body = input + AlpHeader::kSize;
  const int64_t body_size = input_size - static_cast<int64_t>(AlpHeader::kSize);

  ARROW_RETURN_NOT_OK(
      DecodeAlp<TargetType>(num_elements, body, body_size,
                            header.GetIntegerEncoding(), vector_size,
                            header.num_elements, output)
          .status());
  return Status::OK();
}

template Status AlpCodec<float>::Decode(int32_t num_elements, const uint8_t* input,
                                          int64_t input_size, float* output);
template Status AlpCodec<float>::Decode(int32_t num_elements, const uint8_t* input,
                                          int64_t input_size, double* output);
template Status AlpCodec<double>::Decode(int32_t num_elements, const uint8_t* input,
                                           int64_t input_size, double* output);

template <typename T>
int64_t AlpCodec<T>::GetMaxCompressedSize(int64_t num_elements,
                                           int32_t vector_size) {
  ARROW_CHECK(num_elements >= 0) << "alp_num_elements_must_be_non_negative";
  int64_t max_alp_size = AlpHeader::kSize;

  const int64_t vectors_count =
      ::arrow::bit_util::CeilDiv(num_elements, vector_size);

  // Add offsets section (4 bytes per vector)
  max_alp_size += vectors_count * sizeof(AlpConstants::OffsetType);

  // Add per-vector metadata sizes: AlpInfo (4 bytes) + ForInfo (5/9 bytes)
  max_alp_size +=
      (AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize) * vectors_count;

  // Worst case: everything is an exception, except two values that are chosen
  // with large difference to make FOR encoding for placeholders impossible.
  // Values/placeholders.
  max_alp_size += num_elements * static_cast<int64_t>(sizeof(T));
  // Exceptions.
  max_alp_size += num_elements * static_cast<int64_t>(sizeof(T));
  // Exception positions.
  max_alp_size += num_elements * static_cast<int64_t>(sizeof(AlpConstants::PositionType));

  return max_alp_size;
}

template <typename T>
typename AlpCodec<T>::CompressionProgress AlpCodec<T>::EncodeAlp(
    const T* input, int64_t element_count, uint8_t* output,
    int64_t output_size, const AlpEncodingParameters& preset,
    int32_t vector_size) {
  // OFFSET-BASED LAYOUT
  // [Offset₀ | Offset₁ | ... | Offsetₙ₋₁]    ← Byte offsets to each vector (4B each)
  // [AlpInfo₀ | ForInfo₀ | Data₀]             ← Vector 0 (interleaved)
  // [AlpInfo₁ | ForInfo₁ | Data₁]             ← Vector 1
  // ...
  // [AlpInfoₙ₋₁ | ForInfoₙ₋₁ | Dataₙ₋₁]       ← Vector n-1
  //
  // Benefits:
  // - O(1) random access to any vector (no cumulative offset computation)
  // - Better locality for single-vector access (metadata + data together)
  // - Enables parallel decompression without coordination

  // Phase 1: Compress all vectors and collect them
  std::vector<AlpEncodedVector<T>> encoded_vectors;
  const int64_t num_vectors =
      ::arrow::bit_util::CeilDiv(element_count, vector_size);
  encoded_vectors.reserve(num_vectors);

  int64_t input_offset = 0;
  const int64_t vs = vector_size;
  for (int64_t remaining_elements = element_count; remaining_elements > 0;
       remaining_elements -= std::min(vs, remaining_elements)) {
    const int64_t elements_to_encode =
        std::min(vs, remaining_elements);
    encoded_vectors.push_back(AlpCompression<T>::CompressVector(
        input + input_offset, static_cast<uint16_t>(elements_to_encode), preset));
    input_offset += elements_to_encode;
  }

  // Phase 2: Calculate sizes and offsets
  const AlpIntegerEncoding integer_encoding = preset.integer_encoding;
  const int64_t per_vector_metadata_size =
      AlpEncodedVectorInfo::kStoredSize + GetIntegerEncodingMetadataSize<T>(integer_encoding);

  // Offsets section comes first (after header, which is written by Encode())
  const int64_t offsets_section_size =
      num_vectors * static_cast<int64_t>(sizeof(AlpConstants::OffsetType));

  // Calculate total size and per-vector offsets
  std::vector<AlpConstants::OffsetType> vector_offsets;
  vector_offsets.reserve(num_vectors);

  // First vector starts right after the offsets section
  int64_t current_offset = offsets_section_size;
  for (const auto& vec : encoded_vectors) {
    // Store offset to this vector (relative to start of body, after header)
    vector_offsets.push_back(static_cast<AlpConstants::OffsetType>(current_offset));
    // Advance by metadata + data size
    current_offset += per_vector_metadata_size + vec.GetDataStoredSize();
  }
  const int64_t total_size = current_offset;

  if (total_size > output_size) {
    return CompressionProgress{0, 0};
  }

  // Phase 3: Write offsets section
  uint8_t* offset_ptr = output;
  for (const auto& offset : vector_offsets) {
    util::SafeStore(offset_ptr, offset);
    offset_ptr += sizeof(AlpConstants::OffsetType);
  }

  // Phase 4: Write interleaved vectors [AlpInfo | ForInfo | Data]
  for (size_t i = 0; i < encoded_vectors.size(); i++) {
    const auto& vec = encoded_vectors[i];
    uint8_t* vector_start = output + vector_offsets[i];

    // Write AlpInfo
    vec.alp_info().Store({vector_start, AlpEncodedVectorInfo::kStoredSize});
    uint8_t* ptr = vector_start + AlpEncodedVectorInfo::kStoredSize;

    // Write ForInfo — only kForBitPack is supported; validated at the API boundary
    vec.for_info().Store({ptr, AlpEncodedForVectorInfo<T>::kStoredSize});
    ptr += AlpEncodedForVectorInfo<T>::kStoredSize;

    // Write data (packed values + exception positions + exception values)
    const int64_t data_size = vec.GetDataStoredSize();
    vec.StoreDataOnly({ptr, static_cast<size_t>(data_size)});
  }

  return CompressionProgress{total_size, element_count};
}

template <typename T>
template <typename TargetType>
Result<typename AlpCodec<T>::DecompressionProgress> AlpCodec<T>::DecodeAlp(
    int64_t num_elements,
    const uint8_t* input, int64_t input_size,
    AlpIntegerEncoding integer_encoding,
    int32_t vector_size, int32_t total_elements,
    TargetType* output) {
  // OFFSET-BASED LAYOUT:
  // [Offset₀ | Offset₁ | ... | Offsetₙ₋₁]    ← Byte offsets to each vector (4B each)
  // [AlpInfo₀ | ForInfo₀ | Data₀]             ← Vector 0 (interleaved)
  // [AlpInfo₁ | ForInfo₁ | Data₁]             ← Vector 1
  // ...
  //
  // Benefits:
  // - O(1) random access to any vector (no cumulative offset computation)
  // - Better locality for single-vector access (metadata + data together)
  // - Enables parallel decompression without coordination

  // Calculate number of vectors
  const int32_t num_vectors =
      static_cast<int32_t>(::arrow::bit_util::CeilDiv(total_elements, vector_size));

  if (num_vectors == 0) {
    return DecompressionProgress{0, 0};
  }

  const int64_t offsets_section_size =
      static_cast<int64_t>(num_vectors) * sizeof(AlpConstants::OffsetType);
  if (input_size < offsets_section_size) {
    return Status::Invalid("ALP compressed buffer too small for offsets section: ",
                           input_size, " < ", offsets_section_size);
  }

  // Sanity check: each vector must have at least its metadata. Reject obviously
  // corrupted num_vectors before allocating (avoids OOM on malicious data).
  constexpr int64_t kMinBytesPerVector =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;
  if (offsets_section_size + static_cast<int64_t>(num_vectors) * kMinBytesPerVector >
      input_size) {
    return Status::Invalid(
        "ALP num_vectors inconsistent with buffer size: num_vectors=", num_vectors,
        ", input_size=", input_size);
  }

  // Read all offsets
  std::vector<AlpConstants::OffsetType> vector_offsets(num_vectors);
  std::memcpy(vector_offsets.data(), input,
              num_vectors * sizeof(AlpConstants::OffsetType));

  // Decode each vector using its offset for O(1) random access
  int64_t output_offset = 0;
  int64_t bytes_consumed = offsets_section_size;

  for (int32_t vector_index = 0; vector_index < num_vectors; vector_index++) {
    // Calculate number of elements in this vector
    const int32_t num_full_vectors = total_elements / vector_size;
    const int32_t remainder = total_elements % vector_size;
    int32_t this_vector_elements;
    if (vector_index < num_full_vectors) {
      this_vector_elements = vector_size;
    } else if (vector_index == num_full_vectors && remainder > 0) {
      this_vector_elements = remainder;
    } else {
      return Status::Invalid("ALP vector index out of range: ", vector_index,
                             " (total_elements=", total_elements,
                             ", vector_size=", vector_size, ")");
    }

    if (output_offset + this_vector_elements > num_elements) {
      return Status::Invalid("ALP decode output buffer too small: offset=",
                             output_offset, " + elements=", this_vector_elements,
                             " > capacity=", num_elements);
    }

    // Validate offset is within bounds and enough buffer remains for metadata
    const int64_t vector_offset = vector_offsets[vector_index];
    constexpr int64_t kMinVectorMetadataSize =
        AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;
    if (vector_offset + kMinVectorMetadataSize > input_size) {
      return Status::Invalid("ALP vector offset out of bounds or insufficient buffer "
                             "for metadata: offset=", vector_offset,
                             ", metadata_size=", kMinVectorMetadataSize,
                             ", buffer_size=", input_size);
    }

    // Jump directly to this vector using its offset
    const uint8_t* vector_start =
        input + vector_offset;
    const int64_t remaining = input_size - vector_offset;

    // Read AlpInfo (interleaved)
    const size_t remaining_bytes = static_cast<size_t>(remaining);
    ARROW_ASSIGN_OR_RAISE(const AlpEncodedVectorInfo alp_info,
                          AlpEncodedVectorInfo::Load({vector_start, remaining_bytes}));
    const uint8_t* ptr = vector_start + AlpEncodedVectorInfo::kStoredSize;

    // Validate integer encoding before decoding
    if (integer_encoding != AlpIntegerEncoding::kForBitPack) {
      return Status::Invalid("Unsupported ALP integer encoding: ",
                             static_cast<int>(integer_encoding));
    }

    // Read ForInfo (interleaved)
    ARROW_ASSIGN_OR_RAISE(
        const AlpEncodedForVectorInfo<T> for_info,
        AlpEncodedForVectorInfo<T>::Load(
            {ptr, remaining_bytes - AlpEncodedVectorInfo::kStoredSize}));
    ptr += AlpEncodedForVectorInfo<T>::kStoredSize;

    // Validate enough buffer remains for the data section
    const int64_t data_remaining =
        input_size - (ptr - input);
    const int64_t data_size =
        for_info.GetDataStoredSize(this_vector_elements, alp_info.num_exceptions());
    if (data_size > data_remaining) {
      return Status::Invalid("ALP insufficient buffer for vector data: need=",
                             data_size, ", remaining=", data_remaining,
                             ", vector_index=", vector_index);
    }

    // Load view from data section (packed values + exceptions)
    ARROW_ASSIGN_OR_RAISE(
        const AlpEncodedVectorView<T> encoded_view,
        AlpEncodedVectorView<T>::LoadViewDataOnly(
            {ptr, static_cast<size_t>(data_remaining)},
            alp_info, for_info, this_vector_elements));

    AlpCompression<T>::DecompressVectorView(encoded_view, integer_encoding,
                                            output + output_offset);

    // Track bytes consumed for last vector
    if (vector_index == num_vectors - 1) {
      bytes_consumed = (ptr - input) +
          for_info.GetDataStoredSize(this_vector_elements, alp_info.num_exceptions());
    }

    output_offset += this_vector_elements;
  }

  return DecompressionProgress{output_offset, bytes_consumed};
}

// ----------------------------------------------------------------------
// Template instantiations

template class AlpCodec<float>;
template class AlpCodec<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
