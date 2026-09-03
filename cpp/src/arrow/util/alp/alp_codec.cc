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

#include "arrow/util/alp/alp_codec_internal.h"

#include <bit>
#include <cmath>
#include <limits>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/alp/alp_internal.h"
#include "arrow/util/alp/alp_sampler_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

namespace {

/// \brief ALP compression mode
///
/// Currently only ALP (decimal compression) is implemented.
///
/// The underlying type is fixed at `uint8_t` because this enum is serialized as a
/// single byte in `AlpHeader::compression_mode`.
enum class AlpMode : uint8_t { kAlp = 0 };

// ----------------------------------------------------------------------
// AlpHeader

/// \brief Header structure for ALP compression blocks
///
/// Contains page-level metadata for ALP compression. The num_elements field
/// stores the total element count for the page, allowing per-vector element
/// counts to be inferred (all vectors except the last have vector_size elements).
///
/// Note: num_elements is int32_t to match Parquet page headers (i32 for num_values).
/// See:
/// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
///
/// Note: log_vector_size stores the base-2 logarithm of the vector size.
/// The actual vector size is computed as: 1 << log_vector_size (i.e.,
/// 2^log_vector_size). For example, log_vector_size=10 means vector_size=1024.
/// LoadHeader rejects anything outside [kMinLogVectorSize, kMaxLogVectorSize],
/// so the representable vector sizes are 2^3 (8) through 2^15 (32768).
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
///   |  [Vector₀][Vector₁]...[Vectorₙ₋₁]            ← Concatenated       |
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
  /// Log base 2 of vector size. Actual vector size = 1 << log_vector_size.
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
  /// \pre value is positive and a power of two. Callers reach this only after
  ///      ValidateVectorSize has already rejected other inputs with
  ///      Status::Invalid, so a violation here is a programmer error rather
  ///      than malformed data; it is enforced with `ARROW_CHECK`, which aborts.
  static uint8_t Log2(int32_t value) {
    ARROW_CHECK(value > 0 && std::has_single_bit(static_cast<uint32_t>(value)))
        << "value_must_be_power_of_2: " << value;
    return static_cast<uint8_t>(std::countr_zero(static_cast<uint32_t>(value)));
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
  AlpMode GetCompressionMode() const { return static_cast<AlpMode>(compression_mode); }

  /// \brief Get the AlpIntegerEncoding enum from the stored uint8_t
  AlpIntegerEncoding GetIntegerEncoding() const {
    return static_cast<AlpIntegerEncoding>(integer_encoding);
  }
};

/// \brief Validate an element count against what the format can describe
///
/// The header stores the count as int32, so anything above INT32_MAX cannot be
/// written and must be refused before it is used to size a span or a buffer.
Status ValidateElementCount(int64_t num_elements) {
  if (num_elements < 0) {
    return Status::Invalid("ALP num_elements must be non-negative, got ", num_elements);
  }
  if (num_elements > std::numeric_limits<int32_t>::max()) {
    return Status::Invalid("ALP num_elements exceeds INT32_MAX, got ", num_elements);
  }
  return Status::OK();
}

}  // namespace

// ----------------------------------------------------------------------
// AlpCodec::AlpHeader definition

template <typename T>
struct AlpCodec<T>::AlpHeader : public ::arrow::util::alp::AlpHeader {};

// ----------------------------------------------------------------------
// AlpCodec implementation

template <typename T>
Result<typename AlpCodec<T>::AlpHeader> AlpCodec<T>::LoadHeader(const uint8_t* input,
                                                                int64_t input_size) {
  if (input_size < static_cast<int64_t>(AlpHeader::kSize)) {
    return Status::Invalid("ALP compressed buffer too small for header: ", input_size,
                           " < ", AlpHeader::kSize);
  }
  AlpHeader header{};
  header.compression_mode = util::SafeLoadAs<uint8_t>(input);
  header.integer_encoding = util::SafeLoadAs<uint8_t>(input + 1);
  header.log_vector_size = util::SafeLoadAs<uint8_t>(input + 2);
  header.num_elements = bit_util::FromLittleEndian(util::SafeLoadAs<int32_t>(input + 3));

  if (header.compression_mode != static_cast<uint8_t>(AlpMode::kAlp)) {
    return Status::Invalid("ALP unsupported compression mode: ",
                           static_cast<int>(header.compression_mode));
  }
  if (header.integer_encoding != static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack)) {
    return Status::Invalid("ALP unsupported integer encoding: ",
                           static_cast<int>(header.integer_encoding));
  }
  if (header.log_vector_size < AlpConstants::kMinLogVectorSize ||
      header.log_vector_size > AlpConstants::kMaxLogVectorSize) {
    return Status::Invalid(
        "ALP invalid log_vector_size: ", static_cast<int>(header.log_vector_size),
        " (must be in [", static_cast<int>(AlpConstants::kMinLogVectorSize), ", ",
        static_cast<int>(AlpConstants::kMaxLogVectorSize), "])");
  }
  if (header.num_elements < 0) {
    return Status::Invalid("ALP invalid num_elements: ", header.num_elements);
  }
  return header;
}

template <typename T>
Result<typename AlpCodec<T>::AlpSamplerResult> AlpCodec<T>::CreateSamplingPreset(
    const T* input, int64_t num_elements) {
  // Checked before the span below is formed: `num_elements` becomes its length.
  RETURN_NOT_OK(ValidateElementCount(num_elements));

  AlpSampler<T> sampler;
  sampler.AddSample({input, static_cast<size_t>(num_elements)});
  return sampler.Finalize();
}

namespace {

/// \brief Validate a caller-supplied vector_size against the format spec
///
/// The spec constrains `log_vector_size` to the inclusive range
/// [kMinLogVectorSize, kMaxLogVectorSize], so the vector size must be a power
/// of two within [2^3, 2^15].
Status ValidateVectorSize(int32_t vector_size) {
  constexpr int32_t kMin = 1 << AlpConstants::kMinLogVectorSize;
  constexpr int32_t kMax = 1 << AlpConstants::kMaxLogVectorSize;
  if (vector_size <= 0 || !std::has_single_bit(static_cast<uint32_t>(vector_size))) {
    return Status::Invalid("ALP vector_size must be a positive power of 2, got ",
                           vector_size);
  }
  if (vector_size < kMin || vector_size > kMax) {
    return Status::Invalid("ALP vector_size must be in [", kMin, ", ", kMax, "], got ",
                           vector_size);
  }
  return Status::OK();
}

}  // namespace

template <typename T>
Status AlpCodec<T>::EncodeWithPreset(const T* input, int64_t num_elements,
                                     const AlpSamplerResult& preset, int32_t vector_size,
                                     uint8_t* output, int64_t* output_size) {
  RETURN_NOT_OK(ValidateElementCount(num_elements));
  RETURN_NOT_OK(ValidateVectorSize(vector_size));

  // Make room to store header afterwards.
  uint8_t* encoded_header = output;
  uint8_t* body = output + AlpHeader::kSize;
  const int64_t remaining_output_size =
      *output_size - static_cast<int64_t>(AlpHeader::kSize);

  const CompressionProgress compression_progress =
      EncodeAlp(input, num_elements, preset.alp_parameters, vector_size, body,
                remaining_output_size);

  AlpHeader header{};
  header.compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  header.integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  header.log_vector_size = AlpHeader::Log2(vector_size);
  header.num_elements = static_cast<int32_t>(num_elements);

  util::SafeStore(encoded_header + 0, header.compression_mode);
  util::SafeStore(encoded_header + 1, header.integer_encoding);
  util::SafeStore(encoded_header + 2, header.log_vector_size);
  util::SafeStore(encoded_header + 3, bit_util::ToLittleEndian(header.num_elements));
  *output_size = static_cast<int64_t>(AlpHeader::kSize) +
                 compression_progress.num_compressed_bytes_produced;
  return Status::OK();
}

template <typename T>
Status AlpCodec<T>::Encode(const T* input, int64_t num_elements, int32_t vector_size,
                           uint8_t* output, int64_t* output_size) {
  ARROW_ASSIGN_OR_RAISE(auto sampling_result, CreateSamplingPreset(input, num_elements));
  return EncodeWithPreset(input, num_elements, sampling_result, vector_size, output,
                          output_size);
}

template <typename T>
Status AlpCodec<T>::Encode(const T* input, int64_t num_elements, uint8_t* output,
                           int64_t* output_size) {
  return Encode(input, num_elements, AlpConstants::kAlpVectorSize, output, output_size);
}

template <typename T>
template <typename TargetType>
Status AlpCodec<T>::Decode(int32_t num_elements, const uint8_t* input, int64_t input_size,
                           TargetType* output) {
  ARROW_ASSIGN_OR_RAISE(const AlpHeader header, LoadHeader(input, input_size));

  // Decoding is sized entirely from the header's count, so a header claiming
  // fewer elements would leave part of `output` unwritten and still return OK,
  // and a larger one would overrun it. Require the two to agree.
  if (header.num_elements != num_elements) {
    return Status::Invalid("ALP header element count ", header.num_elements,
                           " does not match the expected value count ", num_elements);
  }

  ARROW_ASSIGN_OR_RAISE(VectorReader reader, VectorReader::Open(input, input_size));

  int32_t decoded = 0;
  for (int32_t vector_index = 0; vector_index < reader.num_vectors(); ++vector_index) {
    ARROW_RETURN_NOT_OK(reader.DecodeVector(vector_index, output + decoded));
    decoded += reader.VectorLength(vector_index);
  }
  ARROW_CHECK(decoded == num_elements)
      << "alp_decode_element_count_mismatch: " << decoded << " vs " << num_elements;
  return Status::OK();
}

template Status AlpCodec<float>::Decode(int32_t num_elements, const uint8_t* input,
                                        int64_t input_size, float* output);
template Status AlpCodec<float>::Decode(int32_t num_elements, const uint8_t* input,
                                        int64_t input_size, double* output);
template Status AlpCodec<double>::Decode(int32_t num_elements, const uint8_t* input,
                                         int64_t input_size, double* output);

template <typename T>
Result<int32_t> AlpCodec<T>::DecodeElementCount(const uint8_t* input,
                                                int64_t input_size) {
  ARROW_ASSIGN_OR_RAISE(const AlpHeader header, LoadHeader(input, input_size));
  return header.num_elements;
}

template <typename T>
Result<int64_t> AlpCodec<T>::GetMaxCompressedSize(int64_t num_elements,
                                                  int32_t vector_size) {
  RETURN_NOT_OK(ValidateElementCount(num_elements));
  RETURN_NOT_OK(ValidateVectorSize(vector_size));
  int64_t max_alp_size = AlpHeader::kSize;

  const int64_t vectors_count = ::arrow::bit_util::CeilDiv(num_elements, vector_size);

  // Add offsets section (4 bytes per vector)
  max_alp_size += vectors_count * sizeof(AlpConstants::OffsetType);

  // Add per-vector metadata sizes: AlpInfo (4 bytes) + ForInfo (5/9 bytes)
  max_alp_size +=
      (AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize) *
      vectors_count;

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
    const T* input, int64_t element_count, const AlpEncodingParameters& preset,
    int32_t vector_size, uint8_t* output, int64_t output_size) {
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
  const int64_t num_vectors = ::arrow::bit_util::CeilDiv(element_count, vector_size);
  encoded_vectors.reserve(num_vectors);

  int64_t input_offset = 0;
  const int64_t vs = vector_size;
  for (int64_t remaining_elements = element_count; remaining_elements > 0;
       remaining_elements -= std::min(vs, remaining_elements)) {
    const int64_t elements_to_encode = std::min(vs, remaining_elements);
    encoded_vectors.push_back(AlpCompression<T>::CompressVector(
        input + input_offset, static_cast<uint16_t>(elements_to_encode), preset));
    input_offset += elements_to_encode;
  }

  // Phase 2: Calculate sizes and offsets
  const AlpIntegerEncoding integer_encoding = preset.integer_encoding;
  const int64_t per_vector_metadata_size =
      AlpEncodedVectorInfo::kStoredSize +
      GetIntegerEncodingMetadataSize<T>(integer_encoding);

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
    util::SafeStore(offset_ptr, bit_util::ToLittleEndian(offset));
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
Result<typename AlpCodec<T>::VectorReader> AlpCodec<T>::VectorReader::Open(
    const uint8_t* input, int64_t input_size) {
  // OFFSET-BASED LAYOUT:
  // [Header]                                  ← 7 bytes
  // [Offset₀ | Offset₁ | ... | Offsetₙ₋₁]    ← Byte offsets to each vector (4B each)
  // [AlpInfo₀ | ForInfo₀ | Data₀]             ← Vector 0 (interleaved)
  // [AlpInfo₁ | ForInfo₁ | Data₁]             ← Vector 1
  // ...
  //
  // Offsets are relative to the first byte after the header. Keeping a vector's
  // metadata next to its data gives O(1) access to any vector on its own, which
  // is what lets a caller decode a batch without touching the rest of the page.
  ARROW_ASSIGN_OR_RAISE(const AlpHeader header, LoadHeader(input, input_size));

  VectorReader reader;
  reader.body_ = input + AlpHeader::kSize;
  reader.body_size_ = input_size - static_cast<int64_t>(AlpHeader::kSize);
  reader.num_elements_ = header.num_elements;
  reader.vector_size_ = header.GetVectorSize();
  reader.integer_encoding_ = header.GetIntegerEncoding();

  if (reader.integer_encoding_ != AlpIntegerEncoding::kForBitPack) {
    return Status::Invalid("Unsupported ALP integer encoding: ",
                           static_cast<int>(reader.integer_encoding_));
  }

  const int32_t num_vectors = header.GetNumVectors();
  if (num_vectors == 0) {
    return reader;
  }

  const int64_t offsets_section_size =
      static_cast<int64_t>(num_vectors) * sizeof(AlpConstants::OffsetType);
  if (reader.body_size_ < offsets_section_size) {
    return Status::Invalid("ALP compressed buffer too small for offsets section: ",
                           reader.body_size_, " < ", offsets_section_size);
  }

  // Sanity check: each vector must have at least its metadata. Reject obviously
  // corrupted num_vectors before allocating (avoids OOM on malicious data).
  constexpr int64_t kMinBytesPerVector =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;
  if (offsets_section_size + static_cast<int64_t>(num_vectors) * kMinBytesPerVector >
      reader.body_size_) {
    return Status::Invalid("ALP num_vectors inconsistent with buffer size: num_vectors=",
                           num_vectors, ", input_size=", reader.body_size_);
  }

  // Read all offsets. The wire format is little-endian, so each offset is
  // converted rather than copied in bulk; FromLittleEndian is a no-op on a
  // little-endian host.
  reader.vector_offsets_.resize(num_vectors);
  for (int32_t i = 0; i < num_vectors; ++i) {
    reader.vector_offsets_[i] =
        bit_util::FromLittleEndian(util::SafeLoadAs<AlpConstants::OffsetType>(
            reader.body_ + i * sizeof(AlpConstants::OffsetType)));
  }

  // The spec fixes the whole offset array: vector 0 starts immediately after the
  // array, and every later vector starts where the previous one ended. Checking
  // each offset only against the buffer bounds would accept duplicate, backward
  // or gapped offsets, all of which decode the wrong bytes, so each offset is
  // compared against the position the previous vector ended at instead. Walking
  // the chain here means a later per-vector decode needs no check of its own.
  int64_t expected_offset = offsets_section_size;
  for (int32_t vector_index = 0; vector_index < num_vectors; ++vector_index) {
    const int64_t vector_offset = reader.vector_offsets_[vector_index];
    if (vector_offset != expected_offset) {
      return Status::Invalid("ALP vector ", vector_index, " starts at offset ",
                             vector_offset, " but the previous vector ends at ",
                             expected_offset);
    }
    ARROW_ASSIGN_OR_RAISE(const int64_t vector_size_in_bytes,
                          reader.VectorSizeInBytes(vector_index));
    expected_offset = vector_offset + vector_size_in_bytes;
  }
  if (expected_offset > reader.body_size_) {
    return Status::Invalid("ALP vectors run past the buffer: end=", expected_offset,
                           ", buffer_size=", reader.body_size_);
  }

  return reader;
}

template <typename T>
int32_t AlpCodec<T>::VectorReader::VectorLength(int32_t vector_index) const {
  ARROW_CHECK(vector_index >= 0 && vector_index < num_vectors())
      << "alp_vector_index_out_of_range: " << vector_index << " of " << num_vectors();
  if (vector_index == num_vectors() - 1) {
    const int32_t remainder = num_elements_ % vector_size_;
    return remainder == 0 ? vector_size_ : remainder;
  }
  return vector_size_;
}

template <typename T>
Result<typename AlpCodec<T>::VectorReader::VectorLayout>
AlpCodec<T>::VectorReader::LoadVectorLayout(int32_t vector_index) const {
  const int64_t vector_offset = vector_offsets_[vector_index];
  if (vector_offset >= body_size_) {
    return Status::Invalid("ALP vector offset out of bounds: offset=", vector_offset,
                           ", buffer_size=", body_size_);
  }
  const uint8_t* vector_start = body_ + vector_offset;
  const size_t remaining_bytes = static_cast<size_t>(body_size_ - vector_offset);

  constexpr size_t kMetadataSize =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;
  if (remaining_bytes < kMetadataSize) {
    return Status::Invalid(
        "ALP insufficient buffer for vector metadata: remaining=", remaining_bytes,
        ", metadata_size=", kMetadataSize, ", vector_index=", vector_index);
  }

  VectorLayout layout;
  ARROW_ASSIGN_OR_RAISE(layout.alp_info,
                        AlpEncodedVectorInfo::Load({vector_start, remaining_bytes}));
  ARROW_ASSIGN_OR_RAISE(layout.for_info,
                        AlpEncodedForVectorInfo<T>::Load(
                            {vector_start + AlpEncodedVectorInfo::kStoredSize,
                             remaining_bytes - AlpEncodedVectorInfo::kStoredSize}));

  layout.data = vector_start + kMetadataSize;
  layout.num_elements = VectorLength(vector_index);
  layout.data_size = layout.for_info.GetDataStoredSize(layout.num_elements,
                                                       layout.alp_info.num_exceptions());
  const int64_t data_remaining =
      body_size_ - vector_offset - static_cast<int64_t>(kMetadataSize);
  if (layout.data_size > data_remaining) {
    return Status::Invalid(
        "ALP insufficient buffer for vector data: need=", layout.data_size,
        ", remaining=", data_remaining, ", vector_index=", vector_index);
  }
  return layout;
}

template <typename T>
Result<int64_t> AlpCodec<T>::VectorReader::VectorSizeInBytes(int32_t vector_index) const {
  ARROW_ASSIGN_OR_RAISE(const VectorLayout layout, LoadVectorLayout(vector_index));
  constexpr int64_t kMetadataSize =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;
  return kMetadataSize + layout.data_size;
}

template <typename T>
template <typename TargetType>
Status AlpCodec<T>::VectorReader::DecodeVector(int32_t vector_index, TargetType* output) {
  if (vector_index < 0 || vector_index >= num_vectors()) {
    return Status::Invalid("ALP vector index out of range: ", vector_index, " of ",
                           num_vectors());
  }
  ARROW_ASSIGN_OR_RAISE(const VectorLayout layout, LoadVectorLayout(vector_index));

  RETURN_NOT_OK(
      decode_view_.ResetDataOnly({layout.data, static_cast<size_t>(layout.data_size)},
                                 layout.alp_info, layout.for_info, layout.num_elements));

  // resize() keeps whatever the previous vector allocated, so a run of vectors
  // pays for the largest one rather than for each one.
  unpacked_integers_.resize(layout.num_elements);
  AlpCompression<T>::DecompressVectorView(decode_view_, integer_encoding_, output,
                                          unpacked_integers_);
  return Status::OK();
}

template Status AlpCodec<float>::VectorReader::DecodeVector(int32_t, float*);
template Status AlpCodec<float>::VectorReader::DecodeVector(int32_t, double*);
template Status AlpCodec<double>::VectorReader::DecodeVector(int32_t, double*);

// ----------------------------------------------------------------------
// Template instantiations

template class AlpCodec<float>;
template class AlpCodec<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
