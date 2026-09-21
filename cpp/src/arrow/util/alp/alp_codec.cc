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

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <span>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp_compression_internal.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/alp/alp_metadata_internal.h"
#include "arrow/util/alp/alp_sampler_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow::util::alp {

namespace {

// Page-level ALP header.
struct AlpHeader {
  uint8_t compression_mode{static_cast<uint8_t>(AlpMode::kAlp)};
  uint8_t integer_encoding{static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack)};
  uint8_t log_vector_size{0};
  int32_t num_elements{0};

  static constexpr size_t kSize = 7;

  int32_t GetNumVectors() const {
    const int32_t vector_size = GetVectorSize();
    return static_cast<int32_t>(::arrow::bit_util::CeilDiv(num_elements, vector_size));
  }

  int32_t GetVectorSize() const { return 1 << log_vector_size; }
};

// The header stores the element count as int32.
Status ValidateElementCount(size_t num_elements) {
  if (num_elements > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
    return Status::Invalid("ALP num_elements exceeds INT32_MAX, got ", num_elements);
  }
  return Status::OK();
}

Result<AlpHeader> LoadHeader(std::span<const uint8_t> input) {
  if (input.size() < AlpHeader::kSize) {
    return Status::Invalid("ALP compressed buffer too small for header: ", input.size(),
                           " < ", AlpHeader::kSize);
  }
  const uint8_t* ptr = input.data();
  AlpHeader header{};
  header.compression_mode = util::SafeLoadAs<uint8_t>(ptr);
  header.integer_encoding = util::SafeLoadAs<uint8_t>(ptr + 1);
  header.log_vector_size = util::SafeLoadAs<uint8_t>(ptr + 2);
  header.num_elements = bit_util::FromLittleEndian(util::SafeLoadAs<int32_t>(ptr + 3));

  if (header.compression_mode != static_cast<uint8_t>(AlpMode::kAlp)) {
    return Status::Invalid("ALP unsupported compression mode: ",
                           static_cast<int>(header.compression_mode));
  }
  if (header.integer_encoding != static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack)) {
    return Status::Invalid("ALP unsupported integer encoding: ",
                           static_cast<int>(header.integer_encoding));
  }
  if (header.log_vector_size < AlpFormatConstants::kMinLogVectorSize ||
      header.log_vector_size > AlpFormatConstants::kMaxLogVectorSize) {
    return Status::Invalid(
        "ALP invalid log_vector_size: ", static_cast<int>(header.log_vector_size),
        " (must be in [", static_cast<int>(AlpFormatConstants::kMinLogVectorSize), ", ",
        static_cast<int>(AlpFormatConstants::kMaxLogVectorSize), "])");
  }
  if (header.num_elements < 0) {
    return Status::Invalid("ALP invalid num_elements: ", header.num_elements);
  }
  return header;
}

template <AlpFloatingType T>
Status ValidateEncodingPreset(const AlpEncodingPreset& preset) {
  if (preset.combinations.empty()) {
    return Status::Invalid("ALP encoding preset must contain at least one combination");
  }
  if (preset.integer_encoding != AlpIntegerEncoding::kForBitPack) {
    return Status::Invalid("ALP encoding preset uses unsupported integer encoding: ",
                           static_cast<int>(preset.integer_encoding));
  }
  for (const AlpExponentAndFactor& combination : preset.combinations) {
    if (combination.exponent > AlpTypedConstants<T>::kMaxExponent) {
      return Status::Invalid("ALP preset exponent ",
                             static_cast<int>(combination.exponent), " exceeds ",
                             static_cast<int>(AlpTypedConstants<T>::kMaxExponent));
    }
    if (combination.factor > combination.exponent) {
      return Status::Invalid("ALP preset factor ", static_cast<int>(combination.factor),
                             " exceeds exponent ",
                             static_cast<int>(combination.exponent));
    }
  }
  return Status::OK();
}

// The spec allows vector sizes from 8 to 32768, as powers of two.
Status ValidateVectorSize(int32_t vector_size) {
  constexpr int32_t kMin = 1 << AlpFormatConstants::kMinLogVectorSize;
  constexpr int32_t kMax = 1 << AlpFormatConstants::kMaxLogVectorSize;
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

template <AlpFloatingType T>
Result<int64_t> EncodePage(std::span<const T> inputs, const AlpEncodingPreset& preset,
                           int32_t vector_size, std::span<uint8_t> output) {
  std::span<uint8_t> body = output.subspan(AlpHeader::kSize);
  const int64_t element_count = static_cast<int64_t>(inputs.size());

  // Phase 1: compress all vectors and collect them.
  std::vector<AlpEncodedVector<T>> encoded_vectors;
  const int64_t num_vectors = ::arrow::bit_util::CeilDiv(element_count, vector_size);
  encoded_vectors.reserve(num_vectors);

  int64_t input_offset = 0;
  for (int64_t remaining_elements = element_count; remaining_elements > 0;) {
    const int64_t elements_to_encode =
        std::min(static_cast<int64_t>(vector_size), remaining_elements);
    encoded_vectors.push_back(AlpCompression<T>::Compress(
        inputs.subspan(static_cast<size_t>(input_offset),
                       static_cast<size_t>(elements_to_encode)),
        preset));
    input_offset += elements_to_encode;
    remaining_elements -= elements_to_encode;
  }

  // Phase 2: calculate sizes and offsets.
  const int64_t per_vector_metadata_size =
      AlpInfo::kStoredSize + GetIntegerEncodingMetadataSize<T>(preset.integer_encoding);
  const int64_t offsets_section_size =
      num_vectors * static_cast<int64_t>(sizeof(AlpFormatConstants::OffsetType));

  std::vector<AlpFormatConstants::OffsetType> vector_offsets;
  vector_offsets.reserve(num_vectors);

  int64_t current_offset = offsets_section_size;
  for (const auto& vec : encoded_vectors) {
    if (current_offset >
        static_cast<int64_t>(
            std::numeric_limits<AlpFormatConstants::OffsetType>::max())) {
      return Status::Invalid("ALP encoded data exceeds the uint32 offset range");
    }
    vector_offsets.push_back(static_cast<AlpFormatConstants::OffsetType>(current_offset));
    current_offset += per_vector_metadata_size + vec.GetDataStoredSize();
  }
  const int64_t body_size = current_offset;
  if (body_size > static_cast<int64_t>(body.size())) {
    return Status::Invalid("ALP output buffer too small: ", body.size(), " < ",
                           body_size);
  }

  // Phase 3: write the offset table.
  uint8_t* offset_ptr = body.data();
  for (const auto& offset : vector_offsets) {
    util::SafeStore(offset_ptr, bit_util::ToLittleEndian(offset));
    offset_ptr += sizeof(AlpFormatConstants::OffsetType);
  }

  // Phase 4: write interleaved vectors [AlpInfo | ForInfo | Data].
  for (size_t i = 0; i < encoded_vectors.size(); ++i) {
    const auto& vec = encoded_vectors[i];
    const int64_t stored_size = per_vector_metadata_size + vec.GetDataStoredSize();
    vec.Store(body.subspan(vector_offsets[i], static_cast<size_t>(stored_size)));
  }
  AlpHeader header{};
  header.compression_mode = static_cast<uint8_t>(AlpMode::kAlp);
  header.integer_encoding = static_cast<uint8_t>(AlpIntegerEncoding::kForBitPack);
  header.log_vector_size =
      static_cast<uint8_t>(std::countr_zero(static_cast<uint32_t>(vector_size)));
  header.num_elements = static_cast<int32_t>(inputs.size());

  uint8_t* header_ptr = output.data();
  util::SafeStore(header_ptr + 0, header.compression_mode);
  util::SafeStore(header_ptr + 1, header.integer_encoding);
  util::SafeStore(header_ptr + 2, header.log_vector_size);
  util::SafeStore(header_ptr + 3, bit_util::ToLittleEndian(header.num_elements));
  return static_cast<int64_t>(AlpHeader::kSize) + body_size;
}

}  // namespace

template <AlpFloatingType T>
Result<AlpEncodingPreset> AlpCodec<T>::MakePreset(std::span<const T> inputs) {
  RETURN_NOT_OK(ValidateElementCount(inputs.size()));

  AlpSampler<T> sampler;
  sampler.AddSample(inputs);
  return sampler.MakePreset();
}

template <AlpFloatingType T>
Result<int64_t> AlpCodec<T>::Encode(std::span<const T> inputs,
                                    const AlpEncodingPreset& preset, int32_t vector_size,
                                    std::span<uint8_t> output) {
  RETURN_NOT_OK(ValidateElementCount(inputs.size()));
  RETURN_NOT_OK(ValidateVectorSize(vector_size));
  RETURN_NOT_OK(ValidateEncodingPreset<T>(preset));
  if (output.size() < AlpHeader::kSize) {
    return Status::Invalid("ALP output buffer too small for header: ", output.size(),
                           " < ", AlpHeader::kSize);
  }

  return EncodePage(inputs, preset, vector_size, output);
}

template <AlpFloatingType T>
Result<int64_t> AlpCodec<T>::Encode(std::span<const T> inputs, int32_t vector_size,
                                    std::span<uint8_t> output) {
  ARROW_ASSIGN_OR_RAISE(AlpEncodingPreset preset, MakePreset(inputs));
  return Encode(inputs, preset, vector_size, output);
}

template <AlpFloatingType T>
Result<int64_t> AlpCodec<T>::GetMaxCompressedSize(int64_t num_elements,
                                                  int32_t vector_size) {
  if (num_elements < 0) {
    return Status::Invalid("ALP num_elements must be non-negative, got ", num_elements);
  }
  RETURN_NOT_OK(ValidateElementCount(static_cast<size_t>(num_elements)));
  RETURN_NOT_OK(ValidateVectorSize(vector_size));
  int64_t max_alp_size = AlpHeader::kSize;

  const int64_t vectors_count = ::arrow::bit_util::CeilDiv(num_elements, vector_size);

  // Offsets section.
  max_alp_size += vectors_count * sizeof(AlpFormatConstants::OffsetType);

  // Per-vector AlpInfo and ForInfo.
  max_alp_size += (AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize) * vectors_count;

  // Full-width packed values.
  max_alp_size += num_elements * static_cast<int64_t>(sizeof(T));
  // Exception values.
  max_alp_size += num_elements * static_cast<int64_t>(sizeof(T));
  // Exception positions.
  max_alp_size +=
      num_elements * static_cast<int64_t>(sizeof(AlpFormatConstants::PositionType));

  return max_alp_size;
}

template <AlpFloatingType T>
Result<AlpVectorReader<T>> AlpVectorReader<T>::Open(std::span<const uint8_t> input,
                                                    MemoryPool* pool) {
  // Offsets are relative to the first byte after the header. Validate the whole
  // chain once here so Decode can jump directly to any vector later.
  ARROW_ASSIGN_OR_RAISE(const AlpHeader header, LoadHeader(input));
  if (input.size() > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
    return Status::Invalid("ALP compressed buffer is too large: ", input.size());
  }

  AlpVectorReader<T> reader(pool);
  reader.body_ = input.data() + AlpHeader::kSize;
  reader.body_size_ = static_cast<int64_t>(input.size()) - AlpHeader::kSize;
  reader.num_elements_ = header.num_elements;
  reader.vector_size_ = header.GetVectorSize();

  const int32_t num_vectors = header.GetNumVectors();
  const int64_t offsets_section_size =
      static_cast<int64_t>(num_vectors) * sizeof(AlpFormatConstants::OffsetType);
  if (reader.body_size_ < offsets_section_size) {
    return Status::Invalid("ALP compressed buffer too small for offsets section: ",
                           reader.body_size_, " < ", offsets_section_size);
  }

  // Sanity check: each vector must have at least its metadata. Reject obviously
  // corrupted num_vectors before allocating (avoids OOM on malicious data).
  constexpr int64_t kMinBytesPerVector =
      AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize;
  if (offsets_section_size + static_cast<int64_t>(num_vectors) * kMinBytesPerVector >
      reader.body_size_) {
    return Status::Invalid("ALP num_vectors inconsistent with buffer size: num_vectors=",
                           num_vectors, ", input_size=", reader.body_size_);
  }

  // Read all offsets. The wire format is little-endian, so each offset is
  // converted rather than copied in bulk.
  reader.vector_offsets_.resize(num_vectors);
  for (int32_t i = 0; i < num_vectors; ++i) {
    reader.vector_offsets_[i] =
        bit_util::FromLittleEndian(util::SafeLoadAs<AlpFormatConstants::OffsetType>(
            reader.body_ + i * sizeof(AlpFormatConstants::OffsetType)));
  }

  // Vectors are contiguous: the first starts after the offset array, and each
  // later offset must equal the previous vector's end.
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
  if (expected_offset != reader.body_size_) {
    return Status::Invalid("ALP vector data does not fill the buffer: end=",
                           expected_offset, ", buffer_size=", reader.body_size_);
  }

  return reader;
}

template <AlpFloatingType T>
Result<int32_t> AlpVectorReader<T>::VectorLength(int32_t vector_index) const {
  if (vector_index < 0 || vector_index >= num_vectors()) {
    return Status::Invalid("ALP vector index out of range: ", vector_index, " of ",
                           num_vectors());
  }
  if (vector_index == num_vectors() - 1) {
    const int32_t remainder = num_elements_ % vector_size_;
    return remainder == 0 ? vector_size_ : remainder;
  }
  return vector_size_;
}

template <AlpFloatingType T>
Result<typename AlpVectorReader<T>::VectorLayout> AlpVectorReader<T>::LoadVectorLayout(
    int32_t vector_index) const {
  if (vector_index < 0 || vector_index >= num_vectors()) {
    return Status::Invalid("ALP vector index out of range: ", vector_index, " of ",
                           num_vectors());
  }
  const int64_t vector_offset = vector_offsets_[vector_index];
  if (vector_offset >= body_size_) {
    return Status::Invalid("ALP vector offset out of bounds: offset=", vector_offset,
                           ", buffer_size=", body_size_);
  }
  const uint8_t* vector_start = body_ + vector_offset;
  const size_t remaining_bytes = static_cast<size_t>(body_size_ - vector_offset);

  constexpr size_t kMetadataSize = AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize;
  if (remaining_bytes < kMetadataSize) {
    return Status::Invalid(
        "ALP insufficient buffer for vector metadata: remaining=", remaining_bytes,
        ", metadata_size=", kMetadataSize, ", vector_index=", vector_index);
  }

  VectorLayout layout;
  ARROW_ASSIGN_OR_RAISE(layout.alp_info, AlpInfo::Load({vector_start, remaining_bytes}));
  ARROW_ASSIGN_OR_RAISE(layout.for_info,
                        AlpForInfo<T>::Load({vector_start + AlpInfo::kStoredSize,
                                             remaining_bytes - AlpInfo::kStoredSize}));

  ARROW_ASSIGN_OR_RAISE(layout.num_elements, VectorLength(vector_index));
  layout.data = vector_start + kMetadataSize;
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

template <AlpFloatingType T>
Result<int64_t> AlpVectorReader<T>::VectorSizeInBytes(int32_t vector_index) const {
  ARROW_ASSIGN_OR_RAISE(const VectorLayout layout, LoadVectorLayout(vector_index));
  constexpr int64_t kMetadataSize = AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize;
  return kMetadataSize + layout.data_size;
}

template <AlpFloatingType T>
template <typename TargetType>
  requires AlpDecodeTarget<T, TargetType>
Status AlpVectorReader<T>::Decode(int32_t vector_index, std::span<TargetType> output) {
  ARROW_ASSIGN_OR_RAISE(const VectorLayout layout, LoadVectorLayout(vector_index));
  if (output.size() != static_cast<size_t>(layout.num_elements)) {
    return Status::Invalid("ALP output size does not match vector length: ",
                           output.size(), " != ", layout.num_elements);
  }

  RETURN_NOT_OK(decode_view_.Reset({layout.data, static_cast<size_t>(layout.data_size)},
                                   layout.alp_info, layout.for_info,
                                   layout.num_elements));

  // Reuse the existing capacity; resize() grows it only if this vector is larger.
  unpacked_integers_.resize(layout.num_elements);
  AlpCompression<T>::Decompress(decode_view_, output, unpacked_integers_);
  return Status::OK();
}

template Status AlpVectorReader<float>::Decode(int32_t, std::span<float>);
template Status AlpVectorReader<float>::Decode(int32_t, std::span<double>);
template Status AlpVectorReader<double>::Decode(int32_t, std::span<double>);

template class AlpVectorReader<float>;
template class AlpVectorReader<double>;

template class AlpCodec<float>;
template class AlpCodec<double>;

}  // namespace arrow::util::alp
