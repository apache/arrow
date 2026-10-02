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

#include "arrow/util/alp/alp_metadata_internal.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <span>
#include <utility>

#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

namespace arrow::util::alp {

namespace {

// Wire integers are little-endian. Packed values are byte arrays and need no conversion.
template <typename T>
void StoreLittleEndianArray(std::span<const T> values, std::span<uint8_t> output) {
  if (values.empty()) {
    return;
  }
  ARROW_CHECK_GE(output.size(), values.size_bytes())
      << "ALP output buffer is too small for little-endian array: " << output.size()
      << " < " << values.size_bytes();
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    std::memcpy(output.data(), values.data(), values.size_bytes());
  } else {
    for (size_t i = 0; i < values.size(); ++i) {
      util::SafeStore(output.data() + i * sizeof(T), bit_util::ToLittleEndian(values[i]));
    }
  }
}

template <typename T>
void LoadLittleEndianArray(std::span<const uint8_t> input, std::span<T> values) {
  if (values.empty()) {
    return;
  }
  ARROW_CHECK_GE(input.size(), values.size_bytes())
      << "ALP input buffer is too small for little-endian array: " << input.size()
      << " < " << values.size_bytes();
  if constexpr (ARROW_LITTLE_ENDIAN == 1) {
    std::memcpy(values.data(), input.data(), values.size_bytes());
  } else {
    for (size_t i = 0; i < values.size(); ++i) {
      values[i] =
          bit_util::FromLittleEndian(util::SafeLoadAs<T>(input.data() + i * sizeof(T)));
    }
  }
}

// Validate fields that index tables or size output writes.
template <typename T>
Status ValidateVectorInfo(const AlpInfo& alp_info, const AlpForInfo<T>& for_info,
                          int32_t num_elements) {
  using Constants = AlpTypedConstants<T>;
  if (num_elements < 0) {
    return Status::Invalid("ALP element count must be non-negative: ", num_elements);
  }
  constexpr int32_t kMaxVectorSize = 1 << AlpFormatConstants::kMaxLogVectorSize;
  if (num_elements > kMaxVectorSize) {
    return Status::Invalid("ALP element count too large: ", num_elements, " > ",
                           kMaxVectorSize);
  }
  if (alp_info.exponent() > Constants::kMaxExponent) {
    return Status::Invalid("ALP exponent ", static_cast<int>(alp_info.exponent()),
                           " exceeds ", static_cast<int>(Constants::kMaxExponent));
  }
  // factor <= exponent keeps the decode power non-negative.
  if (alp_info.factor() > alp_info.exponent()) {
    return Status::Invalid("ALP factor ", static_cast<int>(alp_info.factor()),
                           " exceeds exponent ", static_cast<int>(alp_info.exponent()));
  }
  if (alp_info.num_exceptions() > num_elements) {
    return Status::Invalid("ALP vector has ", alp_info.num_exceptions(),
                           " exceptions but only ", num_elements, " elements");
  }
  if (for_info.bit_width() > sizeof(typename AlpForInfo<T>::FrameType) * 8) {
    return Status::Invalid("ALP FOR bit_width out of range: ",
                           static_cast<int>(for_info.bit_width()));
  }
  return Status::OK();
}

// Exception positions index the output and the encoder writes them in order, so
// they must be increasing and inside the vector. A repeated position would make
// the patch step write the same slot twice and leave another slot untouched.
Status ValidateExceptionPositions(
    std::span<const AlpFormatConstants::PositionType> exception_positions,
    int32_t num_elements) {
  for (size_t i = 0; i < exception_positions.size(); ++i) {
    const AlpFormatConstants::PositionType position = exception_positions[i];
    if (position >= num_elements) {
      return Status::Invalid("ALP exception position ", position,
                             " is outside a vector of ", num_elements, " elements");
    }
    if (i > 0 && position <= exception_positions[i - 1]) {
      return Status::Invalid("ALP exception positions must increase, but got ",
                             exception_positions[i - 1], " then ", position);
    }
  }
  return Status::OK();
}

template <typename T>
int64_t ComputeStoredSize(const AlpInfo& alp_info, const AlpForInfo<T>& for_info,
                          int32_t num_elements) {
  const int64_t bit_packed_size =
      bit_util::BytesForBits(int64_t{num_elements} * for_info.bit_width());
  return AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize + bit_packed_size +
         alp_info.num_exceptions() *
             (sizeof(AlpFormatConstants::PositionType) + sizeof(T));
}

}  // namespace

void AlpInfo::Store(std::span<uint8_t> output_buffer) const {
  ARROW_CHECK(output_buffer.size() >= static_cast<size_t>(kStoredSize))
      << "ALP vector metadata output buffer is too small: " << output_buffer.size()
      << " < " << kStoredSize;

  uint8_t* ptr = output_buffer.data();

  *ptr++ = exponent_;
  *ptr++ = factor_;

  util::SafeStore(ptr, bit_util::ToLittleEndian(num_exceptions_));
}

Result<AlpInfo> AlpInfo::Load(std::span<const uint8_t> input_buffer) {
  if (input_buffer.size() < static_cast<size_t>(kStoredSize)) {
    return Status::Invalid("ALP vector info buffer too small: ", input_buffer.size(),
                           " < ", kStoredSize);
  }

  AlpInfo result{};
  const uint8_t* ptr = input_buffer.data();

  result.exponent_ = *ptr++;
  result.factor_ = *ptr++;

  result.num_exceptions_ = bit_util::FromLittleEndian(util::SafeLoadAs<uint16_t>(ptr));

  return result;
}

template <typename T>
void AlpForInfo<T>::Store(std::span<uint8_t> output_buffer) const {
  ARROW_CHECK(output_buffer.size() >= static_cast<size_t>(kStoredSize))
      << "ALP FOR metadata output buffer is too small: " << output_buffer.size() << " < "
      << kStoredSize;

  uint8_t* ptr = output_buffer.data();

  util::SafeStore(ptr, bit_util::ToLittleEndian(frame_of_reference_));
  ptr += sizeof(frame_of_reference_);
  *ptr = bit_width_;
}

template <typename T>
Result<AlpForInfo<T>> AlpForInfo<T>::Load(std::span<const uint8_t> input_buffer) {
  if (input_buffer.size() < static_cast<size_t>(kStoredSize)) {
    return Status::Invalid("ALP FOR vector info buffer too small: ", input_buffer.size(),
                           " < ", kStoredSize);
  }

  AlpForInfo<T> result{};
  const uint8_t* ptr = input_buffer.data();

  result.frame_of_reference_ = bit_util::FromLittleEndian(
      util::SafeLoadAs<typename AlpForInfo<T>::FrameType>(ptr));
  ptr += sizeof(result.frame_of_reference_);
  result.bit_width_ = *ptr;
  if (result.bit_width_ > sizeof(typename AlpForInfo<T>::FrameType) * 8) {
    return Status::Invalid("ALP FOR bit_width out of range: ", result.bit_width_);
  }

  return result;
}

// Explicit template instantiations for AlpForInfo
template class AlpForInfo<float>;
template class AlpForInfo<double>;

template <typename T>
void AlpEncodedVector<T>::Store(std::span<uint8_t> output_buffer) const {
  const int64_t overall_size = GetStoredSize();
  ARROW_CHECK(static_cast<int64_t>(output_buffer.size()) >= overall_size)
      << "ALP encoded vector output buffer is too small: " << output_buffer.size()
      << " < " << overall_size;

  ARROW_CHECK(static_cast<size_t>(alp_info_.num_exceptions()) == exceptions_.size() &&
              static_cast<size_t>(alp_info_.num_exceptions()) ==
                  exception_positions_.size())
      << "ALP exception metadata is inconsistent: metadata=" << alp_info_.num_exceptions()
      << ", exceptions=" << exceptions_.size()
      << ", positions=" << exception_positions_.size();

  const int64_t bit_packed_size =
      bit_util::BytesForBits(int64_t{num_elements_} * for_info_.bit_width());
  ARROW_CHECK(packed_values_.size() == static_cast<size_t>(bit_packed_size))
      << "ALP packed values size does not match bit width and element count: "
      << packed_values_.size() << " != " << bit_packed_size;

  int64_t offset = 0;

  alp_info_.Store({output_buffer.data() + offset, AlpInfo::kStoredSize});
  offset += AlpInfo::kStoredSize;

  for_info_.Store({output_buffer.data() + offset, AlpForInfo<T>::kStoredSize});
  offset += AlpForInfo<T>::kStoredSize;

  // A bit width of zero packs to no bytes, and packed_values_.data() is then null,
  // which memcpy may not be handed.
  if (bit_packed_size > 0) {
    std::memcpy(output_buffer.data() + offset, packed_values_.data(), bit_packed_size);
  }
  offset += bit_packed_size;

  const int64_t exception_position_size =
      alp_info_.num_exceptions() * sizeof(AlpFormatConstants::PositionType);
  StoreLittleEndianArray(
      std::span<const AlpFormatConstants::PositionType>(exception_positions_),
      output_buffer.subspan(static_cast<size_t>(offset), exception_position_size));
  offset += exception_position_size;

  const int64_t exception_size = alp_info_.num_exceptions() * sizeof(T);
  StoreLittleEndianArray(
      std::span<const T>(exceptions_),
      output_buffer.subspan(static_cast<size_t>(offset), exception_size));
  offset += exception_size;

  ARROW_CHECK(offset == overall_size)
      << "ALP encoded vector serialized size mismatch: " << offset
      << " != " << overall_size;
}

template <typename T>
AlpEncodedVector<T>::AlpEncodedVector(
    AlpInfo alp_info, AlpForInfo<T> for_info, int32_t num_elements,
    std::vector<uint8_t> packed_values,
    std::vector<AlpFormatConstants::PositionType> exception_positions,
    std::vector<T> exceptions)
    : alp_info_(std::move(alp_info)),
      for_info_(std::move(for_info)),
      num_elements_(num_elements),
      packed_values_(std::move(packed_values)),
      exception_positions_(std::move(exception_positions)),
      exceptions_(std::move(exceptions)) {}

template <typename T>
AlpEncodedVector<T> AlpEncodedVector<T>::Make(
    AlpInfo alp_info, AlpForInfo<T> for_info, int32_t num_elements,
    std::vector<uint8_t> packed_values,
    std::vector<AlpFormatConstants::PositionType> exception_positions,
    std::vector<T> exceptions) {
  ARROW_CHECK_OK(ValidateVectorInfo<T>(alp_info, for_info, num_elements));
  ARROW_CHECK_EQ(alp_info.num_exceptions(), exceptions.size())
      << "ALP exception count does not match metadata";
  ARROW_CHECK_EQ(exceptions.size(), exception_positions.size())
      << "ALP exception positions and values have different counts";
  ARROW_CHECK_OK(ValidateExceptionPositions(exception_positions, num_elements));

  const int64_t bit_packed_size =
      bit_util::BytesForBits(int64_t{num_elements} * for_info.bit_width());
  ARROW_CHECK_EQ(packed_values.size(), static_cast<size_t>(bit_packed_size))
      << "ALP packed values size does not match bit width and element count";

  return AlpEncodedVector(std::move(alp_info), std::move(for_info), num_elements,
                          std::move(packed_values), std::move(exception_positions),
                          std::move(exceptions));
}

template <typename T>
int64_t AlpEncodedVector<T>::GetStoredSize() const {
  return ComputeStoredSize(alp_info_, for_info_, num_elements_);
}

template <typename T>
Result<AlpEncodedVectorView<T>> AlpEncodedVectorView<T>::Load(
    std::span<const uint8_t> input_buffer, int32_t num_elements, MemoryPool* pool) {
  if (num_elements < 0) {
    return Status::Invalid("ALP element count must be non-negative: ", num_elements);
  }
  if (num_elements > (1 << AlpFormatConstants::kMaxLogVectorSize)) {
    return Status::Invalid("ALP element count too large: ", num_elements, " > ",
                           (1 << AlpFormatConstants::kMaxLogVectorSize));
  }

  constexpr size_t kMetadataSize = AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize;
  if (input_buffer.size() < kMetadataSize) {
    return Status::Invalid("ALP vector buffer is too small for metadata: ",
                           input_buffer.size(), " < ", kMetadataSize);
  }

  ARROW_ASSIGN_OR_RAISE(AlpInfo alp_info,
                        AlpInfo::Load(input_buffer.first(AlpInfo::kStoredSize)));
  ARROW_ASSIGN_OR_RAISE(AlpForInfo<T> for_info,
                        AlpForInfo<T>::Load(input_buffer.subspan(
                            AlpInfo::kStoredSize, AlpForInfo<T>::kStoredSize)));
  RETURN_NOT_OK(ValidateVectorInfo<T>(alp_info, for_info, num_elements));

  const int64_t stored_size = ComputeStoredSize(alp_info, for_info, num_elements);
  if (static_cast<int64_t>(input_buffer.size()) < stored_size) {
    return Status::Invalid("ALP vector buffer is too small: ", input_buffer.size(), " < ",
                           stored_size);
  }

  AlpEncodedVectorView<T> result(pool);
  RETURN_NOT_OK(result.Reset(input_buffer.subspan(kMetadataSize), alp_info, for_info,
                             num_elements));
  return result;
}

template <typename T>
Status AlpEncodedVectorView<T>::Reset(std::span<const uint8_t> input_buffer,
                                      const AlpInfo& alp_info,
                                      const AlpForInfo<T>& for_info,
                                      int32_t num_elements) {
  Clear();
  if (num_elements < 0) {
    return Status::Invalid("ALP view data element count must be non-negative: ",
                           num_elements);
  }
  if (num_elements > (1 << AlpFormatConstants::kMaxLogVectorSize)) {
    return Status::Invalid("ALP view data element count too large: ", num_elements, " > ",
                           (1 << AlpFormatConstants::kMaxLogVectorSize));
  }

  RETURN_NOT_OK(ValidateVectorInfo<T>(alp_info, for_info, num_elements));

  const int64_t data_size =
      for_info.GetDataStoredSize(num_elements, alp_info.num_exceptions());
  if (static_cast<int64_t>(input_buffer.size()) < data_size) {
    return Status::Invalid("ALP view data buffer too small: ", input_buffer.size(), " < ",
                           data_size);
  }

  alp_info_ = alp_info;
  for_info_ = for_info;
  num_elements_ = num_elements;

  int64_t input_offset = 0;

  const int64_t bit_packed_size =
      bit_util::BytesForBits(int64_t{num_elements} * for_info.bit_width());

  // Packed values need no alignment, so keep them zero-copy.
  packed_values_ = std::span<const uint8_t>(input_buffer.data() + input_offset,
                                            static_cast<size_t>(bit_packed_size));
  input_offset += bit_packed_size;

  // Copy to aligned storage; resize() preserves capacity across vectors.
  const int64_t exception_position_size =
      alp_info.num_exceptions() * sizeof(AlpFormatConstants::PositionType);
  exception_positions_.resize(alp_info.num_exceptions());
  LoadLittleEndianArray(
      input_buffer.subspan(static_cast<size_t>(input_offset), exception_position_size),
      std::span<AlpFormatConstants::PositionType>(exception_positions_));
  input_offset += exception_position_size;
  RETURN_NOT_OK(ValidateExceptionPositions(exception_positions_, num_elements));

  exceptions_.resize(alp_info.num_exceptions());
  LoadLittleEndianArray(
      input_buffer.subspan(static_cast<size_t>(input_offset),
                           static_cast<size_t>(alp_info.num_exceptions()) * sizeof(T)),
      std::span<T>(exceptions_));

  return Status::OK();
}

template <typename T>
void AlpEncodedVectorView<T>::Clear() {
  alp_info_ = AlpInfo{};
  for_info_ = AlpForInfo<T>{};
  num_elements_ = 0;
  packed_values_ = {};
  exception_positions_.clear();
  exceptions_.clear();
}

template class AlpEncodedVectorView<float>;
template class AlpEncodedVectorView<double>;

template class AlpEncodedVector<float>;
template class AlpEncodedVector<double>;

}  // namespace arrow::util::alp
