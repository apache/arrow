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

#include "arrow/util/alp/alp.h"

#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>

#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/span.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

// ALP serialization uses memcpy for multi-byte integers (frame_of_reference,
// num_exceptions, offsets) and assumes little-endian byte order on disk.
static_assert(ARROW_LITTLE_ENDIAN,
              "ALP serialization assumes little-endian byte order");

// ----------------------------------------------------------------------
// AlpEncodedVectorInfo implementation (non-templated, 4 bytes)

void AlpEncodedVectorInfo::Store(arrow::util::span<uint8_t> output_buffer) const {
  ARROW_CHECK(output_buffer.size() >= static_cast<size_t>(GetStoredSize()))
      << "alp_vector_info_output_too_small: " << output_buffer.size() << " vs "
      << GetStoredSize();

  uint8_t* ptr = output_buffer.data();

  // exponent, factor: 1 byte each
  *ptr++ = exponent_;
  *ptr++ = factor_;

  // num_exceptions: 2 bytes
  util::SafeStore(ptr, num_exceptions_);
}

Result<AlpEncodedVectorInfo> AlpEncodedVectorInfo::Load(
    arrow::util::span<const uint8_t> input_buffer) {
  if (input_buffer.size() < static_cast<size_t>(GetStoredSize())) {
    return Status::Invalid("ALP vector info buffer too small: ", input_buffer.size(),
                           " < ", GetStoredSize());
  }

  AlpEncodedVectorInfo result{};
  const uint8_t* ptr = input_buffer.data();

  // exponent, factor: 1 byte each
  result.exponent_ = *ptr++;
  result.factor_ = *ptr++;

  // num_exceptions: 2 bytes
  result.num_exceptions_ = util::SafeLoadAs<int16_t>(ptr);

  return result;
}

// ----------------------------------------------------------------------
// AlpEncodedForVectorInfo implementation (templated, 5/9 bytes)

template <typename T>
void AlpEncodedForVectorInfo<T>::Store(arrow::util::span<uint8_t> output_buffer) const {
  ARROW_CHECK(output_buffer.size() >= static_cast<size_t>(GetStoredSize()))
      << "alp_for_vector_info_output_too_small: " << output_buffer.size() << " vs "
      << GetStoredSize();

  uint8_t* ptr = output_buffer.data();

  // frame_of_reference: 4 bytes for float, 8 bytes for double
  util::SafeStore(ptr, frame_of_reference_);
  ptr += sizeof(frame_of_reference_);

  // bit_width: 1 byte
  *ptr = bit_width_;
}

template <typename T>
Result<AlpEncodedForVectorInfo<T>> AlpEncodedForVectorInfo<T>::Load(
    arrow::util::span<const uint8_t> input_buffer) {
  if (input_buffer.size() < static_cast<size_t>(GetStoredSize())) {
    return Status::Invalid("ALP FOR vector info buffer too small: ", input_buffer.size(),
                           " < ", GetStoredSize());
  }

  AlpEncodedForVectorInfo<T> result{};
  const uint8_t* ptr = input_buffer.data();

  // frame_of_reference: 4 bytes for float, 8 bytes for double
  result.frame_of_reference_ = util::SafeLoadAs<typename AlpEncodedForVectorInfo<T>::ExactType>(ptr);
  ptr += sizeof(result.frame_of_reference_);

  // bit_width: 1 byte
  result.bit_width_ = *ptr;
  if (result.bit_width_ > sizeof(typename AlpEncodedForVectorInfo<T>::ExactType) * 8) {
    return Status::Invalid("ALP FOR bit_width out of range: ", result.bit_width_);
  }

  return result;
}

// Explicit template instantiations for AlpEncodedForVectorInfo
template class AlpEncodedForVectorInfo<float>;
template class AlpEncodedForVectorInfo<double>;

// ----------------------------------------------------------------------
// AlpEncodedVector implementation

template <typename T>
void AlpEncodedVector<T>::Store(arrow::util::span<uint8_t> output_buffer) const {
  const int64_t overall_size = GetStoredSize();
  ARROW_CHECK(static_cast<int64_t>(output_buffer.size()) >= overall_size)
      << "alp_bit_packed_vector_store_output_too_small: " << output_buffer.size()
      << " vs " << overall_size;

  int64_t offset = 0;

  // Store AlpInfo (4 bytes)
  alp_info_.Store({output_buffer.data() + offset, AlpEncodedVectorInfo::kStoredSize});
  offset += AlpEncodedVectorInfo::kStoredSize;

  // Store ForInfo
  for_info_.Store(
      {output_buffer.data() + offset, AlpEncodedForVectorInfo<T>::kStoredSize});
  offset += AlpEncodedForVectorInfo<T>::kStoredSize;

  // Store data section
  StoreDataOnly({output_buffer.data() + offset, output_buffer.size() - offset});
}

template <typename T>
void AlpEncodedVector<T>::StoreDataOnly(arrow::util::span<uint8_t> output_buffer) const {
  const int64_t data_size = GetDataStoredSize();
  // Internal invariants: caller must provide adequate buffer and consistent metadata.
  // These are programmer errors (not data errors), so CHECK is appropriate.
  ARROW_CHECK(static_cast<int64_t>(output_buffer.size()) >= data_size)
      << "alp_bit_packed_vector_store_data_output_too_small: " << output_buffer.size()
      << " vs " << data_size;

  ARROW_CHECK(static_cast<size_t>(alp_info_.num_exceptions()) == exceptions_.size() &&
              static_cast<size_t>(alp_info_.num_exceptions()) ==
                  exception_positions_.size())
      << "alp_bit_packed_vector_store_num_exceptions_mismatch: "
      << alp_info_.num_exceptions() << " vs " << exceptions_.size() << " vs "
      << exception_positions_.size();

  int64_t offset = 0;

  // Compute bit_packed_size from num_elements and bit_width
  const int64_t bit_packed_size =
      AlpEncodedForVectorInfo<T>::GetBitPackedSize(num_elements_, for_info_.bit_width());

  // Store all successfully compressed values first.
  std::memcpy(output_buffer.data() + offset, packed_values_.data(), bit_packed_size);
  offset += bit_packed_size;

  // Store exception positions.
  const int64_t exception_position_size =
      alp_info_.num_exceptions() * sizeof(AlpConstants::PositionType);
  std::memcpy(output_buffer.data() + offset, exception_positions_.data(),
              exception_position_size);
  offset += exception_position_size;

  // Store exception values.
  const int64_t exception_size = alp_info_.num_exceptions() * sizeof(T);
  std::memcpy(output_buffer.data() + offset, exceptions_.data(), exception_size);
  offset += exception_size;

  // Internal invariant: total bytes written must match precomputed size.
  ARROW_CHECK(offset == data_size)
      << "alp_bit_packed_vector_data_size_mismatch: " << offset << " vs " << data_size;
}

template <typename T>
Result<AlpEncodedVector<T>> AlpEncodedVector<T>::Load(
    arrow::util::span<const uint8_t> input_buffer, int32_t num_elements) {
  if (num_elements > (1 << AlpConstants::kMaxLogVectorSize)) {
    return Status::Invalid("ALP element count too large: ", num_elements,
                           " > ", (1 << AlpConstants::kMaxLogVectorSize));
  }

  AlpEncodedVector<T> result;
  int64_t input_offset = 0;

  // Load AlpInfo (4 bytes)
  ARROW_ASSIGN_OR_RAISE(
      AlpEncodedVectorInfo alp_info,
      AlpEncodedVectorInfo::Load(
          {input_buffer.data() + input_offset, AlpEncodedVectorInfo::kStoredSize}));
  input_offset += AlpEncodedVectorInfo::kStoredSize;
  result.set_alp_info(alp_info);

  // Load ForInfo
  ARROW_ASSIGN_OR_RAISE(
      AlpEncodedForVectorInfo<T> for_info,
      AlpEncodedForVectorInfo<T>::Load(
          {input_buffer.data() + input_offset, AlpEncodedForVectorInfo<T>::kStoredSize}));
  input_offset += AlpEncodedForVectorInfo<T>::kStoredSize;
  result.set_for_info(for_info);

  result.set_num_elements(num_elements);

  const int64_t overall_size = GetStoredSize(alp_info, for_info, num_elements);

  if (static_cast<int64_t>(input_buffer.size()) < overall_size) {
    return Status::Invalid("ALP compressed vector buffer too small: ",
                           input_buffer.size(), " < ", overall_size);
  }

  // Compute bit_packed_size from num_elements and bit_width
  const int64_t bit_packed_size =
      AlpEncodedForVectorInfo<T>::GetBitPackedSize(num_elements, for_info.bit_width());

  // TODO: resize() zero-initializes before memcpy overwrites. Consider
  // using uninitialized storage if this shows up in decode-path profiling.
  result.mutable_packed_values().resize(bit_packed_size);
  std::memcpy(result.mutable_packed_values().data(), input_buffer.data() + input_offset,
              bit_packed_size);
  input_offset += bit_packed_size;

  result.mutable_exception_positions().resize(alp_info.num_exceptions());
  const int64_t exception_position_size =
      alp_info.num_exceptions() * sizeof(AlpConstants::PositionType);
  std::memcpy(result.mutable_exception_positions().data(),
              input_buffer.data() + input_offset, exception_position_size);
  input_offset += exception_position_size;

  result.mutable_exceptions().resize(alp_info.num_exceptions());
  const int64_t exception_size = alp_info.num_exceptions() * sizeof(T);
  std::memcpy(result.mutable_exceptions().data(), input_buffer.data() + input_offset,
              exception_size);
  return result;
}

template <typename T>
int64_t AlpEncodedVector<T>::GetStoredSize() const {
  return GetStoredSize(alp_info_, for_info_, num_elements_);
}

template <typename T>
int64_t AlpEncodedVector<T>::GetStoredSize(const AlpEncodedVectorInfo& alp_info,
                                           const AlpEncodedForVectorInfo<T>& for_info,
                                           int32_t num_elements) {
  const int64_t bit_packed_size =
      AlpEncodedForVectorInfo<T>::GetBitPackedSize(num_elements, for_info.bit_width());
  return AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize +
         bit_packed_size +
         alp_info.num_exceptions() * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

template <typename T>
bool AlpEncodedVector<T>::operator==(const AlpEncodedVector<T>& other) const {
  if (alp_info_ != other.alp_info_ || for_info_ != other.for_info_ ||
      num_elements_ != other.num_elements_) {
    return false;
  }
  if (packed_values_.size() != other.packed_values_.size() ||
      !std::equal(packed_values_.begin(), packed_values_.end(),
                  other.packed_values_.begin())) {
    return false;
  }
  if (exceptions_.size() != other.exceptions_.size() ||
      !std::equal(exceptions_.begin(), exceptions_.end(), other.exceptions_.begin())) {
    return false;
  }
  if (exception_positions_.size() != other.exception_positions_.size() ||
      !std::equal(exception_positions_.begin(), exception_positions_.end(),
                  other.exception_positions_.begin())) {
    return false;
  }
  return true;
}

// ----------------------------------------------------------------------
// AlpEncodedVectorView implementation

template <typename T>
Result<AlpEncodedVectorView<T>> AlpEncodedVectorView<T>::LoadView(
    arrow::util::span<const uint8_t> input_buffer, int32_t num_elements) {
  if (num_elements > (1 << AlpConstants::kMaxLogVectorSize)) {
    return Status::Invalid("ALP view element count too large: ", num_elements,
                           " > ", (1 << AlpConstants::kMaxLogVectorSize));
  }

  AlpEncodedVectorView<T> result;
  int64_t input_offset = 0;

  // Load AlpInfo (4 bytes)
  ARROW_ASSIGN_OR_RAISE(
      AlpEncodedVectorInfo alp_info,
      AlpEncodedVectorInfo::Load(
          {input_buffer.data() + input_offset, AlpEncodedVectorInfo::kStoredSize}));
  input_offset += AlpEncodedVectorInfo::kStoredSize;
  result.set_alp_info(alp_info);

  // Load ForInfo
  ARROW_ASSIGN_OR_RAISE(
      AlpEncodedForVectorInfo<T> for_info,
      AlpEncodedForVectorInfo<T>::Load(
          {input_buffer.data() + input_offset, AlpEncodedForVectorInfo<T>::kStoredSize}));
  input_offset += AlpEncodedForVectorInfo<T>::kStoredSize;
  result.set_for_info(for_info);

  result.set_num_elements(num_elements);

  const int64_t overall_size =
      AlpEncodedVector<T>::GetStoredSize(alp_info, for_info, num_elements);

  if (static_cast<int64_t>(input_buffer.size()) < overall_size) {
    return Status::Invalid("ALP view buffer too small: ", input_buffer.size(),
                           " < ", overall_size);
  }

  // Load data section (after metadata)
  ARROW_ASSIGN_OR_RAISE(
      AlpEncodedVectorView<T> data_view,
      LoadViewDataOnly(
          {input_buffer.data() + input_offset, input_buffer.size() - input_offset},
          alp_info, for_info, num_elements));

  // Copy the loaded data into result
  result.set_packed_values(data_view.packed_values());
  result.set_exception_positions(std::move(data_view.mutable_exception_positions()));
  result.set_exceptions(std::move(data_view.mutable_exceptions()));

  return result;
}

template <typename T>
Result<AlpEncodedVectorView<T>> AlpEncodedVectorView<T>::LoadViewDataOnly(
    arrow::util::span<const uint8_t> input_buffer, const AlpEncodedVectorInfo& alp_info,
    const AlpEncodedForVectorInfo<T>& for_info, int32_t num_elements) {
  if (num_elements > (1 << AlpConstants::kMaxLogVectorSize)) {
    return Status::Invalid("ALP view data element count too large: ", num_elements,
                           " > ", (1 << AlpConstants::kMaxLogVectorSize));
  }

  AlpEncodedVectorView<T> result;
  result.set_alp_info(alp_info);
  result.set_for_info(for_info);
  result.set_num_elements(num_elements);

  const int64_t data_size = for_info.GetDataStoredSize(num_elements, alp_info.num_exceptions());
  if (static_cast<int64_t>(input_buffer.size()) < data_size) {
    return Status::Invalid("ALP view data buffer too small: ", input_buffer.size(),
                           " < ", data_size);
  }

  int64_t input_offset = 0;

  // Compute bit_packed_size from num_elements and bit_width
  const int64_t bit_packed_size =
      AlpEncodedForVectorInfo<T>::GetBitPackedSize(num_elements, for_info.bit_width());

  // Zero-copy for packed values (bytes have no alignment requirements)
  result.set_packed_values(
      {input_buffer.data() + input_offset, static_cast<size_t>(bit_packed_size)});
  input_offset += bit_packed_size;

  // Copy exception positions into aligned storage to avoid UB from misaligned access.
  // Exceptions are rare (typically < 5%), so the copy overhead is negligible.
  const int64_t exception_position_size =
      alp_info.num_exceptions() * sizeof(AlpConstants::PositionType);
  result.mutable_exception_positions().resize(alp_info.num_exceptions());
  std::memcpy(result.mutable_exception_positions().data(),
              input_buffer.data() + input_offset, exception_position_size);
  input_offset += exception_position_size;

  // Copy exception values into aligned storage to avoid UB from misaligned access.
  const int64_t exception_size = alp_info.num_exceptions() * sizeof(T);
  result.mutable_exceptions().resize(alp_info.num_exceptions());
  std::memcpy(result.mutable_exceptions().data(), input_buffer.data() + input_offset,
              exception_size);

  return result;
}

template <typename T>
int64_t AlpEncodedVectorView<T>::GetStoredSize() const {
  return AlpEncodedVector<T>::GetStoredSize(alp_info_, for_info_, num_elements_);
}

template class AlpEncodedVectorView<float>;
template class AlpEncodedVectorView<double>;

template class AlpEncodedVector<float>;
template class AlpEncodedVector<double>;

// ----------------------------------------------------------------------
// Internal helper classes

namespace {

/// \brief Helper class for encoding/decoding individual values
template <typename T>
class AlpInlines {
 public:
  using Constants = AlpTypedConstants<T>;
  using ExactType = typename Constants::FloatingToExact;
  using SignedExactType = typename Constants::FloatingToSignedExact;

  /// \brief Check if float is a special value that cannot be converted
  static inline bool IsImpossibleToEncode(const T n) {
    // We do not have to check for positive or negative infinity, since
    // std::numeric_limits<T>::infinity() > std::numeric_limits<T>::max()
    // and vice versa for negative infinity.
    return std::isnan(n) || n > Constants::kEncodingUpperLimit ||
           n < Constants::kEncodingLowerLimit ||
           (n == 0.0 && std::signbit(n));  // Verification for -0.0
  }

  /// \brief Round a float to the nearest integer using the magic-number technique
  static inline SignedExactType FastRound(T n) {
    if (n >= 0) {
      n = n + Constants::kMagicNumber - Constants::kMagicNumber;
    } else {
      n = n - Constants::kMagicNumber + Constants::kMagicNumber;
    }
    return static_cast<SignedExactType>(n);
  }

  /// \brief Fast way to round float to nearest integer
  static inline SignedExactType NumberToInt(T n) {
    if (IsImpossibleToEncode(n)) {
      return static_cast<SignedExactType>(Constants::kEncodingUpperLimit);
    }
    return FastRound(n);
  }

  /// \brief Convert a float into an int using encoding options
  static inline SignedExactType EncodeValue(
      const T value, const AlpExponentAndFactor exponent_and_factor) {
    const T tmp_encoded_value = value *
                                Constants::GetExponent(exponent_and_factor.exponent) *
                                Constants::GetFactor(exponent_and_factor.factor);
    return NumberToInt(tmp_encoded_value);
  }

  /// \brief Reconvert an int to a float using encoding options
  static inline T DecodeValue(const SignedExactType encoded_value,
                              const AlpExponentAndFactor exponent_and_factor) {
    // The cast to T is needed to prevent a signed integer overflow.
    return static_cast<T>(encoded_value) * AlpConstants::GetFactor(exponent_and_factor.factor) *
           Constants::GetFactor(exponent_and_factor.exponent);
  }
};

/// \brief Helper struct for tracking compression combinations
struct AlpCombination {
  AlpExponentAndFactor exponent_and_factor;
  int64_t num_appearances{0};
  int64_t estimated_compression_size{0};
};

/// \brief Compare two ALP combinations to determine which is better
///
/// Return true if c1 is a better combination than c2.
/// First criteria is number of times it appears as best combination.
/// Second criteria is the estimated compression size.
/// Third criteria is bigger exponent.
/// Fourth criteria is bigger factor.
bool CompareAlpCombinations(const AlpCombination& c1, const AlpCombination& c2) {
  return (c1.num_appearances > c2.num_appearances) ||
         (c1.num_appearances == c2.num_appearances &&
          (c1.estimated_compression_size < c2.estimated_compression_size)) ||
         ((c1.num_appearances == c2.num_appearances &&
           c1.estimated_compression_size == c2.estimated_compression_size) &&
          (c2.exponent_and_factor.exponent < c1.exponent_and_factor.exponent)) ||
         ((c1.num_appearances == c2.num_appearances &&
           c1.estimated_compression_size == c2.estimated_compression_size &&
           c2.exponent_and_factor.exponent == c1.exponent_and_factor.exponent) &&
          (c2.exponent_and_factor.factor < c1.exponent_and_factor.factor));
}

}  // namespace

// ----------------------------------------------------------------------
// AlpCompression implementation

template <typename T>
std::optional<int64_t> AlpCompression<T>::EstimateCompressedSize(
    const std::vector<T>& input_vector,
    const AlpExponentAndFactor exponent_and_factor,
    const bool penalize_exceptions) {
  // Dry compress a vector (ideally a sample) to estimate ALP compression size
  // given an exponent and factor.
  SignedExactType max_encoded_value = std::numeric_limits<SignedExactType>::min();
  SignedExactType min_encoded_value = std::numeric_limits<SignedExactType>::max();

  // Split encode/decode/compare into separate passes over small batches so the
  // compiler can vectorize the encode and decode passes independently.
  static constexpr int kBatchSize = 8;
  const size_t n = input_vector.size();
  int64_t num_exceptions = 0;

  size_t i = 0;
  for (; i + kBatchSize <= n; i += kBatchSize) {
    SignedExactType encoded_values[kBatchSize];
    T decoded_values[kBatchSize];

    // Pass 1: Encode (vectorizable: multiply + magic-number round)
    for (int j = 0; j < kBatchSize; j++) {
      encoded_values[j] =
          AlpInlines<T>::EncodeValue(input_vector[i + j], exponent_and_factor);
    }
    // Pass 2: Decode (vectorizable: int->float cast + multiply)
    for (int j = 0; j < kBatchSize; j++) {
      decoded_values[j] =
          AlpInlines<T>::DecodeValue(encoded_values[j], exponent_and_factor);
    }
    // Pass 3: Compare and accumulate min/max/exceptions
    for (int j = 0; j < kBatchSize; j++) {
      if (decoded_values[j] == input_vector[i + j]) {
        max_encoded_value = std::max(encoded_values[j], max_encoded_value);
        min_encoded_value = std::min(encoded_values[j], min_encoded_value);
      } else {
        num_exceptions++;
      }
    }
  }
  // Scalar tail for remaining elements
  for (; i < n; i++) {
    const SignedExactType encoded_value =
        AlpInlines<T>::EncodeValue(input_vector[i], exponent_and_factor);
    const T decoded_value =
        AlpInlines<T>::DecodeValue(encoded_value, exponent_and_factor);
    if (decoded_value == input_vector[i]) {
      max_encoded_value = std::max(encoded_value, max_encoded_value);
      min_encoded_value = std::min(encoded_value, min_encoded_value);
    } else {
      num_exceptions++;
    }
  }

  const int64_t num_non_exceptions = static_cast<int64_t>(input_vector.size()) - num_exceptions;

  // We penalize combinations which yield almost all exceptions.
  if (penalize_exceptions && num_non_exceptions < 2) {
    return std::nullopt;
  }

  // Evaluate factor/exponent compression size (we optimize for FOR).
  const ExactType delta = (static_cast<ExactType>(max_encoded_value) -
                           static_cast<ExactType>(min_encoded_value));

  const int32_t estimated_bits_per_value =
      static_cast<int32_t>(bit_util::NumRequiredBits(delta));
  int64_t estimated_compression_size =
      static_cast<int64_t>(input_vector.size()) * estimated_bits_per_value;
  estimated_compression_size +=
      num_exceptions * (kExactTypeBitSize + (sizeof(AlpConstants::PositionType) * 8));
  return estimated_compression_size;
}

template <typename T>
AlpEncodingParameters AlpCompression<T>::CreateEncodingParameters(
    const std::vector<std::vector<T>>& vectors_sampled) {
  // Find the best combinations of factor-exponent from each sampled vector.
  // This function is called once per segment.
  // This operates over ALP first level samples.
  static constexpr size_t kMaxCombinationCount =
      (Constants::kMaxExponent + 1) * (Constants::kMaxExponent + 2) / 2;

  std::map<AlpExponentAndFactor, int64_t> best_k_combinations_hash;

  // Find the best exponent/factor combination for a single sampled vector by
  // trying all (exponent, factor) pairs and picking the one that minimizes
  // estimated compression size. Returns {best_combination, best_size_bits}.
  auto find_best_for_vector = [](const std::vector<T>& sampled_vector)
      -> std::pair<AlpCombination, int64_t> {
    const int64_t num_samples = sampled_vector.size();
    const AlpExponentAndFactor worst_case{Constants::kMaxExponent,
                                          Constants::kMaxExponent};
    // Start with worst possible total bits.
    const int64_t worst_total_bits =
        (num_samples * (kExactTypeBitSize + sizeof(AlpConstants::PositionType) * 8)) +
        (num_samples * kExactTypeBitSize);

    AlpCombination best{worst_case, 0, worst_total_bits};
    int64_t best_size_bits = std::numeric_limits<int64_t>::max();

    for (uint8_t exp_idx = 0; exp_idx <= Constants::kMaxExponent; exp_idx++) {
      for (uint8_t factor_idx = 0; factor_idx <= exp_idx; factor_idx++) {
        const AlpExponentAndFactor current{exp_idx, factor_idx};
        std::optional<int64_t> size = EstimateCompressedSize(
            sampled_vector, current, /*penalize_exceptions=*/true);
        if (!size.has_value()) {
          continue;
        }
        const AlpCombination candidate{current, 0, *size};
        if (CompareAlpCombinations(candidate, best)) {
          best = candidate;
          best_size_bits = std::min(best_size_bits, *size);
        }
      }
    }
    return {best, best_size_bits};
  };

  int64_t best_compressed_size_bits = std::numeric_limits<int64_t>::max();
  for (const std::vector<T>& sampled_vector : vectors_sampled) {
    auto [best, size_bits] = find_best_for_vector(sampled_vector);
    best_k_combinations_hash[best.exponent_and_factor]++;
    best_compressed_size_bits = std::min(best_compressed_size_bits, size_bits);
  }

  // Convert our hash to a Combination vector to be able to sort.
  // Note that this vector should mostly be small (< 10 combinations).
  std::vector<AlpCombination> best_k_combinations;
  best_k_combinations.reserve(
      std::min(best_k_combinations_hash.size(), kMaxCombinationCount));
  for (const auto& [exponent_and_factor, num_appearances] : best_k_combinations_hash) {
    best_k_combinations.emplace_back(AlpCombination{
        exponent_and_factor, num_appearances,
        0  // Compression size is irrelevant since we compare different vectors.
    });
  }
  std::sort(best_k_combinations.begin(), best_k_combinations.end(),
            CompareAlpCombinations);

  // Save k' best combinations.
  const uint8_t num_combinations_to_keep =
      std::min(AlpConstants::kMaxCombinations, static_cast<uint8_t>(best_k_combinations.size()));
  std::vector<AlpExponentAndFactor> combinations;
  combinations.reserve(num_combinations_to_keep);
  for (uint8_t i = 0; i < num_combinations_to_keep; i++) {
    combinations.push_back(best_k_combinations[i].exponent_and_factor);
  }

  const int64_t best_compressed_size_bytes =
      bit_util::BytesForBits(best_compressed_size_bits);
  return {combinations, best_compressed_size_bytes};
}

template <typename T>
std::vector<T> AlpCompression<T>::CreateSample(arrow::util::span<const T> input) {
  // Sample equidistant values within a vector; skip a fixed number of values.
  const int32_t idx_increments = std::max<int32_t>(
      1, static_cast<int32_t>(std::ceil(static_cast<double>(input.size()) /
                                        AlpConstants::kSamplerSamplesPerVector)));
  std::vector<T> vector_sample;
  vector_sample.reserve(std::ceil(input.size() / static_cast<double>(idx_increments)));
  for (size_t i = 0; i < input.size(); i += idx_increments) {
    vector_sample.push_back(input[i]);
  }
  return vector_sample;
}

template <typename T>
AlpExponentAndFactor AlpCompression<T>::FindBestExponentAndFactor(
    arrow::util::span<const T> input,
    const std::vector<AlpExponentAndFactor>& combinations) {
  // Find the best factor-exponent combination from within the best k combinations.
  // This is ALP second level sampling.
  if (combinations.size() == 1) {
    return combinations.front();
  }

  const std::vector<T> sample_vector = CreateSample(input);

  AlpExponentAndFactor best_exponent_and_factor;
  int64_t best_total_bits = std::numeric_limits<int64_t>::max();
  int64_t worse_total_bits_counter = 0;

  // Try each K combination to find the one which minimizes compression size.
  // penalize_exceptions=false because this is second-level sampling: the preset
  // already filtered out bad combinations during first-level sampling, so we
  // just pick whichever pre-vetted combination compresses this vector best.
  for (const AlpExponentAndFactor& exponent_and_factor : combinations) {
    std::optional<int64_t> estimated_compression_size = EstimateCompressedSize(
        sample_vector, exponent_and_factor, /*penalize_exceptions=*/false);

    // Skip exponents and factors which result in many exceptions.
    if (!estimated_compression_size.has_value()) {
      continue;
    }

    // If current compression size is worse or equal than current best combination.
    if (estimated_compression_size >= best_total_bits) {
      worse_total_bits_counter += 1;
      // Early exit strategy.
      if (worse_total_bits_counter == AlpConstants::kSamplingEarlyExitThreshold) {
        break;
      }
      continue;
    }
    // Otherwise replace the best and continue trying with next combination.
    best_total_bits = estimated_compression_size.value();
    best_exponent_and_factor = exponent_and_factor;
    worse_total_bits_counter = 0;
  }
  return best_exponent_and_factor;
}

template <typename T>
typename AlpCompression<T>::EncodingResult AlpCompression<T>::EncodeVector(
    arrow::util::span<const T> input_vector,
    AlpExponentAndFactor exponent_and_factor) {
  if (input_vector.empty()) {
    return EncodingResult{{}, {}, {}, 0, 0};
  }

  std::vector<SignedExactType> encoded_integers;
  encoded_integers.reserve(input_vector.size());
  std::vector<T> exceptions;
  std::vector<AlpConstants::PositionType> exception_positions;

  // Encoding Float/Double to SignedExactType(Int32, Int64).
  // Encode all values regardless of correctness to recover original floating-point.
  int64_t input_offset = 0;
  for (const T input : input_vector) {
    const SignedExactType encoded_value =
        AlpInlines<T>::EncodeValue(input, exponent_and_factor);
    const T decoded_value = AlpInlines<T>::DecodeValue(encoded_value, exponent_and_factor);
    encoded_integers.push_back(encoded_value);
    // Detect exceptions using a predicated comparison.
    if (decoded_value != input) {
      exception_positions.push_back(input_offset);
    }
    input_offset++;
  }

  // Finding first non-exception value.
  SignedExactType first_non_exception_value = 0;
  AlpConstants::PositionType exception_offset = 0;
  for (const AlpConstants::PositionType exception_position : exception_positions) {
    if (exception_offset != exception_position) {
      first_non_exception_value = encoded_integers[exception_offset];
      break;
    }
    exception_offset++;
  }

  // Use first non-exception value as placeholder for all exception values.
  for (const AlpConstants::PositionType exception_position : exception_positions) {
    const T actual_value = input_vector[exception_position];
    encoded_integers[exception_position] = first_non_exception_value;
    exceptions.push_back(actual_value);
  }

  // Analyze FOR.
  const auto [min_it, max_it] =
      std::minmax_element(encoded_integers.begin(), encoded_integers.end());
  const auto frame_of_reference = static_cast<ExactType>(*min_it);

  for (SignedExactType& encoded_integer : encoded_integers) {
    // Use SafeCopy to avoid strict aliasing violation when converting between
    // signed and unsigned integer types of the same size.
    ExactType unsigned_value = util::SafeCopy<ExactType>(encoded_integer);
    unsigned_value -= frame_of_reference;
    encoded_integer = util::SafeCopy<SignedExactType>(unsigned_value);
  }

  const ExactType min_max_diff =
      (static_cast<ExactType>(*max_it) - static_cast<ExactType>(*min_it));
  return EncodingResult{encoded_integers, exceptions, exception_positions, min_max_diff,
                        frame_of_reference};
}

template <typename T>
typename AlpCompression<T>::BitPackingResult AlpCompression<T>::BitPackIntegers(
    arrow::util::span<const SignedExactType> integers, const uint64_t min_max_diff) {
  uint8_t bit_width = 0;

  if (min_max_diff > 0) {
    bit_width = static_cast<uint8_t>(bit_util::NumRequiredBits(min_max_diff));
  }
  const int32_t bit_packed_size = static_cast<int32_t>(
      bit_util::BytesForBits(bit_width * static_cast<int64_t>(integers.size())));

  std::vector<uint8_t> packed_integers(bit_packed_size);
  if (bit_width > 0) {  // Only execute BP if writing data.
    // Use Arrow's BitWriter for packing (loop-based).
    arrow::bit_util::BitWriter writer(packed_integers.data(), bit_packed_size);
    for (size_t i = 0; i < integers.size(); ++i) {
      writer.PutValue(static_cast<uint64_t>(integers[i]), bit_width);
    }
    writer.Flush(false);
  }
  return {packed_integers, bit_width, bit_packed_size};
}

template <typename T>
AlpEncodedVector<T> AlpCompression<T>::CompressVector(const T* input_vector,
                                                      int32_t num_elements,
                                                      const AlpEncodingParameters& preset) {
  // Compress by finding a fitting exponent/factor, encode input, and bitpack.
  const arrow::util::span<const T> input_span{input_vector, static_cast<size_t>(num_elements)};
  const AlpExponentAndFactor exponent_and_factor =
      FindBestExponentAndFactor(input_span, preset.combinations);
  const EncodingResult encoding_result = EncodeVector(input_span, exponent_and_factor);
  const BitPackingResult bitpacking_result =
      BitPackIntegers(encoding_result.encoded_integers, encoding_result.min_max_diff);

  // Build the result with split metadata
  AlpEncodedVector<T> result;

  // ALP metadata (4 bytes)
  result.mutable_alp_info().set_exponent(exponent_and_factor.exponent);
  result.mutable_alp_info().set_factor(exponent_and_factor.factor);
  result.mutable_alp_info().set_num_exceptions(
      static_cast<int16_t>(encoding_result.exceptions.size()));

  // FOR metadata
  result.mutable_for_info().set_frame_of_reference(encoding_result.frame_of_reference);
  result.mutable_for_info().set_bit_width(bitpacking_result.bit_width);

  result.set_num_elements(num_elements);
  result.set_packed_values(bitpacking_result.packed_integers);
  result.set_exceptions(encoding_result.exceptions);
  result.set_exception_positions(encoding_result.exception_positions);
  return result;
}

template <typename T>
std::vector<typename AlpCompression<T>::ExactType> AlpCompression<T>::BitUnpackIntegers(
    arrow::util::span<const uint8_t> packed_integers,
    const AlpEncodedForVectorInfo<T>& for_info, int32_t num_elements) {
  std::vector<ExactType> encoded_integers(num_elements);

  if (for_info.bit_width() > 0) {
    // Arrow's unpack handles arbitrary sizes: SIMD for complete batches,
    // then unpack_exact for the remainder. No need to manually split.
    arrow::internal::unpack(packed_integers.data(), encoded_integers.data(),
                            static_cast<int>(num_elements), for_info.bit_width());
  } else {
    std::memset(encoded_integers.data(), 0, num_elements * sizeof(ExactType));
  }
  return encoded_integers;
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::DecodeVector(arrow::util::span<ExactType> input_vector,
                                     const AlpEncodedVectorInfo& alp_info,
                                     const AlpEncodedForVectorInfo<T>& for_info,
                                     int32_t num_elements,
                                     TargetType* output_vector) {
  // Fused unFOR + decode loop - reduces memory traffic by avoiding
  // intermediate write-then-read of the unFOR'd values.
  const ExactType* data = input_vector.data();
  const ExactType frame_of_ref = for_info.frame_of_reference();

#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (int32_t i = 0; i < num_elements; ++i) {
    // 1. Apply frame of reference (unFOR) - unsigned arithmetic
    const ExactType unfored_value = data[i] + frame_of_ref;
    // 2. Reinterpret as signed integer for decode
    const SignedExactType signed_value = util::SafeCopy<SignedExactType>(unfored_value);
    // 3. Decode using original function to preserve exact floating-point behavior
    output_vector[i] =
        AlpInlines<T>::DecodeValue(signed_value, alp_info.GetExponentAndFactor());
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::PatchExceptions(
    arrow::util::span<const T> exceptions,
    arrow::util::span<const int16_t> exception_positions,
    TargetType* output) {
  // Exceptions Patching.
  int64_t exception_idx = 0;
#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (int16_t const exception_position : exception_positions) {
    output[exception_position] = static_cast<T>(exceptions[exception_idx]);
    exception_idx++;
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::DecompressVector(const AlpEncodedVector<T>& packed_vector,
                                         const AlpIntegerEncoding integer_encoding,
                                         TargetType* output) {
  static_assert(sizeof(T) <= sizeof(TargetType));
  const AlpEncodedVectorInfo& alp_info = packed_vector.alp_info();
  const AlpEncodedForVectorInfo<T>& for_info = packed_vector.for_info();
  const int32_t num_elements = packed_vector.num_elements();

  std::vector<ExactType> encoded_integers =
      BitUnpackIntegers(packed_vector.packed_values(), for_info, num_elements);
  DecodeVector<TargetType>({encoded_integers.data(), static_cast<size_t>(num_elements)},
                           alp_info, for_info, num_elements, output);
  PatchExceptions<TargetType>(packed_vector.exceptions(),
                              packed_vector.exception_positions(), output);
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::DecompressVectorView(const AlpEncodedVectorView<T>& encoded_view,
                                             const AlpIntegerEncoding integer_encoding,
                                             TargetType* output) {
  static_assert(sizeof(T) <= sizeof(TargetType));
  const AlpEncodedVectorInfo& alp_info = encoded_view.alp_info();
  const AlpEncodedForVectorInfo<T>& for_info = encoded_view.for_info();
  const int32_t num_elements = encoded_view.num_elements();

  std::vector<ExactType> encoded_integers =
      BitUnpackIntegers(encoded_view.packed_values(), for_info, num_elements);
  DecodeVector<TargetType>({encoded_integers.data(), static_cast<size_t>(num_elements)},
                           alp_info, for_info, num_elements, output);
  PatchExceptions<TargetType>(
      {encoded_view.exceptions().data(), encoded_view.exceptions().size()},
      {encoded_view.exception_positions().data(),
       encoded_view.exception_positions().size()},
      output);
}

// ----------------------------------------------------------------------
// Template instantiations

template void AlpCompression<float>::DecompressVector<double>(
    const AlpEncodedVector<float>& packed_vector, AlpIntegerEncoding integer_encoding,
    double* output);
template void AlpCompression<float>::DecompressVector<float>(
    const AlpEncodedVector<float>& packed_vector, AlpIntegerEncoding integer_encoding,
    float* output);
template void AlpCompression<double>::DecompressVector<double>(
    const AlpEncodedVector<double>& packed_vector, AlpIntegerEncoding integer_encoding,
    double* output);

template void AlpCompression<float>::DecompressVectorView<double>(
    const AlpEncodedVectorView<float>& encoded_view, AlpIntegerEncoding integer_encoding,
    double* output);
template void AlpCompression<float>::DecompressVectorView<float>(
    const AlpEncodedVectorView<float>& encoded_view, AlpIntegerEncoding integer_encoding,
    float* output);
template void AlpCompression<double>::DecompressVectorView<double>(
    const AlpEncodedVectorView<double>& encoded_view, AlpIntegerEncoding integer_encoding,
    double* output);

template class AlpCompression<float>;
template class AlpCompression<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
