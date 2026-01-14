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
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/span.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// AlpEncodedVectorInfo implementation (templated)

template <typename T>
bool AlpEncodedVectorInfo<T>::operator==(const AlpEncodedVectorInfo<T>& other) const {
  return exponent_and_factor == other.exponent_and_factor &&
         frame_of_reference == other.frame_of_reference &&
         bit_width == other.bit_width && num_exceptions == other.num_exceptions;
}

template <typename T>
void AlpEncodedVectorInfo<T>::Store(arrow::util::span<char> output_buffer) const {
  ARROW_CHECK(output_buffer.size() >= GetStoredSize())
      << "alp_vector_info_output_too_small: " << output_buffer.size() << " vs "
      << GetStoredSize();

  // Store field-by-field to handle different frame_of_reference sizes
  char* ptr = output_buffer.data();

  // frame_of_reference: 4 bytes for float, 8 bytes for double
  std::memcpy(ptr, &frame_of_reference, sizeof(frame_of_reference));
  ptr += sizeof(frame_of_reference);

  // exponent_and_factor: 2 bytes
  std::memcpy(ptr, &exponent_and_factor, sizeof(exponent_and_factor));
  ptr += sizeof(exponent_and_factor);

  // bit_width, reserved: 1 byte each
  *ptr++ = static_cast<char>(bit_width);
  *ptr++ = static_cast<char>(reserved);

  // num_exceptions: 2 bytes
  std::memcpy(ptr, &num_exceptions, sizeof(num_exceptions));
}

template <typename T>
AlpEncodedVectorInfo<T> AlpEncodedVectorInfo<T>::Load(
    arrow::util::span<const char> input_buffer) {
  ARROW_CHECK(input_buffer.size() >= GetStoredSize())
      << "alp_vector_info_input_too_small: " << input_buffer.size() << " vs "
      << GetStoredSize();

  AlpEncodedVectorInfo<T> result{};
  const char* ptr = input_buffer.data();

  // frame_of_reference: 4 bytes for float, 8 bytes for double
  std::memcpy(&result.frame_of_reference, ptr, sizeof(result.frame_of_reference));
  ptr += sizeof(result.frame_of_reference);

  // exponent_and_factor: 2 bytes
  std::memcpy(&result.exponent_and_factor, ptr, sizeof(result.exponent_and_factor));
  ptr += sizeof(result.exponent_and_factor);

  // bit_width, reserved: 1 byte each
  result.bit_width = static_cast<uint8_t>(*ptr++);
  result.reserved = static_cast<uint8_t>(*ptr++);

  // num_exceptions: 2 bytes
  std::memcpy(&result.num_exceptions, ptr, sizeof(result.num_exceptions));

  return result;
}

// Explicit template instantiations for AlpEncodedVectorInfo
template struct AlpEncodedVectorInfo<float>;
template struct AlpEncodedVectorInfo<double>;

// ----------------------------------------------------------------------
// AlpEncodedVector implementation

template <typename T>
void AlpEncodedVector<T>::Store(arrow::util::span<char> output_buffer) const {
  const uint64_t overall_size = GetStoredSize();
  ARROW_CHECK(output_buffer.size() >= overall_size)
      << "alp_bit_packed_vector_store_output_too_small: " << output_buffer.size()
      << " vs " << overall_size;
  vector_info.Store(output_buffer);
  uint64_t compression_offset = AlpEncodedVectorInfo<T>::GetStoredSize();

  // Compute bit_packed_size from num_elements and bit_width
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, vector_info.bit_width);

  // Store all successfully compressed values first.
  std::memcpy(output_buffer.data() + compression_offset, packed_values.data(),
              bit_packed_size);
  compression_offset += bit_packed_size;

  ARROW_CHECK(vector_info.num_exceptions == exceptions.size() &&
              vector_info.num_exceptions == exception_positions.size())
      << "alp_bit_packed_vector_store_num_exceptions_mismatch: "
      << vector_info.num_exceptions << " vs " << exceptions.size() << " vs "
      << exception_positions.size();

  // Store exceptions, consisting of their positions and their values.
  const uint64_t exception_position_size =
      vector_info.num_exceptions * sizeof(AlpConstants::PositionType);
  std::memcpy(output_buffer.data() + compression_offset, exception_positions.data(),
              exception_position_size);
  compression_offset += exception_position_size;

  const uint64_t exception_size = vector_info.num_exceptions * sizeof(T);
  std::memcpy(output_buffer.data() + compression_offset, exceptions.data(),
              exception_size);
  compression_offset += exception_size;

  ARROW_CHECK(compression_offset == overall_size)
      << "alp_bit_packed_vector_size_mismatch: " << compression_offset << " vs "
      << overall_size;
}

template <typename T>
AlpEncodedVector<T> AlpEncodedVector<T>::Load(
    arrow::util::span<const char> input_buffer, uint16_t num_elements) {
  ARROW_CHECK(num_elements <= AlpConstants::kAlpVectorSize)
      << "alp_compression_state_element_count_too_large: " << num_elements << " vs "
      << AlpConstants::kAlpVectorSize;

  AlpEncodedVector<T> result;
  result.vector_info = AlpEncodedVectorInfo<T>::Load(input_buffer);
  result.num_elements = num_elements;
  uint64_t input_offset = AlpEncodedVectorInfo<T>::GetStoredSize();

  const uint64_t overall_size = GetStoredSize(result.vector_info, num_elements);

  ARROW_CHECK(input_buffer.size() >= overall_size)
      << "alp_compression_state_input_too_small: " << input_buffer.size() << " vs "
      << overall_size;

  // Compute bit_packed_size from num_elements and bit_width
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, result.vector_info.bit_width);

  // Optimization: Use UnsafeResize to avoid zero-initialization before memcpy.
  // This is safe for POD types since we immediately overwrite with memcpy.
  result.packed_values.UnsafeResize(bit_packed_size);
  std::memcpy(result.packed_values.data(), input_buffer.data() + input_offset,
              bit_packed_size);
  input_offset += bit_packed_size;

  result.exception_positions.UnsafeResize(result.vector_info.num_exceptions);
  const uint64_t exception_position_size =
      result.vector_info.num_exceptions * sizeof(AlpConstants::PositionType);
  std::memcpy(result.exception_positions.data(), input_buffer.data() + input_offset,
              exception_position_size);
  input_offset += exception_position_size;

  result.exceptions.UnsafeResize(result.vector_info.num_exceptions);
  const uint64_t exception_size = result.vector_info.num_exceptions * sizeof(T);
  std::memcpy(result.exceptions.data(), input_buffer.data() + input_offset,
              exception_size);
  return result;
}

template <typename T>
uint64_t AlpEncodedVector<T>::GetStoredSize() const {
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, vector_info.bit_width);
  return AlpEncodedVectorInfo<T>::GetStoredSize() + bit_packed_size +
         vector_info.num_exceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

// ----------------------------------------------------------------------
// AlpEncodedVectorView implementation

template <typename T>
AlpEncodedVectorView<T> AlpEncodedVectorView<T>::LoadView(
    arrow::util::span<const char> input_buffer, uint16_t num_elements) {
  ARROW_CHECK(num_elements <= AlpConstants::kAlpVectorSize)
      << "alp_view_element_count_too_large: " << num_elements << " vs "
      << AlpConstants::kAlpVectorSize;

  AlpEncodedVectorView<T> result;
  result.vector_info = AlpEncodedVectorInfo<T>::Load(input_buffer);
  result.num_elements = num_elements;
  uint64_t input_offset = AlpEncodedVectorInfo<T>::GetStoredSize();

  const uint64_t overall_size =
      AlpEncodedVector<T>::GetStoredSize(result.vector_info, num_elements);

  ARROW_CHECK(input_buffer.size() >= overall_size)
      << "alp_view_input_too_small: " << input_buffer.size() << " vs " << overall_size;

  // Compute bit_packed_size from num_elements and bit_width
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, result.vector_info.bit_width);

  // Zero-copy for packed values (bytes have no alignment requirements)
  result.packed_values = {
      reinterpret_cast<const uint8_t*>(input_buffer.data() + input_offset),
      bit_packed_size};
  input_offset += bit_packed_size;

  // Copy exception positions into aligned storage to avoid UB from misaligned access.
  // Exceptions are rare (typically < 5%), so the copy overhead is negligible.
  const uint64_t exception_position_size =
      result.vector_info.num_exceptions * sizeof(AlpConstants::PositionType);
  result.exception_positions.UnsafeResize(result.vector_info.num_exceptions);
  std::memcpy(result.exception_positions.data(), input_buffer.data() + input_offset,
              exception_position_size);
  input_offset += exception_position_size;

  // Copy exception values into aligned storage to avoid UB from misaligned access.
  const uint64_t exception_size = result.vector_info.num_exceptions * sizeof(T);
  result.exceptions.UnsafeResize(result.vector_info.num_exceptions);
  std::memcpy(result.exceptions.data(), input_buffer.data() + input_offset,
              exception_size);

  return result;
}

template <typename T>
uint64_t AlpEncodedVectorView<T>::GetStoredSize() const {
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, vector_info.bit_width);
  return AlpEncodedVectorInfo<T>::GetStoredSize() + bit_packed_size +
         vector_info.num_exceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

template struct AlpEncodedVectorView<float>;
template struct AlpEncodedVectorView<double>;

template <typename T>
uint64_t AlpEncodedVector<T>::GetStoredSize(const AlpEncodedVectorInfo<T>& info,
                                            uint16_t num_elements) {
  const uint64_t bit_packed_size =
      AlpEncodedVectorInfo<T>::GetBitPackedSize(num_elements, info.bit_width);
  return AlpEncodedVectorInfo<T>::GetStoredSize() + bit_packed_size +
         info.num_exceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

template <typename T>
bool AlpEncodedVector<T>::operator==(const AlpEncodedVector<T>& other) const {
  // Manual comparison since StaticVector doesn't have operator==.
  const bool packed_values_equal =
      (packed_values.size() == other.packed_values.size()) &&
      std::equal(packed_values.begin(), packed_values.end(),
                 other.packed_values.begin());
  const bool exceptions_equal =
      (exceptions.size() == other.exceptions.size()) &&
      std::equal(exceptions.begin(), exceptions.end(), other.exceptions.begin());
  const bool exception_positions_equal =
      (exception_positions.size() == other.exception_positions.size()) &&
      std::equal(exception_positions.begin(), exception_positions.end(),
                 other.exception_positions.begin());
  return vector_info == other.vector_info && packed_values_equal && exceptions_equal &&
         exception_positions_equal;
}

template class AlpEncodedVector<float>;
template class AlpEncodedVector<double>;

// ----------------------------------------------------------------------
// Internal helper classes

namespace {

/// \brief Helper class for encoding/decoding individual values
template <typename T>
class AlpInlines : private AlpConstants {
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

  /// \brief Convert a float to an int without rounding
  static inline auto FastRound(T n) -> SignedExactType {
    n = n + Constants::kMagicNumber - Constants::kMagicNumber;
    return static_cast<SignedExactType>(n);
  }

  /// \brief Fast way to round float to nearest integer
  static inline auto NumberToInt(T n) -> SignedExactType {
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
    return static_cast<T>(encoded_value) * GetFactor(exponent_and_factor.factor) *
           Constants::GetFactor(exponent_and_factor.exponent);
  }
};

/// \brief Helper struct for tracking compression combinations
struct AlpCombination {
  AlpExponentAndFactor exponent_and_factor;
  uint64_t num_appearances{0};
  uint64_t estimated_compression_size{0};
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
std::optional<uint64_t> AlpCompression<T>::EstimateCompressedSize(
    const std::vector<T>& input_vector,
    const AlpExponentAndFactor exponent_and_factor,
    const bool penalize_exceptions) {
  // Dry compress a vector (ideally a sample) to estimate ALP compression size
  // given an exponent and factor.
  SignedExactType max_encoded_value = std::numeric_limits<SignedExactType>::min();
  SignedExactType min_encoded_value = std::numeric_limits<SignedExactType>::max();

  uint64_t num_exceptions = 0;
  uint64_t num_non_exceptions = 0;
  for (const T& value : input_vector) {
    const SignedExactType encoded_value =
        AlpInlines<T>::EncodeValue(value, exponent_and_factor);
    T decoded_value = AlpInlines<T>::DecodeValue(encoded_value, exponent_and_factor);
    if (decoded_value == value) {
      num_non_exceptions++;
      max_encoded_value = std::max(encoded_value, max_encoded_value);
      min_encoded_value = std::min(encoded_value, min_encoded_value);
      continue;
    }
    num_exceptions++;
  }

  // We penalize combinations which yield almost all exceptions.
  if (penalize_exceptions && num_non_exceptions < 2) {
    return std::nullopt;
  }

  // Evaluate factor/exponent compression size (we optimize for FOR).
  const ExactType delta = (static_cast<ExactType>(max_encoded_value) -
                           static_cast<ExactType>(min_encoded_value));

  const uint32_t estimated_bits_per_value =
      static_cast<uint32_t>(std::ceil(std::log2(delta + 1)));
  uint64_t estimated_compression_size = input_vector.size() * estimated_bits_per_value;
  estimated_compression_size +=
      num_exceptions * (kExactTypeBitSize + (sizeof(PositionType) * 8));
  return estimated_compression_size;
}

template <typename T>
AlpEncodingPreset AlpCompression<T>::CreateEncodingPreset(
    const std::vector<std::vector<T>>& vectors_sampled) {
  // Find the best combinations of factor-exponent from each sampled vector.
  // This function is called once per segment.
  // This operates over ALP first level samples.
  static constexpr uint64_t kMaxCombinationCount =
      (Constants::kMaxExponent + 1) * (Constants::kMaxExponent + 2) / 2;

  std::map<AlpExponentAndFactor, uint64_t> best_k_combinations_hash;

  uint64_t best_compressed_size_bits = std::numeric_limits<uint64_t>::max();
  // For each vector sampled.
  for (const std::vector<T>& sampled_vector : vectors_sampled) {
    const uint64_t num_samples = sampled_vector.size();
    const AlpExponentAndFactor best_encoding_options{Constants::kMaxExponent,
                                                     Constants::kMaxExponent};

    // Start optimization with worst possible total bits from compression.
    const uint64_t best_total_bits =
        (num_samples * (kExactTypeBitSize + sizeof(PositionType) * 8)) +
        (num_samples * kExactTypeBitSize);

    // N of appearances is irrelevant at this phase; we search for best compression.
    AlpCombination best_combination{best_encoding_options, 0, best_total_bits};
    // Try all combinations to find the one which minimizes compression size.
    for (uint8_t exp_idx = 0; exp_idx <= Constants::kMaxExponent; exp_idx++) {
      for (uint8_t factor_idx = 0; factor_idx <= exp_idx; factor_idx++) {
        const AlpExponentAndFactor current_exponent_and_factor{exp_idx, factor_idx};
        std::optional<uint64_t> estimated_compression_size = EstimateCompressedSize(
            sampled_vector, current_exponent_and_factor, /*penalize_exceptions=*/true);

        // Skip comparison for values that are not compressible.
        if (!estimated_compression_size.has_value()) {
          continue;
        }

        const AlpCombination current_combination{current_exponent_and_factor, 0,
                                                 *estimated_compression_size};
        if (CompareAlpCombinations(current_combination, best_combination)) {
          best_combination = current_combination;
          best_compressed_size_bits =
              std::min(best_compressed_size_bits, *estimated_compression_size);
        }
      }
    }
    best_k_combinations_hash[best_combination.exponent_and_factor]++;
  }

  // Convert our hash to a Combination vector to be able to sort.
  // Note that this vector should mostly be small (< 10 combinations).
  std::vector<AlpCombination> best_k_combinations;
  best_k_combinations.reserve(
      std::min(best_k_combinations_hash.size(), kMaxCombinationCount));
  for (const auto& combination : best_k_combinations_hash) {
    best_k_combinations.emplace_back(AlpCombination{
        combination.first,   // Encoding Indices
        combination.second,  // N of times it appeared (hash value)
        0  // Compression size is irrelevant since we compare different vectors.
    });
  }
  std::sort(best_k_combinations.begin(), best_k_combinations.end(),
            CompareAlpCombinations);

  std::vector<AlpExponentAndFactor> combinations;
  // Save k' best combinations.
  for (uint64_t i = 0;
       i < std::min(kMaxCombinations, static_cast<uint8_t>(best_k_combinations.size()));
       i++) {
    combinations.push_back(best_k_combinations[i].exponent_and_factor);
  }

  const uint64_t best_compressed_size_bytes =
      std::ceil(best_compressed_size_bits / 8.0);
  return {combinations, best_compressed_size_bytes};
}

template <typename T>
std::vector<T> AlpCompression<T>::CreateSample(arrow::util::span<const T> input) {
  // Sample equidistant values within a vector; skip a fixed number of values.
  const auto idx_increments = std::max<uint32_t>(
      1, static_cast<uint32_t>(std::ceil(static_cast<double>(input.size()) /
                                         AlpConstants::kSamplerSamplesPerVector)));
  std::vector<T> vector_sample;
  vector_sample.reserve(std::ceil(input.size() / static_cast<double>(idx_increments)));
  for (uint64_t i = 0; i < input.size(); i += idx_increments) {
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
  uint64_t best_total_bits = std::numeric_limits<uint64_t>::max();
  uint64_t worse_total_bits_counter = 0;

  // Try each K combination to find the one which minimizes compression size.
  for (const AlpExponentAndFactor& exponent_and_factor : combinations) {
    std::optional<uint64_t> estimated_compression_size = EstimateCompressedSize(
        sample_vector, exponent_and_factor, /*penalize_exceptions=*/false);

    // Skip exponents and factors which result in many exceptions.
    if (!estimated_compression_size.has_value()) {
      continue;
    }

    // If current compression size is worse or equal than current best combination.
    if (estimated_compression_size >= best_total_bits) {
      worse_total_bits_counter += 1;
      // Early exit strategy.
      if (worse_total_bits_counter == kSamplingEarlyExitThreshold) {
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
auto AlpCompression<T>::EncodeVector(arrow::util::span<const T> input_vector,
                                     AlpExponentAndFactor exponent_and_factor)
    -> EncodingResult {
  arrow::internal::StaticVector<SignedExactType, kAlpVectorSize> encoded_integers;
  arrow::internal::StaticVector<T, kAlpVectorSize> exceptions;
  arrow::internal::StaticVector<PositionType, kAlpVectorSize> exception_positions;

  // Encoding Float/Double to SignedExactType(Int32, Int64).
  // Encode all values regardless of correctness to recover original floating-point.
  uint64_t input_offset = 0;
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
  PositionType exception_offset = 0;
  for (const PositionType exception_position : exception_positions) {
    if (exception_offset != exception_position) {
      first_non_exception_value = encoded_integers[exception_offset];
      break;
    }
    exception_offset++;
  }

  // Use first non-exception value as placeholder for all exception values.
  for (const PositionType exception_position : exception_positions) {
    const T actual_value = input_vector[exception_position];
    encoded_integers[exception_position] = first_non_exception_value;
    exceptions.push_back(actual_value);
  }

  // Analyze FOR.
  const auto [min, max] =
      std::minmax_element(encoded_integers.begin(), encoded_integers.end());
  const auto frame_of_reference = static_cast<ExactType>(*min);

  for (SignedExactType& encoded_integer : encoded_integers) {
    // Use SafeCopy to avoid strict aliasing violation when converting between
    // signed and unsigned integer types of the same size.
    ExactType unsigned_value = util::SafeCopy<ExactType>(encoded_integer);
    unsigned_value -= frame_of_reference;
    encoded_integer = util::SafeCopy<SignedExactType>(unsigned_value);
  }

  const ExactType min_max_diff =
      (static_cast<ExactType>(*max) - static_cast<ExactType>(*min));
  return EncodingResult{encoded_integers, exceptions, exception_positions, min_max_diff,
                        frame_of_reference};
}

template <typename T>
auto AlpCompression<T>::BitPackIntegers(
    arrow::util::span<const SignedExactType> integers, const uint64_t min_max_diff)
    -> BitPackingResult {
  uint8_t bit_width = 0;

  if (min_max_diff == 0) {
    bit_width = 0;
  } else if constexpr (std::is_same_v<T, float>) {
    bit_width = sizeof(T) * 8 - __builtin_clz(min_max_diff);
  } else if constexpr (std::is_same_v<T, double>) {
    bit_width = sizeof(T) * 8 - __builtin_clzll(min_max_diff);
  }
  const uint16_t bit_packed_size =
      static_cast<uint16_t>(std::ceil((bit_width * integers.size()) / 8.0));

  arrow::internal::StaticVector<uint8_t, kAlpVectorSize * sizeof(T)> packed_integers;
  // Use unsafe resize to avoid zero-initialization. Zero initialization was
  // resulting in around 2-3% degradation in compression speed.
  packed_integers.UnsafeResize(bit_packed_size);
  if (bit_width > 0) {  // Only execute BP if writing data.
    // Use Arrow's BitWriter for packing (loop-based).
    arrow::bit_util::BitWriter writer(packed_integers.data(),
                                      static_cast<int>(bit_packed_size));
    for (uint64_t i = 0; i < integers.size(); ++i) {
      writer.PutValue(static_cast<uint64_t>(integers[i]), bit_width);
    }
    writer.Flush(false);
  }
  return {packed_integers, bit_width, bit_packed_size};
}

template <typename T>
AlpEncodedVector<T> AlpCompression<T>::CompressVector(const T* input_vector,
                                                      uint16_t num_elements,
                                                      const AlpEncodingPreset& preset) {
  // Compress by finding a fitting exponent/factor, encode input, and bitpack.
  const arrow::util::span<const T> input_span{input_vector, num_elements};
  const AlpExponentAndFactor exponent_and_factor =
      FindBestExponentAndFactor(input_span, preset.combinations);
  const EncodingResult encoding_result = EncodeVector(input_span, exponent_and_factor);
  BitPackingResult bitpacking_result;
  switch (preset.integer_encoding) {
    case AlpIntegerEncoding::kBitPack:
      bitpacking_result =
          BitPackIntegers(encoding_result.encoded_integers, encoding_result.min_max_diff);
      break;
    default:
      ARROW_CHECK(false) << "invalid_integer_encoding: "
                         << static_cast<int>(preset.integer_encoding);
      break;
  }

  // Transfer compressed data into a serializable format.
  // Field order: frame_of_reference, exponent_and_factor, bit_width, reserved, num_exceptions
  // (num_elements and bit_packed_size are NOT stored in vectorInfo)
  const AlpEncodedVectorInfo<T> vector_info{
      encoding_result.frame_of_reference,
      exponent_and_factor,
      bitpacking_result.bit_width,
      0,  // reserved
      static_cast<uint16_t>(encoding_result.exceptions.size())};  // num_exceptions

  AlpEncodedVector<T> result;
  result.vector_info = vector_info;
  result.num_elements = num_elements;
  result.packed_values = bitpacking_result.packed_integers;
  result.exceptions = encoding_result.exceptions;
  result.exception_positions = encoding_result.exception_positions;
  return result;
}

template <typename T>
auto AlpCompression<T>::BitUnpackIntegers(
    arrow::util::span<const uint8_t> packed_integers,
    const AlpEncodedVectorInfo<T> vector_info, uint16_t num_elements)
    -> arrow::internal::StaticVector<ExactType, kAlpVectorSize> {
  arrow::internal::StaticVector<ExactType, kAlpVectorSize> encoded_integers;
  // Optimization: Use UnsafeResize to avoid zero-initialization.
  // Safe because we immediately write to all elements via unpack().
  encoded_integers.UnsafeResize(num_elements);

  if (vector_info.bit_width > 0) {
    // Arrow's SIMD unpack works in fixed batch sizes. All SIMD implementations
    // (SIMD128/NEON, SIMD256/AVX2, SIMD512/AVX512) have identical batch sizes:
    // - uint32_t (float): Simd*UnpackerForWidth::kValuesUnpacked = 32
    // - uint64_t (double): Simd*UnpackerForWidth::kValuesUnpacked = 64
    // These constants are in anonymous namespaces (internal implementation detail),
    // so we hardcode them here.
    constexpr int kMinBatchSize = std::is_same_v<T, float> ? 32 : 64;
    const int num_elems = static_cast<int>(num_elements);
    const int num_complete_batches = num_elems / kMinBatchSize;
    const int num_complete_elements = num_complete_batches * kMinBatchSize;

    // Use Arrow's SIMD-optimized unpack for complete batches.
    if (num_complete_elements > 0) {
      arrow::internal::unpack(packed_integers.data(), encoded_integers.data(),
                              num_complete_elements, vector_info.bit_width);
    }

    // Handle remaining elements (<64) with BitReader to match BitWriter format.
    const int remaining = num_elems - num_complete_elements;
    if (remaining > 0) {
      // Calculate byte offset where SIMD unpack finished
      const uint64_t bits_consumed_by_simd =
          static_cast<uint64_t>(num_complete_elements) * vector_info.bit_width;
      // Round up to next byte
      const uint64_t bytes_consumed_by_simd = (bits_consumed_by_simd + 7) / 8;

      // Use BitReader for remaining elements starting from where SIMD left off
      arrow::bit_util::BitReader reader(
          packed_integers.data() + bytes_consumed_by_simd,
          static_cast<int>(packed_integers.size() - bytes_consumed_by_simd));

      for (int i = 0; i < remaining; ++i) {
        uint64_t value = 0;
        if (reader.GetValue(vector_info.bit_width, &value)) {
          encoded_integers[num_complete_elements + i] = static_cast<ExactType>(value);
        } else {
          encoded_integers[num_complete_elements + i] = 0;
        }
      }
    }
  } else {
    std::memset(encoded_integers.data(), 0, num_elements * sizeof(ExactType));
  }
  return encoded_integers;
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::DecodeVector(TargetType* output_vector,
                                     arrow::util::span<ExactType> input_vector,
                                     const AlpEncodedVectorInfo<T> vector_info,
                                     uint16_t num_elements) {
  // Fused unFOR + decode loop - reduces memory traffic by avoiding
  // intermediate write-then-read of the unFOR'd values.
  const ExactType* data = input_vector.data();
  const ExactType frame_of_ref = vector_info.frame_of_reference;

#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (size_t i = 0; i < num_elements; ++i) {
    // 1. Apply frame of reference (unFOR) - unsigned arithmetic
    const ExactType unfored_value = data[i] + frame_of_ref;
    // 2. Reinterpret as signed integer for decode
    SignedExactType signed_value;
    std::memcpy(&signed_value, &unfored_value, sizeof(SignedExactType));
    // 3. Decode using original function to preserve exact floating-point behavior
    output_vector[i] =
        AlpInlines<T>::DecodeValue(signed_value, vector_info.exponent_and_factor);
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::PatchExceptions(
    TargetType* output, arrow::util::span<const T> exceptions,
    arrow::util::span<const uint16_t> exception_positions) {
  // Exceptions Patching.
  uint64_t exception_idx = 0;
#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (uint16_t const exception_position : exception_positions) {
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
  const AlpEncodedVectorInfo<T>& vector_info = packed_vector.vector_info;
  const uint16_t num_elements = packed_vector.num_elements;

  switch (integer_encoding) {
    case AlpIntegerEncoding::kBitPack: {
      arrow::internal::StaticVector<ExactType, kAlpVectorSize> encoded_integers =
          BitUnpackIntegers(packed_vector.packed_values, vector_info, num_elements);
      DecodeVector<TargetType>(output, {encoded_integers.data(), num_elements},
                               vector_info, num_elements);
      PatchExceptions<TargetType>(output, packed_vector.exceptions,
                                  packed_vector.exception_positions);
    } break;
    default:
      ARROW_CHECK(false) << "invalid_integer_encoding: "
                         << static_cast<int>(integer_encoding);
      break;
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::DecompressVectorView(const AlpEncodedVectorView<T>& encoded_view,
                                             const AlpIntegerEncoding integer_encoding,
                                             TargetType* output) {
  static_assert(sizeof(T) <= sizeof(TargetType));
  const AlpEncodedVectorInfo<T>& vector_info = encoded_view.vector_info;
  const uint16_t num_elements = encoded_view.num_elements;

  switch (integer_encoding) {
    case AlpIntegerEncoding::kBitPack: {
      // Use zero-copy for packed values, aligned copies for exceptions
      arrow::internal::StaticVector<ExactType, kAlpVectorSize> encoded_integers =
          BitUnpackIntegers(encoded_view.packed_values, vector_info, num_elements);
      DecodeVector<TargetType>(output, {encoded_integers.data(), num_elements},
                               vector_info, num_elements);
      // Create spans from the aligned StaticVectors for PatchExceptions
      PatchExceptions<TargetType>(
          output,
          {encoded_view.exceptions.data(), encoded_view.exceptions.size()},
          {encoded_view.exception_positions.data(),
           encoded_view.exception_positions.size()});
    } break;
    default:
      ARROW_CHECK(false) << "invalid_integer_encoding: "
                         << static_cast<int>(integer_encoding);
      break;
  }
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
