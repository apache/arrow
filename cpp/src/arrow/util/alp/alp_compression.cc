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

#include "arrow/util/alp/alp_compression_internal.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <span>
#include <utility>

#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#if defined(__GNUC__) && !defined(__clang__)
#  define ARROW_ALP_UNROLL_AND_ASSUME_INDEPENDENT \
    _Pragma("GCC unroll kLoopUnrollCount") _Pragma("GCC ivdep")
#else
#  define ARROW_ALP_UNROLL_AND_ASSUME_INDEPENDENT
#endif

namespace arrow::util::alp {

// Internal helper classes

namespace {

#if defined(__GNUC__) && !defined(__clang__)
constexpr int64_t kLoopUnrollCount = 4;
#endif

// Helper for encoding and decoding individual values.
template <AlpFloatingType T>
class AlpInlines {
 public:
  using Constants = AlpTypedConstants<T>;
  using EncodedUnsigned = typename Constants::EncodedUnsigned;
  using EncodedSigned = typename Constants::EncodedSigned;

  // Returns true when the scaled value cannot be encoded.
  static bool IsImpossibleToEncode(const T n) {
    // The two limit comparisons already cover both infinities: the encoding
    // limits are finite, so +infinity compares above kEncodingUpperLimit and
    // -infinity below kEncodingLowerLimit.
    return std::isnan(n) || n > Constants::kEncodingUpperLimit ||
           n < Constants::kEncodingLowerLimit ||
           (n == 0.0 && std::signbit(n));  // -0.0 would decode as +0.0.
  }

  // Rounds to the nearest integer using the magic-number technique. The caller must
  // screen n against the encoding bounds; those bounds keep one ulp in reserve so
  // rounding cannot carry n past the largest value the integer type holds.
  static EncodedSigned FastRound(T n) {
    if (n >= 0) {
      n = n + Constants::kMagicNumber - Constants::kMagicNumber;
    } else {
      n = n - Constants::kMagicNumber + Constants::kMagicNumber;
    }
    return static_cast<EncodedSigned>(n);
  }

  // Converts a scaled value to an encoded integer. Values that cannot be encoded
  // use the upper encoding bound as a sentinel.
  static EncodedSigned NumberToInt(T n) {
    if (IsImpossibleToEncode(n)) {
      return static_cast<EncodedSigned>(Constants::kEncodingUpperLimit);
    }
    return FastRound(n);
  }

  // Encodes one value using the exponent/factor pair.
  static EncodedSigned EncodeValue(const T value,
                                   const AlpExponentAndFactor exponent_and_factor) {
    const T tmp_encoded_value = value *
                                Constants::GetExponent(exponent_and_factor.exponent) *
                                Constants::GetFactor(exponent_and_factor.factor);
    return NumberToInt(tmp_encoded_value);
  }

  // Decodes one encoded integer using the exponent/factor pair.
  static T DecodeValue(const EncodedSigned encoded_value,
                       const AlpExponentAndFactor exponent_and_factor) {
    // The cast to T is needed to prevent a signed integer overflow.
    return static_cast<T>(encoded_value) *
           Constants::GetExponent(exponent_and_factor.factor) *
           Constants::GetFactor(exponent_and_factor.exponent);
  }
};

// Tracks how often a combination wins for a sampled vector.
struct AlpCombination {
  AlpExponentAndFactor exponent_and_factor;
  int64_t num_appearances = 0;
  int64_t estimated_compression_size_bits = 0;
};

// Returns true when c1 is a better preset candidate than c2. Ties follow the ALP
// paper: more wins, smaller estimated size, then larger exponent/factor.
bool CompareAlpCombinations(const AlpCombination& c1, const AlpCombination& c2) {
  return (c1.num_appearances > c2.num_appearances) ||
         (c1.num_appearances == c2.num_appearances &&
          (c1.estimated_compression_size_bits < c2.estimated_compression_size_bits)) ||
         ((c1.num_appearances == c2.num_appearances &&
           c1.estimated_compression_size_bits == c2.estimated_compression_size_bits) &&
          (c2.exponent_and_factor.exponent < c1.exponent_and_factor.exponent)) ||
         ((c1.num_appearances == c2.num_appearances &&
           c1.estimated_compression_size_bits == c2.estimated_compression_size_bits &&
           c2.exponent_and_factor.exponent == c1.exponent_and_factor.exponent) &&
          (c2.exponent_and_factor.factor < c1.exponent_and_factor.factor));
}

template <AlpFloatingType T>
std::optional<int64_t> EstimateCompressedSizeBits(
    std::span<const T> inputs, const AlpExponentAndFactor exponent_and_factor,
    bool penalize_exceptions) {
  using Constants = AlpTypedConstants<T>;
  using EncodedSigned = typename Constants::EncodedSigned;
  using EncodedUnsigned = typename Constants::EncodedUnsigned;
  constexpr uint8_t kEncodedBitSize = sizeof(EncodedUnsigned) * 8;

  // Dry compress a vector (ideally a sample) to estimate ALP compression size
  // given an exponent and factor.
  EncodedSigned max_encoded_value = std::numeric_limits<EncodedSigned>::min();
  EncodedSigned min_encoded_value = std::numeric_limits<EncodedSigned>::max();

  // Split encode/decode/compare into separate passes over small batches so the
  // compiler can vectorize the encode and decode passes independently.
  static constexpr int kBatchSize = 8;
  const size_t n = inputs.size();
  int64_t num_exceptions = 0;

  size_t i = 0;
  for (; i + kBatchSize <= n; i += kBatchSize) {
    EncodedSigned encoded_values[kBatchSize];
    T decoded_values[kBatchSize];

    // Pass 1: Encode (vectorizable: multiply + magic-number round)
    for (int j = 0; j < kBatchSize; j++) {
      encoded_values[j] = AlpInlines<T>::EncodeValue(inputs[i + j], exponent_and_factor);
    }
    // Pass 2: Decode (vectorizable: int->float cast + multiply)
    for (int j = 0; j < kBatchSize; j++) {
      decoded_values[j] =
          AlpInlines<T>::DecodeValue(encoded_values[j], exponent_and_factor);
    }
    // Pass 3: Compare and accumulate min/max/exceptions
    for (int j = 0; j < kBatchSize; j++) {
      if (decoded_values[j] == inputs[i + j]) {
        max_encoded_value = std::max(encoded_values[j], max_encoded_value);
        min_encoded_value = std::min(encoded_values[j], min_encoded_value);
      } else {
        num_exceptions++;
      }
    }
  }
  // Scalar tail for remaining elements
  for (; i < n; i++) {
    const EncodedSigned encoded_value =
        AlpInlines<T>::EncodeValue(inputs[i], exponent_and_factor);
    const T decoded_value =
        AlpInlines<T>::DecodeValue(encoded_value, exponent_and_factor);
    if (decoded_value == inputs[i]) {
      max_encoded_value = std::max(encoded_value, max_encoded_value);
      min_encoded_value = std::min(encoded_value, min_encoded_value);
    } else {
      num_exceptions++;
    }
  }

  const int64_t num_non_exceptions = static_cast<int64_t>(inputs.size()) - num_exceptions;

  // We penalize combinations which yield almost all exceptions.
  if (penalize_exceptions && num_non_exceptions < 2) {
    return std::nullopt;
  }

  // Evaluate factor/exponent compression size (we optimize for FOR).
  const EncodedUnsigned delta = (static_cast<EncodedUnsigned>(max_encoded_value) -
                                 static_cast<EncodedUnsigned>(min_encoded_value));

  const int32_t estimated_bits_per_value = static_cast<int32_t>(std::bit_width(delta));
  int64_t estimated_compression_size =
      static_cast<int64_t>(inputs.size()) * estimated_bits_per_value;
  estimated_compression_size +=
      num_exceptions * (kEncodedBitSize + (sizeof(AlpFormatConstants::PositionType) * 8));
  return estimated_compression_size;
}

}  // namespace

// AlpCompression implementation

template <AlpFloatingType T>
AlpEncodingPreset AlpCompression<T>::MakePreset(
    const std::vector<std::vector<T>>& samples) {
  if (samples.empty()) {
    return AlpEncodingPreset::MakeDefault();
  }

  using Constants = AlpTypedConstants<T>;
  constexpr uint8_t kEncodedBitSize = AlpCompression<T>::kEncodedBitSize;
  constexpr size_t kMaxCombinationCount =
      (Constants::kMaxExponent + 1) * (Constants::kMaxExponent + 2) / 2;

  std::map<AlpExponentAndFactor, int64_t> combination_counts;

  // Returns the best candidate and its estimated size in bits for one sample, or
  // nullopt when the sample cannot produce a usable candidate.
  auto find_best_for_sample = [](const std::vector<T>& sample)
      -> std::optional<std::pair<AlpCombination, int64_t>> {
    if (sample.empty()) {
      return std::nullopt;
    }

    const int64_t num_samples = sample.size();
    const AlpExponentAndFactor worst_case{Constants::kMaxExponent,
                                          Constants::kMaxExponent};
    const int64_t worst_total_bits =
        num_samples * (kEncodedBitSize + sizeof(AlpFormatConstants::PositionType) * 8) +
        num_samples * kEncodedBitSize;

    // Seed with the worst candidate so the first valid result replaces it.
    AlpCombination best{worst_case, 0, worst_total_bits};
    int64_t best_size_bits = std::numeric_limits<int64_t>::max();

    for (uint8_t exp_idx = 0; exp_idx <= Constants::kMaxExponent; exp_idx++) {
      for (uint8_t factor_idx = 0; factor_idx <= exp_idx; factor_idx++) {
        const AlpExponentAndFactor current{exp_idx, factor_idx};
        const std::optional<int64_t> size =
            EstimateCompressedSizeBits<T>(sample, current, /*penalize_exceptions=*/true);
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
    if (best_size_bits == std::numeric_limits<int64_t>::max()) {
      return std::nullopt;
    }
    return std::make_pair(best, best_size_bits);
  };

  int64_t estimated_compressed_size_bits = std::numeric_limits<int64_t>::max();
  bool found_usable_sample = false;
  for (const std::vector<T>& sample : samples) {
    const auto best = find_best_for_sample(sample);
    if (!best.has_value()) {
      continue;
    }
    found_usable_sample = true;
    combination_counts[best->first.exponent_and_factor]++;
    estimated_compressed_size_bits =
        std::min(estimated_compressed_size_bits, best->second);
  }
  if (!found_usable_sample) {
    return AlpEncodingPreset::MakeDefault();
  }

  std::vector<AlpCombination> best_k_combinations;
  best_k_combinations.reserve(std::min(combination_counts.size(), kMaxCombinationCount));
  for (const auto& [exponent_and_factor, num_appearances] : combination_counts) {
    best_k_combinations.emplace_back(
        AlpCombination{exponent_and_factor, num_appearances, 0});
  }
  std::sort(best_k_combinations.begin(), best_k_combinations.end(),
            CompareAlpCombinations);

  const uint8_t num_combinations_to_keep =
      std::min(AlpSamplingOptions::kMaxCombinations,
               static_cast<uint8_t>(best_k_combinations.size()));
  std::vector<AlpExponentAndFactor> combinations;
  combinations.reserve(num_combinations_to_keep);
  for (uint8_t i = 0; i < num_combinations_to_keep; i++) {
    combinations.push_back(best_k_combinations[i].exponent_and_factor);
  }

  const int64_t estimated_compressed_size_bytes =
      bit_util::BytesForBits(estimated_compressed_size_bits);
  return AlpEncodingPreset{std::move(combinations), estimated_compressed_size_bytes};
}

template <AlpFloatingType T>
std::vector<T> AlpCompression<T>::CreateSample(std::span<const T> inputs) {
  // Pick an equidistant stride so the sample count is capped at kSamplesPerChunk.
  const int32_t idx_increments = std::max<int32_t>(
      1, static_cast<int32_t>(std::ceil(static_cast<double>(inputs.size()) /
                                        AlpSamplingOptions::kSamplesPerChunk)));
  const size_t increment = static_cast<size_t>(idx_increments);
  std::vector<T> sample;
  sample.reserve((inputs.size() + increment - 1) / increment);
  for (size_t i = 0; i < inputs.size(); i += idx_increments) {
    sample.push_back(inputs[i]);
  }
  return sample;
}

template <AlpFloatingType T>
AlpExponentAndFactor AlpCompression<T>::FindBestExponentAndFactor(
    std::span<const T> inputs, const std::vector<AlpExponentAndFactor>& combinations) {
  ARROW_CHECK(!combinations.empty())
      << "ALP encoding preset must contain at least one exponent/factor pair";

  // Find the best factor-exponent combination from within the best k combinations.
  // This search is ALP's second-level sampling.
  if (combinations.size() == 1) {
    return combinations.front();
  }

  const std::vector<T> sample = CreateSample(inputs);

  AlpExponentAndFactor best_exponent_and_factor;
  int64_t best_total_bits = std::numeric_limits<int64_t>::max();
  int64_t worse_total_bits_counter = 0;

  // Try each candidate combination to find the one which minimizes compression size.
  // penalize_exceptions=false because this is second-level sampling: the preset
  // already filtered out bad combinations during first-level sampling, so we
  // just pick whichever pre-vetted combination compresses this vector best.
  for (const AlpExponentAndFactor& exponent_and_factor : combinations) {
    const int64_t estimated_compression_size =
        EstimateCompressedSizeBits<T>(sample, exponent_and_factor,
                                      /*penalize_exceptions=*/false)
            .value_or(std::numeric_limits<int64_t>::max());

    // If current compression size is worse or equal than current best combination.
    if (estimated_compression_size >= best_total_bits) {
      worse_total_bits_counter += 1;
      // Early exit strategy.
      if (worse_total_bits_counter == AlpSamplingOptions::kEarlyExitThreshold) {
        break;
      }
      continue;
    }
    // Otherwise replace the best and continue trying with next combination.
    best_total_bits = estimated_compression_size;
    best_exponent_and_factor = exponent_and_factor;
    worse_total_bits_counter = 0;
  }
  return best_exponent_and_factor;
}

template <AlpFloatingType T>
typename AlpCompression<T>::EncodingResult AlpCompression<T>::EncodeVector(
    std::span<const T> inputs, AlpExponentAndFactor exponent_and_factor) {
  if (inputs.empty()) {
    return EncodingResult{};
  }

  std::vector<EncodedUnsigned> for_deltas;
  for_deltas.reserve(inputs.size());
  std::vector<T> exceptions;
  std::vector<AlpFormatConstants::PositionType> exception_positions;

  EncodedSigned min_encoded_value = std::numeric_limits<EncodedSigned>::max();
  EncodedSigned max_encoded_value = std::numeric_limits<EncodedSigned>::min();

  // Encode all values first. Values that do not round-trip are treated as exceptions,
  // while the remaining values define the FOR range.
  int64_t input_offset = 0;
  for (const T input : inputs) {
    const EncodedSigned encoded_value =
        AlpInlines<T>::EncodeValue(input, exponent_and_factor);
    const T decoded_value =
        AlpInlines<T>::DecodeValue(encoded_value, exponent_and_factor);
    for_deltas.push_back(static_cast<EncodedUnsigned>(encoded_value));

    if (decoded_value != input) {
      exception_positions.push_back(
          static_cast<AlpFormatConstants::PositionType>(input_offset));
    } else {
      min_encoded_value = std::min(min_encoded_value, encoded_value);
      max_encoded_value = std::max(max_encoded_value, encoded_value);
    }
    input_offset++;
  }

  // Find the first non-exception value and use it as the placeholder for exceptions.
  // If every value is an exception, use 0 and keep the FOR range at zero.
  EncodedSigned first_non_exception_value = 0;
  if (exception_positions.size() == inputs.size()) {
    min_encoded_value = 0;
    max_encoded_value = 0;
  } else {
    AlpFormatConstants::PositionType exception_offset = 0;
    for (const AlpFormatConstants::PositionType exception_position :
         exception_positions) {
      if (exception_offset != exception_position) {
        break;
      }
      exception_offset++;
    }
    ARROW_DCHECK_LT(exception_offset, inputs.size());
    first_non_exception_value =
        util::SafeCopy<EncodedSigned>(for_deltas[exception_offset]);
  }

  for (const AlpFormatConstants::PositionType exception_position : exception_positions) {
    for_deltas[exception_position] =
        static_cast<EncodedUnsigned>(first_non_exception_value);
    exceptions.push_back(inputs[exception_position]);
  }

  // Apply FOR in the unsigned domain, as required by the wire format.
  const EncodedUnsigned frame_of_reference =
      static_cast<EncodedUnsigned>(min_encoded_value);
  for (EncodedUnsigned& for_delta : for_deltas) {
    for_delta -= frame_of_reference;
  }
  const EncodedUnsigned min_max_diff =
      static_cast<EncodedUnsigned>(max_encoded_value) - frame_of_reference;

  return EncodingResult{std::move(for_deltas), std::move(exception_positions),
                        std::move(exceptions), min_max_diff, frame_of_reference};
}

template <AlpFloatingType T>
typename AlpCompression<T>::BitPackingResult AlpCompression<T>::BitPackIntegers(
    std::span<const EncodedUnsigned> for_deltas, const EncodedUnsigned min_max_diff) {
  uint8_t bit_width = 0;

  if (min_max_diff > 0) {
    bit_width = static_cast<uint8_t>(std::bit_width(min_max_diff));
  }
  const int32_t bit_packed_size = static_cast<int32_t>(
      bit_util::BytesForBits(bit_width * static_cast<int64_t>(for_deltas.size())));

  std::vector<uint8_t> packed_integers(bit_packed_size);
  if (bit_width > 0) {
    // Use Arrow's BitWriter for packing (loop-based).
    arrow::bit_util::BitWriter writer(packed_integers.data(), bit_packed_size);
    // The output buffer is sized exactly for for_deltas.size() values, so
    // PutValue cannot exhaust it.
    for (size_t i = 0; i < for_deltas.size(); ++i) {
      writer.PutValue(static_cast<uint64_t>(for_deltas[i]), bit_width);
    }
    writer.Flush(false);
  }
  return {std::move(packed_integers), bit_width};
}

template <AlpFloatingType T>
AlpEncodedVector<T> AlpCompression<T>::Compress(std::span<const T> inputs,
                                                const AlpEncodingPreset& preset) {
  using Constants = AlpTypedConstants<T>;
  ARROW_CHECK_LE(inputs.size(),
                 static_cast<size_t>(1 << AlpFormatConstants::kMaxLogVectorSize));
  ARROW_CHECK(!preset.combinations.empty())
      << "ALP encoding preset must contain at least one exponent/factor pair";
  ARROW_CHECK_EQ(preset.integer_encoding, AlpIntegerEncoding::kForBitPack);
  for (const AlpExponentAndFactor& combination : preset.combinations) {
    ARROW_CHECK_LE(combination.exponent, Constants::kMaxExponent);
    ARROW_CHECK_LE(combination.factor, combination.exponent);
  }

  const int32_t num_elements = static_cast<int32_t>(inputs.size());
  // Compress by finding a fitting exponent/factor, encode input, and bitpack.
  const AlpExponentAndFactor exponent_and_factor =
      FindBestExponentAndFactor(inputs, preset.combinations);
  const EncodingResult encoding_result = EncodeVector(inputs, exponent_and_factor);
  const BitPackingResult bitpacking_result =
      BitPackIntegers(encoding_result.for_deltas, encoding_result.min_max_diff);

  AlpInfo alp_info;
  alp_info.SetExponent(exponent_and_factor.exponent);
  alp_info.SetFactor(exponent_and_factor.factor);
  alp_info.SetNumExceptions(static_cast<uint16_t>(encoding_result.exceptions.size()));

  AlpForInfo<T> for_info;
  for_info.SetFrameOfReference(encoding_result.frame_of_reference);
  for_info.SetBitWidth(bitpacking_result.bit_width);

  return AlpEncodedVector<T>::Make(alp_info, for_info, num_elements,
                                   std::move(bitpacking_result.packed_integers),
                                   std::move(encoding_result.exception_positions),
                                   std::move(encoding_result.exceptions));
}

template <AlpFloatingType T>
void AlpCompression<T>::BitUnpackIntegers(std::span<const uint8_t> packed_integers,
                                          const AlpForInfo<T>& for_info,
                                          std::span<EncodedUnsigned> outputs) {
  const int32_t num_elements = static_cast<int32_t>(outputs.size());
  if (for_info.bit_width() > 0) {
    // Arrow's unpack handles arbitrary sizes: SIMD for complete batches,
    // then unpack_exact for the remainder. No need to manually split.
    const arrow::internal::UnpackOptions opts{
        .batch_size = static_cast<int>(num_elements),
        .bit_width = for_info.bit_width(),
    };
    arrow::internal::unpack(packed_integers.data(), outputs.data(), opts);
  } else if (num_elements > 0) {
    std::memset(outputs.data(), 0, outputs.size_bytes());
  }
}

template <AlpFloatingType T>
template <typename TargetType>
  requires AlpDecodeTarget<T, TargetType>
void AlpCompression<T>::PatchExceptions(
    std::span<const T> exceptions,
    std::span<const AlpFormatConstants::PositionType> exception_positions,
    std::span<TargetType> outputs) {
  ARROW_DCHECK_EQ(exceptions.size(), exception_positions.size());
  // `ivdep` assumes the positions are distinct. The encoder writes them in
  // order, and loading a page rejects positions that do not increase.
  int64_t exception_idx = 0;
  ARROW_ALP_UNROLL_AND_ASSUME_INDEPENDENT
  for (const AlpFormatConstants::PositionType exception_position : exception_positions) {
    ARROW_DCHECK_LT(exception_position, outputs.size());
    outputs[exception_position] = static_cast<T>(exceptions[exception_idx]);
    exception_idx++;
  }
}

template <AlpFloatingType T>
template <typename TargetType>
  requires AlpDecodeTarget<T, TargetType>
void AlpCompression<T>::Decompress(const AlpEncodedVectorView<T>& encoded_view,
                                   std::span<TargetType> outputs,
                                   std::span<EncodedUnsigned> integer_scratch) {
  const AlpInfo& alp_info = encoded_view.alp_info();
  const AlpForInfo<T>& for_info = encoded_view.for_info();
  const size_t num_elements = static_cast<size_t>(encoded_view.num_elements());
  // Output and scratch may be larger than this vector; only the prefix is used.
  ARROW_CHECK_GE(integer_scratch.size(), num_elements)
      << "ALP integer scratch buffer is too small: " << integer_scratch.size() << " < "
      << num_elements;
  ARROW_CHECK_GE(outputs.size(), num_elements)
      << "ALP output buffer is too small: " << outputs.size() << " < " << num_elements;

  const auto scratch = integer_scratch.first(num_elements);
  const auto output = outputs.first(num_elements);
  BitUnpackIntegers(encoded_view.packed_values(), for_info, scratch);

  // Fused unFOR + decode loop: undo FOR in unsigned arithmetic, reinterpret the
  // result as signed encoded integers, then apply the normative decimal decode.
  const EncodedUnsigned frame_of_ref = for_info.frame_of_reference();
  ARROW_ALP_UNROLL_AND_ASSUME_INDEPENDENT
  for (size_t i = 0; i < output.size(); ++i) {
    const EncodedUnsigned unfored_value = scratch[i] + frame_of_ref;
    const EncodedSigned signed_value = util::SafeCopy<EncodedSigned>(unfored_value);
    output[i] = AlpInlines<T>::DecodeValue(signed_value, alp_info.GetExponentAndFactor());
  }

  PatchExceptions<TargetType>(
      {encoded_view.exceptions().data(), encoded_view.exceptions().size()},
      {encoded_view.exception_positions().data(),
       encoded_view.exception_positions().size()},
      output);
}

// Template instantiations

template void AlpCompression<float>::Decompress<double>(
    const AlpEncodedVectorView<float>& encoded_view, std::span<double> outputs,
    std::span<AlpCompression<float>::EncodedUnsigned> integer_scratch);
template void AlpCompression<float>::Decompress<float>(
    const AlpEncodedVectorView<float>& encoded_view, std::span<float> outputs,
    std::span<AlpCompression<float>::EncodedUnsigned> integer_scratch);
template void AlpCompression<double>::Decompress<double>(
    const AlpEncodedVectorView<double>& encoded_view, std::span<double> outputs,
    std::span<AlpCompression<double>::EncodedUnsigned> integer_scratch);

template class AlpCompression<float>;
template class AlpCompression<double>;

}  // namespace arrow::util::alp

#undef ARROW_ALP_UNROLL_AND_ASSUME_INDEPENDENT
