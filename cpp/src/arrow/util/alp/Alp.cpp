#include "arrow/util/alp/Alp.hpp"

#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>

#include "arrow/util/alp/AlpConstants.hpp"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/span.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace util {
namespace alp {

bool AlpEncodedVectorInfo::operator==(const AlpEncodedVectorInfo& other) const {
  return exponentAndFactor == other.exponentAndFactor &&
         frameOfReference == other.frameOfReference && bitWidth == other.bitWidth &&
         bitPackedSize == other.bitPackedSize && numElements == other.numElements &&
         numExceptions == other.numExceptions;
}

void AlpEncodedVectorInfo::store(const arrow::util::span<char> outputBuffer) const {
  ARROW_CHECK(outputBuffer.size() >= getStoredSize())
      << "alp_vector_info_output_too_small: " << outputBuffer.size() << " vs " << getStoredSize();

  std::memcpy(outputBuffer.data(), this, getStoredSize());
}

AlpEncodedVectorInfo AlpEncodedVectorInfo::load(arrow::util::span<const char> inputBuffer) {
  ARROW_CHECK(inputBuffer.size() >= getStoredSize())
      << "alp_vector_info_input_too_small: " << inputBuffer.size() << " vs " << getStoredSize();

  AlpEncodedVectorInfo result;
  std::memcpy(&result, inputBuffer.data(), getStoredSize());
  ARROW_CHECK(result.numElements <= AlpConstants::kAlpVectorSize)
      << "alp_compression_state_element_count_too_large: " << result.numElements << " vs "
      << AlpConstants::kAlpVectorSize;

  return result;
}

uint64_t AlpEncodedVectorInfo::getStoredSize() { return sizeof(AlpEncodedVectorInfo); }

template <typename T>
void AlpEncodedVector<T>::store(const arrow::util::span<char> outputBuffer) const {
  const uint64_t overallSize = getStoredSize();
  ARROW_CHECK(outputBuffer.size() >= overallSize)
      << "alp_bit_packed_vector_store_output_too_small: " << outputBuffer.size() << " vs "
      << overallSize;
  vectorInfo.store(outputBuffer);
  uint64_t compressionOffset = AlpEncodedVectorInfo::getStoredSize();

  // Store all successfully compressed values first.
  std::memcpy(outputBuffer.data() + compressionOffset, packedValues.data(),
              vectorInfo.bitPackedSize);
  compressionOffset += vectorInfo.bitPackedSize;

  ARROW_CHECK(vectorInfo.numExceptions == exceptions.size() &&
              vectorInfo.numExceptions == exceptionPositions.size())
      << "alp_bit_packed_vector_store_num_exceptions_mismatch: " << vectorInfo.numExceptions
      << " vs " << exceptions.size() << " vs " << exceptionPositions.size();

  // Store exceptions, consisting of their positions and their values.
  const uint64_t exceptionPositionSize =
      vectorInfo.numExceptions * sizeof(AlpConstants::PositionType);
  std::memcpy(outputBuffer.data() + compressionOffset, exceptionPositions.data(),
              exceptionPositionSize);
  compressionOffset += exceptionPositionSize;

  const uint64_t exceptionSize = vectorInfo.numExceptions * sizeof(T);
  std::memcpy(outputBuffer.data() + compressionOffset, exceptions.data(), exceptionSize);
  compressionOffset += exceptionSize;

  ARROW_CHECK(compressionOffset == overallSize)
      << "alp_bit_packed_vector_size_mismatch: " << compressionOffset << " vs " << overallSize;
}

template <typename T>
AlpEncodedVector<T> AlpEncodedVector<T>::load(const arrow::util::span<const char> inputBuffer) {
  AlpEncodedVector<T> result;
  result.vectorInfo = AlpEncodedVectorInfo::load(inputBuffer);
  uint64_t inputOffset = AlpEncodedVectorInfo::getStoredSize();

  const uint64_t overallSize = getStoredSize(result.vectorInfo);

  ARROW_CHECK(inputBuffer.size() >= overallSize)
      << "alp_compression_state_input_too_small: " << inputBuffer.size() << " vs " << overallSize;
  ARROW_CHECK(result.vectorInfo.numElements <= AlpConstants::kAlpVectorSize)
      << "alp_compression_state_element_count_too_large: " << result.vectorInfo.numElements
      << " vs " << AlpConstants::kAlpVectorSize;

  // Optimization: Use UnsafeResize to avoid zero-initialization before memcpy.
  // This is safe for POD types since we immediately overwrite with memcpy.
  result.packedValues.UnsafeResize(result.vectorInfo.bitPackedSize);
  std::memcpy(result.packedValues.data(), inputBuffer.data() + inputOffset,
              result.vectorInfo.bitPackedSize);
  inputOffset += result.vectorInfo.bitPackedSize;

  result.exceptionPositions.UnsafeResize(result.vectorInfo.numExceptions);
  const uint64_t exceptionPositionSize =
      result.vectorInfo.numExceptions * sizeof(AlpConstants::PositionType);
  std::memcpy(result.exceptionPositions.data(), inputBuffer.data() + inputOffset,
              exceptionPositionSize);
  inputOffset += exceptionPositionSize;

  result.exceptions.UnsafeResize(result.vectorInfo.numExceptions);
  const uint64_t exceptionSize = result.vectorInfo.numExceptions * sizeof(T);
  std::memcpy(result.exceptions.data(), inputBuffer.data() + inputOffset, exceptionSize);
  return result;
}

template <typename T>
uint64_t AlpEncodedVector<T>::getStoredSize() const {
  return AlpEncodedVectorInfo::getStoredSize() + vectorInfo.bitPackedSize +
         vectorInfo.numExceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

template <typename T>
uint64_t AlpEncodedVector<T>::getStoredSize(const AlpEncodedVectorInfo& info) {
  return AlpEncodedVectorInfo::getStoredSize() + info.bitPackedSize +
         info.numExceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
}

template <typename T>
bool AlpEncodedVector<T>::operator==(const AlpEncodedVector<T>& other) const {
  // Manual comparison since StaticVector doesn't have operator==.
  const bool packedValuesEqual =
      (packedValues.size() == other.packedValues.size()) &&
      std::equal(packedValues.begin(), packedValues.end(), other.packedValues.begin());
  const bool exceptionsEqual =
      (exceptions.size() == other.exceptions.size()) &&
      std::equal(exceptions.begin(), exceptions.end(), other.exceptions.begin());
  const bool exceptionPositionsEqual =
      (exceptionPositions.size() == other.exceptionPositions.size()) &&
      std::equal(exceptionPositions.begin(), exceptionPositions.end(),
                 other.exceptionPositions.begin());
  return vectorInfo == other.vectorInfo && packedValuesEqual && exceptionsEqual &&
         exceptionPositionsEqual;
}

template class AlpEncodedVector<float>;
template class AlpEncodedVector<double>;

template <typename T>
class AlpInlines : private AlpConstants {
 public:
  using Constants = AlpTypedConstants<T>;
  using ExactType = typename Constants::FloatingToExact;
  using SignedExactType = typename Constants::FloatingToSignedExact;

  // Checks if float is a special value that cannot be converted to a decimal.
  static inline bool isImpossibleToEncode(const T n) {
    // We do not have to check for positive or negative infinity, since
    // std::numeric_limits<T>::infinity() > std::numeric_limits<T>::max()
    // and vice versa for negative infinity.
    return std::isnan(n) || n > Constants::kEncodingUpperLimit ||
           n < Constants::kEncodingLowerLimit ||
           (n == 0.0 && std::signbit(n));  // Verification for -0.0
  }

  // Converts a float to an Int without rounding.
  static inline auto fastRound(T n) -> SignedExactType {
    n = n + Constants::kMagicNumber - Constants::kMagicNumber;
    return static_cast<SignedExactType>(n);
  }

  // Fast way to round float to nearest integer.
  static inline auto numberToInt(T n) -> SignedExactType {
    if (isImpossibleToEncode(n)) {
      return static_cast<SignedExactType>(Constants::kEncodingUpperLimit);
    }
    return fastRound(n);
  }
  // Convert a float into an int using encodingOptions.
  static inline SignedExactType encodeValue(const T value,
                                            const AlpExponentAndFactor exponentAndFactor) {
    const T tmpEncodedValue = value * Constants::getExponent(exponentAndFactor.exponent) *
                              Constants::getFactor(exponentAndFactor.factor);
    return numberToInt(tmpEncodedValue);
  }

  // Reconvert an int to a float using encodingOptions.
  static inline T decodeValue(const SignedExactType encodedValue,
                              const AlpExponentAndFactor exponentAndFactor) {
    // The cast to T is needed to prevent a signed integer overflow.
    return static_cast<T>(encodedValue) * getFactor(exponentAndFactor.factor) *
           Constants::getFactor(exponentAndFactor.exponent);
  }
};

struct AlpCombination {
  AlpExponentAndFactor exponentAndFactor;
  uint64_t numAppearances{0};
  uint64_t estimatedCompressionSize{0};
};

/*
 * Return TRUE if c1 is a better combination than c2.
 * First criteria is number of times it appears as best combination.
 * Second criteria is the estimated compression size.
 * Third criteria is bigger exponent.
 * Fourth criteria is bigger factor.
 */
static bool compareAlpCombinations(const AlpCombination& c1, const AlpCombination& c2) {
  return (c1.numAppearances > c2.numAppearances) ||
         (c1.numAppearances == c2.numAppearances &&
          (c1.estimatedCompressionSize < c2.estimatedCompressionSize)) ||
         ((c1.numAppearances == c2.numAppearances &&
           c1.estimatedCompressionSize == c2.estimatedCompressionSize) &&
          (c2.exponentAndFactor.exponent < c1.exponentAndFactor.exponent)) ||
         ((c1.numAppearances == c2.numAppearances &&
           c1.estimatedCompressionSize == c2.estimatedCompressionSize &&
           c2.exponentAndFactor.exponent == c1.exponentAndFactor.exponent) &&
          (c2.exponentAndFactor.factor < c1.exponentAndFactor.factor));
}

/*
 * Dry compress a vector (ideally a sample) to estimate ALP compression size given an exponent and
 * factor.
 */
template <typename T>
std::optional<uint64_t> AlpCompression<T>::estimateCompressedSize(
    const std::vector<T>& inputVector, const AlpExponentAndFactor exponentAndFactor,
    const bool penalizeExceptions) {
  SignedExactType maxEncodedValue = std::numeric_limits<SignedExactType>::min();
  SignedExactType minEncodedValue = std::numeric_limits<SignedExactType>::max();

  uint64_t numExceptions = 0;
  uint64_t numNonExceptions = 0;
  for (const T& value : inputVector) {
    const SignedExactType encodedValue = AlpInlines<T>::encodeValue(value, exponentAndFactor);
    T decodedValue = AlpInlines<T>::decodeValue(encodedValue, exponentAndFactor);
    if (decodedValue == value) {
      numNonExceptions++;
      maxEncodedValue = std::max(encodedValue, maxEncodedValue);
      minEncodedValue = std::min(encodedValue, minEncodedValue);
      continue;
    }
    numExceptions++;
  }

  // We penalize combinations which yield almost all exceptions.
  if (penalizeExceptions && numNonExceptions < 2) {
    return std::nullopt;
  }

  // Evaluate factor/exponent compression size (we optimize for FOR).
  const ExactType delta =
      (static_cast<ExactType>(maxEncodedValue) - static_cast<ExactType>(minEncodedValue));

  const uint32_t estimatedBitsPerValue = static_cast<uint32_t>(std::ceil(std::log2(delta + 1)));
  uint64_t estimatedCompressionSize = inputVector.size() * estimatedBitsPerValue;
  estimatedCompressionSize += numExceptions * (kExactTypeBitSize + (sizeof(PositionType) * 8));
  return estimatedCompressionSize;
}

/*
 * Find the best combinations of factor-exponent from each vector sampled from a rowgroup.
 * This function is called once per segment.
 * This operates over ALP first level samples.
 */
template <typename T>
AlpEncodingPreset AlpCompression<T>::createEncodingPreset(
    const std::vector<std::vector<T>>& vectorsSampled) {
  static constexpr uint64_t maxCombinationCount =
      (Constants::kMaxExponent + 1) * (Constants::kMaxExponent + 2) / 2;

  std::map<AlpExponentAndFactor, uint64_t> bestKCombinationsHash;

  uint64_t bestCompressedSizeBits = std::numeric_limits<uint64_t>::max();
  // For each vector sampled.
  for (const std::vector<T>& sampledVector : vectorsSampled) {
    const uint64_t numSamples = sampledVector.size();
    const AlpExponentAndFactor bestEncodingOptions{Constants::kMaxExponent,
                                                   Constants::kMaxExponent};

    // We start our optimization with the worst possible total bits obtained from compression.
    const uint64_t bestTotalBits = (numSamples * (kExactTypeBitSize + sizeof(PositionType) * 8)) +
                                   (numSamples * kExactTypeBitSize);

    // N of appearances is irrelevant at this phase; we search for the best compression for the
    // vector.
    AlpCombination bestCombination{bestEncodingOptions, 0, bestTotalBits};
    // We try all combinations in search for the one which minimizes the compression size.
    for (uint8_t expIdx = 0; expIdx <= Constants::kMaxExponent; expIdx++) {
      for (uint8_t factorIdx = 0; factorIdx <= expIdx; factorIdx++) {
        const AlpExponentAndFactor currentExponentAndFactor{expIdx, factorIdx};
        std::optional<uint64_t> estimatedCompressionSize = estimateCompressedSize(
            sampledVector, currentExponentAndFactor, /* penalizeExceptions= */ true);

        // Skip comparison for values that are not compressible.
        if (!estimatedCompressionSize.has_value()) {
          continue;
        }

        const AlpCombination currentCombination{currentExponentAndFactor, 0,
                                                *estimatedCompressionSize};
        if (compareAlpCombinations(currentCombination, bestCombination)) {
          bestCombination = currentCombination;
          bestCompressedSizeBits = std::min(bestCompressedSizeBits, *estimatedCompressionSize);
        }
      }
    }
    bestKCombinationsHash[bestCombination.exponentAndFactor]++;
  }

  // Convert our hash to a Combination vector to be able to sort.
  // Note that this vector should mostly be small (< 10 combinations).
  std::vector<AlpCombination> bestKCombinations;
  bestKCombinations.reserve(std::min(bestKCombinationsHash.size(), maxCombinationCount));
  for (const auto& combination : bestKCombinationsHash) {
    bestKCombinations.emplace_back(
        AlpCombination{combination.first,   // Encoding Indices
                       combination.second,  // N of times it appeared (hash value)
                       0}                   // Compression size is irrelevant at this phase since we
                                            // compare combinations from different vectors.
    );
  }
  std::sort(bestKCombinations.begin(), bestKCombinations.end(), compareAlpCombinations);

  std::vector<AlpExponentAndFactor> combinations;
  // Save k' best combinations.
  for (uint64_t i = 0; i < std::min(kMaxCombinations, (uint8_t)bestKCombinations.size()); i++) {
    combinations.push_back(bestKCombinations[i].exponentAndFactor);
  }

  const uint64_t bestCompressedSizeBytes = std::ceil(bestCompressedSizeBits / 8.0);
  return {combinations, bestCompressedSizeBytes};
}

template <typename T>
std::vector<T> AlpCompression<T>::createSample(const arrow::util::span<const T> input) {
  // We sample equidistant values within a vector; to do this we skip a fixed number of values.
  const auto idxIncrements = std::max<uint32_t>(
      1, static_cast<uint32_t>(std::ceil(static_cast<double>(input.size()) /
                                         AlpConstants::kSamplerSamplesPerVector)));
  std::vector<T> vectorSample;
  vectorSample.reserve(std::ceil(input.size() / static_cast<double>(idxIncrements)));
  for (uint64_t i = 0; i < input.size(); i += idxIncrements) {
    vectorSample.push_back(input[i]);
  }
  return vectorSample;
}

/*
 * Find the best combination of factor-exponent for a vector from within the best k combinations.
 * This is ALP second level sampling.
 */
template <typename T>
AlpExponentAndFactor AlpCompression<T>::findBestExponentAndFactor(
    arrow::util::span<const T> input, const std::vector<AlpExponentAndFactor>& combinations) {
  if (combinations.size() == 1) {
    return combinations.front();
  }

  const std::vector<T> sampleVector = createSample(input);

  AlpExponentAndFactor bestExponentAndFactor;
  uint64_t bestTotalBits = std::numeric_limits<uint64_t>::max();
  uint64_t worseTotalBitsCounter = 0;

  // We try each K combination in search for the one which minimizes the compression size in the
  // vector.
  for (const AlpExponentAndFactor& exponentAndFactor : combinations) {
    std::optional<uint64_t> estimatedCompressionSize =
        estimateCompressedSize(sampleVector, exponentAndFactor, /* penalizeExceptions= */ false);

    // Skip exponents and factors which result in many exceptions.
    if (!estimatedCompressionSize.has_value()) {
      continue;
    }

    // If current compression size is worse (higher) or equal than the current best combination.
    if (estimatedCompressionSize >= bestTotalBits) {
      worseTotalBitsCounter += 1;
      // Early exit strategy.
      if (worseTotalBitsCounter == kSamplingEarlyExitThreshold) {
        break;
      }
      continue;
    }
    // Otherwise we replace the best and continue trying with the next combination.
    bestTotalBits = estimatedCompressionSize.value();
    bestExponentAndFactor = exponentAndFactor;
    worseTotalBitsCounter = 0;
  }
  return bestExponentAndFactor;
}

template <typename T>
auto AlpCompression<T>::encodeVector(const arrow::util::span<const T> inputVector,
                                     AlpExponentAndFactor exponentAndFactor) -> EncodingResult {
  arrow::internal::StaticVector<SignedExactType, kAlpVectorSize> encodedIntegers;
  arrow::internal::StaticVector<T, kAlpVectorSize> exceptions;
  arrow::internal::StaticVector<PositionType, kAlpVectorSize> exceptionPositions;

  // Encoding Float/Double to SignedExactType(Int32, Int64).
  // We encode all the values regardless of their correctness to recover the original
  // floating-point.
  uint64_t inputOffset = 0;
  for (const T input : inputVector) {
    const SignedExactType encodedValue = AlpInlines<T>::encodeValue(input, exponentAndFactor);
    const T decodedValue = AlpInlines<T>::decodeValue(encodedValue, exponentAndFactor);
    encodedIntegers.push_back(encodedValue);
    // We detect exceptions using a predicated comparison.
    if (decodedValue != input) {
      exceptionPositions.push_back(inputOffset);
    }
    inputOffset++;
  }

  // Finding first non-exception value.
  SignedExactType firstNonExceptionValue = 0;
  PositionType exceptionOffset = 0;
  for (const PositionType exceptionPosition : exceptionPositions) {
    if (exceptionOffset != exceptionPosition) {
      firstNonExceptionValue = encodedIntegers[exceptionOffset];
      break;
    }
    exceptionOffset++;
  }

  // Use the first non-exception value as a placeholder for all exception values in the vector.
  for (const PositionType exceptionPosition : exceptionPositions) {
    const T actualValue = inputVector[exceptionPosition];
    encodedIntegers[exceptionPosition] = firstNonExceptionValue;
    exceptions.push_back(actualValue);
  }

  // Analyze FOR.
  const auto [min, max] = std::minmax_element(encodedIntegers.begin(), encodedIntegers.end());
  const auto frameOfReference = static_cast<ExactType>(*min);

  for (SignedExactType& encodedInteger : encodedIntegers) {
    ExactType& uEncodedInteger = *reinterpret_cast<ExactType*>(&encodedInteger);
    uEncodedInteger -= frameOfReference;
  }

  const ExactType minMaxDiff = (static_cast<ExactType>(*max) - static_cast<ExactType>(*min));
  return EncodingResult{encodedIntegers, exceptions, exceptionPositions, minMaxDiff,
                        frameOfReference};
}

template <typename T>
auto AlpCompression<T>::bitPackIntegers(const arrow::util::span<const SignedExactType> integers,
                                        const uint64_t minMaxDiff) -> BitPackingResult {
  uint8_t bitWidth = 0;

  if (minMaxDiff == 0) {
    bitWidth = 0;
  } else if constexpr (std::is_same_v<T, float>) {
    bitWidth = sizeof(T) * 8 - __builtin_clz(minMaxDiff);
  } else if constexpr (std::is_same_v<T, double>) {
    bitWidth = sizeof(T) * 8 - __builtin_clzll(minMaxDiff);
  }
  const uint64_t bitPackedSize = std::ceil((bitWidth * integers.size()) / 8.0);

  arrow::internal::StaticVector<uint8_t, kAlpVectorSize * sizeof(T)> packedIntegers;
  // An unsafe resize is used here because we know the size of the vector and we want to avoid
  // zero-initialization. Zero initialization was resulting in around 2-3% degradation in
  // compression speed.
  packedIntegers.UnsafeResize(bitPackedSize);
  if (bitWidth > 0) {  // We only execute the BP if we are writing the data.
    // Use Arrow's BitWriter for packing (loop-based).
    arrow::bit_util::BitWriter writer(packedIntegers.data(), static_cast<int>(bitPackedSize));
    for (uint64_t i = 0; i < integers.size(); ++i) {
      writer.PutValue(static_cast<uint64_t>(integers[i]), bitWidth);
    }
    writer.Flush(false);
  }
  return {packedIntegers, bitWidth, bitPackedSize};
}

/*
 * ALP Compress.
 */
template <typename T>
AlpEncodedVector<T> AlpCompression<T>::compressVector(const T* inputVector, uint16_t numElements,
                                                      const AlpEncodingPreset& preset) {
  // Perform the compression by finding a fitting exponent and factor, use them to encode the
  // input, and finally bitpack the encoded data.

  // std::cout << numElements << std::endl;
  const arrow::util::span<const T> inputSpan{inputVector, numElements};
  const AlpExponentAndFactor exponentAndFactor =
      findBestExponentAndFactor(inputSpan, preset.combinations);
  const EncodingResult encodingResult = encodeVector(inputSpan, exponentAndFactor);
  BitPackingResult bitpackingResult;
  switch (preset.bitPackLayout) {
    case AlpBitPackLayout::kNormal:
      bitpackingResult = bitPackIntegers(encodingResult.encodedIntegers, encodingResult.minMaxDiff);
      break;
    default:
      ARROW_CHECK(false) << "invalid_bit_pack_layout: " << static_cast<int>(preset.bitPackLayout);
      break;
  }

  // Transfer compressed data into a serializable format.
  const AlpEncodedVectorInfo vectorInfo{exponentAndFactor,
                                        encodingResult.frameOfReference,
                                        bitpackingResult.bitWidth,
                                        bitpackingResult.bitPackedSize,
                                        numElements,
                                        static_cast<uint16_t>(encodingResult.exceptions.size())};

  return AlpEncodedVector<T>{vectorInfo, bitpackingResult.packedIntegers, encodingResult.exceptions,
                             encodingResult.exceptionPositions};
}

template <typename T>
auto AlpCompression<T>::bitUnpackIntegers(const arrow::util::span<const uint8_t> packedIntegers,
                                          const AlpEncodedVectorInfo vectorInfo)
    -> arrow::internal::StaticVector<ExactType, kAlpVectorSize> {
  arrow::internal::StaticVector<ExactType, kAlpVectorSize> encodedIntegers;
  // Optimization: Use UnsafeResize to avoid zero-initialization.
  // Safe because we immediately write to all elements via unpack().
  encodedIntegers.UnsafeResize(vectorInfo.numElements);

  if (vectorInfo.bitWidth > 0) {
    // Arrow's SIMD unpack works in fixed batch sizes. All SIMD implementations
    // (SIMD128/NEON, SIMD256/AVX2, SIMD512/AVX512) have identical batch sizes:
    // - uint32_t (float): Simd*UnpackerForWidth::kValuesUnpacked = 32
    // - uint64_t (double): Simd*UnpackerForWidth::kValuesUnpacked = 64
    // These constants are in anonymous namespaces (internal implementation detail),
    // so we hardcode them here. The values are fundamental to the unpacking algorithm
    // and consistent across all SIMD implementations.
    constexpr int kMinBatchSize = std::is_same_v<T, float> ? 32 : 64;
    const int numElements = static_cast<int>(vectorInfo.numElements);
    const int numCompleteBatches = numElements / kMinBatchSize;
    const int numCompleteElements = numCompleteBatches * kMinBatchSize;

    // Use Arrow's SIMD-optimized unpack for complete batches.
    if (numCompleteElements > 0) {
      arrow::internal::unpack(packedIntegers.data(), encodedIntegers.data(), numCompleteElements,
                              vectorInfo.bitWidth);
    }

    // Handle remaining elements (<64) with BitReader to match BitWriter format.
    const int remaining = numElements - numCompleteElements;
    if (remaining > 0) {
      // Calculate byte offset where SIMD unpack finished
      const uint64_t bitsConsumedBySIMD = static_cast<uint64_t>(numCompleteElements) * vectorInfo.bitWidth;
      // Round up to next byte
      const uint64_t bytesConsumedBySIMD = (bitsConsumedBySIMD + 7) / 8;

      // Use BitReader for the remaining elements starting from where SIMD left off
      arrow::bit_util::BitReader reader(packedIntegers.data() + bytesConsumedBySIMD,
                                        static_cast<int>(packedIntegers.size() - bytesConsumedBySIMD));

      for (int i = 0; i < remaining; ++i) {
        uint64_t value = 0;
        if (reader.GetValue(vectorInfo.bitWidth, &value)) {
          encodedIntegers[numCompleteElements + i] = static_cast<ExactType>(value);
        } else {
          encodedIntegers[numCompleteElements + i] = 0;
        }
      }
    }
  } else {
    std::memset(encodedIntegers.data(), 0, vectorInfo.numElements * sizeof(ExactType));
  }
  return encodedIntegers;
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::decodeVector(TargetType* outputVector,
                                     arrow::util::span<ExactType> inputVector,
                                     const AlpEncodedVectorInfo vectorInfo) {
  // unFOR - Optimized with index-based loop and ivdep for vectorization.
  const size_t numElements = inputVector.size();
  ExactType* data = inputVector.data();
  const ExactType frameOfRef = vectorInfo.frameOfReference;

#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (size_t i = 0; i < numElements; ++i) {
    data[i] += frameOfRef;
  }

  // Decoding - Optimized version.
  const ExactType* input = data;

#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (size_t i = 0; i < numElements; ++i) {
    SignedExactType signedValue;
    std::memcpy(&signedValue, &input[i], sizeof(SignedExactType));
    outputVector[i] = AlpInlines<T>::decodeValue(signedValue, vectorInfo.exponentAndFactor);
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::patchExceptions(TargetType* output, arrow::util::span<const T> exceptions,
                                        arrow::util::span<const uint16_t> exceptionPositions) {
  // Exceptions Patching.
  uint64_t exceptionIdx = 0;
#pragma GCC unroll AlpConstants::kLoopUnrolls
#pragma GCC ivdep
  for (uint16_t const exceptionPosition : exceptionPositions) {
    output[exceptionPosition] = static_cast<T>(exceptions[exceptionIdx]);
    exceptionIdx++;
  }
}

template <typename T>
template <typename TargetType>
void AlpCompression<T>::decompressVector(const AlpEncodedVector<T>& packedVector,
                                         const AlpBitPackLayout bitPackLayout, TargetType* output) {
  static_assert(sizeof(T) <= sizeof(TargetType));
  const AlpEncodedVectorInfo& vectorInfo = packedVector.vectorInfo;

  switch (bitPackLayout) {
    case AlpBitPackLayout::kNormal: {
      arrow::internal::StaticVector<ExactType, kAlpVectorSize> encodedIntegers =
          bitUnpackIntegers(packedVector.packedValues, vectorInfo);
      decodeVector<TargetType>(output, {encodedIntegers.data(), vectorInfo.numElements},
                               vectorInfo);
      patchExceptions<TargetType>(output, packedVector.exceptions, packedVector.exceptionPositions);
    } break;
    default:
      ARROW_CHECK(false) << "invalid_bit_pack_layout: " << static_cast<int>(bitPackLayout);
      break;
  }
}

template void AlpCompression<float>::decompressVector<double>(
    const AlpEncodedVector<float>& packedVector, const AlpBitPackLayout bitPackLayout,
    double* output);
template void AlpCompression<float>::decompressVector<float>(
    const AlpEncodedVector<float>& packedVector, const AlpBitPackLayout bitPackLayout,
    float* output);
template void AlpCompression<double>::decompressVector<double>(
    const AlpEncodedVector<double>& packedVector, const AlpBitPackLayout bitPackLayout,
    double* output);

template class AlpCompression<float>;
template class AlpCompression<double>;

}  // namespace alp
}  // namespace util
}  // namespace arrow
