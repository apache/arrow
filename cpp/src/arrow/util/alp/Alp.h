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

// Adaptive Lossless floating-Point (ALP) compression implementation

#pragma once

#include <vector>

#include "arrow/util/alp/AlpConstants.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// ALP Overview
//
// IMPORTANT: For abstract interfaces or examples how to use ALP, consult AlpWrapper.h.
// This is our implementation of the adaptive lossless floating-point compression for decimals
// (ALP) (https://dl.acm.org/doi/10.1145/3626717). It works by converting a float into a decimal (if
// possible). The exponent and factor are chosen per vector. Each float is converted using c(f) =
// int64(f * 10^exponent * 10^-factor). The converted floats are than encoded via a delta frame of
// reference and bitpacked. Every exception, where the conversion/reconversion changes the value of
// the float, is stored separately and has to be patched into the decompressed vector afterwards.
//
// ==========================================================================================
//                         ALP COMPRESSION/DECOMPRESSION PIPELINE
// ==========================================================================================
//
// COMPRESSION FLOW:
// -----------------
//
//   Input: float/double array
//        |
//        v
//   +------------------------------------------------------------------+
//   | 1. SAMPLING & PRESET GENERATION                                  |
//   |    * Sample vectors from dataset                                 |
//   |    * Try all exponent/factor combinations (e, f)                 |
//   |    * Select best k combinations for preset                       |
//   +------------------------------------+-----------------------------+
//                                        | preset.combinations
//                                        v
//   +------------------------------------------------------------------+
//   | 2. PER-VECTOR COMPRESSION                                        |
//   |    a) Find best (e,f) from preset for this vector                |
//   |    b) Encode: encoded[i] = int64(value[i] * 10^e * 10^-f)        |
//   |    c) Verify: if decode(encoded[i]) != value[i] -> exception     |
//   |    d) Replace exceptions with placeholder value                  |
//   +------------------------------------+-----------------------------+
//                                        | encoded integers + exceptions
//                                        v
//   +------------------------------------------------------------------+
//   | 3. FRAME OF REFERENCE (FOR)                                      |
//   |    * Find min value in encoded integers                          |
//   |    * Subtract min from all values: delta[i] = encoded[i] - min   |
//   +------------------------------------+-----------------------------+
//                                        | delta values (smaller range)
//                                        v
//   +------------------------------------------------------------------+
//   | 4. BIT PACKING                                                   |
//   |    * Calculate bitWidth = log2(max_delta)                        |
//   |    * Pack each value into bitWidth bits                          |
//   |    * Result: tightly packed binary data                          |
//   +------------------------------------+-----------------------------+
//                                        | packed bytes
//                                        v
//   +------------------------------------------------------------------+
//   | 5. SERIALIZATION (see AlpEncodedVector diagram below)            |
//   |    [VectorInfo][PackedData][ExceptionPos][ExceptionValues]       |
//   +------------------------------------------------------------------+
//
//
// DECOMPRESSION FLOW:
// -------------------
//
//   Serialized bytes -> AlpEncodedVector::load()
//        |
//        v
//   +------------------------------------------------------------------+
//   | 1. BIT UNPACKING                                                 |
//   |    * Extract bitWidth from metadata                              |
//   |    * Unpack each value from bitWidth bits -> delta values        |
//   +------------------------------------+-----------------------------+
//                                        | delta values
//                                        v
//   +------------------------------------------------------------------+
//   | 2. REVERSE FRAME OF REFERENCE (unFOR)                            |
//   |    * Add back min: encoded[i] = delta[i] + frameOfReference      |
//   +------------------------------------+-----------------------------+
//                                        | encoded integers
//                                        v
//   +------------------------------------------------------------------+
//   | 3. DECODE                                                        |
//   |    * Apply inverse formula: value[i] = encoded[i] * 10^-e * 10^f |
//   +------------------------------------+-----------------------------+
//                                        | decoded floats (with placeholders)
//                                        v
//   +------------------------------------------------------------------+
//   | 4. PATCH EXCEPTIONS                                              |
//   |    * Replace values at exceptionPositions[] with exceptions[]    |
//   +------------------------------------+-----------------------------+
//                                        |
//                                        v
//   Output: Original float/double array (lossless!)
//
// ==========================================================================================

// ----------------------------------------------------------------------
// AlpMode

/// \brief ALP compression mode
///
/// Currently only ALP (decimal compression) is implemented.
enum class AlpMode { kAlp };

// ----------------------------------------------------------------------
// AlpExponentAndFactor

/// \brief Helper struct to encapsulate the exponent and factor
struct AlpExponentAndFactor {
  uint8_t exponent{0};
  uint8_t factor{0};

  bool operator==(const AlpExponentAndFactor& other) const {
    return exponent == other.exponent && factor == other.factor;
  }

  /// \brief Comparison operator for deterministic std::map ordering
  bool operator<(const AlpExponentAndFactor& other) const {
    if (exponent != other.exponent) return exponent < other.exponent;
    return factor < other.factor;
  }
};

// ----------------------------------------------------------------------
// AlpEncodedVectorInfo

/// \brief Metadata for an encoded vector
///
/// Helper class to encapsulate all metadata of an encoded vector to be able
/// to load and decompress it.
///
/// Serialization format (stored as raw binary struct):
///
///   +------------------------------------------+
///   |  AlpEncodedVectorInfo (23+ bytes)        |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  exponent (uint8_t) |  1 byte  |
///   |    1    |  factor (uint8_t)   |  1 byte  |
///   |    2    |  [padding]          |  6 bytes |
///   |    8    |  frameOfReference   |  8 bytes |
///   |   16    |  bitWidth (uint8_t) |  1 byte  |
///   |   17    |  [padding]          |  7 bytes |
///   |   24    |  bitPackedSize      |  8 bytes |
///   |   32    |  numElements        |  2 bytes |
///   |   34    |  numExceptions      |  2 bytes |
///   +------------------------------------------+
struct AlpEncodedVectorInfo {
  /// Exponent and factor used for compression
  AlpExponentAndFactor exponentAndFactor;
  /// Delta used for frame of reference encoding
  uint64_t frameOfReference = 0;
  /// Bitwidth used for bitpacking
  uint8_t bitWidth = 0;
  /// Overall bitpacked size of non-exception values
  uint64_t bitPackedSize = 0;
  /// Number of elements encoded in this vector
  uint16_t numElements = 0;
  /// Number of exceptions stored in this vector
  uint16_t numExceptions = 0;

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// \param[out] outputBuffer the buffer to store the compressed data into
  void store(arrow::util::span<char> outputBuffer) const;

  /// \brief Load a compressed vector into the state from a compact format
  ///
  /// \param[in] inputBuffer the buffer to load from
  /// \return the loaded AlpEncodedVectorInfo
  static AlpEncodedVectorInfo load(arrow::util::span<const char> inputBuffer);

  /// \brief Get serialized size of the encoded vector info
  ///
  /// \return the size in bytes
  static uint64_t getStoredSize();

  bool operator==(const AlpEncodedVectorInfo& other) const;
};

// ----------------------------------------------------------------------
// AlpEncodedVector

/// \class AlpEncodedVector
/// \brief A compressed ALP vector with metadata
///
/// Complete serialization format for an ALP compressed vector:
///
///   +------------------------------------------------------------+
///   |  AlpEncodedVector<T> Serialized Layout                     |
///   +------------------------------------------------------------+
///   |  Section              |  Size (bytes)        | Description |
///   +-----------------------+----------------------+-------------+
///   |  1. VectorInfo        |  sizeof(VectorInfo)  |  Metadata   |
///   |     (see above)       |  (~36 with padding)  |             |
///   +-----------------------+----------------------+-------------+
///   |  2. Packed Values     |  bitPackedSize       |  Bitpacked  |
///   |     (compressed data) |  (variable)          |  integers   |
///   +-----------------------+----------------------+-------------+
///   |  3. Exception Pos     |  numExceptions * 2   |  uint16_t[] |
///   |     (indices)         |  (variable)          |  positions  |
///   +-----------------------+----------------------+-------------+
///   |  4. Exception Values  |  numExceptions *     |  T[] (float/|
///   |     (original floats) |  sizeof(T)           |  double)    |
///   +------------------------------------------------------------+
///
/// Example for 1024 floats with 5 exceptions and bitWidth=8:
///   - VectorInfo:        36 bytes
///   - Packed Values:    1024 bytes (1024 * 8 bits / 8)
///   - Exception Pos:      10 bytes (5 * 2)
///   - Exception Values:   20 bytes (5 * 4)
///   Total:              1090 bytes
template <typename T>
class AlpEncodedVector {
 public:
  /// Metadata of the encoded vector
  AlpEncodedVectorInfo vectorInfo;
  /// Successfully encoded and bitpacked data
  arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)> packedValues;
  /// Float values that could not be converted successfully
  arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;
  /// Positions of the exceptions in the decompressed vector
  arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize> exceptionPositions;

  /// \brief Get the size of the vector if stored into a sequential memory block
  ///
  /// \return the stored size in bytes
  uint64_t getStoredSize() const;

  /// \brief Get the stored size for a given vector info
  ///
  /// \param[in] info the vector info to calculate size for
  /// \return the stored size in bytes
  static uint64_t getStoredSize(const AlpEncodedVectorInfo& info);

  /// \brief Get the number of elements in this vector
  ///
  /// \return number of elements
  uint64_t getNumElements() const { return vectorInfo.numElements; }

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// \param[out] outputBuffer the buffer to store the compressed data into
  void store(arrow::util::span<char> outputBuffer) const;

  /// \brief Load a compressed vector from a compact format from an input buffer
  ///
  /// \param[in] inputBuffer the buffer to load from
  /// \return the loaded AlpEncodedVector
  static AlpEncodedVector load(arrow::util::span<const char> inputBuffer);

  bool operator==(const AlpEncodedVector<T>& other) const;
};

// ----------------------------------------------------------------------
// AlpBitPackLayout

/// \brief Bit packing layout
///
/// Currently only normal bit packing is implemented.
enum class AlpBitPackLayout { kNormal };

// ----------------------------------------------------------------------
// AlpEncodingPreset

/// \brief Preset for ALP compression
///
/// Helper struct for compression. Before a larger amount of data is compressed,
/// a preset is generated, which contains multiple combinations of exponents and
/// factors. For each vector that is compressed, one of the combinations of this
/// preset is chosen dynamically.
struct AlpEncodingPreset {
  /// Combinations of exponents and factors
  std::vector<AlpExponentAndFactor> combinations;
  /// Best compressed size for the preset
  uint64_t bestCompressedSize = 0;
  /// Bit packing layout used for bitpacking
  AlpBitPackLayout bitPackLayout = AlpBitPackLayout::kNormal;
};

template <typename T>
class AlpSampler;

// ----------------------------------------------------------------------
// AlpCompression

/// \class AlpCompression
/// \brief ALP compression and decompression facilities
///
/// AlpCompression contains all facilities to compress and decompress data with
/// ALP in a vectorized fashion. Use createEncodingPreset() first on a sample of
/// the input data, then compress it vector-wise via compressVector(). To serialize
/// the data, use the facilities provided by AlpEncodedVector.
///
/// \tparam T the type of data to be compressed. Currently supported are float and double.
template <typename T>
class AlpCompression : private AlpConstants {
 public:
  using Constants = AlpTypedConstants<T>;
  using ExactType = typename Constants::FloatingToExact;
  using SignedExactType = typename Constants::FloatingToSignedExact;
  static constexpr uint8_t kExactTypeBitSize = sizeof(T) * 8;

  /// \brief Compress a vector of floating point values via ALP
  ///
  /// \param[in] inputVector a vector of floats containing the input to be compressed
  /// \param[in] numElements the number of values to be compressed
  /// \param[in] preset the preset to be used for compression
  /// \return an ALP encoded vector
  static AlpEncodedVector<T> compressVector(const T* inputVector, uint16_t numElements,
                                            const AlpEncodingPreset& preset);

  /// \brief Decompress a compressed vector with ALP
  ///
  /// \param[in] encodedVector the ALP encoded vector to be decompressed
  /// \param[in] bitPackLayout the bit packing layout used
  /// \param[out] outputVector the vector of floats to decompress the encoded vector into.
  ///             Must be able to contain encodedVector.getNumElements().
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void decompressVector(const AlpEncodedVector<T>& encodedVector,
                               const AlpBitPackLayout bitPackLayout, TargetType* outputVector);

 protected:
  /// \brief Creates an EncodingPreset consisting of multiple factors and exponents
  ///
  /// \param[in] vectorsSampled the sampled vectors used to derive the combinations from
  /// \return the EncodingPreset
  static AlpEncodingPreset createEncodingPreset(const std::vector<std::vector<T>>& vectorsSampled);
  friend AlpSampler<T>;

 private:
  /// \brief Create a subsample of floats from an input vector for preset generation
  ///
  /// \param[in] input the input vector to sample from
  /// \return a vector containing a representative subsample of the input values
  static std::vector<T> createSample(arrow::util::span<const T> input);

  /// \brief Perform a dry-compression to estimate the compressed size
  ///
  /// \param[in] inputVector the input vector to estimate compression for
  /// \param[in] exponentAndFactor the exponent and factor combination to evaluate
  /// \param[in] penalizeExceptions if true, applies a penalty to the estimated size
  ///            for each exception
  /// \return the estimated compressed size in bytes, or std::nullopt if the data
  ///         is not compressible using these settings
  static std::optional<uint64_t> estimateCompressedSize(const std::vector<T>& inputVector,
                                                        AlpExponentAndFactor exponentAndFactor,
                                                        bool penalizeExceptions);

  /// \brief Find the best exponent and factor combination for an input vector
  ///
  /// Iterates through all combinations in the preset and selects the one
  /// that produces the smallest compressed size.
  ///
  /// \param[in] input the input vector to find the best combination for
  /// \param[in] combinations the candidate exponent/factor combinations from the preset
  /// \return the exponent and factor combination that yields the best compression
  static AlpExponentAndFactor findBestExponentAndFactor(
      arrow::util::span<const T> input, const std::vector<AlpExponentAndFactor>& combinations);

  /// \brief Helper struct to encapsulate the result from encodeVector()
  struct EncodingResult {
    arrow::internal::StaticVector<SignedExactType, AlpConstants::kAlpVectorSize> encodedIntegers;
    arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;
    arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize> exceptionPositions;
    ExactType minMaxDiff = 0;
    ExactType frameOfReference = 0;
  };

  /// \brief Encode a vector via decimal encoding and frame of reference (FOR) encoding
  ///
  /// \param[in] inputVector the input vector of floating point values to encode
  /// \param[in] exponentAndFactor the exponent and factor to use for decimal encoding
  /// \return an EncodingResult containing encoded integers, exceptions, exception positions,
  ///         and frame of reference metadata
  static EncodingResult encodeVector(arrow::util::span<const T> inputVector,
                                     AlpExponentAndFactor exponentAndFactor);

  /// \brief Decode a vector of integers back to floating point values
  ///
  /// \param[out] outputVector the output buffer to write decoded floating point values to
  /// \param[in] inputVector the input vector of encoded integers (after bit unpacking and unFOR)
  /// \param[in] vectorInfo the metadata containing exponent, factor, and other decoding parameters
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void decodeVector(TargetType* outputVector, arrow::util::span<ExactType> inputVector,
                           AlpEncodedVectorInfo vectorInfo);

  /// \brief Helper struct to encapsulate the result from bitPackIntegers
  struct BitPackingResult {
    arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)>
        packedIntegers;
    uint8_t bitWidth = 0;
    uint64_t bitPackedSize = 0;
  };

  /// \brief Bitpack the encoded integers as the final step of compression
  ///
  /// Calculates the minimum bit width required and packs each value
  /// using that many bits, resulting in tightly packed binary data.
  ///
  /// \param[in] integers the encoded integers (after FOR subtraction) to bitpack
  /// \param[in] minMaxDiff the difference between the maximum and minimum values,
  ///            used to determine the required bit width
  /// \return a BitPackingResult containing the packed bytes, bit width, and packed size
  static BitPackingResult bitPackIntegers(arrow::util::span<const SignedExactType> integers,
                                          uint64_t minMaxDiff);

  /// \brief Unpack bitpacked integers back to their original integer representation
  ///
  /// The result is still encoded (FOR applied) and needs decoding to get floats.
  ///
  /// \param[in] packedIntegers the bitpacked integer data to unpack
  /// \param[in] vectorInfo the metadata containing bit width and other unpacking parameters
  /// \return a vector of unpacked integers (still with frame of reference applied)
  static arrow::internal::StaticVector<ExactType, kAlpVectorSize> bitUnpackIntegers(
      arrow::util::span<const uint8_t> packedIntegers, AlpEncodedVectorInfo vectorInfo);

  /// \brief Patch exceptions into the decoded output vector
  ///
  /// Replaces placeholder values at exception positions with the original
  /// floating point values that could not be losslessly encoded.
  ///
  /// \param[out] output the decoded output vector to patch exceptions into
  /// \param[in] exceptions the original floating point values that were stored as exceptions
  /// \param[in] exceptionPositions the indices in the output vector where exceptions
  ///            should be placed
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void patchExceptions(TargetType* output, arrow::util::span<const T> exceptions,
                              arrow::util::span<const uint16_t> exceptionPositions);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow
