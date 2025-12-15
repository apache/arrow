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

#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/small_vector.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// ALP Overview
//
// IMPORTANT: For abstract interfaces or examples how to use ALP, consult
// alp_wrapper.h.
// This is our implementation of the adaptive lossless floating-point
// compression for decimals (ALP) (https://dl.acm.org/doi/10.1145/3626717).
// It works by converting a float into a decimal (if possible). The exponent
// and factor are chosen per vector. Each float is converted using
// c(f) = int64(f * 10^exponent * 10^-factor). The converted floats are then
// encoded via a delta frame of reference and bitpacked. Every exception,
// where the conversion/reconversion changes the value of the float, is stored
// separately and has to be patched into the decompressed vector afterwards.
//
// ==========================================================================
//                    ALP COMPRESSION/DECOMPRESSION PIPELINE
// ==========================================================================
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
//   |    * Calculate bit_width = log2(max_delta)                       |
//   |    * Pack each value into bit_width bits                         |
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
//   Serialized bytes -> AlpEncodedVector::Load()
//        |
//        v
//   +------------------------------------------------------------------+
//   | 1. BIT UNPACKING                                                 |
//   |    * Extract bit_width from metadata                             |
//   |    * Unpack each value from bit_width bits -> delta values       |
//   +------------------------------------+-----------------------------+
//                                        | delta values
//                                        v
//   +------------------------------------------------------------------+
//   | 2. REVERSE FRAME OF REFERENCE (unFOR)                            |
//   |    * Add back min: encoded[i] = delta[i] + frame_of_reference    |
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
//   |    * Replace values at exception_positions[] with exceptions[]   |
//   +------------------------------------+-----------------------------+
//                                        |
//                                        v
//   Output: Original float/double array (lossless!)
//
// ==========================================================================

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
///   |    8    |  frame_of_reference |  8 bytes |
///   |   16    |  bit_width (uint8_t)|  1 byte  |
///   |   17    |  [padding]          |  7 bytes |
///   |   24    |  bit_packed_size    |  8 bytes |
///   |   32    |  num_elements       |  2 bytes |
///   |   34    |  num_exceptions     |  2 bytes |
///   +------------------------------------------+
struct AlpEncodedVectorInfo {
  /// Exponent and factor used for compression
  AlpExponentAndFactor exponent_and_factor;
  /// Delta used for frame of reference encoding
  uint64_t frame_of_reference = 0;
  /// Bitwidth used for bitpacking
  uint8_t bit_width = 0;
  /// Overall bitpacked size of non-exception values
  uint64_t bit_packed_size = 0;
  /// Number of elements encoded in this vector
  uint16_t num_elements = 0;
  /// Number of exceptions stored in this vector
  uint16_t num_exceptions = 0;

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// \param[out] output_buffer the buffer to store the compressed data into
  void Store(arrow::util::span<char> output_buffer) const;

  /// \brief Load a compressed vector into the state from a compact format
  ///
  /// \param[in] input_buffer the buffer to load from
  /// \return the loaded AlpEncodedVectorInfo
  static AlpEncodedVectorInfo Load(arrow::util::span<const char> input_buffer);

  /// \brief Get serialized size of the encoded vector info
  ///
  /// \return the size in bytes
  static uint64_t GetStoredSize();

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
///   |  2. Packed Values     |  bit_packed_size     |  Bitpacked  |
///   |     (compressed data) |  (variable)          |  integers   |
///   +-----------------------+----------------------+-------------+
///   |  3. Exception Pos     |  num_exceptions * 2  |  uint16_t[] |
///   |     (indices)         |  (variable)          |  positions  |
///   +-----------------------+----------------------+-------------+
///   |  4. Exception Values  |  num_exceptions *    |  T[] (float/|
///   |     (original floats) |  sizeof(T)           |  double)    |
///   +------------------------------------------------------------+
///
/// Example for 1024 floats with 5 exceptions and bit_width=8:
///   - VectorInfo:        36 bytes
///   - Packed Values:    1024 bytes (1024 * 8 bits / 8)
///   - Exception Pos:      10 bytes (5 * 2)
///   - Exception Values:   20 bytes (5 * 4)
///   Total:              1090 bytes
template <typename T>
class AlpEncodedVector {
 public:
  /// Metadata of the encoded vector
  AlpEncodedVectorInfo vector_info;
  /// Successfully encoded and bitpacked data
  arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)>
      packed_values;
  /// Float values that could not be converted successfully
  arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;
  /// Positions of the exceptions in the decompressed vector
  arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize> exception_positions;

  /// \brief Get the size of the vector if stored into a sequential memory block
  ///
  /// \return the stored size in bytes
  uint64_t GetStoredSize() const;

  /// \brief Get the stored size for a given vector info
  ///
  /// \param[in] info the vector info to calculate size for
  /// \return the stored size in bytes
  static uint64_t GetStoredSize(const AlpEncodedVectorInfo& info);

  /// \brief Get the number of elements in this vector
  ///
  /// \return number of elements
  uint64_t GetNumElements() const { return vector_info.num_elements; }

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// \param[out] output_buffer the buffer to store the compressed data into
  void Store(arrow::util::span<char> output_buffer) const;

  /// \brief Load a compressed vector from a compact format from an input buffer
  ///
  /// \param[in] input_buffer the buffer to load from
  /// \return the loaded AlpEncodedVector
  static AlpEncodedVector Load(arrow::util::span<const char> input_buffer);

  bool operator==(const AlpEncodedVector<T>& other) const;
};

// ----------------------------------------------------------------------
// AlpEncodedVectorView

/// \class AlpEncodedVectorView
/// \brief A zero-copy view into compressed ALP data
///
/// Unlike AlpEncodedVector which copies data into internal buffers,
/// AlpEncodedVectorView holds spans that point directly to the compressed
/// data buffer. This avoids memory copies during decompression.
///
/// Use LoadView() to create a view, then pass to DecompressVectorView().
/// The underlying buffer must remain valid for the lifetime of the view.
template <typename T>
struct AlpEncodedVectorView {
  /// Metadata of the encoded vector (copied, small fixed size)
  AlpEncodedVectorInfo vector_info;
  /// View into bitpacked data (no copy)
  arrow::util::span<const uint8_t> packed_values;
  /// View into exception values (no copy)
  arrow::util::span<const T> exceptions;
  /// View into exception positions (no copy)
  arrow::util::span<const uint16_t> exception_positions;

  /// \brief Create a zero-copy view from a compact format input buffer
  ///
  /// \param[in] input_buffer the buffer to create a view into
  /// \return the view into the compressed data
  static AlpEncodedVectorView LoadView(arrow::util::span<const char> input_buffer);

  /// \brief Get the stored size of this vector in the buffer
  ///
  /// \return the stored size in bytes
  uint64_t GetStoredSize() const;
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
  uint64_t best_compressed_size = 0;
  /// Bit packing layout used for bitpacking
  AlpBitPackLayout bit_pack_layout = AlpBitPackLayout::kNormal;
};

template <typename T>
class AlpSampler;

// ----------------------------------------------------------------------
// AlpCompression

/// \class AlpCompression
/// \brief ALP compression and decompression facilities
///
/// AlpCompression contains all facilities to compress and decompress data with
/// ALP in a vectorized fashion. Use CreateEncodingPreset() first on a sample of
/// the input data, then compress it vector-wise via CompressVector(). To
/// serialize the data, use the facilities provided by AlpEncodedVector.
///
/// \tparam T the type of data to be compressed. Currently float and double.
template <typename T>
class AlpCompression : private AlpConstants {
 public:
  using Constants = AlpTypedConstants<T>;
  using ExactType = typename Constants::FloatingToExact;
  using SignedExactType = typename Constants::FloatingToSignedExact;
  static constexpr uint8_t kExactTypeBitSize = sizeof(T) * 8;

  /// \brief Compress a vector of floating point values via ALP
  ///
  /// \param[in] input_vector a vector of floats containing input to compress
  /// \param[in] num_elements the number of values to be compressed
  /// \param[in] preset the preset to be used for compression
  /// \return an ALP encoded vector
  static AlpEncodedVector<T> CompressVector(const T* input_vector,
                                            uint16_t num_elements,
                                            const AlpEncodingPreset& preset);

  /// \brief Decompress a compressed vector with ALP
  ///
  /// \param[in] encoded_vector the ALP encoded vector to be decompressed
  /// \param[in] bit_pack_layout the bit packing layout used
  /// \param[out] output_vector the vector of floats to decompress into.
  ///             Must be able to contain encoded_vector.GetNumElements().
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecompressVector(const AlpEncodedVector<T>& encoded_vector,
                               AlpBitPackLayout bit_pack_layout,
                               TargetType* output_vector);

  /// \brief Decompress using a zero-copy view (faster, no memory allocation)
  ///
  /// \param[in] encoded_view the zero-copy view into compressed data
  /// \param[in] bit_pack_layout the bit packing layout used
  /// \param[out] output_vector the vector of floats to decompress into.
  ///             Must be able to contain encoded_view.vector_info.num_elements.
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecompressVectorView(const AlpEncodedVectorView<T>& encoded_view,
                                   AlpBitPackLayout bit_pack_layout,
                                   TargetType* output_vector);

 protected:
  /// \brief Creates an EncodingPreset consisting of multiple factors/exponents
  ///
  /// \param[in] vectors_sampled the sampled vectors to derive combinations from
  /// \return the EncodingPreset
  static AlpEncodingPreset CreateEncodingPreset(
      const std::vector<std::vector<T>>& vectors_sampled);
  friend AlpSampler<T>;

 private:
  /// \brief Create a subsample of floats from an input vector for preset gen
  ///
  /// \param[in] input the input vector to sample from
  /// \return a vector containing a representative subsample of input values
  static std::vector<T> CreateSample(arrow::util::span<const T> input);

  /// \brief Perform a dry-compression to estimate the compressed size
  ///
  /// \param[in] input_vector the input vector to estimate compression for
  /// \param[in] exponent_and_factor the exponent/factor combination to evaluate
  /// \param[in] penalize_exceptions if true, applies a penalty for exceptions
  /// \return the estimated compressed size in bytes, or std::nullopt if the
  ///         data is not compressible using these settings
  static std::optional<uint64_t> EstimateCompressedSize(
      const std::vector<T>& input_vector,
      AlpExponentAndFactor exponent_and_factor,
      bool penalize_exceptions);

  /// \brief Find the best exponent and factor combination for an input vector
  ///
  /// Iterates through all combinations in the preset and selects the one
  /// that produces the smallest compressed size.
  ///
  /// \param[in] input the input vector to find the best combination for
  /// \param[in] combinations candidate exponent/factor combinations from preset
  /// \return the exponent and factor combination yielding best compression
  static AlpExponentAndFactor FindBestExponentAndFactor(
      arrow::util::span<const T> input,
      const std::vector<AlpExponentAndFactor>& combinations);

  /// \brief Helper struct to encapsulate the result from EncodeVector()
  struct EncodingResult {
    arrow::internal::StaticVector<SignedExactType, AlpConstants::kAlpVectorSize>
        encoded_integers;
    arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;
    arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize>
        exception_positions;
    ExactType min_max_diff = 0;
    ExactType frame_of_reference = 0;
  };

  /// \brief Encode a vector via decimal encoding and frame of reference (FOR)
  ///
  /// \param[in] input_vector the input vector of floating point values
  /// \param[in] exponent_and_factor the exponent/factor for decimal encoding
  /// \return an EncodingResult containing encoded integers, exceptions, etc.
  static EncodingResult EncodeVector(arrow::util::span<const T> input_vector,
                                     AlpExponentAndFactor exponent_and_factor);

  /// \brief Decode a vector of integers back to floating point values
  ///
  /// \param[out] output_vector output buffer to write decoded floats to
  /// \param[in] input_vector encoded integers (after bit unpacking and unFOR)
  /// \param[in] vector_info metadata with exponent, factor, decoding params
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecodeVector(TargetType* output_vector,
                           arrow::util::span<ExactType> input_vector,
                           AlpEncodedVectorInfo vector_info);

  /// \brief Helper struct to encapsulate the result from BitPackIntegers
  struct BitPackingResult {
    arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)>
        packed_integers;
    uint8_t bit_width = 0;
    uint64_t bit_packed_size = 0;
  };

  /// \brief Bitpack the encoded integers as the final step of compression
  ///
  /// Calculates the minimum bit width required and packs each value
  /// using that many bits, resulting in tightly packed binary data.
  ///
  /// \param[in] integers the encoded integers (after FOR subtraction)
  /// \param[in] min_max_diff the difference between max and min values,
  ///            used to determine the required bit width
  /// \return a BitPackingResult with packed bytes, bit width, and packed size
  static BitPackingResult BitPackIntegers(
      arrow::util::span<const SignedExactType> integers, uint64_t min_max_diff);

  /// \brief Unpack bitpacked integers back to their original representation
  ///
  /// The result is still encoded (FOR applied) and needs decoding to get floats.
  ///
  /// \param[in] packed_integers the bitpacked integer data to unpack
  /// \param[in] vector_info metadata with bit width and unpacking parameters
  /// \return a vector of unpacked integers (still with frame of reference)
  static arrow::internal::StaticVector<ExactType, kAlpVectorSize> BitUnpackIntegers(
      arrow::util::span<const uint8_t> packed_integers,
      AlpEncodedVectorInfo vector_info);

  /// \brief Patch exceptions into the decoded output vector
  ///
  /// Replaces placeholder values at exception positions with the original
  /// floating point values that could not be losslessly encoded.
  ///
  /// \param[out] output the decoded output vector to patch exceptions into
  /// \param[in] exceptions the original floats stored as exceptions
  /// \param[in] exception_positions indices where exceptions should be placed
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void PatchExceptions(TargetType* output,
                              arrow::util::span<const T> exceptions,
                              arrow::util::span<const uint16_t> exception_positions);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow
