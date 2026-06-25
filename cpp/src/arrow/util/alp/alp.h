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

#include <optional>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/alp/alp_constants.h"
#include "arrow/util/span.h"

namespace arrow {
namespace util {
namespace alp {

// ----------------------------------------------------------------------
// ALP Overview
//
// IMPORTANT: For abstract interfaces or examples how to use ALP, consult
// alp_codec.h.
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
//   | 5. SERIALIZATION (offset-based interleaved layout)              |
//   |    [Header][Offsets...][Vector₀][Vector₁]...                    |
//   |    where each Vector = [AlpInfo|ForInfo|Data]                   |
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
///
/// The underlying type is fixed at `uint8_t` because this enum is serialized
/// as a single byte in `AlpHeader::compression_mode`.
enum class AlpMode : uint8_t { kAlp = 0 };

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
// AlpEncodedVectorInfo (non-templated, ALP core metadata)

/// \brief ALP-specific metadata for an encoded vector (non-templated)
///
/// Contains the metadata specific to ALP's float-to-integer conversion:
///   - exponent/factor: parameters for decimal encoding
///   - num_exceptions: count of values that couldn't be losslessly encoded
///
/// This struct is the same size regardless of the floating-point type (float/double).
/// It is separate from the integer encoding metadata (e.g., FOR) to allow
/// different integer encodings to be used in the future.
///
/// Serialization format (4 bytes):
///
///   +------------------------------------------+
///   |  AlpEncodedVectorInfo (4 bytes)          |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  exponent (uint8_t) |  1 byte  |
///   |    1    |  factor (uint8_t)   |  1 byte  |
///   |    2    |  num_exceptions     |  2 bytes |
///   +------------------------------------------+
class AlpEncodedVectorInfo {
 public:
  AlpEncodedVectorInfo() = default;
  AlpEncodedVectorInfo(uint8_t exponent, uint8_t factor, int16_t num_exceptions)
      : exponent_(exponent), factor_(factor), num_exceptions_(num_exceptions) {}

  uint8_t exponent() const { return exponent_; }
  uint8_t factor() const { return factor_; }
  int16_t num_exceptions() const { return num_exceptions_; }

  void set_exponent(uint8_t exponent) { exponent_ = exponent; }
  void set_factor(uint8_t factor) { factor_ = factor; }
  void set_num_exceptions(int16_t num_exceptions) { num_exceptions_ = num_exceptions; }

  /// Size of the serialized portion (4 bytes, fixed)
  static constexpr int64_t kStoredSize =
      sizeof(uint8_t) + sizeof(uint8_t) + sizeof(int16_t);
  static_assert(kStoredSize == 4, "AlpEncodedVectorInfo stored size must be 4 bytes");

  /// \brief Store the ALP metadata into an output buffer
  ///
  /// \pre output_buffer.size() >= kStoredSize
  void Store(arrow::util::span<uint8_t> output_buffer) const;

  /// \brief Load ALP metadata from an input buffer
  ///
  /// \return the loaded metadata, or Status::Invalid if the buffer is too small
  static Result<AlpEncodedVectorInfo> Load(arrow::util::span<const uint8_t> input_buffer);

  /// \brief Get serialized size of the ALP metadata
  static int64_t GetStoredSize() { return kStoredSize; }

  /// \brief Get exponent and factor as a combined struct
  AlpExponentAndFactor GetExponentAndFactor() const {
    return AlpExponentAndFactor{exponent_, factor_};
  }

  bool operator==(const AlpEncodedVectorInfo& other) const {
    return exponent_ == other.exponent_ && factor_ == other.factor_ &&
           num_exceptions_ == other.num_exceptions_;
  }

  bool operator!=(const AlpEncodedVectorInfo& other) const { return !(*this == other); }

 private:
  uint8_t exponent_ = 0;
  uint8_t factor_ = 0;
  int16_t num_exceptions_ = 0;
};

// ----------------------------------------------------------------------
// AlpEncodedForVectorInfo (templated, FOR integer encoding metadata)

/// \brief FOR (Frame of Reference) encoding metadata for an encoded vector
///
/// Contains the metadata specific to FOR bit-packing integer encoding:
///   - frame_of_reference: minimum value subtracted from all encoded integers
///   - bit_width: number of bits used to pack each delta value
///
/// This struct is templated because frame_of_reference size depends on T:
///   - float:  uint32_t frame_of_reference (4 bytes)
///   - double: uint64_t frame_of_reference (8 bytes)
///
/// Serialization format for float (5 bytes):
///
///   +------------------------------------------+
///   |  AlpEncodedForVectorInfo<float> (5B)     |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  frame_of_reference |  4 bytes |
///   |    4    |  bit_width (uint8_t)|  1 byte  |
///   +------------------------------------------+
///
/// Serialization format for double (9 bytes):
///
///   +------------------------------------------+
///   |  AlpEncodedForVectorInfo<double> (9B)    |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  frame_of_reference |  8 bytes |
///   |    8    |  bit_width (uint8_t)|  1 byte  |
///   +------------------------------------------+
///
/// \tparam T the floating point type (float or double)
template <typename T>
class AlpEncodedForVectorInfo {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>,
                "AlpEncodedForVectorInfo only supports float and double");

 public:
  /// Use uint32_t for float, uint64_t for double (matches encoded integer size)
  using ExactType = typename AlpTypedConstants<T>::FloatingToExact;

  AlpEncodedForVectorInfo() = default;
  AlpEncodedForVectorInfo(ExactType frame_of_reference, uint8_t bit_width)
      : frame_of_reference_(frame_of_reference), bit_width_(bit_width) {}

  ExactType frame_of_reference() const { return frame_of_reference_; }
  uint8_t bit_width() const { return bit_width_; }

  void set_frame_of_reference(ExactType frame_of_reference) {
    frame_of_reference_ = frame_of_reference;
  }
  void set_bit_width(uint8_t bit_width) { bit_width_ = bit_width; }

  /// Size of the serialized portion (5 bytes for float, 9 for double)
  static constexpr int64_t kStoredSize = sizeof(ExactType) + 1;

  /// \brief Compute the bitpacked size in bytes from num_elements and bit_width
  ///
  /// \param[in] num_elements number of elements in this vector
  /// \param[in] bw bits per element
  /// \return the size in bytes of the bitpacked data
  static int64_t GetBitPackedSize(int32_t num_elements, uint8_t bw) {
    return (static_cast<int64_t>(num_elements) * bw + 7) / 8;
  }

  /// \brief Store the FOR metadata into an output buffer
  ///
  /// \pre output_buffer.size() >= kStoredSize
  void Store(arrow::util::span<uint8_t> output_buffer) const;

  /// \brief Load FOR metadata from an input buffer
  ///
  /// \return the loaded metadata, or Status::Invalid if the buffer is too small
  static Result<AlpEncodedForVectorInfo> Load(
      arrow::util::span<const uint8_t> input_buffer);

  /// \brief Get serialized size of the FOR metadata
  static int64_t GetStoredSize() { return kStoredSize; }

  /// \brief Get the size of the data section (packed values + exceptions)
  ///
  /// \param[in] num_elements number of elements in this vector
  /// \param[in] num_exceptions number of exceptions (from AlpEncodedVectorInfo)
  /// \return the size in bytes of packed values + exception positions + exceptions
  int64_t GetDataStoredSize(int32_t num_elements, int32_t num_exceptions) const {
    const int64_t bit_packed_size = GetBitPackedSize(num_elements, bit_width_);
    return bit_packed_size +
           num_exceptions * static_cast<int64_t>(sizeof(AlpConstants::PositionType) + sizeof(T));
  }

  bool operator==(const AlpEncodedForVectorInfo& other) const {
    return frame_of_reference_ == other.frame_of_reference_ &&
           bit_width_ == other.bit_width_;
  }

  bool operator!=(const AlpEncodedForVectorInfo& other) const { return !(*this == other); }

 private:
  ExactType frame_of_reference_ = 0;
  uint8_t bit_width_ = 0;
};

// ----------------------------------------------------------------------
// AlpEncodedVector

/// \class AlpEncodedVector
/// \brief A compressed ALP vector with metadata
///
/// Per-vector data layout:
///
///   +------------------------------------------------------------+
///   |  AlpEncodedVector<T> Data Layout                           |
///   +------------------------------------------------------------+
///   |  Section              |  Size (bytes)        | Description |
///   +-----------------------+----------------------+-------------+
///   |  1. AlpInfo           |  4B (fixed)          |  ALP meta   |
///   +-----------------------+----------------------+-------------+
///   |  2. ForInfo           |  6B (float) or       |  FOR meta   |
///   |                       |  10B (double)        |             |
///   +-----------------------+----------------------+-------------+
///   |  3. Packed Values     |  bit_packed_size     |  Bitpacked  |
///   |     (compressed data) |  (computed)          |  integers   |
///   +-----------------------+----------------------+-------------+
///   |  4. Exception Pos     |  num_exceptions * 2  |  uint16_t[] |
///   |     (indices)         |  (variable)          |  positions  |
///   +-----------------------+----------------------+-------------+
///   |  5. Exception Values  |  num_exceptions *    |  T[] (float/|
///   |     (original floats) |  sizeof(T)           |  double)    |
///   +------------------------------------------------------------+
///
/// Page-level layout (offset-based interleaved for O(1) random access):
///
///   +------------------------------------------------------------+
///   |  Page Layout                                               |
///   +------------------------------------------------------------+
///   |  [Header (7B)]                                             |
///   |  [Offset₀ | Offset₁ | ... | Offsetₙ₋₁]   ← Vector offsets  |
///   |  [Vector₀][Vector₁]...[Vectorₙ₋₁]        ← Concatenated     |
///   +------------------------------------------------------------+
///   where each Vector = [AlpInfo | ForInfo | Data]
///
/// The offset-based layout enables:
/// - O(1) random access to any vector via offset lookup
/// - Better locality for single-vector decompression
/// - Parallel decompression without coordination
///
/// Example for 1024 floats with 5 exceptions and bit_width=8:
///   - AlpInfo:            4 bytes (fixed)
///   - ForInfo:            6 bytes (float)
///   - Packed Values:   1024 bytes (1024 * 8 bits / 8)
///   - Exception Pos:     10 bytes (5 * 2)
///   - Exception Values:  20 bytes (5 * 4)
///   Total:             1064 bytes
template <typename T>
class AlpEncodedVector {
 public:
  AlpEncodedVector() = default;

  const AlpEncodedVectorInfo& alp_info() const { return alp_info_; }
  AlpEncodedVectorInfo& mutable_alp_info() { return alp_info_; }
  void set_alp_info(const AlpEncodedVectorInfo& info) { alp_info_ = info; }

  const AlpEncodedForVectorInfo<T>& for_info() const { return for_info_; }
  AlpEncodedForVectorInfo<T>& mutable_for_info() { return for_info_; }
  void set_for_info(const AlpEncodedForVectorInfo<T>& info) { for_info_ = info; }

  int32_t num_elements() const { return num_elements_; }
  void set_num_elements(int32_t n) { num_elements_ = n; }

  const std::vector<uint8_t>& packed_values() const { return packed_values_; }
  std::vector<uint8_t>& mutable_packed_values() { return packed_values_; }
  void set_packed_values(std::vector<uint8_t> v) { packed_values_ = std::move(v); }

  const std::vector<T>& exceptions() const { return exceptions_; }
  std::vector<T>& mutable_exceptions() { return exceptions_; }
  void set_exceptions(std::vector<T> v) { exceptions_ = std::move(v); }

  const std::vector<int16_t>& exception_positions() const { return exception_positions_; }
  std::vector<int16_t>& mutable_exception_positions() { return exception_positions_; }
  void set_exception_positions(std::vector<int16_t> v) {
    exception_positions_ = std::move(v);
  }

  /// Total metadata size (AlpInfo + ForInfo)
  static constexpr int64_t kMetadataStoredSize =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;

  /// \brief Get the size of the vector if stored into a sequential memory block
  ///
  /// \return the stored size in bytes
  int64_t GetStoredSize() const;

  /// \brief Get the stored size for given metadata and element count
  ///
  /// \param[in] alp_info the ALP metadata
  /// \param[in] for_info the FOR metadata
  /// \param[in] num_elements the number of elements in this vector
  /// \return the stored size in bytes
  static int64_t GetStoredSize(const AlpEncodedVectorInfo& alp_info,
                               const AlpEncodedForVectorInfo<T>& for_info,
                               int32_t num_elements);

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// Stores [AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]
  ///
  /// \param[out] output_buffer the buffer to store the compressed data into
  void Store(arrow::util::span<uint8_t> output_buffer) const;

  /// \brief Store only the data section (without metadata) into an output buffer
  ///
  /// Stores [PackedValues][ExceptionPositions][ExceptionValues]
  /// Used when metadata (AlpInfo, ForInfo) is written separately.
  ///
  /// \param[out] output_buffer the buffer to store the data section into
  void StoreDataOnly(arrow::util::span<uint8_t> output_buffer) const;

  /// \brief Get the size of the data section only (without metadata)
  ///
  /// \return the size in bytes of packed values + exception positions + exceptions
  int64_t GetDataStoredSize() const {
    return for_info_.GetDataStoredSize(num_elements_, alp_info_.num_exceptions());
  }

  /// \brief Load a compressed vector from a compact format from an input buffer
  ///
  /// \param[in] input_buffer the buffer to load from
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the loaded AlpEncodedVector, or Status::Invalid if data is malformed
  static Result<AlpEncodedVector> Load(arrow::util::span<const uint8_t> input_buffer,
                                       int32_t num_elements);

  bool operator==(const AlpEncodedVector<T>& other) const;

 private:
  AlpEncodedVectorInfo alp_info_;
  AlpEncodedForVectorInfo<T> for_info_;
  int32_t num_elements_ = 0;
  std::vector<uint8_t> packed_values_;
  std::vector<T> exceptions_;
  std::vector<int16_t> exception_positions_;
};

// ----------------------------------------------------------------------
// AlpEncodedVectorView

/// \class AlpEncodedVectorView
/// \brief A view into compressed ALP data optimized for decompression
///
/// Unlike AlpEncodedVector which copies all data into internal buffers,
/// AlpEncodedVectorView uses zero-copy for the large packed values array
/// while copying the small exception arrays into aligned storage.
///
/// The packed values are accessed via a span (zero-copy) since they are
/// byte arrays with no alignment requirements. Exception positions and
/// values are copied into aligned std::vectors because:
///   1. The serialized data may not be properly aligned for uint16_t/T access
///   2. Exceptions are rare (typically < 5%), so copying is negligible
///   3. This avoids undefined behavior from misaligned memory access
///
/// Use LoadView() to create a view, then pass to DecompressVectorView().
/// The underlying buffer must remain valid for the lifetime of the view
/// (for packed_values access).
template <typename T>
class AlpEncodedVectorView {
 public:
  AlpEncodedVectorView() = default;

  const AlpEncodedVectorInfo& alp_info() const { return alp_info_; }
  AlpEncodedVectorInfo& mutable_alp_info() { return alp_info_; }
  void set_alp_info(const AlpEncodedVectorInfo& info) { alp_info_ = info; }

  const AlpEncodedForVectorInfo<T>& for_info() const { return for_info_; }
  AlpEncodedForVectorInfo<T>& mutable_for_info() { return for_info_; }
  void set_for_info(const AlpEncodedForVectorInfo<T>& info) { for_info_ = info; }

  int32_t num_elements() const { return num_elements_; }
  void set_num_elements(int32_t n) { num_elements_ = n; }

  arrow::util::span<const uint8_t> packed_values() const { return packed_values_; }
  void set_packed_values(arrow::util::span<const uint8_t> v) { packed_values_ = v; }

  const std::vector<int16_t>& exception_positions() const { return exception_positions_; }
  std::vector<int16_t>& mutable_exception_positions() { return exception_positions_; }
  void set_exception_positions(std::vector<int16_t> v) {
    exception_positions_ = std::move(v);
  }

  const std::vector<T>& exceptions() const { return exceptions_; }
  std::vector<T>& mutable_exceptions() { return exceptions_; }
  void set_exceptions(std::vector<T> v) { exceptions_ = std::move(v); }

  /// \brief Create a zero-copy view from a compact format input buffer
  ///
  /// Expects format: [AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]
  ///
  /// \param[in] input_buffer the buffer to create a view into
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the view into the compressed data, or Status::Invalid if data is malformed
  static Result<AlpEncodedVectorView> LoadView(
      arrow::util::span<const uint8_t> input_buffer, int32_t num_elements);

  /// \brief Create a zero-copy view from data-only buffer (metadata provided separately)
  ///
  /// Used with the offset-based interleaved layout where AlpInfo and ForInfo are
  /// read first, then this is called to load the data section.
  /// Expects format: [PackedValues][ExceptionPositions][ExceptionValues] (no metadata)
  ///
  /// \param[in] input_buffer the buffer containing only the data section
  /// \param[in] alp_info the ALP metadata (already read)
  /// \param[in] for_info the FOR metadata (already read)
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the view into the compressed data, or Status::Invalid if data is malformed
  static Result<AlpEncodedVectorView> LoadViewDataOnly(
      arrow::util::span<const uint8_t> input_buffer,
      const AlpEncodedVectorInfo& alp_info,
      const AlpEncodedForVectorInfo<T>& for_info,
      int32_t num_elements);

  /// \brief Get the stored size of this vector in the buffer
  ///
  /// \return the stored size in bytes (includes AlpInfo + ForInfo + data)
  int64_t GetStoredSize() const;

  /// \brief Get the size of the data section only (without metadata)
  ///
  /// \return the size in bytes of packed values + exception positions + exceptions
  int64_t GetDataStoredSize() const {
    return for_info_.GetDataStoredSize(num_elements_, alp_info_.num_exceptions());
  }

 private:
  AlpEncodedVectorInfo alp_info_;
  AlpEncodedForVectorInfo<T> for_info_;
  int32_t num_elements_ = 0;
  arrow::util::span<const uint8_t> packed_values_;
  std::vector<int16_t> exception_positions_;
  std::vector<T> exceptions_;
};

// ----------------------------------------------------------------------
// AlpIntegerEncoding

/// \brief Integer encoding method used after ALP decimal encoding
///
/// Currently only FOR+BitPack is implemented. Future encodings can be added
/// by extending this enum and adding corresponding metadata structs.
enum class AlpIntegerEncoding : uint8_t { kForBitPack = 0 };

/// \brief Get the per-vector metadata size for a given integer encoding
///
/// \tparam T the floating point type (float or double)
/// \param[in] encoding the integer encoding method
/// \return size in bytes of the per-vector metadata for this encoding
template <typename T>
inline int64_t GetIntegerEncodingMetadataSize(AlpIntegerEncoding /*encoding*/) {
  return AlpEncodedForVectorInfo<T>::kStoredSize;
}

// ----------------------------------------------------------------------
// AlpEncodingParameters

/// \brief Preset for ALP compression
///
/// Helper struct for compression. Before a larger amount of data is compressed,
/// a preset is generated, which contains multiple combinations of exponents and
/// factors. For each vector that is compressed, one of the combinations of this
/// preset is chosen dynamically.
struct AlpEncodingParameters {
  /// Combinations of exponents and factors
  std::vector<AlpExponentAndFactor> combinations;
  /// Best compressed size for the parameters
  int64_t best_compressed_size = 0;
  /// Bit packing layout used for bitpacking
  AlpIntegerEncoding integer_encoding = AlpIntegerEncoding::kForBitPack;
};

template <typename T>
class AlpSampler;

// ----------------------------------------------------------------------
// AlpCompression

/// \class AlpCompression
/// \brief ALP compression and decompression facilities
///
/// AlpCompression contains all facilities to compress and decompress data with
/// ALP in a vectorized fashion. Use CreateEncodingParameters() first on a sample of
/// the input data, then compress it vector-wise via CompressVector(). To
/// serialize the data, use the facilities provided by AlpEncodedVector.
///
/// \tparam T the type of data to be compressed. Currently float and double.
template <typename T>
class AlpCompression {
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
                                            int32_t num_elements,
                                            const AlpEncodingParameters& preset);

  /// \brief Decompress a compressed vector with ALP
  ///
  /// \param[in] encoded_vector the ALP encoded vector to be decompressed
  /// \param[in] integer_encoding the integer encoding method used
  /// \param[out] output_vector the vector of floats to decompress into.
  ///             Must be able to contain encoded_vector.num_elements().
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecompressVector(const AlpEncodedVector<T>& encoded_vector,
                               AlpIntegerEncoding integer_encoding,
                               TargetType* output_vector);

  /// \brief Decompress using a zero-copy view (faster, no memory allocation)
  ///
  /// \param[in] encoded_view the zero-copy view into compressed data
  /// \param[in] integer_encoding the integer encoding method used
  /// \param[out] output_vector the vector of floats to decompress into.
  ///             Must be able to contain encoded_view.vector_info.num_elements.
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecompressVectorView(const AlpEncodedVectorView<T>& encoded_view,
                                   AlpIntegerEncoding integer_encoding,
                                   TargetType* output_vector);

 protected:
  /// \brief Creates an EncodingParameters consisting of multiple factors/exponents
  ///
  /// \param[in] vectors_sampled the sampled vectors to derive combinations from
  /// \return the EncodingParameters
  static AlpEncodingParameters CreateEncodingParameters(
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
  static std::optional<int64_t> EstimateCompressedSize(
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
    std::vector<SignedExactType> encoded_integers;
    std::vector<T> exceptions;
    std::vector<int16_t> exception_positions;
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
  /// \param[in] input_vector encoded integers (after bit unpacking, still with FOR)
  /// \param[in] alp_info ALP metadata with exponent and factor
  /// \param[in] for_info FOR metadata with frame_of_reference
  /// \param[in] num_elements number of elements to decode
  /// \param[out] output_vector output buffer to write decoded floats to
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecodeVector(arrow::util::span<ExactType> input_vector,
                           const AlpEncodedVectorInfo& alp_info,
                           const AlpEncodedForVectorInfo<T>& for_info,
                           int32_t num_elements,
                           TargetType* output_vector);

  /// \brief Helper struct to encapsulate the result from BitPackIntegers
  struct BitPackingResult {
    std::vector<uint8_t> packed_integers;
    uint8_t bit_width = 0;
    int32_t bit_packed_size = 0;
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
  /// \param[in] for_info FOR metadata with bit width and frame of reference
  /// \param[in] num_elements number of elements to unpack
  /// \return a vector of unpacked integers (still with frame of reference)
  static std::vector<ExactType> BitUnpackIntegers(
      arrow::util::span<const uint8_t> packed_integers,
      const AlpEncodedForVectorInfo<T>& for_info, int32_t num_elements);

  /// \brief Patch exceptions into the decoded output vector
  ///
  /// Replaces placeholder values at exception positions with the original
  /// floating point values that could not be losslessly encoded.
  ///
  /// \param[in] exceptions the original floats stored as exceptions
  /// \param[in] exception_positions indices where exceptions should be placed
  /// \param[out] output the decoded output vector to patch exceptions into
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void PatchExceptions(arrow::util::span<const T> exceptions,
                              arrow::util::span<const int16_t> exception_positions,
                              TargetType* output);
};

}  // namespace alp
}  // namespace util
}  // namespace arrow
