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
//   | 5. SERIALIZATION (metadata-at-start layout for random access)   |
//   |    [Header][VectorInfo₀|VectorInfo₁|...][Data₀|Data₁|...]       |
//   |    All VectorInfo first, then all data sections consecutively.  |
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
struct AlpEncodedVectorInfo {
  /// Exponent used for decimal encoding (multiply by 10^exponent)
  uint8_t exponent = 0;
  /// Factor used for decimal encoding (divide by 10^factor)
  uint8_t factor = 0;
  /// Number of exceptions stored in this vector
  uint16_t num_exceptions = 0;

  /// Size of the serialized portion (4 bytes, fixed)
  static constexpr uint64_t kStoredSize = 4;

  /// \brief Store the ALP metadata into an output buffer
  void Store(arrow::util::span<char> output_buffer) const;

  /// \brief Load ALP metadata from an input buffer
  static AlpEncodedVectorInfo Load(arrow::util::span<const char> input_buffer);

  /// \brief Get serialized size of the ALP metadata
  static uint64_t GetStoredSize() { return kStoredSize; }

  /// \brief Get exponent and factor as a combined struct
  AlpExponentAndFactor GetExponentAndFactor() const {
    return AlpExponentAndFactor{exponent, factor};
  }

  bool operator==(const AlpEncodedVectorInfo& other) const {
    return exponent == other.exponent && factor == other.factor &&
           num_exceptions == other.num_exceptions;
  }

  bool operator!=(const AlpEncodedVectorInfo& other) const { return !(*this == other); }
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
/// Serialization format for float (6 bytes):
///
///   +------------------------------------------+
///   |  AlpEncodedForVectorInfo<float> (6B)     |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  frame_of_reference |  4 bytes |
///   |    4    |  bit_width (uint8_t)|  1 byte  |
///   |    5    |  reserved (uint8_t) |  1 byte  |
///   +------------------------------------------+
///
/// Serialization format for double (10 bytes):
///
///   +------------------------------------------+
///   |  AlpEncodedForVectorInfo<double> (10B)   |
///   +------------------------------------------+
///   |  Offset |  Field              |  Size    |
///   +---------+---------------------+----------+
///   |    0    |  frame_of_reference |  8 bytes |
///   |    8    |  bit_width (uint8_t)|  1 byte  |
///   |    9    |  reserved (uint8_t) |  1 byte  |
///   +------------------------------------------+
///
/// \tparam T the floating point type (float or double)
template <typename T>
struct AlpEncodedForVectorInfo {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>,
                "AlpEncodedForVectorInfo only supports float and double");

  /// Use uint32_t for float, uint64_t for double (matches encoded integer size)
  using ExactType = typename AlpTypedConstants<T>::FloatingToExact;

  /// Delta used for frame of reference encoding (4 bytes for float, 8 for double)
  ExactType frame_of_reference = 0;
  /// Bitwidth used for bitpacking
  uint8_t bit_width = 0;
  /// Reserved for future use (padding for alignment)
  uint8_t reserved = 0;

  /// Size of the serialized portion (6 bytes for float, 10 for double)
  static constexpr uint64_t kStoredSize = sizeof(ExactType) + 2;

  /// \brief Compute the bitpacked size in bytes from num_elements and bit_width
  ///
  /// \param[in] num_elements number of elements in this vector
  /// \param[in] bit_width bits per element
  /// \return the size in bytes of the bitpacked data
  static uint64_t GetBitPackedSize(uint16_t num_elements, uint8_t bit_width) {
    return (static_cast<uint64_t>(num_elements) * bit_width + 7) / 8;
  }

  /// \brief Store the FOR metadata into an output buffer
  void Store(arrow::util::span<char> output_buffer) const;

  /// \brief Load FOR metadata from an input buffer
  static AlpEncodedForVectorInfo Load(arrow::util::span<const char> input_buffer);

  /// \brief Get serialized size of the FOR metadata
  static uint64_t GetStoredSize() { return kStoredSize; }

  /// \brief Get the size of the data section (packed values + exceptions)
  ///
  /// \param[in] num_elements number of elements in this vector
  /// \param[in] num_exceptions number of exceptions (from AlpEncodedVectorInfo)
  /// \return the size in bytes of packed values + exception positions + exceptions
  uint64_t GetDataStoredSize(uint16_t num_elements, uint16_t num_exceptions) const {
    const uint64_t bit_packed_size = GetBitPackedSize(num_elements, bit_width);
    return bit_packed_size +
           num_exceptions * (sizeof(AlpConstants::PositionType) + sizeof(T));
  }

  bool operator==(const AlpEncodedForVectorInfo& other) const {
    return frame_of_reference == other.frame_of_reference &&
           bit_width == other.bit_width;
  }

  bool operator!=(const AlpEncodedForVectorInfo& other) const { return !(*this == other); }
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
/// Page-level layout (grouped metadata-at-start for efficient random access):
///
///   +------------------------------------------------------------+
///   |  Page Layout                                               |
///   +------------------------------------------------------------+
///   |  [Header (8B)]                                             |
///   |  [AlpInfo₀ | AlpInfo₁ | ... | AlpInfoₙ]    ← ALP metadata  |
///   |  [ForInfo₀ | ForInfo₁ | ... | ForInfoₙ]    ← FOR metadata  |
///   |  [Data₀ | Data₁ | ... | Dataₙ]             ← Compressed    |
///   +------------------------------------------------------------+
///
/// The grouped metadata layout enables O(1) random access and separates
/// ALP-specific metadata from integer encoding metadata (FOR).
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
  /// ALP-specific metadata (exponent, factor, num_exceptions)
  AlpEncodedVectorInfo alp_info;
  /// FOR-specific metadata (frame_of_reference, bit_width)
  AlpEncodedForVectorInfo<T> for_info;
  /// Number of elements in this vector (not serialized; from page header)
  uint16_t num_elements = 0;
  /// Successfully encoded and bitpacked data
  arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)>
      packed_values;
  /// Float values that could not be converted successfully
  arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;
  /// Positions of the exceptions in the decompressed vector
  arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize> exception_positions;

  /// Total metadata size (AlpInfo + ForInfo)
  static constexpr uint64_t kMetadataStoredSize =
      AlpEncodedVectorInfo::kStoredSize + AlpEncodedForVectorInfo<T>::kStoredSize;

  /// \brief Get the size of the vector if stored into a sequential memory block
  ///
  /// \return the stored size in bytes
  uint64_t GetStoredSize() const;

  /// \brief Get the stored size for given metadata and element count
  ///
  /// \param[in] alp_info the ALP metadata
  /// \param[in] for_info the FOR metadata
  /// \param[in] num_elements the number of elements in this vector
  /// \return the stored size in bytes
  static uint64_t GetStoredSize(const AlpEncodedVectorInfo& alp_info,
                                const AlpEncodedForVectorInfo<T>& for_info,
                                uint16_t num_elements);

  /// \brief Get the number of elements in this vector
  ///
  /// \return number of elements
  uint64_t GetNumElements() const { return num_elements; }

  /// \brief Store the compressed vector in a compact format into an output buffer
  ///
  /// Stores [AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]
  ///
  /// \param[out] output_buffer the buffer to store the compressed data into
  void Store(arrow::util::span<char> output_buffer) const;

  /// \brief Store only the data section (without metadata) into an output buffer
  ///
  /// Stores [PackedValues][ExceptionPositions][ExceptionValues]
  /// Use this for the grouped layout where metadata is stored separately.
  ///
  /// \param[out] output_buffer the buffer to store the data section into
  void StoreDataOnly(arrow::util::span<char> output_buffer) const;

  /// \brief Get the size of the data section only (without metadata)
  ///
  /// \return the size in bytes of packed values + exception positions + exceptions
  uint64_t GetDataStoredSize() const {
    return for_info.GetDataStoredSize(num_elements, alp_info.num_exceptions);
  }

  /// \brief Load a compressed vector from a compact format from an input buffer
  ///
  /// \param[in] input_buffer the buffer to load from
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the loaded AlpEncodedVector
  static AlpEncodedVector Load(arrow::util::span<const char> input_buffer,
                               uint16_t num_elements);

  bool operator==(const AlpEncodedVector<T>& other) const;
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
/// values are copied into aligned StaticVectors because:
///   1. The serialized data may not be properly aligned for uint16_t/T access
///   2. Exceptions are rare (typically < 5%), so copying is negligible
///   3. This avoids undefined behavior from misaligned memory access
///
/// Use LoadView() to create a view, then pass to DecompressVectorView().
/// The underlying buffer must remain valid for the lifetime of the view
/// (for packed_values access).
template <typename T>
struct AlpEncodedVectorView {
  /// ALP-specific metadata (exponent, factor, num_exceptions)
  AlpEncodedVectorInfo alp_info;
  /// FOR-specific metadata (frame_of_reference, bit_width)
  AlpEncodedForVectorInfo<T> for_info;
  /// Number of elements in this vector (not serialized; from page header)
  uint16_t num_elements = 0;
  /// View into bitpacked data (zero-copy, bytes have no alignment requirements)
  arrow::util::span<const uint8_t> packed_values;
  /// Exception positions (copied into aligned storage to avoid UB from misaligned access)
  arrow::internal::StaticVector<uint16_t, AlpConstants::kAlpVectorSize> exception_positions;
  /// Exception values (copied into aligned storage to avoid UB from misaligned access)
  arrow::internal::StaticVector<T, AlpConstants::kAlpVectorSize> exceptions;

  /// \brief Create a zero-copy view from a compact format input buffer
  ///
  /// Expects format: [AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues]
  ///
  /// \param[in] input_buffer the buffer to create a view into
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the view into the compressed data
  static AlpEncodedVectorView LoadView(arrow::util::span<const char> input_buffer,
                                       uint16_t num_elements);

  /// \brief Create a zero-copy view from data-only buffer (metadata provided separately)
  ///
  /// Use this for the grouped layout where AlpInfo and ForInfo are stored separately.
  /// Expects format: [PackedValues][ExceptionPositions][ExceptionValues] (no metadata)
  ///
  /// \param[in] input_buffer the buffer containing only the data section
  /// \param[in] alp_info the ALP metadata (loaded separately)
  /// \param[in] for_info the FOR metadata (loaded separately)
  /// \param[in] num_elements the number of elements (from page header)
  /// \return the view into the compressed data
  static AlpEncodedVectorView LoadViewDataOnly(arrow::util::span<const char> input_buffer,
                                               const AlpEncodedVectorInfo& alp_info,
                                               const AlpEncodedForVectorInfo<T>& for_info,
                                               uint16_t num_elements);

  /// \brief Get the stored size of this vector in the buffer
  ///
  /// \return the stored size in bytes (includes AlpInfo + ForInfo + data)
  uint64_t GetStoredSize() const;

  /// \brief Get the size of the data section only (without metadata)
  ///
  /// \return the size in bytes of packed values + exception positions + exceptions
  uint64_t GetDataStoredSize() const {
    return for_info.GetDataStoredSize(num_elements, alp_info.num_exceptions);
  }
};

// ----------------------------------------------------------------------
// AlpMetadataCache

/// \class AlpMetadataCache
/// \brief Cache for vector metadata to enable O(1) random access to any vector
///
/// With the grouped metadata layout, ALP metadata and FOR metadata are stored
/// in separate contiguous sections after the header. This class loads both
/// metadata types into memory and precomputes cumulative data offsets,
/// enabling O(1) access to any vector's data.
///
/// Page layout:
///   [Header][AlpInfos...][ForInfos...][Data...]
///
/// Usage:
/// \code
///   // Load metadata from compressed buffer
///   AlpMetadataCache<T> cache = AlpMetadataCache<T>::Load(
///       num_vectors, vector_size, total_elements, alp_metadata, for_metadata);
///
///   // Access metadata for any vector in O(1)
///   const auto& alp_info = cache.GetAlpInfo(vector_idx);
///   const auto& for_info = cache.GetForInfo(vector_idx);
///
///   // Get offset to any vector's data in O(1)
///   uint64_t data_offset = cache.GetVectorDataOffset(vector_idx);
///
///   // Get number of elements in a specific vector
///   uint16_t num_elements = cache.GetVectorNumElements(vector_idx);
/// \endcode
///
/// \tparam T the floating point type (float or double)
template <typename T>
class AlpMetadataCache {
 public:
  /// \brief Load all metadata from separate ALP and FOR buffers
  ///
  /// \param[in] num_vectors number of vectors in the block
  /// \param[in] vector_size size of each full vector (typically 1024)
  /// \param[in] total_elements total number of elements across all vectors
  /// \param[in] alp_metadata_buffer buffer containing all AlpEncodedVectorInfo contiguously
  /// \param[in] for_metadata_buffer buffer containing all AlpEncodedForVectorInfo contiguously
  /// \return a metadata cache with all metadata and precomputed offsets
  static AlpMetadataCache Load(uint32_t num_vectors, uint32_t vector_size,
                               uint32_t total_elements,
                               arrow::util::span<const char> alp_metadata_buffer,
                               arrow::util::span<const char> for_metadata_buffer);

  /// \brief Get ALP metadata for vector at given index
  ///
  /// \param[in] vector_idx index of the vector (0 to num_vectors-1)
  /// \return reference to the vector's ALP metadata
  const AlpEncodedVectorInfo& GetAlpInfo(uint32_t vector_idx) const {
    ARROW_CHECK(vector_idx < alp_infos_.size())
        << "vector_index_out_of_range: " << vector_idx;
    return alp_infos_[vector_idx];
  }

  /// \brief Get FOR metadata for vector at given index
  ///
  /// \param[in] vector_idx index of the vector (0 to num_vectors-1)
  /// \return reference to the vector's FOR metadata
  const AlpEncodedForVectorInfo<T>& GetForInfo(uint32_t vector_idx) const {
    ARROW_CHECK(vector_idx < for_infos_.size())
        << "vector_index_out_of_range: " << vector_idx;
    return for_infos_[vector_idx];
  }

  /// \brief Get offset to vector's data from start of data section
  ///
  /// \param[in] vector_idx index of the vector (0 to num_vectors-1)
  /// \return byte offset from start of data section to this vector's data
  uint64_t GetVectorDataOffset(uint32_t vector_idx) const {
    ARROW_CHECK(vector_idx < cumulative_data_offsets_.size())
        << "vector_index_out_of_range: " << vector_idx;
    return cumulative_data_offsets_[vector_idx];
  }

  /// \brief Get number of elements in vector at given index
  ///
  /// \param[in] vector_idx index of the vector (0 to num_vectors-1)
  /// \return number of elements in this vector
  uint16_t GetVectorNumElements(uint32_t vector_idx) const {
    ARROW_CHECK(vector_idx < vector_num_elements_.size())
        << "vector_index_out_of_range: " << vector_idx;
    return vector_num_elements_[vector_idx];
  }

  /// \brief Get number of vectors in the cache
  ///
  /// \return number of vectors
  uint32_t GetNumVectors() const { return static_cast<uint32_t>(alp_infos_.size()); }

  /// \brief Get total size of the data section in bytes
  ///
  /// \return total data size
  uint64_t GetTotalDataSize() const { return total_data_size_; }

  /// \brief Get total size of the ALP metadata section in bytes
  ///
  /// \return total ALP metadata size (num_vectors * AlpEncodedVectorInfo::kStoredSize)
  uint64_t GetAlpMetadataSectionSize() const {
    return alp_infos_.size() * AlpEncodedVectorInfo::kStoredSize;
  }

  /// \brief Get total size of the FOR metadata section in bytes
  ///
  /// \return total FOR metadata size (num_vectors * AlpEncodedForVectorInfo<T>::kStoredSize)
  uint64_t GetForMetadataSectionSize() const {
    return for_infos_.size() * AlpEncodedForVectorInfo<T>::kStoredSize;
  }

  /// \brief Get total size of all metadata sections in bytes
  ///
  /// \return total metadata size (ALP + FOR)
  uint64_t GetTotalMetadataSectionSize() const {
    return GetAlpMetadataSectionSize() + GetForMetadataSectionSize();
  }

 private:
  std::vector<AlpEncodedVectorInfo> alp_infos_;           // ALP metadata per vector
  std::vector<AlpEncodedForVectorInfo<T>> for_infos_;     // FOR metadata per vector
  std::vector<uint64_t> cumulative_data_offsets_;         // Offset from data section start
  std::vector<uint16_t> vector_num_elements_;             // Number of elements in each vector
  uint64_t total_data_size_ = 0;                          // Total size of data section
};

// ----------------------------------------------------------------------
// AlpIntegerEncoding

/// \brief Bit packing layout
///
/// Currently only normal bit packing is implemented.
enum class AlpIntegerEncoding { kForBitPack };

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
  /// \param[in] integer_encoding the integer encoding method used
  /// \param[out] output_vector the vector of floats to decompress into.
  ///             Must be able to contain encoded_vector.GetNumElements().
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
  /// \param[in] input_vector encoded integers (after bit unpacking, still with FOR)
  /// \param[in] alp_info ALP metadata with exponent and factor
  /// \param[in] for_info FOR metadata with frame_of_reference
  /// \param[in] num_elements number of elements to decode
  /// \tparam TargetType the type that is used to store the output.
  ///         May not be a narrowing conversion from T.
  template <typename TargetType>
  static void DecodeVector(TargetType* output_vector,
                           arrow::util::span<ExactType> input_vector,
                           const AlpEncodedVectorInfo& alp_info,
                           const AlpEncodedForVectorInfo<T>& for_info,
                           uint16_t num_elements);

  /// \brief Helper struct to encapsulate the result from BitPackIntegers
  struct BitPackingResult {
    arrow::internal::StaticVector<uint8_t, AlpConstants::kAlpVectorSize * sizeof(T)>
        packed_integers;
    uint8_t bit_width = 0;
    uint16_t bit_packed_size = 0;
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
  static arrow::internal::StaticVector<ExactType, kAlpVectorSize> BitUnpackIntegers(
      arrow::util::span<const uint8_t> packed_integers,
      const AlpEncodedForVectorInfo<T>& for_info, uint16_t num_elements);

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
