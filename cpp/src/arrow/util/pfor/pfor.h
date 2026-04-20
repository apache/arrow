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

// Core PFOR (Patched Frame of Reference) compression algorithm
//
// PFOR compresses integer columns by:
//   1. Subtracting the minimum value (Frame of Reference)
//   2. Choosing an optimal bit width via a cost model
//   3. Bit-packing the deltas at the chosen width
//   4. Storing outlier values (exceptions) separately

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "arrow/util/pfor/pfor_constants.h"

namespace arrow {
namespace util {
namespace pfor {

// ----------------------------------------------------------------------
// Per-vector metadata

/// \brief PFOR vector metadata stored at the start of each compressed vector.
///
/// For INT32 (7 bytes): [frame_of_reference(4B)] [bit_width(1B)] [num_exceptions(2B)]
/// For INT64 (11 bytes): [frame_of_reference(8B)] [bit_width(1B)] [num_exceptions(2B)]
template <typename T>
struct PforVectorInfo {
  T frame_of_reference = 0;
  uint8_t bit_width = 0;
  uint16_t num_exceptions = 0;

  /// \brief Store this info to a byte buffer (little-endian)
  void Store(uint8_t* dest) const {
    std::memcpy(dest, &frame_of_reference, sizeof(T));
    dest[sizeof(T)] = bit_width;
    uint16_t le_exceptions = num_exceptions;  // Assume LE platform
    std::memcpy(dest + sizeof(T) + 1, &le_exceptions, sizeof(uint16_t));
  }

  /// \brief Load this info from a byte buffer (little-endian)
  static PforVectorInfo Load(const uint8_t* src) {
    PforVectorInfo info;
    std::memcpy(&info.frame_of_reference, src, sizeof(T));
    info.bit_width = src[sizeof(T)];
    std::memcpy(&info.num_exceptions, src + sizeof(T) + 1, sizeof(uint16_t));
    return info;
  }

  /// \brief Serialized size in bytes
  static constexpr uint8_t kSerializedSize = PforTypeTraits<T>::kVectorInfoSize;
};

// ----------------------------------------------------------------------
// Encoded vector representation

/// \brief A PFOR-encoded vector with all its data sections
template <typename T>
struct PforEncodedVector {
  PforVectorInfo<T> info;
  std::vector<uint8_t> packed_values;
  std::vector<uint16_t> exception_positions;
  std::vector<T> exception_values;
};

// ----------------------------------------------------------------------
// Cost model result

/// \brief Result of the optimal bit width search
struct BitWidthResult {
  uint8_t bit_width = 0;
  uint16_t num_exceptions = 0;
};

// ----------------------------------------------------------------------
// Core compression/decompression

/// \brief PFOR compression and decompression algorithms
///
/// \tparam T the integer type (int32_t or int64_t)
template <typename T>
class PforCompression {
 public:
  using UnsignedT = typename PforTypeTraits<T>::UnsignedType;

  /// \brief Find the optimal bit width using the cost model
  ///
  /// Evaluates every candidate bit width and selects the one that
  /// minimizes total encoded size (packing cost + exception cost).
  ///
  /// \param[in] deltas unsigned deltas after FOR subtraction
  /// \param[in] num_elements number of elements
  /// \return the optimal bit width and exception count
  static BitWidthResult FindOptimalBitWidth(const UnsignedT* deltas,
                                            uint32_t num_elements);

  /// \brief Encode a single vector of integers
  ///
  /// \param[in] values input integer values
  /// \param[in] num_elements number of elements (up to vector_size)
  /// \return the encoded vector with all sections
  static PforEncodedVector<T> EncodeVector(const T* values, uint32_t num_elements);

  /// \brief Decode a single vector from compressed data
  ///
  /// \param[out] values output buffer for decoded integers
  /// \param[in] data pointer to the start of the vector data
  /// \param[in] num_elements number of elements in this vector
  /// \return number of bytes consumed from data
  static size_t DecodeVector(T* values, const uint8_t* data, uint32_t num_elements);

  /// \brief Calculate the serialized size of an encoded vector
  static size_t SerializedVectorSize(const PforEncodedVector<T>& vec,
                                     uint32_t num_elements);

  /// \brief Serialize an encoded vector to a byte buffer
  ///
  /// \param[in] vec the encoded vector
  /// \param[in] num_elements number of elements
  /// \param[out] dest output buffer (must be large enough)
  /// \return number of bytes written
  static size_t SerializeVector(const PforEncodedVector<T>& vec,
                                uint32_t num_elements, uint8_t* dest);
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
