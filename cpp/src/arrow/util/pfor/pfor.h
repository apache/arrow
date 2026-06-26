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

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/pfor/pfor_constants.h"
#include "arrow/util/span.h"
#include "arrow/util/ubsan.h"

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
class PforVectorInfo {
 public:
  PforVectorInfo() = default;
  PforVectorInfo(T frame_of_reference, uint8_t bit_width, int16_t num_exceptions)
      : frame_of_reference_(frame_of_reference),
        bit_width_(bit_width),
        num_exceptions_(num_exceptions) {}

  T frame_of_reference() const { return frame_of_reference_; }
  uint8_t bit_width() const { return bit_width_; }
  int16_t num_exceptions() const { return num_exceptions_; }

  void set_frame_of_reference(T frame_of_reference) {
    frame_of_reference_ = frame_of_reference;
  }
  void set_bit_width(uint8_t bit_width) { bit_width_ = bit_width; }
  void set_num_exceptions(int16_t num_exceptions) { num_exceptions_ = num_exceptions; }

  /// \brief Store this info to a byte buffer (little-endian)
  void Store(arrow::util::span<uint8_t> dest) const {
    uint8_t* ptr = dest.data();
    util::SafeStore(ptr, frame_of_reference_);
    ptr[sizeof(T)] = bit_width_;
    util::SafeStore(ptr + sizeof(T) + 1, num_exceptions_);
  }

  /// \brief Load this info from a byte buffer (little-endian)
  static Result<PforVectorInfo> Load(arrow::util::span<const uint8_t> src) {
    if (src.size() < static_cast<size_t>(kStoredSize)) {
      return Status::Invalid("PFOR vector info buffer too small: ", src.size(),
                             " < ", kStoredSize);
    }
    PforVectorInfo info;
    const uint8_t* ptr = src.data();
    info.frame_of_reference_ = util::SafeLoadAs<T>(ptr);
    info.bit_width_ = ptr[sizeof(T)];
    info.num_exceptions_ = util::SafeLoadAs<int16_t>(ptr + sizeof(T) + 1);
    if (info.bit_width_ > PforTypeTraits<T>::kMaxBitWidth) {
      return Status::Invalid("PFOR bit_width out of range: ",
                             static_cast<int>(info.bit_width_));
    }
    if (info.num_exceptions_ < 0) {
      return Status::Invalid("PFOR num_exceptions negative: ",
                             info.num_exceptions_);
    }
    return info;
  }

  /// \brief Serialized size in bytes
  static constexpr int64_t kStoredSize = PforTypeTraits<T>::kVectorInfoSize;

 private:
  T frame_of_reference_ = 0;
  uint8_t bit_width_ = 0;
  int16_t num_exceptions_ = 0;
};

// ----------------------------------------------------------------------
// Encoded vector representation

/// \brief A PFOR-encoded vector with all its data sections
template <typename T>
class PforEncodedVector {
 public:
  PforEncodedVector() = default;

  const PforVectorInfo<T>& info() const { return info_; }
  PforVectorInfo<T>& mutable_info() { return info_; }
  void set_info(const PforVectorInfo<T>& info) { info_ = info; }

  const std::vector<uint8_t>& packed_values() const { return packed_values_; }
  std::vector<uint8_t>& mutable_packed_values() { return packed_values_; }
  void set_packed_values(std::vector<uint8_t> v) { packed_values_ = std::move(v); }

  const std::vector<int16_t>& exception_positions() const { return exception_positions_; }
  std::vector<int16_t>& mutable_exception_positions() { return exception_positions_; }
  void set_exception_positions(std::vector<int16_t> v) {
    exception_positions_ = std::move(v);
  }

  const std::vector<T>& exception_values() const { return exception_values_; }
  std::vector<T>& mutable_exception_values() { return exception_values_; }
  void set_exception_values(std::vector<T> v) { exception_values_ = std::move(v); }

 private:
  PforVectorInfo<T> info_;
  std::vector<uint8_t> packed_values_;
  std::vector<int16_t> exception_positions_;
  std::vector<T> exception_values_;
};

// ----------------------------------------------------------------------
// Zero-copy encoded vector view

/// \brief A zero-copy view over a serialized PFOR vector
///
/// The packed_values span points directly into the compressed buffer.
/// Exception positions and values are copied into aligned storage.
template <typename T>
class PforEncodedVectorView {
 public:
  PforEncodedVectorView() = default;

  const PforVectorInfo<T>& info() const { return info_; }
  PforVectorInfo<T>& mutable_info() { return info_; }
  void set_info(const PforVectorInfo<T>& info) { info_ = info; }

  int32_t num_elements() const { return num_elements_; }
  void set_num_elements(int32_t n) { num_elements_ = n; }

  arrow::util::span<const uint8_t> packed_values() const { return packed_values_; }
  void set_packed_values(arrow::util::span<const uint8_t> v) { packed_values_ = v; }

  const std::vector<int16_t>& exception_positions() const { return exception_positions_; }
  std::vector<int16_t>& mutable_exception_positions() { return exception_positions_; }
  void set_exception_positions(std::vector<int16_t> v) {
    exception_positions_ = std::move(v);
  }

  const std::vector<T>& exception_values() const { return exception_values_; }
  std::vector<T>& mutable_exception_values() { return exception_values_; }
  void set_exception_values(std::vector<T> v) { exception_values_ = std::move(v); }

  /// \brief Create a zero-copy view from a serialized vector buffer
  ///
  /// \param[in] data span over the serialized vector data
  /// \param[in] num_elements number of elements in this vector
  /// \return the view, or an error if the buffer is too small
  static Result<PforEncodedVectorView> LoadView(
      arrow::util::span<const uint8_t> data, int32_t num_elements);

 private:
  PforVectorInfo<T> info_;
  int32_t num_elements_ = 0;
  arrow::util::span<const uint8_t> packed_values_;
  std::vector<int16_t> exception_positions_;
  std::vector<T> exception_values_;
};

// ----------------------------------------------------------------------
// Cost model result

/// \brief Result of the optimal bit width search
struct BitWidthResult {
  uint8_t bit_width = 0;
  int16_t num_exceptions = 0;
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
                                            int32_t num_elements);

  /// \brief Encode a single vector of integers
  ///
  /// \param[in] values input integer values
  /// \param[in] num_elements number of elements (up to vector_size)
  /// \return the encoded vector with all sections
  static PforEncodedVector<T> EncodeVector(const T* values, int32_t num_elements);

  /// \brief Decode a single vector from compressed data
  ///
  /// \param[out] values output buffer for decoded integers
  /// \param[in] data span over the compressed vector data
  /// \param[in] num_elements number of elements in this vector
  /// \return number of bytes consumed from data, or error
  static Result<int64_t> DecodeVector(T* values, arrow::util::span<const uint8_t> data,
                                      int32_t num_elements);

  /// \brief Calculate the serialized size of an encoded vector
  static int64_t SerializedVectorSize(const PforEncodedVector<T>& vec,
                                      int32_t num_elements);

  /// \brief Serialize an encoded vector to a byte buffer
  ///
  /// \param[in] vec the encoded vector
  /// \param[in] num_elements number of elements
  /// \param[out] dest output buffer (must be large enough)
  /// \return number of bytes written
  static int64_t SerializeVector(const PforEncodedVector<T>& vec,
                                 int32_t num_elements,
                                 arrow::util::span<uint8_t> dest);
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
