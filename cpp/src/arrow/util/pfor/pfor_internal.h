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
#include <span>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/endian.h"
#include "arrow/util/pfor/pfor_constants_internal.h"
#include "arrow/util/pfor/pfor_plan_internal.h"
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
///
/// num_exceptions is unsigned: a vector holds up to kMaxVectorSize elements,
/// so the count has to reach 32768, which a signed 16-bit field cannot hold.
/// Its wire type is PforConstants::ExceptionCountType.
///
/// The bit_width byte stores the actual bit width in bits 0..6 and the delta
/// flag in bit 7. A vector with the flag set packs the backward differences of
/// its values rather than the values, and carries one extra full-width field
/// after this info block -- the vector's first value -- so that it still
/// decodes without reading the vector before it.
///
/// The flag is deliberately a bit of its own rather than another code in a
/// mode field: differencing is orthogonal to how the payload is laid out, so
/// the two have to be able to combine.
///
/// Seven bits, not six: the range is 0..64 inclusive, and 64 does not fit in
/// six. A 6-bit mask stored an INT64 vector whose deltas need the full 64 bits
/// as width 0, and since such a vector has no exceptions either, Load reported
/// a constant vector and the decoder filled the output with the frame of
/// reference -- silently, with no error and no size mismatch.
template <typename T>
class PforVectorInfo {
 public:
  static constexpr uint8_t kBitWidthMask = 0x7F;
  static constexpr uint8_t kDeltaFlag = 0x80;

  PforVectorInfo() = default;
  PforVectorInfo(T frame_of_reference, uint8_t bit_width,
                 PforConstants::ExceptionCountType num_exceptions, bool is_delta = false)
      : frame_of_reference_(frame_of_reference),
        bit_width_(bit_width),
        num_exceptions_(num_exceptions),
        is_delta_(is_delta) {}

  T frame_of_reference() const { return frame_of_reference_; }
  uint8_t bit_width() const { return bit_width_; }
  PforConstants::ExceptionCountType num_exceptions() const { return num_exceptions_; }
  bool is_delta() const { return is_delta_; }

  void set_frame_of_reference(T frame_of_reference) {
    frame_of_reference_ = frame_of_reference;
  }
  void set_bit_width(uint8_t bit_width) { bit_width_ = bit_width; }
  void set_num_exceptions(PforConstants::ExceptionCountType num_exceptions) {
    num_exceptions_ = num_exceptions;
  }
  void set_is_delta(bool is_delta) { is_delta_ = is_delta; }

  /// \brief Bytes this info occupies on the wire, start value included.
  ///
  /// Not a constant: the start value is only present on a delta vector. Paying
  /// for it unconditionally would be free at a 1024-value vector and ruinous at
  /// the smallest one the format allows (kMinLogVectorSize is 3, where eight
  /// bytes across eight values is a byte per value).
  int64_t stored_bytes() const {
    return kStoredSize + (is_delta_ ? static_cast<int64_t>(sizeof(T)) : 0);
  }

  /// \brief Store this info to a byte buffer (little-endian)
  void Store(std::span<uint8_t> dest) const {
    uint8_t* ptr = dest.data();
    util::SafeStore(ptr, bit_util::ToLittleEndian(frame_of_reference_));
    // bits 0..6 = bit width; bit 7 = delta flag.
    ptr[sizeof(T)] =
        static_cast<uint8_t>((bit_width_ & kBitWidthMask) | (is_delta_ ? kDeltaFlag : 0));
    util::SafeStore(ptr + sizeof(T) + 1, bit_util::ToLittleEndian(num_exceptions_));
  }

  /// \brief Load this info from a byte buffer (little-endian)
  static Result<PforVectorInfo> Load(std::span<const uint8_t> src) {
    if (src.size() < static_cast<size_t>(kStoredSize)) {
      return Status::Invalid("PFOR vector info buffer too small: ", src.size(), " < ",
                             kStoredSize);
    }
    PforVectorInfo info;
    const uint8_t* ptr = src.data();
    info.frame_of_reference_ = bit_util::FromLittleEndian(util::SafeLoadAs<T>(ptr));
    const uint8_t packed_bit_width = ptr[sizeof(T)];
    info.bit_width_ = static_cast<uint8_t>(packed_bit_width & kBitWidthMask);
    info.is_delta_ = (packed_bit_width & kDeltaFlag) != 0;
    info.num_exceptions_ = bit_util::FromLittleEndian(
        util::SafeLoadAs<PforConstants::ExceptionCountType>(ptr + sizeof(T) + 1));
    if (info.bit_width_ > PforTypeTraits<T>::kMaxBitWidth) {
      return Status::Invalid("PFOR bit_width out of range: ",
                             static_cast<int>(info.bit_width_));
    }
    if (info.num_exceptions_ > PforConstants::kMaxVectorSize) {
      return Status::Invalid("PFOR num_exceptions exceeds the maximum vector size: ",
                             info.num_exceptions_);
    }
    return info;
  }

  /// \brief Serialized size in bytes
  static constexpr int64_t kStoredSize = PforTypeTraits<T>::kVectorInfoSize;

 private:
  T frame_of_reference_ = 0;
  uint8_t bit_width_ = 0;
  PforConstants::ExceptionCountType num_exceptions_ = 0;
  bool is_delta_ = false;

  // is_delta_ rides in a spare bit of the bit-width byte, so it adds nothing to
  // the stored size.
  static_assert(kStoredSize == sizeof(frame_of_reference_) + sizeof(bit_width_) +
                                   sizeof(num_exceptions_),
                "kStoredSize must match the fields Store writes and Load reads");
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

  const std::vector<PforConstants::PositionType>& exception_positions() const {
    return exception_positions_;
  }
  std::vector<PforConstants::PositionType>& mutable_exception_positions() {
    return exception_positions_;
  }
  void set_exception_positions(std::vector<PforConstants::PositionType> v) {
    exception_positions_ = std::move(v);
  }

  const std::vector<T>& exception_values() const { return exception_values_; }
  std::vector<T>& mutable_exception_values() { return exception_values_; }
  void set_exception_values(std::vector<T> v) { exception_values_ = std::move(v); }

  /// \brief First value of the vector; stored only when info().is_delta().
  T start_value() const { return start_value_; }
  void set_start_value(T start_value) { start_value_ = start_value; }

 private:
  PforVectorInfo<T> info_;
  T start_value_ = 0;
  std::vector<uint8_t> packed_values_;
  std::vector<PforConstants::PositionType> exception_positions_;
  std::vector<T> exception_values_;
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
  /// The cost model decides per vector whether to difference the values first
  /// and where to put the frame of reference; see ChooseVectorPlan.
  ///
  /// \param[in] values input integer values
  /// \param[in] num_elements number of elements (up to vector_size)
  /// \pre num_elements > 0; there is no frame of reference for an empty vector
  /// \return the encoded vector with all sections
  static PforEncodedVector<T> EncodeVector(const T* values, int32_t num_elements);

  /// \brief Decode a single vector from compressed data
  ///
  /// \param[in] data span over the compressed vector data
  /// \param[in] num_elements number of elements in this vector
  /// \param[out] values output buffer for num_elements decoded integers
  /// \return number of bytes consumed from data, or error
  static Result<int64_t> DecodeVector(std::span<const uint8_t> data, int32_t num_elements,
                                      T* values);

  /// \brief Calculate the serialized size of an encoded vector
  static int64_t SerializedVectorSize(const PforEncodedVector<T>& vec,
                                      int32_t num_elements);

  /// \brief Serialize an encoded vector to a byte buffer
  ///
  /// \param[in] vec the encoded vector
  /// \param[in] num_elements number of elements
  /// \param[out] dest output buffer, at least SerializedVectorSize() bytes
  /// \return number of bytes written, or an error if `dest` is too small
  static Result<int64_t> SerializeVector(const PforEncodedVector<T>& vec,
                                         int32_t num_elements, std::span<uint8_t> dest);
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
