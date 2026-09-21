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

// ALP metadata types and serialization.

#pragma once

#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/alp/alp_constants_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/visibility.h"

namespace arrow::util::alp {

template <typename T>
using AlpAllocator = ::arrow::stl::allocator<T>;

template <typename T>
using AlpVector = std::vector<T, AlpAllocator<T>>;

/// Exponent/factor pair used for ALP decimal encoding.
struct AlpExponentAndFactor {
  uint8_t exponent{0};
  uint8_t factor{0};

  bool operator==(const AlpExponentAndFactor& other) const {
    return exponent == other.exponent && factor == other.factor;
  }

  bool operator<(const AlpExponentAndFactor& other) const {
    if (exponent != other.exponent) {
      return exponent < other.exponent;
    }
    return factor < other.factor;
  }
};

/// ALP metadata for one encoded vector: exponent, factor, and num_exceptions.
class ARROW_EXPORT AlpInfo {
 public:
  AlpInfo() = default;
  AlpInfo(uint8_t exponent, uint8_t factor, uint16_t num_exceptions)
      : exponent_(exponent), factor_(factor), num_exceptions_(num_exceptions) {}

  uint8_t exponent() const { return exponent_; }
  uint8_t factor() const { return factor_; }
  uint16_t num_exceptions() const { return num_exceptions_; }
  AlpExponentAndFactor GetExponentAndFactor() const {
    return AlpExponentAndFactor{exponent_, factor_};
  }

  void SetExponent(uint8_t exponent) { exponent_ = exponent; }
  void SetFactor(uint8_t factor) { factor_ = factor; }
  void SetNumExceptions(uint16_t num_exceptions) { num_exceptions_ = num_exceptions; }

  static constexpr int64_t kStoredSize = 4;

  /// Writes the four metadata bytes.
  void Store(std::span<uint8_t> output_buffer) const;

  /// Reads the four metadata bytes, or returns Invalid if the buffer is too small.
  static Result<AlpInfo> Load(std::span<const uint8_t> input_buffer);

  bool operator==(const AlpInfo& other) const {
    return exponent_ == other.exponent_ && factor_ == other.factor_ &&
           num_exceptions_ == other.num_exceptions_;
  }

 private:
  uint8_t exponent_{0};
  uint8_t factor_{0};
  uint16_t num_exceptions_{0};
};

/// Frame-of-reference metadata for one encoded vector.
///
/// frame_of_reference is stored unsigned for wrapping arithmetic; the wire
/// interpretation is int32_t for float and int64_t for double.
template <typename T>
class ARROW_EXPORT AlpForInfo {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>,
                "AlpForInfo only supports float and double");

 public:
  using FrameType = typename AlpTypedConstants<T>::EncodedUnsigned;

  AlpForInfo() = default;
  AlpForInfo(FrameType frame_of_reference, uint8_t bit_width)
      : frame_of_reference_(frame_of_reference), bit_width_(bit_width) {}

  FrameType frame_of_reference() const { return frame_of_reference_; }
  uint8_t bit_width() const { return bit_width_; }

  void SetFrameOfReference(FrameType frame_of_reference) {
    frame_of_reference_ = frame_of_reference;
  }
  void SetBitWidth(uint8_t bit_width) { bit_width_ = bit_width; }

  static constexpr int64_t kStoredSize = sizeof(FrameType) + sizeof(uint8_t);

  /// Returns the size of packed values, exception positions, and exception values.
  int64_t GetDataStoredSize(int32_t num_elements, int32_t num_exceptions) const {
    ARROW_DCHECK_GE(num_elements, 0) << "ALP element count must be non-negative";
    ARROW_DCHECK_GE(num_exceptions, 0) << "ALP exception count must be non-negative";
    const int64_t bit_packed_size =
        bit_util::BytesForBits(int64_t{num_elements} * bit_width_);
    return bit_packed_size +
           num_exceptions *
               static_cast<int64_t>(sizeof(AlpFormatConstants::PositionType) + sizeof(T));
  }

  /// Writes frame_of_reference and bit_width.
  void Store(std::span<uint8_t> output_buffer) const;

  /// Reads frame_of_reference and bit_width, or returns Invalid if malformed.
  static Result<AlpForInfo> Load(std::span<const uint8_t> input_buffer);

  bool operator==(const AlpForInfo& other) const {
    return frame_of_reference_ == other.frame_of_reference_ &&
           bit_width_ == other.bit_width_;
  }

 private:
  FrameType frame_of_reference_{0};
  uint8_t bit_width_{0};
};

/// An owned, serialized ALP vector.
///
/// Layout: [AlpInfo][ForInfo][PackedValues][ExceptionPositions][ExceptionValues].
template <typename T>
class ARROW_EXPORT AlpEncodedVector {
 public:
  const AlpInfo& alp_info() const { return alp_info_; }
  const AlpForInfo<T>& for_info() const { return for_info_; }
  int32_t num_elements() const { return num_elements_; }
  const std::vector<uint8_t>& packed_values() const { return packed_values_; }
  const std::vector<AlpFormatConstants::PositionType>& exception_positions() const {
    return exception_positions_;
  }
  const std::vector<T>& exceptions() const { return exceptions_; }

  /// Serialized size of AlpInfo + ForInfo.
  static constexpr int64_t kMetadataStoredSize =
      AlpInfo::kStoredSize + AlpForInfo<T>::kStoredSize;

  /// Returns the size of metadata, packed values, and exception data.
  int64_t GetStoredSize() const;

  /// Returns the size of packed values, exception positions, and exception values.
  int64_t GetDataStoredSize() const {
    return for_info_.GetDataStoredSize(num_elements_, alp_info_.num_exceptions());
  }

  /// Writes all vector sections.
  void Store(std::span<uint8_t> output_buffer) const;

  /// Constructs a complete vector after validating its invariants.
  static AlpEncodedVector Make(
      AlpInfo alp_info, AlpForInfo<T> for_info, int32_t num_elements,
      std::vector<uint8_t> packed_values,
      std::vector<AlpFormatConstants::PositionType> exception_positions,
      std::vector<T> exceptions);

 private:
  AlpEncodedVector(AlpInfo alp_info, AlpForInfo<T> for_info, int32_t num_elements,
                   std::vector<uint8_t> packed_values,
                   std::vector<AlpFormatConstants::PositionType> exception_positions,
                   std::vector<T> exceptions);

  AlpInfo alp_info_;
  AlpForInfo<T> for_info_;
  int32_t num_elements_{0};
  std::vector<uint8_t> packed_values_;
  std::vector<AlpFormatConstants::PositionType> exception_positions_;
  std::vector<T> exceptions_;
};

/// Non-owning view of one serialized vector. Packed values reference
/// input_buffer; exception arrays are copied and reused by Reset().
template <typename T>
class ARROW_EXPORT AlpEncodedVectorView {
 public:
  explicit AlpEncodedVectorView(MemoryPool* pool)
      : exception_positions_(AlpAllocator<AlpFormatConstants::PositionType>(pool)),
        exceptions_(AlpAllocator<T>(pool)) {}

  const AlpInfo& alp_info() const { return alp_info_; }
  const AlpForInfo<T>& for_info() const { return for_info_; }
  int32_t num_elements() const { return num_elements_; }
  std::span<const uint8_t> packed_values() const { return packed_values_; }

  const AlpVector<AlpFormatConstants::PositionType>& exception_positions() const {
    return exception_positions_;
  }

  const AlpVector<T>& exceptions() const { return exceptions_; }

  /// Returns the size of packed values, exception positions, and exception values.
  int64_t GetDataStoredSize() const {
    return for_info_.GetDataStoredSize(num_elements_, alp_info_.num_exceptions());
  }

  /// Creates a view from [AlpInfo][ForInfo][PackedValues][ExceptionPositions]
  /// [ExceptionValues], or returns Invalid if data is malformed.
  static Result<AlpEncodedVectorView> Load(std::span<const uint8_t> input_buffer,
                                           int32_t num_elements, MemoryPool* pool);

  /// Resets this view from a new buffer. The view cannot be used after an error.
  Status Reset(std::span<const uint8_t> input_buffer, const AlpInfo& alp_info,
               const AlpForInfo<T>& for_info, int32_t num_elements);

 private:
  void Clear();

  AlpInfo alp_info_;
  AlpForInfo<T> for_info_;
  int32_t num_elements_{0};
  std::span<const uint8_t> packed_values_;
  AlpVector<AlpFormatConstants::PositionType> exception_positions_;
  AlpVector<T> exceptions_;
};

/// Returns the metadata size for one per-vector integer encoding.
template <typename T>
inline int64_t GetIntegerEncodingMetadataSize(AlpIntegerEncoding encoding) {
  ARROW_DCHECK_EQ(encoding, AlpIntegerEncoding::kForBitPack)
      << "Unsupported ALP integer encoding";
  return AlpForInfo<T>::kStoredSize;
}

}  // namespace arrow::util::alp
