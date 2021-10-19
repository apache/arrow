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

#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

enum class DecimalStatus {
  kSuccess,
  kDivideByZero,
  kOverflow,
  kRescaleDataLoss,
};

/// Represents a signed 128-bit integer in two's complement.
///
/// This class is also compiled into LLVM IR - so, it should not have cpp references like
/// streams and boost.
class ARROW_EXPORT BasicDecimal128 {
  struct LittleEndianArrayTag {};

 public:
  static constexpr int kBitWidth = 128;
  static constexpr int kMaxPrecision = 38;
  static constexpr int kMaxScale = 38;

  // A constructor tag to introduce a little-endian encoded array
  static constexpr LittleEndianArrayTag LittleEndianArray{};

  /// \brief Create a BasicDecimal128 from the two's complement representation.
#if ARROW_LITTLE_ENDIAN
  constexpr BasicDecimal128(int64_t high, uint64_t low) noexcept
      : low_bits_(low), high_bits_(high) {}
#else
  constexpr BasicDecimal128(int64_t high, uint64_t low) noexcept
      : high_bits_(high), low_bits_(low) {}
#endif

  /// \brief Create a BasicDecimal256 from the two's complement representation.
  ///
  /// Input array is assumed to be in native endianness.
#if ARROW_LITTLE_ENDIAN
  constexpr BasicDecimal128(const std::array<uint64_t, 2>& array) noexcept
      : low_bits_(array[0]), high_bits_(static_cast<int64_t>(array[1])) {}
#else
  constexpr BasicDecimal128(const std::array<uint64_t, 2>& array) noexcept
      : high_bits_(static_cast<int64_t>(array[0])), low_bits_(array[1]) {}
#endif

  /// \brief Create a BasicDecimal128 from the two's complement representation.
  ///
  /// Input array is assumed to be in little endianness, with native endian elements.
  BasicDecimal128(LittleEndianArrayTag, const std::array<uint64_t, 2>& array) noexcept
      : BasicDecimal128(BitUtil::LittleEndianArray::ToNative(array)) {}

  /// \brief Empty constructor creates a BasicDecimal128 with a value of 0.
  constexpr BasicDecimal128() noexcept : BasicDecimal128(0, 0) {}

  /// \brief Convert any integer value into a BasicDecimal128.
  template <typename T,
            typename = typename std::enable_if<
                std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
  constexpr BasicDecimal128(T value) noexcept
      : BasicDecimal128(value >= T{0} ? 0 : -1, static_cast<uint64_t>(value)) {  // NOLINT
  }

  /// \brief Create a BasicDecimal128 from an array of bytes. Bytes are assumed to be in
  /// native-endian byte order.
  explicit BasicDecimal128(const uint8_t* bytes);

  /// \brief Negate the current value (in-place)
  BasicDecimal128& Negate();

  /// \brief Absolute value (in-place)
  BasicDecimal128& Abs();

  /// \brief Absolute value
  static BasicDecimal128 Abs(const BasicDecimal128& left);

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  BasicDecimal128& operator+=(const BasicDecimal128& right);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  BasicDecimal128& operator-=(const BasicDecimal128& right);

  /// \brief Multiply this number by another number. The result is truncated to 128 bits.
  BasicDecimal128& operator*=(const BasicDecimal128& right);

  /// Divide this number by right and return the result.
  ///
  /// This operation is not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param[in] divisor the number to divide by
  /// \param[out] result the quotient
  /// \param[out] remainder the remainder after the division
  DecimalStatus Divide(const BasicDecimal128& divisor, BasicDecimal128* result,
                       BasicDecimal128* remainder) const;

  /// \brief In-place division.
  BasicDecimal128& operator/=(const BasicDecimal128& right);

  /// \brief Bitwise "or" between two BasicDecimal128.
  BasicDecimal128& operator|=(const BasicDecimal128& right);

  /// \brief Bitwise "and" between two BasicDecimal128.
  BasicDecimal128& operator&=(const BasicDecimal128& right);

  /// \brief Shift left by the given number of bits.
  BasicDecimal128& operator<<=(uint32_t bits);

  /// \brief Shift right by the given number of bits. Negative values will
  BasicDecimal128& operator>>=(uint32_t bits);

  /// \brief Get the high bits of the two's complement representation of the number.
  inline constexpr int64_t high_bits() const { return high_bits_; }

  /// \brief Get the low bits of the two's complement representation of the number.
  inline constexpr uint64_t low_bits() const { return low_bits_; }

  /// \brief Get the bits of the two's complement representation of the number.
  ///
  /// The 2 elements are in native endian order. The bits within each uint64_t element
  /// are in native endian order. For example, on a little endian machine,
  /// BasicDecimal128(123).native_endian_array() = {123, 0};
  /// but on a big endian machine,
  /// BasicDecimal128(123).native_endian_array() = {0, 123};
  inline std::array<uint64_t, 2> native_endian_array() const {
#if ARROW_LITTLE_ENDIAN
    return {low_bits_, static_cast<uint64_t>(high_bits_)};
#else
    return {static_cast<uint64_t>(high_bits_), low_bits_};
#endif
  }

  /// \brief Get the bits of the two's complement representation of the number.
  ///
  /// The 2 elements are in little endian order. However, the bits within each
  /// uint64_t element are in native endian order.
  /// For example, BasicDecimal128(123).little_endian_array() = {123, 0};
  inline std::array<uint64_t, 2> little_endian_array() const {
    return {low_bits_, static_cast<uint64_t>(high_bits_)};
  }

  inline const uint8_t* native_endian_bytes() const {
#if ARROW_LITTLE_ENDIAN
    return reinterpret_cast<const uint8_t*>(&low_bits_);
#else
    return reinterpret_cast<const uint8_t*>(&high_bits_);
#endif
  }

  inline uint8_t* mutable_native_endian_bytes() {
#if ARROW_LITTLE_ENDIAN
    return reinterpret_cast<uint8_t*>(&low_bits_);
#else
    return reinterpret_cast<uint8_t*>(&high_bits_);
#endif
  }

  /// \brief Return the raw bytes of the value in native-endian byte order.
  std::array<uint8_t, 16> ToBytes() const;
  void ToBytes(uint8_t* out) const;

  /// \brief separate the integer and fractional parts for the given scale.
  void GetWholeAndFraction(int32_t scale, BasicDecimal128* whole,
                           BasicDecimal128* fraction) const;

  /// \brief Scale multiplier for given scale value.
  static const BasicDecimal128& GetScaleMultiplier(int32_t scale);
  /// \brief Half-scale multiplier for given scale value.
  static const BasicDecimal128& GetHalfScaleMultiplier(int32_t scale);

  /// \brief Convert BasicDecimal128 from one scale to another
  DecimalStatus Rescale(int32_t original_scale, int32_t new_scale,
                        BasicDecimal128* out) const;

  /// \brief Scale up.
  BasicDecimal128 IncreaseScaleBy(int32_t increase_by) const;

  /// \brief Scale down.
  /// - If 'round' is true, the right-most digits are dropped and the result value is
  ///   rounded up (+1 for +ve, -1 for -ve) based on the value of the dropped digits
  ///   (>= 10^reduce_by / 2).
  /// - If 'round' is false, the right-most digits are simply dropped.
  BasicDecimal128 ReduceScaleBy(int32_t reduce_by, bool round = true) const;

  /// \brief Whether this number fits in the given precision
  ///
  /// Return true if the number of significant digits is less or equal to `precision`.
  bool FitsInPrecision(int32_t precision) const;

  // returns 1 for positive and zero decimal values, -1 for negative decimal values.
  inline int64_t Sign() const { return 1 | (high_bits_ >> 63); }

  /// \brief count the number of leading binary zeroes.
  int32_t CountLeadingBinaryZeros() const;

  /// \brief Get the maximum valid unscaled decimal value.
  static const BasicDecimal128& GetMaxValue();

  /// \brief Get the maximum decimal value (is not a valid value).
  static inline constexpr BasicDecimal128 GetMaxSentinel() {
    return BasicDecimal128(/*high=*/std::numeric_limits<int64_t>::max(),
                           /*low=*/std::numeric_limits<uint64_t>::max());
  }
  /// \brief Get the minimum decimal value (is not a valid value).
  static inline constexpr BasicDecimal128 GetMinSentinel() {
    return BasicDecimal128(/*high=*/std::numeric_limits<int64_t>::min(),
                           /*low=*/std::numeric_limits<uint64_t>::min());
  }

 private:
#if ARROW_LITTLE_ENDIAN
  uint64_t low_bits_;
  int64_t high_bits_;
#else
  int64_t high_bits_;
  uint64_t low_bits_;
#endif
};

ARROW_EXPORT bool operator==(const BasicDecimal128& left, const BasicDecimal128& right);
ARROW_EXPORT bool operator!=(const BasicDecimal128& left, const BasicDecimal128& right);
ARROW_EXPORT bool operator<(const BasicDecimal128& left, const BasicDecimal128& right);
ARROW_EXPORT bool operator<=(const BasicDecimal128& left, const BasicDecimal128& right);
ARROW_EXPORT bool operator>(const BasicDecimal128& left, const BasicDecimal128& right);
ARROW_EXPORT bool operator>=(const BasicDecimal128& left, const BasicDecimal128& right);

ARROW_EXPORT BasicDecimal128 operator-(const BasicDecimal128& operand);
ARROW_EXPORT BasicDecimal128 operator~(const BasicDecimal128& operand);
ARROW_EXPORT BasicDecimal128 operator+(const BasicDecimal128& left,
                                       const BasicDecimal128& right);
ARROW_EXPORT BasicDecimal128 operator-(const BasicDecimal128& left,
                                       const BasicDecimal128& right);
ARROW_EXPORT BasicDecimal128 operator*(const BasicDecimal128& left,
                                       const BasicDecimal128& right);
ARROW_EXPORT BasicDecimal128 operator/(const BasicDecimal128& left,
                                       const BasicDecimal128& right);
ARROW_EXPORT BasicDecimal128 operator%(const BasicDecimal128& left,
                                       const BasicDecimal128& right);

class ARROW_EXPORT BasicDecimal256 {
 private:
  // Due to a bug in clang, we have to declare the extend method prior to its
  // usage.
  template <typename T>
  inline static constexpr uint64_t extend(T low_bits) noexcept {
    return low_bits >= T() ? uint64_t{0} : ~uint64_t{0};
  }

  struct LittleEndianArrayTag {};

 public:
  static constexpr int kBitWidth = 256;
  static constexpr int kMaxPrecision = 76;
  static constexpr int kMaxScale = 76;

  // A constructor tag to denote a little-endian encoded array
  static constexpr LittleEndianArrayTag LittleEndianArray{};

  /// \brief Create a BasicDecimal256 from the two's complement representation.
  ///
  /// Input array is assumed to be in native endianness.
  constexpr BasicDecimal256(const std::array<uint64_t, 4>& array) noexcept
      : array_(array) {}

  /// \brief Create a BasicDecimal256 from the two's complement representation.
  ///
  /// Input array is assumed to be in little endianness, with native endian elements.
  BasicDecimal256(LittleEndianArrayTag, const std::array<uint64_t, 4>& array) noexcept
      : BasicDecimal256(BitUtil::LittleEndianArray::ToNative(array)) {}

  /// \brief Empty constructor creates a BasicDecimal256 with a value of 0.
  constexpr BasicDecimal256() noexcept : array_({0, 0, 0, 0}) {}

  /// \brief Convert any integer value into a BasicDecimal256.
  template <typename T,
            typename = typename std::enable_if<
                std::is_integral<T>::value && (sizeof(T) <= sizeof(uint64_t)), T>::type>
  constexpr BasicDecimal256(T value) noexcept
      : array_(BitUtil::LittleEndianArray::ToNative<uint64_t, 4>(
            {static_cast<uint64_t>(value), extend(value), extend(value),
             extend(value)})) {}

  explicit BasicDecimal256(const BasicDecimal128& value) noexcept
      : array_(BitUtil::LittleEndianArray::ToNative<uint64_t, 4>(
            {value.low_bits(), static_cast<uint64_t>(value.high_bits()),
             extend(value.high_bits()), extend(value.high_bits())})) {}

  /// \brief Create a BasicDecimal256 from an array of bytes. Bytes are assumed to be in
  /// native-endian byte order.
  explicit BasicDecimal256(const uint8_t* bytes);

  /// \brief Negate the current value (in-place)
  BasicDecimal256& Negate();

  /// \brief Absolute value (in-place)
  BasicDecimal256& Abs();

  /// \brief Absolute value
  static BasicDecimal256 Abs(const BasicDecimal256& left);

  /// \brief Add a number to this one. The result is truncated to 256 bits.
  BasicDecimal256& operator+=(const BasicDecimal256& right);

  /// \brief Subtract a number from this one. The result is truncated to 256 bits.
  BasicDecimal256& operator-=(const BasicDecimal256& right);

  /// \brief Get the bits of the two's complement representation of the number.
  ///
  /// The 4 elements are in native endian order. The bits within each uint64_t element
  /// are in native endian order. For example, on a little endian machine,
  ///   BasicDecimal256(123).native_endian_array() = {123, 0, 0, 0};
  ///   BasicDecimal256(-2).native_endian_array() = {0xFF...FE, 0xFF...FF, 0xFF...FF,
  /// 0xFF...FF}.
  /// while on a big endian machine,
  ///   BasicDecimal256(123).native_endian_array() = {0, 0, 0, 123};
  ///   BasicDecimal256(-2).native_endian_array() = {0xFF...FF, 0xFF...FF, 0xFF...FF,
  /// 0xFF...FE}.
  inline const std::array<uint64_t, 4>& native_endian_array() const { return array_; }

  /// \brief Get the bits of the two's complement representation of the number.
  ///
  /// The 4 elements are in little endian order. However, the bits within each
  /// uint64_t element are in native endian order.
  /// For example, BasicDecimal256(123).little_endian_array() = {123, 0};
  inline const std::array<uint64_t, 4> little_endian_array() const {
    return BitUtil::LittleEndianArray::FromNative(array_);
  }

  inline const uint8_t* native_endian_bytes() const {
    return reinterpret_cast<const uint8_t*>(array_.data());
  }

  inline uint8_t* mutable_native_endian_bytes() {
    return reinterpret_cast<uint8_t*>(array_.data());
  }

  /// \brief Get the lowest bits of the two's complement representation of the number.
  inline uint64_t low_bits() const { return BitUtil::LittleEndianArray::Make(array_)[0]; }

  /// \brief Return the raw bytes of the value in native-endian byte order.
  std::array<uint8_t, 32> ToBytes() const;
  void ToBytes(uint8_t* out) const;

  /// \brief Scale multiplier for given scale value.
  static const BasicDecimal256& GetScaleMultiplier(int32_t scale);
  /// \brief Half-scale multiplier for given scale value.
  static const BasicDecimal256& GetHalfScaleMultiplier(int32_t scale);

  /// \brief Convert BasicDecimal256 from one scale to another
  DecimalStatus Rescale(int32_t original_scale, int32_t new_scale,
                        BasicDecimal256* out) const;

  /// \brief Scale up.
  BasicDecimal256 IncreaseScaleBy(int32_t increase_by) const;

  /// \brief Scale down.
  /// - If 'round' is true, the right-most digits are dropped and the result value is
  ///   rounded up (+1 for positive, -1 for negative) based on the value of the
  ///   dropped digits (>= 10^reduce_by / 2).
  /// - If 'round' is false, the right-most digits are simply dropped.
  BasicDecimal256 ReduceScaleBy(int32_t reduce_by, bool round = true) const;

  /// \brief Whether this number fits in the given precision
  ///
  /// Return true if the number of significant digits is less or equal to `precision`.
  bool FitsInPrecision(int32_t precision) const;

  inline int64_t Sign() const {
    return 1 | (static_cast<int64_t>(BitUtil::LittleEndianArray::Make(array_)[3]) >> 63);
  }

  inline int64_t IsNegative() const {
    return static_cast<int64_t>(BitUtil::LittleEndianArray::Make(array_)[3]) < 0;
  }

  /// \brief Multiply this number by another number. The result is truncated to 256 bits.
  BasicDecimal256& operator*=(const BasicDecimal256& right);

  /// Divide this number by right and return the result.
  ///
  /// This operation is not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param[in] divisor the number to divide by
  /// \param[out] result the quotient
  /// \param[out] remainder the remainder after the division
  DecimalStatus Divide(const BasicDecimal256& divisor, BasicDecimal256* result,
                       BasicDecimal256* remainder) const;

  /// \brief Shift left by the given number of bits.
  BasicDecimal256& operator<<=(uint32_t bits);

  /// \brief In-place division.
  BasicDecimal256& operator/=(const BasicDecimal256& right);

  /// \brief Get the maximum decimal value (is not a valid value).
  static inline constexpr BasicDecimal256 GetMaxSentinel() {
#if ARROW_LITTLE_ENDIAN
    return BasicDecimal256({std::numeric_limits<uint64_t>::max(),
                            std::numeric_limits<uint64_t>::max(),
                            std::numeric_limits<uint64_t>::max(),
                            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())});
#else
    return BasicDecimal256({static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
                            std::numeric_limits<uint64_t>::max(),
                            std::numeric_limits<uint64_t>::max(),
                            std::numeric_limits<uint64_t>::max()});
#endif
  }
  /// \brief Get the minimum decimal value (is not a valid value).
  static inline constexpr BasicDecimal256 GetMinSentinel() {
#if ARROW_LITTLE_ENDIAN
    return BasicDecimal256(
        {0, 0, 0, static_cast<uint64_t>(std::numeric_limits<int64_t>::min())});
#else
    return BasicDecimal256(
        {static_cast<uint64_t>(std::numeric_limits<int64_t>::min()), 0, 0, 0});
#endif
  }

 private:
  std::array<uint64_t, 4> array_;
};

ARROW_EXPORT inline bool operator==(const BasicDecimal256& left,
                                    const BasicDecimal256& right) {
  return left.native_endian_array() == right.native_endian_array();
}

ARROW_EXPORT inline bool operator!=(const BasicDecimal256& left,
                                    const BasicDecimal256& right) {
  return left.native_endian_array() != right.native_endian_array();
}

ARROW_EXPORT bool operator<(const BasicDecimal256& left, const BasicDecimal256& right);

ARROW_EXPORT inline bool operator<=(const BasicDecimal256& left,
                                    const BasicDecimal256& right) {
  return !operator<(right, left);
}

ARROW_EXPORT inline bool operator>(const BasicDecimal256& left,
                                   const BasicDecimal256& right) {
  return operator<(right, left);
}

ARROW_EXPORT inline bool operator>=(const BasicDecimal256& left,
                                    const BasicDecimal256& right) {
  return !operator<(left, right);
}

ARROW_EXPORT BasicDecimal256 operator-(const BasicDecimal256& operand);
ARROW_EXPORT BasicDecimal256 operator~(const BasicDecimal256& operand);
ARROW_EXPORT BasicDecimal256 operator+(const BasicDecimal256& left,
                                       const BasicDecimal256& right);
ARROW_EXPORT BasicDecimal256 operator*(const BasicDecimal256& left,
                                       const BasicDecimal256& right);
ARROW_EXPORT BasicDecimal256 operator/(const BasicDecimal256& left,
                                       const BasicDecimal256& right);
}  // namespace arrow
