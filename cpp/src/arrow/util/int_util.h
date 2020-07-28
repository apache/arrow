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

#include <cstdint>
#include <limits>
#include <type_traits>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class DataType;
struct ArrayData;
struct Datum;
struct Scalar;

namespace internal {

ARROW_EXPORT
uint8_t DetectUIntWidth(const uint64_t* values, int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectUIntWidth(const uint64_t* values, const uint8_t* valid_bytes,
                        int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectIntWidth(const int64_t* values, int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectIntWidth(const int64_t* values, const uint8_t* valid_bytes, int64_t length,
                       uint8_t min_width = 1);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int8_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int16_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int32_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int64_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint8_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint16_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint32_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint64_t* dest, int64_t length);

template <typename InputInt, typename OutputInt>
ARROW_EXPORT void TransposeInts(const InputInt* source, OutputInt* dest, int64_t length,
                                const int32_t* transpose_map);

/// Signed addition with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedAdd(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) +
                                static_cast<UnsignedInt>(v));
}

/// Signed subtraction with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedSubtract(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) -
                                static_cast<UnsignedInt>(v));
}

/// Signed left shift with well-defined behaviour on negative numbers or overflow
template <typename SignedInt, typename Shift>
SignedInt SafeLeftShift(SignedInt u, Shift shift) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
}

// TODO Add portable wrappers for __builtin_add_overflow and friends
// see http://www.open-std.org/jtc1/sc22/wg14/www/docs/n2428.pdf

/// Detect multiplication overflow between *positive* integers
template <typename Integer>
bool HasPositiveMultiplyOverflow(Integer value, Integer multiplicand) {
  return (multiplicand != 0 &&
          value > std::numeric_limits<Integer>::max() / multiplicand);
}

/// Detect addition overflow between *positive* integers
template <typename Integer>
bool HasPositiveAdditionOverflow(Integer value, Integer addend) {
  return (value > std::numeric_limits<Integer>::max() - addend);
}

/// Detect addition overflow between signed integers
template <typename Integer>
bool HasSignedAdditionOverflow(Integer value, Integer addend) {
  return (addend > 0) ? (value > std::numeric_limits<Integer>::max() - addend)
                      : (value < std::numeric_limits<Integer>::min() - addend);
}

/// Detect subtraction overflow between *positive* integers
template <typename Integer>
bool HasPositiveSubtractionOverflow(Integer value, Integer minuend) {
  return (value < minuend);
}

/// Detect subtraction overflow between signed integers
template <typename Integer>
bool HasSignedSubtractionOverflow(Integer value, Integer subtrahend) {
  return (subtrahend > 0) ? (value < std::numeric_limits<Integer>::min() + subtrahend)
                          : (value > std::numeric_limits<Integer>::max() + subtrahend);
}

/// Upcast an integer to the largest possible width (currently 64 bits)

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_signed<Integer>::value, int64_t>::type
UpcastInt(Integer v) {
  return v;
}

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_unsigned<Integer>::value, uint64_t>::type
UpcastInt(Integer v) {
  return v;
}

static inline Status CheckSliceParams(int64_t object_length, int64_t slice_offset,
                                      int64_t slice_length, const char* object_name) {
  if (slice_offset < 0) {
    return Status::Invalid("Negative ", object_name, " slice offset");
  }
  if (slice_length < 0) {
    return Status::Invalid("Negative ", object_name, " slice length");
  }
  if (internal::HasPositiveAdditionOverflow(slice_offset, slice_length)) {
    return Status::Invalid(object_name, " slice would overflow");
  }
  if (slice_offset + slice_length > object_length) {
    return Status::Invalid(object_name, " slice would exceed ", object_name, " length");
  }
  return Status::OK();
}

/// \brief Do vectorized boundschecking of integer-type array indices. The
/// indices must be non-nonnegative and strictly less than the passed upper
/// limit (which is usually the length of an array that is being indexed-into).
ARROW_EXPORT
Status CheckIndexBounds(const ArrayData& indices, uint64_t upper_limit);

/// \brief Boundscheck integer values to determine if they are all between the
/// passed upper and lower limits (inclusive). Upper and lower bounds must be
/// the same type as the data and are not currently casted.
ARROW_EXPORT
Status CheckIntegersInRange(const Datum& datum, const Scalar& bound_lower,
                            const Scalar& bound_upper);

/// \brief Use CheckIntegersInRange to determine whether the passed integers
/// can fit safely in the passed integer type. This helps quickly determine if
/// integer narrowing (e.g. int64->int32) is safe to do.
ARROW_EXPORT
Status IntegersCanFit(const Datum& datum, const DataType& target_type);

}  // namespace internal
}  // namespace arrow
