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

#include "arrow/util/decimal.h"

namespace arrow {
namespace decimal {

template <typename T>
ARROW_EXPORT Status FromString(
    const std::string& s, Decimal<T>* out, int* precision, int* scale) {
  // Implements this regex: "(\\+?|-?)((0*)(\\d*))(\\.(\\d+))?";
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to decimal");
  }

  int8_t sign = 1;
  auto charp = s.cbegin();
  auto end = s.cend();

  if (*charp == '+' || *charp == '-') {
    if (*charp == '-') { sign = -1; }
    ++charp;
  }

  auto numeric_string_start = charp;

  // skip leading zeros
  while (*charp == '0') {
    ++charp;
  }

  // all zeros and no decimal point
  if (charp == end) {
    if (out != nullptr) { out->value = static_cast<T>(0); }

    // Not sure what other libraries assign precision to for this case (this case of
    // a string consisting only of one or more zeros)
    if (precision != nullptr) {
      *precision = static_cast<int>(charp - numeric_string_start);
    }

    if (scale != nullptr) { *scale = 0; }

    return Status::OK();
  }

  auto whole_part_start = charp;
  while (isdigit(*charp)) {
    ++charp;
  }
  auto whole_part_end = charp;
  std::string whole_part(whole_part_start, whole_part_end);

  if (*charp == '.') {
    ++charp;
  } else {
    // no decimal point
    DCHECK_EQ(charp, end);
  }

  auto fractional_part_start = charp;
  while (isdigit(*charp)) {
    ++charp;
  }
  auto fractional_part_end = charp;
  std::string fractional_part(fractional_part_start, fractional_part_end);

  if (precision != nullptr) {
    *precision = static_cast<int>(whole_part.size() + fractional_part.size());
  }

  if (scale != nullptr) { *scale = static_cast<int>(fractional_part.size()); }

  if (out != nullptr) { StringToInteger(whole_part, fractional_part, sign, &out->value); }

  return Status::OK();
}

template ARROW_EXPORT Status FromString(
    const std::string& s, Decimal32* out, int* precision, int* scale);
template ARROW_EXPORT Status FromString(
    const std::string& s, Decimal64* out, int* precision, int* scale);
template ARROW_EXPORT Status FromString(
    const std::string& s, Decimal128* out, int* precision, int* scale);

void StringToInteger(
    const std::string& whole, const std::string& fractional, int8_t sign, int32_t* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  if (!whole.empty()) {
    *out = std::stoi(whole, nullptr, 10) *
           static_cast<int32_t>(pow(10.0, static_cast<double>(fractional.size())));
  }
  if (!fractional.empty()) { *out += std::stoi(fractional, nullptr, 10); }
  *out *= sign;
}

void StringToInteger(
    const std::string& whole, const std::string& fractional, int8_t sign, int64_t* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  if (!whole.empty()) {
    *out = static_cast<int64_t>(std::stoll(whole, nullptr, 10)) *
           static_cast<int64_t>(pow(10.0, static_cast<double>(fractional.size())));
  }
  if (!fractional.empty()) { *out += std::stoll(fractional, nullptr, 10); }
  *out *= sign;
}

void StringToInteger(
    const std::string& whole, const std::string& fractional, int8_t sign, int128_t* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  *out = int128_t(whole + fractional) * sign;
}

void FromBytes(const uint8_t* bytes, Decimal32* decimal) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(decimal, nullptr);
  decimal->value = *reinterpret_cast<const int32_t*>(bytes);
}

void FromBytes(const uint8_t* bytes, Decimal64* decimal) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(decimal, nullptr);
  decimal->value = *reinterpret_cast<const int64_t*>(bytes);
}

constexpr static const size_t BYTES_IN_128_BITS = 128 / CHAR_BIT;
constexpr static const size_t LIMB_SIZE =
    sizeof(std::remove_pointer<int128_t::backend_type::limb_pointer>::type);
constexpr static const size_t BYTES_PER_LIMB = BYTES_IN_128_BITS / LIMB_SIZE;

void FromBytes(const uint8_t* bytes, bool is_negative, Decimal128* decimal) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(decimal, nullptr);

  auto& decimal_value(decimal->value);
  int128_t::backend_type& backend(decimal_value.backend());
  backend.resize(BYTES_PER_LIMB, BYTES_PER_LIMB);
  std::memcpy(backend.limbs(), bytes, BYTES_IN_128_BITS);
  if (is_negative) { decimal->value = -decimal->value; }
}

void ToBytes(const Decimal32& value, uint8_t** bytes) {
  DCHECK_NE(*bytes, nullptr);
  *reinterpret_cast<int32_t*>(*bytes) = value.value;
}

void ToBytes(const Decimal64& value, uint8_t** bytes) {
  DCHECK_NE(*bytes, nullptr);
  *reinterpret_cast<int64_t*>(*bytes) = value.value;
}

void ToBytes(const Decimal128& decimal, uint8_t** bytes, bool* is_negative) {
  DCHECK_NE(*bytes, nullptr);
  DCHECK_NE(is_negative, nullptr);

  /// TODO(phillipc): boost multiprecision is unreliable here, int128_t can't be
  /// roundtripped
  const auto& backend(decimal.value.backend());
  auto boost_bytes = reinterpret_cast<const uint8_t*>(backend.limbs());
  std::memcpy(*bytes, boost_bytes, BYTES_IN_128_BITS);
  *is_negative = backend.isneg();
}

}  // namespace decimal
}  // namespace arrow
