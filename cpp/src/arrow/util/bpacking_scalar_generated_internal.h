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

// WARNING: this file is generated, DO NOT EDIT.
// Usage:
//   python cpp/src/arrow/util/bpacking_scalar_codegen.py

#pragma once

#include <cstdint>
#include <cstring>

#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"

namespace arrow::internal {
namespace {

template <typename Int>
Int LoadInt(const uint8_t* in) {
  return bit_util::FromLittleEndian(util::SafeLoadAs<Int>(in));
}

template<typename Uint, int kBitWidth>
struct ScalarUnpackerForWidth;
template<int kBitWidth>
struct ScalarUnpackerForWidth<bool, kBitWidth> {

  static constexpr int kValuesUnpacked = ScalarUnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, bool* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = ScalarUnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<bool>(buffer[k]);
    }
    return in;
  }
};

template<int kBitWidth>
struct ScalarUnpackerForWidth<uint8_t, kBitWidth> {

  static constexpr int kValuesUnpacked = ScalarUnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint8_t* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = ScalarUnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<uint8_t>(buffer[k]);
    }
    return in;
  }
};

template<int kBitWidth>
struct ScalarUnpackerForWidth<uint16_t, kBitWidth> {

  static constexpr int kValuesUnpacked = ScalarUnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = ScalarUnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<uint16_t>(buffer[k]);
    }
    return in;
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 1> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 1) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 1) & mask;
    out[2] = (w0 >> 2) & mask;
    out[3] = (w0 >> 3) & mask;
    out[4] = (w0 >> 4) & mask;
    out[5] = (w0 >> 5) & mask;
    out[6] = (w0 >> 6) & mask;
    out[7] = (w0 >> 7) & mask;
    out[8] = (w0 >> 8) & mask;
    out[9] = (w0 >> 9) & mask;
    out[10] = (w0 >> 10) & mask;
    out[11] = (w0 >> 11) & mask;
    out[12] = (w0 >> 12) & mask;
    out[13] = (w0 >> 13) & mask;
    out[14] = (w0 >> 14) & mask;
    out[15] = (w0 >> 15) & mask;
    out[16] = (w0 >> 16) & mask;
    out[17] = (w0 >> 17) & mask;
    out[18] = (w0 >> 18) & mask;
    out[19] = (w0 >> 19) & mask;
    out[20] = (w0 >> 20) & mask;
    out[21] = (w0 >> 21) & mask;
    out[22] = (w0 >> 22) & mask;
    out[23] = (w0 >> 23) & mask;
    out[24] = (w0 >> 24) & mask;
    out[25] = (w0 >> 25) & mask;
    out[26] = (w0 >> 26) & mask;
    out[27] = (w0 >> 27) & mask;
    out[28] = (w0 >> 28) & mask;
    out[29] = (w0 >> 29) & mask;
    out[30] = (w0 >> 30) & mask;
    out[31] = w0 >> 31;

    return in + (1 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 2> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 2) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 2) & mask;
    out[2] = (w0 >> 4) & mask;
    out[3] = (w0 >> 6) & mask;
    out[4] = (w0 >> 8) & mask;
    out[5] = (w0 >> 10) & mask;
    out[6] = (w0 >> 12) & mask;
    out[7] = (w0 >> 14) & mask;
    out[8] = (w0 >> 16) & mask;
    out[9] = (w0 >> 18) & mask;
    out[10] = (w0 >> 20) & mask;
    out[11] = (w0 >> 22) & mask;
    out[12] = (w0 >> 24) & mask;
    out[13] = (w0 >> 26) & mask;
    out[14] = (w0 >> 28) & mask;
    out[15] = w0 >> 30;
    out[16] = (w1) & mask;
    out[17] = (w1 >> 2) & mask;
    out[18] = (w1 >> 4) & mask;
    out[19] = (w1 >> 6) & mask;
    out[20] = (w1 >> 8) & mask;
    out[21] = (w1 >> 10) & mask;
    out[22] = (w1 >> 12) & mask;
    out[23] = (w1 >> 14) & mask;
    out[24] = (w1 >> 16) & mask;
    out[25] = (w1 >> 18) & mask;
    out[26] = (w1 >> 20) & mask;
    out[27] = (w1 >> 22) & mask;
    out[28] = (w1 >> 24) & mask;
    out[29] = (w1 >> 26) & mask;
    out[30] = (w1 >> 28) & mask;
    out[31] = w1 >> 30;

    return in + (2 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 3> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 3) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 3) & mask;
    out[2] = (w0 >> 6) & mask;
    out[3] = (w0 >> 9) & mask;
    out[4] = (w0 >> 12) & mask;
    out[5] = (w0 >> 15) & mask;
    out[6] = (w0 >> 18) & mask;
    out[7] = (w0 >> 21) & mask;
    out[8] = (w0 >> 24) & mask;
    out[9] = (w0 >> 27) & mask;
    out[10] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[11] = (w1 >> 1) & mask;
    out[12] = (w1 >> 4) & mask;
    out[13] = (w1 >> 7) & mask;
    out[14] = (w1 >> 10) & mask;
    out[15] = (w1 >> 13) & mask;
    out[16] = (w1 >> 16) & mask;
    out[17] = (w1 >> 19) & mask;
    out[18] = (w1 >> 22) & mask;
    out[19] = (w1 >> 25) & mask;
    out[20] = (w1 >> 28) & mask;
    out[21] = ((w1 >> 31) | (w2 << 1)) & mask;
    out[22] = (w2 >> 2) & mask;
    out[23] = (w2 >> 5) & mask;
    out[24] = (w2 >> 8) & mask;
    out[25] = (w2 >> 11) & mask;
    out[26] = (w2 >> 14) & mask;
    out[27] = (w2 >> 17) & mask;
    out[28] = (w2 >> 20) & mask;
    out[29] = (w2 >> 23) & mask;
    out[30] = (w2 >> 26) & mask;
    out[31] = w2 >> 29;

    return in + (3 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 4> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 4) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 4) & mask;
    out[2] = (w0 >> 8) & mask;
    out[3] = (w0 >> 12) & mask;
    out[4] = (w0 >> 16) & mask;
    out[5] = (w0 >> 20) & mask;
    out[6] = (w0 >> 24) & mask;
    out[7] = w0 >> 28;
    out[8] = (w1) & mask;
    out[9] = (w1 >> 4) & mask;
    out[10] = (w1 >> 8) & mask;
    out[11] = (w1 >> 12) & mask;
    out[12] = (w1 >> 16) & mask;
    out[13] = (w1 >> 20) & mask;
    out[14] = (w1 >> 24) & mask;
    out[15] = w1 >> 28;
    out[16] = (w2) & mask;
    out[17] = (w2 >> 4) & mask;
    out[18] = (w2 >> 8) & mask;
    out[19] = (w2 >> 12) & mask;
    out[20] = (w2 >> 16) & mask;
    out[21] = (w2 >> 20) & mask;
    out[22] = (w2 >> 24) & mask;
    out[23] = w2 >> 28;
    out[24] = (w3) & mask;
    out[25] = (w3 >> 4) & mask;
    out[26] = (w3 >> 8) & mask;
    out[27] = (w3 >> 12) & mask;
    out[28] = (w3 >> 16) & mask;
    out[29] = (w3 >> 20) & mask;
    out[30] = (w3 >> 24) & mask;
    out[31] = w3 >> 28;

    return in + (4 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 5> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 5) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 5) & mask;
    out[2] = (w0 >> 10) & mask;
    out[3] = (w0 >> 15) & mask;
    out[4] = (w0 >> 20) & mask;
    out[5] = (w0 >> 25) & mask;
    out[6] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[7] = (w1 >> 3) & mask;
    out[8] = (w1 >> 8) & mask;
    out[9] = (w1 >> 13) & mask;
    out[10] = (w1 >> 18) & mask;
    out[11] = (w1 >> 23) & mask;
    out[12] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[13] = (w2 >> 1) & mask;
    out[14] = (w2 >> 6) & mask;
    out[15] = (w2 >> 11) & mask;
    out[16] = (w2 >> 16) & mask;
    out[17] = (w2 >> 21) & mask;
    out[18] = (w2 >> 26) & mask;
    out[19] = ((w2 >> 31) | (w3 << 1)) & mask;
    out[20] = (w3 >> 4) & mask;
    out[21] = (w3 >> 9) & mask;
    out[22] = (w3 >> 14) & mask;
    out[23] = (w3 >> 19) & mask;
    out[24] = (w3 >> 24) & mask;
    out[25] = ((w3 >> 29) | (w4 << 3)) & mask;
    out[26] = (w4 >> 2) & mask;
    out[27] = (w4 >> 7) & mask;
    out[28] = (w4 >> 12) & mask;
    out[29] = (w4 >> 17) & mask;
    out[30] = (w4 >> 22) & mask;
    out[31] = w4 >> 27;

    return in + (5 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 6> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 6) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 6) & mask;
    out[2] = (w0 >> 12) & mask;
    out[3] = (w0 >> 18) & mask;
    out[4] = (w0 >> 24) & mask;
    out[5] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[6] = (w1 >> 4) & mask;
    out[7] = (w1 >> 10) & mask;
    out[8] = (w1 >> 16) & mask;
    out[9] = (w1 >> 22) & mask;
    out[10] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[11] = (w2 >> 2) & mask;
    out[12] = (w2 >> 8) & mask;
    out[13] = (w2 >> 14) & mask;
    out[14] = (w2 >> 20) & mask;
    out[15] = w2 >> 26;
    out[16] = (w3) & mask;
    out[17] = (w3 >> 6) & mask;
    out[18] = (w3 >> 12) & mask;
    out[19] = (w3 >> 18) & mask;
    out[20] = (w3 >> 24) & mask;
    out[21] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[22] = (w4 >> 4) & mask;
    out[23] = (w4 >> 10) & mask;
    out[24] = (w4 >> 16) & mask;
    out[25] = (w4 >> 22) & mask;
    out[26] = ((w4 >> 28) | (w5 << 4)) & mask;
    out[27] = (w5 >> 2) & mask;
    out[28] = (w5 >> 8) & mask;
    out[29] = (w5 >> 14) & mask;
    out[30] = (w5 >> 20) & mask;
    out[31] = w5 >> 26;

    return in + (6 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 7> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 7) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 7) & mask;
    out[2] = (w0 >> 14) & mask;
    out[3] = (w0 >> 21) & mask;
    out[4] = ((w0 >> 28) | (w1 << 4)) & mask;
    out[5] = (w1 >> 3) & mask;
    out[6] = (w1 >> 10) & mask;
    out[7] = (w1 >> 17) & mask;
    out[8] = (w1 >> 24) & mask;
    out[9] = ((w1 >> 31) | (w2 << 1)) & mask;
    out[10] = (w2 >> 6) & mask;
    out[11] = (w2 >> 13) & mask;
    out[12] = (w2 >> 20) & mask;
    out[13] = ((w2 >> 27) | (w3 << 5)) & mask;
    out[14] = (w3 >> 2) & mask;
    out[15] = (w3 >> 9) & mask;
    out[16] = (w3 >> 16) & mask;
    out[17] = (w3 >> 23) & mask;
    out[18] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[19] = (w4 >> 5) & mask;
    out[20] = (w4 >> 12) & mask;
    out[21] = (w4 >> 19) & mask;
    out[22] = ((w4 >> 26) | (w5 << 6)) & mask;
    out[23] = (w5 >> 1) & mask;
    out[24] = (w5 >> 8) & mask;
    out[25] = (w5 >> 15) & mask;
    out[26] = (w5 >> 22) & mask;
    out[27] = ((w5 >> 29) | (w6 << 3)) & mask;
    out[28] = (w6 >> 4) & mask;
    out[29] = (w6 >> 11) & mask;
    out[30] = (w6 >> 18) & mask;
    out[31] = w6 >> 25;

    return in + (7 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 8> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 8) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 8) & mask;
    out[2] = (w0 >> 16) & mask;
    out[3] = w0 >> 24;
    out[4] = (w1) & mask;
    out[5] = (w1 >> 8) & mask;
    out[6] = (w1 >> 16) & mask;
    out[7] = w1 >> 24;
    out[8] = (w2) & mask;
    out[9] = (w2 >> 8) & mask;
    out[10] = (w2 >> 16) & mask;
    out[11] = w2 >> 24;
    out[12] = (w3) & mask;
    out[13] = (w3 >> 8) & mask;
    out[14] = (w3 >> 16) & mask;
    out[15] = w3 >> 24;
    out[16] = (w4) & mask;
    out[17] = (w4 >> 8) & mask;
    out[18] = (w4 >> 16) & mask;
    out[19] = w4 >> 24;
    out[20] = (w5) & mask;
    out[21] = (w5 >> 8) & mask;
    out[22] = (w5 >> 16) & mask;
    out[23] = w5 >> 24;
    out[24] = (w6) & mask;
    out[25] = (w6 >> 8) & mask;
    out[26] = (w6 >> 16) & mask;
    out[27] = w6 >> 24;
    out[28] = (w7) & mask;
    out[29] = (w7 >> 8) & mask;
    out[30] = (w7 >> 16) & mask;
    out[31] = w7 >> 24;

    return in + (8 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 9> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 9) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 9) & mask;
    out[2] = (w0 >> 18) & mask;
    out[3] = ((w0 >> 27) | (w1 << 5)) & mask;
    out[4] = (w1 >> 4) & mask;
    out[5] = (w1 >> 13) & mask;
    out[6] = (w1 >> 22) & mask;
    out[7] = ((w1 >> 31) | (w2 << 1)) & mask;
    out[8] = (w2 >> 8) & mask;
    out[9] = (w2 >> 17) & mask;
    out[10] = ((w2 >> 26) | (w3 << 6)) & mask;
    out[11] = (w3 >> 3) & mask;
    out[12] = (w3 >> 12) & mask;
    out[13] = (w3 >> 21) & mask;
    out[14] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[15] = (w4 >> 7) & mask;
    out[16] = (w4 >> 16) & mask;
    out[17] = ((w4 >> 25) | (w5 << 7)) & mask;
    out[18] = (w5 >> 2) & mask;
    out[19] = (w5 >> 11) & mask;
    out[20] = (w5 >> 20) & mask;
    out[21] = ((w5 >> 29) | (w6 << 3)) & mask;
    out[22] = (w6 >> 6) & mask;
    out[23] = (w6 >> 15) & mask;
    out[24] = ((w6 >> 24) | (w7 << 8)) & mask;
    out[25] = (w7 >> 1) & mask;
    out[26] = (w7 >> 10) & mask;
    out[27] = (w7 >> 19) & mask;
    out[28] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[29] = (w8 >> 5) & mask;
    out[30] = (w8 >> 14) & mask;
    out[31] = w8 >> 23;

    return in + (9 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 10> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 10) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 10) & mask;
    out[2] = (w0 >> 20) & mask;
    out[3] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[4] = (w1 >> 8) & mask;
    out[5] = (w1 >> 18) & mask;
    out[6] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[7] = (w2 >> 6) & mask;
    out[8] = (w2 >> 16) & mask;
    out[9] = ((w2 >> 26) | (w3 << 6)) & mask;
    out[10] = (w3 >> 4) & mask;
    out[11] = (w3 >> 14) & mask;
    out[12] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[13] = (w4 >> 2) & mask;
    out[14] = (w4 >> 12) & mask;
    out[15] = w4 >> 22;
    out[16] = (w5) & mask;
    out[17] = (w5 >> 10) & mask;
    out[18] = (w5 >> 20) & mask;
    out[19] = ((w5 >> 30) | (w6 << 2)) & mask;
    out[20] = (w6 >> 8) & mask;
    out[21] = (w6 >> 18) & mask;
    out[22] = ((w6 >> 28) | (w7 << 4)) & mask;
    out[23] = (w7 >> 6) & mask;
    out[24] = (w7 >> 16) & mask;
    out[25] = ((w7 >> 26) | (w8 << 6)) & mask;
    out[26] = (w8 >> 4) & mask;
    out[27] = (w8 >> 14) & mask;
    out[28] = ((w8 >> 24) | (w9 << 8)) & mask;
    out[29] = (w9 >> 2) & mask;
    out[30] = (w9 >> 12) & mask;
    out[31] = w9 >> 22;

    return in + (10 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 11> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 11) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 11) & mask;
    out[2] = ((w0 >> 22) | (w1 << 10)) & mask;
    out[3] = (w1 >> 1) & mask;
    out[4] = (w1 >> 12) & mask;
    out[5] = ((w1 >> 23) | (w2 << 9)) & mask;
    out[6] = (w2 >> 2) & mask;
    out[7] = (w2 >> 13) & mask;
    out[8] = ((w2 >> 24) | (w3 << 8)) & mask;
    out[9] = (w3 >> 3) & mask;
    out[10] = (w3 >> 14) & mask;
    out[11] = ((w3 >> 25) | (w4 << 7)) & mask;
    out[12] = (w4 >> 4) & mask;
    out[13] = (w4 >> 15) & mask;
    out[14] = ((w4 >> 26) | (w5 << 6)) & mask;
    out[15] = (w5 >> 5) & mask;
    out[16] = (w5 >> 16) & mask;
    out[17] = ((w5 >> 27) | (w6 << 5)) & mask;
    out[18] = (w6 >> 6) & mask;
    out[19] = (w6 >> 17) & mask;
    out[20] = ((w6 >> 28) | (w7 << 4)) & mask;
    out[21] = (w7 >> 7) & mask;
    out[22] = (w7 >> 18) & mask;
    out[23] = ((w7 >> 29) | (w8 << 3)) & mask;
    out[24] = (w8 >> 8) & mask;
    out[25] = (w8 >> 19) & mask;
    out[26] = ((w8 >> 30) | (w9 << 2)) & mask;
    out[27] = (w9 >> 9) & mask;
    out[28] = (w9 >> 20) & mask;
    out[29] = ((w9 >> 31) | (w10 << 1)) & mask;
    out[30] = (w10 >> 10) & mask;
    out[31] = w10 >> 21;

    return in + (11 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 12> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 12) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 12) & mask;
    out[2] = ((w0 >> 24) | (w1 << 8)) & mask;
    out[3] = (w1 >> 4) & mask;
    out[4] = (w1 >> 16) & mask;
    out[5] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[6] = (w2 >> 8) & mask;
    out[7] = w2 >> 20;
    out[8] = (w3) & mask;
    out[9] = (w3 >> 12) & mask;
    out[10] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[11] = (w4 >> 4) & mask;
    out[12] = (w4 >> 16) & mask;
    out[13] = ((w4 >> 28) | (w5 << 4)) & mask;
    out[14] = (w5 >> 8) & mask;
    out[15] = w5 >> 20;
    out[16] = (w6) & mask;
    out[17] = (w6 >> 12) & mask;
    out[18] = ((w6 >> 24) | (w7 << 8)) & mask;
    out[19] = (w7 >> 4) & mask;
    out[20] = (w7 >> 16) & mask;
    out[21] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[22] = (w8 >> 8) & mask;
    out[23] = w8 >> 20;
    out[24] = (w9) & mask;
    out[25] = (w9 >> 12) & mask;
    out[26] = ((w9 >> 24) | (w10 << 8)) & mask;
    out[27] = (w10 >> 4) & mask;
    out[28] = (w10 >> 16) & mask;
    out[29] = ((w10 >> 28) | (w11 << 4)) & mask;
    out[30] = (w11 >> 8) & mask;
    out[31] = w11 >> 20;

    return in + (12 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 13> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 13) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 13) & mask;
    out[2] = ((w0 >> 26) | (w1 << 6)) & mask;
    out[3] = (w1 >> 7) & mask;
    out[4] = ((w1 >> 20) | (w2 << 12)) & mask;
    out[5] = (w2 >> 1) & mask;
    out[6] = (w2 >> 14) & mask;
    out[7] = ((w2 >> 27) | (w3 << 5)) & mask;
    out[8] = (w3 >> 8) & mask;
    out[9] = ((w3 >> 21) | (w4 << 11)) & mask;
    out[10] = (w4 >> 2) & mask;
    out[11] = (w4 >> 15) & mask;
    out[12] = ((w4 >> 28) | (w5 << 4)) & mask;
    out[13] = (w5 >> 9) & mask;
    out[14] = ((w5 >> 22) | (w6 << 10)) & mask;
    out[15] = (w6 >> 3) & mask;
    out[16] = (w6 >> 16) & mask;
    out[17] = ((w6 >> 29) | (w7 << 3)) & mask;
    out[18] = (w7 >> 10) & mask;
    out[19] = ((w7 >> 23) | (w8 << 9)) & mask;
    out[20] = (w8 >> 4) & mask;
    out[21] = (w8 >> 17) & mask;
    out[22] = ((w8 >> 30) | (w9 << 2)) & mask;
    out[23] = (w9 >> 11) & mask;
    out[24] = ((w9 >> 24) | (w10 << 8)) & mask;
    out[25] = (w10 >> 5) & mask;
    out[26] = (w10 >> 18) & mask;
    out[27] = ((w10 >> 31) | (w11 << 1)) & mask;
    out[28] = (w11 >> 12) & mask;
    out[29] = ((w11 >> 25) | (w12 << 7)) & mask;
    out[30] = (w12 >> 6) & mask;
    out[31] = w12 >> 19;

    return in + (13 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 14> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 14) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 14) & mask;
    out[2] = ((w0 >> 28) | (w1 << 4)) & mask;
    out[3] = (w1 >> 10) & mask;
    out[4] = ((w1 >> 24) | (w2 << 8)) & mask;
    out[5] = (w2 >> 6) & mask;
    out[6] = ((w2 >> 20) | (w3 << 12)) & mask;
    out[7] = (w3 >> 2) & mask;
    out[8] = (w3 >> 16) & mask;
    out[9] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[10] = (w4 >> 12) & mask;
    out[11] = ((w4 >> 26) | (w5 << 6)) & mask;
    out[12] = (w5 >> 8) & mask;
    out[13] = ((w5 >> 22) | (w6 << 10)) & mask;
    out[14] = (w6 >> 4) & mask;
    out[15] = w6 >> 18;
    out[16] = (w7) & mask;
    out[17] = (w7 >> 14) & mask;
    out[18] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[19] = (w8 >> 10) & mask;
    out[20] = ((w8 >> 24) | (w9 << 8)) & mask;
    out[21] = (w9 >> 6) & mask;
    out[22] = ((w9 >> 20) | (w10 << 12)) & mask;
    out[23] = (w10 >> 2) & mask;
    out[24] = (w10 >> 16) & mask;
    out[25] = ((w10 >> 30) | (w11 << 2)) & mask;
    out[26] = (w11 >> 12) & mask;
    out[27] = ((w11 >> 26) | (w12 << 6)) & mask;
    out[28] = (w12 >> 8) & mask;
    out[29] = ((w12 >> 22) | (w13 << 10)) & mask;
    out[30] = (w13 >> 4) & mask;
    out[31] = w13 >> 18;

    return in + (14 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 15> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 15) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 15) & mask;
    out[2] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[3] = (w1 >> 13) & mask;
    out[4] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[5] = (w2 >> 11) & mask;
    out[6] = ((w2 >> 26) | (w3 << 6)) & mask;
    out[7] = (w3 >> 9) & mask;
    out[8] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[9] = (w4 >> 7) & mask;
    out[10] = ((w4 >> 22) | (w5 << 10)) & mask;
    out[11] = (w5 >> 5) & mask;
    out[12] = ((w5 >> 20) | (w6 << 12)) & mask;
    out[13] = (w6 >> 3) & mask;
    out[14] = ((w6 >> 18) | (w7 << 14)) & mask;
    out[15] = (w7 >> 1) & mask;
    out[16] = (w7 >> 16) & mask;
    out[17] = ((w7 >> 31) | (w8 << 1)) & mask;
    out[18] = (w8 >> 14) & mask;
    out[19] = ((w8 >> 29) | (w9 << 3)) & mask;
    out[20] = (w9 >> 12) & mask;
    out[21] = ((w9 >> 27) | (w10 << 5)) & mask;
    out[22] = (w10 >> 10) & mask;
    out[23] = ((w10 >> 25) | (w11 << 7)) & mask;
    out[24] = (w11 >> 8) & mask;
    out[25] = ((w11 >> 23) | (w12 << 9)) & mask;
    out[26] = (w12 >> 6) & mask;
    out[27] = ((w12 >> 21) | (w13 << 11)) & mask;
    out[28] = (w13 >> 4) & mask;
    out[29] = ((w13 >> 19) | (w14 << 13)) & mask;
    out[30] = (w14 >> 2) & mask;
    out[31] = w14 >> 17;

    return in + (15 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 16> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 16) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    out[0] = (w0) & mask;
    out[1] = w0 >> 16;
    out[2] = (w1) & mask;
    out[3] = w1 >> 16;
    out[4] = (w2) & mask;
    out[5] = w2 >> 16;
    out[6] = (w3) & mask;
    out[7] = w3 >> 16;
    out[8] = (w4) & mask;
    out[9] = w4 >> 16;
    out[10] = (w5) & mask;
    out[11] = w5 >> 16;
    out[12] = (w6) & mask;
    out[13] = w6 >> 16;
    out[14] = (w7) & mask;
    out[15] = w7 >> 16;
    out[16] = (w8) & mask;
    out[17] = w8 >> 16;
    out[18] = (w9) & mask;
    out[19] = w9 >> 16;
    out[20] = (w10) & mask;
    out[21] = w10 >> 16;
    out[22] = (w11) & mask;
    out[23] = w11 >> 16;
    out[24] = (w12) & mask;
    out[25] = w12 >> 16;
    out[26] = (w13) & mask;
    out[27] = w13 >> 16;
    out[28] = (w14) & mask;
    out[29] = w14 >> 16;
    out[30] = (w15) & mask;
    out[31] = w15 >> 16;

    return in + (16 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 17> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 17) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 17) | (w1 << 15)) & mask;
    out[2] = (w1 >> 2) & mask;
    out[3] = ((w1 >> 19) | (w2 << 13)) & mask;
    out[4] = (w2 >> 4) & mask;
    out[5] = ((w2 >> 21) | (w3 << 11)) & mask;
    out[6] = (w3 >> 6) & mask;
    out[7] = ((w3 >> 23) | (w4 << 9)) & mask;
    out[8] = (w4 >> 8) & mask;
    out[9] = ((w4 >> 25) | (w5 << 7)) & mask;
    out[10] = (w5 >> 10) & mask;
    out[11] = ((w5 >> 27) | (w6 << 5)) & mask;
    out[12] = (w6 >> 12) & mask;
    out[13] = ((w6 >> 29) | (w7 << 3)) & mask;
    out[14] = (w7 >> 14) & mask;
    out[15] = ((w7 >> 31) | (w8 << 1)) & mask;
    out[16] = ((w8 >> 16) | (w9 << 16)) & mask;
    out[17] = (w9 >> 1) & mask;
    out[18] = ((w9 >> 18) | (w10 << 14)) & mask;
    out[19] = (w10 >> 3) & mask;
    out[20] = ((w10 >> 20) | (w11 << 12)) & mask;
    out[21] = (w11 >> 5) & mask;
    out[22] = ((w11 >> 22) | (w12 << 10)) & mask;
    out[23] = (w12 >> 7) & mask;
    out[24] = ((w12 >> 24) | (w13 << 8)) & mask;
    out[25] = (w13 >> 9) & mask;
    out[26] = ((w13 >> 26) | (w14 << 6)) & mask;
    out[27] = (w14 >> 11) & mask;
    out[28] = ((w14 >> 28) | (w15 << 4)) & mask;
    out[29] = (w15 >> 13) & mask;
    out[30] = ((w15 >> 30) | (w16 << 2)) & mask;
    out[31] = w16 >> 15;

    return in + (17 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 18> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 18) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 18) | (w1 << 14)) & mask;
    out[2] = (w1 >> 4) & mask;
    out[3] = ((w1 >> 22) | (w2 << 10)) & mask;
    out[4] = (w2 >> 8) & mask;
    out[5] = ((w2 >> 26) | (w3 << 6)) & mask;
    out[6] = (w3 >> 12) & mask;
    out[7] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[8] = ((w4 >> 16) | (w5 << 16)) & mask;
    out[9] = (w5 >> 2) & mask;
    out[10] = ((w5 >> 20) | (w6 << 12)) & mask;
    out[11] = (w6 >> 6) & mask;
    out[12] = ((w6 >> 24) | (w7 << 8)) & mask;
    out[13] = (w7 >> 10) & mask;
    out[14] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[15] = w8 >> 14;
    out[16] = (w9) & mask;
    out[17] = ((w9 >> 18) | (w10 << 14)) & mask;
    out[18] = (w10 >> 4) & mask;
    out[19] = ((w10 >> 22) | (w11 << 10)) & mask;
    out[20] = (w11 >> 8) & mask;
    out[21] = ((w11 >> 26) | (w12 << 6)) & mask;
    out[22] = (w12 >> 12) & mask;
    out[23] = ((w12 >> 30) | (w13 << 2)) & mask;
    out[24] = ((w13 >> 16) | (w14 << 16)) & mask;
    out[25] = (w14 >> 2) & mask;
    out[26] = ((w14 >> 20) | (w15 << 12)) & mask;
    out[27] = (w15 >> 6) & mask;
    out[28] = ((w15 >> 24) | (w16 << 8)) & mask;
    out[29] = (w16 >> 10) & mask;
    out[30] = ((w16 >> 28) | (w17 << 4)) & mask;
    out[31] = w17 >> 14;

    return in + (18 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 19> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 19) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 19) | (w1 << 13)) & mask;
    out[2] = (w1 >> 6) & mask;
    out[3] = ((w1 >> 25) | (w2 << 7)) & mask;
    out[4] = (w2 >> 12) & mask;
    out[5] = ((w2 >> 31) | (w3 << 1)) & mask;
    out[6] = ((w3 >> 18) | (w4 << 14)) & mask;
    out[7] = (w4 >> 5) & mask;
    out[8] = ((w4 >> 24) | (w5 << 8)) & mask;
    out[9] = (w5 >> 11) & mask;
    out[10] = ((w5 >> 30) | (w6 << 2)) & mask;
    out[11] = ((w6 >> 17) | (w7 << 15)) & mask;
    out[12] = (w7 >> 4) & mask;
    out[13] = ((w7 >> 23) | (w8 << 9)) & mask;
    out[14] = (w8 >> 10) & mask;
    out[15] = ((w8 >> 29) | (w9 << 3)) & mask;
    out[16] = ((w9 >> 16) | (w10 << 16)) & mask;
    out[17] = (w10 >> 3) & mask;
    out[18] = ((w10 >> 22) | (w11 << 10)) & mask;
    out[19] = (w11 >> 9) & mask;
    out[20] = ((w11 >> 28) | (w12 << 4)) & mask;
    out[21] = ((w12 >> 15) | (w13 << 17)) & mask;
    out[22] = (w13 >> 2) & mask;
    out[23] = ((w13 >> 21) | (w14 << 11)) & mask;
    out[24] = (w14 >> 8) & mask;
    out[25] = ((w14 >> 27) | (w15 << 5)) & mask;
    out[26] = ((w15 >> 14) | (w16 << 18)) & mask;
    out[27] = (w16 >> 1) & mask;
    out[28] = ((w16 >> 20) | (w17 << 12)) & mask;
    out[29] = (w17 >> 7) & mask;
    out[30] = ((w17 >> 26) | (w18 << 6)) & mask;
    out[31] = w18 >> 13;

    return in + (19 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 20> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 20) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 20) | (w1 << 12)) & mask;
    out[2] = (w1 >> 8) & mask;
    out[3] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[4] = ((w2 >> 16) | (w3 << 16)) & mask;
    out[5] = (w3 >> 4) & mask;
    out[6] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[7] = w4 >> 12;
    out[8] = (w5) & mask;
    out[9] = ((w5 >> 20) | (w6 << 12)) & mask;
    out[10] = (w6 >> 8) & mask;
    out[11] = ((w6 >> 28) | (w7 << 4)) & mask;
    out[12] = ((w7 >> 16) | (w8 << 16)) & mask;
    out[13] = (w8 >> 4) & mask;
    out[14] = ((w8 >> 24) | (w9 << 8)) & mask;
    out[15] = w9 >> 12;
    out[16] = (w10) & mask;
    out[17] = ((w10 >> 20) | (w11 << 12)) & mask;
    out[18] = (w11 >> 8) & mask;
    out[19] = ((w11 >> 28) | (w12 << 4)) & mask;
    out[20] = ((w12 >> 16) | (w13 << 16)) & mask;
    out[21] = (w13 >> 4) & mask;
    out[22] = ((w13 >> 24) | (w14 << 8)) & mask;
    out[23] = w14 >> 12;
    out[24] = (w15) & mask;
    out[25] = ((w15 >> 20) | (w16 << 12)) & mask;
    out[26] = (w16 >> 8) & mask;
    out[27] = ((w16 >> 28) | (w17 << 4)) & mask;
    out[28] = ((w17 >> 16) | (w18 << 16)) & mask;
    out[29] = (w18 >> 4) & mask;
    out[30] = ((w18 >> 24) | (w19 << 8)) & mask;
    out[31] = w19 >> 12;

    return in + (20 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 21> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 21) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 21) | (w1 << 11)) & mask;
    out[2] = (w1 >> 10) & mask;
    out[3] = ((w1 >> 31) | (w2 << 1)) & mask;
    out[4] = ((w2 >> 20) | (w3 << 12)) & mask;
    out[5] = (w3 >> 9) & mask;
    out[6] = ((w3 >> 30) | (w4 << 2)) & mask;
    out[7] = ((w4 >> 19) | (w5 << 13)) & mask;
    out[8] = (w5 >> 8) & mask;
    out[9] = ((w5 >> 29) | (w6 << 3)) & mask;
    out[10] = ((w6 >> 18) | (w7 << 14)) & mask;
    out[11] = (w7 >> 7) & mask;
    out[12] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[13] = ((w8 >> 17) | (w9 << 15)) & mask;
    out[14] = (w9 >> 6) & mask;
    out[15] = ((w9 >> 27) | (w10 << 5)) & mask;
    out[16] = ((w10 >> 16) | (w11 << 16)) & mask;
    out[17] = (w11 >> 5) & mask;
    out[18] = ((w11 >> 26) | (w12 << 6)) & mask;
    out[19] = ((w12 >> 15) | (w13 << 17)) & mask;
    out[20] = (w13 >> 4) & mask;
    out[21] = ((w13 >> 25) | (w14 << 7)) & mask;
    out[22] = ((w14 >> 14) | (w15 << 18)) & mask;
    out[23] = (w15 >> 3) & mask;
    out[24] = ((w15 >> 24) | (w16 << 8)) & mask;
    out[25] = ((w16 >> 13) | (w17 << 19)) & mask;
    out[26] = (w17 >> 2) & mask;
    out[27] = ((w17 >> 23) | (w18 << 9)) & mask;
    out[28] = ((w18 >> 12) | (w19 << 20)) & mask;
    out[29] = (w19 >> 1) & mask;
    out[30] = ((w19 >> 22) | (w20 << 10)) & mask;
    out[31] = w20 >> 11;

    return in + (21 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 22> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 22) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 22) | (w1 << 10)) & mask;
    out[2] = ((w1 >> 12) | (w2 << 20)) & mask;
    out[3] = (w2 >> 2) & mask;
    out[4] = ((w2 >> 24) | (w3 << 8)) & mask;
    out[5] = ((w3 >> 14) | (w4 << 18)) & mask;
    out[6] = (w4 >> 4) & mask;
    out[7] = ((w4 >> 26) | (w5 << 6)) & mask;
    out[8] = ((w5 >> 16) | (w6 << 16)) & mask;
    out[9] = (w6 >> 6) & mask;
    out[10] = ((w6 >> 28) | (w7 << 4)) & mask;
    out[11] = ((w7 >> 18) | (w8 << 14)) & mask;
    out[12] = (w8 >> 8) & mask;
    out[13] = ((w8 >> 30) | (w9 << 2)) & mask;
    out[14] = ((w9 >> 20) | (w10 << 12)) & mask;
    out[15] = w10 >> 10;
    out[16] = (w11) & mask;
    out[17] = ((w11 >> 22) | (w12 << 10)) & mask;
    out[18] = ((w12 >> 12) | (w13 << 20)) & mask;
    out[19] = (w13 >> 2) & mask;
    out[20] = ((w13 >> 24) | (w14 << 8)) & mask;
    out[21] = ((w14 >> 14) | (w15 << 18)) & mask;
    out[22] = (w15 >> 4) & mask;
    out[23] = ((w15 >> 26) | (w16 << 6)) & mask;
    out[24] = ((w16 >> 16) | (w17 << 16)) & mask;
    out[25] = (w17 >> 6) & mask;
    out[26] = ((w17 >> 28) | (w18 << 4)) & mask;
    out[27] = ((w18 >> 18) | (w19 << 14)) & mask;
    out[28] = (w19 >> 8) & mask;
    out[29] = ((w19 >> 30) | (w20 << 2)) & mask;
    out[30] = ((w20 >> 20) | (w21 << 12)) & mask;
    out[31] = w21 >> 10;

    return in + (22 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 23> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 23) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 23) | (w1 << 9)) & mask;
    out[2] = ((w1 >> 14) | (w2 << 18)) & mask;
    out[3] = (w2 >> 5) & mask;
    out[4] = ((w2 >> 28) | (w3 << 4)) & mask;
    out[5] = ((w3 >> 19) | (w4 << 13)) & mask;
    out[6] = ((w4 >> 10) | (w5 << 22)) & mask;
    out[7] = (w5 >> 1) & mask;
    out[8] = ((w5 >> 24) | (w6 << 8)) & mask;
    out[9] = ((w6 >> 15) | (w7 << 17)) & mask;
    out[10] = (w7 >> 6) & mask;
    out[11] = ((w7 >> 29) | (w8 << 3)) & mask;
    out[12] = ((w8 >> 20) | (w9 << 12)) & mask;
    out[13] = ((w9 >> 11) | (w10 << 21)) & mask;
    out[14] = (w10 >> 2) & mask;
    out[15] = ((w10 >> 25) | (w11 << 7)) & mask;
    out[16] = ((w11 >> 16) | (w12 << 16)) & mask;
    out[17] = (w12 >> 7) & mask;
    out[18] = ((w12 >> 30) | (w13 << 2)) & mask;
    out[19] = ((w13 >> 21) | (w14 << 11)) & mask;
    out[20] = ((w14 >> 12) | (w15 << 20)) & mask;
    out[21] = (w15 >> 3) & mask;
    out[22] = ((w15 >> 26) | (w16 << 6)) & mask;
    out[23] = ((w16 >> 17) | (w17 << 15)) & mask;
    out[24] = (w17 >> 8) & mask;
    out[25] = ((w17 >> 31) | (w18 << 1)) & mask;
    out[26] = ((w18 >> 22) | (w19 << 10)) & mask;
    out[27] = ((w19 >> 13) | (w20 << 19)) & mask;
    out[28] = (w20 >> 4) & mask;
    out[29] = ((w20 >> 27) | (w21 << 5)) & mask;
    out[30] = ((w21 >> 18) | (w22 << 14)) & mask;
    out[31] = w22 >> 9;

    return in + (23 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 24> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 24) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 24) | (w1 << 8)) & mask;
    out[2] = ((w1 >> 16) | (w2 << 16)) & mask;
    out[3] = w2 >> 8;
    out[4] = (w3) & mask;
    out[5] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[6] = ((w4 >> 16) | (w5 << 16)) & mask;
    out[7] = w5 >> 8;
    out[8] = (w6) & mask;
    out[9] = ((w6 >> 24) | (w7 << 8)) & mask;
    out[10] = ((w7 >> 16) | (w8 << 16)) & mask;
    out[11] = w8 >> 8;
    out[12] = (w9) & mask;
    out[13] = ((w9 >> 24) | (w10 << 8)) & mask;
    out[14] = ((w10 >> 16) | (w11 << 16)) & mask;
    out[15] = w11 >> 8;
    out[16] = (w12) & mask;
    out[17] = ((w12 >> 24) | (w13 << 8)) & mask;
    out[18] = ((w13 >> 16) | (w14 << 16)) & mask;
    out[19] = w14 >> 8;
    out[20] = (w15) & mask;
    out[21] = ((w15 >> 24) | (w16 << 8)) & mask;
    out[22] = ((w16 >> 16) | (w17 << 16)) & mask;
    out[23] = w17 >> 8;
    out[24] = (w18) & mask;
    out[25] = ((w18 >> 24) | (w19 << 8)) & mask;
    out[26] = ((w19 >> 16) | (w20 << 16)) & mask;
    out[27] = w20 >> 8;
    out[28] = (w21) & mask;
    out[29] = ((w21 >> 24) | (w22 << 8)) & mask;
    out[30] = ((w22 >> 16) | (w23 << 16)) & mask;
    out[31] = w23 >> 8;

    return in + (24 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 25> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 25) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 25) | (w1 << 7)) & mask;
    out[2] = ((w1 >> 18) | (w2 << 14)) & mask;
    out[3] = ((w2 >> 11) | (w3 << 21)) & mask;
    out[4] = (w3 >> 4) & mask;
    out[5] = ((w3 >> 29) | (w4 << 3)) & mask;
    out[6] = ((w4 >> 22) | (w5 << 10)) & mask;
    out[7] = ((w5 >> 15) | (w6 << 17)) & mask;
    out[8] = ((w6 >> 8) | (w7 << 24)) & mask;
    out[9] = (w7 >> 1) & mask;
    out[10] = ((w7 >> 26) | (w8 << 6)) & mask;
    out[11] = ((w8 >> 19) | (w9 << 13)) & mask;
    out[12] = ((w9 >> 12) | (w10 << 20)) & mask;
    out[13] = (w10 >> 5) & mask;
    out[14] = ((w10 >> 30) | (w11 << 2)) & mask;
    out[15] = ((w11 >> 23) | (w12 << 9)) & mask;
    out[16] = ((w12 >> 16) | (w13 << 16)) & mask;
    out[17] = ((w13 >> 9) | (w14 << 23)) & mask;
    out[18] = (w14 >> 2) & mask;
    out[19] = ((w14 >> 27) | (w15 << 5)) & mask;
    out[20] = ((w15 >> 20) | (w16 << 12)) & mask;
    out[21] = ((w16 >> 13) | (w17 << 19)) & mask;
    out[22] = (w17 >> 6) & mask;
    out[23] = ((w17 >> 31) | (w18 << 1)) & mask;
    out[24] = ((w18 >> 24) | (w19 << 8)) & mask;
    out[25] = ((w19 >> 17) | (w20 << 15)) & mask;
    out[26] = ((w20 >> 10) | (w21 << 22)) & mask;
    out[27] = (w21 >> 3) & mask;
    out[28] = ((w21 >> 28) | (w22 << 4)) & mask;
    out[29] = ((w22 >> 21) | (w23 << 11)) & mask;
    out[30] = ((w23 >> 14) | (w24 << 18)) & mask;
    out[31] = w24 >> 7;

    return in + (25 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 26> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 26) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 26) | (w1 << 6)) & mask;
    out[2] = ((w1 >> 20) | (w2 << 12)) & mask;
    out[3] = ((w2 >> 14) | (w3 << 18)) & mask;
    out[4] = ((w3 >> 8) | (w4 << 24)) & mask;
    out[5] = (w4 >> 2) & mask;
    out[6] = ((w4 >> 28) | (w5 << 4)) & mask;
    out[7] = ((w5 >> 22) | (w6 << 10)) & mask;
    out[8] = ((w6 >> 16) | (w7 << 16)) & mask;
    out[9] = ((w7 >> 10) | (w8 << 22)) & mask;
    out[10] = (w8 >> 4) & mask;
    out[11] = ((w8 >> 30) | (w9 << 2)) & mask;
    out[12] = ((w9 >> 24) | (w10 << 8)) & mask;
    out[13] = ((w10 >> 18) | (w11 << 14)) & mask;
    out[14] = ((w11 >> 12) | (w12 << 20)) & mask;
    out[15] = w12 >> 6;
    out[16] = (w13) & mask;
    out[17] = ((w13 >> 26) | (w14 << 6)) & mask;
    out[18] = ((w14 >> 20) | (w15 << 12)) & mask;
    out[19] = ((w15 >> 14) | (w16 << 18)) & mask;
    out[20] = ((w16 >> 8) | (w17 << 24)) & mask;
    out[21] = (w17 >> 2) & mask;
    out[22] = ((w17 >> 28) | (w18 << 4)) & mask;
    out[23] = ((w18 >> 22) | (w19 << 10)) & mask;
    out[24] = ((w19 >> 16) | (w20 << 16)) & mask;
    out[25] = ((w20 >> 10) | (w21 << 22)) & mask;
    out[26] = (w21 >> 4) & mask;
    out[27] = ((w21 >> 30) | (w22 << 2)) & mask;
    out[28] = ((w22 >> 24) | (w23 << 8)) & mask;
    out[29] = ((w23 >> 18) | (w24 << 14)) & mask;
    out[30] = ((w24 >> 12) | (w25 << 20)) & mask;
    out[31] = w25 >> 6;

    return in + (26 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 27> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 27) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    const auto w26 = LoadInt<uint32_t>(in + 26 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 27) | (w1 << 5)) & mask;
    out[2] = ((w1 >> 22) | (w2 << 10)) & mask;
    out[3] = ((w2 >> 17) | (w3 << 15)) & mask;
    out[4] = ((w3 >> 12) | (w4 << 20)) & mask;
    out[5] = ((w4 >> 7) | (w5 << 25)) & mask;
    out[6] = (w5 >> 2) & mask;
    out[7] = ((w5 >> 29) | (w6 << 3)) & mask;
    out[8] = ((w6 >> 24) | (w7 << 8)) & mask;
    out[9] = ((w7 >> 19) | (w8 << 13)) & mask;
    out[10] = ((w8 >> 14) | (w9 << 18)) & mask;
    out[11] = ((w9 >> 9) | (w10 << 23)) & mask;
    out[12] = (w10 >> 4) & mask;
    out[13] = ((w10 >> 31) | (w11 << 1)) & mask;
    out[14] = ((w11 >> 26) | (w12 << 6)) & mask;
    out[15] = ((w12 >> 21) | (w13 << 11)) & mask;
    out[16] = ((w13 >> 16) | (w14 << 16)) & mask;
    out[17] = ((w14 >> 11) | (w15 << 21)) & mask;
    out[18] = ((w15 >> 6) | (w16 << 26)) & mask;
    out[19] = (w16 >> 1) & mask;
    out[20] = ((w16 >> 28) | (w17 << 4)) & mask;
    out[21] = ((w17 >> 23) | (w18 << 9)) & mask;
    out[22] = ((w18 >> 18) | (w19 << 14)) & mask;
    out[23] = ((w19 >> 13) | (w20 << 19)) & mask;
    out[24] = ((w20 >> 8) | (w21 << 24)) & mask;
    out[25] = (w21 >> 3) & mask;
    out[26] = ((w21 >> 30) | (w22 << 2)) & mask;
    out[27] = ((w22 >> 25) | (w23 << 7)) & mask;
    out[28] = ((w23 >> 20) | (w24 << 12)) & mask;
    out[29] = ((w24 >> 15) | (w25 << 17)) & mask;
    out[30] = ((w25 >> 10) | (w26 << 22)) & mask;
    out[31] = w26 >> 5;

    return in + (27 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 28> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 28) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    const auto w26 = LoadInt<uint32_t>(in + 26 * 4);
    const auto w27 = LoadInt<uint32_t>(in + 27 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 28) | (w1 << 4)) & mask;
    out[2] = ((w1 >> 24) | (w2 << 8)) & mask;
    out[3] = ((w2 >> 20) | (w3 << 12)) & mask;
    out[4] = ((w3 >> 16) | (w4 << 16)) & mask;
    out[5] = ((w4 >> 12) | (w5 << 20)) & mask;
    out[6] = ((w5 >> 8) | (w6 << 24)) & mask;
    out[7] = w6 >> 4;
    out[8] = (w7) & mask;
    out[9] = ((w7 >> 28) | (w8 << 4)) & mask;
    out[10] = ((w8 >> 24) | (w9 << 8)) & mask;
    out[11] = ((w9 >> 20) | (w10 << 12)) & mask;
    out[12] = ((w10 >> 16) | (w11 << 16)) & mask;
    out[13] = ((w11 >> 12) | (w12 << 20)) & mask;
    out[14] = ((w12 >> 8) | (w13 << 24)) & mask;
    out[15] = w13 >> 4;
    out[16] = (w14) & mask;
    out[17] = ((w14 >> 28) | (w15 << 4)) & mask;
    out[18] = ((w15 >> 24) | (w16 << 8)) & mask;
    out[19] = ((w16 >> 20) | (w17 << 12)) & mask;
    out[20] = ((w17 >> 16) | (w18 << 16)) & mask;
    out[21] = ((w18 >> 12) | (w19 << 20)) & mask;
    out[22] = ((w19 >> 8) | (w20 << 24)) & mask;
    out[23] = w20 >> 4;
    out[24] = (w21) & mask;
    out[25] = ((w21 >> 28) | (w22 << 4)) & mask;
    out[26] = ((w22 >> 24) | (w23 << 8)) & mask;
    out[27] = ((w23 >> 20) | (w24 << 12)) & mask;
    out[28] = ((w24 >> 16) | (w25 << 16)) & mask;
    out[29] = ((w25 >> 12) | (w26 << 20)) & mask;
    out[30] = ((w26 >> 8) | (w27 << 24)) & mask;
    out[31] = w27 >> 4;

    return in + (28 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 29> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 29) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    const auto w26 = LoadInt<uint32_t>(in + 26 * 4);
    const auto w27 = LoadInt<uint32_t>(in + 27 * 4);
    const auto w28 = LoadInt<uint32_t>(in + 28 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 29) | (w1 << 3)) & mask;
    out[2] = ((w1 >> 26) | (w2 << 6)) & mask;
    out[3] = ((w2 >> 23) | (w3 << 9)) & mask;
    out[4] = ((w3 >> 20) | (w4 << 12)) & mask;
    out[5] = ((w4 >> 17) | (w5 << 15)) & mask;
    out[6] = ((w5 >> 14) | (w6 << 18)) & mask;
    out[7] = ((w6 >> 11) | (w7 << 21)) & mask;
    out[8] = ((w7 >> 8) | (w8 << 24)) & mask;
    out[9] = ((w8 >> 5) | (w9 << 27)) & mask;
    out[10] = (w9 >> 2) & mask;
    out[11] = ((w9 >> 31) | (w10 << 1)) & mask;
    out[12] = ((w10 >> 28) | (w11 << 4)) & mask;
    out[13] = ((w11 >> 25) | (w12 << 7)) & mask;
    out[14] = ((w12 >> 22) | (w13 << 10)) & mask;
    out[15] = ((w13 >> 19) | (w14 << 13)) & mask;
    out[16] = ((w14 >> 16) | (w15 << 16)) & mask;
    out[17] = ((w15 >> 13) | (w16 << 19)) & mask;
    out[18] = ((w16 >> 10) | (w17 << 22)) & mask;
    out[19] = ((w17 >> 7) | (w18 << 25)) & mask;
    out[20] = ((w18 >> 4) | (w19 << 28)) & mask;
    out[21] = (w19 >> 1) & mask;
    out[22] = ((w19 >> 30) | (w20 << 2)) & mask;
    out[23] = ((w20 >> 27) | (w21 << 5)) & mask;
    out[24] = ((w21 >> 24) | (w22 << 8)) & mask;
    out[25] = ((w22 >> 21) | (w23 << 11)) & mask;
    out[26] = ((w23 >> 18) | (w24 << 14)) & mask;
    out[27] = ((w24 >> 15) | (w25 << 17)) & mask;
    out[28] = ((w25 >> 12) | (w26 << 20)) & mask;
    out[29] = ((w26 >> 9) | (w27 << 23)) & mask;
    out[30] = ((w27 >> 6) | (w28 << 26)) & mask;
    out[31] = w28 >> 3;

    return in + (29 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 30> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 30) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    const auto w26 = LoadInt<uint32_t>(in + 26 * 4);
    const auto w27 = LoadInt<uint32_t>(in + 27 * 4);
    const auto w28 = LoadInt<uint32_t>(in + 28 * 4);
    const auto w29 = LoadInt<uint32_t>(in + 29 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 30) | (w1 << 2)) & mask;
    out[2] = ((w1 >> 28) | (w2 << 4)) & mask;
    out[3] = ((w2 >> 26) | (w3 << 6)) & mask;
    out[4] = ((w3 >> 24) | (w4 << 8)) & mask;
    out[5] = ((w4 >> 22) | (w5 << 10)) & mask;
    out[6] = ((w5 >> 20) | (w6 << 12)) & mask;
    out[7] = ((w6 >> 18) | (w7 << 14)) & mask;
    out[8] = ((w7 >> 16) | (w8 << 16)) & mask;
    out[9] = ((w8 >> 14) | (w9 << 18)) & mask;
    out[10] = ((w9 >> 12) | (w10 << 20)) & mask;
    out[11] = ((w10 >> 10) | (w11 << 22)) & mask;
    out[12] = ((w11 >> 8) | (w12 << 24)) & mask;
    out[13] = ((w12 >> 6) | (w13 << 26)) & mask;
    out[14] = ((w13 >> 4) | (w14 << 28)) & mask;
    out[15] = w14 >> 2;
    out[16] = (w15) & mask;
    out[17] = ((w15 >> 30) | (w16 << 2)) & mask;
    out[18] = ((w16 >> 28) | (w17 << 4)) & mask;
    out[19] = ((w17 >> 26) | (w18 << 6)) & mask;
    out[20] = ((w18 >> 24) | (w19 << 8)) & mask;
    out[21] = ((w19 >> 22) | (w20 << 10)) & mask;
    out[22] = ((w20 >> 20) | (w21 << 12)) & mask;
    out[23] = ((w21 >> 18) | (w22 << 14)) & mask;
    out[24] = ((w22 >> 16) | (w23 << 16)) & mask;
    out[25] = ((w23 >> 14) | (w24 << 18)) & mask;
    out[26] = ((w24 >> 12) | (w25 << 20)) & mask;
    out[27] = ((w25 >> 10) | (w26 << 22)) & mask;
    out[28] = ((w26 >> 8) | (w27 << 24)) & mask;
    out[29] = ((w27 >> 6) | (w28 << 26)) & mask;
    out[30] = ((w28 >> 4) | (w29 << 28)) & mask;
    out[31] = w29 >> 2;

    return in + (30 * 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint32_t, 31> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t mask = ((uint32_t{1} << 31) - uint32_t{1});

    const auto w0 = LoadInt<uint32_t>(in + 0 * 4);
    const auto w1 = LoadInt<uint32_t>(in + 1 * 4);
    const auto w2 = LoadInt<uint32_t>(in + 2 * 4);
    const auto w3 = LoadInt<uint32_t>(in + 3 * 4);
    const auto w4 = LoadInt<uint32_t>(in + 4 * 4);
    const auto w5 = LoadInt<uint32_t>(in + 5 * 4);
    const auto w6 = LoadInt<uint32_t>(in + 6 * 4);
    const auto w7 = LoadInt<uint32_t>(in + 7 * 4);
    const auto w8 = LoadInt<uint32_t>(in + 8 * 4);
    const auto w9 = LoadInt<uint32_t>(in + 9 * 4);
    const auto w10 = LoadInt<uint32_t>(in + 10 * 4);
    const auto w11 = LoadInt<uint32_t>(in + 11 * 4);
    const auto w12 = LoadInt<uint32_t>(in + 12 * 4);
    const auto w13 = LoadInt<uint32_t>(in + 13 * 4);
    const auto w14 = LoadInt<uint32_t>(in + 14 * 4);
    const auto w15 = LoadInt<uint32_t>(in + 15 * 4);
    const auto w16 = LoadInt<uint32_t>(in + 16 * 4);
    const auto w17 = LoadInt<uint32_t>(in + 17 * 4);
    const auto w18 = LoadInt<uint32_t>(in + 18 * 4);
    const auto w19 = LoadInt<uint32_t>(in + 19 * 4);
    const auto w20 = LoadInt<uint32_t>(in + 20 * 4);
    const auto w21 = LoadInt<uint32_t>(in + 21 * 4);
    const auto w22 = LoadInt<uint32_t>(in + 22 * 4);
    const auto w23 = LoadInt<uint32_t>(in + 23 * 4);
    const auto w24 = LoadInt<uint32_t>(in + 24 * 4);
    const auto w25 = LoadInt<uint32_t>(in + 25 * 4);
    const auto w26 = LoadInt<uint32_t>(in + 26 * 4);
    const auto w27 = LoadInt<uint32_t>(in + 27 * 4);
    const auto w28 = LoadInt<uint32_t>(in + 28 * 4);
    const auto w29 = LoadInt<uint32_t>(in + 29 * 4);
    const auto w30 = LoadInt<uint32_t>(in + 30 * 4);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 31) | (w1 << 1)) & mask;
    out[2] = ((w1 >> 30) | (w2 << 2)) & mask;
    out[3] = ((w2 >> 29) | (w3 << 3)) & mask;
    out[4] = ((w3 >> 28) | (w4 << 4)) & mask;
    out[5] = ((w4 >> 27) | (w5 << 5)) & mask;
    out[6] = ((w5 >> 26) | (w6 << 6)) & mask;
    out[7] = ((w6 >> 25) | (w7 << 7)) & mask;
    out[8] = ((w7 >> 24) | (w8 << 8)) & mask;
    out[9] = ((w8 >> 23) | (w9 << 9)) & mask;
    out[10] = ((w9 >> 22) | (w10 << 10)) & mask;
    out[11] = ((w10 >> 21) | (w11 << 11)) & mask;
    out[12] = ((w11 >> 20) | (w12 << 12)) & mask;
    out[13] = ((w12 >> 19) | (w13 << 13)) & mask;
    out[14] = ((w13 >> 18) | (w14 << 14)) & mask;
    out[15] = ((w14 >> 17) | (w15 << 15)) & mask;
    out[16] = ((w15 >> 16) | (w16 << 16)) & mask;
    out[17] = ((w16 >> 15) | (w17 << 17)) & mask;
    out[18] = ((w17 >> 14) | (w18 << 18)) & mask;
    out[19] = ((w18 >> 13) | (w19 << 19)) & mask;
    out[20] = ((w19 >> 12) | (w20 << 20)) & mask;
    out[21] = ((w20 >> 11) | (w21 << 21)) & mask;
    out[22] = ((w21 >> 10) | (w22 << 22)) & mask;
    out[23] = ((w22 >> 9) | (w23 << 23)) & mask;
    out[24] = ((w23 >> 8) | (w24 << 24)) & mask;
    out[25] = ((w24 >> 7) | (w25 << 25)) & mask;
    out[26] = ((w25 >> 6) | (w26 << 26)) & mask;
    out[27] = ((w26 >> 5) | (w27 << 27)) & mask;
    out[28] = ((w27 >> 4) | (w28 << 28)) & mask;
    out[29] = ((w28 >> 3) | (w29 << 29)) & mask;
    out[30] = ((w29 >> 2) | (w30 << 30)) & mask;
    out[31] = w30 >> 1;

    return in + (31 * 4);
  }
};


template<>
struct ScalarUnpackerForWidth<uint64_t, 1> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 1) - uint64_t{1});

    const auto w0 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 0 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 1) & mask;
    out[2] = (w0 >> 2) & mask;
    out[3] = (w0 >> 3) & mask;
    out[4] = (w0 >> 4) & mask;
    out[5] = (w0 >> 5) & mask;
    out[6] = (w0 >> 6) & mask;
    out[7] = (w0 >> 7) & mask;
    out[8] = (w0 >> 8) & mask;
    out[9] = (w0 >> 9) & mask;
    out[10] = (w0 >> 10) & mask;
    out[11] = (w0 >> 11) & mask;
    out[12] = (w0 >> 12) & mask;
    out[13] = (w0 >> 13) & mask;
    out[14] = (w0 >> 14) & mask;
    out[15] = (w0 >> 15) & mask;
    out[16] = (w0 >> 16) & mask;
    out[17] = (w0 >> 17) & mask;
    out[18] = (w0 >> 18) & mask;
    out[19] = (w0 >> 19) & mask;
    out[20] = (w0 >> 20) & mask;
    out[21] = (w0 >> 21) & mask;
    out[22] = (w0 >> 22) & mask;
    out[23] = (w0 >> 23) & mask;
    out[24] = (w0 >> 24) & mask;
    out[25] = (w0 >> 25) & mask;
    out[26] = (w0 >> 26) & mask;
    out[27] = (w0 >> 27) & mask;
    out[28] = (w0 >> 28) & mask;
    out[29] = (w0 >> 29) & mask;
    out[30] = (w0 >> 30) & mask;
    out[31] = (w0 >> 31) & mask;

    return in + (0 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 2> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 2) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 2) & mask;
    out[2] = (w0 >> 4) & mask;
    out[3] = (w0 >> 6) & mask;
    out[4] = (w0 >> 8) & mask;
    out[5] = (w0 >> 10) & mask;
    out[6] = (w0 >> 12) & mask;
    out[7] = (w0 >> 14) & mask;
    out[8] = (w0 >> 16) & mask;
    out[9] = (w0 >> 18) & mask;
    out[10] = (w0 >> 20) & mask;
    out[11] = (w0 >> 22) & mask;
    out[12] = (w0 >> 24) & mask;
    out[13] = (w0 >> 26) & mask;
    out[14] = (w0 >> 28) & mask;
    out[15] = (w0 >> 30) & mask;
    out[16] = (w0 >> 32) & mask;
    out[17] = (w0 >> 34) & mask;
    out[18] = (w0 >> 36) & mask;
    out[19] = (w0 >> 38) & mask;
    out[20] = (w0 >> 40) & mask;
    out[21] = (w0 >> 42) & mask;
    out[22] = (w0 >> 44) & mask;
    out[23] = (w0 >> 46) & mask;
    out[24] = (w0 >> 48) & mask;
    out[25] = (w0 >> 50) & mask;
    out[26] = (w0 >> 52) & mask;
    out[27] = (w0 >> 54) & mask;
    out[28] = (w0 >> 56) & mask;
    out[29] = (w0 >> 58) & mask;
    out[30] = (w0 >> 60) & mask;
    out[31] = w0 >> 62;

    return in + (1 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 3> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 3) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 1 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 3) & mask;
    out[2] = (w0 >> 6) & mask;
    out[3] = (w0 >> 9) & mask;
    out[4] = (w0 >> 12) & mask;
    out[5] = (w0 >> 15) & mask;
    out[6] = (w0 >> 18) & mask;
    out[7] = (w0 >> 21) & mask;
    out[8] = (w0 >> 24) & mask;
    out[9] = (w0 >> 27) & mask;
    out[10] = (w0 >> 30) & mask;
    out[11] = (w0 >> 33) & mask;
    out[12] = (w0 >> 36) & mask;
    out[13] = (w0 >> 39) & mask;
    out[14] = (w0 >> 42) & mask;
    out[15] = (w0 >> 45) & mask;
    out[16] = (w0 >> 48) & mask;
    out[17] = (w0 >> 51) & mask;
    out[18] = (w0 >> 54) & mask;
    out[19] = (w0 >> 57) & mask;
    out[20] = (w0 >> 60) & mask;
    out[21] = ((w0 >> 63) | (w1 << 1)) & mask;
    out[22] = (w1 >> 2) & mask;
    out[23] = (w1 >> 5) & mask;
    out[24] = (w1 >> 8) & mask;
    out[25] = (w1 >> 11) & mask;
    out[26] = (w1 >> 14) & mask;
    out[27] = (w1 >> 17) & mask;
    out[28] = (w1 >> 20) & mask;
    out[29] = (w1 >> 23) & mask;
    out[30] = (w1 >> 26) & mask;
    out[31] = (w1 >> 29) & mask;

    return in + (1 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 4> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 4) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 4) & mask;
    out[2] = (w0 >> 8) & mask;
    out[3] = (w0 >> 12) & mask;
    out[4] = (w0 >> 16) & mask;
    out[5] = (w0 >> 20) & mask;
    out[6] = (w0 >> 24) & mask;
    out[7] = (w0 >> 28) & mask;
    out[8] = (w0 >> 32) & mask;
    out[9] = (w0 >> 36) & mask;
    out[10] = (w0 >> 40) & mask;
    out[11] = (w0 >> 44) & mask;
    out[12] = (w0 >> 48) & mask;
    out[13] = (w0 >> 52) & mask;
    out[14] = (w0 >> 56) & mask;
    out[15] = w0 >> 60;
    out[16] = (w1) & mask;
    out[17] = (w1 >> 4) & mask;
    out[18] = (w1 >> 8) & mask;
    out[19] = (w1 >> 12) & mask;
    out[20] = (w1 >> 16) & mask;
    out[21] = (w1 >> 20) & mask;
    out[22] = (w1 >> 24) & mask;
    out[23] = (w1 >> 28) & mask;
    out[24] = (w1 >> 32) & mask;
    out[25] = (w1 >> 36) & mask;
    out[26] = (w1 >> 40) & mask;
    out[27] = (w1 >> 44) & mask;
    out[28] = (w1 >> 48) & mask;
    out[29] = (w1 >> 52) & mask;
    out[30] = (w1 >> 56) & mask;
    out[31] = w1 >> 60;

    return in + (2 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 5> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 5) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 2 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 5) & mask;
    out[2] = (w0 >> 10) & mask;
    out[3] = (w0 >> 15) & mask;
    out[4] = (w0 >> 20) & mask;
    out[5] = (w0 >> 25) & mask;
    out[6] = (w0 >> 30) & mask;
    out[7] = (w0 >> 35) & mask;
    out[8] = (w0 >> 40) & mask;
    out[9] = (w0 >> 45) & mask;
    out[10] = (w0 >> 50) & mask;
    out[11] = (w0 >> 55) & mask;
    out[12] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[13] = (w1 >> 1) & mask;
    out[14] = (w1 >> 6) & mask;
    out[15] = (w1 >> 11) & mask;
    out[16] = (w1 >> 16) & mask;
    out[17] = (w1 >> 21) & mask;
    out[18] = (w1 >> 26) & mask;
    out[19] = (w1 >> 31) & mask;
    out[20] = (w1 >> 36) & mask;
    out[21] = (w1 >> 41) & mask;
    out[22] = (w1 >> 46) & mask;
    out[23] = (w1 >> 51) & mask;
    out[24] = (w1 >> 56) & mask;
    out[25] = ((w1 >> 61) | (w2 << 3)) & mask;
    out[26] = (w2 >> 2) & mask;
    out[27] = (w2 >> 7) & mask;
    out[28] = (w2 >> 12) & mask;
    out[29] = (w2 >> 17) & mask;
    out[30] = (w2 >> 22) & mask;
    out[31] = (w2 >> 27) & mask;

    return in + (2 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 6> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 6) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 6) & mask;
    out[2] = (w0 >> 12) & mask;
    out[3] = (w0 >> 18) & mask;
    out[4] = (w0 >> 24) & mask;
    out[5] = (w0 >> 30) & mask;
    out[6] = (w0 >> 36) & mask;
    out[7] = (w0 >> 42) & mask;
    out[8] = (w0 >> 48) & mask;
    out[9] = (w0 >> 54) & mask;
    out[10] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[11] = (w1 >> 2) & mask;
    out[12] = (w1 >> 8) & mask;
    out[13] = (w1 >> 14) & mask;
    out[14] = (w1 >> 20) & mask;
    out[15] = (w1 >> 26) & mask;
    out[16] = (w1 >> 32) & mask;
    out[17] = (w1 >> 38) & mask;
    out[18] = (w1 >> 44) & mask;
    out[19] = (w1 >> 50) & mask;
    out[20] = (w1 >> 56) & mask;
    out[21] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[22] = (w2 >> 4) & mask;
    out[23] = (w2 >> 10) & mask;
    out[24] = (w2 >> 16) & mask;
    out[25] = (w2 >> 22) & mask;
    out[26] = (w2 >> 28) & mask;
    out[27] = (w2 >> 34) & mask;
    out[28] = (w2 >> 40) & mask;
    out[29] = (w2 >> 46) & mask;
    out[30] = (w2 >> 52) & mask;
    out[31] = w2 >> 58;

    return in + (3 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 7> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 7) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 3 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 7) & mask;
    out[2] = (w0 >> 14) & mask;
    out[3] = (w0 >> 21) & mask;
    out[4] = (w0 >> 28) & mask;
    out[5] = (w0 >> 35) & mask;
    out[6] = (w0 >> 42) & mask;
    out[7] = (w0 >> 49) & mask;
    out[8] = (w0 >> 56) & mask;
    out[9] = ((w0 >> 63) | (w1 << 1)) & mask;
    out[10] = (w1 >> 6) & mask;
    out[11] = (w1 >> 13) & mask;
    out[12] = (w1 >> 20) & mask;
    out[13] = (w1 >> 27) & mask;
    out[14] = (w1 >> 34) & mask;
    out[15] = (w1 >> 41) & mask;
    out[16] = (w1 >> 48) & mask;
    out[17] = (w1 >> 55) & mask;
    out[18] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[19] = (w2 >> 5) & mask;
    out[20] = (w2 >> 12) & mask;
    out[21] = (w2 >> 19) & mask;
    out[22] = (w2 >> 26) & mask;
    out[23] = (w2 >> 33) & mask;
    out[24] = (w2 >> 40) & mask;
    out[25] = (w2 >> 47) & mask;
    out[26] = (w2 >> 54) & mask;
    out[27] = ((w2 >> 61) | (w3 << 3)) & mask;
    out[28] = (w3 >> 4) & mask;
    out[29] = (w3 >> 11) & mask;
    out[30] = (w3 >> 18) & mask;
    out[31] = (w3 >> 25) & mask;

    return in + (3 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 8> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 8) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 8) & mask;
    out[2] = (w0 >> 16) & mask;
    out[3] = (w0 >> 24) & mask;
    out[4] = (w0 >> 32) & mask;
    out[5] = (w0 >> 40) & mask;
    out[6] = (w0 >> 48) & mask;
    out[7] = w0 >> 56;
    out[8] = (w1) & mask;
    out[9] = (w1 >> 8) & mask;
    out[10] = (w1 >> 16) & mask;
    out[11] = (w1 >> 24) & mask;
    out[12] = (w1 >> 32) & mask;
    out[13] = (w1 >> 40) & mask;
    out[14] = (w1 >> 48) & mask;
    out[15] = w1 >> 56;
    out[16] = (w2) & mask;
    out[17] = (w2 >> 8) & mask;
    out[18] = (w2 >> 16) & mask;
    out[19] = (w2 >> 24) & mask;
    out[20] = (w2 >> 32) & mask;
    out[21] = (w2 >> 40) & mask;
    out[22] = (w2 >> 48) & mask;
    out[23] = w2 >> 56;
    out[24] = (w3) & mask;
    out[25] = (w3 >> 8) & mask;
    out[26] = (w3 >> 16) & mask;
    out[27] = (w3 >> 24) & mask;
    out[28] = (w3 >> 32) & mask;
    out[29] = (w3 >> 40) & mask;
    out[30] = (w3 >> 48) & mask;
    out[31] = w3 >> 56;

    return in + (4 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 9> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 9) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 4 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 9) & mask;
    out[2] = (w0 >> 18) & mask;
    out[3] = (w0 >> 27) & mask;
    out[4] = (w0 >> 36) & mask;
    out[5] = (w0 >> 45) & mask;
    out[6] = (w0 >> 54) & mask;
    out[7] = ((w0 >> 63) | (w1 << 1)) & mask;
    out[8] = (w1 >> 8) & mask;
    out[9] = (w1 >> 17) & mask;
    out[10] = (w1 >> 26) & mask;
    out[11] = (w1 >> 35) & mask;
    out[12] = (w1 >> 44) & mask;
    out[13] = (w1 >> 53) & mask;
    out[14] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[15] = (w2 >> 7) & mask;
    out[16] = (w2 >> 16) & mask;
    out[17] = (w2 >> 25) & mask;
    out[18] = (w2 >> 34) & mask;
    out[19] = (w2 >> 43) & mask;
    out[20] = (w2 >> 52) & mask;
    out[21] = ((w2 >> 61) | (w3 << 3)) & mask;
    out[22] = (w3 >> 6) & mask;
    out[23] = (w3 >> 15) & mask;
    out[24] = (w3 >> 24) & mask;
    out[25] = (w3 >> 33) & mask;
    out[26] = (w3 >> 42) & mask;
    out[27] = (w3 >> 51) & mask;
    out[28] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[29] = (w4 >> 5) & mask;
    out[30] = (w4 >> 14) & mask;
    out[31] = (w4 >> 23) & mask;

    return in + (4 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 10> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 10) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 10) & mask;
    out[2] = (w0 >> 20) & mask;
    out[3] = (w0 >> 30) & mask;
    out[4] = (w0 >> 40) & mask;
    out[5] = (w0 >> 50) & mask;
    out[6] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[7] = (w1 >> 6) & mask;
    out[8] = (w1 >> 16) & mask;
    out[9] = (w1 >> 26) & mask;
    out[10] = (w1 >> 36) & mask;
    out[11] = (w1 >> 46) & mask;
    out[12] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[13] = (w2 >> 2) & mask;
    out[14] = (w2 >> 12) & mask;
    out[15] = (w2 >> 22) & mask;
    out[16] = (w2 >> 32) & mask;
    out[17] = (w2 >> 42) & mask;
    out[18] = (w2 >> 52) & mask;
    out[19] = ((w2 >> 62) | (w3 << 2)) & mask;
    out[20] = (w3 >> 8) & mask;
    out[21] = (w3 >> 18) & mask;
    out[22] = (w3 >> 28) & mask;
    out[23] = (w3 >> 38) & mask;
    out[24] = (w3 >> 48) & mask;
    out[25] = ((w3 >> 58) | (w4 << 6)) & mask;
    out[26] = (w4 >> 4) & mask;
    out[27] = (w4 >> 14) & mask;
    out[28] = (w4 >> 24) & mask;
    out[29] = (w4 >> 34) & mask;
    out[30] = (w4 >> 44) & mask;
    out[31] = w4 >> 54;

    return in + (5 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 11> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 11) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 5 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 11) & mask;
    out[2] = (w0 >> 22) & mask;
    out[3] = (w0 >> 33) & mask;
    out[4] = (w0 >> 44) & mask;
    out[5] = ((w0 >> 55) | (w1 << 9)) & mask;
    out[6] = (w1 >> 2) & mask;
    out[7] = (w1 >> 13) & mask;
    out[8] = (w1 >> 24) & mask;
    out[9] = (w1 >> 35) & mask;
    out[10] = (w1 >> 46) & mask;
    out[11] = ((w1 >> 57) | (w2 << 7)) & mask;
    out[12] = (w2 >> 4) & mask;
    out[13] = (w2 >> 15) & mask;
    out[14] = (w2 >> 26) & mask;
    out[15] = (w2 >> 37) & mask;
    out[16] = (w2 >> 48) & mask;
    out[17] = ((w2 >> 59) | (w3 << 5)) & mask;
    out[18] = (w3 >> 6) & mask;
    out[19] = (w3 >> 17) & mask;
    out[20] = (w3 >> 28) & mask;
    out[21] = (w3 >> 39) & mask;
    out[22] = (w3 >> 50) & mask;
    out[23] = ((w3 >> 61) | (w4 << 3)) & mask;
    out[24] = (w4 >> 8) & mask;
    out[25] = (w4 >> 19) & mask;
    out[26] = (w4 >> 30) & mask;
    out[27] = (w4 >> 41) & mask;
    out[28] = (w4 >> 52) & mask;
    out[29] = ((w4 >> 63) | (w5 << 1)) & mask;
    out[30] = (w5 >> 10) & mask;
    out[31] = (w5 >> 21) & mask;

    return in + (5 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 12> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 12) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 12) & mask;
    out[2] = (w0 >> 24) & mask;
    out[3] = (w0 >> 36) & mask;
    out[4] = (w0 >> 48) & mask;
    out[5] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[6] = (w1 >> 8) & mask;
    out[7] = (w1 >> 20) & mask;
    out[8] = (w1 >> 32) & mask;
    out[9] = (w1 >> 44) & mask;
    out[10] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[11] = (w2 >> 4) & mask;
    out[12] = (w2 >> 16) & mask;
    out[13] = (w2 >> 28) & mask;
    out[14] = (w2 >> 40) & mask;
    out[15] = w2 >> 52;
    out[16] = (w3) & mask;
    out[17] = (w3 >> 12) & mask;
    out[18] = (w3 >> 24) & mask;
    out[19] = (w3 >> 36) & mask;
    out[20] = (w3 >> 48) & mask;
    out[21] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[22] = (w4 >> 8) & mask;
    out[23] = (w4 >> 20) & mask;
    out[24] = (w4 >> 32) & mask;
    out[25] = (w4 >> 44) & mask;
    out[26] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[27] = (w5 >> 4) & mask;
    out[28] = (w5 >> 16) & mask;
    out[29] = (w5 >> 28) & mask;
    out[30] = (w5 >> 40) & mask;
    out[31] = w5 >> 52;

    return in + (6 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 13> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 13) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 6 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 13) & mask;
    out[2] = (w0 >> 26) & mask;
    out[3] = (w0 >> 39) & mask;
    out[4] = ((w0 >> 52) | (w1 << 12)) & mask;
    out[5] = (w1 >> 1) & mask;
    out[6] = (w1 >> 14) & mask;
    out[7] = (w1 >> 27) & mask;
    out[8] = (w1 >> 40) & mask;
    out[9] = ((w1 >> 53) | (w2 << 11)) & mask;
    out[10] = (w2 >> 2) & mask;
    out[11] = (w2 >> 15) & mask;
    out[12] = (w2 >> 28) & mask;
    out[13] = (w2 >> 41) & mask;
    out[14] = ((w2 >> 54) | (w3 << 10)) & mask;
    out[15] = (w3 >> 3) & mask;
    out[16] = (w3 >> 16) & mask;
    out[17] = (w3 >> 29) & mask;
    out[18] = (w3 >> 42) & mask;
    out[19] = ((w3 >> 55) | (w4 << 9)) & mask;
    out[20] = (w4 >> 4) & mask;
    out[21] = (w4 >> 17) & mask;
    out[22] = (w4 >> 30) & mask;
    out[23] = (w4 >> 43) & mask;
    out[24] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[25] = (w5 >> 5) & mask;
    out[26] = (w5 >> 18) & mask;
    out[27] = (w5 >> 31) & mask;
    out[28] = (w5 >> 44) & mask;
    out[29] = ((w5 >> 57) | (w6 << 7)) & mask;
    out[30] = (w6 >> 6) & mask;
    out[31] = (w6 >> 19) & mask;

    return in + (6 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 14> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 14) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 14) & mask;
    out[2] = (w0 >> 28) & mask;
    out[3] = (w0 >> 42) & mask;
    out[4] = ((w0 >> 56) | (w1 << 8)) & mask;
    out[5] = (w1 >> 6) & mask;
    out[6] = (w1 >> 20) & mask;
    out[7] = (w1 >> 34) & mask;
    out[8] = (w1 >> 48) & mask;
    out[9] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[10] = (w2 >> 12) & mask;
    out[11] = (w2 >> 26) & mask;
    out[12] = (w2 >> 40) & mask;
    out[13] = ((w2 >> 54) | (w3 << 10)) & mask;
    out[14] = (w3 >> 4) & mask;
    out[15] = (w3 >> 18) & mask;
    out[16] = (w3 >> 32) & mask;
    out[17] = (w3 >> 46) & mask;
    out[18] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[19] = (w4 >> 10) & mask;
    out[20] = (w4 >> 24) & mask;
    out[21] = (w4 >> 38) & mask;
    out[22] = ((w4 >> 52) | (w5 << 12)) & mask;
    out[23] = (w5 >> 2) & mask;
    out[24] = (w5 >> 16) & mask;
    out[25] = (w5 >> 30) & mask;
    out[26] = (w5 >> 44) & mask;
    out[27] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[28] = (w6 >> 8) & mask;
    out[29] = (w6 >> 22) & mask;
    out[30] = (w6 >> 36) & mask;
    out[31] = w6 >> 50;

    return in + (7 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 15> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 15) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 7 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 15) & mask;
    out[2] = (w0 >> 30) & mask;
    out[3] = (w0 >> 45) & mask;
    out[4] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[5] = (w1 >> 11) & mask;
    out[6] = (w1 >> 26) & mask;
    out[7] = (w1 >> 41) & mask;
    out[8] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[9] = (w2 >> 7) & mask;
    out[10] = (w2 >> 22) & mask;
    out[11] = (w2 >> 37) & mask;
    out[12] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[13] = (w3 >> 3) & mask;
    out[14] = (w3 >> 18) & mask;
    out[15] = (w3 >> 33) & mask;
    out[16] = (w3 >> 48) & mask;
    out[17] = ((w3 >> 63) | (w4 << 1)) & mask;
    out[18] = (w4 >> 14) & mask;
    out[19] = (w4 >> 29) & mask;
    out[20] = (w4 >> 44) & mask;
    out[21] = ((w4 >> 59) | (w5 << 5)) & mask;
    out[22] = (w5 >> 10) & mask;
    out[23] = (w5 >> 25) & mask;
    out[24] = (w5 >> 40) & mask;
    out[25] = ((w5 >> 55) | (w6 << 9)) & mask;
    out[26] = (w6 >> 6) & mask;
    out[27] = (w6 >> 21) & mask;
    out[28] = (w6 >> 36) & mask;
    out[29] = ((w6 >> 51) | (w7 << 13)) & mask;
    out[30] = (w7 >> 2) & mask;
    out[31] = (w7 >> 17) & mask;

    return in + (7 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 16> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 16) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 16) & mask;
    out[2] = (w0 >> 32) & mask;
    out[3] = w0 >> 48;
    out[4] = (w1) & mask;
    out[5] = (w1 >> 16) & mask;
    out[6] = (w1 >> 32) & mask;
    out[7] = w1 >> 48;
    out[8] = (w2) & mask;
    out[9] = (w2 >> 16) & mask;
    out[10] = (w2 >> 32) & mask;
    out[11] = w2 >> 48;
    out[12] = (w3) & mask;
    out[13] = (w3 >> 16) & mask;
    out[14] = (w3 >> 32) & mask;
    out[15] = w3 >> 48;
    out[16] = (w4) & mask;
    out[17] = (w4 >> 16) & mask;
    out[18] = (w4 >> 32) & mask;
    out[19] = w4 >> 48;
    out[20] = (w5) & mask;
    out[21] = (w5 >> 16) & mask;
    out[22] = (w5 >> 32) & mask;
    out[23] = w5 >> 48;
    out[24] = (w6) & mask;
    out[25] = (w6 >> 16) & mask;
    out[26] = (w6 >> 32) & mask;
    out[27] = w6 >> 48;
    out[28] = (w7) & mask;
    out[29] = (w7 >> 16) & mask;
    out[30] = (w7 >> 32) & mask;
    out[31] = w7 >> 48;

    return in + (8 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 17> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 17) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 8 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 17) & mask;
    out[2] = (w0 >> 34) & mask;
    out[3] = ((w0 >> 51) | (w1 << 13)) & mask;
    out[4] = (w1 >> 4) & mask;
    out[5] = (w1 >> 21) & mask;
    out[6] = (w1 >> 38) & mask;
    out[7] = ((w1 >> 55) | (w2 << 9)) & mask;
    out[8] = (w2 >> 8) & mask;
    out[9] = (w2 >> 25) & mask;
    out[10] = (w2 >> 42) & mask;
    out[11] = ((w2 >> 59) | (w3 << 5)) & mask;
    out[12] = (w3 >> 12) & mask;
    out[13] = (w3 >> 29) & mask;
    out[14] = (w3 >> 46) & mask;
    out[15] = ((w3 >> 63) | (w4 << 1)) & mask;
    out[16] = (w4 >> 16) & mask;
    out[17] = (w4 >> 33) & mask;
    out[18] = ((w4 >> 50) | (w5 << 14)) & mask;
    out[19] = (w5 >> 3) & mask;
    out[20] = (w5 >> 20) & mask;
    out[21] = (w5 >> 37) & mask;
    out[22] = ((w5 >> 54) | (w6 << 10)) & mask;
    out[23] = (w6 >> 7) & mask;
    out[24] = (w6 >> 24) & mask;
    out[25] = (w6 >> 41) & mask;
    out[26] = ((w6 >> 58) | (w7 << 6)) & mask;
    out[27] = (w7 >> 11) & mask;
    out[28] = (w7 >> 28) & mask;
    out[29] = (w7 >> 45) & mask;
    out[30] = ((w7 >> 62) | (w8 << 2)) & mask;
    out[31] = (w8 >> 15) & mask;

    return in + (8 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 18> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 18) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 18) & mask;
    out[2] = (w0 >> 36) & mask;
    out[3] = ((w0 >> 54) | (w1 << 10)) & mask;
    out[4] = (w1 >> 8) & mask;
    out[5] = (w1 >> 26) & mask;
    out[6] = (w1 >> 44) & mask;
    out[7] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[8] = (w2 >> 16) & mask;
    out[9] = (w2 >> 34) & mask;
    out[10] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[11] = (w3 >> 6) & mask;
    out[12] = (w3 >> 24) & mask;
    out[13] = (w3 >> 42) & mask;
    out[14] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[15] = (w4 >> 14) & mask;
    out[16] = (w4 >> 32) & mask;
    out[17] = ((w4 >> 50) | (w5 << 14)) & mask;
    out[18] = (w5 >> 4) & mask;
    out[19] = (w5 >> 22) & mask;
    out[20] = (w5 >> 40) & mask;
    out[21] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[22] = (w6 >> 12) & mask;
    out[23] = (w6 >> 30) & mask;
    out[24] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[25] = (w7 >> 2) & mask;
    out[26] = (w7 >> 20) & mask;
    out[27] = (w7 >> 38) & mask;
    out[28] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[29] = (w8 >> 10) & mask;
    out[30] = (w8 >> 28) & mask;
    out[31] = w8 >> 46;

    return in + (9 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 19> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 19) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 9 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 19) & mask;
    out[2] = (w0 >> 38) & mask;
    out[3] = ((w0 >> 57) | (w1 << 7)) & mask;
    out[4] = (w1 >> 12) & mask;
    out[5] = (w1 >> 31) & mask;
    out[6] = ((w1 >> 50) | (w2 << 14)) & mask;
    out[7] = (w2 >> 5) & mask;
    out[8] = (w2 >> 24) & mask;
    out[9] = (w2 >> 43) & mask;
    out[10] = ((w2 >> 62) | (w3 << 2)) & mask;
    out[11] = (w3 >> 17) & mask;
    out[12] = (w3 >> 36) & mask;
    out[13] = ((w3 >> 55) | (w4 << 9)) & mask;
    out[14] = (w4 >> 10) & mask;
    out[15] = (w4 >> 29) & mask;
    out[16] = ((w4 >> 48) | (w5 << 16)) & mask;
    out[17] = (w5 >> 3) & mask;
    out[18] = (w5 >> 22) & mask;
    out[19] = (w5 >> 41) & mask;
    out[20] = ((w5 >> 60) | (w6 << 4)) & mask;
    out[21] = (w6 >> 15) & mask;
    out[22] = (w6 >> 34) & mask;
    out[23] = ((w6 >> 53) | (w7 << 11)) & mask;
    out[24] = (w7 >> 8) & mask;
    out[25] = (w7 >> 27) & mask;
    out[26] = ((w7 >> 46) | (w8 << 18)) & mask;
    out[27] = (w8 >> 1) & mask;
    out[28] = (w8 >> 20) & mask;
    out[29] = (w8 >> 39) & mask;
    out[30] = ((w8 >> 58) | (w9 << 6)) & mask;
    out[31] = (w9 >> 13) & mask;

    return in + (9 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 20> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 20) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 20) & mask;
    out[2] = (w0 >> 40) & mask;
    out[3] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[4] = (w1 >> 16) & mask;
    out[5] = (w1 >> 36) & mask;
    out[6] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[7] = (w2 >> 12) & mask;
    out[8] = (w2 >> 32) & mask;
    out[9] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[10] = (w3 >> 8) & mask;
    out[11] = (w3 >> 28) & mask;
    out[12] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[13] = (w4 >> 4) & mask;
    out[14] = (w4 >> 24) & mask;
    out[15] = w4 >> 44;
    out[16] = (w5) & mask;
    out[17] = (w5 >> 20) & mask;
    out[18] = (w5 >> 40) & mask;
    out[19] = ((w5 >> 60) | (w6 << 4)) & mask;
    out[20] = (w6 >> 16) & mask;
    out[21] = (w6 >> 36) & mask;
    out[22] = ((w6 >> 56) | (w7 << 8)) & mask;
    out[23] = (w7 >> 12) & mask;
    out[24] = (w7 >> 32) & mask;
    out[25] = ((w7 >> 52) | (w8 << 12)) & mask;
    out[26] = (w8 >> 8) & mask;
    out[27] = (w8 >> 28) & mask;
    out[28] = ((w8 >> 48) | (w9 << 16)) & mask;
    out[29] = (w9 >> 4) & mask;
    out[30] = (w9 >> 24) & mask;
    out[31] = w9 >> 44;

    return in + (10 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 21> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 21) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 10 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 21) & mask;
    out[2] = (w0 >> 42) & mask;
    out[3] = ((w0 >> 63) | (w1 << 1)) & mask;
    out[4] = (w1 >> 20) & mask;
    out[5] = (w1 >> 41) & mask;
    out[6] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[7] = (w2 >> 19) & mask;
    out[8] = (w2 >> 40) & mask;
    out[9] = ((w2 >> 61) | (w3 << 3)) & mask;
    out[10] = (w3 >> 18) & mask;
    out[11] = (w3 >> 39) & mask;
    out[12] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[13] = (w4 >> 17) & mask;
    out[14] = (w4 >> 38) & mask;
    out[15] = ((w4 >> 59) | (w5 << 5)) & mask;
    out[16] = (w5 >> 16) & mask;
    out[17] = (w5 >> 37) & mask;
    out[18] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[19] = (w6 >> 15) & mask;
    out[20] = (w6 >> 36) & mask;
    out[21] = ((w6 >> 57) | (w7 << 7)) & mask;
    out[22] = (w7 >> 14) & mask;
    out[23] = (w7 >> 35) & mask;
    out[24] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[25] = (w8 >> 13) & mask;
    out[26] = (w8 >> 34) & mask;
    out[27] = ((w8 >> 55) | (w9 << 9)) & mask;
    out[28] = (w9 >> 12) & mask;
    out[29] = (w9 >> 33) & mask;
    out[30] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[31] = (w10 >> 11) & mask;

    return in + (10 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 22> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 22) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 22) & mask;
    out[2] = ((w0 >> 44) | (w1 << 20)) & mask;
    out[3] = (w1 >> 2) & mask;
    out[4] = (w1 >> 24) & mask;
    out[5] = ((w1 >> 46) | (w2 << 18)) & mask;
    out[6] = (w2 >> 4) & mask;
    out[7] = (w2 >> 26) & mask;
    out[8] = ((w2 >> 48) | (w3 << 16)) & mask;
    out[9] = (w3 >> 6) & mask;
    out[10] = (w3 >> 28) & mask;
    out[11] = ((w3 >> 50) | (w4 << 14)) & mask;
    out[12] = (w4 >> 8) & mask;
    out[13] = (w4 >> 30) & mask;
    out[14] = ((w4 >> 52) | (w5 << 12)) & mask;
    out[15] = (w5 >> 10) & mask;
    out[16] = (w5 >> 32) & mask;
    out[17] = ((w5 >> 54) | (w6 << 10)) & mask;
    out[18] = (w6 >> 12) & mask;
    out[19] = (w6 >> 34) & mask;
    out[20] = ((w6 >> 56) | (w7 << 8)) & mask;
    out[21] = (w7 >> 14) & mask;
    out[22] = (w7 >> 36) & mask;
    out[23] = ((w7 >> 58) | (w8 << 6)) & mask;
    out[24] = (w8 >> 16) & mask;
    out[25] = (w8 >> 38) & mask;
    out[26] = ((w8 >> 60) | (w9 << 4)) & mask;
    out[27] = (w9 >> 18) & mask;
    out[28] = (w9 >> 40) & mask;
    out[29] = ((w9 >> 62) | (w10 << 2)) & mask;
    out[30] = (w10 >> 20) & mask;
    out[31] = w10 >> 42;

    return in + (11 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 23> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 23) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 11 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 23) & mask;
    out[2] = ((w0 >> 46) | (w1 << 18)) & mask;
    out[3] = (w1 >> 5) & mask;
    out[4] = (w1 >> 28) & mask;
    out[5] = ((w1 >> 51) | (w2 << 13)) & mask;
    out[6] = (w2 >> 10) & mask;
    out[7] = (w2 >> 33) & mask;
    out[8] = ((w2 >> 56) | (w3 << 8)) & mask;
    out[9] = (w3 >> 15) & mask;
    out[10] = (w3 >> 38) & mask;
    out[11] = ((w3 >> 61) | (w4 << 3)) & mask;
    out[12] = (w4 >> 20) & mask;
    out[13] = ((w4 >> 43) | (w5 << 21)) & mask;
    out[14] = (w5 >> 2) & mask;
    out[15] = (w5 >> 25) & mask;
    out[16] = ((w5 >> 48) | (w6 << 16)) & mask;
    out[17] = (w6 >> 7) & mask;
    out[18] = (w6 >> 30) & mask;
    out[19] = ((w6 >> 53) | (w7 << 11)) & mask;
    out[20] = (w7 >> 12) & mask;
    out[21] = (w7 >> 35) & mask;
    out[22] = ((w7 >> 58) | (w8 << 6)) & mask;
    out[23] = (w8 >> 17) & mask;
    out[24] = (w8 >> 40) & mask;
    out[25] = ((w8 >> 63) | (w9 << 1)) & mask;
    out[26] = (w9 >> 22) & mask;
    out[27] = ((w9 >> 45) | (w10 << 19)) & mask;
    out[28] = (w10 >> 4) & mask;
    out[29] = (w10 >> 27) & mask;
    out[30] = ((w10 >> 50) | (w11 << 14)) & mask;
    out[31] = (w11 >> 9) & mask;

    return in + (11 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 24> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 24) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 24) & mask;
    out[2] = ((w0 >> 48) | (w1 << 16)) & mask;
    out[3] = (w1 >> 8) & mask;
    out[4] = (w1 >> 32) & mask;
    out[5] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[6] = (w2 >> 16) & mask;
    out[7] = w2 >> 40;
    out[8] = (w3) & mask;
    out[9] = (w3 >> 24) & mask;
    out[10] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[11] = (w4 >> 8) & mask;
    out[12] = (w4 >> 32) & mask;
    out[13] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[14] = (w5 >> 16) & mask;
    out[15] = w5 >> 40;
    out[16] = (w6) & mask;
    out[17] = (w6 >> 24) & mask;
    out[18] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[19] = (w7 >> 8) & mask;
    out[20] = (w7 >> 32) & mask;
    out[21] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[22] = (w8 >> 16) & mask;
    out[23] = w8 >> 40;
    out[24] = (w9) & mask;
    out[25] = (w9 >> 24) & mask;
    out[26] = ((w9 >> 48) | (w10 << 16)) & mask;
    out[27] = (w10 >> 8) & mask;
    out[28] = (w10 >> 32) & mask;
    out[29] = ((w10 >> 56) | (w11 << 8)) & mask;
    out[30] = (w11 >> 16) & mask;
    out[31] = w11 >> 40;

    return in + (12 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 25> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 25) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 12 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 25) & mask;
    out[2] = ((w0 >> 50) | (w1 << 14)) & mask;
    out[3] = (w1 >> 11) & mask;
    out[4] = (w1 >> 36) & mask;
    out[5] = ((w1 >> 61) | (w2 << 3)) & mask;
    out[6] = (w2 >> 22) & mask;
    out[7] = ((w2 >> 47) | (w3 << 17)) & mask;
    out[8] = (w3 >> 8) & mask;
    out[9] = (w3 >> 33) & mask;
    out[10] = ((w3 >> 58) | (w4 << 6)) & mask;
    out[11] = (w4 >> 19) & mask;
    out[12] = ((w4 >> 44) | (w5 << 20)) & mask;
    out[13] = (w5 >> 5) & mask;
    out[14] = (w5 >> 30) & mask;
    out[15] = ((w5 >> 55) | (w6 << 9)) & mask;
    out[16] = (w6 >> 16) & mask;
    out[17] = ((w6 >> 41) | (w7 << 23)) & mask;
    out[18] = (w7 >> 2) & mask;
    out[19] = (w7 >> 27) & mask;
    out[20] = ((w7 >> 52) | (w8 << 12)) & mask;
    out[21] = (w8 >> 13) & mask;
    out[22] = (w8 >> 38) & mask;
    out[23] = ((w8 >> 63) | (w9 << 1)) & mask;
    out[24] = (w9 >> 24) & mask;
    out[25] = ((w9 >> 49) | (w10 << 15)) & mask;
    out[26] = (w10 >> 10) & mask;
    out[27] = (w10 >> 35) & mask;
    out[28] = ((w10 >> 60) | (w11 << 4)) & mask;
    out[29] = (w11 >> 21) & mask;
    out[30] = ((w11 >> 46) | (w12 << 18)) & mask;
    out[31] = (w12 >> 7) & mask;

    return in + (12 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 26> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 26) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 26) & mask;
    out[2] = ((w0 >> 52) | (w1 << 12)) & mask;
    out[3] = (w1 >> 14) & mask;
    out[4] = ((w1 >> 40) | (w2 << 24)) & mask;
    out[5] = (w2 >> 2) & mask;
    out[6] = (w2 >> 28) & mask;
    out[7] = ((w2 >> 54) | (w3 << 10)) & mask;
    out[8] = (w3 >> 16) & mask;
    out[9] = ((w3 >> 42) | (w4 << 22)) & mask;
    out[10] = (w4 >> 4) & mask;
    out[11] = (w4 >> 30) & mask;
    out[12] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[13] = (w5 >> 18) & mask;
    out[14] = ((w5 >> 44) | (w6 << 20)) & mask;
    out[15] = (w6 >> 6) & mask;
    out[16] = (w6 >> 32) & mask;
    out[17] = ((w6 >> 58) | (w7 << 6)) & mask;
    out[18] = (w7 >> 20) & mask;
    out[19] = ((w7 >> 46) | (w8 << 18)) & mask;
    out[20] = (w8 >> 8) & mask;
    out[21] = (w8 >> 34) & mask;
    out[22] = ((w8 >> 60) | (w9 << 4)) & mask;
    out[23] = (w9 >> 22) & mask;
    out[24] = ((w9 >> 48) | (w10 << 16)) & mask;
    out[25] = (w10 >> 10) & mask;
    out[26] = (w10 >> 36) & mask;
    out[27] = ((w10 >> 62) | (w11 << 2)) & mask;
    out[28] = (w11 >> 24) & mask;
    out[29] = ((w11 >> 50) | (w12 << 14)) & mask;
    out[30] = (w12 >> 12) & mask;
    out[31] = w12 >> 38;

    return in + (13 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 27> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 27) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 13 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 27) & mask;
    out[2] = ((w0 >> 54) | (w1 << 10)) & mask;
    out[3] = (w1 >> 17) & mask;
    out[4] = ((w1 >> 44) | (w2 << 20)) & mask;
    out[5] = (w2 >> 7) & mask;
    out[6] = (w2 >> 34) & mask;
    out[7] = ((w2 >> 61) | (w3 << 3)) & mask;
    out[8] = (w3 >> 24) & mask;
    out[9] = ((w3 >> 51) | (w4 << 13)) & mask;
    out[10] = (w4 >> 14) & mask;
    out[11] = ((w4 >> 41) | (w5 << 23)) & mask;
    out[12] = (w5 >> 4) & mask;
    out[13] = (w5 >> 31) & mask;
    out[14] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[15] = (w6 >> 21) & mask;
    out[16] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[17] = (w7 >> 11) & mask;
    out[18] = ((w7 >> 38) | (w8 << 26)) & mask;
    out[19] = (w8 >> 1) & mask;
    out[20] = (w8 >> 28) & mask;
    out[21] = ((w8 >> 55) | (w9 << 9)) & mask;
    out[22] = (w9 >> 18) & mask;
    out[23] = ((w9 >> 45) | (w10 << 19)) & mask;
    out[24] = (w10 >> 8) & mask;
    out[25] = (w10 >> 35) & mask;
    out[26] = ((w10 >> 62) | (w11 << 2)) & mask;
    out[27] = (w11 >> 25) & mask;
    out[28] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[29] = (w12 >> 15) & mask;
    out[30] = ((w12 >> 42) | (w13 << 22)) & mask;
    out[31] = (w13 >> 5) & mask;

    return in + (13 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 28> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 28) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 28) & mask;
    out[2] = ((w0 >> 56) | (w1 << 8)) & mask;
    out[3] = (w1 >> 20) & mask;
    out[4] = ((w1 >> 48) | (w2 << 16)) & mask;
    out[5] = (w2 >> 12) & mask;
    out[6] = ((w2 >> 40) | (w3 << 24)) & mask;
    out[7] = (w3 >> 4) & mask;
    out[8] = (w3 >> 32) & mask;
    out[9] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[10] = (w4 >> 24) & mask;
    out[11] = ((w4 >> 52) | (w5 << 12)) & mask;
    out[12] = (w5 >> 16) & mask;
    out[13] = ((w5 >> 44) | (w6 << 20)) & mask;
    out[14] = (w6 >> 8) & mask;
    out[15] = w6 >> 36;
    out[16] = (w7) & mask;
    out[17] = (w7 >> 28) & mask;
    out[18] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[19] = (w8 >> 20) & mask;
    out[20] = ((w8 >> 48) | (w9 << 16)) & mask;
    out[21] = (w9 >> 12) & mask;
    out[22] = ((w9 >> 40) | (w10 << 24)) & mask;
    out[23] = (w10 >> 4) & mask;
    out[24] = (w10 >> 32) & mask;
    out[25] = ((w10 >> 60) | (w11 << 4)) & mask;
    out[26] = (w11 >> 24) & mask;
    out[27] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[28] = (w12 >> 16) & mask;
    out[29] = ((w12 >> 44) | (w13 << 20)) & mask;
    out[30] = (w13 >> 8) & mask;
    out[31] = w13 >> 36;

    return in + (14 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 29> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 29) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 14 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 29) & mask;
    out[2] = ((w0 >> 58) | (w1 << 6)) & mask;
    out[3] = (w1 >> 23) & mask;
    out[4] = ((w1 >> 52) | (w2 << 12)) & mask;
    out[5] = (w2 >> 17) & mask;
    out[6] = ((w2 >> 46) | (w3 << 18)) & mask;
    out[7] = (w3 >> 11) & mask;
    out[8] = ((w3 >> 40) | (w4 << 24)) & mask;
    out[9] = (w4 >> 5) & mask;
    out[10] = (w4 >> 34) & mask;
    out[11] = ((w4 >> 63) | (w5 << 1)) & mask;
    out[12] = (w5 >> 28) & mask;
    out[13] = ((w5 >> 57) | (w6 << 7)) & mask;
    out[14] = (w6 >> 22) & mask;
    out[15] = ((w6 >> 51) | (w7 << 13)) & mask;
    out[16] = (w7 >> 16) & mask;
    out[17] = ((w7 >> 45) | (w8 << 19)) & mask;
    out[18] = (w8 >> 10) & mask;
    out[19] = ((w8 >> 39) | (w9 << 25)) & mask;
    out[20] = (w9 >> 4) & mask;
    out[21] = (w9 >> 33) & mask;
    out[22] = ((w9 >> 62) | (w10 << 2)) & mask;
    out[23] = (w10 >> 27) & mask;
    out[24] = ((w10 >> 56) | (w11 << 8)) & mask;
    out[25] = (w11 >> 21) & mask;
    out[26] = ((w11 >> 50) | (w12 << 14)) & mask;
    out[27] = (w12 >> 15) & mask;
    out[28] = ((w12 >> 44) | (w13 << 20)) & mask;
    out[29] = (w13 >> 9) & mask;
    out[30] = ((w13 >> 38) | (w14 << 26)) & mask;
    out[31] = (w14 >> 3) & mask;

    return in + (14 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 30> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 30) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    out[0] = (w0) & mask;
    out[1] = (w0 >> 30) & mask;
    out[2] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[3] = (w1 >> 26) & mask;
    out[4] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[5] = (w2 >> 22) & mask;
    out[6] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[7] = (w3 >> 18) & mask;
    out[8] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[9] = (w4 >> 14) & mask;
    out[10] = ((w4 >> 44) | (w5 << 20)) & mask;
    out[11] = (w5 >> 10) & mask;
    out[12] = ((w5 >> 40) | (w6 << 24)) & mask;
    out[13] = (w6 >> 6) & mask;
    out[14] = ((w6 >> 36) | (w7 << 28)) & mask;
    out[15] = (w7 >> 2) & mask;
    out[16] = (w7 >> 32) & mask;
    out[17] = ((w7 >> 62) | (w8 << 2)) & mask;
    out[18] = (w8 >> 28) & mask;
    out[19] = ((w8 >> 58) | (w9 << 6)) & mask;
    out[20] = (w9 >> 24) & mask;
    out[21] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[22] = (w10 >> 20) & mask;
    out[23] = ((w10 >> 50) | (w11 << 14)) & mask;
    out[24] = (w11 >> 16) & mask;
    out[25] = ((w11 >> 46) | (w12 << 18)) & mask;
    out[26] = (w12 >> 12) & mask;
    out[27] = ((w12 >> 42) | (w13 << 22)) & mask;
    out[28] = (w13 >> 8) & mask;
    out[29] = ((w13 >> 38) | (w14 << 26)) & mask;
    out[30] = (w14 >> 4) & mask;
    out[31] = w14 >> 34;

    return in + (15 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 31> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 31) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 15 * 8));
    out[0] = (w0) & mask;
    out[1] = (w0 >> 31) & mask;
    out[2] = ((w0 >> 62) | (w1 << 2)) & mask;
    out[3] = (w1 >> 29) & mask;
    out[4] = ((w1 >> 60) | (w2 << 4)) & mask;
    out[5] = (w2 >> 27) & mask;
    out[6] = ((w2 >> 58) | (w3 << 6)) & mask;
    out[7] = (w3 >> 25) & mask;
    out[8] = ((w3 >> 56) | (w4 << 8)) & mask;
    out[9] = (w4 >> 23) & mask;
    out[10] = ((w4 >> 54) | (w5 << 10)) & mask;
    out[11] = (w5 >> 21) & mask;
    out[12] = ((w5 >> 52) | (w6 << 12)) & mask;
    out[13] = (w6 >> 19) & mask;
    out[14] = ((w6 >> 50) | (w7 << 14)) & mask;
    out[15] = (w7 >> 17) & mask;
    out[16] = ((w7 >> 48) | (w8 << 16)) & mask;
    out[17] = (w8 >> 15) & mask;
    out[18] = ((w8 >> 46) | (w9 << 18)) & mask;
    out[19] = (w9 >> 13) & mask;
    out[20] = ((w9 >> 44) | (w10 << 20)) & mask;
    out[21] = (w10 >> 11) & mask;
    out[22] = ((w10 >> 42) | (w11 << 22)) & mask;
    out[23] = (w11 >> 9) & mask;
    out[24] = ((w11 >> 40) | (w12 << 24)) & mask;
    out[25] = (w12 >> 7) & mask;
    out[26] = ((w12 >> 38) | (w13 << 26)) & mask;
    out[27] = (w13 >> 5) & mask;
    out[28] = ((w13 >> 36) | (w14 << 28)) & mask;
    out[29] = (w14 >> 3) & mask;
    out[30] = ((w14 >> 34) | (w15 << 30)) & mask;
    out[31] = (w15 >> 1) & mask;

    return in + (15 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 32> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 32) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    out[0] = (w0) & mask;
    out[1] = w0 >> 32;
    out[2] = (w1) & mask;
    out[3] = w1 >> 32;
    out[4] = (w2) & mask;
    out[5] = w2 >> 32;
    out[6] = (w3) & mask;
    out[7] = w3 >> 32;
    out[8] = (w4) & mask;
    out[9] = w4 >> 32;
    out[10] = (w5) & mask;
    out[11] = w5 >> 32;
    out[12] = (w6) & mask;
    out[13] = w6 >> 32;
    out[14] = (w7) & mask;
    out[15] = w7 >> 32;
    out[16] = (w8) & mask;
    out[17] = w8 >> 32;
    out[18] = (w9) & mask;
    out[19] = w9 >> 32;
    out[20] = (w10) & mask;
    out[21] = w10 >> 32;
    out[22] = (w11) & mask;
    out[23] = w11 >> 32;
    out[24] = (w12) & mask;
    out[25] = w12 >> 32;
    out[26] = (w13) & mask;
    out[27] = w13 >> 32;
    out[28] = (w14) & mask;
    out[29] = w14 >> 32;
    out[30] = (w15) & mask;
    out[31] = w15 >> 32;

    return in + (16 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 33> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 33) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 16 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 33) | (w1 << 31)) & mask;
    out[2] = (w1 >> 2) & mask;
    out[3] = ((w1 >> 35) | (w2 << 29)) & mask;
    out[4] = (w2 >> 4) & mask;
    out[5] = ((w2 >> 37) | (w3 << 27)) & mask;
    out[6] = (w3 >> 6) & mask;
    out[7] = ((w3 >> 39) | (w4 << 25)) & mask;
    out[8] = (w4 >> 8) & mask;
    out[9] = ((w4 >> 41) | (w5 << 23)) & mask;
    out[10] = (w5 >> 10) & mask;
    out[11] = ((w5 >> 43) | (w6 << 21)) & mask;
    out[12] = (w6 >> 12) & mask;
    out[13] = ((w6 >> 45) | (w7 << 19)) & mask;
    out[14] = (w7 >> 14) & mask;
    out[15] = ((w7 >> 47) | (w8 << 17)) & mask;
    out[16] = (w8 >> 16) & mask;
    out[17] = ((w8 >> 49) | (w9 << 15)) & mask;
    out[18] = (w9 >> 18) & mask;
    out[19] = ((w9 >> 51) | (w10 << 13)) & mask;
    out[20] = (w10 >> 20) & mask;
    out[21] = ((w10 >> 53) | (w11 << 11)) & mask;
    out[22] = (w11 >> 22) & mask;
    out[23] = ((w11 >> 55) | (w12 << 9)) & mask;
    out[24] = (w12 >> 24) & mask;
    out[25] = ((w12 >> 57) | (w13 << 7)) & mask;
    out[26] = (w13 >> 26) & mask;
    out[27] = ((w13 >> 59) | (w14 << 5)) & mask;
    out[28] = (w14 >> 28) & mask;
    out[29] = ((w14 >> 61) | (w15 << 3)) & mask;
    out[30] = (w15 >> 30) & mask;
    out[31] = ((w15 >> 63) | (w16 << 1)) & mask;

    return in + (16 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 34> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 34) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 34) | (w1 << 30)) & mask;
    out[2] = (w1 >> 4) & mask;
    out[3] = ((w1 >> 38) | (w2 << 26)) & mask;
    out[4] = (w2 >> 8) & mask;
    out[5] = ((w2 >> 42) | (w3 << 22)) & mask;
    out[6] = (w3 >> 12) & mask;
    out[7] = ((w3 >> 46) | (w4 << 18)) & mask;
    out[8] = (w4 >> 16) & mask;
    out[9] = ((w4 >> 50) | (w5 << 14)) & mask;
    out[10] = (w5 >> 20) & mask;
    out[11] = ((w5 >> 54) | (w6 << 10)) & mask;
    out[12] = (w6 >> 24) & mask;
    out[13] = ((w6 >> 58) | (w7 << 6)) & mask;
    out[14] = (w7 >> 28) & mask;
    out[15] = ((w7 >> 62) | (w8 << 2)) & mask;
    out[16] = ((w8 >> 32) | (w9 << 32)) & mask;
    out[17] = (w9 >> 2) & mask;
    out[18] = ((w9 >> 36) | (w10 << 28)) & mask;
    out[19] = (w10 >> 6) & mask;
    out[20] = ((w10 >> 40) | (w11 << 24)) & mask;
    out[21] = (w11 >> 10) & mask;
    out[22] = ((w11 >> 44) | (w12 << 20)) & mask;
    out[23] = (w12 >> 14) & mask;
    out[24] = ((w12 >> 48) | (w13 << 16)) & mask;
    out[25] = (w13 >> 18) & mask;
    out[26] = ((w13 >> 52) | (w14 << 12)) & mask;
    out[27] = (w14 >> 22) & mask;
    out[28] = ((w14 >> 56) | (w15 << 8)) & mask;
    out[29] = (w15 >> 26) & mask;
    out[30] = ((w15 >> 60) | (w16 << 4)) & mask;
    out[31] = w16 >> 30;

    return in + (17 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 35> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 35) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 17 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 35) | (w1 << 29)) & mask;
    out[2] = (w1 >> 6) & mask;
    out[3] = ((w1 >> 41) | (w2 << 23)) & mask;
    out[4] = (w2 >> 12) & mask;
    out[5] = ((w2 >> 47) | (w3 << 17)) & mask;
    out[6] = (w3 >> 18) & mask;
    out[7] = ((w3 >> 53) | (w4 << 11)) & mask;
    out[8] = (w4 >> 24) & mask;
    out[9] = ((w4 >> 59) | (w5 << 5)) & mask;
    out[10] = ((w5 >> 30) | (w6 << 34)) & mask;
    out[11] = (w6 >> 1) & mask;
    out[12] = ((w6 >> 36) | (w7 << 28)) & mask;
    out[13] = (w7 >> 7) & mask;
    out[14] = ((w7 >> 42) | (w8 << 22)) & mask;
    out[15] = (w8 >> 13) & mask;
    out[16] = ((w8 >> 48) | (w9 << 16)) & mask;
    out[17] = (w9 >> 19) & mask;
    out[18] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[19] = (w10 >> 25) & mask;
    out[20] = ((w10 >> 60) | (w11 << 4)) & mask;
    out[21] = ((w11 >> 31) | (w12 << 33)) & mask;
    out[22] = (w12 >> 2) & mask;
    out[23] = ((w12 >> 37) | (w13 << 27)) & mask;
    out[24] = (w13 >> 8) & mask;
    out[25] = ((w13 >> 43) | (w14 << 21)) & mask;
    out[26] = (w14 >> 14) & mask;
    out[27] = ((w14 >> 49) | (w15 << 15)) & mask;
    out[28] = (w15 >> 20) & mask;
    out[29] = ((w15 >> 55) | (w16 << 9)) & mask;
    out[30] = (w16 >> 26) & mask;
    out[31] = ((w16 >> 61) | (w17 << 3)) & mask;

    return in + (17 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 36> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 36) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 36) | (w1 << 28)) & mask;
    out[2] = (w1 >> 8) & mask;
    out[3] = ((w1 >> 44) | (w2 << 20)) & mask;
    out[4] = (w2 >> 16) & mask;
    out[5] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[6] = (w3 >> 24) & mask;
    out[7] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[8] = ((w4 >> 32) | (w5 << 32)) & mask;
    out[9] = (w5 >> 4) & mask;
    out[10] = ((w5 >> 40) | (w6 << 24)) & mask;
    out[11] = (w6 >> 12) & mask;
    out[12] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[13] = (w7 >> 20) & mask;
    out[14] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[15] = w8 >> 28;
    out[16] = (w9) & mask;
    out[17] = ((w9 >> 36) | (w10 << 28)) & mask;
    out[18] = (w10 >> 8) & mask;
    out[19] = ((w10 >> 44) | (w11 << 20)) & mask;
    out[20] = (w11 >> 16) & mask;
    out[21] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[22] = (w12 >> 24) & mask;
    out[23] = ((w12 >> 60) | (w13 << 4)) & mask;
    out[24] = ((w13 >> 32) | (w14 << 32)) & mask;
    out[25] = (w14 >> 4) & mask;
    out[26] = ((w14 >> 40) | (w15 << 24)) & mask;
    out[27] = (w15 >> 12) & mask;
    out[28] = ((w15 >> 48) | (w16 << 16)) & mask;
    out[29] = (w16 >> 20) & mask;
    out[30] = ((w16 >> 56) | (w17 << 8)) & mask;
    out[31] = w17 >> 28;

    return in + (18 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 37> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 37) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 18 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 37) | (w1 << 27)) & mask;
    out[2] = (w1 >> 10) & mask;
    out[3] = ((w1 >> 47) | (w2 << 17)) & mask;
    out[4] = (w2 >> 20) & mask;
    out[5] = ((w2 >> 57) | (w3 << 7)) & mask;
    out[6] = ((w3 >> 30) | (w4 << 34)) & mask;
    out[7] = (w4 >> 3) & mask;
    out[8] = ((w4 >> 40) | (w5 << 24)) & mask;
    out[9] = (w5 >> 13) & mask;
    out[10] = ((w5 >> 50) | (w6 << 14)) & mask;
    out[11] = (w6 >> 23) & mask;
    out[12] = ((w6 >> 60) | (w7 << 4)) & mask;
    out[13] = ((w7 >> 33) | (w8 << 31)) & mask;
    out[14] = (w8 >> 6) & mask;
    out[15] = ((w8 >> 43) | (w9 << 21)) & mask;
    out[16] = (w9 >> 16) & mask;
    out[17] = ((w9 >> 53) | (w10 << 11)) & mask;
    out[18] = (w10 >> 26) & mask;
    out[19] = ((w10 >> 63) | (w11 << 1)) & mask;
    out[20] = ((w11 >> 36) | (w12 << 28)) & mask;
    out[21] = (w12 >> 9) & mask;
    out[22] = ((w12 >> 46) | (w13 << 18)) & mask;
    out[23] = (w13 >> 19) & mask;
    out[24] = ((w13 >> 56) | (w14 << 8)) & mask;
    out[25] = ((w14 >> 29) | (w15 << 35)) & mask;
    out[26] = (w15 >> 2) & mask;
    out[27] = ((w15 >> 39) | (w16 << 25)) & mask;
    out[28] = (w16 >> 12) & mask;
    out[29] = ((w16 >> 49) | (w17 << 15)) & mask;
    out[30] = (w17 >> 22) & mask;
    out[31] = ((w17 >> 59) | (w18 << 5)) & mask;

    return in + (18 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 38> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 38) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 38) | (w1 << 26)) & mask;
    out[2] = (w1 >> 12) & mask;
    out[3] = ((w1 >> 50) | (w2 << 14)) & mask;
    out[4] = (w2 >> 24) & mask;
    out[5] = ((w2 >> 62) | (w3 << 2)) & mask;
    out[6] = ((w3 >> 36) | (w4 << 28)) & mask;
    out[7] = (w4 >> 10) & mask;
    out[8] = ((w4 >> 48) | (w5 << 16)) & mask;
    out[9] = (w5 >> 22) & mask;
    out[10] = ((w5 >> 60) | (w6 << 4)) & mask;
    out[11] = ((w6 >> 34) | (w7 << 30)) & mask;
    out[12] = (w7 >> 8) & mask;
    out[13] = ((w7 >> 46) | (w8 << 18)) & mask;
    out[14] = (w8 >> 20) & mask;
    out[15] = ((w8 >> 58) | (w9 << 6)) & mask;
    out[16] = ((w9 >> 32) | (w10 << 32)) & mask;
    out[17] = (w10 >> 6) & mask;
    out[18] = ((w10 >> 44) | (w11 << 20)) & mask;
    out[19] = (w11 >> 18) & mask;
    out[20] = ((w11 >> 56) | (w12 << 8)) & mask;
    out[21] = ((w12 >> 30) | (w13 << 34)) & mask;
    out[22] = (w13 >> 4) & mask;
    out[23] = ((w13 >> 42) | (w14 << 22)) & mask;
    out[24] = (w14 >> 16) & mask;
    out[25] = ((w14 >> 54) | (w15 << 10)) & mask;
    out[26] = ((w15 >> 28) | (w16 << 36)) & mask;
    out[27] = (w16 >> 2) & mask;
    out[28] = ((w16 >> 40) | (w17 << 24)) & mask;
    out[29] = (w17 >> 14) & mask;
    out[30] = ((w17 >> 52) | (w18 << 12)) & mask;
    out[31] = w18 >> 26;

    return in + (19 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 39> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 39) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 19 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 39) | (w1 << 25)) & mask;
    out[2] = (w1 >> 14) & mask;
    out[3] = ((w1 >> 53) | (w2 << 11)) & mask;
    out[4] = ((w2 >> 28) | (w3 << 36)) & mask;
    out[5] = (w3 >> 3) & mask;
    out[6] = ((w3 >> 42) | (w4 << 22)) & mask;
    out[7] = (w4 >> 17) & mask;
    out[8] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[9] = ((w5 >> 31) | (w6 << 33)) & mask;
    out[10] = (w6 >> 6) & mask;
    out[11] = ((w6 >> 45) | (w7 << 19)) & mask;
    out[12] = (w7 >> 20) & mask;
    out[13] = ((w7 >> 59) | (w8 << 5)) & mask;
    out[14] = ((w8 >> 34) | (w9 << 30)) & mask;
    out[15] = (w9 >> 9) & mask;
    out[16] = ((w9 >> 48) | (w10 << 16)) & mask;
    out[17] = (w10 >> 23) & mask;
    out[18] = ((w10 >> 62) | (w11 << 2)) & mask;
    out[19] = ((w11 >> 37) | (w12 << 27)) & mask;
    out[20] = (w12 >> 12) & mask;
    out[21] = ((w12 >> 51) | (w13 << 13)) & mask;
    out[22] = ((w13 >> 26) | (w14 << 38)) & mask;
    out[23] = (w14 >> 1) & mask;
    out[24] = ((w14 >> 40) | (w15 << 24)) & mask;
    out[25] = (w15 >> 15) & mask;
    out[26] = ((w15 >> 54) | (w16 << 10)) & mask;
    out[27] = ((w16 >> 29) | (w17 << 35)) & mask;
    out[28] = (w17 >> 4) & mask;
    out[29] = ((w17 >> 43) | (w18 << 21)) & mask;
    out[30] = (w18 >> 18) & mask;
    out[31] = ((w18 >> 57) | (w19 << 7)) & mask;

    return in + (19 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 40> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 40) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 40) | (w1 << 24)) & mask;
    out[2] = (w1 >> 16) & mask;
    out[3] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[4] = ((w2 >> 32) | (w3 << 32)) & mask;
    out[5] = (w3 >> 8) & mask;
    out[6] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[7] = w4 >> 24;
    out[8] = (w5) & mask;
    out[9] = ((w5 >> 40) | (w6 << 24)) & mask;
    out[10] = (w6 >> 16) & mask;
    out[11] = ((w6 >> 56) | (w7 << 8)) & mask;
    out[12] = ((w7 >> 32) | (w8 << 32)) & mask;
    out[13] = (w8 >> 8) & mask;
    out[14] = ((w8 >> 48) | (w9 << 16)) & mask;
    out[15] = w9 >> 24;
    out[16] = (w10) & mask;
    out[17] = ((w10 >> 40) | (w11 << 24)) & mask;
    out[18] = (w11 >> 16) & mask;
    out[19] = ((w11 >> 56) | (w12 << 8)) & mask;
    out[20] = ((w12 >> 32) | (w13 << 32)) & mask;
    out[21] = (w13 >> 8) & mask;
    out[22] = ((w13 >> 48) | (w14 << 16)) & mask;
    out[23] = w14 >> 24;
    out[24] = (w15) & mask;
    out[25] = ((w15 >> 40) | (w16 << 24)) & mask;
    out[26] = (w16 >> 16) & mask;
    out[27] = ((w16 >> 56) | (w17 << 8)) & mask;
    out[28] = ((w17 >> 32) | (w18 << 32)) & mask;
    out[29] = (w18 >> 8) & mask;
    out[30] = ((w18 >> 48) | (w19 << 16)) & mask;
    out[31] = w19 >> 24;

    return in + (20 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 41> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 41) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 20 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 41) | (w1 << 23)) & mask;
    out[2] = (w1 >> 18) & mask;
    out[3] = ((w1 >> 59) | (w2 << 5)) & mask;
    out[4] = ((w2 >> 36) | (w3 << 28)) & mask;
    out[5] = (w3 >> 13) & mask;
    out[6] = ((w3 >> 54) | (w4 << 10)) & mask;
    out[7] = ((w4 >> 31) | (w5 << 33)) & mask;
    out[8] = (w5 >> 8) & mask;
    out[9] = ((w5 >> 49) | (w6 << 15)) & mask;
    out[10] = ((w6 >> 26) | (w7 << 38)) & mask;
    out[11] = (w7 >> 3) & mask;
    out[12] = ((w7 >> 44) | (w8 << 20)) & mask;
    out[13] = (w8 >> 21) & mask;
    out[14] = ((w8 >> 62) | (w9 << 2)) & mask;
    out[15] = ((w9 >> 39) | (w10 << 25)) & mask;
    out[16] = (w10 >> 16) & mask;
    out[17] = ((w10 >> 57) | (w11 << 7)) & mask;
    out[18] = ((w11 >> 34) | (w12 << 30)) & mask;
    out[19] = (w12 >> 11) & mask;
    out[20] = ((w12 >> 52) | (w13 << 12)) & mask;
    out[21] = ((w13 >> 29) | (w14 << 35)) & mask;
    out[22] = (w14 >> 6) & mask;
    out[23] = ((w14 >> 47) | (w15 << 17)) & mask;
    out[24] = ((w15 >> 24) | (w16 << 40)) & mask;
    out[25] = (w16 >> 1) & mask;
    out[26] = ((w16 >> 42) | (w17 << 22)) & mask;
    out[27] = (w17 >> 19) & mask;
    out[28] = ((w17 >> 60) | (w18 << 4)) & mask;
    out[29] = ((w18 >> 37) | (w19 << 27)) & mask;
    out[30] = (w19 >> 14) & mask;
    out[31] = ((w19 >> 55) | (w20 << 9)) & mask;

    return in + (20 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 42> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 42) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 42) | (w1 << 22)) & mask;
    out[2] = (w1 >> 20) & mask;
    out[3] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[4] = ((w2 >> 40) | (w3 << 24)) & mask;
    out[5] = (w3 >> 18) & mask;
    out[6] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[7] = ((w4 >> 38) | (w5 << 26)) & mask;
    out[8] = (w5 >> 16) & mask;
    out[9] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[10] = ((w6 >> 36) | (w7 << 28)) & mask;
    out[11] = (w7 >> 14) & mask;
    out[12] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[13] = ((w8 >> 34) | (w9 << 30)) & mask;
    out[14] = (w9 >> 12) & mask;
    out[15] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[16] = ((w10 >> 32) | (w11 << 32)) & mask;
    out[17] = (w11 >> 10) & mask;
    out[18] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[19] = ((w12 >> 30) | (w13 << 34)) & mask;
    out[20] = (w13 >> 8) & mask;
    out[21] = ((w13 >> 50) | (w14 << 14)) & mask;
    out[22] = ((w14 >> 28) | (w15 << 36)) & mask;
    out[23] = (w15 >> 6) & mask;
    out[24] = ((w15 >> 48) | (w16 << 16)) & mask;
    out[25] = ((w16 >> 26) | (w17 << 38)) & mask;
    out[26] = (w17 >> 4) & mask;
    out[27] = ((w17 >> 46) | (w18 << 18)) & mask;
    out[28] = ((w18 >> 24) | (w19 << 40)) & mask;
    out[29] = (w19 >> 2) & mask;
    out[30] = ((w19 >> 44) | (w20 << 20)) & mask;
    out[31] = w20 >> 22;

    return in + (21 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 43> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 43) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 21 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 43) | (w1 << 21)) & mask;
    out[2] = ((w1 >> 22) | (w2 << 42)) & mask;
    out[3] = (w2 >> 1) & mask;
    out[4] = ((w2 >> 44) | (w3 << 20)) & mask;
    out[5] = ((w3 >> 23) | (w4 << 41)) & mask;
    out[6] = (w4 >> 2) & mask;
    out[7] = ((w4 >> 45) | (w5 << 19)) & mask;
    out[8] = ((w5 >> 24) | (w6 << 40)) & mask;
    out[9] = (w6 >> 3) & mask;
    out[10] = ((w6 >> 46) | (w7 << 18)) & mask;
    out[11] = ((w7 >> 25) | (w8 << 39)) & mask;
    out[12] = (w8 >> 4) & mask;
    out[13] = ((w8 >> 47) | (w9 << 17)) & mask;
    out[14] = ((w9 >> 26) | (w10 << 38)) & mask;
    out[15] = (w10 >> 5) & mask;
    out[16] = ((w10 >> 48) | (w11 << 16)) & mask;
    out[17] = ((w11 >> 27) | (w12 << 37)) & mask;
    out[18] = (w12 >> 6) & mask;
    out[19] = ((w12 >> 49) | (w13 << 15)) & mask;
    out[20] = ((w13 >> 28) | (w14 << 36)) & mask;
    out[21] = (w14 >> 7) & mask;
    out[22] = ((w14 >> 50) | (w15 << 14)) & mask;
    out[23] = ((w15 >> 29) | (w16 << 35)) & mask;
    out[24] = (w16 >> 8) & mask;
    out[25] = ((w16 >> 51) | (w17 << 13)) & mask;
    out[26] = ((w17 >> 30) | (w18 << 34)) & mask;
    out[27] = (w18 >> 9) & mask;
    out[28] = ((w18 >> 52) | (w19 << 12)) & mask;
    out[29] = ((w19 >> 31) | (w20 << 33)) & mask;
    out[30] = (w20 >> 10) & mask;
    out[31] = ((w20 >> 53) | (w21 << 11)) & mask;

    return in + (21 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 44> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 44) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 44) | (w1 << 20)) & mask;
    out[2] = ((w1 >> 24) | (w2 << 40)) & mask;
    out[3] = (w2 >> 4) & mask;
    out[4] = ((w2 >> 48) | (w3 << 16)) & mask;
    out[5] = ((w3 >> 28) | (w4 << 36)) & mask;
    out[6] = (w4 >> 8) & mask;
    out[7] = ((w4 >> 52) | (w5 << 12)) & mask;
    out[8] = ((w5 >> 32) | (w6 << 32)) & mask;
    out[9] = (w6 >> 12) & mask;
    out[10] = ((w6 >> 56) | (w7 << 8)) & mask;
    out[11] = ((w7 >> 36) | (w8 << 28)) & mask;
    out[12] = (w8 >> 16) & mask;
    out[13] = ((w8 >> 60) | (w9 << 4)) & mask;
    out[14] = ((w9 >> 40) | (w10 << 24)) & mask;
    out[15] = w10 >> 20;
    out[16] = (w11) & mask;
    out[17] = ((w11 >> 44) | (w12 << 20)) & mask;
    out[18] = ((w12 >> 24) | (w13 << 40)) & mask;
    out[19] = (w13 >> 4) & mask;
    out[20] = ((w13 >> 48) | (w14 << 16)) & mask;
    out[21] = ((w14 >> 28) | (w15 << 36)) & mask;
    out[22] = (w15 >> 8) & mask;
    out[23] = ((w15 >> 52) | (w16 << 12)) & mask;
    out[24] = ((w16 >> 32) | (w17 << 32)) & mask;
    out[25] = (w17 >> 12) & mask;
    out[26] = ((w17 >> 56) | (w18 << 8)) & mask;
    out[27] = ((w18 >> 36) | (w19 << 28)) & mask;
    out[28] = (w19 >> 16) & mask;
    out[29] = ((w19 >> 60) | (w20 << 4)) & mask;
    out[30] = ((w20 >> 40) | (w21 << 24)) & mask;
    out[31] = w21 >> 20;

    return in + (22 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 45> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 45) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 22 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 45) | (w1 << 19)) & mask;
    out[2] = ((w1 >> 26) | (w2 << 38)) & mask;
    out[3] = (w2 >> 7) & mask;
    out[4] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[5] = ((w3 >> 33) | (w4 << 31)) & mask;
    out[6] = (w4 >> 14) & mask;
    out[7] = ((w4 >> 59) | (w5 << 5)) & mask;
    out[8] = ((w5 >> 40) | (w6 << 24)) & mask;
    out[9] = ((w6 >> 21) | (w7 << 43)) & mask;
    out[10] = (w7 >> 2) & mask;
    out[11] = ((w7 >> 47) | (w8 << 17)) & mask;
    out[12] = ((w8 >> 28) | (w9 << 36)) & mask;
    out[13] = (w9 >> 9) & mask;
    out[14] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[15] = ((w10 >> 35) | (w11 << 29)) & mask;
    out[16] = (w11 >> 16) & mask;
    out[17] = ((w11 >> 61) | (w12 << 3)) & mask;
    out[18] = ((w12 >> 42) | (w13 << 22)) & mask;
    out[19] = ((w13 >> 23) | (w14 << 41)) & mask;
    out[20] = (w14 >> 4) & mask;
    out[21] = ((w14 >> 49) | (w15 << 15)) & mask;
    out[22] = ((w15 >> 30) | (w16 << 34)) & mask;
    out[23] = (w16 >> 11) & mask;
    out[24] = ((w16 >> 56) | (w17 << 8)) & mask;
    out[25] = ((w17 >> 37) | (w18 << 27)) & mask;
    out[26] = (w18 >> 18) & mask;
    out[27] = ((w18 >> 63) | (w19 << 1)) & mask;
    out[28] = ((w19 >> 44) | (w20 << 20)) & mask;
    out[29] = ((w20 >> 25) | (w21 << 39)) & mask;
    out[30] = (w21 >> 6) & mask;
    out[31] = ((w21 >> 51) | (w22 << 13)) & mask;

    return in + (22 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 46> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 46) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 46) | (w1 << 18)) & mask;
    out[2] = ((w1 >> 28) | (w2 << 36)) & mask;
    out[3] = (w2 >> 10) & mask;
    out[4] = ((w2 >> 56) | (w3 << 8)) & mask;
    out[5] = ((w3 >> 38) | (w4 << 26)) & mask;
    out[6] = ((w4 >> 20) | (w5 << 44)) & mask;
    out[7] = (w5 >> 2) & mask;
    out[8] = ((w5 >> 48) | (w6 << 16)) & mask;
    out[9] = ((w6 >> 30) | (w7 << 34)) & mask;
    out[10] = (w7 >> 12) & mask;
    out[11] = ((w7 >> 58) | (w8 << 6)) & mask;
    out[12] = ((w8 >> 40) | (w9 << 24)) & mask;
    out[13] = ((w9 >> 22) | (w10 << 42)) & mask;
    out[14] = (w10 >> 4) & mask;
    out[15] = ((w10 >> 50) | (w11 << 14)) & mask;
    out[16] = ((w11 >> 32) | (w12 << 32)) & mask;
    out[17] = (w12 >> 14) & mask;
    out[18] = ((w12 >> 60) | (w13 << 4)) & mask;
    out[19] = ((w13 >> 42) | (w14 << 22)) & mask;
    out[20] = ((w14 >> 24) | (w15 << 40)) & mask;
    out[21] = (w15 >> 6) & mask;
    out[22] = ((w15 >> 52) | (w16 << 12)) & mask;
    out[23] = ((w16 >> 34) | (w17 << 30)) & mask;
    out[24] = (w17 >> 16) & mask;
    out[25] = ((w17 >> 62) | (w18 << 2)) & mask;
    out[26] = ((w18 >> 44) | (w19 << 20)) & mask;
    out[27] = ((w19 >> 26) | (w20 << 38)) & mask;
    out[28] = (w20 >> 8) & mask;
    out[29] = ((w20 >> 54) | (w21 << 10)) & mask;
    out[30] = ((w21 >> 36) | (w22 << 28)) & mask;
    out[31] = w22 >> 18;

    return in + (23 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 47> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 47) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 23 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 47) | (w1 << 17)) & mask;
    out[2] = ((w1 >> 30) | (w2 << 34)) & mask;
    out[3] = (w2 >> 13) & mask;
    out[4] = ((w2 >> 60) | (w3 << 4)) & mask;
    out[5] = ((w3 >> 43) | (w4 << 21)) & mask;
    out[6] = ((w4 >> 26) | (w5 << 38)) & mask;
    out[7] = (w5 >> 9) & mask;
    out[8] = ((w5 >> 56) | (w6 << 8)) & mask;
    out[9] = ((w6 >> 39) | (w7 << 25)) & mask;
    out[10] = ((w7 >> 22) | (w8 << 42)) & mask;
    out[11] = (w8 >> 5) & mask;
    out[12] = ((w8 >> 52) | (w9 << 12)) & mask;
    out[13] = ((w9 >> 35) | (w10 << 29)) & mask;
    out[14] = ((w10 >> 18) | (w11 << 46)) & mask;
    out[15] = (w11 >> 1) & mask;
    out[16] = ((w11 >> 48) | (w12 << 16)) & mask;
    out[17] = ((w12 >> 31) | (w13 << 33)) & mask;
    out[18] = (w13 >> 14) & mask;
    out[19] = ((w13 >> 61) | (w14 << 3)) & mask;
    out[20] = ((w14 >> 44) | (w15 << 20)) & mask;
    out[21] = ((w15 >> 27) | (w16 << 37)) & mask;
    out[22] = (w16 >> 10) & mask;
    out[23] = ((w16 >> 57) | (w17 << 7)) & mask;
    out[24] = ((w17 >> 40) | (w18 << 24)) & mask;
    out[25] = ((w18 >> 23) | (w19 << 41)) & mask;
    out[26] = (w19 >> 6) & mask;
    out[27] = ((w19 >> 53) | (w20 << 11)) & mask;
    out[28] = ((w20 >> 36) | (w21 << 28)) & mask;
    out[29] = ((w21 >> 19) | (w22 << 45)) & mask;
    out[30] = (w22 >> 2) & mask;
    out[31] = ((w22 >> 49) | (w23 << 15)) & mask;

    return in + (23 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 48> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 48) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 48) | (w1 << 16)) & mask;
    out[2] = ((w1 >> 32) | (w2 << 32)) & mask;
    out[3] = w2 >> 16;
    out[4] = (w3) & mask;
    out[5] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[6] = ((w4 >> 32) | (w5 << 32)) & mask;
    out[7] = w5 >> 16;
    out[8] = (w6) & mask;
    out[9] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[10] = ((w7 >> 32) | (w8 << 32)) & mask;
    out[11] = w8 >> 16;
    out[12] = (w9) & mask;
    out[13] = ((w9 >> 48) | (w10 << 16)) & mask;
    out[14] = ((w10 >> 32) | (w11 << 32)) & mask;
    out[15] = w11 >> 16;
    out[16] = (w12) & mask;
    out[17] = ((w12 >> 48) | (w13 << 16)) & mask;
    out[18] = ((w13 >> 32) | (w14 << 32)) & mask;
    out[19] = w14 >> 16;
    out[20] = (w15) & mask;
    out[21] = ((w15 >> 48) | (w16 << 16)) & mask;
    out[22] = ((w16 >> 32) | (w17 << 32)) & mask;
    out[23] = w17 >> 16;
    out[24] = (w18) & mask;
    out[25] = ((w18 >> 48) | (w19 << 16)) & mask;
    out[26] = ((w19 >> 32) | (w20 << 32)) & mask;
    out[27] = w20 >> 16;
    out[28] = (w21) & mask;
    out[29] = ((w21 >> 48) | (w22 << 16)) & mask;
    out[30] = ((w22 >> 32) | (w23 << 32)) & mask;
    out[31] = w23 >> 16;

    return in + (24 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 49> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 49) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 24 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 49) | (w1 << 15)) & mask;
    out[2] = ((w1 >> 34) | (w2 << 30)) & mask;
    out[3] = ((w2 >> 19) | (w3 << 45)) & mask;
    out[4] = (w3 >> 4) & mask;
    out[5] = ((w3 >> 53) | (w4 << 11)) & mask;
    out[6] = ((w4 >> 38) | (w5 << 26)) & mask;
    out[7] = ((w5 >> 23) | (w6 << 41)) & mask;
    out[8] = (w6 >> 8) & mask;
    out[9] = ((w6 >> 57) | (w7 << 7)) & mask;
    out[10] = ((w7 >> 42) | (w8 << 22)) & mask;
    out[11] = ((w8 >> 27) | (w9 << 37)) & mask;
    out[12] = (w9 >> 12) & mask;
    out[13] = ((w9 >> 61) | (w10 << 3)) & mask;
    out[14] = ((w10 >> 46) | (w11 << 18)) & mask;
    out[15] = ((w11 >> 31) | (w12 << 33)) & mask;
    out[16] = ((w12 >> 16) | (w13 << 48)) & mask;
    out[17] = (w13 >> 1) & mask;
    out[18] = ((w13 >> 50) | (w14 << 14)) & mask;
    out[19] = ((w14 >> 35) | (w15 << 29)) & mask;
    out[20] = ((w15 >> 20) | (w16 << 44)) & mask;
    out[21] = (w16 >> 5) & mask;
    out[22] = ((w16 >> 54) | (w17 << 10)) & mask;
    out[23] = ((w17 >> 39) | (w18 << 25)) & mask;
    out[24] = ((w18 >> 24) | (w19 << 40)) & mask;
    out[25] = (w19 >> 9) & mask;
    out[26] = ((w19 >> 58) | (w20 << 6)) & mask;
    out[27] = ((w20 >> 43) | (w21 << 21)) & mask;
    out[28] = ((w21 >> 28) | (w22 << 36)) & mask;
    out[29] = (w22 >> 13) & mask;
    out[30] = ((w22 >> 62) | (w23 << 2)) & mask;
    out[31] = ((w23 >> 47) | (w24 << 17)) & mask;

    return in + (24 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 50> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 50) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 50) | (w1 << 14)) & mask;
    out[2] = ((w1 >> 36) | (w2 << 28)) & mask;
    out[3] = ((w2 >> 22) | (w3 << 42)) & mask;
    out[4] = (w3 >> 8) & mask;
    out[5] = ((w3 >> 58) | (w4 << 6)) & mask;
    out[6] = ((w4 >> 44) | (w5 << 20)) & mask;
    out[7] = ((w5 >> 30) | (w6 << 34)) & mask;
    out[8] = ((w6 >> 16) | (w7 << 48)) & mask;
    out[9] = (w7 >> 2) & mask;
    out[10] = ((w7 >> 52) | (w8 << 12)) & mask;
    out[11] = ((w8 >> 38) | (w9 << 26)) & mask;
    out[12] = ((w9 >> 24) | (w10 << 40)) & mask;
    out[13] = (w10 >> 10) & mask;
    out[14] = ((w10 >> 60) | (w11 << 4)) & mask;
    out[15] = ((w11 >> 46) | (w12 << 18)) & mask;
    out[16] = ((w12 >> 32) | (w13 << 32)) & mask;
    out[17] = ((w13 >> 18) | (w14 << 46)) & mask;
    out[18] = (w14 >> 4) & mask;
    out[19] = ((w14 >> 54) | (w15 << 10)) & mask;
    out[20] = ((w15 >> 40) | (w16 << 24)) & mask;
    out[21] = ((w16 >> 26) | (w17 << 38)) & mask;
    out[22] = (w17 >> 12) & mask;
    out[23] = ((w17 >> 62) | (w18 << 2)) & mask;
    out[24] = ((w18 >> 48) | (w19 << 16)) & mask;
    out[25] = ((w19 >> 34) | (w20 << 30)) & mask;
    out[26] = ((w20 >> 20) | (w21 << 44)) & mask;
    out[27] = (w21 >> 6) & mask;
    out[28] = ((w21 >> 56) | (w22 << 8)) & mask;
    out[29] = ((w22 >> 42) | (w23 << 22)) & mask;
    out[30] = ((w23 >> 28) | (w24 << 36)) & mask;
    out[31] = w24 >> 14;

    return in + (25 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 51> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 51) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 25 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 51) | (w1 << 13)) & mask;
    out[2] = ((w1 >> 38) | (w2 << 26)) & mask;
    out[3] = ((w2 >> 25) | (w3 << 39)) & mask;
    out[4] = (w3 >> 12) & mask;
    out[5] = ((w3 >> 63) | (w4 << 1)) & mask;
    out[6] = ((w4 >> 50) | (w5 << 14)) & mask;
    out[7] = ((w5 >> 37) | (w6 << 27)) & mask;
    out[8] = ((w6 >> 24) | (w7 << 40)) & mask;
    out[9] = (w7 >> 11) & mask;
    out[10] = ((w7 >> 62) | (w8 << 2)) & mask;
    out[11] = ((w8 >> 49) | (w9 << 15)) & mask;
    out[12] = ((w9 >> 36) | (w10 << 28)) & mask;
    out[13] = ((w10 >> 23) | (w11 << 41)) & mask;
    out[14] = (w11 >> 10) & mask;
    out[15] = ((w11 >> 61) | (w12 << 3)) & mask;
    out[16] = ((w12 >> 48) | (w13 << 16)) & mask;
    out[17] = ((w13 >> 35) | (w14 << 29)) & mask;
    out[18] = ((w14 >> 22) | (w15 << 42)) & mask;
    out[19] = (w15 >> 9) & mask;
    out[20] = ((w15 >> 60) | (w16 << 4)) & mask;
    out[21] = ((w16 >> 47) | (w17 << 17)) & mask;
    out[22] = ((w17 >> 34) | (w18 << 30)) & mask;
    out[23] = ((w18 >> 21) | (w19 << 43)) & mask;
    out[24] = (w19 >> 8) & mask;
    out[25] = ((w19 >> 59) | (w20 << 5)) & mask;
    out[26] = ((w20 >> 46) | (w21 << 18)) & mask;
    out[27] = ((w21 >> 33) | (w22 << 31)) & mask;
    out[28] = ((w22 >> 20) | (w23 << 44)) & mask;
    out[29] = (w23 >> 7) & mask;
    out[30] = ((w23 >> 58) | (w24 << 6)) & mask;
    out[31] = ((w24 >> 45) | (w25 << 19)) & mask;

    return in + (25 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 52> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 52) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 52) | (w1 << 12)) & mask;
    out[2] = ((w1 >> 40) | (w2 << 24)) & mask;
    out[3] = ((w2 >> 28) | (w3 << 36)) & mask;
    out[4] = ((w3 >> 16) | (w4 << 48)) & mask;
    out[5] = (w4 >> 4) & mask;
    out[6] = ((w4 >> 56) | (w5 << 8)) & mask;
    out[7] = ((w5 >> 44) | (w6 << 20)) & mask;
    out[8] = ((w6 >> 32) | (w7 << 32)) & mask;
    out[9] = ((w7 >> 20) | (w8 << 44)) & mask;
    out[10] = (w8 >> 8) & mask;
    out[11] = ((w8 >> 60) | (w9 << 4)) & mask;
    out[12] = ((w9 >> 48) | (w10 << 16)) & mask;
    out[13] = ((w10 >> 36) | (w11 << 28)) & mask;
    out[14] = ((w11 >> 24) | (w12 << 40)) & mask;
    out[15] = w12 >> 12;
    out[16] = (w13) & mask;
    out[17] = ((w13 >> 52) | (w14 << 12)) & mask;
    out[18] = ((w14 >> 40) | (w15 << 24)) & mask;
    out[19] = ((w15 >> 28) | (w16 << 36)) & mask;
    out[20] = ((w16 >> 16) | (w17 << 48)) & mask;
    out[21] = (w17 >> 4) & mask;
    out[22] = ((w17 >> 56) | (w18 << 8)) & mask;
    out[23] = ((w18 >> 44) | (w19 << 20)) & mask;
    out[24] = ((w19 >> 32) | (w20 << 32)) & mask;
    out[25] = ((w20 >> 20) | (w21 << 44)) & mask;
    out[26] = (w21 >> 8) & mask;
    out[27] = ((w21 >> 60) | (w22 << 4)) & mask;
    out[28] = ((w22 >> 48) | (w23 << 16)) & mask;
    out[29] = ((w23 >> 36) | (w24 << 28)) & mask;
    out[30] = ((w24 >> 24) | (w25 << 40)) & mask;
    out[31] = w25 >> 12;

    return in + (26 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 53> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 53) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 26 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 53) | (w1 << 11)) & mask;
    out[2] = ((w1 >> 42) | (w2 << 22)) & mask;
    out[3] = ((w2 >> 31) | (w3 << 33)) & mask;
    out[4] = ((w3 >> 20) | (w4 << 44)) & mask;
    out[5] = (w4 >> 9) & mask;
    out[6] = ((w4 >> 62) | (w5 << 2)) & mask;
    out[7] = ((w5 >> 51) | (w6 << 13)) & mask;
    out[8] = ((w6 >> 40) | (w7 << 24)) & mask;
    out[9] = ((w7 >> 29) | (w8 << 35)) & mask;
    out[10] = ((w8 >> 18) | (w9 << 46)) & mask;
    out[11] = (w9 >> 7) & mask;
    out[12] = ((w9 >> 60) | (w10 << 4)) & mask;
    out[13] = ((w10 >> 49) | (w11 << 15)) & mask;
    out[14] = ((w11 >> 38) | (w12 << 26)) & mask;
    out[15] = ((w12 >> 27) | (w13 << 37)) & mask;
    out[16] = ((w13 >> 16) | (w14 << 48)) & mask;
    out[17] = (w14 >> 5) & mask;
    out[18] = ((w14 >> 58) | (w15 << 6)) & mask;
    out[19] = ((w15 >> 47) | (w16 << 17)) & mask;
    out[20] = ((w16 >> 36) | (w17 << 28)) & mask;
    out[21] = ((w17 >> 25) | (w18 << 39)) & mask;
    out[22] = ((w18 >> 14) | (w19 << 50)) & mask;
    out[23] = (w19 >> 3) & mask;
    out[24] = ((w19 >> 56) | (w20 << 8)) & mask;
    out[25] = ((w20 >> 45) | (w21 << 19)) & mask;
    out[26] = ((w21 >> 34) | (w22 << 30)) & mask;
    out[27] = ((w22 >> 23) | (w23 << 41)) & mask;
    out[28] = ((w23 >> 12) | (w24 << 52)) & mask;
    out[29] = (w24 >> 1) & mask;
    out[30] = ((w24 >> 54) | (w25 << 10)) & mask;
    out[31] = ((w25 >> 43) | (w26 << 21)) & mask;

    return in + (26 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 54> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 54) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 54) | (w1 << 10)) & mask;
    out[2] = ((w1 >> 44) | (w2 << 20)) & mask;
    out[3] = ((w2 >> 34) | (w3 << 30)) & mask;
    out[4] = ((w3 >> 24) | (w4 << 40)) & mask;
    out[5] = ((w4 >> 14) | (w5 << 50)) & mask;
    out[6] = (w5 >> 4) & mask;
    out[7] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[8] = ((w6 >> 48) | (w7 << 16)) & mask;
    out[9] = ((w7 >> 38) | (w8 << 26)) & mask;
    out[10] = ((w8 >> 28) | (w9 << 36)) & mask;
    out[11] = ((w9 >> 18) | (w10 << 46)) & mask;
    out[12] = (w10 >> 8) & mask;
    out[13] = ((w10 >> 62) | (w11 << 2)) & mask;
    out[14] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[15] = ((w12 >> 42) | (w13 << 22)) & mask;
    out[16] = ((w13 >> 32) | (w14 << 32)) & mask;
    out[17] = ((w14 >> 22) | (w15 << 42)) & mask;
    out[18] = ((w15 >> 12) | (w16 << 52)) & mask;
    out[19] = (w16 >> 2) & mask;
    out[20] = ((w16 >> 56) | (w17 << 8)) & mask;
    out[21] = ((w17 >> 46) | (w18 << 18)) & mask;
    out[22] = ((w18 >> 36) | (w19 << 28)) & mask;
    out[23] = ((w19 >> 26) | (w20 << 38)) & mask;
    out[24] = ((w20 >> 16) | (w21 << 48)) & mask;
    out[25] = (w21 >> 6) & mask;
    out[26] = ((w21 >> 60) | (w22 << 4)) & mask;
    out[27] = ((w22 >> 50) | (w23 << 14)) & mask;
    out[28] = ((w23 >> 40) | (w24 << 24)) & mask;
    out[29] = ((w24 >> 30) | (w25 << 34)) & mask;
    out[30] = ((w25 >> 20) | (w26 << 44)) & mask;
    out[31] = w26 >> 10;

    return in + (27 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 55> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 55) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 27 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 55) | (w1 << 9)) & mask;
    out[2] = ((w1 >> 46) | (w2 << 18)) & mask;
    out[3] = ((w2 >> 37) | (w3 << 27)) & mask;
    out[4] = ((w3 >> 28) | (w4 << 36)) & mask;
    out[5] = ((w4 >> 19) | (w5 << 45)) & mask;
    out[6] = ((w5 >> 10) | (w6 << 54)) & mask;
    out[7] = (w6 >> 1) & mask;
    out[8] = ((w6 >> 56) | (w7 << 8)) & mask;
    out[9] = ((w7 >> 47) | (w8 << 17)) & mask;
    out[10] = ((w8 >> 38) | (w9 << 26)) & mask;
    out[11] = ((w9 >> 29) | (w10 << 35)) & mask;
    out[12] = ((w10 >> 20) | (w11 << 44)) & mask;
    out[13] = ((w11 >> 11) | (w12 << 53)) & mask;
    out[14] = (w12 >> 2) & mask;
    out[15] = ((w12 >> 57) | (w13 << 7)) & mask;
    out[16] = ((w13 >> 48) | (w14 << 16)) & mask;
    out[17] = ((w14 >> 39) | (w15 << 25)) & mask;
    out[18] = ((w15 >> 30) | (w16 << 34)) & mask;
    out[19] = ((w16 >> 21) | (w17 << 43)) & mask;
    out[20] = ((w17 >> 12) | (w18 << 52)) & mask;
    out[21] = (w18 >> 3) & mask;
    out[22] = ((w18 >> 58) | (w19 << 6)) & mask;
    out[23] = ((w19 >> 49) | (w20 << 15)) & mask;
    out[24] = ((w20 >> 40) | (w21 << 24)) & mask;
    out[25] = ((w21 >> 31) | (w22 << 33)) & mask;
    out[26] = ((w22 >> 22) | (w23 << 42)) & mask;
    out[27] = ((w23 >> 13) | (w24 << 51)) & mask;
    out[28] = (w24 >> 4) & mask;
    out[29] = ((w24 >> 59) | (w25 << 5)) & mask;
    out[30] = ((w25 >> 50) | (w26 << 14)) & mask;
    out[31] = ((w26 >> 41) | (w27 << 23)) & mask;

    return in + (27 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 56> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 56) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 56) | (w1 << 8)) & mask;
    out[2] = ((w1 >> 48) | (w2 << 16)) & mask;
    out[3] = ((w2 >> 40) | (w3 << 24)) & mask;
    out[4] = ((w3 >> 32) | (w4 << 32)) & mask;
    out[5] = ((w4 >> 24) | (w5 << 40)) & mask;
    out[6] = ((w5 >> 16) | (w6 << 48)) & mask;
    out[7] = w6 >> 8;
    out[8] = (w7) & mask;
    out[9] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[10] = ((w8 >> 48) | (w9 << 16)) & mask;
    out[11] = ((w9 >> 40) | (w10 << 24)) & mask;
    out[12] = ((w10 >> 32) | (w11 << 32)) & mask;
    out[13] = ((w11 >> 24) | (w12 << 40)) & mask;
    out[14] = ((w12 >> 16) | (w13 << 48)) & mask;
    out[15] = w13 >> 8;
    out[16] = (w14) & mask;
    out[17] = ((w14 >> 56) | (w15 << 8)) & mask;
    out[18] = ((w15 >> 48) | (w16 << 16)) & mask;
    out[19] = ((w16 >> 40) | (w17 << 24)) & mask;
    out[20] = ((w17 >> 32) | (w18 << 32)) & mask;
    out[21] = ((w18 >> 24) | (w19 << 40)) & mask;
    out[22] = ((w19 >> 16) | (w20 << 48)) & mask;
    out[23] = w20 >> 8;
    out[24] = (w21) & mask;
    out[25] = ((w21 >> 56) | (w22 << 8)) & mask;
    out[26] = ((w22 >> 48) | (w23 << 16)) & mask;
    out[27] = ((w23 >> 40) | (w24 << 24)) & mask;
    out[28] = ((w24 >> 32) | (w25 << 32)) & mask;
    out[29] = ((w25 >> 24) | (w26 << 40)) & mask;
    out[30] = ((w26 >> 16) | (w27 << 48)) & mask;
    out[31] = w27 >> 8;

    return in + (28 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 57> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 57) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 28 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 57) | (w1 << 7)) & mask;
    out[2] = ((w1 >> 50) | (w2 << 14)) & mask;
    out[3] = ((w2 >> 43) | (w3 << 21)) & mask;
    out[4] = ((w3 >> 36) | (w4 << 28)) & mask;
    out[5] = ((w4 >> 29) | (w5 << 35)) & mask;
    out[6] = ((w5 >> 22) | (w6 << 42)) & mask;
    out[7] = ((w6 >> 15) | (w7 << 49)) & mask;
    out[8] = ((w7 >> 8) | (w8 << 56)) & mask;
    out[9] = (w8 >> 1) & mask;
    out[10] = ((w8 >> 58) | (w9 << 6)) & mask;
    out[11] = ((w9 >> 51) | (w10 << 13)) & mask;
    out[12] = ((w10 >> 44) | (w11 << 20)) & mask;
    out[13] = ((w11 >> 37) | (w12 << 27)) & mask;
    out[14] = ((w12 >> 30) | (w13 << 34)) & mask;
    out[15] = ((w13 >> 23) | (w14 << 41)) & mask;
    out[16] = ((w14 >> 16) | (w15 << 48)) & mask;
    out[17] = ((w15 >> 9) | (w16 << 55)) & mask;
    out[18] = (w16 >> 2) & mask;
    out[19] = ((w16 >> 59) | (w17 << 5)) & mask;
    out[20] = ((w17 >> 52) | (w18 << 12)) & mask;
    out[21] = ((w18 >> 45) | (w19 << 19)) & mask;
    out[22] = ((w19 >> 38) | (w20 << 26)) & mask;
    out[23] = ((w20 >> 31) | (w21 << 33)) & mask;
    out[24] = ((w21 >> 24) | (w22 << 40)) & mask;
    out[25] = ((w22 >> 17) | (w23 << 47)) & mask;
    out[26] = ((w23 >> 10) | (w24 << 54)) & mask;
    out[27] = (w24 >> 3) & mask;
    out[28] = ((w24 >> 60) | (w25 << 4)) & mask;
    out[29] = ((w25 >> 53) | (w26 << 11)) & mask;
    out[30] = ((w26 >> 46) | (w27 << 18)) & mask;
    out[31] = ((w27 >> 39) | (w28 << 25)) & mask;

    return in + (28 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 58> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 58) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 58) | (w1 << 6)) & mask;
    out[2] = ((w1 >> 52) | (w2 << 12)) & mask;
    out[3] = ((w2 >> 46) | (w3 << 18)) & mask;
    out[4] = ((w3 >> 40) | (w4 << 24)) & mask;
    out[5] = ((w4 >> 34) | (w5 << 30)) & mask;
    out[6] = ((w5 >> 28) | (w6 << 36)) & mask;
    out[7] = ((w6 >> 22) | (w7 << 42)) & mask;
    out[8] = ((w7 >> 16) | (w8 << 48)) & mask;
    out[9] = ((w8 >> 10) | (w9 << 54)) & mask;
    out[10] = (w9 >> 4) & mask;
    out[11] = ((w9 >> 62) | (w10 << 2)) & mask;
    out[12] = ((w10 >> 56) | (w11 << 8)) & mask;
    out[13] = ((w11 >> 50) | (w12 << 14)) & mask;
    out[14] = ((w12 >> 44) | (w13 << 20)) & mask;
    out[15] = ((w13 >> 38) | (w14 << 26)) & mask;
    out[16] = ((w14 >> 32) | (w15 << 32)) & mask;
    out[17] = ((w15 >> 26) | (w16 << 38)) & mask;
    out[18] = ((w16 >> 20) | (w17 << 44)) & mask;
    out[19] = ((w17 >> 14) | (w18 << 50)) & mask;
    out[20] = ((w18 >> 8) | (w19 << 56)) & mask;
    out[21] = (w19 >> 2) & mask;
    out[22] = ((w19 >> 60) | (w20 << 4)) & mask;
    out[23] = ((w20 >> 54) | (w21 << 10)) & mask;
    out[24] = ((w21 >> 48) | (w22 << 16)) & mask;
    out[25] = ((w22 >> 42) | (w23 << 22)) & mask;
    out[26] = ((w23 >> 36) | (w24 << 28)) & mask;
    out[27] = ((w24 >> 30) | (w25 << 34)) & mask;
    out[28] = ((w25 >> 24) | (w26 << 40)) & mask;
    out[29] = ((w26 >> 18) | (w27 << 46)) & mask;
    out[30] = ((w27 >> 12) | (w28 << 52)) & mask;
    out[31] = w28 >> 6;

    return in + (29 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 59> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 59) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    const auto w29 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 29 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 59) | (w1 << 5)) & mask;
    out[2] = ((w1 >> 54) | (w2 << 10)) & mask;
    out[3] = ((w2 >> 49) | (w3 << 15)) & mask;
    out[4] = ((w3 >> 44) | (w4 << 20)) & mask;
    out[5] = ((w4 >> 39) | (w5 << 25)) & mask;
    out[6] = ((w5 >> 34) | (w6 << 30)) & mask;
    out[7] = ((w6 >> 29) | (w7 << 35)) & mask;
    out[8] = ((w7 >> 24) | (w8 << 40)) & mask;
    out[9] = ((w8 >> 19) | (w9 << 45)) & mask;
    out[10] = ((w9 >> 14) | (w10 << 50)) & mask;
    out[11] = ((w10 >> 9) | (w11 << 55)) & mask;
    out[12] = (w11 >> 4) & mask;
    out[13] = ((w11 >> 63) | (w12 << 1)) & mask;
    out[14] = ((w12 >> 58) | (w13 << 6)) & mask;
    out[15] = ((w13 >> 53) | (w14 << 11)) & mask;
    out[16] = ((w14 >> 48) | (w15 << 16)) & mask;
    out[17] = ((w15 >> 43) | (w16 << 21)) & mask;
    out[18] = ((w16 >> 38) | (w17 << 26)) & mask;
    out[19] = ((w17 >> 33) | (w18 << 31)) & mask;
    out[20] = ((w18 >> 28) | (w19 << 36)) & mask;
    out[21] = ((w19 >> 23) | (w20 << 41)) & mask;
    out[22] = ((w20 >> 18) | (w21 << 46)) & mask;
    out[23] = ((w21 >> 13) | (w22 << 51)) & mask;
    out[24] = ((w22 >> 8) | (w23 << 56)) & mask;
    out[25] = (w23 >> 3) & mask;
    out[26] = ((w23 >> 62) | (w24 << 2)) & mask;
    out[27] = ((w24 >> 57) | (w25 << 7)) & mask;
    out[28] = ((w25 >> 52) | (w26 << 12)) & mask;
    out[29] = ((w26 >> 47) | (w27 << 17)) & mask;
    out[30] = ((w27 >> 42) | (w28 << 22)) & mask;
    out[31] = ((w28 >> 37) | (w29 << 27)) & mask;

    return in + (29 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 60> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 60) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    const auto w29 = LoadInt<uint64_t>(in + 29 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 60) | (w1 << 4)) & mask;
    out[2] = ((w1 >> 56) | (w2 << 8)) & mask;
    out[3] = ((w2 >> 52) | (w3 << 12)) & mask;
    out[4] = ((w3 >> 48) | (w4 << 16)) & mask;
    out[5] = ((w4 >> 44) | (w5 << 20)) & mask;
    out[6] = ((w5 >> 40) | (w6 << 24)) & mask;
    out[7] = ((w6 >> 36) | (w7 << 28)) & mask;
    out[8] = ((w7 >> 32) | (w8 << 32)) & mask;
    out[9] = ((w8 >> 28) | (w9 << 36)) & mask;
    out[10] = ((w9 >> 24) | (w10 << 40)) & mask;
    out[11] = ((w10 >> 20) | (w11 << 44)) & mask;
    out[12] = ((w11 >> 16) | (w12 << 48)) & mask;
    out[13] = ((w12 >> 12) | (w13 << 52)) & mask;
    out[14] = ((w13 >> 8) | (w14 << 56)) & mask;
    out[15] = w14 >> 4;
    out[16] = (w15) & mask;
    out[17] = ((w15 >> 60) | (w16 << 4)) & mask;
    out[18] = ((w16 >> 56) | (w17 << 8)) & mask;
    out[19] = ((w17 >> 52) | (w18 << 12)) & mask;
    out[20] = ((w18 >> 48) | (w19 << 16)) & mask;
    out[21] = ((w19 >> 44) | (w20 << 20)) & mask;
    out[22] = ((w20 >> 40) | (w21 << 24)) & mask;
    out[23] = ((w21 >> 36) | (w22 << 28)) & mask;
    out[24] = ((w22 >> 32) | (w23 << 32)) & mask;
    out[25] = ((w23 >> 28) | (w24 << 36)) & mask;
    out[26] = ((w24 >> 24) | (w25 << 40)) & mask;
    out[27] = ((w25 >> 20) | (w26 << 44)) & mask;
    out[28] = ((w26 >> 16) | (w27 << 48)) & mask;
    out[29] = ((w27 >> 12) | (w28 << 52)) & mask;
    out[30] = ((w28 >> 8) | (w29 << 56)) & mask;
    out[31] = w29 >> 4;

    return in + (30 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 61> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 61) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    const auto w29 = LoadInt<uint64_t>(in + 29 * 8);
    const auto w30 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 30 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 61) | (w1 << 3)) & mask;
    out[2] = ((w1 >> 58) | (w2 << 6)) & mask;
    out[3] = ((w2 >> 55) | (w3 << 9)) & mask;
    out[4] = ((w3 >> 52) | (w4 << 12)) & mask;
    out[5] = ((w4 >> 49) | (w5 << 15)) & mask;
    out[6] = ((w5 >> 46) | (w6 << 18)) & mask;
    out[7] = ((w6 >> 43) | (w7 << 21)) & mask;
    out[8] = ((w7 >> 40) | (w8 << 24)) & mask;
    out[9] = ((w8 >> 37) | (w9 << 27)) & mask;
    out[10] = ((w9 >> 34) | (w10 << 30)) & mask;
    out[11] = ((w10 >> 31) | (w11 << 33)) & mask;
    out[12] = ((w11 >> 28) | (w12 << 36)) & mask;
    out[13] = ((w12 >> 25) | (w13 << 39)) & mask;
    out[14] = ((w13 >> 22) | (w14 << 42)) & mask;
    out[15] = ((w14 >> 19) | (w15 << 45)) & mask;
    out[16] = ((w15 >> 16) | (w16 << 48)) & mask;
    out[17] = ((w16 >> 13) | (w17 << 51)) & mask;
    out[18] = ((w17 >> 10) | (w18 << 54)) & mask;
    out[19] = ((w18 >> 7) | (w19 << 57)) & mask;
    out[20] = ((w19 >> 4) | (w20 << 60)) & mask;
    out[21] = (w20 >> 1) & mask;
    out[22] = ((w20 >> 62) | (w21 << 2)) & mask;
    out[23] = ((w21 >> 59) | (w22 << 5)) & mask;
    out[24] = ((w22 >> 56) | (w23 << 8)) & mask;
    out[25] = ((w23 >> 53) | (w24 << 11)) & mask;
    out[26] = ((w24 >> 50) | (w25 << 14)) & mask;
    out[27] = ((w25 >> 47) | (w26 << 17)) & mask;
    out[28] = ((w26 >> 44) | (w27 << 20)) & mask;
    out[29] = ((w27 >> 41) | (w28 << 23)) & mask;
    out[30] = ((w28 >> 38) | (w29 << 26)) & mask;
    out[31] = ((w29 >> 35) | (w30 << 29)) & mask;

    return in + (30 * 8 + 4);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 62> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 62) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    const auto w29 = LoadInt<uint64_t>(in + 29 * 8);
    const auto w30 = LoadInt<uint64_t>(in + 30 * 8);
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 62) | (w1 << 2)) & mask;
    out[2] = ((w1 >> 60) | (w2 << 4)) & mask;
    out[3] = ((w2 >> 58) | (w3 << 6)) & mask;
    out[4] = ((w3 >> 56) | (w4 << 8)) & mask;
    out[5] = ((w4 >> 54) | (w5 << 10)) & mask;
    out[6] = ((w5 >> 52) | (w6 << 12)) & mask;
    out[7] = ((w6 >> 50) | (w7 << 14)) & mask;
    out[8] = ((w7 >> 48) | (w8 << 16)) & mask;
    out[9] = ((w8 >> 46) | (w9 << 18)) & mask;
    out[10] = ((w9 >> 44) | (w10 << 20)) & mask;
    out[11] = ((w10 >> 42) | (w11 << 22)) & mask;
    out[12] = ((w11 >> 40) | (w12 << 24)) & mask;
    out[13] = ((w12 >> 38) | (w13 << 26)) & mask;
    out[14] = ((w13 >> 36) | (w14 << 28)) & mask;
    out[15] = ((w14 >> 34) | (w15 << 30)) & mask;
    out[16] = ((w15 >> 32) | (w16 << 32)) & mask;
    out[17] = ((w16 >> 30) | (w17 << 34)) & mask;
    out[18] = ((w17 >> 28) | (w18 << 36)) & mask;
    out[19] = ((w18 >> 26) | (w19 << 38)) & mask;
    out[20] = ((w19 >> 24) | (w20 << 40)) & mask;
    out[21] = ((w20 >> 22) | (w21 << 42)) & mask;
    out[22] = ((w21 >> 20) | (w22 << 44)) & mask;
    out[23] = ((w22 >> 18) | (w23 << 46)) & mask;
    out[24] = ((w23 >> 16) | (w24 << 48)) & mask;
    out[25] = ((w24 >> 14) | (w25 << 50)) & mask;
    out[26] = ((w25 >> 12) | (w26 << 52)) & mask;
    out[27] = ((w26 >> 10) | (w27 << 54)) & mask;
    out[28] = ((w27 >> 8) | (w28 << 56)) & mask;
    out[29] = ((w28 >> 6) | (w29 << 58)) & mask;
    out[30] = ((w29 >> 4) | (w30 << 60)) & mask;
    out[31] = w30 >> 2;

    return in + (31 * 8);
  }
};

template<>
struct ScalarUnpackerForWidth<uint64_t, 63> {

  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t mask = ((uint64_t{1} << 63) - uint64_t{1});

    const auto w0 = LoadInt<uint64_t>(in + 0 * 8);
    const auto w1 = LoadInt<uint64_t>(in + 1 * 8);
    const auto w2 = LoadInt<uint64_t>(in + 2 * 8);
    const auto w3 = LoadInt<uint64_t>(in + 3 * 8);
    const auto w4 = LoadInt<uint64_t>(in + 4 * 8);
    const auto w5 = LoadInt<uint64_t>(in + 5 * 8);
    const auto w6 = LoadInt<uint64_t>(in + 6 * 8);
    const auto w7 = LoadInt<uint64_t>(in + 7 * 8);
    const auto w8 = LoadInt<uint64_t>(in + 8 * 8);
    const auto w9 = LoadInt<uint64_t>(in + 9 * 8);
    const auto w10 = LoadInt<uint64_t>(in + 10 * 8);
    const auto w11 = LoadInt<uint64_t>(in + 11 * 8);
    const auto w12 = LoadInt<uint64_t>(in + 12 * 8);
    const auto w13 = LoadInt<uint64_t>(in + 13 * 8);
    const auto w14 = LoadInt<uint64_t>(in + 14 * 8);
    const auto w15 = LoadInt<uint64_t>(in + 15 * 8);
    const auto w16 = LoadInt<uint64_t>(in + 16 * 8);
    const auto w17 = LoadInt<uint64_t>(in + 17 * 8);
    const auto w18 = LoadInt<uint64_t>(in + 18 * 8);
    const auto w19 = LoadInt<uint64_t>(in + 19 * 8);
    const auto w20 = LoadInt<uint64_t>(in + 20 * 8);
    const auto w21 = LoadInt<uint64_t>(in + 21 * 8);
    const auto w22 = LoadInt<uint64_t>(in + 22 * 8);
    const auto w23 = LoadInt<uint64_t>(in + 23 * 8);
    const auto w24 = LoadInt<uint64_t>(in + 24 * 8);
    const auto w25 = LoadInt<uint64_t>(in + 25 * 8);
    const auto w26 = LoadInt<uint64_t>(in + 26 * 8);
    const auto w27 = LoadInt<uint64_t>(in + 27 * 8);
    const auto w28 = LoadInt<uint64_t>(in + 28 * 8);
    const auto w29 = LoadInt<uint64_t>(in + 29 * 8);
    const auto w30 = LoadInt<uint64_t>(in + 30 * 8);
    const auto w31 = static_cast<uint64_t>(LoadInt<uint32_t>(in + 31 * 8));
    out[0] = (w0) & mask;
    out[1] = ((w0 >> 63) | (w1 << 1)) & mask;
    out[2] = ((w1 >> 62) | (w2 << 2)) & mask;
    out[3] = ((w2 >> 61) | (w3 << 3)) & mask;
    out[4] = ((w3 >> 60) | (w4 << 4)) & mask;
    out[5] = ((w4 >> 59) | (w5 << 5)) & mask;
    out[6] = ((w5 >> 58) | (w6 << 6)) & mask;
    out[7] = ((w6 >> 57) | (w7 << 7)) & mask;
    out[8] = ((w7 >> 56) | (w8 << 8)) & mask;
    out[9] = ((w8 >> 55) | (w9 << 9)) & mask;
    out[10] = ((w9 >> 54) | (w10 << 10)) & mask;
    out[11] = ((w10 >> 53) | (w11 << 11)) & mask;
    out[12] = ((w11 >> 52) | (w12 << 12)) & mask;
    out[13] = ((w12 >> 51) | (w13 << 13)) & mask;
    out[14] = ((w13 >> 50) | (w14 << 14)) & mask;
    out[15] = ((w14 >> 49) | (w15 << 15)) & mask;
    out[16] = ((w15 >> 48) | (w16 << 16)) & mask;
    out[17] = ((w16 >> 47) | (w17 << 17)) & mask;
    out[18] = ((w17 >> 46) | (w18 << 18)) & mask;
    out[19] = ((w18 >> 45) | (w19 << 19)) & mask;
    out[20] = ((w19 >> 44) | (w20 << 20)) & mask;
    out[21] = ((w20 >> 43) | (w21 << 21)) & mask;
    out[22] = ((w21 >> 42) | (w22 << 22)) & mask;
    out[23] = ((w22 >> 41) | (w23 << 23)) & mask;
    out[24] = ((w23 >> 40) | (w24 << 24)) & mask;
    out[25] = ((w24 >> 39) | (w25 << 25)) & mask;
    out[26] = ((w25 >> 38) | (w26 << 26)) & mask;
    out[27] = ((w26 >> 37) | (w27 << 27)) & mask;
    out[28] = ((w27 >> 36) | (w28 << 28)) & mask;
    out[29] = ((w28 >> 35) | (w29 << 29)) & mask;
    out[30] = ((w29 >> 34) | (w30 << 30)) & mask;
    out[31] = ((w30 >> 33) | (w31 << 31)) & mask;

    return in + (31 * 8 + 4);
  }
};


}  // namespace
}  // namespace arrow::internal

