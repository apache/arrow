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
//   python cpp/src/arrow/util/bpacking_simd_codegen.py 128

#pragma once

#include <cstdint>
#include <cstring>

#include <xsimd/xsimd.hpp>

#include "arrow/util/ubsan.h"

namespace arrow::internal {
namespace {

using ::arrow::util::SafeLoadAs;

template<typename Uint, int BitWidth>
struct Simd128UnpackerForWidth;

template<int kBitWidth>
struct Simd128UnpackerForWidth<bool, kBitWidth> {

  static constexpr int kValuesUnpacked = Simd128UnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, bool* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = Simd128UnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<bool>(buffer[k]);
    }
    return in;
  }
};

template<int kBitWidth>
struct Simd128UnpackerForWidth<uint8_t, kBitWidth> {

  static constexpr int kValuesUnpacked = Simd128UnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint8_t* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = Simd128UnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<uint8_t>(buffer[k]);
    }
    return in;
  }
};

template<int kBitWidth>
struct Simd128UnpackerForWidth<uint16_t, kBitWidth> {

  static constexpr int kValuesUnpacked = Simd128UnpackerForWidth<uint32_t, kBitWidth>::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    uint32_t buffer[kValuesUnpacked] = {};
    in = Simd128UnpackerForWidth<uint32_t, kBitWidth>::unpack(in, buffer);
    for(int k = 0; k< kValuesUnpacked; ++k) {
      out[k] = static_cast<uint16_t>(buffer[k]);
    }
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 1> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 1-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 1, 2, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 4, 5, 6, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 8, 9, 10, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 12, 13, 14, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 16, 17, 18, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 20, 21, 22, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 24, 25, 26, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 1-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 28, 29, 30, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 1 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 2> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 2-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 2, 4, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 8, 10, 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 16, 18, 20, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 24, 26, 28, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 2, 4, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 8, 10, 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 16, 18, 20, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 2-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 24, 26, 28, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 2 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 3> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 3-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 3, 6, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 12, 15, 18, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 24, 27, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 4, 7, 10, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 16, 19, 22, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 2) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 28, 0, 2, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 8, 11, 14, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 3-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 20, 23, 26, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 3 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 4> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xf;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 4-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 16, 20, 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 16, 20, 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 16, 20, 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 4-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 16, 20, 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 4 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 5> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 5-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 5, 10, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 20, 25, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 8, 13, 18, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 1, 6, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 3) << 1,
    };
    shifts = simd_batch{ 16, 21, 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 4, 9, 14, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 4) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 24, 0, 2, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 5-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 12, 17, 22, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 5 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 6> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 6-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 6, 12, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 24, 0, 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 16, 22, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 8, 14, 20, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 6, 12, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 24, 0, 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 5) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 16, 22, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 6-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 8, 14, 20, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 6 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 7> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 7-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 7, 14, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 1) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 3, 10, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 2) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 24, 0, 6, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 3) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 20, 0, 2, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 16, 23, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 5) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 12, 19, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 6) << 3,
    };
    shifts = simd_batch{ 8, 15, 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 7-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 4, 11, 18, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 7 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 8> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 8-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 8-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 0, 8, 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 8 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 9> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 9-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 1) << 5,
    };
    shifts = simd_batch{ 0, 9, 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 2) << 1,
    };
    shifts = simd_batch{ 4, 13, 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 3) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 8, 17, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 12, 21, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 5) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 16, 0, 2, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 6) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 20, 0, 6, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 7) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 0, 1, 10, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 9-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 0, 5, 14, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 9 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 10> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 10-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
    };
    shifts = simd_batch{ 0, 10, 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 8, 18, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 3) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 16, 0, 4, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 0, 2, 12, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 6) << 2,
    };
    shifts = simd_batch{ 0, 10, 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 7) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 8, 18, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 8) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 16, 0, 4, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 10-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 9) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9),
    };
    shifts = simd_batch{ 0, 2, 12, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 10 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 11> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 11-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 1) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 11, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 2) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 12, 0, 2, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 3) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 4) << 7,
    };
    shifts = simd_batch{ 0, 3, 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 5) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 4, 15, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 6) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 16, 0, 6, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 7) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 8) << 3,
    };
    shifts = simd_batch{ 0, 7, 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 9) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 9),
    };
    shifts = simd_batch{ 8, 19, 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 11-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 10) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10),
    };
    shifts = simd_batch{ 20, 0, 10, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 11 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 12> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xfff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 12-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 1) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 12, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 16, 0, 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 0, 12, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 5) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 16, 0, 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 7) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 0, 12, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 16, 0, 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 10) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 10),
    };
    shifts = simd_batch{ 0, 12, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 12-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 11) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11),
    };
    shifts = simd_batch{ 16, 0, 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 12 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 13> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 13-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 1) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 13, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 2) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 3) << 5,
    };
    shifts = simd_batch{ 0, 1, 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 4) << 11,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 8, 0, 2, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 5) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 6) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 0, 9, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 7) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 8) << 9,
    };
    shifts = simd_batch{ 16, 0, 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 9) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 9),
    };
    shifts = simd_batch{ 4, 17, 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 10) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 11) << 1,
    };
    shifts = simd_batch{ 0, 5, 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 13-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 12) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12),
    };
    shifts = simd_batch{ 12, 0, 6, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 13 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 14> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 14-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 1) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 14, 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 2) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 3) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 6, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 5) << 6,
    };
    shifts = simd_batch{ 16, 0, 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 6) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 8, 0, 4, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 0, 14, 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 9) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 10) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 10),
    };
    shifts = simd_batch{ 0, 6, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 11) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 12) << 6,
    };
    shifts = simd_batch{ 16, 0, 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 14-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 13) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13),
    };
    shifts = simd_batch{ 8, 0, 4, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 14 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 15> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 15-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 15, 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 3) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 11, 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 5) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 0, 7, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 6) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 7) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 0, 3, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 8) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 9) << 3,
    };
    shifts = simd_batch{ 16, 0, 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 10) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 11) << 7,
    };
    shifts = simd_batch{ 12, 0, 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 12) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 13) << 11,
    };
    shifts = simd_batch{ 8, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 15-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 14) << 13,
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14),
    };
    shifts = simd_batch{ 4, 0, 2, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 15 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 16> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 16-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 16-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15),
    };
    shifts = simd_batch{ 0, 16, 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 16 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 17> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 17-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 1) << 15,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 2) << 13,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 3) << 11,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 4) << 9,
    };
    shifts = simd_batch{ 4, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 5) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 6) << 5,
    };
    shifts = simd_batch{ 8, 0, 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 7) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 8) << 1,
    };
    shifts = simd_batch{ 12, 0, 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 9) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 10) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 10),
    };
    shifts = simd_batch{ 0, 1, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 11) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 12) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 12),
    };
    shifts = simd_batch{ 0, 5, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 13) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 14) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 14),
    };
    shifts = simd_batch{ 0, 9, 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 17-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 15) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 16) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 16),
    };
    shifts = simd_batch{ 0, 13, 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 17 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 18> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 18-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 1) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 2) << 10,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 3) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
    };
    shifts = simd_batch{ 8, 0, 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 5) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 6) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 0, 2, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 7) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 0, 10, 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 10) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 11) << 10,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 12) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 13) << 2,
    };
    shifts = simd_batch{ 8, 0, 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 14) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 15) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 15),
    };
    shifts = simd_batch{ 0, 2, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 18-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 16) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 16),
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 17) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 17),
    };
    shifts = simd_batch{ 0, 10, 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 18 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 19> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 19-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 1) << 13,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 2) << 7,
    };
    shifts = simd_batch{ 0, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2),
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 3) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 4) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 12, 0, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 5) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 6) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 7) << 15,
    };
    shifts = simd_batch{ 0, 11, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 8) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 9) << 3,
    };
    shifts = simd_batch{ 4, 0, 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 10) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 11) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 11),
    };
    shifts = simd_batch{ 0, 3, 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 12) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 13) << 17,
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 14) << 11,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 15) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 16) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 16),
    };
    shifts = simd_batch{ 8, 0, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 19-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 17) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 18) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 18),
    };
    shifts = simd_batch{ 0, 7, 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 19 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 20> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xfffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 20-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 1) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
    };
    shifts = simd_batch{ 0, 0, 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 3) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4),
    };
    shifts = simd_batch{ 0, 4, 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 6) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 7) << 4,
    };
    shifts = simd_batch{ 0, 0, 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 8) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 9) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 9),
    };
    shifts = simd_batch{ 0, 4, 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 11) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 12) << 4,
    };
    shifts = simd_batch{ 0, 0, 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 13) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 14) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 14),
    };
    shifts = simd_batch{ 0, 4, 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 16) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 16),
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 17) << 4,
    };
    shifts = simd_batch{ 0, 0, 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 20-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 18) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 18),
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 19) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 19),
    };
    shifts = simd_batch{ 0, 4, 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 20 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 21> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 21-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 1) << 11,
      SafeLoadAs<uint32_t>(in + 4 * 1),
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 2) << 1,
    };
    shifts = simd_batch{ 0, 0, 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 3) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 4) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 5) << 13,
    };
    shifts = simd_batch{ 0, 9, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 6) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 7) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 7),
    };
    shifts = simd_batch{ 8, 0, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 9) << 15,
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 10) << 5,
    };
    shifts = simd_batch{ 0, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 11) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 12) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 13) << 17,
    };
    shifts = simd_batch{ 0, 5, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 14) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 15) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 15),
    };
    shifts = simd_batch{ 4, 0, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 16) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 17) << 19,
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 18) << 9,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 21-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 19) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 19),
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 20) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 20),
    };
    shifts = simd_batch{ 0, 1, 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 21 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 22> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 22-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 1) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 2) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 3) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 4) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 5) << 6,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 6) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 7) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 8) << 14,
    };
    shifts = simd_batch{ 0, 6, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 9) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 10) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 10),
    };
    shifts = simd_batch{ 8, 0, 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11),
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 12) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 13) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 13),
    };
    shifts = simd_batch{ 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 14) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 15) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 16) << 6,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 17) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 18) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 19) << 14,
    };
    shifts = simd_batch{ 0, 6, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 22-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 19),
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 20) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 21) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 21),
    };
    shifts = simd_batch{ 8, 0, 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 22 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 23> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 23-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 1) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 2) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 0, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 3) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 4) << 13,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 5) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 0, 0, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 6) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 7) << 17,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 8) << 3,
    };
    shifts = simd_batch{ 0, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 9) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 11 | SafeLoadAs<uint32_t>(in + 4 * 10) << 21,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 11) << 7,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 12) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 13) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 14) << 11,
    };
    shifts = simd_batch{ 0, 7, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 15) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 16) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 17) << 15,
    };
    shifts = simd_batch{ 0, 3, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 18) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 19) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 20) << 19,
    };
    shifts = simd_batch{ 8, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 23-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 20),
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 21) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 22) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 22),
    };
    shifts = simd_batch{ 4, 0, 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 23 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 24> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 24-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 1) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 2) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 2),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 5) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 5),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6),
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 7) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 8) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 8),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 10) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 11) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 11),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12),
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 13) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 14) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 14),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 16) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 17) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 17),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 18),
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 19) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 20) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 20),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 24-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 21),
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 22) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 23) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 23),
    };
    shifts = simd_batch{ 0, 0, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 24 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 25> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 25-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 1) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 2) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 11 | SafeLoadAs<uint32_t>(in + 4 * 3) << 21,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3),
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 4) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 5) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 6) << 17,
    };
    shifts = simd_batch{ 4, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 7) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 8) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 9) << 13,
    };
    shifts = simd_batch{ 0, 1, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 10) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 11) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 12) << 9,
    };
    shifts = simd_batch{ 0, 5, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 13) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 9 | SafeLoadAs<uint32_t>(in + 4 * 14) << 23,
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 15) << 5,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 16) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 17) << 19,
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 18) << 1,
    };
    shifts = simd_batch{ 0, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 19) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 20) << 15,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 21) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 21),
    };
    shifts = simd_batch{ 0, 0, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 25-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 22) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 23) << 11,
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 24) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 24),
    };
    shifts = simd_batch{ 0, 0, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 25 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 26> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 26-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 1) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 2) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 3) << 18,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 4) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 4),
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 5) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 6) << 10,
    };
    shifts = simd_batch{ 0, 2, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 7) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 8) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 8),
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 9) << 2,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 10) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 11) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 12) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 12),
    };
    shifts = simd_batch{ 0, 0, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13),
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 14) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 15) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 16) << 18,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 17) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 17),
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 18) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 19) << 10,
    };
    shifts = simd_batch{ 0, 2, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 20) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 21) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 21),
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 22) << 2,
    };
    shifts = simd_batch{ 0, 0, 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 26-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 23) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 24) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 25) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 25),
    };
    shifts = simd_batch{ 0, 0, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 26 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 27> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 27-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 1) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 2) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 3) << 15,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 4) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 7 | SafeLoadAs<uint32_t>(in + 4 * 5) << 25,
      SafeLoadAs<uint32_t>(in + 4 * 5),
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 6) << 3,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 7) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 8) << 13,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 9) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 9 | SafeLoadAs<uint32_t>(in + 4 * 10) << 23,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10),
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 11) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 12) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 13) << 11,
    };
    shifts = simd_batch{ 4, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 14) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 11 | SafeLoadAs<uint32_t>(in + 4 * 15) << 21,
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 6 | SafeLoadAs<uint32_t>(in + 4 * 16) << 26,
      SafeLoadAs<uint32_t>(in + 4 * 16),
    };
    shifts = simd_batch{ 0, 0, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 17) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 18) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 19) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 20) << 19,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 21) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 21),
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 22) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 23) << 7,
    };
    shifts = simd_batch{ 0, 3, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 27-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 24) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 25) << 17,
      SafeLoadAs<uint32_t>(in + 4 * 25) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 26) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 26),
    };
    shifts = simd_batch{ 0, 0, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 27 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 28> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0xfffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 28-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 1) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 2) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 3) << 12,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 4) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 5) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 6) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 6),
    };
    shifts = simd_batch{ 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7),
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 8) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 9) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 10) << 12,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 11) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 12) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 13) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 13),
    };
    shifts = simd_batch{ 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14),
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 15) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 16) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 17) << 12,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 18) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 19) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 20) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 20),
    };
    shifts = simd_batch{ 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 21),
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 22) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 23) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 24) << 12,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 28-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 25) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 25) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 26) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 26) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 27) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 27),
    };
    shifts = simd_batch{ 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 28 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 29> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x1fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 29-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 1) << 3,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 2) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 3) << 9,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 4) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 5) << 15,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 6) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 11 | SafeLoadAs<uint32_t>(in + 4 * 7) << 21,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 8) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 5 | SafeLoadAs<uint32_t>(in + 4 * 9) << 27,
      SafeLoadAs<uint32_t>(in + 4 * 9),
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 10) << 1,
    };
    shifts = simd_batch{ 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 11) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 12) << 7,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 13) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 14) << 13,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 15) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 16) << 19,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 17) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 7 | SafeLoadAs<uint32_t>(in + 4 * 18) << 25,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 4 | SafeLoadAs<uint32_t>(in + 4 * 19) << 28,
      SafeLoadAs<uint32_t>(in + 4 * 19),
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 20) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 21) << 5,
    };
    shifts = simd_batch{ 0, 1, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 22) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 23) << 11,
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 24) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 25) << 17,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 29-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 25) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 26) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 26) >> 9 | SafeLoadAs<uint32_t>(in + 4 * 27) << 23,
      SafeLoadAs<uint32_t>(in + 4 * 27) >> 6 | SafeLoadAs<uint32_t>(in + 4 * 28) << 26,
      SafeLoadAs<uint32_t>(in + 4 * 28),
    };
    shifts = simd_batch{ 0, 0, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 29 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 30> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x3fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 30-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 1) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 2) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 3) << 6,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 4) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 5) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 6) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 7) << 14,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 8) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 9) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 10) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 11) << 22,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 12) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 6 | SafeLoadAs<uint32_t>(in + 4 * 13) << 26,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 4 | SafeLoadAs<uint32_t>(in + 4 * 14) << 28,
      SafeLoadAs<uint32_t>(in + 4 * 14),
    };
    shifts = simd_batch{ 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15),
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 16) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 17) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 18) << 6,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 19) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 20) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 21) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 22) << 14,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 23) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 24) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 25) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 25) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 26) << 22,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 30-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 26) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 27) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 27) >> 6 | SafeLoadAs<uint32_t>(in + 4 * 28) << 26,
      SafeLoadAs<uint32_t>(in + 4 * 28) >> 4 | SafeLoadAs<uint32_t>(in + 4 * 29) << 28,
      SafeLoadAs<uint32_t>(in + 4 * 29),
    };
    shifts = simd_batch{ 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 30 * 4;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 31> {

  using simd_batch = xsimd::make_sized_batch_t<uint32_t, 4>;
  static constexpr int kValuesUnpacked = 32;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    constexpr uint32_t kMask = 0x7fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 31-bit bundles 0 to 3
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 0),
      SafeLoadAs<uint32_t>(in + 4 * 0) >> 31 | SafeLoadAs<uint32_t>(in + 4 * 1) << 1,
      SafeLoadAs<uint32_t>(in + 4 * 1) >> 30 | SafeLoadAs<uint32_t>(in + 4 * 2) << 2,
      SafeLoadAs<uint32_t>(in + 4 * 2) >> 29 | SafeLoadAs<uint32_t>(in + 4 * 3) << 3,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 4 to 7
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 3) >> 28 | SafeLoadAs<uint32_t>(in + 4 * 4) << 4,
      SafeLoadAs<uint32_t>(in + 4 * 4) >> 27 | SafeLoadAs<uint32_t>(in + 4 * 5) << 5,
      SafeLoadAs<uint32_t>(in + 4 * 5) >> 26 | SafeLoadAs<uint32_t>(in + 4 * 6) << 6,
      SafeLoadAs<uint32_t>(in + 4 * 6) >> 25 | SafeLoadAs<uint32_t>(in + 4 * 7) << 7,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 8 to 11
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 7) >> 24 | SafeLoadAs<uint32_t>(in + 4 * 8) << 8,
      SafeLoadAs<uint32_t>(in + 4 * 8) >> 23 | SafeLoadAs<uint32_t>(in + 4 * 9) << 9,
      SafeLoadAs<uint32_t>(in + 4 * 9) >> 22 | SafeLoadAs<uint32_t>(in + 4 * 10) << 10,
      SafeLoadAs<uint32_t>(in + 4 * 10) >> 21 | SafeLoadAs<uint32_t>(in + 4 * 11) << 11,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 12 to 15
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 11) >> 20 | SafeLoadAs<uint32_t>(in + 4 * 12) << 12,
      SafeLoadAs<uint32_t>(in + 4 * 12) >> 19 | SafeLoadAs<uint32_t>(in + 4 * 13) << 13,
      SafeLoadAs<uint32_t>(in + 4 * 13) >> 18 | SafeLoadAs<uint32_t>(in + 4 * 14) << 14,
      SafeLoadAs<uint32_t>(in + 4 * 14) >> 17 | SafeLoadAs<uint32_t>(in + 4 * 15) << 15,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 16 to 19
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 15) >> 16 | SafeLoadAs<uint32_t>(in + 4 * 16) << 16,
      SafeLoadAs<uint32_t>(in + 4 * 16) >> 15 | SafeLoadAs<uint32_t>(in + 4 * 17) << 17,
      SafeLoadAs<uint32_t>(in + 4 * 17) >> 14 | SafeLoadAs<uint32_t>(in + 4 * 18) << 18,
      SafeLoadAs<uint32_t>(in + 4 * 18) >> 13 | SafeLoadAs<uint32_t>(in + 4 * 19) << 19,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 20 to 23
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 19) >> 12 | SafeLoadAs<uint32_t>(in + 4 * 20) << 20,
      SafeLoadAs<uint32_t>(in + 4 * 20) >> 11 | SafeLoadAs<uint32_t>(in + 4 * 21) << 21,
      SafeLoadAs<uint32_t>(in + 4 * 21) >> 10 | SafeLoadAs<uint32_t>(in + 4 * 22) << 22,
      SafeLoadAs<uint32_t>(in + 4 * 22) >> 9 | SafeLoadAs<uint32_t>(in + 4 * 23) << 23,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 24 to 27
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 23) >> 8 | SafeLoadAs<uint32_t>(in + 4 * 24) << 24,
      SafeLoadAs<uint32_t>(in + 4 * 24) >> 7 | SafeLoadAs<uint32_t>(in + 4 * 25) << 25,
      SafeLoadAs<uint32_t>(in + 4 * 25) >> 6 | SafeLoadAs<uint32_t>(in + 4 * 26) << 26,
      SafeLoadAs<uint32_t>(in + 4 * 26) >> 5 | SafeLoadAs<uint32_t>(in + 4 * 27) << 27,
    };
    shifts = simd_batch{ 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    // extract 31-bit bundles 28 to 31
    words = simd_batch{
      SafeLoadAs<uint32_t>(in + 4 * 27) >> 4 | SafeLoadAs<uint32_t>(in + 4 * 28) << 28,
      SafeLoadAs<uint32_t>(in + 4 * 28) >> 3 | SafeLoadAs<uint32_t>(in + 4 * 29) << 29,
      SafeLoadAs<uint32_t>(in + 4 * 29) >> 2 | SafeLoadAs<uint32_t>(in + 4 * 30) << 30,
      SafeLoadAs<uint32_t>(in + 4 * 30),
    };
    shifts = simd_batch{ 0, 0, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 4;

    in += 31 * 4;
    return in;
  }
};


template<>
struct Simd128UnpackerForWidth<uint64_t, 1> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 1-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 2, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 4, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 6, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 8, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 10, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 12, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 14, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 16, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 18, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 20, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 22, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 26, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 28, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 30, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 32, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 34, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 36, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 38, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 40, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 42, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 44, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 46, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 50, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 52, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 54, 55 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 56, 57 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 58, 59 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 60, 61 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 1-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 62, 63 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 1 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 2> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 2-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 4, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 8, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 16, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 20, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 28, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 32, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 36, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 40, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 44, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 52, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 56, 58 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 60, 62 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 4, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 24, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 28, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 36, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 40, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 44, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 48, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 52, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 56, 58 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 2-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 60, 62 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 2 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 3> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 3-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 6, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 12, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 18, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 30, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 36, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 42, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 54, 57 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 1) << 1,
    };
    shifts = simd_batch{ 60, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 2, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 14, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 26, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 38, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 44, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 50, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 56, 59 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 4, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 10, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 22, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 28, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 34, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 40, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 46, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 52, 55 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 3-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 58, 61 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 3 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 4> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xf;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 4-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 16, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 32, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 40, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 56, 60 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 40, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 48, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 56, 60 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 32, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 40, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 48, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 56, 60 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 16, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 24, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 32, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 40, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 48, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 4-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 56, 60 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 4 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 5> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 5-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 10, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 20, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 30, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 40, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 50, 55 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 6, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 26, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 36, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 46, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 2) << 3,
    };
    shifts = simd_batch{ 56, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 2, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 12, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 22, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 32, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 42, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 52, 57 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 3) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 8, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 18, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 28, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 38, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 48, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 4) << 1,
    };
    shifts = simd_batch{ 58, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 4, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 14, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 24, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 34, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 44, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 5-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 54, 59 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 5 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 6> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 6-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 12, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 36, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 44, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
    };
    shifts = simd_batch{ 56, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 28, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 40, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 52, 58 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 12, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 24, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 36, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 48, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 8, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 20, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 32, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 44, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 5) << 2,
    };
    shifts = simd_batch{ 56, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 28, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 40, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 6-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 52, 58 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 6 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 7> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 7-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 14, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 28, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 42, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 1) << 1,
    };
    shifts = simd_batch{ 56, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 6, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 34, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 48, 55 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 12, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 26, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 40, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 3) << 3,
    };
    shifts = simd_batch{ 54, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 4, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 18, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 32, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 46, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 10, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 24, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 38, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 52, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 2, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 30, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 44, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 8, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 22, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 36, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 7-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 50, 57 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 7 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 8> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 8-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 16, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 32, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 8-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 48, 56 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 8 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 9> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 9-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 18, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 36, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 1) << 1,
    };
    shifts = simd_batch{ 54, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 26, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 44, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 34, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 3) << 3,
    };
    shifts = simd_batch{ 52, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 6, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 24, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 42, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 14, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 32, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 50, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 4, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 22, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 40, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 12, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 30, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 7) << 7,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 2, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 20, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 38, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 10, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 28, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 9-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 46, 55 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 9 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 10> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 10-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 20, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 40, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 36, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 12, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 32, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 3) << 2,
    };
    shifts = simd_batch{ 52, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 8, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 28, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 4) << 6,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 4, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 24, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 44, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 20, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 40, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 6) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 16, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 36, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 12, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 32, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
    };
    shifts = simd_batch{ 52, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 8, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 28, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 4, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 24, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 10-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 44, 54 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 10 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 11> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 11-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 22, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 1) << 9,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 2, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 24, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 2) << 7,
    };
    shifts = simd_batch{ 46, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 4, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 26, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 3) << 5,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 6, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 28, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 4) << 3,
    };
    shifts = simd_batch{ 50, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 8, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 30, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 5) << 1,
    };
    shifts = simd_batch{ 52, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 10, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 32, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 6) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 12, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 34, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 14, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 36, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 8) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 16, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 38, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 18, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 40, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 10) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 20, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 11-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 42, 53 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 11 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 12> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 12-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 24, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 40, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 24, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 32, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 40, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 24, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 7) << 4,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 32, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 16, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 40, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 24, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 10) << 4,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 8, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 32, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 11) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 16, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 12-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 40, 52 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 12 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 13> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 13-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 26, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 1) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 14, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 2) << 11,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 2, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 28, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 3) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 16, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 4) << 9,
    };
    shifts = simd_batch{ 42, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 4, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 30, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 18, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 6) << 7,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 6, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 32, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 7) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 20, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 8) << 5,
    };
    shifts = simd_batch{ 46, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 8, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 34, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 22, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 10) << 3,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 10, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 36, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 24, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 12) << 1,
    };
    shifts = simd_batch{ 50, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 12, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 13-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 38, 51 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 13 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 14> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 14-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 28, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 1) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 12, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 3) << 10,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 4, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 32, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 24, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 5) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 8, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 36, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 28, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 20, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 9) << 2,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 12, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 4, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 32, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 24, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 16, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 13) << 6,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 8, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 14-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 36, 50 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 14 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 15> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 15-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 30, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 26, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 22, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 18, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 4) << 1,
    };
    shifts = simd_batch{ 48, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 14, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 10, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 6) << 9,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 6, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 7) << 13,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 2, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 32, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 28, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 24, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 20, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 11) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 16, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 12) << 3,
    };
    shifts = simd_batch{ 46, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 12, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 13) << 7,
    };
    shifts = simd_batch{ 42, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 8, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 14) << 11,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 4, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 15-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 34, 49 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 15 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 16> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 16-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 16-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 32, 48 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 16 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 17> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 17-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 1) << 13,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 4, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 2) << 9,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 8, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 3) << 5,
    };
    shifts = simd_batch{ 42, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 12, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 4) << 1,
    };
    shifts = simd_batch{ 46, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 16, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 5) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 20, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 6) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 24, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 7) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 28, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 9) << 15,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 2, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 10) << 11,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 6, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 11) << 7,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 10, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 12) << 3,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 14, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 13) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 18, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 14) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 22, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 15) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 26, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 17-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 30, 47 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 17 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 18> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 18-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 1) << 10,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 8, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 24, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 5) << 14,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 4, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 12, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 20, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 28, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 8, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 16, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 24, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 13) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 14) << 14,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 4, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 15) << 6,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 12, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 20, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 18-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 28, 46 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 18 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 19> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 19-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 1) << 7,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 12, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 2) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 24, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 3) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 4) << 9,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 10, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 5) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 22, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 6) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 7) << 11,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 8, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 8) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 20, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 10) << 13,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 6, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 11) << 1,
    };
    shifts = simd_batch{ 44, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 18, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 12) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 13) << 15,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 4, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 14) << 3,
    };
    shifts = simd_batch{ 42, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 16, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 15) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 16) << 17,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 2, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 17) << 5,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 14, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 19-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 26, 45 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 19 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 20> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 20-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 16, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 8, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 24, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 6) << 4,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 16, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 8) << 12,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 8, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 9) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 24, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 16, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 12) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 13) << 12,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 8, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 14) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 24, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 16, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 8, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 20-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 24, 44 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 20 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 21> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 21-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 1) << 1,
    };
    shifts = simd_batch{ 42, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 20, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 3) << 3,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 18, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 7) << 7,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 14, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 9) << 9,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 12, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 11) << 11,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 10, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 13) << 13,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 8, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 14) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 15) << 15,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 6, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 17) << 17,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 4, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 18) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 19) << 19,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 2, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 21-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 22, 43 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 21 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 22> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 22-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 1) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 2) << 18,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 4, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 3) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 4) << 14,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 8, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 5) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 6) << 10,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 12, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 8) << 6,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 16, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 10) << 2,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 20, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 12) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 13) << 18,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 4, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 14) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 15) << 14,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 8, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 16) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 17) << 10,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 12, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 18) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 19) << 6,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 16, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 20) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 21) << 2,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 22-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 20, 42 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 22 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 23> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 23-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 1) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 2) << 13,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 10, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 3) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 4) << 3,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 5) << 21,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 2, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 6) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 7) << 11,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 12, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 8) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 9) << 1,
    };
    shifts = simd_batch{ 40, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 10) << 19,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 4, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 11) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 12) << 9,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 14, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 13) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 14) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 15) << 17,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 6, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 16) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 17) << 7,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 16, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 18) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 19) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 20) << 15,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 8, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 21) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 22) << 5,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 23-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 18, 41 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 23 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 24> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 24-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 1) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 10) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 11) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 13) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 14) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 20) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 22) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 23) << 8,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 24-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 16, 40 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 24 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 25> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 25-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 1) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 2) << 3,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 3) << 17,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 8, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 4) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 5) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 6) << 9,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 7) << 23,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 2, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 8) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 9) << 1,
    };
    shifts = simd_batch{ 38, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 10) << 15,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 10, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 12) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 13) << 7,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 14) << 21,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 4, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 15) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 16) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 17) << 13,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 12, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 18) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 20) << 5,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 21) << 19,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 6, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 23) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 24) << 11,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 25-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 14, 39 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 25 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 26> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 26-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 1) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 2) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 3) << 10,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 4) << 22,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 4, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 6) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 7) << 6,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 8) << 18,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 8, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 10) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 12) << 14,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 12, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 14) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 15) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 16) << 10,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 17) << 22,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 4, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 18) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 19) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 20) << 6,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 21) << 18,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 8, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 23) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 24) << 2,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 25) << 14,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 26-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 12, 38 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 26 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 27> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 27-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 1) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 2) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 3) << 3,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 4) << 13,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 5) << 23,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 4, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 8) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 9) << 9,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 10) << 19,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 8, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 13) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 14) << 5,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 15) << 15,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 16) << 25,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 2, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 18) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 19) << 1,
    };
    shifts = simd_batch{ 36, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 20) << 11,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 21) << 21,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 6, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 23) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 25) << 7,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 26) << 17,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 27-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 10, 37 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 27 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 28> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 28-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 1) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 2) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 3) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 5) << 12,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 6) << 20,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 8, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 9) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 10) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 13) << 20,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 8, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 15) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 17) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 18) << 4,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 19) << 12,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 8, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 23) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 25) << 4,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 26) << 12,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 27) << 20,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 28-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 8, 36 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 28 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 29> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 29-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 1) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 2) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 3) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 4) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 5) << 1,
    };
    shifts = simd_batch{ 34, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 6) << 7,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 7) << 13,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 8) << 19,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 9) << 25,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 4, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 10) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 11) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 12) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 13) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 14) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 15) << 3,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 16) << 9,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 17) << 15,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 18) << 21,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 19) << 27,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 2, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 20) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 21) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 22) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 23) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 24) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 25) << 5,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 26) << 11,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 27) << 17,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 28) << 23,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 29-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 6, 35 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 29 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 30> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 30-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 5) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 6) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 7) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 11) << 14,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 12) << 18,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 13) << 22,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 14) << 26,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 4, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 22) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 23) << 2,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 24) << 6,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 25) << 10,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 26) << 14,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 27) << 18,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 28) << 22,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 29) << 26,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 30-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 4, 34 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 30 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 31> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 31-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 1) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 2) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 3) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 4) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 5) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 6) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 7) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 8) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 9) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 10) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 11) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 12) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 13) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 14) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 15) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 16) << 1,
    };
    shifts = simd_batch{ 32, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 17) << 3,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 18) << 5,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 19) << 7,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 20) << 9,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 21) << 11,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 22) << 13,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 23) << 15,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 24) << 17,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 25) << 19,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 26) << 21,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 27) << 23,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 28) << 25,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 29) << 27,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 30) << 29,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 31-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 2, 33 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 31 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 32> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 32-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 32-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31),
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 32 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 32 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 33> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 33-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 1) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 2) << 29,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 3) << 27,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 4) << 25,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 5) << 23,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 6) << 21,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 7) << 19,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 8) << 17,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 9) << 15,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 10) << 13,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 11) << 11,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 12) << 9,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 13) << 7,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 14) << 5,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 15) << 3,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 16) << 1,
    };
    shifts = simd_batch{ 30, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 17) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 18) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 19) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 20) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 22) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 23) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 24) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 25) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 26) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 27) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 28) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 29) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 30) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 33-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 32) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 31 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 33 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 34> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 34-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 1) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 2) << 26,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 3) << 22,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 4) << 18,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 5) << 14,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 6) << 10,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 7) << 6,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 9) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 10) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 11) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 12) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 13) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 14) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 15) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 18) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 19) << 26,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 20) << 22,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 21) << 18,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 22) << 14,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 23) << 10,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 24) << 6,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 25) << 2,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 26) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 27) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 28) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 29) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 30) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 31) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 32) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 34-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 33) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 30 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 34 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 35> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 35-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 1) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 2) << 23,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 3) << 17,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 4) << 11,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 6) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 7) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 8) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 9) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 12) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 13) << 27,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 14) << 21,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 15) << 15,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 16) << 9,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 17) << 3,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 18) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 19) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 21) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 23) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 24) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 25) << 25,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 26) << 19,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 27) << 13,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 28) << 7,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 29) << 1,
    };
    shifts = simd_batch{ 28, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 30) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 31) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 32) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 33) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 35-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 34) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 29 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 35 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 36> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 36-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 1) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 2) << 20,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 5) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 6) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 10) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 11) << 20,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 13) << 4,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 14) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 15) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 19) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 21) << 12,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 23) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 25) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 26) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 28) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 29) << 20,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 30) << 12,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 32) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 33) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 36-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 35) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 28 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 36 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 37> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 37-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 1) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 2) << 17,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 3) << 7,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 4) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 5) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 6) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 7) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 8) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 9) << 21,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 10) << 11,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 11) << 1,
    };
    shifts = simd_batch{ 26, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 12) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 13) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 14) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 15) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 16) << 25,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 17) << 15,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 18) << 5,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 19) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 20) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 21) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 22) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 23) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 24) << 19,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 25) << 9,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 26) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 27) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 28) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 29) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 30) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 31) << 23,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31),
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 32) << 13,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 33) << 3,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 34) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 35) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 37-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 36) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 36),
    };
    shifts = simd_batch{ 0, 27 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 37 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 38> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 38-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 1) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 2) << 14,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2),
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 3) << 2,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 4) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 5) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 6) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 7) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 8) << 18,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 10) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 11) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 12) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 13) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 14) << 22,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 15) << 10,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 16) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 17) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 20) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 21) << 14,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 22) << 2,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 23) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 24) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 25) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 26) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 27) << 18,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 28) << 6,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 29) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 30) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 31) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 32) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 33) << 22,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 34) << 10,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 35) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 36) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 36),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 38-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 37) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 37),
    };
    shifts = simd_batch{ 0, 26 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 38 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 39> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 39-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 1) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 2) << 11,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 3) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 4) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 6) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 7) << 19,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 8) << 5,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 9) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 10) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 12) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 13) << 13,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 14) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 15) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 16) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 17) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 18) << 21,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 19) << 7,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 20) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 21) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 23) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 24) << 15,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 25) << 1,
    };
    shifts = simd_batch{ 24, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 26) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 27) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 28) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 29) << 23,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 30) << 9,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 31) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 32) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 33) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 34) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34),
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 35) << 17,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 36) << 3,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 37) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 37),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 39-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 38) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 25 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 39 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 40> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 40-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 1) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 3) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 6) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 8) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 9) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 11) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 12) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 13) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 14) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 16) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 18) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 23) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 24) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 26) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 27) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 28) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 29) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 31) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31),
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 32) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 33) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 36) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36),
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 37) << 8,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 38) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 40-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 39) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 39),
    };
    shifts = simd_batch{ 0, 24 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 40 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 41> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 41-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 1) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 2) << 5,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 3) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 4) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 5) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 6) << 15,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 7) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 8) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 9) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 10) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 11) << 7,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 12) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 13) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 14) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 15) << 17,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 16) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 17) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 18) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 19) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 20) << 9,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 21) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 22) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 23) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 24) << 19,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 25) << 1,
    };
    shifts = simd_batch{ 22, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 26) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 27) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 28) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 29) << 11,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 30) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 31) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 32) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 33) << 21,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 34) << 3,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 35) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 36) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 37) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37),
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 38) << 13,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 39) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 39),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 41-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 40) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 40),
    };
    shifts = simd_batch{ 0, 23 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 41 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 42> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 42-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 1) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1),
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 3) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 3),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 5) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 7) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 9) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 11) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 13) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 14) << 14,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 15) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 17) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 18) << 18,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 19) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 22) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 23) << 2,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 25) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 26) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 27) << 6,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 28) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 29) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 30) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 31) << 10,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 32) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 33) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 34) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34),
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 35) << 14,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 36) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 36),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 37) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 38) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38),
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 39) << 18,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 40) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 40),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 42-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 41) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 41),
    };
    shifts = simd_batch{ 0, 22 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 42 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 43> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 43-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 1) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 2) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 3) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 4) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 5) << 19,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 6) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 7) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 8) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 9) << 17,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 10) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 11) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 12) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 13) << 15,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 14) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 15) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 16) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 17) << 13,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 18) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 19) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 20) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 21) << 11,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 22) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 23) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 24) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 25) << 9,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 26) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 27) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 28) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 29) << 7,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 30) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 31) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 32) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 33) << 5,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 34) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 35) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 36) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36),
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 37) << 3,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 38) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 39) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 40) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40),
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 41) << 1,
    };
    shifts = simd_batch{ 20, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 43-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 42) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 42),
    };
    shifts = simd_batch{ 0, 21 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 43 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 44> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 44-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 1) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 2) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 3) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 4) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 5) << 12,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 6) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 8) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 10) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 12) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 13) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 14) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 15) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 16) << 12,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 17) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 18) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 19) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 20) << 4,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 23) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 24) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 25) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 26) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 27) << 12,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 28) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 29) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 30) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 32) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 34) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 35) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 36) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 37) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37),
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 38) << 12,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 39) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 39),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 40) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 41) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41),
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 42) << 4,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 44-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 43) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 43),
    };
    shifts = simd_batch{ 0, 20 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 44 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 45> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 45-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 1) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 2) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 4) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4),
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 6) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 7) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 8) << 17,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 9) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 11) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 12) << 3,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 13) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 14) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 15) << 15,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 16) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 18) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 19) << 1,
    };
    shifts = simd_batch{ 18, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 21) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 22) << 13,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 23) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 24) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 25) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 26) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 27) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 28) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 29) << 11,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 30) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 32) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 33) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 35) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 36) << 9,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 37) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 37),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 38) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 39) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 40) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 40),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 41) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 42) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42),
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 43) << 7,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 45-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 44) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 44),
    };
    shifts = simd_batch{ 0, 19 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 45 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 46> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 46-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 1) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 2) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 3) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 4) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 5) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 6) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 7) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 8) << 6,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 9) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 10) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 11) << 14,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 12) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 13) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 14) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 15) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 16) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 17) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 18) << 2,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 19) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 20) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20),
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 21) << 10,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 22) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23),
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 24) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 25) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 26) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 27) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 28) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 29) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 30) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 31) << 6,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 32) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 33) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 34) << 14,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 35) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 36) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 37) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 38) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 39) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 40) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40),
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 41) << 2,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 42) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 43) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43),
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 44) << 10,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 46-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 45) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 45),
    };
    shifts = simd_batch{ 0, 18 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 46 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 47> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 47-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 1) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 2) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 3) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 4) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 5) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 6) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 7) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 8) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 9) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 10) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 11) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 12) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 13) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 14) << 3,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 15) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 16) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 17) << 7,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 18) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 19) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 20) << 11,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 21) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 22) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22),
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 23) << 15,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 24) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 25) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 26) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 27) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 28) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 29) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 30) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 31) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 32) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 33) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 33),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 34) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 35) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 36) << 1,
    };
    shifts = simd_batch{ 16, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 37) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 38) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38),
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 39) << 5,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 40) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 41) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41),
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 42) << 9,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 43) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 44) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44),
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 45) << 13,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 47-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 46) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 46),
    };
    shifts = simd_batch{ 0, 17 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 47 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 48> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 48-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 1) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 2) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 2),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 5) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 5),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 8) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 10) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 11) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 11),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 13) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 14) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 17) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18),
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 20) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 22) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 23) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 25) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 26) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 28) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 29) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 31) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 32) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 35) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36),
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 37) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 38) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39),
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 40) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 41) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 41),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42),
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 43) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 44) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 44),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45),
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 46) << 16,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 48-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 47) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 47),
    };
    shifts = simd_batch{ 0, 16 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 48 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 49> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 49-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 1) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 2) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 3) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 4) << 11,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 5) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 6) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6),
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 7) << 7,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 8) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 9) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 10) << 3,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 11) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 12) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 13) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 14) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 15) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 16) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 17) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 18) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 19) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 20) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 21) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 22) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 22),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 23) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 24) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 25) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 26) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 27) << 13,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 28) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 29) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 30) << 9,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 31) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 32) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 33) << 5,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 34) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 35) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 36) << 1,
    };
    shifts = simd_batch{ 14, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 37) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 38) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 39) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 39),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 40) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 41) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 42) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 42),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 43) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 44) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 45) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 45),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 46) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 47) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 49-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 48) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 48),
    };
    shifts = simd_batch{ 0, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 49 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 50> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 50-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 1) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 2) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 3) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 4) << 6,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 5) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 6) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 7) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 8) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 9) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 10) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 10),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 11) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 12) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 13) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 14) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 15) << 10,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 16) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 17) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17),
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 18) << 2,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 20) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 21) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 23) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 24) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25),
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 26) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 27) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 28) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 29) << 6,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 30) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 31) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 32) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 32),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 33) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 34) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 35) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 35),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 36) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 37) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 38) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 39) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39),
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 40) << 10,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 41) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 42) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42),
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 43) << 2,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 44) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 45) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 46) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 46),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 47) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 48) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 50-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 49) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 49),
    };
    shifts = simd_batch{ 0, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 50 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 51> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 51-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 1) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 2) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 3) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3),
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 4) << 1,
    };
    shifts = simd_batch{ 12, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 5) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 6) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 7) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 7),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 8) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 9) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 10) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 11) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 12) << 3,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 13) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 14) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 15) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 15),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 17) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 18) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 19) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19),
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 20) << 5,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 21) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 22) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 23) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 24) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 25) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 26) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 27) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 28) << 7,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 29) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 30) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 31) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 31),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 32) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 33) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 34) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 35) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 36) << 9,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 37) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 38) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 39) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 39),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 40) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 41) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 42) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 43) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43),
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 44) << 11,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 45) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 46) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 47) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 47),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 48) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 49) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 51-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 50) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 50),
    };
    shifts = simd_batch{ 0, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 51 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 52> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 52-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 1) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 2) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 3) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 4) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 5) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 6) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 7) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 8) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8),
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 9) << 4,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 10) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 11) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 12) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 12),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13),
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 14) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 15) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 16) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 17) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 17),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 18) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 19) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 20) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 21) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 23) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 24) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 25) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 25),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26),
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 27) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 28) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 29) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 30) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 31) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 32) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 33) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 34) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34),
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 35) << 4,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 36) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 37) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 38) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 38),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39),
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 40) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 41) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 42) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 43) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 43),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 44) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 45) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 46) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 47) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47),
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 48) << 4,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 49) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 50) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 52-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 51) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 51),
    };
    shifts = simd_batch{ 0, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 52 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 53> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 53-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 1) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 2) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 3) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 4) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 4),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 5) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 6) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 7) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 8) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 9) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 9),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 10) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 11) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 12) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 13) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 14) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 15) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 16) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 17) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 18) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 19) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 20) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 21) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 22) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 23) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 24) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 25) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 26) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 27) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 28) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 29) << 1,
    };
    shifts = simd_batch{ 10, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 30) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 31) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 32) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 33) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33),
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 34) << 3,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 35) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 36) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 37) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 38) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38),
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 39) << 5,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 40) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 41) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 42) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 43) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43),
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 44) << 7,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 45) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 46) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 47) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 48) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48),
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 49) << 9,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 50) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 51) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 53-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 52) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 52),
    };
    shifts = simd_batch{ 0, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 53 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 54> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 54-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 1) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 2) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 3) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 4) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 5) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5),
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 7) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 8) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 9) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 10) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10),
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 11) << 2,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 13) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 14) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 15) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 16) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 16),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 18) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 19) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 20) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 21) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 21),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 22) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 23) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 25) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 26) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 26),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27),
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 28) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 29) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 30) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 31) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 32) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 33) << 6,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 35) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 36) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 37) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37),
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 38) << 2,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 39) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 40) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 41) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 42) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 43) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 43),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 44) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 45) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 46) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 47) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 48) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 48),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 49) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 50) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 51) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 52) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 54-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 53) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 53),
    };
    shifts = simd_batch{ 0, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 54 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 55> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 55-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 1) << 9,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 2) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 3) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 4) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 5) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 6) << 54,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 7) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 8) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 9) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 10) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 11) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 11 | SafeLoadAs<uint64_t>(in + 8 * 12) << 53,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12),
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 13) << 7,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 14) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 15) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 16) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 17) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 18) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 18),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 19) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 20) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 22) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 23) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 24) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24),
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 25) << 5,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 26) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 27) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 28) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 29) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 30) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 32) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 33) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 34) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 35) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 36) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36),
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 37) << 3,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 38) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 39) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 40) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 41) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 42) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 42),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 43) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 44) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 45) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 46) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 47) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 48) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48),
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 49) << 1,
    };
    shifts = simd_batch{ 8, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 50) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 51) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 52) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 53) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 55-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 54) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 54),
    };
    shifts = simd_batch{ 0, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 55 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 56> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 56-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 1) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 2) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 3) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 4) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 5) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 6) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 6),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7),
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 9) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 10) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 11) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 12) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 13) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 13),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14),
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 15) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 17) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 18) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 19) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 20) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21),
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 22) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 23) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 25) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 26) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 27) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 27),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28),
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 29) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 30) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 31) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 32) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 33) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 34) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 34),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 36) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 37) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 38) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 39) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 40) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 41) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 41),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42),
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 43) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 44) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 45) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 46) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 47) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 48) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 48),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49),
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 50) << 8,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 51) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 52) << 24,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 53) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 54) << 40,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 56-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 55) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 55),
    };
    shifts = simd_batch{ 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 56 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 57> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1ffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 57-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 1) << 7,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 2) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 3) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 4) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 5) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 6) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 7) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 8) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 8),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 9) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 10) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 11) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 12) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 13) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 14) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 15) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 9 | SafeLoadAs<uint64_t>(in + 8 * 16) << 55,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16),
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 17) << 5,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 19) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 20) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 21) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 22) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 23) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 24) << 54,
      SafeLoadAs<uint64_t>(in + 8 * 24),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 25) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 26) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 27) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 28) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 29) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 30) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 31) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 11 | SafeLoadAs<uint64_t>(in + 8 * 32) << 53,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32),
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 33) << 3,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 34) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 35) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 36) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 37) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 38) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 39) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 40) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 40),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 41) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 42) << 9,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 43) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 44) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 45) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 46) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 47) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 48) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48),
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 49) << 1,
    };
    shifts = simd_batch{ 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 50) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 51) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 52) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 53) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 54) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 55) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 57-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 56) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 56),
    };
    shifts = simd_batch{ 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 57 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 58> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3ffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 58-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 1) << 6,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 2) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 3) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 4) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 5) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 6) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 7) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 8) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 9) << 54,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9),
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 10) << 2,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 11) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 12) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 13) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 14) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 15) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 16) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 17) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 18) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 19) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 19),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 20) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 21) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 22) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 23) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 24) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 25) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 26) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 27) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 28) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 28),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29),
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 30) << 6,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 31) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 32) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 33) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 34) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 35) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 36) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 37) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 38) << 54,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38),
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 39) << 2,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 40) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 41) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 42) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 43) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 44) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 45) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 46) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 47) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 48) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 48),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 49) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 50) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 51) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 52) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 53) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 54) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 55) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 56) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 58-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 57) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 57),
    };
    shifts = simd_batch{ 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 58 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 59> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7ffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 59-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 1) << 5,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 2) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 3) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 4) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 5) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 6) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 7) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 8) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 9) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 10) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 9 | SafeLoadAs<uint64_t>(in + 8 * 11) << 55,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11),
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 12) << 1,
    };
    shifts = simd_batch{ 4, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 13) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 14) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 15) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 16) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 17) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 18) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 19) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 20) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 21) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 22) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 23) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 23),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 24) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 25) << 7,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 26) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 27) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 28) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 29) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 30) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 31) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 32) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 33) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 34) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 7 | SafeLoadAs<uint64_t>(in + 8 * 35) << 57,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35),
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 36) << 3,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 37) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 38) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 39) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 40) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 41) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 42) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 43) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 44) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 45) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 11 | SafeLoadAs<uint64_t>(in + 8 * 46) << 53,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 6 | SafeLoadAs<uint64_t>(in + 8 * 47) << 58,
      SafeLoadAs<uint64_t>(in + 8 * 47),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 48) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 49) << 9,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 50) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 51) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 52) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 53) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 54) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 55) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 56) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 57) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 59-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 57) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 58) << 54,
      SafeLoadAs<uint64_t>(in + 8 * 58),
    };
    shifts = simd_batch{ 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 59 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 60> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0xfffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 60-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 1) << 4,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 2) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 3) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 4) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 5) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 6) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 7) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 8) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 9) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 10) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 11) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 12) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 13) << 52,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 14) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 14),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15),
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 16) << 4,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 17) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 18) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 19) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 21) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 22) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 23) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 24) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 25) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 26) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 27) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 28) << 52,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 29) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 29),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30),
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 31) << 4,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 32) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 33) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 34) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 35) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 36) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 37) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 38) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 39) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 40) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 41) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 42) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 43) << 52,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 44) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 44),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45),
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 46) << 4,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 47) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 48) << 12,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 49) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 50) << 20,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 51) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 52) << 28,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 53) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 54) << 36,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 55) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 56) << 44,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 57) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 57) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 58) << 52,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 60-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 58) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 59) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 59),
    };
    shifts = simd_batch{ 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 60 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 61> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x1fffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 61-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 1) << 3,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 2) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 3) << 9,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 4) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 5) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 6) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 7) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 8) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 9) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 10) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 11) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 12) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 13) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 14) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 15) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 16) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 17) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 18) << 54,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 7 | SafeLoadAs<uint64_t>(in + 8 * 19) << 57,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 4 | SafeLoadAs<uint64_t>(in + 8 * 20) << 60,
      SafeLoadAs<uint64_t>(in + 8 * 20),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 21) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 22) << 5,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 23) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 24) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 25) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 26) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 27) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 28) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 29) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 30) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 31) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 32) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 33) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 34) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 35) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 36) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 37) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 11 | SafeLoadAs<uint64_t>(in + 8 * 38) << 53,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 39) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 5 | SafeLoadAs<uint64_t>(in + 8 * 40) << 59,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40),
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 41) << 1,
    };
    shifts = simd_batch{ 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 42) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 43) << 7,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 44) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 45) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 46) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 47) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 48) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 49) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 50) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 51) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 52) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 53) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 54) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 55) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 56) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 57) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 57) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 58) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 58) >> 9 | SafeLoadAs<uint64_t>(in + 8 * 59) << 55,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 61-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 59) >> 6 | SafeLoadAs<uint64_t>(in + 8 * 60) << 58,
      SafeLoadAs<uint64_t>(in + 8 * 60),
    };
    shifts = simd_batch{ 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 61 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 62> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x3fffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 62-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 1) << 2,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 2) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 3) << 6,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 4) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 5) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 6) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 7) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 8) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 9) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 10) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 11) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 12) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 13) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 14) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 15) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 16) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 17) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 18) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 19) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 20) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 21) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 22) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 23) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 24) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 25) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 26) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 27) << 54,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 28) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 6 | SafeLoadAs<uint64_t>(in + 8 * 29) << 58,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 4 | SafeLoadAs<uint64_t>(in + 8 * 30) << 60,
      SafeLoadAs<uint64_t>(in + 8 * 30),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31),
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 32) << 2,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 33) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 34) << 6,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 35) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 36) << 10,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 37) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 38) << 14,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 39) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 40) << 18,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 41) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 42) << 22,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 43) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 44) << 26,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 45) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 46) << 30,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 47) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 48) << 34,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 49) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 50) << 38,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 51) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 52) << 42,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 53) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 54) << 46,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 55) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 56) << 50,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 57) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 57) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 58) << 54,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 58) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 59) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 59) >> 6 | SafeLoadAs<uint64_t>(in + 8 * 60) << 58,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 62-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 60) >> 4 | SafeLoadAs<uint64_t>(in + 8 * 61) << 60,
      SafeLoadAs<uint64_t>(in + 8 * 61),
    };
    shifts = simd_batch{ 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 62 * 8;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 63> {

  using simd_batch = xsimd::make_sized_batch_t<uint64_t, 2>;
  static constexpr int kValuesUnpacked = 64;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    constexpr uint64_t kMask = 0x7fffffffffffffff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 63-bit bundles 0 to 1
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 0),
      SafeLoadAs<uint64_t>(in + 8 * 0) >> 63 | SafeLoadAs<uint64_t>(in + 8 * 1) << 1,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 2 to 3
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 1) >> 62 | SafeLoadAs<uint64_t>(in + 8 * 2) << 2,
      SafeLoadAs<uint64_t>(in + 8 * 2) >> 61 | SafeLoadAs<uint64_t>(in + 8 * 3) << 3,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 4 to 5
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 3) >> 60 | SafeLoadAs<uint64_t>(in + 8 * 4) << 4,
      SafeLoadAs<uint64_t>(in + 8 * 4) >> 59 | SafeLoadAs<uint64_t>(in + 8 * 5) << 5,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 6 to 7
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 5) >> 58 | SafeLoadAs<uint64_t>(in + 8 * 6) << 6,
      SafeLoadAs<uint64_t>(in + 8 * 6) >> 57 | SafeLoadAs<uint64_t>(in + 8 * 7) << 7,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 8 to 9
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 7) >> 56 | SafeLoadAs<uint64_t>(in + 8 * 8) << 8,
      SafeLoadAs<uint64_t>(in + 8 * 8) >> 55 | SafeLoadAs<uint64_t>(in + 8 * 9) << 9,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 10 to 11
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 9) >> 54 | SafeLoadAs<uint64_t>(in + 8 * 10) << 10,
      SafeLoadAs<uint64_t>(in + 8 * 10) >> 53 | SafeLoadAs<uint64_t>(in + 8 * 11) << 11,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 12 to 13
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 11) >> 52 | SafeLoadAs<uint64_t>(in + 8 * 12) << 12,
      SafeLoadAs<uint64_t>(in + 8 * 12) >> 51 | SafeLoadAs<uint64_t>(in + 8 * 13) << 13,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 14 to 15
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 13) >> 50 | SafeLoadAs<uint64_t>(in + 8 * 14) << 14,
      SafeLoadAs<uint64_t>(in + 8 * 14) >> 49 | SafeLoadAs<uint64_t>(in + 8 * 15) << 15,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 16 to 17
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 15) >> 48 | SafeLoadAs<uint64_t>(in + 8 * 16) << 16,
      SafeLoadAs<uint64_t>(in + 8 * 16) >> 47 | SafeLoadAs<uint64_t>(in + 8 * 17) << 17,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 18 to 19
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 17) >> 46 | SafeLoadAs<uint64_t>(in + 8 * 18) << 18,
      SafeLoadAs<uint64_t>(in + 8 * 18) >> 45 | SafeLoadAs<uint64_t>(in + 8 * 19) << 19,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 20 to 21
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 19) >> 44 | SafeLoadAs<uint64_t>(in + 8 * 20) << 20,
      SafeLoadAs<uint64_t>(in + 8 * 20) >> 43 | SafeLoadAs<uint64_t>(in + 8 * 21) << 21,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 22 to 23
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 21) >> 42 | SafeLoadAs<uint64_t>(in + 8 * 22) << 22,
      SafeLoadAs<uint64_t>(in + 8 * 22) >> 41 | SafeLoadAs<uint64_t>(in + 8 * 23) << 23,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 24 to 25
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 23) >> 40 | SafeLoadAs<uint64_t>(in + 8 * 24) << 24,
      SafeLoadAs<uint64_t>(in + 8 * 24) >> 39 | SafeLoadAs<uint64_t>(in + 8 * 25) << 25,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 26 to 27
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 25) >> 38 | SafeLoadAs<uint64_t>(in + 8 * 26) << 26,
      SafeLoadAs<uint64_t>(in + 8 * 26) >> 37 | SafeLoadAs<uint64_t>(in + 8 * 27) << 27,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 28 to 29
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 27) >> 36 | SafeLoadAs<uint64_t>(in + 8 * 28) << 28,
      SafeLoadAs<uint64_t>(in + 8 * 28) >> 35 | SafeLoadAs<uint64_t>(in + 8 * 29) << 29,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 30 to 31
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 29) >> 34 | SafeLoadAs<uint64_t>(in + 8 * 30) << 30,
      SafeLoadAs<uint64_t>(in + 8 * 30) >> 33 | SafeLoadAs<uint64_t>(in + 8 * 31) << 31,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 32 to 33
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 31) >> 32 | SafeLoadAs<uint64_t>(in + 8 * 32) << 32,
      SafeLoadAs<uint64_t>(in + 8 * 32) >> 31 | SafeLoadAs<uint64_t>(in + 8 * 33) << 33,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 34 to 35
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 33) >> 30 | SafeLoadAs<uint64_t>(in + 8 * 34) << 34,
      SafeLoadAs<uint64_t>(in + 8 * 34) >> 29 | SafeLoadAs<uint64_t>(in + 8 * 35) << 35,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 36 to 37
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 35) >> 28 | SafeLoadAs<uint64_t>(in + 8 * 36) << 36,
      SafeLoadAs<uint64_t>(in + 8 * 36) >> 27 | SafeLoadAs<uint64_t>(in + 8 * 37) << 37,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 38 to 39
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 37) >> 26 | SafeLoadAs<uint64_t>(in + 8 * 38) << 38,
      SafeLoadAs<uint64_t>(in + 8 * 38) >> 25 | SafeLoadAs<uint64_t>(in + 8 * 39) << 39,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 40 to 41
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 39) >> 24 | SafeLoadAs<uint64_t>(in + 8 * 40) << 40,
      SafeLoadAs<uint64_t>(in + 8 * 40) >> 23 | SafeLoadAs<uint64_t>(in + 8 * 41) << 41,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 42 to 43
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 41) >> 22 | SafeLoadAs<uint64_t>(in + 8 * 42) << 42,
      SafeLoadAs<uint64_t>(in + 8 * 42) >> 21 | SafeLoadAs<uint64_t>(in + 8 * 43) << 43,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 44 to 45
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 43) >> 20 | SafeLoadAs<uint64_t>(in + 8 * 44) << 44,
      SafeLoadAs<uint64_t>(in + 8 * 44) >> 19 | SafeLoadAs<uint64_t>(in + 8 * 45) << 45,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 46 to 47
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 45) >> 18 | SafeLoadAs<uint64_t>(in + 8 * 46) << 46,
      SafeLoadAs<uint64_t>(in + 8 * 46) >> 17 | SafeLoadAs<uint64_t>(in + 8 * 47) << 47,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 48 to 49
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 47) >> 16 | SafeLoadAs<uint64_t>(in + 8 * 48) << 48,
      SafeLoadAs<uint64_t>(in + 8 * 48) >> 15 | SafeLoadAs<uint64_t>(in + 8 * 49) << 49,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 50 to 51
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 49) >> 14 | SafeLoadAs<uint64_t>(in + 8 * 50) << 50,
      SafeLoadAs<uint64_t>(in + 8 * 50) >> 13 | SafeLoadAs<uint64_t>(in + 8 * 51) << 51,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 52 to 53
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 51) >> 12 | SafeLoadAs<uint64_t>(in + 8 * 52) << 52,
      SafeLoadAs<uint64_t>(in + 8 * 52) >> 11 | SafeLoadAs<uint64_t>(in + 8 * 53) << 53,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 54 to 55
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 53) >> 10 | SafeLoadAs<uint64_t>(in + 8 * 54) << 54,
      SafeLoadAs<uint64_t>(in + 8 * 54) >> 9 | SafeLoadAs<uint64_t>(in + 8 * 55) << 55,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 56 to 57
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 55) >> 8 | SafeLoadAs<uint64_t>(in + 8 * 56) << 56,
      SafeLoadAs<uint64_t>(in + 8 * 56) >> 7 | SafeLoadAs<uint64_t>(in + 8 * 57) << 57,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 58 to 59
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 57) >> 6 | SafeLoadAs<uint64_t>(in + 8 * 58) << 58,
      SafeLoadAs<uint64_t>(in + 8 * 58) >> 5 | SafeLoadAs<uint64_t>(in + 8 * 59) << 59,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 60 to 61
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 59) >> 4 | SafeLoadAs<uint64_t>(in + 8 * 60) << 60,
      SafeLoadAs<uint64_t>(in + 8 * 60) >> 3 | SafeLoadAs<uint64_t>(in + 8 * 61) << 61,
    };
    shifts = simd_batch{ 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    // extract 63-bit bundles 62 to 63
    words = simd_batch{
      SafeLoadAs<uint64_t>(in + 8 * 61) >> 2 | SafeLoadAs<uint64_t>(in + 8 * 62) << 62,
      SafeLoadAs<uint64_t>(in + 8 * 62),
    };
    shifts = simd_batch{ 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 2;

    in += 63 * 8;
    return in;
  }
};


}  // namespace
}  // namespace arrow::internal
