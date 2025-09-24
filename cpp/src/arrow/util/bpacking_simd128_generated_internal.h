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

using ::arrow::util::SafeLoadAs;

template<typename Uint, int BitWidth>
struct Simd128UnpackerForWidth;

template<>
struct Simd128UnpackerForWidth<uint16_t, 1> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x1;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 1-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
    };
    shifts = simd_batch{ 0, 1, 2, 3, 4, 5, 6, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 1-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
    };
    shifts = simd_batch{ 8, 9, 10, 11, 12, 13, 14, 15 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 1 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 2> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x3;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 2-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
    };
    shifts = simd_batch{ 0, 2, 4, 6, 8, 10, 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 2-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
    };
    shifts = simd_batch{ 0, 2, 4, 6, 8, 10, 12, 14 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 2 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 3> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x7;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 3-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 1) << 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
    };
    shifts = simd_batch{ 0, 3, 6, 9, 12, 0, 2, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 3-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 2) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
    };
    shifts = simd_batch{ 8, 11, 0, 1, 4, 7, 10, 13 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 3 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 4> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0xf;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 4-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
    };
    shifts = simd_batch{ 0, 4, 8, 12, 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 4-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
    };
    shifts = simd_batch{ 0, 4, 8, 12, 0, 4, 8, 12 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 4 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 5> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x1f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 5-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 1) << 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 2) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
    };
    shifts = simd_batch{ 0, 5, 10, 0, 4, 9, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 5-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 3) << 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 4) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
    };
    shifts = simd_batch{ 8, 0, 2, 7, 0, 1, 6, 11 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 5 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 6> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x3f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 6-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 1) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 2) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
    };
    shifts = simd_batch{ 0, 6, 0, 2, 8, 0, 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 6-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 4) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 5) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 5),
      SafeLoadAs<uint16_t>(in + 2 * 5),
    };
    shifts = simd_batch{ 0, 6, 0, 2, 8, 0, 4, 10 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 6 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 7> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x7f;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 7-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 1) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 2) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 3) << 6),
      SafeLoadAs<uint16_t>(in + 2 * 3),
    };
    shifts = simd_batch{ 0, 7, 0, 5, 0, 3, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 7-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 4) << 1),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 5) << 3),
      SafeLoadAs<uint16_t>(in + 2 * 5),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 11 | SafeLoadAs<uint16_t>(in + 2 * 6) << 5),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      SafeLoadAs<uint16_t>(in + 2 * 6),
    };
    shifts = simd_batch{ 8, 0, 6, 0, 4, 0, 2, 9 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 7 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 8> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0xff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 8-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 0),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
    };
    shifts = simd_batch{ 0, 8, 0, 8, 0, 8, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 8-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      SafeLoadAs<uint16_t>(in + 2 * 5),
      SafeLoadAs<uint16_t>(in + 2 * 5),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      SafeLoadAs<uint16_t>(in + 2 * 7),
      SafeLoadAs<uint16_t>(in + 2 * 7),
    };
    shifts = simd_batch{ 0, 8, 0, 8, 0, 8, 0, 8 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 8 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 9> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x1ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 9-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 9 | SafeLoadAs<uint16_t>(in + 2 * 1) << 7),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 11 | SafeLoadAs<uint16_t>(in + 2 * 2) << 5),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 3) << 3),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 4) << 1),
    };
    shifts = simd_batch{ 0, 0, 2, 0, 4, 0, 6, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 9-bit bundles 8 to 15
    words = simd_batch{
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 5) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 5),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 6) << 6),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 7) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 7),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 8) << 2),
      SafeLoadAs<uint16_t>(in + 2 * 8),
    };
    shifts = simd_batch{ 0, 1, 0, 3, 0, 5, 0, 7 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 9 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 10> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x3ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 10-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 1) << 6),
      SafeLoadAs<uint16_t>(in + 2 * 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 2) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 3) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 4) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 4),
    };
    shifts = simd_batch{ 0, 0, 4, 0, 0, 2, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 10-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 5),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 6) << 6),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 7) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 8) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 8) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 9) << 4),
      SafeLoadAs<uint16_t>(in + 2 * 9),
    };
    shifts = simd_batch{ 0, 0, 4, 0, 0, 2, 0, 6 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 10 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 11> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x7ff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 11-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 11 | SafeLoadAs<uint16_t>(in + 2 * 1) << 5),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 6 | SafeLoadAs<uint16_t>(in + 2 * 2) << 10),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 3) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 7 | SafeLoadAs<uint16_t>(in + 2 * 4) << 9),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 5) << 3),
    };
    shifts = simd_batch{ 0, 0, 0, 1, 0, 0, 2, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 11-bit bundles 8 to 15
    words = simd_batch{
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 6) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 7) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 9 | SafeLoadAs<uint16_t>(in + 2 * 8) << 7),
      SafeLoadAs<uint16_t>(in + 2 * 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 8) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 9) << 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 9) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 10) << 6),
      SafeLoadAs<uint16_t>(in + 2 * 10),
    };
    shifts = simd_batch{ 0, 3, 0, 0, 4, 0, 0, 5 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 11 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 12> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0xfff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 12-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 1) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 2) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 2),
      SafeLoadAs<uint16_t>(in + 2 * 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 4) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 5) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 5),
    };
    shifts = simd_batch{ 0, 0, 0, 4, 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 12-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 7) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 8) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 8),
      SafeLoadAs<uint16_t>(in + 2 * 9),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 9) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 10) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 10) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 11) << 8),
      SafeLoadAs<uint16_t>(in + 2 * 11),
    };
    shifts = simd_batch{ 0, 0, 0, 4, 0, 0, 0, 4 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 12 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 13> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x1fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 13-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 1) << 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 2) << 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 7 | SafeLoadAs<uint16_t>(in + 2 * 3) << 9),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 4 | SafeLoadAs<uint16_t>(in + 2 * 4) << 12),
      SafeLoadAs<uint16_t>(in + 2 * 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 5) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 11 | SafeLoadAs<uint16_t>(in + 2 * 6) << 5),
    };
    shifts = simd_batch{ 0, 0, 0, 0, 0, 1, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 13-bit bundles 8 to 15
    words = simd_batch{
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 7) << 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 5 | SafeLoadAs<uint16_t>(in + 2 * 8) << 11),
      SafeLoadAs<uint16_t>(in + 2 * 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 8) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 9) << 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 9) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 10) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 10) >> 9 | SafeLoadAs<uint16_t>(in + 2 * 11) << 7),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 11) >> 6 | SafeLoadAs<uint16_t>(in + 2 * 12) << 10),
      SafeLoadAs<uint16_t>(in + 2 * 12),
    };
    shifts = simd_batch{ 0, 0, 2, 0, 0, 0, 0, 3 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 13 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 14> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x3fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 14-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 1) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 2) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 3) << 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 4) << 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 6 | SafeLoadAs<uint16_t>(in + 2 * 5) << 10),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 4 | SafeLoadAs<uint16_t>(in + 2 * 6) << 12),
      SafeLoadAs<uint16_t>(in + 2 * 6),
    };
    shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 14-bit bundles 8 to 15
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 7),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 8) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 8) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 9) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 9) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 10) << 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 10) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 11) << 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 11) >> 6 | SafeLoadAs<uint16_t>(in + 2 * 12) << 10),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 12) >> 4 | SafeLoadAs<uint16_t>(in + 2 * 13) << 12),
      SafeLoadAs<uint16_t>(in + 2 * 13),
    };
    shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 2 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 14 * 2;
    return in;
  }
};

template<>
struct Simd128UnpackerForWidth<uint16_t, 15> {

  using simd_batch = xsimd::make_sized_batch_t<uint16_t, 8>;
  static constexpr int kValuesUnpacked = 16;

  static const uint8_t* unpack(const uint8_t* in, uint16_t* out) {
    constexpr uint16_t kMask = 0x7fff;

    simd_batch masks(kMask);
    simd_batch words, shifts;
    simd_batch results;
    // extract 15-bit bundles 0 to 7
    words = simd_batch{
      SafeLoadAs<uint16_t>(in + 2 * 0),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 0) >> 15 | SafeLoadAs<uint16_t>(in + 2 * 1) << 1),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 1) >> 14 | SafeLoadAs<uint16_t>(in + 2 * 2) << 2),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 2) >> 13 | SafeLoadAs<uint16_t>(in + 2 * 3) << 3),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 3) >> 12 | SafeLoadAs<uint16_t>(in + 2 * 4) << 4),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 4) >> 11 | SafeLoadAs<uint16_t>(in + 2 * 5) << 5),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 5) >> 10 | SafeLoadAs<uint16_t>(in + 2 * 6) << 6),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 6) >> 9 | SafeLoadAs<uint16_t>(in + 2 * 7) << 7),
    };
    shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 0 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    // extract 15-bit bundles 8 to 15
    words = simd_batch{
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 7) >> 8 | SafeLoadAs<uint16_t>(in + 2 * 8) << 8),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 8) >> 7 | SafeLoadAs<uint16_t>(in + 2 * 9) << 9),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 9) >> 6 | SafeLoadAs<uint16_t>(in + 2 * 10) << 10),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 10) >> 5 | SafeLoadAs<uint16_t>(in + 2 * 11) << 11),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 11) >> 4 | SafeLoadAs<uint16_t>(in + 2 * 12) << 12),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 12) >> 3 | SafeLoadAs<uint16_t>(in + 2 * 13) << 13),
      static_cast<uint16_t>(SafeLoadAs<uint16_t>(in + 2 * 13) >> 2 | SafeLoadAs<uint16_t>(in + 2 * 14) << 14),
      SafeLoadAs<uint16_t>(in + 2 * 14),
    };
    shifts = simd_batch{ 0, 0, 0, 0, 0, 0, 0, 1 };
    results = (words >> shifts) & masks;
    results.store_unaligned(out);
    out += 8;

    in += 15 * 2;
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


}  // namespace arrow::internal
