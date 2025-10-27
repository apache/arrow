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
#include "arrow/util/bpacking_simd_impl_internal.h"

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

  using Dispatch = Kernel<uint32_t, 1, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 2> {

  using Dispatch = Kernel<uint32_t, 2, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 3> {

  using Dispatch = Kernel<uint32_t, 3, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 4> {

  using Dispatch = Kernel<uint32_t, 4, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 5> {

  using Dispatch = Kernel<uint32_t, 5, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 6> {

  using Dispatch = Kernel<uint32_t, 6, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 7> {

  using Dispatch = Kernel<uint32_t, 7, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 8> {

  using Dispatch = Kernel<uint32_t, 8, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 9> {

  using Dispatch = Kernel<uint32_t, 9, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 10> {

  using Dispatch = Kernel<uint32_t, 10, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 11> {

  using Dispatch = Kernel<uint32_t, 11, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 12> {

  using Dispatch = Kernel<uint32_t, 12, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 13> {

  using Dispatch = Kernel<uint32_t, 13, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 14> {

  using Dispatch = Kernel<uint32_t, 14, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 15> {

  using Dispatch = Kernel<uint32_t, 15, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 16> {

  using Dispatch = Kernel<uint32_t, 16, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 17> {

  using Dispatch = Kernel<uint32_t, 17, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 18> {

  using Dispatch = Kernel<uint32_t, 18, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 19> {

  using Dispatch = Kernel<uint32_t, 19, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 20> {

  using Dispatch = Kernel<uint32_t, 20, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 21> {

  using Dispatch = Kernel<uint32_t, 21, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 22> {

  using Dispatch = Kernel<uint32_t, 22, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 23> {

  using Dispatch = Kernel<uint32_t, 23, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 24> {

  using Dispatch = Kernel<uint32_t, 24, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 25> {

  using Dispatch = Kernel<uint32_t, 25, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint32_t, 26> {

  using Dispatch = Kernel<uint32_t, 26, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
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

  using Dispatch = Kernel<uint32_t, 28, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint32_t* out) {
    return Dispatch::unpack(in, out);
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

  using Dispatch = Kernel<uint64_t, 1, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 2> {

  using Dispatch = Kernel<uint64_t, 2, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 3> {

  using Dispatch = Kernel<uint64_t, 3, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 4> {

  using Dispatch = Kernel<uint64_t, 4, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 5> {

  using Dispatch = Kernel<uint64_t, 5, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 6> {

  using Dispatch = Kernel<uint64_t, 6, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 7> {

  using Dispatch = Kernel<uint64_t, 7, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 8> {

  using Dispatch = Kernel<uint64_t, 8, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 9> {

  using Dispatch = Kernel<uint64_t, 9, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 10> {

  using Dispatch = Kernel<uint64_t, 10, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 11> {

  using Dispatch = Kernel<uint64_t, 11, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 12> {

  using Dispatch = Kernel<uint64_t, 12, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 13> {

  using Dispatch = Kernel<uint64_t, 13, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 14> {

  using Dispatch = Kernel<uint64_t, 14, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 15> {

  using Dispatch = Kernel<uint64_t, 15, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 16> {

  using Dispatch = Kernel<uint64_t, 16, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 17> {

  using Dispatch = Kernel<uint64_t, 17, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 18> {

  using Dispatch = Kernel<uint64_t, 18, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 19> {

  using Dispatch = Kernel<uint64_t, 19, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 20> {

  using Dispatch = Kernel<uint64_t, 20, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 21> {

  using Dispatch = Kernel<uint64_t, 21, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 22> {

  using Dispatch = Kernel<uint64_t, 22, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 23> {

  using Dispatch = Kernel<uint64_t, 23, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 24> {

  using Dispatch = Kernel<uint64_t, 24, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 25> {

  using Dispatch = Kernel<uint64_t, 25, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 26> {

  using Dispatch = Kernel<uint64_t, 26, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 27> {

  using Dispatch = Kernel<uint64_t, 27, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 28> {

  using Dispatch = Kernel<uint64_t, 28, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 29> {

  using Dispatch = Kernel<uint64_t, 29, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 30> {

  using Dispatch = Kernel<uint64_t, 30, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 31> {

  using Dispatch = Kernel<uint64_t, 31, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 32> {

  using Dispatch = Kernel<uint64_t, 32, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 33> {

  using Dispatch = Kernel<uint64_t, 33, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 34> {

  using Dispatch = Kernel<uint64_t, 34, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 35> {

  using Dispatch = Kernel<uint64_t, 35, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 36> {

  using Dispatch = Kernel<uint64_t, 36, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 37> {

  using Dispatch = Kernel<uint64_t, 37, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 38> {

  using Dispatch = Kernel<uint64_t, 38, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 39> {

  using Dispatch = Kernel<uint64_t, 39, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 40> {

  using Dispatch = Kernel<uint64_t, 40, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 41> {

  using Dispatch = Kernel<uint64_t, 41, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 42> {

  using Dispatch = Kernel<uint64_t, 42, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 43> {

  using Dispatch = Kernel<uint64_t, 43, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 44> {

  using Dispatch = Kernel<uint64_t, 44, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 45> {

  using Dispatch = Kernel<uint64_t, 45, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 46> {

  using Dispatch = Kernel<uint64_t, 46, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 47> {

  using Dispatch = Kernel<uint64_t, 47, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 48> {

  using Dispatch = Kernel<uint64_t, 48, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 49> {

  using Dispatch = Kernel<uint64_t, 49, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 50> {

  using Dispatch = Kernel<uint64_t, 50, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 51> {

  using Dispatch = Kernel<uint64_t, 51, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 52> {

  using Dispatch = Kernel<uint64_t, 52, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 53> {

  using Dispatch = Kernel<uint64_t, 53, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 54> {

  using Dispatch = Kernel<uint64_t, 54, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 55> {

  using Dispatch = Kernel<uint64_t, 55, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 56> {

  using Dispatch = Kernel<uint64_t, 56, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 57> {

  using Dispatch = Kernel<uint64_t, 57, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
  }
};

template<>
struct Simd128UnpackerForWidth<uint64_t, 58> {

  using Dispatch = Kernel<uint64_t, 58, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
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

  using Dispatch = Kernel<uint64_t, 60, 128>;

  static constexpr int kValuesUnpacked = Dispatch::kValuesUnpacked;

  static const uint8_t* unpack(const uint8_t* in, uint64_t* out) {
    return Dispatch::unpack(in, out);
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
