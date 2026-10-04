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

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <utility>
#include <vector>

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"
#include "arrow/util/fastlanes/transposed_delta.h"

namespace arrow::util::fastlanes {

namespace {

// Applies `fn` to every bit width the element type can hold, as a compile-time
// constant, so each width instantiates its own kernel the way a caller does.
template <typename T, typename Fn, uint32_t... Ws>
void ForEachWidthImpl(Fn&& fn, std::integer_sequence<uint32_t, Ws...>) {
  (fn(std::integral_constant<uint32_t, Ws + 1>{}), ...);
}

template <typename T, typename Fn>
void ForEachWidth(Fn&& fn) {
  ForEachWidthImpl<T>(std::forward<Fn>(fn),
                      std::make_integer_sequence<uint32_t, BlockGeometry<T>::kBits>{});
}

template <typename T>
std::vector<T> RandomValues(uint32_t w, uint32_t seed) {
  const auto mask =
      static_cast<T>(BlockWord<T>(~BlockWord<T>(0)) >> (sizeof(BlockWord<T>) * 8 - w));
  std::mt19937 rng(seed);
  std::vector<T> in(kBlockSize);
  for (auto& v : in) v = static_cast<T>(rng() & mask);
  return in;
}

// A caller packs block after block into one page buffer, so a kernel that left
// any of its payload untouched would read back whatever the previous block put
// there. Tests hand it a buffer already full of this pattern rather than a
// zeroed one, so clearing the payload stays load-bearing.
template <typename T>
constexpr T kDirty = static_cast<T>(0xACACACACu);

// Reads `w` bits starting at `bit_offset` from one lane's own stream, which is
// the words at packed[k * kLanes + lane] taken LSB-first.
template <typename T>
uint64_t ReadLaneBits(const T* packed, size_t lane, uint32_t bit_offset, uint32_t w) {
  constexpr size_t kBits = BlockGeometry<T>::kBits;
  constexpr size_t kLanesT = BlockGeometry<T>::kLanes;
  uint64_t out = 0;
  for (uint32_t i = 0; i < w; ++i) {
    const uint32_t bit = bit_offset + i;
    const uint64_t word = packed[(bit / kBits) * kLanesT + lane];
    out |= ((word >> (bit % kBits)) & 1ULL) << i;
  }
  return out;
}

}  // namespace

template <typename T>
class FastlanesKernelsTest : public ::testing::Test {};

using ElementTypes = ::testing::Types<uint8_t, uint16_t, uint32_t>;
TYPED_TEST_SUITE(FastlanesKernelsTest, ElementTypes);

// A block holds kBlockSize values at every element width, so a narrower element
// trades rows for lanes.
TYPED_TEST(FastlanesKernelsTest, GridCoversTheBlockExactly) {
  using G = BlockGeometry<TypeParam>;
  EXPECT_EQ(G::kLanes * G::kRowsPerBlock, kBlockSize);
  EXPECT_EQ(G::kRowsPerBlock, sizeof(TypeParam) * 8);
}

// The packed payload is 128 * w bytes however wide the element is, which is the
// size the sequential layout needs for the same full block.
TYPED_TEST(FastlanesKernelsTest, PayloadSizeDoesNotDependOnElementWidth) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    EXPECT_EQ(w * G::kLanes * sizeof(TypeParam), 128u * w) << "w=" << w;
  });
}

TYPED_TEST(FastlanesKernelsTest, RoundTripsEveryWidth) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    const auto in = RandomValues<TypeParam>(w, 1000u * w + sizeof(TypeParam));
    std::vector<TypeParam> packed(w * G::kLanes, kDirty<TypeParam>);
    std::vector<TypeParam> out(kBlockSize, 0);
    PackBlock<TypeParam, w>(in.data(), packed.data());
    UnpackBlock<TypeParam, w, false>(packed.data(), out.data());
    EXPECT_EQ(in, out) << "w=" << w;
  });
}

#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
// UnpackBlockFlToFileOrder claims to equal an UnpackBlock into a scratch grid
// followed by Transpose32x32, with the permutation done in registers instead.
// That is the claim a caller depends on when it picks the fused kernel over the
// grid, and the two are separate implementations of it, so comparing them is a
// real check rather than a kernel agreeing with itself. 32-bit only, which is
// why this is not one of the typed tests above.
TEST(FastlanesFileOrderTest, MatchesTheGridAndTransposeAtEveryWidth) {
  using G = BlockGeometry<uint32_t>;
  ForEachWidth<uint32_t>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    const auto in = RandomValues<uint32_t>(w, 311u * w + 7u);
    std::vector<uint32_t> packed(w * G::kLanes, kDirty<uint32_t>);
    PackBlock<uint32_t, w>(in.data(), packed.data());

    std::vector<uint32_t> grid(kBlockSize, 0);
    std::vector<int32_t> expected(kBlockSize, 0);
    UnpackBlock<uint32_t, w, false>(packed.data(), grid.data());
    Transpose32x32(grid.data(), expected.data());

    std::vector<int32_t> actual(kBlockSize, 0);
    UnpackBlockFlToFileOrder<w, false>(packed.data(), actual.data());

    for (size_t i = 0; i < kBlockSize; ++i) {
      ASSERT_EQ(actual[i], expected[i]) << "w=" << w << " i=" << i;
    }
  });
}
#endif

// The bias is folded into the unpack so a frame-of-reference decoder needs no
// second pass. It is modular in the element type, matching the subtraction the
// encoder did.
TYPED_TEST(FastlanesKernelsTest, FoldsBiasModuloTheElementWidth) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    const auto in = RandomValues<TypeParam>(w, 77u * w + sizeof(TypeParam));
    // A bias large enough to wrap for every element width.
    const auto bias = static_cast<TypeParam>(0xF1u);
    std::vector<TypeParam> packed(w * G::kLanes, kDirty<TypeParam>);
    std::vector<TypeParam> out(kBlockSize, 0);
    PackBlock<TypeParam, w>(in.data(), packed.data());
    UnpackBlock<TypeParam, w, true>(packed.data(), out.data(), bias);
    for (size_t i = 0; i < kBlockSize; ++i) {
      ASSERT_EQ(out[i], static_cast<TypeParam>(in[i] + bias)) << "w=" << w << " i=" << i;
    }
  });
}

// The payload is exactly w * kLanes words. A kernel that wrote past it would
// corrupt the next block of a page.
TYPED_TEST(FastlanesKernelsTest, PackWritesNothingPastThePayload) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    constexpr size_t kGuard = 16;
    const auto in = RandomValues<TypeParam>(w, 5u * w + sizeof(TypeParam));
    std::vector<TypeParam> packed(w * G::kLanes + kGuard, kDirty<TypeParam>);
    PackBlock<TypeParam, w>(in.data(), packed.data());
    // Still holding the pattern, so the kernel did not write here at all,
    // rather than having written a zero that happened to look untouched.
    for (size_t i = w * G::kLanes; i < packed.size(); ++i) {
      ASSERT_EQ(packed[i], kDirty<TypeParam>) << "w=" << w << " guard index " << i;
    }
  });
}

// The layout contract: within one lane, successive rows occupy successive bit
// positions LSB-first, exactly as sequential bit packing assembles them. The
// shift and the straddle therefore depend on the row and never on the lane.
TYPED_TEST(FastlanesKernelsTest, LaneBitsAreSequentialLsbFirst) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    const auto in = RandomValues<TypeParam>(w, 31u * w + sizeof(TypeParam));
    std::vector<TypeParam> packed(w * G::kLanes, kDirty<TypeParam>);
    PackBlock<TypeParam, w>(in.data(), packed.data());
    // Checking two lanes is enough to show the bit positions do not depend on
    // the lane, and keeps the test linear in the block size.
    for (size_t lane : {size_t{0}, G::kLanes - 1}) {
      for (uint32_t row = 0; row < G::kRowsPerBlock; ++row) {
        const uint64_t got = ReadLaneBits<TypeParam>(packed.data(), lane, row * w, w);
        ASSERT_EQ(got, static_cast<uint64_t>(in[row * G::kLanes + lane]))
            << "w=" << w << " lane=" << lane << " row=" << row;
      }
    }
  });
}

// Packing into a buffer a previous block wrote must leave none of that block
// behind, which is the contract a page encoder relies on when it walks one
// output buffer block by block.
TYPED_TEST(FastlanesKernelsTest, PackOverwritesAnEarlierBlock) {
  using G = BlockGeometry<TypeParam>;
  ForEachWidth<TypeParam>([](auto w_const) {
    constexpr uint32_t w = decltype(w_const)::value;
    const auto first = RandomValues<TypeParam>(w, 400u * w + sizeof(TypeParam));
    const auto second = RandomValues<TypeParam>(w, 900u * w + sizeof(TypeParam));
    std::vector<TypeParam> packed(w * G::kLanes, 0);
    std::vector<TypeParam> out(kBlockSize, 0);
    PackBlock<TypeParam, w>(first.data(), packed.data());
    PackBlock<TypeParam, w>(second.data(), packed.data());
    UnpackBlock<TypeParam, w, false>(packed.data(), out.data());
    EXPECT_EQ(second, out) << "w=" << w;
  });
}

// At w == the element width there is nothing to pack, and the kernels degrade to
// a copy. Callers rely on that staying a round trip rather than a special case
// they have to avoid.
TYPED_TEST(FastlanesKernelsTest, FullWidthIsACopy) {
  constexpr uint32_t kFull = BlockGeometry<TypeParam>::kBits;
  const auto in = RandomValues<TypeParam>(kFull, 9u);
  std::vector<TypeParam> packed(kBlockSize, kDirty<TypeParam>);
  std::vector<TypeParam> out(kBlockSize, 0);
  PackBlock<TypeParam, kFull>(in.data(), packed.data());
  EXPECT_EQ(in, packed);
  UnpackBlock<TypeParam, kFull, false>(packed.data(), out.data());
  EXPECT_EQ(in, out);
}

}  // namespace arrow::util::fastlanes
