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

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/rle_bitmap_internal.h"
#include "arrow/util/rle_encoding_internal.h"

namespace arrow::util {

namespace {

// Expand a list of bytes into the bit-packed (LSB-first) sequence of booleans
// they encode, keeping only the first `count` bits.
std::vector<bool> BitsFromBytes(const std::vector<uint8_t>& bytes, rle_size_t count) {
  std::vector<bool> bits(count);
  for (rle_size_t i = 0; i < count; ++i) {
    bits[i] = bit_util::GetBit(bytes.data(), i);
  }
  return bits;
}

// Skip the first `skip` values with Advance(), then decode the rest of the run
// into a contiguous output bitmap, in successive calls of at most `chunk` values
// each, and compare the result against `expected`.
//
// This drives both decoder types through their byte-aligned and bit-unaligned
// code paths depending on `chunk`: as soon as `chunk` is not a multiple of 8,
// later calls land on a non-zero output bit offset.
//
// A non-zero `skip` additionally desynchronizes the decoder's read offset from
// the (byte-aligned) output offset, which exercises the bit-unaligned
// (GetBatchMisaligned) path of BitPackedRunToBitMapDecoder. With `skip == 0` the
// read and output offsets stay in lockstep and only the aligned path is used.
template <typename Decoder>
void CheckChunkedDecode(const typename Decoder::RunType& run,
                        const std::vector<bool>& expected, rle_size_t chunk,
                        rle_size_t skip = 0) {
  ARROW_SCOPED_TRACE("chunk = ", chunk, ", skip = ", skip);
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_LE(skip, n_vals);

  Decoder decoder(run);
  ASSERT_EQ(decoder.Advance(skip), skip);
  const auto rest = n_vals - skip;

  // Output buffer with one guard byte to catch out-of-bounds writes.
  std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(rest)) + 1, 0);
  const uint8_t guard = 0xA5;
  out.back() = guard;

  rle_size_t read = 0;
  while (read < rest) {
    const auto want = std::min(chunk, rest - read);
    const auto got =
        decoder.GetBatch(BitmapSpanMut(out.data(), /*bit_start=*/read), want);
    EXPECT_EQ(got, want) << "at pos " << read;
    ASSERT_GT(got, 0) << "at pos " << read;  // break on failure
    read += got;
    EXPECT_EQ(decoder.remaining(), rest - read);
  }

  EXPECT_EQ(decoder.remaining(), 0);
  EXPECT_EQ(out.back(), guard) << "decoder wrote past the end of the output";
  for (rle_size_t i = 0; i < rest; ++i) {
    EXPECT_EQ(bit_util::GetBit(out.data(), i), expected[skip + i]) << "at bit " << i;
  }
}

// The full battery of checks shared by both decoder types. `expected` is the
// full sequence of booleans the run is supposed to decode to.
template <typename Decoder>
void CheckBitMapDecoder(const typename Decoder::RunType& run,
                        const std::vector<bool>& expected) {
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  // The sub-checks below (Advance, single Get, several chunk sizes) assume a
  // run with a handful of values to exercise.
  ASSERT_GE(n_vals, 9);

  // remaining() reflects the run size before any value is read.
  {
    Decoder decoder(run);
    EXPECT_EQ(decoder.remaining(), n_vals);
  }

  // Empty requests are a no-op.
  {
    Decoder decoder(run);
    uint8_t out = 0;
    EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(&out), /*batch_size=*/0), 0);
    EXPECT_EQ(decoder.remaining(), n_vals);
  }

  // Decode the whole run with various chunk sizes: aligned (8), unaligned
  // (1, 3, 7, 9) and all-at-once.
  for (const rle_size_t chunk : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                 rle_size_t{8}, rle_size_t{9}, n_vals}) {
    CheckChunkedDecode<Decoder>(run, expected, chunk);
    // Same, but skipping the first 1..7 values first so the decoder's read
    // offset no longer matches the byte-aligned output (driving the
    // bit-unaligned path for decoders that have one).
    for (rle_size_t skip = 1; skip < 8 && skip < n_vals; ++skip) {
      CheckChunkedDecode<Decoder>(run, expected, chunk, skip);
    }
  }

  // Get() one value at a time, then read past the end.
  {
    Decoder decoder(run);
    std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)) + 1, 0);
    for (rle_size_t i = 0; i < n_vals; ++i) {
      EXPECT_TRUE(decoder.Get(BitmapSpanMut(out.data(), /*bit_start=*/i)));
      EXPECT_EQ(decoder.remaining(), n_vals - i - 1);
    }
    // Exhausted: nothing more can be read or advanced.
    EXPECT_FALSE(decoder.Get(BitmapSpanMut(out.data())));
    EXPECT_EQ(decoder.Advance(1), 0);
    EXPECT_EQ(decoder.remaining(), 0);
    for (rle_size_t i = 0; i < n_vals; ++i) {
      EXPECT_EQ(bit_util::GetBit(out.data(), i), expected[i]) << "at bit " << i;
    }
  }

  // Advance() skips values; the remainder still decodes correctly and
  // contiguously from the skipped position.
  {
    Decoder decoder(run);
    const rle_size_t skip = 5;
    EXPECT_EQ(decoder.Advance(skip), skip);
    EXPECT_EQ(decoder.remaining(), n_vals - skip);

    std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)) + 1, 0);
    rle_size_t read = skip;
    while (read < n_vals) {
      const auto got =
          decoder.GetBatch(BitmapSpanMut(out.data(), /*bit_start=*/read), n_vals - read);
      ASSERT_GT(got, 0);  // break on failure
      read += got;
    }
    EXPECT_EQ(decoder.remaining(), 0);
    // Bits before the skipped position must be left untouched (still zero).
    for (rle_size_t i = 0; i < skip; ++i) {
      EXPECT_FALSE(bit_util::GetBit(out.data(), i)) << "clobbered bit " << i;
    }
    for (rle_size_t i = skip; i < n_vals; ++i) {
      EXPECT_EQ(bit_util::GetBit(out.data(), i), expected[i]) << "at bit " << i;
    }
  }

  // Advancing more than available stops at the run boundary.
  {
    Decoder decoder(run);
    EXPECT_EQ(decoder.Advance(n_vals + 100), n_vals);
    EXPECT_EQ(decoder.remaining(), 0);
  }

  // Reset() rewinds the decoder so the run can be decoded again.
  {
    Decoder decoder(run);
    std::vector<uint8_t> scratch(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
    EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(scratch.data()), n_vals), n_vals);
    EXPECT_EQ(decoder.remaining(), 0);

    decoder.Reset(run);
    EXPECT_EQ(decoder.remaining(), n_vals);
    std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
    EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out.data()), n_vals), n_vals);
    for (rle_size_t i = 0; i < n_vals; ++i) {
      EXPECT_EQ(bit_util::GetBit(out.data(), i), expected[i]) << "at bit " << i;
    }
  }
}

}  // namespace

/***************************
 *  RleRunToBitMapDecoder  *
 ***************************/

struct RleBitMapCase {
  // The repeated boolean value of the run.
  bool value;
  // The number of values in the run.
  rle_size_t count;
};

class RleRunToBitMapDecoderTest : public ::testing::TestWithParam<RleBitMapCase> {};

TEST_P(RleRunToBitMapDecoderTest, Decode) {
  const auto& param = GetParam();

  // A boolean RLE run stores its value in a single (1-bit-wide) byte.
  const uint8_t data = param.value ? 1 : 0;
  const auto run = RleRun(&data, param.count, /*value_bit_width=*/1);

  // value() reports the repeated boolean.
  {
    RleRunToBitMapDecoder decoder(run);
    EXPECT_EQ(decoder.value(), param.value);
  }

  const std::vector<bool> expected(param.count, param.value);
  CheckBitMapDecoder<RleRunToBitMapDecoder>(run, expected);
}

INSTANTIATE_TEST_SUITE_P(RleBitmap, RleRunToBitMapDecoderTest,
                         ::testing::Values(RleBitMapCase{/*value=*/false, /*count=*/9},
                                           RleBitMapCase{/*value=*/true, /*count=*/9},
                                           RleBitMapCase{/*value=*/false, /*count=*/13},
                                           RleBitMapCase{/*value=*/true, /*count=*/13},
                                           RleBitMapCase{/*value=*/false, /*count=*/64},
                                           RleBitMapCase{/*value=*/true, /*count=*/64},
                                           RleBitMapCase{/*value=*/false, /*count=*/100},
                                           RleBitMapCase{/*value=*/true, /*count=*/100},
                                           RleBitMapCase{/*value=*/true, /*count=*/1000}),
                         [](const ::testing::TestParamInfo<RleBitMapCase>& info) {
                           return std::string(info.param.value ? "true_" : "false_") +
                                  std::to_string(info.param.count);
                         });

/*********************************
 *  BitPackedRunToBitMapDecoder  *
 *********************************/

struct BitPackedBitMapCase {
  std::string name;
  // The raw bit-packed bytes (LSB first). Must hold at least `count` bits.
  std::vector<uint8_t> bytes;
  // The number of values in the run.
  rle_size_t count;
};

class BitPackedRunToBitMapDecoderTest
    : public ::testing::TestWithParam<BitPackedBitMapCase> {};

TEST_P(BitPackedRunToBitMapDecoderTest, Decode) {
  const auto& param = GetParam();
  ASSERT_GE(param.bytes.size(), static_cast<size_t>(bit_util::BytesForBits(param.count)));

  const auto run = BitPackedRun(param.bytes.data(), param.count, /*value_bit_width=*/1,
                                /*max_read_bytes=*/-1);

  const std::vector<bool> expected = BitsFromBytes(param.bytes, param.count);
  CheckBitMapDecoder<BitPackedRunToBitMapDecoder>(run, expected);
}

INSTANTIATE_TEST_SUITE_P(
    RleBitmap, BitPackedRunToBitMapDecoderTest,
    ::testing::Values(BitPackedBitMapCase{/*name=*/"alternating",
                                          /*bytes=*/{0b10101010, 0b10101010},
                                          /*count=*/13},
                      BitPackedBitMapCase{/*name=*/"all_zeros",
                                          /*bytes=*/{0x00, 0x00},
                                          /*count=*/16},
                      BitPackedBitMapCase{/*name=*/"all_ones",
                                          /*bytes=*/{0xFF, 0xFF},
                                          /*count=*/16},
                      BitPackedBitMapCase{/*name=*/"mixed",
                                          /*bytes=*/{0b11001010, 0b00001111, 0b10110001},
                                          /*count=*/24},
                      BitPackedBitMapCase{/*name=*/"unaligned_count",
                                          /*bytes=*/{0b00110101, 0b11100100},
                                          /*count=*/11},
                      BitPackedBitMapCase{/*name=*/"large",
                                          /*bytes=*/std::vector<uint8_t>(16, 0b01101001),
                                          /*count=*/128}),
    [](const ::testing::TestParamInfo<BitPackedBitMapCase>& info) {
      return info.param.name;
    });

/*********************************
 *  RleBitPackedToBitMapDecoder  *
 *********************************/

namespace {

// Append the LEB128 (unsigned, little-endian base-128) encoding of `value`.
void AppendLeb128(std::vector<uint8_t>& out, uint32_t value) {
  uint8_t buf[bit_util::kMaxLEB128ByteLenFor<uint32_t>];
  const auto n_vals =
      bit_util::WriteLEB128(value, buf, static_cast<int32_t>(sizeof(buf)));
  ASSERT_GT(n_vals, 0);
  out.insert(out.end(), buf, buf + n_vals);
}

// Append an RLE run of `count` copies of `value` (1-bit values) to the encoded
// `bytes` stream and to the `expected` decoded sequence.
void AppendRleRun(std::vector<uint8_t>& bytes, std::vector<bool>& expected, bool value,
                  rle_size_t count) {
  AppendLeb128(bytes, static_cast<uint32_t>(count) << 1);  // low bit 0 => RLE
  bytes.push_back(value ? 1 : 0);
  expected.insert(expected.end(), count, value);
}

// Append a bit-packed run holding `packed.size()` groups of 8 (1-bit) values,
// LSB first, to the encoded `bytes` stream and to the `expected` sequence.
void AppendBitPackedRun(std::vector<uint8_t>& bytes, std::vector<bool>& expected,
                        const std::vector<uint8_t>& packed) {
  const auto groups = static_cast<rle_size_t>(packed.size());
  AppendLeb128(bytes, (static_cast<uint32_t>(groups) << 1) | 1);  // low bit 1 => packed
  bytes.insert(bytes.end(), packed.begin(), packed.end());
  for (rle_size_t i = 0; i < groups * 8; ++i) {
    expected.push_back(bit_util::GetBit(packed.data(), i));
  }
}

// Decode the whole `bytes` stream into a bitmap starting at bit offset
// `out_offset`, in successive GetBatch calls of at most `chunk` values, and
// compare against `expected`.
//
// Decoding in small, byte-unaligned chunks and (with a non-zero `out_offset`)
// at a non-aligned output position exercises how the decoder threads its output
// bit offset across runs that do not end on a byte boundary.
void CheckRleBitPackedDecode(const std::vector<uint8_t>& bytes,
                             const std::vector<bool>& expected, rle_size_t chunk,
                             rle_size_t out_offset = 0) {
  ARROW_SCOPED_TRACE("chunk = ", chunk, ", out_offset = ", out_offset);
  const auto n_vals = static_cast<rle_size_t>(expected.size());

  RleBitPackedToBitMapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  EXPECT_EQ(decoder.exhausted(), n_vals == 0);

  // Output buffer with one guard byte to catch out-of-bounds writes.
  std::vector<uint8_t> out(
      static_cast<size_t>(bit_util::BytesForBits(out_offset + n_vals)) + 1, 0);
  const uint8_t guard = 0xA5;
  out.back() = guard;

  rle_size_t read = 0;
  while (read < n_vals) {
    const auto want = std::min(chunk, n_vals - read);
    const auto got = decoder.GetBatch(
        BitmapSpanMut(out.data(), /*bit_start=*/out_offset + read), want);
    EXPECT_EQ(got, want) << "at pos " << read;
    ASSERT_GT(got, 0) << "at pos " << read;  // break on failure
    read += got;
  }

  EXPECT_EQ(read, n_vals);
  EXPECT_TRUE(decoder.exhausted());
  // Reading past the end yields nothing and leaves the decoder exhausted.
  uint8_t scratch = 0;
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(&scratch), 8), 0);
  EXPECT_TRUE(decoder.exhausted());

  EXPECT_EQ(out.back(), guard) << "decoder wrote past the end of the output";
  // Bits before the output offset must be left untouched (still zero).
  for (rle_size_t i = 0; i < out_offset; ++i) {
    EXPECT_FALSE(bit_util::GetBit(out.data(), i)) << "clobbered bit " << i;
  }
  for (rle_size_t i = 0; i < n_vals; ++i) {
    EXPECT_EQ(bit_util::GetBit(out.data(), out_offset + i), expected[i])
        << "at bit " << i;
  }
}

// Run the decode check over a battery of chunk sizes and output offsets.
void CheckRleBitPackedToBitMap(const std::vector<uint8_t>& bytes,
                               const std::vector<bool>& expected) {
  const auto n = static_cast<rle_size_t>(expected.size());
  ASSERT_GT(n, 0);
  for (const rle_size_t chunk : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                 rle_size_t{8}, rle_size_t{9}, rle_size_t{33}, n}) {
    CheckRleBitPackedDecode(bytes, expected, chunk);
    // A non-zero output offset forces the first run to start at a non-byte
    // aligned output position.
    for (rle_size_t out_offset = 1; out_offset < 8; ++out_offset) {
      CheckRleBitPackedDecode(bytes, expected, chunk, out_offset);
    }
  }
}

}  // namespace

TEST(RleBitPackedToBitMapDecoder, Empty) {
  // A default-constructed decoder is already exhausted.
  RleBitPackedToBitMapDecoder decoder;
  EXPECT_TRUE(decoder.exhausted());
  uint8_t out = 0;
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(&out), 8), 0);

  // So is one reset on an empty buffer.
  decoder.Reset(nullptr, 0);
  EXPECT_TRUE(decoder.exhausted());
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(&out), 8), 0);
}

TEST(RleBitPackedToBitMapDecoder, SingleRleZeros) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/100);
  CheckRleBitPackedToBitMap(bytes, expected);
}

TEST(RleBitPackedToBitMapDecoder, SingleRleOnes) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/100);
  CheckRleBitPackedToBitMap(bytes, expected);
}

TEST(RleBitPackedToBitMapDecoder, SingleBitPacked) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendBitPackedRun(bytes, expected, {0b10101010, 0b11001100, 0b11110000});
  CheckRleBitPackedToBitMap(bytes, expected);
}

TEST(RleBitPackedToBitMapDecoder, MixedRunsAligned) {
  // All runs end on a byte boundary, so each run starts byte-aligned in the
  // output.
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/16);
  AppendBitPackedRun(bytes, expected, {0b10101010, 0b01010101});
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/64);
  AppendBitPackedRun(bytes, expected, {0b00001111});
  CheckRleBitPackedToBitMap(bytes, expected);
}

TEST(RleBitPackedToBitMapDecoder, MixedRunsUnaligned) {
  // RLE runs with counts that are not multiples of 8 make each following run
  // start at a non-byte-aligned output position.
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/13);
  AppendBitPackedRun(bytes, expected, {0b01101001, 0b10010110});
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/5);
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/200);
  AppendBitPackedRun(bytes, expected, {0b11110000});
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/3);
  AppendBitPackedRun(bytes, expected, {0b10110001, 0b00011101});
  CheckRleBitPackedToBitMap(bytes, expected);
}

TEST(RleBitPackedToBitMapDecoder, ReadPastEnd) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/10);
  AppendBitPackedRun(bytes, expected, {0b10110010});
  const auto n = static_cast<rle_size_t>(expected.size());

  RleBitPackedToBitMapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n)) + 1, 0);
  // Requesting more values than available produces only the available ones.
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out.data()), n + 100), n);
  EXPECT_TRUE(decoder.exhausted());
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out.data()), 10), 0);
  for (rle_size_t i = 0; i < n; ++i) {
    EXPECT_EQ(bit_util::GetBit(out.data(), i), expected[i]) << "at bit " << i;
  }
}

TEST(RleBitPackedToBitMapDecoder, Reset) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/13);
  AppendBitPackedRun(bytes, expected, {0b01101001, 0b10010110});
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/20);
  const auto n = static_cast<rle_size_t>(expected.size());
  const auto size = static_cast<rle_size_t>(bytes.size());

  RleBitPackedToBitMapDecoder decoder(bytes.data(), size);
  std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n)), 0);
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out.data()), n), n);
  EXPECT_TRUE(decoder.exhausted());

  // Reset rewinds the decoder so the same buffer decodes again.
  decoder.Reset(bytes.data(), size);
  EXPECT_FALSE(decoder.exhausted());
  std::vector<uint8_t> out2(static_cast<size_t>(bit_util::BytesForBits(n)), 0);
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out2.data()), n), n);
  EXPECT_TRUE(decoder.exhausted());
  for (rle_size_t i = 0; i < n; ++i) {
    EXPECT_EQ(bit_util::GetBit(out2.data(), i), expected[i]) << "at bit " << i;
  }
}

TEST(RleBitPackedToBitMapDecoder, Truncated) {
  // GH-style malformed input: a bit-packed run declares more groups than the
  // buffer holds. The decoder yields what it can and is not exhausted.
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/10);
  // Declare 4 groups (32 values) of bit-packed data but provide only 1 byte.
  AppendLeb128(bytes, (4u << 1) | 1);
  bytes.push_back(0b10101010);

  RleBitPackedToBitMapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(16, 0);
  // The RLE run decodes fully; the truncated bit-packed run cannot be parsed.
  EXPECT_EQ(decoder.GetBatch(BitmapSpanMut(out.data()), 1000), 10);
  EXPECT_FALSE(decoder.exhausted());
  for (rle_size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(bit_util::GetBit(out.data(), i), true) << "at bit " << i;
  }
}

}  // namespace arrow::util
