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

#include <array>
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

/// Read the first `count` bits of `bytes` (LSB first) into a vector of booleans.
std::vector<bool> BitsFromBytes(const std::vector<uint8_t>& bytes, rle_size_t count) {
  std::vector<bool> bits(count);
  for (rle_size_t i = 0; i < count; ++i) {
    bits[i] = bit_util::GetBit(bytes.data(), i);
  }
  return bits;
}

/// Check the decoded output in `out` against `expected`.
/// Bits `out[out_offset..out_offset + count]` must equal `expected[skip..skip + count]`.
/// The `out_offset` bits before them must still be zero.
void CheckDecodedBits(const std::vector<uint8_t>& out, const std::vector<bool>& expected,
                      rle_size_t count, rle_size_t out_offset = 0, rle_size_t skip = 0) {
  ARROW_SCOPED_TRACE("out_offset = ", out_offset, ", skip = ", skip);
  for (rle_size_t i = 0; i < out_offset; ++i) {
    EXPECT_FALSE(bit_util::GetBit(out.data(), i)) << "clobbered bit " << i;
  }
  for (rle_size_t i = 0; i < count; ++i) {
    EXPECT_EQ(bit_util::GetBit(out.data(), out_offset + i), expected[skip + i])
        << "at bit " << i;
  }
}

/// Skip the first `skip` values with Advance(), then decode the rest of the run
/// into one output bitmap, `chunk` values at a time. Compare against `expected`.
///
/// `chunk` controls output bit alignment. When `chunk` is not a multiple of 8,
/// later calls start at a non-zero output bit offset.
///
/// `skip` shifts the decoder's read offset relative to the output offset.
/// A non-zero `skip` makes the two differ, which exercises the bit-unaligned read
/// path of BitPackedRunToBitMapDecoder. With `skip == 0` they stay in sync and
/// only the aligned path runs.
template <typename Decoder>
void CheckChunkedDecode(const typename Decoder::RunType& run,
                        const std::vector<bool>& expected, rle_size_t chunk = 1,
                        rle_size_t skip = 0) {
  ARROW_SCOPED_TRACE("chunk = ", chunk, ", skip = ", skip);
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_LE(skip, n_vals);

  Decoder decoder(run);
  const auto advanced = decoder.Advance(skip);
  ASSERT_EQ(advanced, skip);
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
  CheckDecodedBits(out, expected, /*count=*/rest, /*out_offset=*/0, skip);
}

/// All the checks shared by both decoder types.
///
/// `expected` is the full sequence of booleans the run should decode to.
template <typename Decoder>
void CheckBitMapDecoder(const typename Decoder::RunType& run,
                        const std::vector<bool>& expected) {
  const auto n_vals = static_cast<rle_size_t>(expected.size());

  // remaining() reflects the run size before any value is read.
  {
    Decoder decoder(run);
    EXPECT_EQ(decoder.remaining(), n_vals);
  }

  // Empty requests are a no-op.
  {
    Decoder decoder(run);
    uint8_t out = 0;
    const auto got = decoder.GetBatch(BitmapSpanMut(&out), /*batch_size=*/0);
    EXPECT_EQ(got, 0);
    EXPECT_EQ(decoder.remaining(), n_vals);
  }

  // Decode the whole run in several chunks.
  for (const rle_size_t chunk : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                 rle_size_t{8}, rle_size_t{9}, n_vals}) {
    CheckChunkedDecode<Decoder>(run, expected, chunk);
  }

  // Decode the whole run in several chunks, after an initial Advance that shifts
  // the run and output bit alignment.
  for (const rle_size_t chunk : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                 rle_size_t{8}, rle_size_t{9}, n_vals}) {
    for (rle_size_t skip = 1; skip < 8 && skip < n_vals; ++skip) {
      CheckChunkedDecode<Decoder>(run, expected, chunk, skip);
    }
  }

  // Get() one value at a time, then read past the end.
  {
    Decoder decoder(run);
    std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)) + 1, 0);
    for (rle_size_t i = 0; i < n_vals; ++i) {
      const bool ok = decoder.Get(BitmapSpanMut(out.data(), /*bit_start=*/i));
      EXPECT_TRUE(ok);
      EXPECT_EQ(decoder.remaining(), n_vals - i - 1);
    }
    // Exhausted: nothing more can be read or advanced.
    const bool ok = decoder.Get(BitmapSpanMut(out.data()));
    EXPECT_FALSE(ok);
    const auto advanced = decoder.Advance(1);
    EXPECT_EQ(advanced, 0);
    EXPECT_EQ(decoder.remaining(), 0);
    CheckDecodedBits(out, expected, /*count=*/n_vals);
  }

  // Advancing more than available stops at the run boundary.
  {
    Decoder decoder(run);
    const auto advanced = decoder.Advance(n_vals + 100);
    EXPECT_EQ(advanced, n_vals);
    EXPECT_EQ(decoder.remaining(), 0);
  }

  // Reset() rewinds the decoder so the run can be decoded again.
  {
    Decoder decoder(run);
    std::vector<uint8_t> out_1(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
    const auto scratch_got = decoder.GetBatch(BitmapSpanMut(out_1.data()), n_vals);
    EXPECT_EQ(scratch_got, n_vals);
    EXPECT_EQ(decoder.remaining(), 0);

    decoder.Reset(run);
    EXPECT_EQ(decoder.remaining(), n_vals);
    std::vector<uint8_t> out_2(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
    const auto got = decoder.GetBatch(BitmapSpanMut(out_2.data()), n_vals);
    EXPECT_EQ(got, n_vals);
    CheckDecodedBits(out_2, expected, /*count=*/n_vals);
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

INSTANTIATE_TEST_SUITE_P(  //
    RleBitmap, RleRunToBitMapDecoderTest,
    ::testing::Values(  //
        RleBitMapCase{.value = false, .count = 0},
        RleBitMapCase{.value = true, .count = 1},
        RleBitMapCase{.value = false, .count = 3},
        RleBitMapCase{.value = true, .count = 8},
        RleBitMapCase{.value = false, .count = 9},
        RleBitMapCase{.value = true, .count = 9},
        RleBitMapCase{.value = false, .count = 13},
        RleBitMapCase{.value = true, .count = 13},
        RleBitMapCase{.value = false, .count = 64},
        RleBitMapCase{.value = true, .count = 64},
        RleBitMapCase{.value = false, .count = 100},
        RleBitMapCase{.value = true, .count = 100},
        RleBitMapCase{.value = true, .count = 1000}),
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

INSTANTIATE_TEST_SUITE_P(  //
    RleBitmap, BitPackedRunToBitMapDecoderTest,
    ::testing::Values(  //
        BitPackedBitMapCase{.name = "empty", .bytes = {0b10110010}, .count = 0},
        BitPackedBitMapCase{.name = "single", .bytes = {0b00000001}, .count = 1},
        BitPackedBitMapCase{.name = "three", .bytes = {0b00000101}, .count = 3},
        BitPackedBitMapCase{.name = "eight", .bytes = {0b11010010}, .count = 8},
        BitPackedBitMapCase{
            .name = "alternating", .bytes = {0b10101010, 0b10101010}, .count = 13},
        BitPackedBitMapCase{.name = "all_zeros", .bytes = {0x00, 0x00}, .count = 16},
        BitPackedBitMapCase{.name = "all_ones", .bytes = {0xFF, 0xFF}, .count = 16},
        BitPackedBitMapCase{
            .name = "mixed", .bytes = {0b11001010, 0b00001111, 0b10110001}, .count = 24},
        BitPackedBitMapCase{
            .name = "unaligned_count", .bytes = {0b00110101, 0b11100100}, .count = 11},
        BitPackedBitMapCase{
            .name = "large",
            .bytes = std::vector<uint8_t>(16, 0b01101001),
            .count = 128,
        }),
    [](const ::testing::TestParamInfo<BitPackedBitMapCase>& info) {
      return info.param.name;
    });

/*********************************
 *  RleBitPackedToBitMapDecoder  *
 *********************************/

namespace {

/// Append the LEB128 (unsigned, little-endian base-128) encoding of `value`.
void AppendLeb128(std::vector<uint8_t>& out, uint32_t value) {
  std::array<uint8_t, bit_util::kMaxLEB128ByteLenFor<uint32_t>> buf;
  const auto n_bytes =
      bit_util::WriteLEB128(value, buf.data(), static_cast<int32_t>(buf.size()));
  ASSERT_GT(n_bytes, 0);
  out.insert(out.end(), buf.data(), buf.data() + n_bytes);
}

void AppendRleRun(std::vector<uint8_t>& bytes, std::vector<bool>& expected, bool value,
                  rle_size_t count) {
  AppendLeb128(bytes, static_cast<uint32_t>(count) << 1);  // low bit 0 => RLE
  bytes.push_back(value ? 1 : 0);
  expected.insert(expected.end(), count, value);
}

void AppendBitPackedRun(std::vector<uint8_t>& bytes, std::vector<bool>& expected,
                        const std::vector<uint8_t>& packed) {
  const auto groups = static_cast<rle_size_t>(packed.size());
  AppendLeb128(bytes, (static_cast<uint32_t>(groups) << 1) | 1);  // low bit 1 => packed
  bytes.insert(bytes.end(), packed.begin(), packed.end());
  for (rle_size_t i = 0; i < groups * 8; ++i) {
    expected.push_back(bit_util::GetBit(packed.data(), i));
  }
}

/// Decode the whole `bytes` into a bitmap and check it against `expected`.
///
/// Decode `chunk` values per GetBatch call to check the decoder state between
/// calls. The output starts at bit offset `out_offset`. A non-zero offset makes
/// the output and the encoded `bytes` use different bit alignment.
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
  const auto past_end = decoder.GetBatch(BitmapSpanMut(&scratch), 8);
  EXPECT_EQ(past_end, 0);
  EXPECT_TRUE(decoder.exhausted());

  EXPECT_EQ(out.back(), guard) << "decoder wrote past the end of the output";
  CheckDecodedBits(out, expected, /*count=*/n_vals, out_offset);
}

/// Run the decode check over a battery of chunk sizes and output offsets.
void CheckRleBitPackedToBitMap(const std::vector<uint8_t>& bytes,
                               const std::vector<bool>& expected) {
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_GT(n_vals, 0);
  for (const rle_size_t chunk : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                 rle_size_t{8}, rle_size_t{9}, rle_size_t{33}, n_vals}) {
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
  auto got = decoder.GetBatch(BitmapSpanMut(&out), 8);
  EXPECT_EQ(got, 0);

  // So is one reset on an empty buffer.
  decoder.Reset(nullptr, 0);
  EXPECT_TRUE(decoder.exhausted());
  got = decoder.GetBatch(BitmapSpanMut(&out), 8);
  EXPECT_EQ(got, 0);
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
  const auto n_vals = static_cast<rle_size_t>(expected.size());

  RleBitPackedToBitMapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)) + 1, 0);
  // Requesting more values than available produces only the available ones.
  auto got = decoder.GetBatch(BitmapSpanMut(out.data()), n_vals + 100);
  EXPECT_EQ(got, n_vals);
  EXPECT_TRUE(decoder.exhausted());
  got = decoder.GetBatch(BitmapSpanMut(out.data()), 10);
  EXPECT_EQ(got, 0);
  CheckDecodedBits(out, expected, /*count=*/n_vals);
}

TEST(RleBitPackedToBitMapDecoder, Reset) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/13);
  AppendBitPackedRun(bytes, expected, {0b01101001, 0b10010110});
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/20);
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  const auto data_size = static_cast<rle_size_t>(bytes.size());

  RleBitPackedToBitMapDecoder decoder(bytes.data(), data_size);
  std::vector<uint8_t> out_1(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
  const auto got_1 = decoder.GetBatch(BitmapSpanMut(out_1.data()), n_vals);
  EXPECT_EQ(got_1, n_vals);
  EXPECT_TRUE(decoder.exhausted());

  // Reset rewinds the decoder so the same buffer decodes again.
  decoder.Reset(bytes.data(), data_size);
  EXPECT_FALSE(decoder.exhausted());
  std::vector<uint8_t> out_2(static_cast<size_t>(bit_util::BytesForBits(n_vals)), 0);
  const auto got_2 = decoder.GetBatch(BitmapSpanMut(out_2.data()), n_vals);
  EXPECT_EQ(got_2, n_vals);
  EXPECT_TRUE(decoder.exhausted());
  CheckDecodedBits(out_2, expected, /*count=*/n_vals);
}

TEST(RleBitPackedToBitMapDecoder, Truncated) {
  // Malformed input: a bit-packed run declares more values than the buffer
  // holds. The decoder should return the values it can read and report that it
  // is not exhausted, rather than crash or read out of bounds.
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/10);
  // The header declares 4 bytes (4 * 8 = 32 values) of bit-packed data, but only
  // 1 byte follows.
  AppendLeb128(bytes, (4u << 1) | 1);
  bytes.push_back(0b10101010);

  RleBitPackedToBitMapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(16, 0);
  // The RLE run decodes fully; the truncated bit-packed run cannot be parsed.
  const auto got = decoder.GetBatch(BitmapSpanMut(out.data()), 1000);
  EXPECT_EQ(got, 10);
  EXPECT_FALSE(decoder.exhausted());
  CheckDecodedBits(out, expected, /*count=*/10);
}

}  // namespace arrow::util
