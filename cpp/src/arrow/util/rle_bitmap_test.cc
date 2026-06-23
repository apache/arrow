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
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/rle_bitmap_internal.h"
#include "arrow/util/rle_encoding_internal.h"

namespace arrow::util {

namespace {

/// Make a vector of `size` pseudo-random bytes, deterministic for a given `seed`.
std::vector<uint8_t> MakeRandomBytes(size_t size, uint32_t seed = 56) {
  std::vector<uint8_t> bytes(size);
  std::minstd_rand gen(seed);
  std::uniform_int_distribution<int> dist(0, 255);  // no standard support for uint8_t
  for (auto& byte : bytes) {
    byte = static_cast<uint8_t>(dist(gen));
  }
  return bytes;
}

/// Read the first `count` bits of `bytes` (LSB first) into a vector of booleans.
std::vector<bool> BitsFromBytes(const std::vector<uint8_t>& bytes, rle_size_t count) {
  std::vector<bool> bits(count);
  for (rle_size_t i = 0; i < count; ++i) {
    bits[i] = bit_util::GetBit(bytes.data(), i);
  }
  return bits;
}

struct CheckDecodedBitsParams {
  const std::vector<uint8_t>& actual;
  const std::vector<bool>& expected;
  rle_size_t count;
  rle_size_t actual_start_bit = 0;
  rle_size_t expected_start_idx = 0;
};

/// Check the decoded output in `out` against `expected`.
void CheckDecodedBits(const CheckDecodedBitsParams& params) {
  ARROW_SCOPED_TRACE("out_start_bit = ", params.actual_start_bit,
                     ", expected_start_idx = ", params.expected_start_idx);
  for (rle_size_t i = 0; i < params.count; ++i) {
    ASSERT_EQ(bit_util::GetBit(params.actual.data(), params.actual_start_bit + i),
              params.expected[params.expected_start_idx + i])
        << "first difference at bit " << i;
  }
}

struct CheckBitsEqualParams {
  const std::vector<uint8_t>& actual;
  const std::vector<uint8_t>& expected;
  rle_size_t count;
  rle_size_t actual_start_bit = 0;
  rle_size_t expected_start_bit = 0;
};

/// Check that two bit ranges, stored in `actual` and `expected`, are equal.
void CheckBitsEqual(const CheckBitsEqualParams& params) {
  ARROW_SCOPED_TRACE("actual_start_bit = ", params.actual_start_bit,
                     ", expected_start_bit = ", params.expected_start_bit);
  for (rle_size_t i = 0; i < params.count; ++i) {
    ASSERT_EQ(bit_util::GetBit(params.actual.data(), params.actual_start_bit + i),
              bit_util::GetBit(params.expected.data(), params.expected_start_bit + i))
        << "first difference at bit " << i;
  }
}

/// Skip the first `expected_skip` values with Advance(), then decode the rest of the run
/// into one output bitmap, `chunk_size` values at a time. Compare against `expected`.
///
/// `chunk_size` controls output bit alignment. When `chunk_size` is not a multiple of 8,
/// later calls start at a non-zero output bit offset.
///
/// `expected_skip` shifts the decoder's read offset relative to the output offset.
/// A non-zero `expected_skip` makes the two differ, which exercises the bit-unaligned
/// read path of BitPackedRunToBitmapDecoder. With `expected_skip == 0` they stay in sync
/// and only the aligned path runs.
template <typename Decoder>
void CheckDecoderValuesChunked(const typename Decoder::RunType& run,
                               const std::vector<bool>& expected,
                               rle_size_t chunk_size = 1, rle_size_t expected_skip = 0) {
  ARROW_SCOPED_TRACE("chunk_size = ", chunk_size, ", expected_skip = ", expected_skip);

  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_LE(expected_skip, n_vals);

  Decoder decoder(run);
  const auto advanced = decoder.Advance(expected_skip);
  ASSERT_EQ(advanced, expected_skip);
  const auto n_vals_to_decode = n_vals - expected_skip;

  // Output buffer
  const auto n_bytes = static_cast<size_t>(bit_util::BytesForBits(n_vals_to_decode));
  std::vector<uint8_t> out(n_bytes, 0);

  rle_size_t n_val_read = 0;
  while (n_val_read < n_vals_to_decode) {
    const auto want = std::min(chunk_size, n_vals_to_decode - n_val_read);
    const auto got =
        decoder.GetBatch(BitmapSpanMut(out.data(), /*bit_start=*/n_val_read), want);
    EXPECT_EQ(got, want) << "at pos " << n_val_read;
    ASSERT_GT(got, 0) << "at pos " << n_val_read;  // break on failure
    n_val_read += got;
    EXPECT_EQ(decoder.remaining(), n_vals_to_decode - n_val_read);
  }

  EXPECT_EQ(decoder.remaining(), 0);
  CheckDecodedBits({
      .actual = out,
      .expected = expected,
      .count = n_vals_to_decode,
      .actual_start_bit = 0,
      .expected_start_idx = expected_skip,
  });
}

/// Decode a chunk of data into a known output to check for out of bounds write.
///
/// @see CheckDecoderValuesChunked
template <typename Decoder>
void CheckDecoderClobber(const typename Decoder::RunType& run,
                         const std::vector<bool>& expected, rle_size_t chunk_size = 1,
                         rle_size_t expected_skip = 0) {
  ARROW_SCOPED_TRACE("chunk_size = ", chunk_size, ", expected_skip = ", expected_skip);

  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_LE(expected_skip, n_vals);

  Decoder decoder(run);
  const auto advanced = decoder.Advance(expected_skip);
  ASSERT_EQ(advanced, expected_skip);
  const auto n_vals_to_decode = n_vals - expected_skip;

  // Output buffer with enough capacity to store a full chunk plus extra bytes as
  // clobbers/guard to check for out of bounds write.
  const auto n_bytes = static_cast<size_t>(bit_util::BytesForBits(chunk_size) +
                                           bit_util::CeilDiv(n_vals, chunk_size) + 2);
  // This seed is arbitrary and of little importance. We are simply trying to avoid an
  // unlikely case where guards have the same pattern in all invocations.
  const auto out_pattern =
      MakeRandomBytes(n_bytes, /* seed= */ (chunk_size << 16) ^ expected_skip);
  auto out = out_pattern;

  rle_size_t n_val_read = 0;
  rle_size_t out_bit_start = 0;
  while (n_val_read < n_vals_to_decode) {
    // Clean output buffer
    out = out_pattern;
    const auto want = std::min(chunk_size, n_vals_to_decode - n_val_read);
    const auto got = decoder.GetBatch(BitmapSpanMut(out.data(), out_bit_start), want);
    ASSERT_GT(got, 0) << "at pos " << n_val_read;  // break on failure
    EXPECT_EQ(got, want) << "at pos " << n_val_read;
    // Check that the leading bits have not been modified
    CheckBitsEqual({.actual = out, .expected = out_pattern, .count = out_bit_start});
    // Check that the trailing bits have not been modified
    CheckBitsEqual({
        .actual = out,
        .expected = out_pattern,
        .count = static_cast<rle_size_t>(8 * n_bytes) - (out_bit_start + want),
        .actual_start_bit = out_bit_start + want,
        .expected_start_bit = out_bit_start + want,
    });
    // Check decoded bits are also correct
    CheckDecodedBits({
        .actual = out,
        .expected = expected,
        .count = want,
        .actual_start_bit = out_bit_start,
        .expected_start_idx = expected_skip + n_val_read,
    });

    n_val_read += got;
    ++out_bit_start;
    EXPECT_EQ(decoder.remaining(), n_vals_to_decode - n_val_read);
  }
}

/// All the checks shared by both decoder types.
///
/// `expected` is the full sequence of booleans the run should decode to.
template <typename Decoder>
void CheckBitmapDecoder(const typename Decoder::RunType& run,
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
  for (const rle_size_t chunk_size : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                      rle_size_t{8}, rle_size_t{9}, n_vals, n_vals + 1}) {
    CheckDecoderValuesChunked<Decoder>(run, expected, chunk_size);
  }

  // Decode the whole run in several chunks, after an initial Advance that shifts
  // the run and output bit alignment.
  for (const rle_size_t chunk_size : {rle_size_t{1}, rle_size_t{3}, rle_size_t{7},
                                      rle_size_t{8}, rle_size_t{9}, n_vals, n_vals + 1}) {
    for (rle_size_t expected_skip = 1; expected_skip < 8 && expected_skip < n_vals;
         ++expected_skip) {
      // Check the decoding happens as expected
      CheckDecoderValuesChunked<Decoder>(run, expected, chunk_size, expected_skip);
      // Check the decoding does not write out of bounds
      CheckDecoderClobber<Decoder>(run, expected, chunk_size, expected_skip);
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
    CheckDecodedBits({.actual = out, .expected = expected, .count = n_vals});
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
    CheckDecodedBits({.actual = out_2, .expected = expected, .count = n_vals});
  }
}

}  // namespace

/***************************
 *  RleRunToBitmapDecoder  *
 ***************************/

class RleRunToBitmapDecoderTest : public ::testing::TestWithParam<rle_size_t> {};

TEST_P(RleRunToBitmapDecoderTest, Decode) {
  const auto& count = GetParam();

  // Only two possible repeated value
  for (bool value : {true, false}) {
    ARROW_SCOPED_TRACE("value = ", value);

    // A boolean RLE run stores its value in a single (1-bit-wide) byte.
    const uint8_t data = value ? 1 : 0;
    const auto run = RleRun(&data, count, /*value_bit_width=*/1);

    // value() reports the repeated boolean.
    RleRunToBitmapDecoder decoder(run);
    EXPECT_EQ(decoder.value(), value);

    const std::vector<bool> expected(count, value);
    CheckBitmapDecoder<RleRunToBitmapDecoder>(run, expected);
  }
}

INSTANTIATE_TEST_SUITE_P(  //
    RleBitmap, RleRunToBitmapDecoderTest,
    ::testing::Values(0, 1, 3, 8, 9, 13, 64, 100, 177));

/*********************************
 *  BitPackedRunToBitmapDecoder  *
 *********************************/

struct BitPackedBitmapCase {
  std::string name;
  // The raw bit-packed bytes (LSB first). Must hold at least `count` bits.
  std::vector<uint8_t> bytes;
  // The number of values in the run.
  rle_size_t count;
};

class BitPackedRunToBitmapDecoderTest
    : public ::testing::TestWithParam<BitPackedBitmapCase> {};

TEST_P(BitPackedRunToBitmapDecoderTest, Decode) {
  const auto& param = GetParam();
  ASSERT_GE(param.bytes.size(), static_cast<size_t>(bit_util::BytesForBits(param.count)));

  const auto run = BitPackedRun(param.bytes.data(), param.count, /*value_bit_width=*/1,
                                /*max_read_bytes=*/-1);

  const std::vector<bool> expected = BitsFromBytes(param.bytes, param.count);
  CheckBitmapDecoder<BitPackedRunToBitmapDecoder>(run, expected);
}

INSTANTIATE_TEST_SUITE_P(  //
    RleBitmap, BitPackedRunToBitmapDecoderTest,
    ::testing::Values(  //
        BitPackedBitmapCase{.name = "empty", .bytes = {0b10110010}, .count = 0},
        BitPackedBitmapCase{.name = "single", .bytes = {0b00000001}, .count = 1},
        BitPackedBitmapCase{.name = "three", .bytes = {0b00000101}, .count = 3},
        BitPackedBitmapCase{.name = "eight", .bytes = {0b11010010}, .count = 8},
        BitPackedBitmapCase{
            .name = "alternating", .bytes = {0b10101010, 0b10101010}, .count = 13},
        BitPackedBitmapCase{.name = "all_zeros", .bytes = {0x00, 0x00}, .count = 16},
        BitPackedBitmapCase{.name = "all_ones", .bytes = {0xFF, 0xFF}, .count = 16},
        BitPackedBitmapCase{
            .name = "mixed", .bytes = {0b11001010, 0b00001111, 0b10110001}, .count = 24},
        BitPackedBitmapCase{
            .name = "unaligned_count", .bytes = {0b00110101, 0b11100100}, .count = 11},
        BitPackedBitmapCase{
            .name = "large",
            .bytes = std::vector<uint8_t>(16, 0b01101001),
            .count = 128,
        }),
    [](const ::testing::TestParamInfo<BitPackedBitmapCase>& info) {
      return info.param.name;
    });

/*********************************
 *  RleBitPackedToBitmapDecoder  *
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
/// Decode `chunk_size` values per GetBatch call to check the decoder state between
/// calls. The output starts at bit offset `out_offset`. A non-zero offset makes
/// the output and the encoded `bytes` use different bit alignment.
void CheckRleBitPackedDecode(const std::vector<uint8_t>& bytes,
                             const std::vector<bool>& expected, rle_size_t chunk_size,
                             rle_size_t out_offset = 0) {
  ARROW_SCOPED_TRACE("chunk_size = ", chunk_size, ", out_offset = ", out_offset);
  const auto n_vals = static_cast<rle_size_t>(expected.size());

  RleBitPackedToBitmapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  EXPECT_EQ(decoder.exhausted(), n_vals == 0);

  // Output buffer with one guard byte to catch out-of-bounds writes.
  std::vector<uint8_t> out(
      static_cast<size_t>(bit_util::BytesForBits(out_offset + n_vals)) + 1, 0);
  const uint8_t guard = 0xA5;
  out.back() = guard;

  rle_size_t read = 0;
  while (read < n_vals) {
    const auto want = std::min(chunk_size, n_vals - read);
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
  CheckDecodedBits({
      .actual = out,
      .expected = expected,
      .count = n_vals,
      .actual_start_bit = out_offset,
  });
}

/// Run the decode check over a battery of chunk sizes and output offsets.
void CheckRleBitPackedToBitmap(const std::vector<uint8_t>& bytes,
                               const std::vector<bool>& expected) {
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  ASSERT_GT(n_vals, 0);
  for (const rle_size_t chunk_size :
       {rle_size_t{1}, rle_size_t{3}, rle_size_t{7}, rle_size_t{8}, rle_size_t{9},
        rle_size_t{33}, n_vals, n_vals + 1}) {
    CheckRleBitPackedDecode(bytes, expected, chunk_size);
    // A non-zero output offset forces the first run to start at a non-byte
    // aligned output position.
    for (rle_size_t out_offset = 1; out_offset < 8; ++out_offset) {
      CheckRleBitPackedDecode(bytes, expected, chunk_size, out_offset);
    }
  }
}

}  // namespace

TEST(RleBitPackedToBitmapDecoder, Empty) {
  // A default-constructed decoder is already exhausted.
  RleBitPackedToBitmapDecoder decoder;
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

TEST(RleBitPackedToBitmapDecoder, SingleRleZeros) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/100);
  CheckRleBitPackedToBitmap(bytes, expected);
}

TEST(RleBitPackedToBitmapDecoder, SingleRleOnes) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/100);
  CheckRleBitPackedToBitmap(bytes, expected);
}

TEST(RleBitPackedToBitmapDecoder, SingleBitPacked) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendBitPackedRun(bytes, expected, {0b10101010, 0b11001100, 0b11110000});
  CheckRleBitPackedToBitmap(bytes, expected);
}

TEST(RleBitPackedToBitmapDecoder, MixedRunsAligned) {
  // All runs end on a byte boundary, so each run starts byte-aligned in the
  // output.
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/16);
  AppendBitPackedRun(bytes, expected, {0b10101010, 0b01010101});
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/64);
  AppendBitPackedRun(bytes, expected, {0b00001111});
  CheckRleBitPackedToBitmap(bytes, expected);
}

TEST(RleBitPackedToBitmapDecoder, MixedRunsUnaligned) {
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
  CheckRleBitPackedToBitmap(bytes, expected);
}

TEST(RleBitPackedToBitmapDecoder, ReadPastEnd) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/10);
  AppendBitPackedRun(bytes, expected, {0b10110010});
  const auto n_vals = static_cast<rle_size_t>(expected.size());

  RleBitPackedToBitmapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(static_cast<size_t>(bit_util::BytesForBits(n_vals)) + 1, 0);
  // Requesting more values than available produces only the available ones.
  auto got = decoder.GetBatch(BitmapSpanMut(out.data()), n_vals + 100);
  EXPECT_EQ(got, n_vals);
  EXPECT_TRUE(decoder.exhausted());
  got = decoder.GetBatch(BitmapSpanMut(out.data()), 10);
  EXPECT_EQ(got, 0);
  CheckDecodedBits({.actual = out, .expected = expected, .count = n_vals});
}

TEST(RleBitPackedToBitmapDecoder, Reset) {
  std::vector<uint8_t> bytes;
  std::vector<bool> expected;
  AppendRleRun(bytes, expected, /*value=*/true, /*count=*/13);
  AppendBitPackedRun(bytes, expected, {0b01101001, 0b10010110});
  AppendRleRun(bytes, expected, /*value=*/false, /*count=*/20);
  const auto n_vals = static_cast<rle_size_t>(expected.size());
  const auto data_size = static_cast<rle_size_t>(bytes.size());

  RleBitPackedToBitmapDecoder decoder(bytes.data(), data_size);
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
  CheckDecodedBits({.actual = out_2, .expected = expected, .count = n_vals});
}

TEST(RleBitPackedToBitmapDecoder, Truncated) {
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

  RleBitPackedToBitmapDecoder decoder(bytes.data(),
                                      static_cast<rle_size_t>(bytes.size()));
  std::vector<uint8_t> out(16, 0);
  // The RLE run decodes fully; the truncated bit-packed run cannot be parsed.
  const auto got = decoder.GetBatch(BitmapSpanMut(out.data()), 1000);
  EXPECT_EQ(got, 10);
  EXPECT_FALSE(decoder.exhausted());
  CheckDecodedBits({.actual = out, .expected = expected, .count = 10});
}

}  // namespace arrow::util
