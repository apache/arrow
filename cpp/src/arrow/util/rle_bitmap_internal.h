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

#include <algorithm>
#include <cstring>

#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/rle_encoding_internal.h"

namespace arrow::util {

/// A lightweight view over a bitmap.
template <typename B = uint8_t>
class BitmapSpan {
 public:
  using size_type = rle_size_t;
  using byte_type = B;

  explicit constexpr BitmapSpan(byte_type* data, size_type bit_start = 0) noexcept
      : data_{data}, bit_start_{bit_start} {
    Normalize();
  }

  /// Pointer to the byte where the first value is stored.
  constexpr byte_type* data() const noexcept { return data_; }

  /// Bit offset of the first value in the first byte.
  constexpr size_type bit_start() const noexcept { return bit_start_; }

  /// Return a new span starting at the given position.
  constexpr BitmapSpan NewStartingAt(size_type bit_start) const noexcept {
    auto out = *this;
    out.bit_start_ += bit_start;
    out.Normalize();
    return out;
  }

 private:
  byte_type* data_;
  size_type bit_start_ = 0;

  /// Ensure `bit_start` always ends up in [0, 8[, even when it starts negative.
  constexpr void Normalize() {
    // On two's-complement targets an arithmetic right shift floors, and the AND mask
    // yields the non-negative remainder (e.g. -1 -> byte -1, offset 7), unlike `/` and
    // `%` which truncate toward zero.
    data_ += bit_start_ >> 3;
    bit_start_ &= 0b0111;
  }
};

using BitmapSpanMut = BitmapSpan<uint8_t>;
using BitmapSpanConst = BitmapSpan<const uint8_t>;

namespace internal_rle {
template <typename CRTP>
class RunToBitmapDecoderMixin {
 public:
  /// Advance by as many values as provided or until exhaustion of the decoder.
  /// Return the number of values skipped.
  [[nodiscard]] rle_size_t Advance(rle_size_t batch_size) {
    const auto steps = std::min(batch_size, derived()->remaining());
    derived()->AdvanceUnsafe(steps);
    ARROW_DCHECK_GE(derived()->remaining(), 0);
    return steps;
  }

  /// Get the next value and return false if there are no more.
  [[nodiscard]] constexpr bool Get(BitmapSpanMut out) {
    return derived()->GetBatch(out, 1) == 1;
  }

  /// Get a batch of values return the number of decoded elements.
  ///
  /// May write fewer elements to the output than requested if there are not
  /// enough values left.
  [[nodiscard]] rle_size_t GetBatch(BitmapSpanMut out, rle_size_t batch_size) {
    const auto out_bit_offset = out.bit_start();
    ARROW_DCHECK_GE(out_bit_offset, 0);
    ARROW_DCHECK_LT(out_bit_offset, 8);

    if (ARROW_PREDICT_FALSE(derived()->remaining() == 0 || batch_size == 0)) {
      return 0;
    }

    // HEADER: Writing inside the first byte if caller gives a non-aligned input
    if (ARROW_PREDICT_FALSE(out_bit_offset != 0)) {
      const auto n_vals = derived()->GetBatchFirstByte(out, batch_size);
      // If we exhausted the values in this decoder, or we filled what was required,
      // then the following recursive call will return 0.
      // If in the opposite case, then we will continue on a byte-aligned boundary.
      return n_vals + GetBatch(out.NewStartingAt(n_vals), batch_size - n_vals);
    }

    // Writing full bytes
    const auto n_vals = derived()->GetBatchFast(out, batch_size);

    // TRAILER: Writing inside the last byte if caller asked for non multiple of 8 values
    const auto n_last_vals = std::min(batch_size - n_vals, derived()->remaining());
    if (ARROW_PREDICT_FALSE(n_last_vals > 0)) {
      ARROW_DCHECK_LT(n_last_vals, 8);
      out = out.NewStartingAt(n_vals);
      return n_vals + derived()->GetBatchFirstByte(out, n_last_vals);
    }

    return n_vals;
  }

 protected:
  [[nodiscard]] rle_size_t GetBatchFromByte(BitmapSpanMut out, rle_size_t batch_size,
                                            uint8_t src) {
    const auto out_bit_offset = out.bit_start();
    ARROW_DCHECK_GE(out_bit_offset, 0);
    ARROW_DCHECK_LT(out_bit_offset, 8);
    ARROW_DCHECK_GT(derived()->remaining(), 0);
    ARROW_DCHECK_GE(batch_size, 0);

    // Empty bits in first byte that we can fill
    const auto empty_bits = rle_size_t{8} - out_bit_offset;
    // Number of bits in first byte that we want to fill
    const auto desired_bits = std::min(empty_bits, batch_size);
    // Try to advance, and get number of bits we had remaining
    const auto n_bits = Advance(desired_bits);
    // Copy relevant bits from the value pattern to the output.
    *out.data() = bit_util::CopyBits<uint8_t, /* kAllowFullCopy= */ false>({
        .src = src,
        .dst = *out.data(),
        .start = static_cast<uint8_t>(out_bit_offset),
        .end = static_cast<uint8_t>(out_bit_offset + n_bits),
    });

    return n_bits;
  }

 private:
  CRTP* derived() { return static_cast<CRTP*>(this); }
  CRTP const* derived() const { return static_cast<CRTP const*>(this); }
};
}  // namespace internal_rle

class RleRunToBitmapDecoder
    : public internal_rle::RunToBitmapDecoderMixin<RleRunToBitmapDecoder> {
 public:
  /// The type of run that can be decoded.
  using RunType = RleRun;

  constexpr RleRunToBitmapDecoder() noexcept = default;

  explicit RleRunToBitmapDecoder(const RunType& run) noexcept { Reset(run); }

  void Reset(const RunType& run) noexcept {
    values_left_ = run.values_count();
    if (run.value_little_endian() == 0) {
      value_pattern_ = uint8_t{0};
    } else {
      value_pattern_ = uint8_t{0xFF};
    }
  }

  /// Return the number of values that can be advanced.
  rle_size_t remaining() const { return values_left_; }

  /// Return the repeated value of this decoder.
  constexpr bool value() const { return value_pattern_ != 0; }

 private:
  friend class internal_rle::RunToBitmapDecoderMixin<RleRunToBitmapDecoder>;

  /// The byte pattern for 8 values (full ones or full zeros).
  uint8_t value_pattern_ = {};
  /// Number of values left to decode.
  rle_size_t values_left_ = 0;

  void AdvanceUnsafe(rle_size_t batch_size) { values_left_ -= batch_size; }

  /// Get batch values to fill the first incomplete byte of the output.
  [[nodiscard]] rle_size_t GetBatchFirstByte(BitmapSpanMut out, rle_size_t batch_size) {
    return GetBatchFromByte(out, batch_size, value_pattern_);
  }

  /// Get batch in full bytes using memset.
  [[nodiscard]] rle_size_t GetBatchFast(BitmapSpanMut out, rle_size_t batch_size) {
    ARROW_DCHECK_EQ(out.bit_start(), 0);
    const auto n_bytes = std::min(batch_size, remaining()) / 8;
    std::memset(out.data(), value_pattern_, n_bytes);
    const auto n_vals = 8 * n_bytes;
    AdvanceUnsafe(n_vals);
    return n_vals;
  }
};

class BitPackedRunToBitmapDecoder
    : public internal_rle::RunToBitmapDecoderMixin<BitPackedRunToBitmapDecoder> {
 public:
  /// The type of run that can be decoded.
  using RunType = BitPackedRun;

  constexpr BitPackedRunToBitmapDecoder() noexcept = default;

  explicit BitPackedRunToBitmapDecoder(const RunType& run) noexcept { Reset(run); }

  void Reset(const RunType& run) noexcept {
    data_ = run.raw_data_ptr();
    values_count_ = run.values_count();
    values_read_ = 0;
    ARROW_DCHECK(run.raw_data_max_size() < 0 ||
                 bit_util::BytesForBits(values_count_) <= run.raw_data_max_size());
  }

  /// Return the number of values that can be advanced.
  constexpr rle_size_t remaining() const { return values_count_ - values_read_; }

  /// Get a batch of values return the number of decoded elements.
  /// May write fewer elements to the output than requested if there are not enough values
  /// left.
  [[nodiscard]] rle_size_t GetBatch(BitmapSpanMut out, rle_size_t batch_size) {
    // WARN: Slow case where bit offsets of input and output are not aligned.
    // We loose the ability to do memcpy
    if (ARROW_PREDICT_FALSE(out.bit_start() != unread_values_bit_offset())) {
      return GetBatchMisaligned(out, batch_size);
    }
    return Base::GetBatch(out, batch_size);
  }

 private:
  using Base = internal_rle::RunToBitmapDecoderMixin<BitPackedRunToBitmapDecoder>;
  friend class internal_rle::RunToBitmapDecoderMixin<BitPackedRunToBitmapDecoder>;

  /// The pointer to the beginning of the run
  const uint8_t* data_ = nullptr;
  /// The total number of values in the run
  rle_size_t values_count_ = 0;
  /// The number of values read by the decoder
  rle_size_t values_read_ = 0;

  /// Start pointer of the unread values (may contain values already read).
  const uint8_t* unread_values_ptr() const noexcept { return data_ + (values_read_ / 8); }

  /// Bit in @ref unread_values_ptr where the unread values start.
  rle_size_t unread_values_bit_offset() const noexcept { return values_read_ % 8; }

  void AdvanceUnsafe(rle_size_t batch_size) { values_read_ += batch_size; }

  /// Get batch values to fill the first incomplete byte of the output.
  [[nodiscard]] rle_size_t GetBatchFirstByte(BitmapSpanMut out, rle_size_t batch_size) {
    return GetBatchFromByte(out, batch_size, *unread_values_ptr());
  }

  /// Get batch in full bytes using memcpy.
  [[nodiscard]] rle_size_t GetBatchFast(BitmapSpanMut out, rle_size_t batch_size) {
    ARROW_DCHECK_EQ(out.bit_start(), 0);
    const auto n_bytes = std::min(batch_size, remaining()) / 8;
    std::memcpy(out.data(), unread_values_ptr(), n_bytes);
    const auto n_vals = 8 * n_bytes;
    AdvanceUnsafe(n_vals);
    return n_vals;
  }

  /// Correct and slow function for reading a batch one bit at a time.
  [[nodiscard]] rle_size_t GetBatchSlow(BitmapSpanMut out, rle_size_t batch_size) {
    const rle_size_t to_read = std::min(batch_size, remaining());
    for (rle_size_t i = 0; i < to_read; ++i) {
      const bool bit = bit_util::GetBit(data_, values_read_ + i);
      bit_util::SetBitTo(out.data(), out.bit_start() + i, bit);
    }
    AdvanceUnsafe(to_read);
    return to_read;
  }

  [[nodiscard]] rle_size_t GetBatchMisaligned(BitmapSpanMut out, rle_size_t batch_size) {
    const auto out_bit_offset = out.bit_start();
    ARROW_DCHECK_LT(out_bit_offset, 8);
    ARROW_DCHECK_GE(out_bit_offset, 0);

    if (ARROW_PREDICT_FALSE(remaining() == 0 || batch_size == 0)) {
      return 0;
    }

    const rle_size_t to_read = std::min(batch_size, remaining());
    rle_size_t read = 0;

    // HEADER: copy bits one by one unit the output is byte-aligned
    const rle_size_t to_read_until_aligned = (8 - out_bit_offset) % 8;
    const rle_size_t to_read_header = std::min(to_read, to_read_until_aligned);
    const auto read_header = GetBatchSlow(out, to_read_header);
    // We ensured there was enough capacity
    ARROW_DCHECK_EQ(read_header, to_read_header);
    read += read_header;
    out = out.NewStartingAt(read_header);
    // Either we are done or we have aligned the output values on a byte.
    ARROW_DCHECK(read == to_read || read == to_read_until_aligned);

    // Main loop, copy 32 bits at the time
    const rle_size_t n_batches = (to_read - read) / 32;
    for (rle_size_t i = 0; i < n_batches; ++i) {
      const auto bits = bit_util::Get32Bits(data_, values_read_ + 32 * i);
      std::memcpy(out.data(), &bits, sizeof(bits));
      out = out.NewStartingAt(8 * sizeof(bits));
    }
    const rle_size_t read_main = n_batches * 32;
    AdvanceUnsafe(read_main);
    read += read_main;

    // TRAILER: copy remaining bits one by one
    const auto read_trailer = GetBatchSlow(out, to_read - read);
    read += read_trailer;
    ARROW_DCHECK_EQ(read, to_read);

    return to_read;
  }
};

/// A specialized decoder class to extract RLE+bitpacked booleans.
///
/// In some cases, such as when reading definition levels for nullable values (with
/// no repetition and no nesting), we know values to be decoded will end up in an
/// Arrow validity bitmap. In such cases, decoding values to a ``int16`` before
/// encoding them again in overly wasteful.
class RleBitPackedToBitmapDecoder {
 public:
  RleBitPackedToBitmapDecoder() noexcept = default;

  /// Create a decoder object.
  ///
  /// data and data_size are the raw bytes to decode.
  RleBitPackedToBitmapDecoder(const uint8_t* data, rle_size_t data_size) noexcept {
    Reset(data, data_size);
  }

  void Reset(const uint8_t* data, rle_size_t data_size) noexcept {
    parser_.Reset(data, data_size, /* value_bit_width= */ 1);
    decoder_ = {};
  }

  /// Whether there is still runs to iterate over.
  bool exhausted() const { return (run_remaining() == 0) && parser_.exhausted(); }

  /// Get a batch of values return the number of decoded elements.
  /// May write fewer elements to the output than requested if there are not enough
  /// values left or if an error occurred.
  [[nodiscard]] rle_size_t GetBatch(BitmapSpanMut out, rle_size_t batch_size);

 private:
  RleBitPackedParser parser_ = {};
  std::variant<RleRunToBitmapDecoder, BitPackedRunToBitmapDecoder> decoder_ = {};

  /// Return the number of values that are remaining in the current run.
  rle_size_t run_remaining() const {
    return std::visit([](const auto& dec) { return dec.remaining(); }, decoder_);
  }

  /// Get a batch of values from the current run and return the number elements read.
  [[nodiscard]] rle_size_t RunGetBatch(BitmapSpanMut out, rle_size_t batch_size) {
    return std::visit([&](auto& dec) { return dec.GetBatch(out, batch_size); }, decoder_);
  }
};

/************************************************
 *  RleBitPackedToBitmapDecoder implementation  *
 ************************************************/

/// Utility to map a run type to the associate decoder.
template <typename Run>
struct RleBitPackedToBitmapDecoderGetDecoder;

template <>
struct RleBitPackedToBitmapDecoderGetDecoder<RleRun> {
  using type = RleRunToBitmapDecoder;
};

template <>
struct RleBitPackedToBitmapDecoderGetDecoder<BitPackedRun> {
  using type = BitPackedRunToBitmapDecoder;
};

inline auto RleBitPackedToBitmapDecoder::GetBatch(BitmapSpanMut out,
                                                  rle_size_t batch_size) -> rle_size_t {
  using ControlFlow = RleBitPackedParser::ControlFlow;

  rle_size_t values_read = 0;

  // Remaining from a previous call that would have left some unread data from a run.
  if (ARROW_PREDICT_FALSE(run_remaining() > 0)) {
    const auto read = RunGetBatch(out, batch_size);
    values_read += read;

    // Either we fulfilled all the batch to be read or we finished remaining run.
    if (ARROW_PREDICT_FALSE(values_read == batch_size)) {
      return values_read;
    }
    ARROW_DCHECK(run_remaining() == 0);
  }

  parser_.ParseWithCallable([&](auto run) {
    using RunDecoder = RleBitPackedToBitmapDecoderGetDecoder<decltype(run)>::type;

    ARROW_DCHECK_LT(values_read, batch_size);
    RunDecoder decoder(run);
    // The output span carries its own bit offset, so advancing it past the values
    // already written keeps successive runs correctly aligned in the bitmap.
    const auto read =
        decoder.GetBatch(out.NewStartingAt(values_read), batch_size - values_read);
    ARROW_DCHECK_LE(read, batch_size - values_read);
    values_read += read;

    // Stop reading and store remaining decoder
    if (ARROW_PREDICT_FALSE(values_read == batch_size || read == 0)) {
      decoder_ = std::move(decoder);
      return ControlFlow::Break;
    }

    return ControlFlow::Continue;
  });

  return values_read;
}

}  // namespace arrow::util
