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

// Imported from Apache Impala (incubating) on 2016-01-29 and modified for use
// in parquet-cpp, Arrow

#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <type_traits>
#include <variant>

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/macros.h"

namespace arrow::util {

/// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
/// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
/// (literal encoding).
/// For both types of runs, there is a byte-aligned indicator which encodes the length
/// of the run and the type of the run.
/// This encoding has the benefit that when there aren't any long enough runs, values
/// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
/// the run length are byte aligned. This allows for very efficient decoding
/// implementations.
/// The encoding is:
///    encoded-block := run*
///    run := literal-run | repeated-run
///    literal-run := literal-indicator < literal bytes >
///    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
///    literal-indicator := varint_encode( number_of_groups << 1 | 1)
///    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
/// Each run is preceded by a varint. The varint's least significant bit is
/// used to indicate whether the run is a literal run or a repeated run. The rest
/// of the varint is used to determine the length of the run (eg how many times the
/// value repeats).
//
/// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
/// in groups of 8), so that no matter the bit-width of the value, the sequence will end
/// on a byte boundary without padding.
/// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
/// the actual number of encoded ints. (This means that the total number of encoded values
/// cannot be determined from the encoded data, since the number of values in the last
/// group may not be a multiple of 8). For the last group of literal runs, we pad
/// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
/// without the need for additional checks.
//
/// There is a break-even point when it is more storage efficient to do run length
/// encoding.  For 1 bit-width values, that point is 8 values.  They require 2 bytes
/// for both the repeated encoding or the literal encoding.  This value can always
/// be computed based on the bit-width.
/// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
/// Examples with bit-width 1 (eg encoding booleans):
/// ----------------------------------------
/// 100 1s followed by 100 0s:
/// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1 byte>
///  - (total 4 bytes)
//
/// alternating 1s and 0s (200 total):
/// 200 ints = 25 groups of 8
/// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
/// (total 26 bytes, 1 byte overhead)
//

template <typename T>
class RleRunDecoder;

class RleRun {
 public:
  /// Enough space to store a 64bit value
  using raw_data_storage = std::array<uint8_t, 8>;
  using raw_data_const_pointer = const uint8_t*;
  using raw_data_size_type = int32_t;
  /// The type of the size of either run, between 1 and 2^31-1 as per Parquet spec
  using values_count_type = int32_t;
  /// The type to represent a size in bits
  using bit_size_type = int32_t;

  /// The decoder class used to decode a single run in the given type.
  template <typename T>
  using DecoderType = RleRunDecoder<T>;

  constexpr RleRun() noexcept = default;
  constexpr RleRun(RleRun const&) noexcept = default;
  constexpr RleRun(RleRun&&) noexcept = default;

  explicit RleRun(raw_data_const_pointer data, values_count_type values_count,
                  bit_size_type value_bit_width) noexcept;

  constexpr RleRun& operator=(RleRun const&) noexcept = default;
  constexpr RleRun& operator=(RleRun&&) noexcept = default;

  /// The number of repeated values in this run.
  [[nodiscard]] constexpr values_count_type ValuesCount() const noexcept;

  /// The size in bits of each encoded value.
  [[nodiscard]] constexpr bit_size_type ValuesBitWidth() const noexcept;

  /// A pointer to the repeated value raw bytes.
  [[nodiscard]] constexpr raw_data_const_pointer RawDataPtr() const noexcept;

  /// The number of bytes used for the raw repeated value.
  [[nodiscard]] constexpr raw_data_size_type RawDataSize() const noexcept;

 private:
  /// The repeated value raw bytes stored inside the class
  raw_data_storage data_ = {};
  /// The number of time the value is repeated
  values_count_type values_count_ = 0;
  /// The size in bit of a packed value in the run
  bit_size_type value_bit_width_ = 0;
};

template <typename T>
class BitPackedRunDecoder;

class BitPackedRun {
 public:
  using raw_data_const_pointer = const uint8_t*;
  /// According to the Parquet thrift definition the page size can be written into an
  /// int32_t.
  using raw_data_size_type = int32_t;
  /// The type of the size of either run, between 1 and 2^31-1 as per Parquet spec
  using values_count_type = int32_t;
  /// The type to represent a size in bits
  using bit_size_type = int32_t;

  /// The decoder class used to decode a single run in the given type.
  template <typename T>
  using DecoderType = BitPackedRunDecoder<T>;

  constexpr BitPackedRun() noexcept = default;
  constexpr BitPackedRun(BitPackedRun const&) noexcept = default;
  constexpr BitPackedRun(BitPackedRun&&) noexcept = default;

  constexpr BitPackedRun(raw_data_const_pointer data, values_count_type values_count,
                         bit_size_type value_bit_width) noexcept;

  constexpr BitPackedRun& operator=(BitPackedRun const&) noexcept = default;
  constexpr BitPackedRun& operator=(BitPackedRun&&) noexcept = default;

  [[nodiscard]] constexpr values_count_type ValuesCount() const noexcept;

  /// The size in bits of each encoded value.
  [[nodiscard]] constexpr bit_size_type ValuesBitWidth() const noexcept;

  [[nodiscard]] constexpr raw_data_const_pointer RawDataPtr() const noexcept;

  [[nodiscard]] constexpr raw_data_size_type RawDataSize() const noexcept;

 private:
  /// The pointer to the beginning of the run
  raw_data_const_pointer data_ = nullptr;
  /// Number of values in this run.
  raw_data_size_type values_count_ = 0;
  /// The size in bit of a packed value in the run
  bit_size_type value_bit_width_ = 0;
};

/// A parser that emits either a ``BitPackedRun`` or a ``RleRun``.
class RleBitPackedParser {
 public:
  using raw_data_const_pointer = const uint8_t*;
  /// By Parquet thrift definition the page size can be written into an int32_t.
  using raw_data_size_type = int32_t;
  /// The type to represent a size in bits
  using bit_size_type = int32_t;
  /// The different types of runs emitted by the parser
  using dynamic_run_type = std::variant<RleRun, BitPackedRun>;

  constexpr RleBitPackedParser() noexcept = default;

  constexpr RleBitPackedParser(raw_data_const_pointer data, raw_data_size_type data_size,
                               bit_size_type value_bit_width) noexcept;

  constexpr void Reset(raw_data_const_pointer data, raw_data_size_type data_size,
                       bit_size_type value_bit_width_) noexcept;

  /// Get the current run with a small parsing cost without advancing the iteration.
  [[nodiscard]] std::optional<dynamic_run_type> Peek() const;

  /// Move to the next run.
  [[nodiscard]] bool Advance();

  /// Advance and return the current run.
  [[nodiscard]] std::optional<dynamic_run_type> Next();

  /// Whether there is still runs to iterate over.
  ///
  /// WARN: Due to simplistic error handling, iteration with Next and Peek could
  /// fail to return data while the parser is not exhausted.
  /// This is how one can check for errors.
  [[nodiscard]] bool Exhausted() const;

  /// Enum to return from an ``Parse`` handler.
  ///
  /// Since a callback has no way to know when to stop, the handler must return
  /// a value indicating to the ``Parse`` function whether to stop or continue.
  enum class ControlFlow {
    Continue,
    Break,
  };

  /// A callback approach to parsing.
  ///
  /// This approach is used to reduce the number of dynamic lookups involved with using a
  /// variant.
  ///
  /// The handler must be of the form
  /// ```cpp
  /// struct Handler {
  ///   ControlFlow OnBitPackedRun(BitPackedRun run);
  ///
  ///   ControlFlow OnRleRun(RleRun run);
  /// };
  /// ```
  template <typename Handler>
  void Parse(Handler&& handler);

 private:
  /// The pointer to the beginning of the run
  raw_data_const_pointer data_ = nullptr;
  /// Size in bytes of the run.
  raw_data_size_type data_size_ = 0;
  /// The size in bit of a packed value in the run
  bit_size_type value_bit_width_ = 0;

  /// Run the handler on the run read and return the number of values read.
  /// Does not advance the parser.
  template <typename Handler>
  std::pair<raw_data_size_type, ControlFlow> PeekImpl(Handler&&) const;
};

/// Decoder class for a single run of RLE encoded data.
template <typename T>
class RleRunDecoder {
 public:
  /// The type in which the data should be decoded.
  using value_type = T;
  /// The type of run that can be decoded.
  using run_type = RleRun;
  using values_count_type = run_type::values_count_type;

  constexpr RleRunDecoder() noexcept = default;

  explicit RleRunDecoder(run_type const& run) noexcept;

  void Reset(run_type const& run) noexcept;

  /// Return the number of values that can be advanced.
  [[nodiscard]] values_count_type Remaining() const;

  /// Return the repeated value of this decoder.
  [[nodiscard]] constexpr value_type Value() const;

  /// Try to advance by as many values as provided.
  /// Return the number of values skipped.
  [[nodiscard]] values_count_type Advance(values_count_type batch_size);

  /// Get the next value and return false if there are no more.
  [[nodiscard]] constexpr bool Get(value_type* out_value);

  /// Get a batch of values return the number of decoded elements.
  [[nodiscard]] values_count_type GetBatch(value_type* out, values_count_type batch_size);

 private:
  value_type value_ = {};
  values_count_type remaining_count_ = 0;

  static_assert(std::is_integral_v<value_type>,
                "This class makes assumptions about integer endianness and padding");
};

/// Decoder class for single run of bit-packed encoded data.
template <typename T>
class BitPackedRunDecoder {
 public:
  /// The type in which the data should be decoded.
  using value_type = T;
  /// The type of run that can be decoded.
  using run_type = BitPackedRun;
  using values_count_type = run_type::values_count_type;
  using bit_size_type = run_type::bit_size_type;

  BitPackedRunDecoder() noexcept = default;

  explicit BitPackedRunDecoder(run_type const& run) noexcept;

  void Reset(run_type const& run) noexcept;

  /// Return the number of values that can be advanced.
  [[nodiscard]] constexpr values_count_type Remaining() const;

  /// Return the size in bit in which each encoded value is written.
  [[nodiscard]] constexpr bit_size_type ValueBitWidth() const;

  /// Try to advance by as many values as provided.
  /// Return the number of values skipped.
  [[nodiscard]] values_count_type Advance(values_count_type batch_size);

  /// Get the next value and return false if there are no more.
  [[nodiscard]] bool Get(value_type* out_value);

  /// Get a batch of values return the number of decoded elements.
  [[nodiscard]] values_count_type GetBatch(value_type* out, values_count_type batch_size);

 private:
  ::arrow::bit_util::BitReader bit_reader_ = {};
  bit_size_type value_bit_width_ = 0;
  values_count_type remaining_count_ = 0;

  static_assert(std::is_integral_v<value_type>,
                "This class makes assumptions about integer endianness and padding");
};

/// Decoder class for Parquet RLE bit-packed data.
template <typename T>
class RleBitPackedDecoder {
 public:
  /// The type in which the data should be decoded.
  using value_type = T;
  using raw_data_const_pointer = RleBitPackedParser::raw_data_const_pointer;
  using raw_data_size_type = RleBitPackedParser::raw_data_size_type;
  using bit_size_type = RleBitPackedParser::bit_size_type;
  using dynamic_run_type = RleBitPackedParser::dynamic_run_type;
  /// The type of the size of either run, between 1 and 2^31-1 as per Parquet spec
  using values_count_type = int32_t;

  RleBitPackedDecoder() noexcept = default;

  /// Create a decoder object.
  ///
  /// data and data_size are the raw bytes to decode.
  /// value_bit_width is the size in bits of each encoded value.
  RleBitPackedDecoder(raw_data_const_pointer data, raw_data_size_type data_size,
                      bit_size_type value_bit_width) noexcept;

  void Reset(raw_data_const_pointer data, raw_data_size_type data_size,
             bit_size_type value_bit_width_) noexcept;

  /// Whether there is still runs to iterate over.
  ///
  /// WARN: Due to lack of proper error handling, iteration with Get methods could return
  /// no data while the parser is not exhausted.
  /// This is how one can check for errors.
  [[nodiscard]] bool Exhausted() const;

  /// Gets the next value.  Returns false if there are no more.
  ///
  /// NB: Because the encoding only supports literal runs with lengths
  /// that are multiples of 8, RleEncoder sometimes pads the end of its
  /// input with zeros. Since the encoding does not differentiate between
  /// input values and padding, Get() returns true even for these padding
  /// values.
  [[nodiscard]] bool Get(value_type* val);

  /// Get a batch of values return the number of decoded elements.
  [[nodiscard]] values_count_type GetBatch(value_type* out, values_count_type batch_size);

  /// Like GetBatch but add spacing for null entries
  [[nodiscard]] values_count_type GetBatchSpaced(values_count_type batch_size,
                                                 values_count_type null_count,
                                                 const uint8_t* valid_bits,
                                                 int64_t valid_bits_offset,
                                                 value_type* out);

  /// Like GetBatch but the values are then decoded using the provided dictionary
  template <typename V>
  [[nodiscard]] values_count_type GetBatchWithDict(const V* dictionary,
                                                   int32_t dictionary_length, V* out,
                                                   values_count_type batch_size);

  /// Like GetBatchWithDict but add spacing for null entries
  ///
  /// Null entries will be zero-initialized in `values` to avoid leaking
  /// private data.
  template <typename V>
  [[nodiscard]] values_count_type GetBatchWithDictSpaced(
      const V* dictionary, int32_t dictionary_length, V* out,
      values_count_type batch_size, values_count_type null_count,
      const uint8_t* valid_bits, int64_t valid_bits_offset);

 private:
  RleBitPackedParser parser_ = {};
  std::variant<RleRunDecoder<value_type>, BitPackedRunDecoder<value_type>> decoder_ = {};

  /// Return the number of values that are remaining in the current run.
  [[nodiscard]] values_count_type RunRemaining() const;

  /// Get a batch of values from the current run and return the number elements read.
  [[nodiscard]] values_count_type RunGetBatch(value_type* out,
                                              values_count_type batch_size);

  /// Call the parser with a single callable for all event types.
  template <typename Callable>
  void ParseWithCallable(Callable&& func);

  /// Utility methods for retrieving spaced values.
  template <typename Converter>
  [[nodiscard]] values_count_type GetSpaced(Converter converter,
                                            typename Converter::out_type* out,
                                            values_count_type batch_size,
                                            const uint8_t* valid_bits,
                                            int64_t valid_bits_offset,
                                            values_count_type null_count);
};

/// Class to incrementally build the rle data.   This class does not allocate any memory.
/// The encoding has two modes: encoding repeated runs and literal runs.
/// If the run is sufficiently short, it is more efficient to encode as a literal run.
/// This class does so by buffering 8 values at a time.  If they are not all the same
/// they are added to the literal run.  If they are the same, they are added to the
/// repeated run.  When we switch modes, the previous run is flushed out.
class RleBitPackedEncoder {
 public:
  /// buffer/buffer_len: preallocated output buffer.
  /// bit_width: max number of bits for value.
  /// TODO: consider adding a min_repeated_run_length so the caller can control
  /// when values should be encoded as repeated runs.  Currently this is derived
  /// based on the bit_width, which can determine a storage optimal choice.
  /// TODO: allow 0 bit_width (and have dict encoder use it)
  RleBitPackedEncoder(uint8_t* buffer, int buffer_len, int bit_width)
      : bit_width_(bit_width), bit_writer_(buffer, buffer_len) {
    ARROW_DCHECK_GE(bit_width_, 0);
    ARROW_DCHECK_LE(bit_width_, 64);
    max_run_byte_size_ = MinBufferSize(bit_width);
    ARROW_DCHECK_GE(buffer_len, max_run_byte_size_) << "Input buffer not big enough.";
    Clear();
  }

  /// Returns the minimum buffer size needed to use the encoder for 'bit_width'
  /// This is the maximum length of a single run for 'bit_width'.
  /// It is not valid to pass a buffer less than this length.
  static int MinBufferSize(int bit_width) {
    /// 1 indicator byte and MAX_VALUES_PER_LITERAL_RUN 'bit_width' values.
    int max_literal_run_size = 1 + static_cast<int>(::arrow::bit_util::BytesForBits(
                                       MAX_VALUES_PER_LITERAL_RUN * bit_width));
    /// Up to kMaxVlqByteLength indicator and a single 'bit_width' value.
    int max_repeated_run_size =
        ::arrow::bit_util::BitReader::kMaxVlqByteLengthForInt32 +
        static_cast<int>(::arrow::bit_util::BytesForBits(bit_width));
    return std::max(max_literal_run_size, max_repeated_run_size);
  }

  /// Returns the maximum byte size it could take to encode 'num_values'.
  static int MaxBufferSize(int bit_width, int num_values) {
    // For a bit_width > 1, the worst case is the repetition of "literal run of length 8
    // and then a repeated run of length 8".
    // 8 values per smallest run, 8 bits per byte
    int bytes_per_run = bit_width;
    int num_runs = static_cast<int>(::arrow::bit_util::CeilDiv(num_values, 8));
    int literal_max_size = num_runs + num_runs * bytes_per_run;

    // In the very worst case scenario, the data is a concatenation of repeated
    // runs of 8 values. Repeated run has a 1 byte varint followed by the
    // bit-packed repeated value
    int min_repeated_run_size =
        1 + static_cast<int>(::arrow::bit_util::BytesForBits(bit_width));
    int repeated_max_size = num_runs * min_repeated_run_size;

    return std::max(literal_max_size, repeated_max_size);
  }

  /// Encode value.  Returns true if the value fits in buffer, false otherwise.
  /// This value must be representable with bit_width_ bits.
  bool Put(uint64_t value);

  /// Flushes any pending values to the underlying buffer.
  /// Returns the total number of bytes written
  int Flush();

  /// Resets all the state in the encoder.
  void Clear();

  /// Returns pointer to underlying buffer
  uint8_t* buffer() { return bit_writer_.buffer(); }
  int32_t len() { return bit_writer_.bytes_written(); }

 private:
  /// Flushes any buffered values.  If this is part of a repeated run, this is largely
  /// a no-op.
  /// If it is part of a literal run, this will call FlushLiteralRun, which writes
  /// out the buffered literal values.
  /// If 'done' is true, the current run would be written even if it would normally
  /// have been buffered more.  This should only be called at the end, when the
  /// encoder has received all values even if it would normally continue to be
  /// buffered.
  void FlushBufferedValues(bool done);

  /// Flushes literal values to the underlying buffer.  If update_indicator_byte,
  /// then the current literal run is complete and the indicator byte is updated.
  void FlushLiteralRun(bool update_indicator_byte);

  /// Flushes a repeated run to the underlying buffer.
  void FlushRepeatedRun();

  /// Checks and sets buffer_full_. This must be called after flushing a run to
  /// make sure there are enough bytes remaining to encode the next run.
  void CheckBufferFull();

  /// The maximum number of values in a single literal run
  /// (number of groups encodable by a 1-byte indicator * 8)
  static const int MAX_VALUES_PER_LITERAL_RUN = (1 << 6) * 8;

  /// Number of bits needed to encode the value. Must be between 0 and 64.
  const int bit_width_;

  /// Underlying buffer.
  ::arrow::bit_util::BitWriter bit_writer_;

  /// If true, the buffer is full and subsequent Put()'s will fail.
  bool buffer_full_;

  /// The maximum byte size a single run can take.
  int max_run_byte_size_;

  /// We need to buffer at most 8 values for literals.  This happens when the
  /// bit_width is 1 (so 8 values fit in one byte).
  /// TODO: generalize this to other bit widths
  int64_t buffered_values_[8];

  /// Number of values in buffered_values_
  int num_buffered_values_;

  /// The current (also last) value that was written and the count of how
  /// many times in a row that value has been seen.  This is maintained even
  /// if we are in a literal run.  If the repeat_count_ get high enough, we switch
  /// to encoding repeated runs.
  uint64_t current_value_;
  int repeat_count_;

  /// Number of literals in the current run.  This does not include the literals
  /// that might be in buffered_values_.  Only after we've got a group big enough
  /// can we decide if they should part of the literal_count_ or repeat_count_
  int literal_count_;

  /// Pointer to a byte in the underlying buffer that stores the indicator byte.
  /// This is reserved as soon as we need a literal run but the value is written
  /// when the literal run is complete.
  uint8_t* literal_indicator_byte_;
};

/*************************
 *  RleBitPackedDecoder  *
 *************************/

template <typename T>
RleBitPackedDecoder<T>::RleBitPackedDecoder(raw_data_const_pointer data,
                                            raw_data_size_type data_size,
                                            bit_size_type value_bit_width) noexcept {
  Reset(data, data_size, value_bit_width);
}

template <typename T>
void RleBitPackedDecoder<T>::Reset(raw_data_const_pointer data,
                                   raw_data_size_type data_size,
                                   bit_size_type value_bit_width) noexcept {
  ARROW_DCHECK_GE(value_bit_width, 0);
  ARROW_DCHECK_LE(value_bit_width, 64);
  parser_.Reset(data, data_size, value_bit_width);
  decoder_ = {};
}

template <typename T>
auto RleBitPackedDecoder<T>::RunRemaining() const -> values_count_type {
  return std::visit([](auto const& dec) { return dec.Remaining(); }, decoder_);
}

template <typename T>
bool RleBitPackedDecoder<T>::Exhausted() const {
  return (RunRemaining() == 0) && parser_.Exhausted();
}

template <typename T>
template <typename Callable>
void RleBitPackedDecoder<T>::ParseWithCallable(Callable&& func) {
  struct {
    Callable func;
    auto OnBitPackedRun(BitPackedRun run) { return func(std::move(run)); }
    auto OnRleRun(RleRun run) { return func(std::move(run)); }
  } handler{std::move(func)};

  parser_.Parse(std::move(handler));
}

template <typename T>
auto RleBitPackedDecoder<T>::RunGetBatch(value_type* out, values_count_type batch_size)
    -> values_count_type {
  return std::visit([&](auto& dec) { return dec.GetBatch(out, batch_size); }, decoder_);
}

template <typename T>
bool RleBitPackedDecoder<T>::Get(value_type* val) {
  return GetBatch(val, 1) == 1;
}

namespace internal {

/// A ``Parse`` handler that calls a single lambda.
///
/// This lambda would typically take the input run as ``auto run`` (i.e. the lambda is
/// templated) and deduce other types from it.
template <typename Lambda>
struct LambdaHandler {
  Lambda handler_;

  auto OnBitPackedRun(BitPackedRun run) { return handler_(std::move(run)); }

  auto OnRleRun(RleRun run) { return handler_(std::move(run)); }
};

template <typename Lambda>
LambdaHandler(Lambda) -> LambdaHandler<Lambda>;

}  // namespace internal

template <typename T>
auto RleBitPackedDecoder<T>::GetBatch(value_type* out, values_count_type batch_size)
    -> values_count_type {
  using ControlFlow = RleBitPackedParser::ControlFlow;

  values_count_type values_read = 0;

  // Remaining from a previous call that would have left some unread data from a run.
  if (ARROW_PREDICT_FALSE(RunRemaining() > 0)) {
    auto const read = RunGetBatch(out, batch_size);
    values_read += read;
    out += read;

    // Either we fulfilled all the batch to be read or we finished remaining run.
    if (ARROW_PREDICT_FALSE(values_read == batch_size)) {
      return values_read;
    }
    ARROW_DCHECK(RunRemaining() == 0);
  }

  ParseWithCallable([&](auto run) {
    using RunDecoder = typename decltype(run)::template DecoderType<value_type>;

    ARROW_DCHECK_LT(values_read, batch_size);
    RunDecoder decoder(run);
    auto const read = decoder.GetBatch(out, batch_size - values_read);
    ARROW_DCHECK_LE(read, batch_size - values_read);
    values_read += read;
    out += read;

    // Stop reading and store remaining decoder
    if (ARROW_PREDICT_FALSE(values_read == batch_size || read == 0)) {
      decoder_ = std::move(decoder);
      return ControlFlow::Break;
    }

    return ControlFlow::Continue;
  });

  return values_read;
}

namespace internal {

/// Utility class to safely handle values and null count without too error-prone
/// verbosity.
class BatchCounter {
 public:
  using size_type = int32_t;

  [[nodiscard]] static constexpr BatchCounter FromBatchSizeAndNulls(
      size_type batch_size, size_type null_count) {
    ARROW_DCHECK_LE(null_count, batch_size);
    return {batch_size - null_count, null_count};
  }

  constexpr BatchCounter(size_type values_count, size_type null_count) noexcept
      : values_count_(values_count), null_count_(null_count) {}

  [[nodiscard]] constexpr size_type ValuesCount() const noexcept { return values_count_; }

  [[nodiscard]] constexpr size_type ValuesRead() const noexcept { return values_read_; }

  [[nodiscard]] constexpr size_type ValuesRemaining() const noexcept {
    ARROW_DCHECK_LE(values_read_, values_count_);
    return values_count_ - values_read_;
  }

  constexpr void AccrueReadValues(size_type to_read) noexcept {
    ARROW_DCHECK_LE(to_read, ValuesRemaining());
    values_read_ += to_read;
  }

  [[nodiscard]] constexpr size_type NullCount() const noexcept { return null_count_; }

  [[nodiscard]] constexpr size_type NullRead() const noexcept { return null_read_; }

  [[nodiscard]] constexpr size_type NullRemaining() const noexcept {
    ARROW_DCHECK_LE(null_read_, null_count_);
    return null_count_ - null_read_;
  }

  constexpr void AccrueReadNulls(size_type to_read) noexcept {
    ARROW_DCHECK_LE(to_read, NullRemaining());
    null_read_ += to_read;
  }

  [[nodiscard]] constexpr size_type TotalRemaining() const noexcept {
    return ValuesRemaining() + NullRemaining();
  }

  [[nodiscard]] constexpr size_type TotalRead() const noexcept {
    return values_read_ + null_read_;
  }

  [[nodiscard]] constexpr bool IsFullyNull() const noexcept {
    return ValuesRemaining() == 0;
  }

  [[nodiscard]] constexpr bool IsDone() const noexcept { return TotalRemaining() == 0; }

 private:
  size_type values_count_ = 0;
  size_type values_read_ = 0;
  size_type null_count_ = 0;
  size_type null_read_ = 0;
};

// The maximal unsigned size that a variable can fit.
template <typename T>
constexpr auto max_size_for_v =
    static_cast<std::make_unsigned_t<T>>(std::numeric_limits<T>::max());

/// Overload for GetSpaced for a single run in a RleDecoder
template <typename Converter, typename BitRunReader, typename BitRun,
          typename values_count_type, typename value_type>
auto RunGetSpaced(Converter& converter, typename Converter::out_type* out,
                  values_count_type batch_size, values_count_type null_count,
                  BitRunReader&& validity_reader, BitRun&& validity_run,
                  RleRunDecoder<value_type>& decoder)
    -> std::pair<values_count_type, values_count_type> {
  ARROW_DCHECK_GT(batch_size, 0);
  // The equality case is handled in the main loop in GetSpaced
  ARROW_DCHECK_LT(null_count, batch_size);

  auto batch = BatchCounter::FromBatchSizeAndNulls(batch_size, null_count);

  values_count_type const values_available = decoder.Remaining();
  ARROW_DCHECK_GT(values_available, 0);
  auto values_remaining_run = [&]() {
    auto out = values_available - batch.ValuesRead();
    ARROW_DCHECK_GE(out, 0);
    return out;
  };

  // Consume as much as possible from the repeated run.
  // We only need to count the number of nulls and non-nulls because we can fill in the
  // same value for nulls and non-nulls.
  // This proves to be a big efficiency win.
  while (values_remaining_run() > 0 && !batch.IsDone()) {
    ARROW_DCHECK_GE(validity_run.length, 0);
    ARROW_DCHECK_LT(validity_run.length, max_size_for_v<values_count_type>);
    ARROW_DCHECK_LE(validity_run.length, batch.TotalRemaining());
    auto const& validity_run_size = static_cast<values_count_type>(validity_run.length);

    if (validity_run.set) {
      // We may end the current RLE run in the middle of the validity run
      auto update_size = std::min(validity_run_size, values_remaining_run());
      batch.AccrueReadValues(update_size);
      validity_run.length -= update_size;
    } else {
      // We can consume all nulls here because it does not matter if we consume on this
      // RLE run, or an a next encoded run. The value filled does not matter.
      auto update_size = std::min(validity_run_size, batch.NullRemaining());
      batch.AccrueReadNulls(update_size);
      validity_run.length -= update_size;
    }

    if (ARROW_PREDICT_TRUE(validity_run.length == 0)) {
      validity_run = validity_reader.NextRun();
    }
  }

  value_type const value = decoder.Value();
  if (ARROW_PREDICT_FALSE(!converter.InputIsValid(value))) {
    return {0, 0};
  }
  converter.WriteRepeated(out, out + batch.TotalRead(), value);
  auto const actual_values_read = decoder.Advance(batch.ValuesRead());
  // We always cropped the number of values_read by the remaining values in the run.
  // What's more the RLE decoder should not encounter any errors.
  ARROW_DCHECK_EQ(actual_values_read, batch.ValuesRead());

  return {batch.ValuesRead(), batch.NullRead()};
}

template <typename T, typename... Ts>
[[nodiscard]] constexpr T min(T x, Ts... ys) {
  ((x = std::min(x, ys)), ...);
  return x;
}

static_assert(min(5) == 5);
static_assert(min(5, 4, -1) == -1);
static_assert(min(5, 41) == 5);

template <typename Converter, typename BitRunReader, typename BitRun,
          typename values_count_type, typename value_type>
auto RunGetSpaced(Converter& converter, typename Converter::out_type* out,
                  values_count_type batch_size, values_count_type null_count,
                  BitRunReader&& validity_reader, BitRun&& validity_run,
                  BitPackedRunDecoder<value_type>& decoder)
    -> std::pair<values_count_type, values_count_type> {
  ARROW_DCHECK_GT(batch_size, 0);
  // The equality case is handled in the main loop in GetSpaced
  ARROW_DCHECK_LT(null_count, batch_size);

  auto batch = BatchCounter::FromBatchSizeAndNulls(batch_size, null_count);

  values_count_type const values_available = decoder.Remaining();
  ARROW_DCHECK_GT(values_available, 0);
  auto run_values_remaining = [&]() {
    auto out = values_available - batch.ValuesRead();
    ARROW_DCHECK_GE(out, 0);
    return out;
  };

  while (run_values_remaining() > 0 && batch.ValuesRemaining() > 0) {
    // TODO should this size be tune depending on sizeof(value_size)? cpu cache size?
    // Pull a batch of values from the bit packed encoded data and store it in a local
    // buffer to benefit from unpacking intrinsics and data locality.
    static constexpr values_count_type kBufferCapacity = 1024;
    std::array<value_type, kBufferCapacity> buffer = {};

    values_count_type buffer_start = 0;
    values_count_type buffer_end = 0;
    auto buffer_size = [&]() {
      auto out = buffer_end - buffer_start;
      ARROW_DCHECK_GE(out, 0);
      return out;
    };

    // buffer_start is 0 at this point so size is end
    buffer_end = min(run_values_remaining(), batch.ValuesRemaining(), kBufferCapacity);
    buffer_end = decoder.GetBatch(buffer.data(), buffer_size());
    ARROW_DCHECK_LE(buffer_size(), kBufferCapacity);

    if (ARROW_PREDICT_FALSE(!converter.InputIsValid(buffer.data(), buffer_size()))) {
      return {batch.ValuesRead(), batch.NullRead()};
    }

    // Copy chunks of valid values into the output, while adjusting spacing for null
    // values.
    while (buffer_size() > 0) {
      ARROW_DCHECK_GE(validity_run.length, 0);
      ARROW_DCHECK_LT(validity_run.length, max_size_for_v<values_count_type>);
      ARROW_DCHECK_LE(validity_run.length, batch.TotalRemaining());
      auto const validity_run_length =
          static_cast<values_count_type>(validity_run.length);

      // Copy as much as possible from the buffer into the output while not exceeding
      // validity run
      if (validity_run.set) {
        auto const update_size = std::min(validity_run_length, buffer_size());
        converter.WriteRange(out, buffer.data() + buffer_start, update_size);
        buffer_start += update_size;
        batch.AccrueReadValues(update_size);
        out += update_size;
        validity_run.length -= update_size;
        // Simply write zeros in the output
      } else {
        auto const update_size = std::min(validity_run_length, batch.NullRemaining());
        converter.WriteZero(out, out + update_size);
        batch.AccrueReadNulls(update_size);
        out += update_size;
        validity_run.length -= update_size;
      }

      if (validity_run.length == 0) {
        validity_run = validity_reader.NextRun();
      }
    }

    ARROW_DCHECK_EQ(buffer_size(), 0);
  }

  ARROW_DCHECK_EQ(values_available - decoder.Remaining(), batch.ValuesRead());
  ARROW_DCHECK_LE(batch.TotalRead(), batch_size);
  ARROW_DCHECK_LE(batch.NullRead(), batch.NullCount());

  return {batch.ValuesRead(), batch.NullRead()};
}

/// Overload for GetSpaced for a single run in a decoder variant
template <typename Converter, typename BitRunReader, typename BitRun,
          typename values_count_type, typename value_type>
auto RunGetSpaced(
    Converter& converter, typename Converter::out_type* out, values_count_type batch_size,
    values_count_type null_count, BitRunReader&& validity_reader, BitRun&& validity_run,
    std::variant<RleRunDecoder<value_type>, BitPackedRunDecoder<value_type>>& decoder)
    -> std::pair<values_count_type, values_count_type> {
  return std::visit(
      [&](auto& dec) {
        ARROW_DCHECK_GT(dec.Remaining(), 0);
        return RunGetSpaced(converter, out, batch_size, null_count, validity_reader,
                            validity_run, dec);
      },
      decoder);
}

}  // namespace internal

template <typename T>
template <typename Converter>
auto RleBitPackedDecoder<T>::GetSpaced(Converter converter,
                                       typename Converter::out_type* out,
                                       values_count_type batch_size,
                                       const uint8_t* validity_bits,
                                       int64_t validity_bits_offset,
                                       values_count_type null_count)
    -> values_count_type {
  using ControlFlow = RleBitPackedParser::ControlFlow;

  ARROW_DCHECK_GT(batch_size, 0);

  auto batch = internal::BatchCounter::FromBatchSizeAndNulls(batch_size, null_count);

  if (ARROW_PREDICT_FALSE(batch.IsFullyNull())) {
    converter.WriteZero(out, out + batch.NullRemaining());
    return batch.NullRemaining();
  }

  arrow::internal::BitRunReader validity_reader(validity_bits, validity_bits_offset,
                                                /*length=*/batch.TotalRemaining());
  arrow::internal::BitRun validity_run = validity_reader.NextRun();

  auto const check_and_handle_fully_null_remaining = [&]() {
    if (batch.IsFullyNull()) {
      ARROW_DCHECK(validity_run.length == 0 || !validity_run.set);
      ARROW_DCHECK_GE(validity_run.length, batch.NullRemaining());

      converter.WriteZero(out, out + batch.NullRemaining());
      out += batch.NullRemaining();
      batch.AccrueReadNulls(batch.NullRemaining());
    }
  };

  // Remaining from a previous call that would have left some unread data from a run.
  if (ARROW_PREDICT_FALSE(RunRemaining() > 0)) {
    auto const [values_read, null_read] =
        RunGetSpaced(converter, out, batch.TotalRemaining(), batch.NullRemaining(),
                     validity_reader, validity_run, decoder_);

    batch.AccrueReadNulls(null_read);
    batch.AccrueReadValues(values_read);
    out += values_read + null_read;

    // Either we fulfilled all the batch values to be read
    if (ARROW_PREDICT_FALSE(batch.ValuesRemaining() == 0)) {
      // There may be remaining null if they are not greedily filled
      check_and_handle_fully_null_remaining();
      return batch.TotalRead();
    }

    /// We finished the remaining run
    ARROW_DCHECK(RunRemaining() == 0);
  }

  ParseWithCallable([&](auto run) {
    using RunDecoder = typename decltype(run)::template DecoderType<value_type>;

    RunDecoder decoder(run);

    const auto [values_read, null_read] = internal::RunGetSpaced(
        converter, out, batch.TotalRemaining(), batch.NullRemaining(), validity_reader,
        validity_run, decoder);

    batch.AccrueReadNulls(null_read);
    batch.AccrueReadValues(values_read);
    out += values_read + null_read;

    // Stop reading and store remaining decoder
    if (ARROW_PREDICT_FALSE(values_read == 0 || batch.ValuesRemaining() == 0)) {
      decoder_ = std::move(decoder);
      return ControlFlow::Break;
    }

    return ControlFlow::Continue;
  });

  // There may be remaining null if they are not greedily filled by either decoder calls
  check_and_handle_fully_null_remaining();

  ARROW_DCHECK(batch.IsDone() || Exhausted());
  // batch.Done() => batch.NullRemaining() == 0
  ARROW_DCHECK(!batch.IsDone() || (batch.NullRemaining() == 0));
  return batch.TotalRead();
}

namespace internal {

// Converter for GetSpaced that handles runs that get returned
// directly as output.
template <typename T>
struct NoOpConverter {
  using in_type = T;
  using out_type = T;
  using size_type = int32_t;

  static constexpr bool kIsIdentity = true;

  [[nodiscard]] static constexpr bool InputIsValid(const in_type& values) { return true; }

  [[nodiscard]] static constexpr bool InputIsValid(const in_type* values,
                                                   size_type length) {
    return true;
  }

  static void WriteRepeated(out_type* begin, out_type* end, in_type run_value) {
    std::fill(begin, end, run_value);
  }

  static void WriteZero(out_type* begin, out_type* end) {
    std::fill(begin, end, out_type{});
  }

  static void WriteRange(out_type* out, const in_type* values, size_type length) {
    std::memcpy(out, values, length * sizeof(out_type));
  }
};

}  // namespace internal

template <typename T>
auto RleBitPackedDecoder<T>::GetBatchSpaced(values_count_type batch_size,
                                            values_count_type null_count,
                                            const uint8_t* valid_bits,
                                            int64_t valid_bits_offset, value_type* out)
    -> values_count_type {
  if (null_count == 0) {
    return GetBatch(out, batch_size);
  }

  internal::NoOpConverter<value_type> converter;

  return GetSpaced(converter, out, batch_size, valid_bits, valid_bits_offset, null_count);
}

namespace internal {

template <typename I>
bool IndexInRange(I idx, int32_t dictionary_length) {
  ARROW_DCHECK_GT(dictionary_length, 0);
  using T = std::common_type_t<decltype(idx), decltype(dictionary_length)>;
  return idx >= 0 && static_cast<T>(idx) < static_cast<T>(dictionary_length);
}

// Converter for GetSpaced that handles runs of returned dictionary
// indices.
template <typename V, typename I>
struct DictionaryConverter {
  using out_type = V;
  using in_type = I;
  using size_type = int32_t;

  static constexpr bool kIsIdentity = false;

  const out_type* dictionary;
  size_type dictionary_length;

  [[nodiscard]] bool InputIsValid(in_type idx) const {
    return IndexInRange(idx, dictionary_length);
  }

  [[nodiscard]] bool InputIsValid(const in_type* indices, size_type length) const {
    in_type min_index = std::numeric_limits<in_type>::max();
    in_type max_index = std::numeric_limits<in_type>::min();
    for (size_type x = 0; x < length; x++) {
      min_index = std::min(indices[x], min_index);
      max_index = std::max(indices[x], max_index);
    }

    return IndexInRange(min_index, dictionary_length) &&
           IndexInRange(max_index, dictionary_length);
  }

  void WriteRepeated(out_type* begin, out_type* end, in_type run_value) const {
    std::fill(begin, end, dictionary[run_value]);
  }

  static void WriteZero(out_type* begin, out_type* end) {
    std::fill(begin, end, out_type{});
  }

  void WriteRange(out_type* out, const in_type* values, size_type length) const {
    for (size_type x = 0; x < length; x++) {
      out[x] = dictionary[values[x]];
    }
  }
};

/// Dummy imitation of BitRun that is all set.
struct AllSetBitRun {
  static constexpr bool set = true;
  int64_t length = 0;
};

/// Dummy imitation of BitRunReader that should never be called.
struct UnreachableBitRunReader {
  constexpr static AllSetBitRun NextRun() { return {}; }
};

}  // namespace internal

template <typename T>
template <typename V>
auto RleBitPackedDecoder<T>::GetBatchWithDict(const V* dictionary,
                                              int32_t dictionary_length, V* out,
                                              values_count_type batch_size)
    -> values_count_type {
  using ControlFlow = RleBitPackedParser::ControlFlow;

  if (ARROW_PREDICT_FALSE(batch_size <= 0)) {
    return 0;
  }

  internal::DictionaryConverter<V, value_type> converter{dictionary, dictionary_length};

  // Make lightweight BitRun class to reuse previous methods.
  constexpr internal::UnreachableBitRunReader validity_reader{};
  internal::AllSetBitRun validity_run = {batch_size};

  values_count_type values_read = 0;
  auto batch_values_remaining = [&]() {
    ARROW_DCHECK_LE(values_read, batch_size);
    return batch_size - values_read;
  };

  if (ARROW_PREDICT_FALSE(RunRemaining() > 0)) {
    auto const [run_values_read, run_null_read] =
        RunGetSpaced(converter, out, batch_size, /* null_count= */ 0, validity_reader,
                     validity_run, decoder_);

    ARROW_DCHECK_EQ(run_null_read, 0);
    values_read += run_values_read;
    out += run_values_read;

    // Either we fulfilled all the batch values to be read
    if (ARROW_PREDICT_FALSE(values_read >= batch_size)) {
      // There may be remaining null if they are not greedily filled
      return values_read;
    }

    /// We finished the remaining run
    ARROW_DCHECK(RunRemaining() == 0);
  }

  ParseWithCallable([&](auto run) {
    using RunDecoder = typename decltype(run)::template DecoderType<value_type>;

    RunDecoder decoder(run);

    auto const [run_values_read, run_null_read] = internal::RunGetSpaced(
        converter, out, batch_values_remaining(), /* null_count= */ 0, validity_reader,
        validity_run, decoder);

    ARROW_DCHECK_EQ(run_null_read, 0);
    values_read += run_values_read;
    out += run_values_read;

    // Stop reading and store remaining decoder
    if (ARROW_PREDICT_FALSE(run_values_read == 0 || values_read == batch_size)) {
      decoder_ = std::move(decoder);
      return ControlFlow::Break;
    }

    return ControlFlow::Continue;
  });

  return values_read;
}

template <typename T>
template <typename V>
auto RleBitPackedDecoder<T>::GetBatchWithDictSpaced(
    const V* dictionary, int32_t dictionary_length, V* out, values_count_type batch_size,
    values_count_type null_count, const uint8_t* valid_bits, int64_t valid_bits_offset)
    -> values_count_type {
  if (null_count == 0) {
    return GetBatchWithDict<V>(dictionary, dictionary_length, out, batch_size);
  }
  internal::DictionaryConverter<V, value_type> converter{dictionary, dictionary_length};

  return GetSpaced(converter, out, batch_size, valid_bits, valid_bits_offset, null_count);
}

/************
 *  RleRun  *
 ************/

inline RleRun::RleRun(raw_data_const_pointer data, values_count_type values_count,
                      bit_size_type value_bit_width) noexcept
    : values_count_(values_count), value_bit_width_(value_bit_width) {
  ARROW_DCHECK_GE(value_bit_width, 0);
  ARROW_DCHECK_GE(values_count, 0);
  std::copy(data, data + RawDataSize(), data_.begin());
}

constexpr auto RleRun::ValuesCount() const noexcept -> values_count_type {
  return values_count_;
}

constexpr auto RleRun::ValuesBitWidth() const noexcept -> bit_size_type {
  return value_bit_width_;
}

constexpr auto RleRun::RawDataPtr() const noexcept -> raw_data_const_pointer {
  return data_.data();
}

constexpr auto RleRun::RawDataSize() const noexcept -> raw_data_size_type {
  auto out = bit_util::BytesForBits(value_bit_width_);
  ARROW_DCHECK_LE(out, std::numeric_limits<raw_data_size_type>::max());
  return static_cast<raw_data_size_type>(out);
};

/******************
 *  BitPackedRun  *
 ******************/

constexpr BitPackedRun::BitPackedRun(raw_data_const_pointer data,
                                     values_count_type values_count,
                                     bit_size_type value_bit_width) noexcept
    : data_(data), values_count_(values_count), value_bit_width_(value_bit_width) {
  ARROW_CHECK_GE(value_bit_width_, 0);
  ARROW_CHECK_GE(values_count_, 0);
}

constexpr auto BitPackedRun::ValuesCount() const noexcept -> values_count_type {
  return values_count_;
}

constexpr auto BitPackedRun::ValuesBitWidth() const noexcept -> bit_size_type {
  return value_bit_width_;
}

constexpr auto BitPackedRun::RawDataPtr() const noexcept -> raw_data_const_pointer {
  return data_;
}

constexpr auto BitPackedRun::RawDataSize() const noexcept -> raw_data_size_type {
  auto out = bit_util::BytesForBits(static_cast<int64_t>(value_bit_width_) *
                                    static_cast<int64_t>(values_count_));
  ARROW_CHECK_LE(out, std::numeric_limits<raw_data_size_type>::max());
  return static_cast<raw_data_size_type>(out);
}

/************************
 *  RleBitPackedParser  *
 ************************/

constexpr RleBitPackedParser::RleBitPackedParser(raw_data_const_pointer data,
                                                 raw_data_size_type size,
                                                 bit_size_type value_bit_width) noexcept {
  Reset(data, size, value_bit_width);
}

constexpr void RleBitPackedParser::Reset(raw_data_const_pointer data,
                                         raw_data_size_type data_size,
                                         bit_size_type value_bit_width) noexcept {
  data_ = data;
  data_size_ = data_size;
  value_bit_width_ = value_bit_width;
}

inline auto RleBitPackedParser::Peek() const -> std::optional<dynamic_run_type> {
  if (ARROW_PREDICT_FALSE(Exhausted())) {
    return {};
  }

  auto out = std::optional<dynamic_run_type>{};
  auto handler = internal::LambdaHandler{[&](auto run) {
    out = run;
    return ControlFlow::Break;
  }};
  PeekImpl(handler);
  return out;
}

inline auto RleBitPackedParser::Next() -> std::optional<dynamic_run_type> {
  if (ARROW_PREDICT_FALSE(Exhausted())) {
    return {};
  }

  auto out = std::optional<dynamic_run_type>{};
  auto handler = internal::LambdaHandler{[&](auto run) {
    out = run;
    return ControlFlow::Break;
  }};
  PeekImpl(handler);
  auto [read, control] = PeekImpl(handler);
  data_ += read;
  data_size_ -= read;
  return out;
}

inline bool RleBitPackedParser::Advance() { return Next().has_value(); }

inline bool RleBitPackedParser::Exhausted() const { return data_size_ == 0; }

template <typename Handler>
auto RleBitPackedParser::PeekImpl(Handler&& handler) const
    -> std::pair<raw_data_size_type, ControlFlow> {
  ARROW_DCHECK(!Exhausted());

  constexpr auto kMaxSize = bit_util::kMaxLEB128ByteLenFor<uint32_t>;
  uint32_t run_len_type = 0;
  auto const header_bytes = bit_util::ParseLeadingLEB128(data_, kMaxSize, &run_len_type);

  if (ARROW_PREDICT_FALSE(header_bytes == 0)) {
    // Malfomrmed LEB128 data
    return {};
  }

  bool const is_bit_packed = run_len_type & 1;
  uint32_t const count = run_len_type >> 1;
  if (is_bit_packed) {
    using values_count_type = BitPackedRun::values_count_type;
    constexpr auto kMaxCount =
        bit_util::CeilDiv(internal::max_size_for_v<values_count_type>, 8);
    if (ARROW_PREDICT_FALSE(count == 0 || count > kMaxCount)) {
      /// Illegal number of encoded values
      return {};
    }

    auto const values_count = static_cast<values_count_type>(count * 8);
    ARROW_DCHECK_LT(count, internal::max_size_for_v<raw_data_size_type>);
    // Count Already divided by 8
    auto const bytes_read =
        header_bytes + static_cast<raw_data_size_type>(count) * value_bit_width_;

    auto control = handler.OnBitPackedRun(
        BitPackedRun(data_ + header_bytes, values_count, value_bit_width_));

    return {bytes_read, control};
  }

  using values_count_type = RleRun::values_count_type;
  if (ARROW_PREDICT_FALSE(
          count == 0 ||
          count > static_cast<uint32_t>(std::numeric_limits<values_count_type>::max()))) {
    /// Illegal number of encoded values
    return {};
  }

  auto const values_count = static_cast<values_count_type>(count);
  auto const value_bytes = bit_util::BytesForBits(value_bit_width_);
  ARROW_DCHECK_LT(value_bytes, internal::max_size_for_v<raw_data_size_type>);
  auto const bytes_read = header_bytes + static_cast<raw_data_size_type>(value_bytes);

  auto control =
      handler.OnRleRun(RleRun(data_ + header_bytes, values_count, value_bit_width_));

  return {bytes_read, control};
}

template <typename Handler>
void RleBitPackedParser::Parse(Handler&& handler) {
  while (!Exhausted()) {
    auto [read, control] = PeekImpl(handler);
    data_ += read;
    data_size_ -= read;
    if (ARROW_PREDICT_FALSE(control == ControlFlow::Break)) {
      break;
    }
  }
}

/****************
 *  RleDecoder  *
 ****************/

template <typename T>
RleRunDecoder<T>::RleRunDecoder(run_type const& run) noexcept {
  Reset(run);
}

template <typename T>
void RleRunDecoder<T>::Reset(run_type const& run) noexcept {
  remaining_count_ = run.ValuesCount();
  if constexpr (std::is_same_v<value_type, bool>) {
    // ARROW-18031:  just check the LSB of the next byte and move on.
    // If we memcpy + FromLittleEndian, we have potential undefined behavior
    // if the bool value isn't 0 or 1.
    value_ = *run.RawDataPtr() & 1;
  } else {
    // Memcopy is required to avoid undefined behavior.
    value_ = {};
    std::memcpy(&value_, run.RawDataPtr(), run.RawDataSize());
    value_ = ::arrow::bit_util::FromLittleEndian(value_);
  }
}

template <typename T>
auto RleRunDecoder<T>::Remaining() const -> values_count_type {
  return remaining_count_;
}

template <typename T>
auto constexpr RleRunDecoder<T>::Value() const -> value_type {
  return value_;
}

template <typename T>
auto RleRunDecoder<T>::Advance(values_count_type batch_size) -> values_count_type {
  auto const steps = std::min(batch_size, remaining_count_);
  remaining_count_ -= steps;
  return steps;
}

template <typename T>
constexpr bool RleRunDecoder<T>::Get(value_type* out_value) {
  return GetBatch(out_value, 1) == 1;
}

template <typename T>
auto RleRunDecoder<T>::GetBatch(value_type* out, values_count_type batch_size)
    -> values_count_type {
  if (ARROW_PREDICT_FALSE(remaining_count_ == 0)) {
    return 0;
  }

  auto const to_read = std::min(remaining_count_, batch_size);
  std::fill(out, out + to_read, value_);
  remaining_count_ -= to_read;
  return to_read;
}

/**********************
 *  BitPackedDecoder  *
 **********************/

template <typename T>
BitPackedRunDecoder<T>::BitPackedRunDecoder(run_type const& run) noexcept {
  Reset(run);
}

template <typename T>
void BitPackedRunDecoder<T>::Reset(run_type const& run) noexcept {
  value_bit_width_ = run.ValuesBitWidth();
  remaining_count_ = run.ValuesCount();
  ARROW_DCHECK_GE(value_bit_width_, 0);
  ARROW_DCHECK_LE(value_bit_width_, 64);
  bit_reader_.Reset(run.RawDataPtr(), run.RawDataSize());
}

template <typename T>
auto constexpr BitPackedRunDecoder<T>::Remaining() const -> values_count_type {
  return remaining_count_;
}

template <typename T>
auto constexpr BitPackedRunDecoder<T>::ValueBitWidth() const -> bit_size_type {
  return value_bit_width_;
}

template <typename T>
auto BitPackedRunDecoder<T>::Advance(values_count_type batch_size) -> values_count_type {
  auto const steps = std::min(batch_size, remaining_count_);
  if (bit_reader_.Advance(steps * value_bit_width_)) {
    remaining_count_ -= steps;
    return steps;
  }
  return 0;
}

template <typename T>
bool BitPackedRunDecoder<T>::Get(value_type* out_value) {
  return GetBatch(out_value, 1) == 1;
}

template <typename T>
auto BitPackedRunDecoder<T>::GetBatch(value_type* out, values_count_type batch_size)
    -> values_count_type {
  if (ARROW_PREDICT_FALSE(remaining_count_ == 0)) {
    return 0;
  }

  auto const to_read = std::min(remaining_count_, batch_size);
  auto const actual_read = bit_reader_.GetBatch(value_bit_width_, out, to_read);
  // There should not be any reason why the actual read would be different
  // but this is error resistant.
  remaining_count_ -= actual_read;
  return actual_read;
}

/*************************
 *  RleBitPackedEncoder  *
 *************************/

/// This function buffers input values 8 at a time.  After seeing all 8 values,
/// it decides whether they should be encoded as a literal or repeated run.
inline bool RleBitPackedEncoder::Put(uint64_t value) {
  ARROW_DCHECK(bit_width_ == 64 || value < (1ULL << bit_width_));
  if (ARROW_PREDICT_FALSE(buffer_full_)) return false;

  if (ARROW_PREDICT_TRUE(current_value_ == value)) {
    ++repeat_count_;
    if (repeat_count_ > 8) {
      // This is just a continuation of the current run, no need to buffer the
      // values.
      // Note that this is the fast path for long repeated runs.
      return true;
    }
  } else {
    if (repeat_count_ >= 8) {
      // We had a run that was long enough but it has ended.  Flush the
      // current repeated run.
      ARROW_DCHECK_EQ(literal_count_, 0);
      FlushRepeatedRun();
    }
    repeat_count_ = 1;
    current_value_ = value;
  }

  buffered_values_[num_buffered_values_] = value;
  if (++num_buffered_values_ == 8) {
    ARROW_DCHECK_EQ(literal_count_ % 8, 0);
    FlushBufferedValues(false);
  }
  return true;
}

inline void RleBitPackedEncoder::FlushLiteralRun(bool update_indicator_byte) {
  if (literal_indicator_byte_ == NULL) {
    // The literal indicator byte has not been reserved yet, get one now.
    literal_indicator_byte_ = bit_writer_.GetNextBytePtr();
    ARROW_DCHECK(literal_indicator_byte_ != NULL);
  }

  // Write all the buffered values as bit packed literals
  for (int i = 0; i < num_buffered_values_; ++i) {
    bool success = bit_writer_.PutValue(buffered_values_[i], bit_width_);
    ARROW_DCHECK(success) << "There is a bug in using CheckBufferFull()";
  }
  num_buffered_values_ = 0;

  if (update_indicator_byte) {
    // At this point we need to write the indicator byte for the literal run.
    // We only reserve one byte, to allow for streaming writes of literal values.
    // The logic makes sure we flush literal runs often enough to not overrun
    // the 1 byte.
    ARROW_DCHECK_EQ(literal_count_ % 8, 0);
    int num_groups = literal_count_ / 8;
    int32_t indicator_value = (num_groups << 1) | 1;
    ARROW_DCHECK_EQ(indicator_value & 0xFFFFFF00, 0);
    *literal_indicator_byte_ = static_cast<uint8_t>(indicator_value);
    literal_indicator_byte_ = NULL;
    literal_count_ = 0;
    CheckBufferFull();
  }
}

inline void RleBitPackedEncoder::FlushRepeatedRun() {
  ARROW_DCHECK_GT(repeat_count_, 0);
  bool result = true;
  // The lsb of 0 indicates this is a repeated run
  int32_t indicator_value = repeat_count_ << 1 | 0;
  result &= bit_writer_.PutVlqInt(static_cast<uint32_t>(indicator_value));
  result &= bit_writer_.PutAligned(
      current_value_, static_cast<int>(::arrow::bit_util::CeilDiv(bit_width_, 8)));
  ARROW_DCHECK(result);
  num_buffered_values_ = 0;
  repeat_count_ = 0;
  CheckBufferFull();
}

/// Flush the values that have been buffered.  At this point we decide whether
/// we need to switch between the run types or continue the current one.
inline void RleBitPackedEncoder::FlushBufferedValues(bool done) {
  if (repeat_count_ >= 8) {
    // Clear the buffered values.  They are part of the repeated run now and we
    // don't want to flush them out as literals.
    num_buffered_values_ = 0;
    if (literal_count_ != 0) {
      // There was a current literal run.  All the values in it have been flushed
      // but we still need to update the indicator byte.
      ARROW_DCHECK_EQ(literal_count_ % 8, 0);
      ARROW_DCHECK_EQ(repeat_count_, 8);
      FlushLiteralRun(true);
    }
    ARROW_DCHECK_EQ(literal_count_, 0);
    return;
  }

  literal_count_ += num_buffered_values_;
  ARROW_DCHECK_EQ(literal_count_ % 8, 0);
  int num_groups = literal_count_ / 8;
  if (num_groups + 1 >= (1 << 6)) {
    // We need to start a new literal run because the indicator byte we've reserved
    // cannot store more values.
    ARROW_DCHECK(literal_indicator_byte_ != NULL);
    FlushLiteralRun(true);
  } else {
    FlushLiteralRun(done);
  }
  repeat_count_ = 0;
}

inline int RleBitPackedEncoder::Flush() {
  if (literal_count_ > 0 || repeat_count_ > 0 || num_buffered_values_ > 0) {
    bool all_repeat = literal_count_ == 0 && (repeat_count_ == num_buffered_values_ ||
                                              num_buffered_values_ == 0);
    // There is something pending, figure out if it's a repeated or literal run
    if (repeat_count_ > 0 && all_repeat) {
      FlushRepeatedRun();
    } else {
      ARROW_DCHECK_EQ(literal_count_ % 8, 0);
      // Buffer the last group of literals to 8 by padding with 0s.
      for (; num_buffered_values_ != 0 && num_buffered_values_ < 8;
           ++num_buffered_values_) {
        buffered_values_[num_buffered_values_] = 0;
      }
      literal_count_ += num_buffered_values_;
      FlushLiteralRun(true);
      repeat_count_ = 0;
    }
  }
  bit_writer_.Flush();
  ARROW_DCHECK_EQ(num_buffered_values_, 0);
  ARROW_DCHECK_EQ(literal_count_, 0);
  ARROW_DCHECK_EQ(repeat_count_, 0);

  return bit_writer_.bytes_written();
}

inline void RleBitPackedEncoder::CheckBufferFull() {
  int bytes_written = bit_writer_.bytes_written();
  if (bytes_written + max_run_byte_size_ > bit_writer_.buffer_len()) {
    buffer_full_ = true;
  }
}

inline void RleBitPackedEncoder::Clear() {
  buffer_full_ = false;
  current_value_ = 0;
  repeat_count_ = 0;
  num_buffered_values_ = 0;
  literal_count_ = 0;
  literal_indicator_byte_ = NULL;
  bit_writer_.Clear();
}

}  // namespace arrow::util
