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

#include <cstdint>
#include <variant>

#include "arrow/util/bit_util.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"
#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

/**********************
 *  PageLevelDecoder  *
 **********************/

/// Decoder for repetition or definition level.
///
/// This decoder is used with either a deprecated bit packed (`BIT_PACKED = 4`)
/// encoding or a mixed bit packed and RLE one (`RLE = 3`).
/// Because it take as input a single buffer, `SetData` and `Decode` are typically
/// used on each of the parquet `DataPage`.
/// The number of levels is guaranteed to fit into an `int32_t` by the specification.
///
/// @see https://research.google.com/pubs/archive/36632.pdf
template <typename BitDecoder, typename RleDecoder>
class PageLevelDecoder {
 public:
  struct DataParams {
    int32_t value_count = 0;
    int16_t max_level = 0;
  };

  explicit PageLevelDecoder(int16_t max_level = 0)
      : decoder_(BitDecoder(nullptr, 0, DataParams{.max_level = max_level})),
        max_level_(max_level) {}

  /// Initialize the decoder state with new data from a legacy (V1) page.
  ///
  /// @return the number of bytes consumed
  int32_t SetDataV1(Encoding::type encoding, const uint8_t* data, int32_t max_data_size,
                    const DataParams& params);

  /// Initialize the decoder state with new data from a V2 page.
  ///
  /// Repetition and definition levels in V2 pages are always RLE encoded.
  void SetDataV2(const uint8_t* data, int32_t data_size, const DataParams& params);

  /// Decode a batch of levels into `out` and return the number of levels decoded.
  template <typename Out>
  int32_t Decode(Out&& out, int32_t batch_size);

  /// Advance the decoder and throw away decoded levels.
  int32_t Skip(int32_t batch_size);

  struct CountUpToResult {
    int32_t matching_count;
    int32_t processed_count;
  };

  /// Advance and count the number of occurrences of `value`.
  ///
  /// The count is limited to at most the next `batch_size` items.
  /// @return The matching value count and number of elements that were processed.
  CountUpToResult CountUpTo(bool value, int32_t batch_size);

  /// Return the max level used in this decoder.
  int16_t max_level() const { return max_level_; }

  /// Return the number of values left to be decoded.
  int32_t remaining() const { return num_values_remaining_; }

 private:
  std::variant<BitDecoder, RleDecoder> decoder_;
  /// Number of values remaining. The underlying decoder zero pads bit packed values
  /// up to a multiple of 8 so it cannot know the exact number of remaining values.
  int32_t num_values_remaining_ = 0;
  int16_t max_level_ = 0;
};

/****************************************
 *  Implementation of PageLevelDecoder  *
 ****************************************/

template <typename BitDec, typename RleDec>
int32_t PageLevelDecoder<BitDec, RleDec>::SetDataV1(Encoding::type encoding,
                                                    const uint8_t* data,
                                                    int32_t max_data_size,
                                                    const DataParams& params) {
  using ::arrow::bit_util::BytesForBits;
  using ::arrow::bit_util::Log2;
  using ::arrow::internal::MultiplyWithOverflow;

  num_values_remaining_ = params.value_count;
  max_level_ = params.max_level;
  const int32_t value_bit_width = Log2(params.max_level + 1);

  switch (encoding) {
    case Encoding::RLE: {
      if (ARROW_PREDICT_FALSE(max_data_size < 4)) {
        throw ParquetException("Received invalid levels (corrupt data page?)");
      }
      const auto data_size = ::arrow::util::SafeLoadAs<int32_t>(data);
      if (ARROW_PREDICT_FALSE(data_size < 0 || data_size > max_data_size - 4)) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      decoder_ = RleDec(/* data= */ data + 4, /* data_size= */ data_size, params);
      return 4 + data_size;
    }
    case Encoding::BIT_PACKED: {
      int32_t num_bits = 0;
      if (MultiplyWithOverflow(params.value_count, value_bit_width, &num_bits)) {
        throw ParquetException(
            "Number of buffered values too large (corrupt data page?)");
      }
      const auto data_size = static_cast<int32_t>(BytesForBits(num_bits));
      if (ARROW_PREDICT_FALSE(data_size < 0 || data_size > max_data_size)) {
        throw ParquetException("Received invalid number of bytes (corrupt data page?)");
      }
      decoder_ = BitDec(/* data= */ data, /* data_size= */ data_size, params);
      return data_size;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

template <typename BitDec, typename RleDec>
void PageLevelDecoder<BitDec, RleDec>::SetDataV2(const uint8_t* data, int32_t data_size,
                                                 const DataParams& params) {
  if (data_size < 0) {
    throw ParquetException("Invalid page header (corrupt data page?)");
  }
  num_values_remaining_ = params.value_count;
  max_level_ = params.max_level;
  decoder_ = RleDec(data, data_size, params);
}

template <typename BitDec, typename RleDec>
template <typename Out>
int32_t PageLevelDecoder<BitDec, RleDec>::Decode(Out&& out, int32_t batch_size) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_decoded = std::visit(
      [&](auto& dec) { return dec.GetBatch(out, num_values, max_level_); }, decoder_);
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

template <typename BitDec, typename RleDec>
int32_t PageLevelDecoder<BitDec, RleDec>::Skip(int32_t batch_size) {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const int32_t num_advanced =
      std::visit([&](auto& dec) { return dec.Advance(num_values); }, decoder_);
  num_values_remaining_ -= num_advanced;
  return num_advanced;
}

template <typename BitDec, typename RleDec>
auto PageLevelDecoder<BitDec, RleDec>::CountUpTo(bool value,
                                                 int32_t batch_size) -> CountUpToResult {
  const int32_t num_values = std::min(num_values_remaining_, batch_size);
  const auto result =
      std::visit([&](auto& dec) { return dec.CountUpTo(value, num_values); }, decoder_);
  num_values_remaining_ -= result.processed_count;
  return {
      .matching_count = result.matching_count,
      .processed_count = result.processed_count,
  };
};
}  // namespace parquet
