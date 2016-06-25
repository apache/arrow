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

#include "parquet/column/levels.h"

#include <cstdint>

#include "parquet/util/rle-encoding.h"

namespace parquet {

LevelEncoder::LevelEncoder() {}
LevelEncoder::~LevelEncoder() {}

void LevelEncoder::Init(Encoding::type encoding, int16_t max_level,
    int num_buffered_values, uint8_t* data, int data_size) {
  bit_width_ = BitUtil::Log2(max_level + 1);
  encoding_ = encoding;
  switch (encoding) {
    case Encoding::RLE: {
      rle_encoder_.reset(new RleEncoder(data, data_size, bit_width_));
      break;
    }
    case Encoding::BIT_PACKED: {
      int num_bytes = BitUtil::Ceil(num_buffered_values * bit_width_, 8);
      bit_packed_encoder_.reset(new BitWriter(data, num_bytes));
      break;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
}

int LevelEncoder::MaxBufferSize(
    Encoding::type encoding, int16_t max_level, int num_buffered_values) {
  int bit_width = BitUtil::Log2(max_level + 1);
  int num_bytes = 0;
  switch (encoding) {
    case Encoding::RLE: {
      // TODO: Due to the way we currently check if the buffer is full enough,
      // we need to have MinBufferSize as head room.
      num_bytes = RleEncoder::MaxBufferSize(bit_width, num_buffered_values) +
                  RleEncoder::MinBufferSize(bit_width);
      break;
    }
    case Encoding::BIT_PACKED: {
      num_bytes = BitUtil::Ceil(num_buffered_values * bit_width, 8);
      break;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return num_bytes;
}

int LevelEncoder::Encode(int batch_size, const int16_t* levels) {
  int num_encoded = 0;
  if (!rle_encoder_ && !bit_packed_encoder_) {
    throw ParquetException("Level encoders are not initialized.");
  }

  if (encoding_ == Encoding::RLE) {
    for (int i = 0; i < batch_size; ++i) {
      if (!rle_encoder_->Put(*(levels + i))) { break; }
      ++num_encoded;
    }
    rle_encoder_->Flush();
    rle_length_ = rle_encoder_->len();
  } else {
    for (int i = 0; i < batch_size; ++i) {
      if (!bit_packed_encoder_->PutValue(*(levels + i), bit_width_)) { break; }
      ++num_encoded;
    }
    bit_packed_encoder_->Flush();
  }
  return num_encoded;
}

LevelDecoder::LevelDecoder()
    : num_values_remaining_(0) {}

LevelDecoder::~LevelDecoder() {}

int LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
    int num_buffered_values, const uint8_t* data) {
  uint32_t num_bytes = 0;
  encoding_ = encoding;
  num_values_remaining_ = num_buffered_values;
  bit_width_ = BitUtil::Log2(max_level + 1);
  switch (encoding) {
    case Encoding::RLE: {
      num_bytes = *reinterpret_cast<const uint32_t*>(data);
      const uint8_t* decoder_data = data + sizeof(uint32_t);
      if (!rle_decoder_) {
        rle_decoder_.reset(new RleDecoder(decoder_data, num_bytes, bit_width_));
      } else {
        rle_decoder_->Reset(decoder_data, num_bytes, bit_width_);
      }
      return sizeof(uint32_t) + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      num_bytes = BitUtil::Ceil(num_buffered_values * bit_width_, 8);
      if (!bit_packed_decoder_) {
        bit_packed_decoder_.reset(new BitReader(data, num_bytes));
      } else {
        bit_packed_decoder_->Reset(data, num_bytes);
      }
      return num_bytes;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

int LevelDecoder::Decode(int batch_size, int16_t* levels) {
  int num_decoded = 0;

  int num_values = std::min(num_values_remaining_, batch_size);
  if (encoding_ == Encoding::RLE) {
    num_decoded = rle_decoder_->GetBatch(levels, num_values);
  } else {
    for (int i = 0; i < num_values; ++i) {
      if (!bit_packed_decoder_->GetValue(bit_width_, levels + i)) { break; }
      ++num_decoded;
    }
  }
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

}  // namespace parquet
