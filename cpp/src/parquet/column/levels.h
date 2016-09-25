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

#ifndef PARQUET_COLUMN_LEVELS_H
#define PARQUET_COLUMN_LEVELS_H

#include <algorithm>
#include <memory>

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

class BitReader;
class BitWriter;
class RleDecoder;
class RleEncoder;

class LevelEncoder {
 public:
  LevelEncoder();
  ~LevelEncoder();

  static int MaxBufferSize(
      Encoding::type encoding, int16_t max_level, int num_buffered_values);

  // Initialize the LevelEncoder.
  void Init(Encoding::type encoding, int16_t max_level, int num_buffered_values,
      uint8_t* data, int data_size);

  // Encodes a batch of levels from an array and returns the number of levels encoded
  int Encode(int batch_size, const int16_t* levels);

  int32_t len() {
    if (encoding_ != Encoding::RLE) {
      throw ParquetException("Only implemented for RLE encoding");
    }
    return rle_length_;
  }

 private:
  int bit_width_;
  int rle_length_;
  Encoding::type encoding_;
  std::unique_ptr<RleEncoder> rle_encoder_;
  std::unique_ptr<BitWriter> bit_packed_encoder_;
};

class LevelDecoder {
 public:
  LevelDecoder();
  ~LevelDecoder();

  // Initialize the LevelDecoder state with new data
  // and return the number of bytes consumed
  int SetData(Encoding::type encoding, int16_t max_level, int num_buffered_values,
      const uint8_t* data);

  // Decodes a batch of levels into an array and returns the number of levels decoded
  int Decode(int batch_size, int16_t* levels);

 private:
  int bit_width_;
  int num_values_remaining_;
  Encoding::type encoding_;
  std::unique_ptr<RleDecoder> rle_decoder_;
  std::unique_ptr<BitReader> bit_packed_decoder_;
};

}  // namespace parquet
#endif  // PARQUET_COLUMN_LEVELS_H
