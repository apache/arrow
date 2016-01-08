// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PARQUET_DELTA_LENGTH_BYTE_ARRAY_ENCODING_H
#define PARQUET_DELTA_LENGTH_BYTE_ARRAY_ENCODING_H

#include "parquet/encodings/encodings.h"

#include <algorithm>

namespace parquet_cpp {

class DeltaLengthByteArrayDecoder : public Decoder {
 public:
  DeltaLengthByteArrayDecoder()
    : Decoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY),
      len_decoder_(parquet::Type::INT32) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int total_lengths_len = *reinterpret_cast<const int*>(data);
    data += 4;
    len_decoder_.SetData(num_values, data, total_lengths_len);
    data_ = data + total_lengths_len;
    len_ = len - 4 - total_lengths_len;
  }

  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int lengths[max_values];
    len_decoder_.GetInt32(lengths, max_values);
    for (int  i = 0; i < max_values; ++i) {
      buffer[i].len = lengths[i];
      buffer[i].ptr = data_;
      data_ += lengths[i];
      len_ -= lengths[i];
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  DeltaBitPackDecoder len_decoder_;
  const uint8_t* data_;
  int len_;
};

} // namespace parquet_cpp

#endif
