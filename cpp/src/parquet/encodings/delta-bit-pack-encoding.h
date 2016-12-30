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

#ifndef PARQUET_DELTA_BIT_PACK_ENCODING_H
#define PARQUET_DELTA_BIT_PACK_ENCODING_H

#include <algorithm>
#include <cstdint>
#include <vector>

#include "parquet/encodings/decoder.h"
#include "parquet/util/bit-stream-utils.inline.h"
#include "parquet/util/memory.h"

namespace parquet {

template <typename DType>
class DeltaBitPackDecoder : public Decoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit DeltaBitPackDecoder(
      const ColumnDescriptor* descr, MemoryAllocator* allocator = default_allocator())
      : Decoder<DType>(descr, Encoding::DELTA_BINARY_PACKED),
        delta_bit_widths_(new PoolBuffer(allocator)) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = BitReader(data, len);
    values_current_block_ = 0;
    values_current_mini_block_ = 0;
  }

  virtual int Decode(T* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

 private:
  using Decoder<DType>::num_values_;

  void InitBlock() {
    int32_t block_size;
    if (!decoder_.GetVlqInt(&block_size)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&values_current_block_)) { ParquetException::EofException(); }
    if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();
    PARQUET_THROW_NOT_OK(delta_bit_widths_->Resize(num_mini_blocks_));

    uint8_t* bit_width_data = delta_bit_widths_->mutable_data();

    if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();
    for (int i = 0; i < num_mini_blocks_; ++i) {
      if (!decoder_.GetAligned<uint8_t>(1, bit_width_data + i)) {
        ParquetException::EofException();
      }
    }
    values_per_mini_block_ = block_size / num_mini_blocks_;
    mini_block_idx_ = 0;
    delta_bit_width_ = bit_width_data[0];
    values_current_mini_block_ = values_per_mini_block_;
  }

  template <typename T>
  int GetInternal(T* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    const uint8_t* bit_width_data = delta_bit_widths_->data();
    for (int i = 0; i < max_values; ++i) {
      if (UNLIKELY(values_current_mini_block_ == 0)) {
        ++mini_block_idx_;
        if (mini_block_idx_ < static_cast<size_t>(delta_bit_widths_->size())) {
          delta_bit_width_ = bit_width_data[mini_block_idx_];
          values_current_mini_block_ = values_per_mini_block_;
        } else {
          InitBlock();
          buffer[i] = last_value_;
          continue;
        }
      }

      // TODO: the key to this algorithm is to decode the entire miniblock at once.
      int64_t delta;
      if (!decoder_.GetValue(delta_bit_width_, &delta)) ParquetException::EofException();
      delta += min_delta_;
      last_value_ += delta;
      buffer[i] = last_value_;
      --values_current_mini_block_;
    }
    num_values_ -= max_values;
    return max_values;
  }

  BitReader decoder_;
  int32_t values_current_block_;
  int32_t num_mini_blocks_;
  uint64_t values_per_mini_block_;
  uint64_t values_current_mini_block_;

  int32_t min_delta_;
  size_t mini_block_idx_;
  std::unique_ptr<PoolBuffer> delta_bit_widths_;
  int delta_bit_width_;

  int32_t last_value_;
};
}  // namespace parquet

#endif
