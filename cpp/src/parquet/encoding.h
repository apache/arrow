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

#ifndef PARQUET_ENCODING_H
#define PARQUET_ENCODING_H

#include <cstdint>
#include <memory>

#include <arrow/util/bit-util.h>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/bit-util.h"
#include "parquet/util/memory.h"

namespace parquet {

class ColumnDescriptor;

// Base class for value encoders. Since encoders may or not have state (e.g.,
// dictionary encoding) we use a class instance to maintain any state.
//
// TODO(wesm): Encode interface API is temporary
template <typename DType>
class Encoder {
 public:
  typedef typename DType::c_type T;

  virtual ~Encoder() {}

  virtual int64_t EstimatedDataEncodedSize() = 0;
  virtual std::shared_ptr<Buffer> FlushValues() = 0;
  virtual void Put(const T* src, int num_values) = 0;
  virtual void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
      int64_t valid_bits_offset) {
    PoolBuffer buffer(allocator_);
    buffer.Resize(num_values * sizeof(T));
    int32_t num_valid_values = 0;
    INIT_BITSET(valid_bits, valid_bits_offset);
    T* data = reinterpret_cast<T*>(buffer.mutable_data());
    for (int32_t i = 0; i < num_values; i++) {
      if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
        data[num_valid_values++] = src[i];
      }
      READ_NEXT_BITSET(valid_bits);
    }
    Put(data, num_valid_values);
  }

  const Encoding::type encoding() const { return encoding_; }

 protected:
  explicit Encoder(const ColumnDescriptor* descr, const Encoding::type& encoding,
      MemoryAllocator* allocator)
      : descr_(descr), encoding_(encoding), allocator_(allocator) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;
  const Encoding::type encoding_;
  MemoryAllocator* allocator_;
};

// The Decoder template is parameterized on parquet::DataType subclasses
template <typename DType>
class Decoder {
 public:
  typedef typename DType::c_type T;

  virtual ~Decoder() {}

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support. In each of these functions,
  // the decoder would decode put to 'max_values', storing the result in 'buffer'.
  // The function returns the number of values decoded, which should be max_values
  // except for end of the current data page.
  virtual int Decode(T* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }

  // Decode the values in this data page but leave spaces for null entries.
  //
  // num_values is the size of the def_levels and buffer arrays including the number of
  // null values.
  virtual int DecodeSpaced(T* buffer, int num_values, int null_count,
      const uint8_t* valid_bits, int64_t valid_bits_offset) {
    int values_to_read = num_values - null_count;
    int values_read = Decode(buffer, values_to_read);
    if (values_read != values_to_read) {
      throw ParquetException("Number of values / definition_levels read did not match");
    }

    // Add spacing for null entries. As we have filled the buffer from the front,
    // we need to add the spacing from the back.
    int values_to_move = values_read;
    for (int i = num_values - 1; i >= 0; i--) {
      if (::arrow::BitUtil::GetBit(valid_bits, valid_bits_offset + i)) {
        buffer[i] = buffer[--values_to_move];
      }
    }
    return num_values;
  }

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  int values_left() const { return num_values_; }

  const Encoding::type encoding() const { return encoding_; }

 protected:
  explicit Decoder(const ColumnDescriptor* descr, const Encoding::type& encoding)
      : descr_(descr), encoding_(encoding), num_values_(0) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;

  const Encoding::type encoding_;
  int num_values_;
};

}  // namespace parquet

#endif  // PARQUET_ENCODING_H
