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

#ifndef PARQUET_PLAIN_ENCODING_H
#define PARQUET_PLAIN_ENCODING_H

#include "parquet/encodings/encodings.h"

#include <algorithm>

namespace parquet_cpp {

template <int TYPE>
class PlainDecoder : public Decoder<TYPE> {
 public:
  typedef typename type_traits<TYPE>::value_type T;
  using Decoder<TYPE>::num_values_;

  explicit PlainDecoder(const parquet::SchemaElement* schema) :
      Decoder<TYPE>(schema, parquet::Encoding::PLAIN),
      data_(NULL), len_(0) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int Decode(T* buffer, int max_values);
 private:
  const uint8_t* data_;
  int len_;
};

template <int TYPE>
inline int PlainDecoder<TYPE>::Decode(T* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int size = max_values * sizeof(T);
  if (len_ < size)  ParquetException::EofException();
  memcpy(buffer, data_, size);
  data_ += size;
  len_ -= size;
  num_values_ -= max_values;
  return max_values;
}

// Template specialization for BYTE_ARRAY
template <>
inline int PlainDecoder<parquet::Type::BYTE_ARRAY>::Decode(ByteArray* buffer,
    int max_values) {
  max_values = std::min(max_values, num_values_);
  for (int i = 0; i < max_values; ++i) {
    buffer[i].len = *reinterpret_cast<const uint32_t*>(data_);
    if (len_ < sizeof(uint32_t) + buffer[i].len) ParquetException::EofException();
    buffer[i].ptr = data_ + sizeof(uint32_t);
    data_ += sizeof(uint32_t) + buffer[i].len;
    len_ -= sizeof(uint32_t) + buffer[i].len;
  }
  num_values_ -= max_values;
  return max_values;
}

// Template specialization for FIXED_LEN_BYTE_ARRAY
template <>
inline int PlainDecoder<parquet::Type::FIXED_LEN_BYTE_ARRAY>::Decode(
    FixedLenByteArray* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int len = schema_->type_length;
  for (int i = 0; i < max_values; ++i) {
    if (len_ < len) ParquetException::EofException();
    buffer[i].ptr = data_;
    data_ += len;
    len_ -= len;
  }
  num_values_ -= max_values;
  return max_values;
}

template <>
class PlainDecoder<parquet::Type::BOOLEAN> : public Decoder<parquet::Type::BOOLEAN> {
 public:
  explicit PlainDecoder(const parquet::SchemaElement* schema) :
      Decoder<parquet::Type::BOOLEAN>(schema, parquet::Encoding::PLAIN) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = RleDecoder(data, len, 1);
  }

  virtual int Decode(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      if (!decoder_.Get(&buffer[i])) ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }
 private:
  RleDecoder decoder_;
};

} // namespace parquet_cpp

#endif
