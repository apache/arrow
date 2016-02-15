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

#include <algorithm>
#include <vector>

#include "parquet/encodings/decoder.h"
#include "parquet/encodings/encoder.h"
#include "parquet/schema/descriptor.h"
#include "parquet/util/bit-stream-utils.inline.h"
#include "parquet/util/output.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// Encoding::PLAIN decoder implementation

template <int TYPE>
class PlainDecoder : public Decoder<TYPE> {
 public:
  typedef typename type_traits<TYPE>::value_type T;
  using Decoder<TYPE>::num_values_;

  explicit PlainDecoder(const ColumnDescriptor* descr) :
      Decoder<TYPE>(descr, Encoding::PLAIN),
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
inline int PlainDecoder<Type::BYTE_ARRAY>::Decode(ByteArray* buffer,
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
inline int PlainDecoder<Type::FIXED_LEN_BYTE_ARRAY>::Decode(
    FixedLenByteArray* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int len = descr_->type_length();
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
class PlainDecoder<Type::BOOLEAN> : public Decoder<Type::BOOLEAN> {
 public:
  explicit PlainDecoder(const ColumnDescriptor* descr) :
      Decoder<Type::BOOLEAN>(descr, Encoding::PLAIN) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    bit_reader_ = BitReader(data, len);
  }

  // Two flavors of bool decoding
  int Decode(uint8_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    bool val;
    for (int i = 0; i < max_values; ++i) {
      if (!bit_reader_.GetValue(1, &val)) {
        ParquetException::EofException();
      }
      BitUtil::SetArrayBit(buffer, i, val);
    }
    num_values_ -= max_values;
    return max_values;
  }

  virtual int Decode(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    bool val;
    for (int i = 0; i < max_values; ++i) {
      if (!bit_reader_.GetValue(1, &val)) {
        ParquetException::EofException();
      }
      buffer[i] = val;
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  BitReader bit_reader_;
};

// ----------------------------------------------------------------------
// Encoding::PLAIN encoder implementation

template <int TYPE>
class PlainEncoder : public Encoder<TYPE> {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  explicit PlainEncoder(const ColumnDescriptor* descr) :
      Encoder<TYPE>(descr, Encoding::PLAIN) {}

  virtual void Encode(const T* src, int num_values, OutputStream* dst);
};

template <>
class PlainEncoder<Type::BOOLEAN> : public Encoder<Type::BOOLEAN> {
 public:
  explicit PlainEncoder(const ColumnDescriptor* descr) :
      Encoder<Type::BOOLEAN>(descr, Encoding::PLAIN) {}

  virtual void Encode(const bool* src, int num_values, OutputStream* dst) {
    throw ParquetException("this API for encoding bools not implemented");
  }

  void Encode(const std::vector<bool>& src, int num_values, OutputStream* dst) {
    size_t bytes_required = BitUtil::RoundUp(num_values, 8) / 8;

    // TODO(wesm)
    // Use a temporary buffer for now and copy, because the BitWriter is not
    // aware of OutputStream. Later we can add some kind of Request/Flush API
    // to OutputStream
    std::vector<uint8_t> tmp_buffer(bytes_required);

    BitWriter bit_writer(&tmp_buffer[0], bytes_required);
    for (size_t i = 0; i < num_values; ++i) {
      bit_writer.PutValue(src[i], 1);
    }
    bit_writer.Flush();

    // Write the result to the output stream
    dst->Write(bit_writer.buffer(), bit_writer.bytes_written());
  }
};

template <int TYPE>
inline void PlainEncoder<TYPE>::Encode(const T* buffer, int num_values,
    OutputStream* dst) {
  dst->Write(reinterpret_cast<const uint8_t*>(buffer), num_values * sizeof(T));
}

template <>
inline void PlainEncoder<Type::BYTE_ARRAY>::Encode(const ByteArray* src,
    int num_values, OutputStream* dst) {
  ParquetException::NYI("byte array encoding");
}

template <>
inline void PlainEncoder<Type::FIXED_LEN_BYTE_ARRAY>::Encode(
    const FixedLenByteArray* src, int num_values, OutputStream* dst) {
  ParquetException::NYI("FLBA encoding");
}

} // namespace parquet_cpp

#endif
