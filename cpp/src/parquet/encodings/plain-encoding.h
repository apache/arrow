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
#include "parquet/util/buffer.h"
#include "parquet/util/output.h"

namespace parquet {

// ----------------------------------------------------------------------
// Encoding::PLAIN decoder implementation

template <int TYPE>
class PlainDecoder : public Decoder<TYPE> {
 public:
  typedef typename type_traits<TYPE>::value_type T;
  using Decoder<TYPE>::num_values_;

  explicit PlainDecoder(const ColumnDescriptor* descr) :
      Decoder<TYPE>(descr, Encoding::PLAIN),
      data_(NULL), len_(0) {
    if (descr_ && descr_->physical_type() == Type::FIXED_LEN_BYTE_ARRAY) {
      type_length_ = descr_->type_length();
    } else {
      type_length_ = -1;
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int Decode(T* buffer, int max_values);

 private:
  using Decoder<TYPE>::descr_;
  const uint8_t* data_;
  int len_;
  int type_length_;
};

// Decode routine templated on C++ type rather than type enum
template <typename T>
inline int DecodePlain(const uint8_t* data, int64_t data_size, int num_values,
    int type_length, T* out) {
  int bytes_to_decode = num_values * sizeof(T);
  if (data_size < bytes_to_decode) {
    ParquetException::EofException();
  }
  memcpy(out, data, bytes_to_decode);
  return bytes_to_decode;
}

// Template specialization for BYTE_ARRAY. The written values do not own their
// own data.
template <>
inline int DecodePlain<ByteArray>(const uint8_t* data, int64_t data_size, int num_values,
    int type_length, ByteArray* out) {
  int bytes_decoded = 0;
  int increment;
  for (int i = 0; i < num_values; ++i) {
    uint32_t len = out[i].len = *reinterpret_cast<const uint32_t*>(data);
    increment = sizeof(uint32_t) + len;
    if (data_size < increment) ParquetException::EofException();
    out[i].ptr = data + sizeof(uint32_t);
    data += increment;
    data_size -= increment;
    bytes_decoded += increment;
  }
  return bytes_decoded;
}

// Template specialization for FIXED_LEN_BYTE_ARRAY. The written values do not
// own their own data.
template <>
inline int DecodePlain<FixedLenByteArray>(const uint8_t* data, int64_t data_size,
    int num_values, int type_length, FixedLenByteArray* out) {
  int bytes_to_decode = type_length * num_values;
  if (data_size < bytes_to_decode) {
    ParquetException::EofException();
  }
  for (int i = 0; i < num_values; ++i) {
    out[i].ptr = data;
    data += type_length;
    data_size -= type_length;
  }
  return bytes_to_decode;
}

template <int TYPE>
inline int PlainDecoder<TYPE>::Decode(T* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int bytes_consumed = DecodePlain<T>(data_, len_, max_values,
      type_length_, buffer);
  data_ += bytes_consumed;
  len_ -= bytes_consumed;
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

  explicit PlainEncoder(const ColumnDescriptor* descr,
      MemoryAllocator* allocator = default_allocator()) :
      Encoder<TYPE>(descr, Encoding::PLAIN, allocator) {}

  void Encode(const T* src, int num_values, OutputStream* dst) override;
};

template <>
class PlainEncoder<Type::BOOLEAN> : public Encoder<Type::BOOLEAN> {
 public:
  explicit PlainEncoder(const ColumnDescriptor* descr,
      MemoryAllocator* allocator = default_allocator()) :
      Encoder<Type::BOOLEAN>(descr, Encoding::PLAIN, allocator) {}

  virtual void Encode(const bool* src, int num_values, OutputStream* dst) {
    int bytes_required = BitUtil::Ceil(num_values, 8);
    OwnedMutableBuffer tmp_buffer(bytes_required, allocator_);

    BitWriter bit_writer(&tmp_buffer[0], bytes_required);
    for (int i = 0; i < num_values; ++i) {
      bit_writer.PutValue(src[i], 1);
    }
    bit_writer.Flush();

    // Write the result to the output stream
    dst->Write(bit_writer.buffer(), bit_writer.bytes_written());
  }

  void Encode(const std::vector<bool>& src, int num_values, OutputStream* dst) {
    int bytes_required = BitUtil::Ceil(num_values, 8);

    // TODO(wesm)
    // Use a temporary buffer for now and copy, because the BitWriter is not
    // aware of OutputStream. Later we can add some kind of Request/Flush API
    // to OutputStream
    OwnedMutableBuffer tmp_buffer(bytes_required, allocator_);

    BitWriter bit_writer(&tmp_buffer[0], bytes_required);
    for (int i = 0; i < num_values; ++i) {
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
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    dst->Write(reinterpret_cast<const uint8_t*>(&src[i].len), sizeof(uint32_t));
    dst->Write(reinterpret_cast<const uint8_t*>(src[i].ptr), src[i].len);
  }
}

template <>
inline void PlainEncoder<Type::FIXED_LEN_BYTE_ARRAY>::Encode(
    const FixedLenByteArray* src, int num_values, OutputStream* dst) {
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    dst->Write(reinterpret_cast<const uint8_t*>(src[i].ptr), descr_->type_length());
  }
}
} // namespace parquet

#endif
