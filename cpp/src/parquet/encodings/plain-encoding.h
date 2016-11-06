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

template <typename DType>
class PlainDecoder : public Decoder<DType> {
 public:
  typedef typename DType::c_type T;
  using Decoder<DType>::num_values_;

  explicit PlainDecoder(const ColumnDescriptor* descr)
      : Decoder<DType>(descr, Encoding::PLAIN), data_(NULL), len_(0) {
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
  using Decoder<DType>::descr_;
  const uint8_t* data_;
  int len_;
  int type_length_;
};

// Decode routine templated on C++ type rather than type enum
template <typename T>
inline int DecodePlain(
    const uint8_t* data, int64_t data_size, int num_values, int type_length, T* out) {
  int bytes_to_decode = num_values * sizeof(T);
  if (data_size < bytes_to_decode) { ParquetException::EofException(); }
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
  if (data_size < bytes_to_decode) { ParquetException::EofException(); }
  for (int i = 0; i < num_values; ++i) {
    out[i].ptr = data;
    data += type_length;
    data_size -= type_length;
  }
  return bytes_to_decode;
}

template <typename DType>
inline int PlainDecoder<DType>::Decode(T* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int bytes_consumed = DecodePlain<T>(data_, len_, max_values, type_length_, buffer);
  data_ += bytes_consumed;
  len_ -= bytes_consumed;
  num_values_ -= max_values;
  return max_values;
}

template <>
class PlainDecoder<BooleanType> : public Decoder<BooleanType> {
 public:
  explicit PlainDecoder(const ColumnDescriptor* descr)
      : Decoder<BooleanType>(descr, Encoding::PLAIN) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    bit_reader_ = BitReader(data, len);
  }

  // Two flavors of bool decoding
  int Decode(uint8_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    bool val;
    for (int i = 0; i < max_values; ++i) {
      if (!bit_reader_.GetValue(1, &val)) { ParquetException::EofException(); }
      BitUtil::SetArrayBit(buffer, i, val);
    }
    num_values_ -= max_values;
    return max_values;
  }

  virtual int Decode(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    if (bit_reader_.GetBatch(1, buffer, max_values) != max_values) {
      ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  BitReader bit_reader_;
};

// ----------------------------------------------------------------------
// Encoding::PLAIN encoder implementation

template <typename DType>
class PlainEncoder : public Encoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit PlainEncoder(
      const ColumnDescriptor* descr, MemoryAllocator* allocator = default_allocator())
      : Encoder<DType>(descr, Encoding::PLAIN, allocator),
        values_sink_(new InMemoryOutputStream(IN_MEMORY_DEFAULT_CAPACITY, allocator)) {}

  int64_t EstimatedDataEncodedSize() override { return values_sink_->Tell(); }

  std::shared_ptr<Buffer> FlushValues() override;
  void Put(const T* src, int num_values) override;

 protected:
  std::shared_ptr<InMemoryOutputStream> values_sink_;
};

template <>
class PlainEncoder<BooleanType> : public Encoder<BooleanType> {
 public:
  explicit PlainEncoder(
      const ColumnDescriptor* descr, MemoryAllocator* allocator = default_allocator())
      : Encoder<BooleanType>(descr, Encoding::PLAIN, allocator),
        bits_available_(IN_MEMORY_DEFAULT_CAPACITY * 8),
        bits_buffer_(IN_MEMORY_DEFAULT_CAPACITY, allocator),
        values_sink_(new InMemoryOutputStream(IN_MEMORY_DEFAULT_CAPACITY, allocator)) {
    bit_writer_.reset(new BitWriter(bits_buffer_.mutable_data(), bits_buffer_.size()));
  }

  int64_t EstimatedDataEncodedSize() override {
    return values_sink_->Tell() + bit_writer_->bytes_written();
  }

  std::shared_ptr<Buffer> FlushValues() override {
    if (bits_available_ > 0) {
      bit_writer_->Flush();
      values_sink_->Write(bit_writer_->buffer(), bit_writer_->bytes_written());
      bits_available_ = 0;
      bit_writer_->Clear();
      bits_available_ = bits_buffer_.size() * 8;
    }

    std::shared_ptr<Buffer> buffer = values_sink_->GetBuffer();
    values_sink_.reset(
        new InMemoryOutputStream(IN_MEMORY_DEFAULT_CAPACITY, this->allocator_));
    return buffer;
  }

#define PLAINDECODER_BOOLEAN_PUT(input_type, function_attributes)                 \
  void Put(input_type src, int num_values) function_attributes {                  \
    int bit_offset = 0;                                                           \
    if (bits_available_ > 0) {                                                    \
      int bits_to_write = std::min(bits_available_, num_values);                  \
      for (int i = 0; i < bits_to_write; i++) {                                   \
        bit_writer_->PutValue(src[i], 1);                                         \
      }                                                                           \
      bits_available_ -= bits_to_write;                                           \
      bit_offset = bits_to_write;                                                 \
                                                                                  \
      if (bits_available_ == 0) {                                                 \
        bit_writer_->Flush();                                                     \
        values_sink_->Write(bit_writer_->buffer(), bit_writer_->bytes_written()); \
        bit_writer_->Clear();                                                     \
      }                                                                           \
    }                                                                             \
                                                                                  \
    int bits_remaining = num_values - bit_offset;                                 \
    while (bit_offset < num_values) {                                             \
      bits_available_ = bits_buffer_.size() * 8;                                  \
                                                                                  \
      int bits_to_write = std::min(bits_available_, bits_remaining);              \
      for (int i = bit_offset; i < bit_offset + bits_to_write; i++) {             \
        bit_writer_->PutValue(src[i], 1);                                         \
      }                                                                           \
      bit_offset += bits_to_write;                                                \
      bits_available_ -= bits_to_write;                                           \
      bits_remaining -= bits_to_write;                                            \
                                                                                  \
      if (bits_available_ == 0) {                                                 \
        bit_writer_->Flush();                                                     \
        values_sink_->Write(bit_writer_->buffer(), bit_writer_->bytes_written()); \
        bit_writer_->Clear();                                                     \
      }                                                                           \
    }                                                                             \
  }

  PLAINDECODER_BOOLEAN_PUT(const bool*, override)
  PLAINDECODER_BOOLEAN_PUT(const std::vector<bool>&, )

 protected:
  int bits_available_;
  std::unique_ptr<BitWriter> bit_writer_;
  OwnedMutableBuffer bits_buffer_;
  std::shared_ptr<InMemoryOutputStream> values_sink_;
};

template <typename DType>
inline std::shared_ptr<Buffer> PlainEncoder<DType>::FlushValues() {
  std::shared_ptr<Buffer> buffer = values_sink_->GetBuffer();
  values_sink_.reset(
      new InMemoryOutputStream(IN_MEMORY_DEFAULT_CAPACITY, this->allocator_));
  return buffer;
}

template <typename DType>
inline void PlainEncoder<DType>::Put(const T* buffer, int num_values) {
  values_sink_->Write(reinterpret_cast<const uint8_t*>(buffer), num_values * sizeof(T));
}

template <>
inline void PlainEncoder<ByteArrayType>::Put(const ByteArray* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    values_sink_->Write(reinterpret_cast<const uint8_t*>(&src[i].len), sizeof(uint32_t));
    values_sink_->Write(reinterpret_cast<const uint8_t*>(src[i].ptr), src[i].len);
  }
}

template <>
inline void PlainEncoder<FLBAType>::Put(const FixedLenByteArray* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    values_sink_->Write(
        reinterpret_cast<const uint8_t*>(src[i].ptr), descr_->type_length());
  }
}
}  // namespace parquet

#endif
