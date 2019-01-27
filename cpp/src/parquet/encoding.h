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
#include <cstring>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace arrow {

class BinaryDictionaryBuilder;

namespace internal {

class ChunkedBinaryBuilder;

}  // namespace internal
}  // namespace arrow

namespace parquet {

class ColumnDescriptor;

// Untyped base for all encoders
class Encoder {
 public:
  virtual ~Encoder() = default;

  virtual int64_t EstimatedDataEncodedSize() = 0;
  virtual std::shared_ptr<Buffer> FlushValues() = 0;
  virtual Encoding::type encoding() const = 0;

  virtual ::arrow::MemoryPool* memory_pool() const = 0;
};

// Base class for value encoders. Since encoders may or not have state (e.g.,
// dictionary encoding) we use a class instance to maintain any state.
//
// TODO(wesm): Encode interface API is temporary
template <typename DType>
class TypedEncoder : virtual public Encoder {
 public:
  typedef typename DType::c_type T;

  virtual void Put(const T* src, int num_values) = 0;

  virtual void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                         int64_t valid_bits_offset) {
    std::shared_ptr<ResizableBuffer> buffer;
    PARQUET_THROW_NOT_OK(::arrow::AllocateResizableBuffer(
        this->memory_pool(), num_values * sizeof(T), &buffer));
    int32_t num_valid_values = 0;
    ::arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                      num_values);
    T* data = reinterpret_cast<T*>(buffer->mutable_data());
    for (int32_t i = 0; i < num_values; i++) {
      if (valid_bits_reader.IsSet()) {
        data[num_valid_values++] = src[i];
      }
      valid_bits_reader.Next();
    }
    Put(data, num_valid_values);
  }
};

// Base class for dictionary encoders
template <typename DType>
class DictEncoder : virtual public TypedEncoder<DType> {
 public:
  /// Writes out any buffered indices to buffer preceded by the bit width of this data.
  /// Returns the number of bytes written.
  /// If the supplied buffer is not big enough, returns -1.
  /// buffer must be preallocated with buffer_len bytes. Use EstimatedDataEncodedSize()
  /// to size buffer.
  virtual int WriteIndices(uint8_t* buffer, int buffer_len) = 0;

  virtual int dict_encoded_size() = 0;
  // virtual int dict_encoded_size() { return dict_encoded_size_; }

  virtual int bit_width() const = 0;

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  virtual void WriteDict(uint8_t* buffer) = 0;

  virtual int num_entries() const = 0;
};

// ----------------------------------------------------------------------
// Value decoding

class Decoder {
 public:
  virtual ~Decoder() = default;

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  virtual int values_left() const = 0;
  virtual Encoding::type encoding() const = 0;
};

template <typename DType>
class TypedDecoder : virtual public Decoder {
 public:
  using T = typename DType::c_type;

  // Subclasses should override the ones they support. In each of these functions,
  // the decoder would decode put to 'max_values', storing the result in 'buffer'.
  // The function returns the number of values decoded, which should be max_values
  // except for end of the current data page.
  virtual int Decode(T* buffer, int max_values) = 0;

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

    // Depending on the number of nulls, some of the value slots in buffer may
    // be uninitialized, and this will cause valgrind warnings / potentially UB
    memset(static_cast<void*>(buffer + values_read), 0,
           (num_values - values_read) * sizeof(T));

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
};

template <typename DType>
class DictDecoder : virtual public TypedDecoder<DType> {
 public:
  virtual void SetDict(TypedDecoder<DType>* dictionary) = 0;
};

// ----------------------------------------------------------------------
// TypedEncoder specializations, traits, and factory functions

class BooleanEncoder : virtual public TypedEncoder<BooleanType> {
 public:
  using TypedEncoder<BooleanType>::Put;
  virtual void Put(const std::vector<bool>& src, int num_values) = 0;
};

using Int32Encoder = TypedEncoder<Int32Type>;
using Int64Encoder = TypedEncoder<Int64Type>;
using Int96Encoder = TypedEncoder<Int96Type>;
using FloatEncoder = TypedEncoder<FloatType>;
using DoubleEncoder = TypedEncoder<DoubleType>;
class ByteArrayEncoder : virtual public TypedEncoder<ByteArrayType> {};
class FLBAEncoder : virtual public TypedEncoder<FLBAType> {};

class BooleanDecoder : virtual public TypedDecoder<BooleanType> {
 public:
  using TypedDecoder<BooleanType>::Decode;
  virtual int Decode(uint8_t* buffer, int max_values) = 0;
};

using Int32Decoder = TypedDecoder<Int32Type>;
using Int64Decoder = TypedDecoder<Int64Type>;
using Int96Decoder = TypedDecoder<Int96Type>;
using FloatDecoder = TypedDecoder<FloatType>;
using DoubleDecoder = TypedDecoder<DoubleType>;

class ByteArrayDecoder : virtual public TypedDecoder<ByteArrayType> {
 public:
  using TypedDecoder<ByteArrayType>::DecodeSpaced;
  virtual int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          ::arrow::internal::ChunkedBinaryBuilder* builder) = 0;

  virtual int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          ::arrow::BinaryDictionaryBuilder* builder) = 0;

  // TODO(wesm): Implement DecodeArrowNonNull as part of ARROW-3325
  // See also ARROW-3772, ARROW-3769
  virtual int DecodeArrowNonNull(int num_values,
                                 ::arrow::internal::ChunkedBinaryBuilder* builder) = 0;
};

class FLBADecoder : virtual public TypedDecoder<FLBAType> {
 public:
  using TypedDecoder<FLBAType>::DecodeSpaced;

  // TODO(wesm): As possible follow-up to PARQUET-1508, we should examine if
  // there is value in adding specialized read methods for
  // FIXED_LEN_BYTE_ARRAY. If only Decimal data can occur with this data type
  // then perhaps not
};

template <typename T>
struct EncodingTraits {};

template <>
struct EncodingTraits<BooleanType> {
  using Encoder = BooleanEncoder;
  using Decoder = BooleanDecoder;
};

template <>
struct EncodingTraits<Int32Type> {
  using Encoder = Int32Encoder;
  using Decoder = Int32Decoder;
};

template <>
struct EncodingTraits<Int64Type> {
  using Encoder = Int64Encoder;
  using Decoder = Int64Decoder;
};

template <>
struct EncodingTraits<Int96Type> {
  using Encoder = Int96Encoder;
  using Decoder = Int96Decoder;
};

template <>
struct EncodingTraits<FloatType> {
  using Encoder = FloatEncoder;
  using Decoder = FloatDecoder;
};

template <>
struct EncodingTraits<DoubleType> {
  using Encoder = DoubleEncoder;
  using Decoder = DoubleDecoder;
};

template <>
struct EncodingTraits<ByteArrayType> {
  using Encoder = ByteArrayEncoder;
  using Decoder = ByteArrayDecoder;
};

template <>
struct EncodingTraits<FLBAType> {
  using Encoder = FLBAEncoder;
  using Decoder = FLBADecoder;
};

PARQUET_EXPORT
std::unique_ptr<Encoder> MakeEncoder(
    Type::type type_num, Encoding::type encoding, bool use_dictionary = false,
    const ColumnDescriptor* descr = NULLPTR,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

template <typename DType>
std::unique_ptr<typename EncodingTraits<DType>::Encoder> MakeTypedEncoder(
    Encoding::type encoding, bool use_dictionary = false,
    const ColumnDescriptor* descr = NULLPTR,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool()) {
  using OutType = typename EncodingTraits<DType>::Encoder;
  std::unique_ptr<Encoder> base =
      MakeEncoder(DType::type_num, encoding, use_dictionary, descr, pool);
  return std::unique_ptr<OutType>(dynamic_cast<OutType*>(base.release()));
}

PARQUET_EXPORT
std::unique_ptr<Decoder> MakeDecoder(Type::type type_num, Encoding::type encoding,
                                     const ColumnDescriptor* descr = NULLPTR);

namespace detail {

PARQUET_EXPORT
std::unique_ptr<Decoder> MakeDictDecoder(Type::type type_num,
                                         const ColumnDescriptor* descr,
                                         ::arrow::MemoryPool* pool);

}  // namespace detail

template <typename DType>
std::unique_ptr<DictDecoder<DType>> MakeDictDecoder(
    const ColumnDescriptor* descr,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool()) {
  using OutType = DictDecoder<DType>;
  auto decoder = detail::MakeDictDecoder(DType::type_num, descr, pool);
  return std::unique_ptr<OutType>(dynamic_cast<OutType*>(decoder.release()));
}

template <typename DType>
std::unique_ptr<typename EncodingTraits<DType>::Decoder> MakeTypedDecoder(
    Encoding::type encoding, const ColumnDescriptor* descr = NULLPTR) {
  using OutType = typename EncodingTraits<DType>::Decoder;
  std::unique_ptr<Decoder> base = MakeDecoder(DType::type_num, encoding, descr);
  return std::unique_ptr<OutType>(dynamic_cast<OutType*>(base.release()));
}

}  // namespace parquet
