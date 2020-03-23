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

#include "parquet/encoding.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"
#include "arrow/util/ubsan.h"
#include "arrow/visitor_inline.h"

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Status;
using arrow::VisitNullBitmapInline;
using arrow::internal::checked_cast;

template <typename T>
using ArrowPoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

namespace parquet {

class DecoderImpl : virtual public Decoder {
 public:
  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  int values_left() const override { return num_values_; }
  Encoding::type encoding() const override { return encoding_; }

 protected:
  explicit DecoderImpl(const ColumnDescriptor* descr, Encoding::type encoding)
      : descr_(descr), encoding_(encoding), num_values_(0), data_(NULLPTR), len_(0) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;

  const Encoding::type encoding_;
  int num_values_;
  const uint8_t* data_;
  int len_;
  int type_length_;
};

template <typename DType>
class PlainDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
 public:
  using T = typename DType::c_type;
  explicit PlainDecoder(const ColumnDescriptor* descr);

  int Decode(T* buffer, int max_values) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* builder) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* builder) override;
};

template <>
inline int PlainDecoder<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::Accumulator* builder) {
  ParquetException::NYI("DecodeArrow not supported for Int96");
}

template <>
inline int PlainDecoder<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::DictAccumulator* builder) {
  ParquetException::NYI("DecodeArrow not supported for Int96");
}

template <>
inline int PlainDecoder<BooleanType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::DictAccumulator* builder) {
  ParquetException::NYI("dictionaries of BooleanType");
}

template <typename DType>
int PlainDecoder<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::Accumulator* builder) {
  using value_type = typename DType::c_type;

  constexpr int value_size = static_cast<int>(sizeof(value_type));
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(len_ < value_size * values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      builder->UnsafeAppend(arrow::util::SafeLoadAs<value_type>(data_));
      data_ += sizeof(value_type);
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  len_ -= sizeof(value_type) * values_decoded;
  return values_decoded;
}

template <typename DType>
int PlainDecoder<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::DictAccumulator* builder) {
  using value_type = typename DType::c_type;

  constexpr int value_size = static_cast<int>(sizeof(value_type));
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(len_ < value_size * values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      PARQUET_THROW_NOT_OK(builder->Append(arrow::util::SafeLoadAs<value_type>(data_)));
      data_ += sizeof(value_type);
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  len_ -= sizeof(value_type) * values_decoded;
  return values_decoded;
}

// Decode routine templated on C++ type rather than type enum
template <typename T>
inline int DecodePlain(const uint8_t* data, int64_t data_size, int num_values,
                       int type_length, T* out) {
  int bytes_to_decode = num_values * static_cast<int>(sizeof(T));
  if (data_size < bytes_to_decode) {
    ParquetException::EofException();
  }
  // If bytes_to_decode == 0, data could be null
  if (bytes_to_decode > 0) {
    memcpy(out, data, bytes_to_decode);
  }
  return bytes_to_decode;
}

template <typename DType>
PlainDecoder<DType>::PlainDecoder(const ColumnDescriptor* descr)
    : DecoderImpl(descr, Encoding::PLAIN) {
  if (descr_ && descr_->physical_type() == Type::FIXED_LEN_BYTE_ARRAY) {
    type_length_ = descr_->type_length();
  } else {
    type_length_ = -1;
  }
}

// Template specialization for BYTE_ARRAY. The written values do not own their
// own data.

static inline int64_t ReadByteArray(const uint8_t* data, int64_t data_size,
                                    ByteArray* out) {
  if (ARROW_PREDICT_FALSE(data_size < 4)) {
    ParquetException::EofException();
  }
  const uint32_t len = arrow::util::SafeLoadAs<uint32_t>(data);
  const int64_t increment = static_cast<int64_t>(4 + len);
  if (ARROW_PREDICT_FALSE(data_size < increment)) {
    ParquetException::EofException();
  }
  *out = ByteArray{len, data + 4};
  return increment;
}

template <>
inline int DecodePlain<ByteArray>(const uint8_t* data, int64_t data_size, int num_values,
                                  int type_length, ByteArray* out) {
  int bytes_decoded = 0;
  for (int i = 0; i < num_values; ++i) {
    const auto increment = ReadByteArray(data, data_size, out + i);
    if (ARROW_PREDICT_FALSE(increment > INT_MAX - bytes_decoded)) {
      throw ParquetException("BYTE_ARRAY chunk too large");
    }
    data += increment;
    data_size -= increment;
    bytes_decoded += static_cast<int>(increment);
  }
  return bytes_decoded;
}

// Template specialization for FIXED_LEN_BYTE_ARRAY. The written values do not
// own their own data.
template <>
inline int DecodePlain<FixedLenByteArray>(const uint8_t* data, int64_t data_size,
                                          int num_values, int type_length,
                                          FixedLenByteArray* out) {
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

template <typename DType>
int PlainDecoder<DType>::Decode(T* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  int bytes_consumed = DecodePlain<T>(data_, len_, max_values, type_length_, buffer);
  data_ += bytes_consumed;
  len_ -= bytes_consumed;
  num_values_ -= max_values;
  return max_values;
}

class PlainBooleanDecoder : public DecoderImpl,
                            virtual public TypedDecoder<BooleanType>,
                            virtual public BooleanDecoder {
 public:
  explicit PlainBooleanDecoder(const ColumnDescriptor* descr);
  void SetData(int num_values, const uint8_t* data, int len) override;

  // Two flavors of bool decoding
  int Decode(uint8_t* buffer, int max_values) override;
  int Decode(bool* buffer, int max_values) override;
  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<BooleanType>::Accumulator* out) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<BooleanType>::DictAccumulator* out) override;

 private:
  std::unique_ptr<arrow::BitUtil::BitReader> bit_reader_;
};

PlainBooleanDecoder::PlainBooleanDecoder(const ColumnDescriptor* descr)
    : DecoderImpl(descr, Encoding::PLAIN) {}

void PlainBooleanDecoder::SetData(int num_values, const uint8_t* data, int len) {
  num_values_ = num_values;
  bit_reader_.reset(new BitUtil::BitReader(data, len));
}

int PlainBooleanDecoder::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::Accumulator* builder) {
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(num_values_ < values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      bool value;
      ARROW_IGNORE_EXPR(bit_reader_->GetValue(1, &value));
      builder->UnsafeAppend(value);
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  return values_decoded;
}

inline int PlainBooleanDecoder::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::DictAccumulator* builder) {
  ParquetException::NYI("dictionaries of BooleanType");
}

int PlainBooleanDecoder::Decode(uint8_t* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  bool val;
  arrow::internal::BitmapWriter bit_writer(buffer, 0, max_values);
  for (int i = 0; i < max_values; ++i) {
    if (!bit_reader_->GetValue(1, &val)) {
      ParquetException::EofException();
    }
    if (val) {
      bit_writer.Set();
    }
    bit_writer.Next();
  }
  bit_writer.Finish();
  num_values_ -= max_values;
  return max_values;
}

int PlainBooleanDecoder::Decode(bool* buffer, int max_values) {
  max_values = std::min(max_values, num_values_);
  if (bit_reader_->GetBatch(1, buffer, max_values) != max_values) {
    ParquetException::EofException();
  }
  num_values_ -= max_values;
  return max_values;
}

struct ArrowBinaryHelper {
  explicit ArrowBinaryHelper(typename EncodingTraits<ByteArrayType>::Accumulator* out) {
    this->out = out;
    this->builder = out->builder.get();
    this->chunk_space_remaining =
        ::arrow::kBinaryMemoryLimit - this->builder->value_data_length();
  }

  Status PushChunk() {
    std::shared_ptr<::arrow::Array> result;
    RETURN_NOT_OK(builder->Finish(&result));
    out->chunks.push_back(result);
    chunk_space_remaining = ::arrow::kBinaryMemoryLimit;
    return Status::OK();
  }

  bool CanFit(int64_t length) const { return length <= chunk_space_remaining; }

  void UnsafeAppend(const uint8_t* data, int32_t length) {
    chunk_space_remaining -= length;
    builder->UnsafeAppend(data, length);
  }

  void UnsafeAppendNull() { builder->UnsafeAppendNull(); }

  Status Append(const uint8_t* data, int32_t length) {
    chunk_space_remaining -= length;
    return builder->Append(data, length);
  }

  Status AppendNull() { return builder->AppendNull(); }

  typename EncodingTraits<ByteArrayType>::Accumulator* out;
  arrow::BinaryBuilder* builder;
  int64_t chunk_space_remaining;
};

template <>
inline int PlainDecoder<ByteArrayType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<ByteArrayType>::Accumulator* builder) {
  ParquetException::NYI();
}

template <>
inline int PlainDecoder<ByteArrayType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<ByteArrayType>::DictAccumulator* builder) {
  ParquetException::NYI();
}

template <>
inline int PlainDecoder<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::Accumulator* builder) {
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(len_ < descr_->type_length() * values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      builder->UnsafeAppend(data_);
      data_ += descr_->type_length();
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  len_ -= descr_->type_length() * values_decoded;
  return values_decoded;
}

template <>
inline int PlainDecoder<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::DictAccumulator* builder) {
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(len_ < descr_->type_length() * values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      PARQUET_THROW_NOT_OK(builder->Append(data_));
      data_ += descr_->type_length();
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  len_ -= descr_->type_length() * values_decoded;
  return values_decoded;
}

class PlainByteArrayDecoder : public PlainDecoder<ByteArrayType>,
                              virtual public ByteArrayDecoder {
 public:
  using Base = PlainDecoder<ByteArrayType>;
  using Base::DecodeSpaced;
  using Base::PlainDecoder;

  // ----------------------------------------------------------------------
  // Dictionary read paths

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  arrow::BinaryDictionary32Builder* builder) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrow(num_values, null_count, valid_bits,
                                     valid_bits_offset, builder, &result));
    return result;
  }

  // ----------------------------------------------------------------------
  // Optimized dense binary read paths

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::Accumulator* out) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                          valid_bits_offset, out, &result));
    return result;
  }

 private:
  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_values_decoded) {
    ArrowBinaryHelper helper(out);
    int values_decoded = 0;

    RETURN_NOT_OK(helper.builder->Reserve(num_values));
    RETURN_NOT_OK(helper.builder->ReserveData(
        std::min<int64_t>(len_, helper.chunk_space_remaining)));
    int i = 0;

    auto decode_value = [&](bool is_valid) {
      if (is_valid) {
        if (ARROW_PREDICT_FALSE(len_ < 4)) {
          ParquetException::EofException();
        }
        auto value_len = arrow::util::SafeLoadAs<int32_t>(data_);
        if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > INT32_MAX - 4)) {
          return Status::Invalid("Invalid or corrupted value_len '", value_len, "'");
        }
        auto increment = value_len + 4;
        if (ARROW_PREDICT_FALSE(len_ < increment)) {
          ParquetException::EofException();
        }
        if (ARROW_PREDICT_FALSE(!helper.CanFit(value_len))) {
          // This element would exceed the capacity of a chunk
          RETURN_NOT_OK(helper.PushChunk());
          RETURN_NOT_OK(helper.builder->Reserve(num_values - i));
          RETURN_NOT_OK(helper.builder->ReserveData(
              std::min<int64_t>(len_, helper.chunk_space_remaining)));
        }
        helper.UnsafeAppend(data_ + 4, value_len);
        data_ += increment;
        len_ -= increment;
        ++values_decoded;
      } else {
        helper.UnsafeAppendNull();
      }
      ++i;
      return Status::OK();
    };

    RETURN_NOT_OK(VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values,
                                        null_count, std::move(decode_value)));

    num_values_ -= values_decoded;
    *out_values_decoded = values_decoded;
    return Status::OK();
  }

  template <typename BuilderType>
  Status DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset, BuilderType* builder,
                     int* out_values_decoded) {
    RETURN_NOT_OK(builder->Reserve(num_values));
    int values_decoded = 0;

    auto decode_value = [&](bool is_valid) {
      if (is_valid) {
        if (ARROW_PREDICT_FALSE(len_ < 4)) {
          ParquetException::EofException();
        }
        auto value_len = arrow::util::SafeLoadAs<int32_t>(data_);
        if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > INT32_MAX - 4)) {
          return Status::Invalid("Invalid or corrupted value_len '", value_len, "'");
        }
        auto increment = value_len + 4;
        if (ARROW_PREDICT_FALSE(len_ < increment)) {
          ParquetException::EofException();
        }
        RETURN_NOT_OK(builder->Append(data_ + 4, value_len));
        data_ += increment;
        len_ -= increment;
        ++values_decoded;
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
      return Status::OK();
    };

    RETURN_NOT_OK(VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values,
                                        null_count, std::move(decode_value)));

    num_values_ -= values_decoded;
    *out_values_decoded = values_decoded;
    return Status::OK();
  }
};

class PlainFLBADecoder : public PlainDecoder<FLBAType>, virtual public FLBADecoder {
 public:
  using Base = PlainDecoder<FLBAType>;
  using Base::PlainDecoder;
};

// ----------------------------------------------------------------------
// Dictionary encoding and decoding

template <typename Type>
class DictDecoderImpl : public DecoderImpl, virtual public DictDecoder<Type> {
 public:
  typedef typename Type::c_type T;

  // Initializes the dictionary with values from 'dictionary'. The data in
  // dictionary is not guaranteed to persist in memory after this call so the
  // dictionary decoder needs to copy the data out if necessary.
  explicit DictDecoderImpl(const ColumnDescriptor* descr,
                           MemoryPool* pool = arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::RLE_DICTIONARY),
        dictionary_(AllocateBuffer(pool, 0)),
        dictionary_length_(0),
        byte_array_data_(AllocateBuffer(pool, 0)),
        byte_array_offsets_(AllocateBuffer(pool, 0)),
        indices_scratch_space_(AllocateBuffer(pool, 0)) {}

  // Perform type-specific initiatialization
  void SetDict(TypedDecoder<Type>* dictionary) override;

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    if (ARROW_PREDICT_FALSE(bit_width >= 64)) {
      throw ParquetException("Invalid or corrupted bit_width");
    }
    idx_decoder_ = arrow::util::RleDecoder(++data, --len, bit_width);
  }

  int Decode(T* buffer, int num_values) override {
    num_values = std::min(num_values, num_values_);
    int decoded_values =
        idx_decoder_.GetBatchWithDict(reinterpret_cast<const T*>(dictionary_->data()),
                                      dictionary_length_, buffer, num_values);
    if (decoded_values != num_values) {
      ParquetException::EofException();
    }
    num_values_ -= num_values;
    return num_values;
  }

  int DecodeSpaced(T* buffer, int num_values, int null_count, const uint8_t* valid_bits,
                   int64_t valid_bits_offset) override {
    num_values = std::min(num_values, num_values_);
    if (num_values != idx_decoder_.GetBatchWithDictSpaced(
                          reinterpret_cast<const T*>(dictionary_->data()),
                          dictionary_length_, buffer, num_values, null_count, valid_bits,
                          valid_bits_offset)) {
      ParquetException::EofException();
    }
    num_values_ -= num_values;
    return num_values;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<Type>::Accumulator* out) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<Type>::DictAccumulator* out) override;

  void InsertDictionary(arrow::ArrayBuilder* builder) override;

  int DecodeIndicesSpaced(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          arrow::ArrayBuilder* builder) override {
    if (num_values > 0) {
      // TODO(wesm): Refactor to batch reads for improved memory use. It is not
      // trivial because the null_count is relative to the entire bitmap
      PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
          num_values, /*shrink_to_fit=*/false));
    }

    auto indices_buffer =
        reinterpret_cast<int32_t*>(indices_scratch_space_->mutable_data());

    if (num_values != idx_decoder_.GetBatchSpaced(num_values, null_count, valid_bits,
                                                  valid_bits_offset, indices_buffer)) {
      ParquetException::EofException();
    }

    /// XXX(wesm): Cannot append "valid bits" directly to the builder
    std::vector<uint8_t> valid_bytes(num_values);
    arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);
    for (int64_t i = 0; i < num_values; ++i) {
      valid_bytes[i] = static_cast<uint8_t>(bit_reader.IsSet());
      bit_reader.Next();
    }

    auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(
        binary_builder->AppendIndices(indices_buffer, num_values, valid_bytes.data()));
    num_values_ -= num_values - null_count;
    return num_values - null_count;
  }

  int DecodeIndices(int num_values, arrow::ArrayBuilder* builder) override {
    num_values = std::min(num_values, num_values_);
    num_values = std::min(num_values, num_values_);
    if (num_values > 0) {
      // TODO(wesm): Refactor to batch reads for improved memory use. This is
      // relatively simple here because we don't have to do any bookkeeping of
      // nulls
      PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
          num_values, /*shrink_to_fit=*/false));
    }
    auto indices_buffer =
        reinterpret_cast<int32_t*>(indices_scratch_space_->mutable_data());
    if (num_values != idx_decoder_.GetBatch(indices_buffer, num_values)) {
      ParquetException::EofException();
    }
    auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(binary_builder->AppendIndices(indices_buffer, num_values));
    num_values_ -= num_values;
    return num_values;
  }

 protected:
  inline void DecodeDict(TypedDecoder<Type>* dictionary) {
    dictionary_length_ = static_cast<int32_t>(dictionary->values_left());
    PARQUET_THROW_NOT_OK(dictionary_->Resize(dictionary_length_ * sizeof(T),
                                             /*shrink_to_fit=*/false));
    dictionary->Decode(reinterpret_cast<T*>(dictionary_->mutable_data()),
                       dictionary_length_);
  }

  // Only one is set.
  std::shared_ptr<ResizableBuffer> dictionary_;

  int32_t dictionary_length_;

  // Data that contains the byte array data (byte_array_dictionary_ just has the
  // pointers).
  std::shared_ptr<ResizableBuffer> byte_array_data_;

  // Arrow-style byte offsets for each dictionary value. We maintain two
  // representations of the dictionary, one as ByteArray* for non-Arrow
  // consumers and this one for Arrow consumers. Since dictionaries are
  // generally pretty small to begin with this doesn't mean too much extra
  // memory use in most cases
  std::shared_ptr<ResizableBuffer> byte_array_offsets_;

  // Reusable buffer for decoding dictionary indices to be appended to a
  // BinaryDictionary32Builder
  std::shared_ptr<ResizableBuffer> indices_scratch_space_;

  arrow::util::RleDecoder idx_decoder_;
};

template <typename Type>
void DictDecoderImpl<Type>::SetDict(TypedDecoder<Type>* dictionary) {
  DecodeDict(dictionary);
}

template <>
void DictDecoderImpl<BooleanType>::SetDict(TypedDecoder<BooleanType>* dictionary) {
  ParquetException::NYI("Dictionary encoding is not implemented for boolean values");
}

template <>
void DictDecoderImpl<ByteArrayType>::SetDict(TypedDecoder<ByteArrayType>* dictionary) {
  DecodeDict(dictionary);

  auto dict_values = reinterpret_cast<ByteArray*>(dictionary_->mutable_data());

  int total_size = 0;
  for (int i = 0; i < dictionary_length_; ++i) {
    total_size += dict_values[i].len;
  }
  if (total_size > 0) {
    PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
                                                  /*shrink_to_fit=*/false));
    PARQUET_THROW_NOT_OK(
        byte_array_offsets_->Resize((dictionary_length_ + 1) * sizeof(int32_t),
                                    /*shrink_to_fit=*/false));
  }

  int32_t offset = 0;
  uint8_t* bytes_data = byte_array_data_->mutable_data();
  int32_t* bytes_offsets =
      reinterpret_cast<int32_t*>(byte_array_offsets_->mutable_data());
  for (int i = 0; i < dictionary_length_; ++i) {
    memcpy(bytes_data + offset, dict_values[i].ptr, dict_values[i].len);
    bytes_offsets[i] = offset;
    dict_values[i].ptr = bytes_data + offset;
    offset += dict_values[i].len;
  }
  bytes_offsets[dictionary_length_] = offset;
}

template <>
inline void DictDecoderImpl<FLBAType>::SetDict(TypedDecoder<FLBAType>* dictionary) {
  DecodeDict(dictionary);

  auto dict_values = reinterpret_cast<FLBA*>(dictionary_->mutable_data());

  int fixed_len = descr_->type_length();
  int total_size = dictionary_length_ * fixed_len;

  PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
                                                /*shrink_to_fit=*/false));
  uint8_t* bytes_data = byte_array_data_->mutable_data();
  for (int32_t i = 0, offset = 0; i < dictionary_length_; ++i, offset += fixed_len) {
    memcpy(bytes_data + offset, dict_values[i].ptr, fixed_len);
    dict_values[i].ptr = bytes_data + offset;
  }
}

template <>
inline int DictDecoderImpl<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::Accumulator* builder) {
  ParquetException::NYI("DecodeArrow to Int96Type");
}

template <>
inline int DictDecoderImpl<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::DictAccumulator* builder) {
  ParquetException::NYI("DecodeArrow to Int96Type");
}

template <>
inline int DictDecoderImpl<ByteArrayType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<ByteArrayType>::Accumulator* builder) {
  ParquetException::NYI("DecodeArrow implemented elsewhere");
}

template <>
inline int DictDecoderImpl<ByteArrayType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<ByteArrayType>::DictAccumulator* builder) {
  ParquetException::NYI("DecodeArrow implemented elsewhere");
}

template <typename DType>
int DictDecoderImpl<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::DictAccumulator* builder) {
  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto dict_values = reinterpret_cast<const typename DType::c_type*>(dictionary_->data());

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      int32_t index;
      if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
        throw ParquetException("");
      }
      PARQUET_THROW_NOT_OK(builder->Append(dict_values[index]));
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  return num_values - null_count;
}

template <>
int DictDecoderImpl<BooleanType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::DictAccumulator* builder) {
  ParquetException::NYI("No dictionary encoding for BooleanType");
}

template <>
inline int DictDecoderImpl<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::Accumulator* builder) {
  if (builder->byte_width() != descr_->type_length()) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(builder->byte_width()) + " but decoder was " +
                           std::to_string(descr_->type_length()));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto dict_values = reinterpret_cast<const FLBA*>(dictionary_->data());

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      int32_t index;
      if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
        throw ParquetException("");
      }
      builder->UnsafeAppend(dict_values[index].ptr);
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  return num_values - null_count;
}

template <>
int DictDecoderImpl<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::DictAccumulator* builder) {
  auto value_type =
      checked_cast<const arrow::DictionaryType&>(*builder->type()).value_type();
  auto byte_width =
      checked_cast<const arrow::FixedSizeBinaryType&>(*value_type).byte_width();
  if (byte_width != descr_->type_length()) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(byte_width) + " but decoder was " +
                           std::to_string(descr_->type_length()));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto dict_values = reinterpret_cast<const FLBA*>(dictionary_->data());

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      int32_t index;
      if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
        throw ParquetException("");
      }
      PARQUET_THROW_NOT_OK(builder->Append(dict_values[index].ptr));
    } else {
      PARQUET_THROW_NOT_OK(builder->AppendNull());
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  return num_values - null_count;
}

template <typename Type>
int DictDecoderImpl<Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Type>::Accumulator* builder) {
  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  using value_type = typename Type::c_type;
  auto dict_values = reinterpret_cast<const value_type*>(dictionary_->data());

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      int32_t index;
      if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
        throw ParquetException("");
      }
      builder->UnsafeAppend(dict_values[index]);
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  return num_values - null_count;
}

template <typename Type>
void DictDecoderImpl<Type>::InsertDictionary(arrow::ArrayBuilder* builder) {
  ParquetException::NYI("InsertDictionary only implemented for BYTE_ARRAY types");
}

template <>
void DictDecoderImpl<ByteArrayType>::InsertDictionary(arrow::ArrayBuilder* builder) {
  auto binary_builder = checked_cast<arrow::BinaryDictionary32Builder*>(builder);

  // Make a BinaryArray referencing the internal dictionary data
  auto arr = std::make_shared<arrow::BinaryArray>(dictionary_length_, byte_array_offsets_,
                                                  byte_array_data_);
  PARQUET_THROW_NOT_OK(binary_builder->InsertMemoValues(*arr));
}

class DictByteArrayDecoderImpl : public DictDecoderImpl<ByteArrayType>,
                                 virtual public ByteArrayDecoder {
 public:
  using BASE = DictDecoderImpl<ByteArrayType>;
  using BASE::DictDecoderImpl;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  arrow::BinaryDictionary32Builder* builder) override {
    int result = 0;
    if (null_count == 0) {
      PARQUET_THROW_NOT_OK(DecodeArrowNonNull(num_values, builder, &result));
    } else {
      PARQUET_THROW_NOT_OK(DecodeArrow(num_values, null_count, valid_bits,
                                       valid_bits_offset, builder, &result));
    }
    return result;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::Accumulator* out) override {
    int result = 0;
    if (null_count == 0) {
      PARQUET_THROW_NOT_OK(DecodeArrowDenseNonNull(num_values, out, &result));
    } else {
      PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                            valid_bits_offset, out, &result));
    }
    return result;
  }

 private:
  Status IndexInBounds(int32_t index) {
    if (ARROW_PREDICT_TRUE(0 <= index && index < dictionary_length_)) {
      return Status::OK();
    }
    return Status::Invalid("Index not in dictionary bounds");
  }

  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_num_values) {
    constexpr int32_t kBufferSize = 1024;
    int32_t indices[kBufferSize];

    ArrowBinaryHelper helper(out);

    arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

    auto dict_values = reinterpret_cast<const ByteArray*>(dictionary_->data());
    int values_decoded = 0;
    int num_appended = 0;
    while (num_appended < num_values) {
      bool is_valid = bit_reader.IsSet();
      bit_reader.Next();

      if (is_valid) {
        int32_t batch_size =
            std::min<int32_t>(kBufferSize, num_values - num_appended - null_count);
        int num_indices = idx_decoder_.GetBatch(indices, batch_size);

        if (ARROW_PREDICT_FALSE(num_indices < 1)) {
          return Status::Invalid("Invalid number of indices '", num_indices, "'");
        }

        int i = 0;
        while (true) {
          // Consume all indices
          if (is_valid) {
            auto idx = indices[i];
            RETURN_NOT_OK(IndexInBounds(idx));
            const auto& val = dict_values[idx];
            if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
              RETURN_NOT_OK(helper.PushChunk());
            }
            RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
            ++i;
            ++values_decoded;
          } else {
            RETURN_NOT_OK(helper.AppendNull());
            --null_count;
          }
          ++num_appended;
          if (i == num_indices) {
            // Do not advance the bit_reader if we have fulfilled the decode
            // request
            break;
          }
          is_valid = bit_reader.IsSet();
          bit_reader.Next();
        }
      } else {
        RETURN_NOT_OK(helper.AppendNull());
        --null_count;
        ++num_appended;
      }
    }
    *out_num_values = values_decoded;
    return Status::OK();
  }

  Status DecodeArrowDenseNonNull(int num_values,
                                 typename EncodingTraits<ByteArrayType>::Accumulator* out,
                                 int* out_num_values) {
    constexpr int32_t kBufferSize = 2048;
    int32_t indices[kBufferSize];
    int values_decoded = 0;

    ArrowBinaryHelper helper(out);
    auto dict_values = reinterpret_cast<const ByteArray*>(dictionary_->data());

    while (values_decoded < num_values) {
      int32_t batch_size = std::min<int32_t>(kBufferSize, num_values - values_decoded);
      int num_indices = idx_decoder_.GetBatch(indices, batch_size);
      if (num_indices == 0) ParquetException::EofException();
      for (int i = 0; i < num_indices; ++i) {
        auto idx = indices[i];
        RETURN_NOT_OK(IndexInBounds(idx));
        const auto& val = dict_values[idx];
        if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
          RETURN_NOT_OK(helper.PushChunk());
        }
        RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
      }
      values_decoded += num_indices;
    }
    *out_num_values = values_decoded;
    return Status::OK();
  }

  template <typename BuilderType>
  Status DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset, BuilderType* builder,
                     int* out_num_values) {
    constexpr int32_t kBufferSize = 1024;
    int32_t indices[kBufferSize];

    RETURN_NOT_OK(builder->Reserve(num_values));
    arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

    auto dict_values = reinterpret_cast<const ByteArray*>(dictionary_->data());

    int values_decoded = 0;
    int num_appended = 0;
    while (num_appended < num_values) {
      bool is_valid = bit_reader.IsSet();
      bit_reader.Next();

      if (is_valid) {
        int32_t batch_size =
            std::min<int32_t>(kBufferSize, num_values - num_appended - null_count);
        int num_indices = idx_decoder_.GetBatch(indices, batch_size);

        int i = 0;
        while (true) {
          // Consume all indices
          if (is_valid) {
            auto idx = indices[i];
            RETURN_NOT_OK(IndexInBounds(idx));
            const auto& val = dict_values[idx];
            RETURN_NOT_OK(builder->Append(val.ptr, val.len));
            ++i;
            ++values_decoded;
          } else {
            RETURN_NOT_OK(builder->AppendNull());
            --null_count;
          }
          ++num_appended;
          if (i == num_indices) {
            // Do not advance the bit_reader if we have fulfilled the decode
            // request
            break;
          }
          is_valid = bit_reader.IsSet();
          bit_reader.Next();
        }
      } else {
        RETURN_NOT_OK(builder->AppendNull());
        --null_count;
        ++num_appended;
      }
    }
    *out_num_values = values_decoded;
    return Status::OK();
  }

  template <typename BuilderType>
  Status DecodeArrowNonNull(int num_values, BuilderType* builder, int* out_num_values) {
    constexpr int32_t kBufferSize = 2048;
    int32_t indices[kBufferSize];

    RETURN_NOT_OK(builder->Reserve(num_values));

    auto dict_values = reinterpret_cast<const ByteArray*>(dictionary_->data());

    int values_decoded = 0;
    while (values_decoded < num_values) {
      int32_t batch_size = std::min<int32_t>(kBufferSize, num_values - values_decoded);
      int num_indices = idx_decoder_.GetBatch(indices, batch_size);
      if (num_indices == 0) ParquetException::EofException();
      for (int i = 0; i < num_indices; ++i) {
        auto idx = indices[i];
        RETURN_NOT_OK(IndexInBounds(idx));
        const auto& val = dict_values[idx];
        RETURN_NOT_OK(builder->Append(val.ptr, val.len));
      }
      values_decoded += num_indices;
    }
    *out_num_values = values_decoded;
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// DeltaBitPackDecoder

template <typename DType>
class DeltaBitPackDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit DeltaBitPackDecoder(const ColumnDescriptor* descr,
                               MemoryPool* pool = arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BINARY_PACKED), pool_(pool) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  void SetData(int num_values, const uint8_t* data, int len) override {
    this->num_values_ = num_values;
    decoder_ = arrow::BitUtil::BitReader(data, len);
    values_current_block_ = 0;
    values_current_mini_block_ = 0;
  }

  int Decode(T* buffer, int max_values) override {
    return GetInternal(buffer, max_values);
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* out) override {
    if (null_count != 0) {
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->AppendValues(values));
    return num_values;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* out) override {
    if (null_count != 0) {
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->Reserve(num_values));
    for (T value : values) {
      PARQUET_THROW_NOT_OK(out->Append(value));
    }
    return num_values;
  }

 private:
  void InitBlock() {
    // The number of values per block.
    uint32_t block_size;
    if (!decoder_.GetVlqInt(&block_size)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&values_current_block_)) {
      ParquetException::EofException();
    }
    if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();

    delta_bit_widths_ = AllocateBuffer(pool_, num_mini_blocks_);
    uint8_t* bit_width_data = delta_bit_widths_->mutable_data();

    if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();
    for (uint32_t i = 0; i < num_mini_blocks_; ++i) {
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
    max_values = std::min(max_values, this->num_values_);
    const uint8_t* bit_width_data = delta_bit_widths_->data();
    for (int i = 0; i < max_values; ++i) {
      if (ARROW_PREDICT_FALSE(values_current_mini_block_ == 0)) {
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
      last_value_ += static_cast<int32_t>(delta);
      buffer[i] = last_value_;
      --values_current_mini_block_;
    }
    this->num_values_ -= max_values;
    return max_values;
  }

  MemoryPool* pool_;
  arrow::BitUtil::BitReader decoder_;
  uint32_t values_current_block_;
  uint32_t num_mini_blocks_;
  uint64_t values_per_mini_block_;
  uint64_t values_current_mini_block_;

  int32_t min_delta_;
  size_t mini_block_idx_;
  std::shared_ptr<ResizableBuffer> delta_bit_widths_;
  int delta_bit_width_;

  int32_t last_value_;
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY

class DeltaLengthByteArrayDecoder : public DecoderImpl,
                                    virtual public TypedDecoder<ByteArrayType> {
 public:
  explicit DeltaLengthByteArrayDecoder(const ColumnDescriptor* descr,
                                       MemoryPool* pool = arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY),
        len_decoder_(nullptr, pool),
        pool_(pool) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    if (len == 0) return;
    int total_lengths_len = arrow::util::SafeLoadAs<int32_t>(data);
    data += 4;
    this->len_decoder_.SetData(num_values, data, total_lengths_len);
    data_ = data + total_lengths_len;
    this->len_ = len - 4 - total_lengths_len;
  }

  int Decode(ByteArray* buffer, int max_values) override {
    using VectorT = ArrowPoolVector<int>;
    max_values = std::min(max_values, num_values_);
    VectorT lengths(max_values, 0, ::arrow::stl::allocator<int>(pool_));
    len_decoder_.Decode(lengths.data(), max_values);
    for (int i = 0; i < max_values; ++i) {
      buffer[i].len = lengths[i];
      buffer[i].ptr = data_;
      this->data_ += lengths[i];
      this->len_ -= lengths[i];
    }
    this->num_values_ -= max_values;
    return max_values;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::Accumulator* out) override {
    ParquetException::NYI("DecodeArrow for DeltaLengthByteArrayDecoder");
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::DictAccumulator* out) override {
    ParquetException::NYI("DecodeArrow for DeltaLengthByteArrayDecoder");
  }

 private:
  DeltaBitPackDecoder<Int32Type> len_decoder_;
  ::arrow::MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY

class DeltaByteArrayDecoder : public DecoderImpl,
                              virtual public TypedDecoder<ByteArrayType> {
 public:
  explicit DeltaByteArrayDecoder(const ColumnDescriptor* descr,
                                 MemoryPool* pool = arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BYTE_ARRAY),
        prefix_len_decoder_(nullptr, pool),
        suffix_decoder_(nullptr, pool),
        last_value_(0, nullptr) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int prefix_len_length = arrow::util::SafeLoadAs<int32_t>(data);
    data += 4;
    len -= 4;
    prefix_len_decoder_.SetData(num_values, data, prefix_len_length);
    data += prefix_len_length;
    len -= prefix_len_length;
    suffix_decoder_.SetData(num_values, data, len);
  }

  // TODO: this doesn't work and requires memory management. We need to allocate
  // new strings to store the results.
  virtual int Decode(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, this->num_values_);
    for (int i = 0; i < max_values; ++i) {
      int prefix_len = 0;
      prefix_len_decoder_.Decode(&prefix_len, 1);
      ByteArray suffix = {0, nullptr};
      suffix_decoder_.Decode(&suffix, 1);
      buffer[i].len = prefix_len + suffix.len;

      uint8_t* result = reinterpret_cast<uint8_t*>(malloc(buffer[i].len));
      memcpy(result, last_value_.ptr, prefix_len);
      memcpy(result + prefix_len, suffix.ptr, suffix.len);

      buffer[i].ptr = result;
      last_value_ = buffer[i];
    }
    this->num_values_ -= max_values;
    return max_values;
  }

 private:
  DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  ByteArray last_value_;
};

// ----------------------------------------------------------------------
// BYTE_STREAM_SPLIT

template <typename DType>
class ByteStreamSplitDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
 public:
  using T = typename DType::c_type;
  explicit ByteStreamSplitDecoder(const ColumnDescriptor* descr);

  int Decode(T* buffer, int max_values) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* builder) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* builder) override;

  void SetData(int num_values, const uint8_t* data, int len) override;

 private:
  int num_values_in_buffer{0U};

  static constexpr size_t kNumStreams = sizeof(T);
};

template <typename DType>
ByteStreamSplitDecoder<DType>::ByteStreamSplitDecoder(const ColumnDescriptor* descr)
    : DecoderImpl(descr, Encoding::BYTE_STREAM_SPLIT) {}

template <typename DType>
void ByteStreamSplitDecoder<DType>::SetData(int num_values, const uint8_t* data,
                                            int len) {
  DecoderImpl::SetData(num_values, data, len);
  num_values_in_buffer = num_values;
}

template <typename DType>
int ByteStreamSplitDecoder<DType>::Decode(T* buffer, int max_values) {
  const int values_to_decode = std::min(num_values_, max_values);
  const int num_decoded_previously = num_values_in_buffer - num_values_;
  for (int i = 0; i < values_to_decode; ++i) {
    uint8_t gathered_byte_data[kNumStreams];
    for (size_t b = 0; b < kNumStreams; ++b) {
      const size_t byte_index = b * num_values_in_buffer + num_decoded_previously + i;
      gathered_byte_data[b] = data_[byte_index];
    }
    buffer[i] = arrow::util::SafeLoadAs<T>(&gathered_byte_data[0]);
  }
  num_values_ -= values_to_decode;
  len_ -= sizeof(T) * values_to_decode;
  return values_to_decode;
}

template <typename DType>
int ByteStreamSplitDecoder<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::Accumulator* builder) {
  constexpr int value_size = static_cast<int>(kNumStreams);
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(len_ < value_size * values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  const int num_decoded_previously = num_values_in_buffer - num_values_;
  int offset = 0;

  auto decode_value = [&](bool is_valid) {
    if (is_valid) {
      uint8_t gathered_byte_data[kNumStreams];
      for (size_t b = 0; b < kNumStreams; ++b) {
        const size_t byte_index =
            b * num_values_in_buffer + num_decoded_previously + offset;
        gathered_byte_data[b] = data_[byte_index];
      }
      builder->UnsafeAppend(arrow::util::SafeLoadAs<T>(&gathered_byte_data[0]));
      ++offset;
    } else {
      builder->UnsafeAppendNull();
    }
  };

  VisitNullBitmapInline(valid_bits, valid_bits_offset, num_values, null_count,
                        std::move(decode_value));

  num_values_ -= values_decoded;
  len_ -= sizeof(T) * values_decoded;
  return values_decoded;
}

template <typename DType>
int ByteStreamSplitDecoder<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::DictAccumulator* builder) {
  ParquetException::NYI("DecodeArrow for ByteStreamSplitDecoder");
}

// ----------------------------------------------------------------------

std::unique_ptr<Decoder> MakeDecoder(Type::type type_num, Encoding::type encoding,
                                     const ColumnDescriptor* descr) {
  if (encoding == Encoding::PLAIN) {
    switch (type_num) {
      case Type::BOOLEAN:
        return std::unique_ptr<Decoder>(new PlainBooleanDecoder(descr));
      case Type::INT32:
        return std::unique_ptr<Decoder>(new PlainDecoder<Int32Type>(descr));
      case Type::INT64:
        return std::unique_ptr<Decoder>(new PlainDecoder<Int64Type>(descr));
      case Type::INT96:
        return std::unique_ptr<Decoder>(new PlainDecoder<Int96Type>(descr));
      case Type::FLOAT:
        return std::unique_ptr<Decoder>(new PlainDecoder<FloatType>(descr));
      case Type::DOUBLE:
        return std::unique_ptr<Decoder>(new PlainDecoder<DoubleType>(descr));
      case Type::BYTE_ARRAY:
        return std::unique_ptr<Decoder>(new PlainByteArrayDecoder(descr));
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::unique_ptr<Decoder>(new PlainFLBADecoder(descr));
      default:
        break;
    }
  } else if (encoding == Encoding::BYTE_STREAM_SPLIT) {
    switch (type_num) {
      case Type::FLOAT:
        return std::unique_ptr<Decoder>(new ByteStreamSplitDecoder<FloatType>(descr));
      case Type::DOUBLE:
        return std::unique_ptr<Decoder>(new ByteStreamSplitDecoder<DoubleType>(descr));
      default:
        throw ParquetException("BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE");
        break;
    }
  } else {
    ParquetException::NYI("Selected encoding is not supported");
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
}

namespace detail {
std::unique_ptr<Decoder> MakeDictDecoder(Type::type type_num,
                                         const ColumnDescriptor* descr,
                                         MemoryPool* pool) {
  switch (type_num) {
    case Type::BOOLEAN:
      ParquetException::NYI("Dictionary encoding not implemented for boolean type");
    case Type::INT32:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<Int32Type>(descr, pool));
    case Type::INT64:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<Int64Type>(descr, pool));
    case Type::INT96:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<Int96Type>(descr, pool));
    case Type::FLOAT:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<FloatType>(descr, pool));
    case Type::DOUBLE:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<DoubleType>(descr, pool));
    case Type::BYTE_ARRAY:
      return std::unique_ptr<Decoder>(new DictByteArrayDecoderImpl(descr, pool));
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::unique_ptr<Decoder>(new DictDecoderImpl<FLBAType>(descr, pool));
    default:
      break;
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
}

}  // namespace detail
}  // namespace parquet
