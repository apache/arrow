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
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/byte_stream_split_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/rle_encoding_internal.h"
#include "arrow/util/spaced_internal.h"
#include "arrow/util/ubsan.h"
#include "arrow/visit_data_inline.h"

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace bit_util = arrow::bit_util;

using arrow::Status;
using arrow::VisitNullBitmapInline;
using arrow::internal::AddWithOverflow;
using arrow::internal::BitBlockCounter;
using arrow::internal::checked_cast;
using arrow::internal::VisitBitRuns;
using arrow::util::SafeLoad;
using arrow::util::SafeLoadAs;

namespace parquet {
namespace {

// A helper class to abstract away differences between EncodingTraits<DType>::Accumulator
// for ByteArrayType and FLBAType.

template <typename DType, typename ArrowType>
struct ArrowBinaryHelper;

template <>
struct ArrowBinaryHelper<ByteArrayType, ::arrow::BinaryType> {
  using Accumulator = typename EncodingTraits<ByteArrayType>::Accumulator;

  explicit ArrowBinaryHelper(Accumulator* acc)
      : acc_(acc),
        builder_(checked_cast<::arrow::BinaryBuilder*>(acc->builder.get())),
        chunk_space_remaining_(::arrow::kBinaryMemoryLimit -
                               builder_->value_data_length()) {}

  // Prepare will reserve the number of entries in the current chunk.
  // If estimated_data_length is provided, it will also reserve the estimated data length.
  Status Prepare(int64_t length, std::optional<int64_t> estimated_data_length = {}) {
    entries_remaining_ = length;
    RETURN_NOT_OK(builder_->Reserve(entries_remaining_));
    if (estimated_data_length.has_value()) {
      RETURN_NOT_OK(builder_->ReserveData(
          std::min<int64_t>(*estimated_data_length, this->chunk_space_remaining_)));
    }
    return Status::OK();
  }

  // If a new chunk is created and estimated_remaining_data_length is provided,
  // it will also reserve the estimated data length for this chunk.
  Status AppendValue(const uint8_t* data, int32_t length,
                     std::optional<int64_t> estimated_remaining_data_length = {}) {
    DCHECK_GT(entries_remaining_, 0);

    if (ARROW_PREDICT_FALSE(!CanFit(length))) {
      // This element would exceed the capacity of a chunk
      RETURN_NOT_OK(PushChunk());
      // Reserve entries and data in new chunk
      RETURN_NOT_OK(builder_->Reserve(entries_remaining_));
      if (estimated_remaining_data_length.has_value()) {
        RETURN_NOT_OK(builder_->ReserveData(
            std::min<int64_t>(*estimated_remaining_data_length, chunk_space_remaining_)));
      }
    }
    chunk_space_remaining_ -= length;
    --entries_remaining_;
    if (estimated_remaining_data_length.has_value()) {
      // Assume Prepare() was already called with an estimated_data_length
      builder_->UnsafeAppend(data, length);
      return Status::OK();
    } else {
      return builder_->Append(data, length);
    }
  }

  void UnsafeAppendNull() {
    DCHECK_GT(entries_remaining_, 0);
    --entries_remaining_;
    builder_->UnsafeAppendNull();
  }

  Status AppendNulls(int64_t length) {
    DCHECK_GE(entries_remaining_, length);
    entries_remaining_ -= length;
    return builder_->AppendNulls(length);
  }

 private:
  Status PushChunk() {
    ARROW_ASSIGN_OR_RAISE(auto chunk, acc_->builder->Finish());
    acc_->chunks.push_back(std::move(chunk));
    chunk_space_remaining_ = ::arrow::kBinaryMemoryLimit;
    return Status::OK();
  }

  bool CanFit(int64_t length) const { return length <= chunk_space_remaining_; }

  Accumulator* acc_;
  ::arrow::BinaryBuilder* builder_;
  int64_t entries_remaining_;
  int64_t chunk_space_remaining_;
};

template <typename ArrowBinaryType>
struct ArrowBinaryHelper<ByteArrayType, ArrowBinaryType> {
  using Accumulator = typename EncodingTraits<ByteArrayType>::Accumulator;
  using BuilderType = typename ::arrow::TypeTraits<ArrowBinaryType>::BuilderType;

  static constexpr bool kIsBinaryView =
      ::arrow::is_binary_view_like_type<ArrowBinaryType>::value;

  explicit ArrowBinaryHelper(Accumulator* acc)
      : builder_(checked_cast<BuilderType*>(acc->builder.get())) {}

  // Prepare will reserve the number of entries in the current chunk.
  // If estimated_data_length is provided, it will also reserve the estimated data length,
  // and the caller should better call `UnsafeAppend` instead of `Append` to avoid
  // double-checking the data length.
  Status Prepare(int64_t length, std::optional<int64_t> estimated_data_length = {}) {
    RETURN_NOT_OK(builder_->Reserve(length));
    // Avoid reserving data when reading into a binary-view array, because many
    // values may be very short and not require any heap storage, which would make
    // the initial allocation wasteful.
    if (!kIsBinaryView && estimated_data_length.has_value()) {
      RETURN_NOT_OK(builder_->ReserveData(*estimated_data_length));
    }
    return Status::OK();
  }

  Status AppendValue(const uint8_t* data, int32_t length,
                     std::optional<int64_t> estimated_remaining_data_length = {}) {
    if (!kIsBinaryView && estimated_remaining_data_length.has_value()) {
      // Assume Prepare() was already called with an estimated_data_length
      builder_->UnsafeAppend(data, length);
      return Status::OK();
    } else {
      return builder_->Append(data, length);
    }
  }

  void UnsafeAppendNull() { builder_->UnsafeAppendNull(); }

  Status AppendNulls(int64_t length) { return builder_->AppendNulls(length); }

 private:
  BuilderType* builder_;
};

template <>
struct ArrowBinaryHelper<FLBAType, ::arrow::FixedSizeBinaryType> {
  using Accumulator = typename EncodingTraits<FLBAType>::Accumulator;

  explicit ArrowBinaryHelper(Accumulator* acc) : acc_(acc) {}

  Status Prepare(int64_t length, std::optional<int64_t> estimated_data_length = {}) {
    return acc_->Reserve(length);
  }

  Status AppendValue(const uint8_t* data, int32_t length,
                     std::optional<int64_t> estimated_remaining_data_length = {}) {
    acc_->UnsafeAppend(data);
    return Status::OK();
  }

  void UnsafeAppendNull() { acc_->UnsafeAppendNull(); }

  Status AppendNulls(int64_t length) { return acc_->AppendNulls(length); }

 private:
  Accumulator* acc_;
};

// Call `func(&helper, args...)` where `helper` is a ArrowBinaryHelper<> instance
// suitable for the Parquet DType and accumulator `acc`.
template <typename DType, typename Function, typename... Args>
auto DispatchArrowBinaryHelper(typename EncodingTraits<DType>::Accumulator* acc,
                               int64_t length,
                               std::optional<int64_t> estimated_data_length,
                               Function&& func, Args&&... args) {
  static_assert(std::is_same_v<DType, ByteArrayType> || std::is_same_v<DType, FLBAType>,
                "unsupported DType");
  if constexpr (std::is_same_v<DType, ByteArrayType>) {
    switch (acc->builder->type()->id()) {
      case ::arrow::Type::BINARY:
      case ::arrow::Type::STRING: {
        ArrowBinaryHelper<DType, ::arrow::BinaryType> helper(acc);
        RETURN_NOT_OK(helper.Prepare(length, estimated_data_length));
        return func(&helper, std::forward<Args>(args)...);
      }
      case ::arrow::Type::LARGE_BINARY:
      case ::arrow::Type::LARGE_STRING: {
        ArrowBinaryHelper<DType, ::arrow::LargeBinaryType> helper(acc);
        RETURN_NOT_OK(helper.Prepare(length, estimated_data_length));
        return func(&helper, std::forward<Args>(args)...);
      }
      case ::arrow::Type::BINARY_VIEW:
      case ::arrow::Type::STRING_VIEW: {
        ArrowBinaryHelper<DType, ::arrow::BinaryViewType> helper(acc);
        RETURN_NOT_OK(helper.Prepare(length, estimated_data_length));
        return func(&helper, std::forward<Args>(args)...);
      }
      default:
        throw ParquetException(
            "Unsupported Arrow builder type when reading from BYTE_ARRAY column: " +
            acc->builder->type()->ToString());
    }
  } else {
    ArrowBinaryHelper<DType, ::arrow::FixedSizeBinaryType> helper(acc);
    RETURN_NOT_OK(helper.Prepare(length, estimated_data_length));
    return func(&helper, std::forward<Args>(args)...);
  }
}

void CheckPageLargeEnough(int64_t remaining_bytes, int32_t value_width,
                          int64_t num_values) {
  if (remaining_bytes < value_width * num_values) {
    ParquetException::EofException();
  }
}

// Internal decoder class hierarchy

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
  DecoderImpl(const ColumnDescriptor* descr, Encoding::type encoding)
      : descr_(descr), encoding_(encoding), num_values_(0), data_(NULLPTR), len_(0) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;

  const Encoding::type encoding_;
  int num_values_;
  const uint8_t* data_;
  int len_;
};

template <typename DType>
class TypedDecoderImpl : public DecoderImpl, virtual public TypedDecoder<DType> {
 public:
  using T = typename DType::c_type;

 protected:
  TypedDecoderImpl(const ColumnDescriptor* descr, Encoding::type encoding)
      : DecoderImpl(descr, encoding) {
    if constexpr (std::is_same_v<DType, FLBAType>) {
      if (descr_ == nullptr) {
        throw ParquetException(
            "Must pass a ColumnDescriptor when creating a Decoder for "
            "FIXED_LEN_BYTE_ARRAY");
      }
      type_length_ = descr_->type_length();
    } else if constexpr (std::is_same_v<DType, ByteArrayType>) {
      type_length_ = -1;
    } else {
      type_length_ = sizeof(T);
    }
  }

  int DecodeSpaced(T* buffer, int num_values, int null_count, const uint8_t* valid_bits,
                   int64_t valid_bits_offset) override {
    if (null_count > 0) {
      int values_to_read = num_values - null_count;
      int values_read = this->Decode(buffer, values_to_read);
      if (values_read != values_to_read) {
        throw ParquetException("Number of values / definition_levels read did not match");
      }
      ::arrow::util::internal::SpacedExpandRightward<T>(buffer, num_values, null_count,
                                                        valid_bits, valid_bits_offset);
      return num_values;
    } else {
      return this->Decode(buffer, num_values);
    }
  }

  int type_length_;
};

// ----------------------------------------------------------------------
// PLAIN decoder

template <typename DType>
class PlainDecoder : public TypedDecoderImpl<DType> {
 public:
  using T = typename DType::c_type;

  explicit PlainDecoder(const ColumnDescriptor* descr)
      : TypedDecoderImpl<DType>(descr, Encoding::PLAIN) {}

  int Decode(T* buffer, int max_values) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* builder) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* builder) override;
};

template <>
int PlainDecoder<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::Accumulator* builder) {
  ParquetException::NYI("DecodeArrow not supported for Int96");
}

template <>
int PlainDecoder<Int96Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Int96Type>::DictAccumulator* builder) {
  ParquetException::NYI("DecodeArrow not supported for Int96");
}

template <>
int PlainDecoder<BooleanType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::Accumulator* builder) {
  ParquetException::NYI("BooleanType handled in concrete subclass");
}

template <>
int PlainDecoder<BooleanType>::DecodeArrow(
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
  CheckPageLargeEnough(this->len_, value_size, values_decoded);

  const uint8_t* data = this->data_;

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
  PARQUET_THROW_NOT_OK(
      VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                   [&](int64_t position, int64_t run_length, bool is_valid) {
                     if (is_valid) {
                       RETURN_NOT_OK(builder->AppendValues(
                           reinterpret_cast<const value_type*>(data), run_length));
                       data += run_length * sizeof(value_type);
                     } else {
                       RETURN_NOT_OK(builder->AppendNulls(run_length));
                     }
                     return Status::OK();
                   }));

  this->data_ = data;
  this->len_ -= sizeof(value_type) * values_decoded;
  this->num_values_ -= values_decoded;
  return values_decoded;
}

template <typename DType>
int PlainDecoder<DType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<DType>::DictAccumulator* builder) {
  using value_type = typename DType::c_type;

  constexpr int value_size = static_cast<int>(sizeof(value_type));
  int values_decoded = num_values - null_count;
  CheckPageLargeEnough(this->len_, value_size, values_decoded);

  const uint8_t* data = this->data_;

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        PARQUET_THROW_NOT_OK(builder->Append(SafeLoadAs<value_type>(data)));
        data += sizeof(value_type);
      },
      [&]() { PARQUET_THROW_NOT_OK(builder->AppendNull()); });

  this->data_ = data;
  this->len_ -= sizeof(value_type) * values_decoded;
  this->num_values_ -= values_decoded;
  return values_decoded;
}

// Decode routine templated on C++ type rather than type enum
template <typename T>
inline int DecodePlain(const uint8_t* data, int64_t data_size, int num_values,
                       int type_length, T* out) {
  int64_t bytes_to_decode = num_values * static_cast<int64_t>(sizeof(T));
  if (bytes_to_decode > data_size || bytes_to_decode > INT_MAX) {
    ParquetException::EofException();
  }
  // If bytes_to_decode == 0, data could be null
  if (bytes_to_decode > 0) {
    memcpy(out, data, static_cast<size_t>(bytes_to_decode));
  }
  return static_cast<int>(bytes_to_decode);
}

// Template specialization for BYTE_ARRAY. The written values do not own their
// own data.

static inline int64_t ReadByteArray(const uint8_t* data, int64_t data_size,
                                    ByteArray* out) {
  if (ARROW_PREDICT_FALSE(data_size < 4)) {
    ParquetException::EofException();
  }
  const int32_t len = SafeLoadAs<int32_t>(data);
  if (len < 0) {
    throw ParquetException("Invalid BYTE_ARRAY value");
  }
  const int64_t consumed_length = static_cast<int64_t>(len) + 4;
  if (ARROW_PREDICT_FALSE(data_size < consumed_length)) {
    ParquetException::EofException();
  }
  *out = ByteArray{static_cast<uint32_t>(len), data + 4};
  return consumed_length;
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
  int64_t bytes_to_decode = static_cast<int64_t>(type_length) * num_values;
  if (bytes_to_decode > data_size || bytes_to_decode > INT_MAX) {
    ParquetException::EofException();
  }
  for (int i = 0; i < num_values; ++i) {
    out[i].ptr = data + i * static_cast<int64_t>(type_length);
  }
  return static_cast<int>(bytes_to_decode);
}

template <typename DType>
int PlainDecoder<DType>::Decode(T* buffer, int max_values) {
  max_values = std::min(max_values, this->num_values_);
  int bytes_consumed =
      DecodePlain<T>(this->data_, this->len_, max_values, this->type_length_, buffer);
  this->data_ += bytes_consumed;
  this->len_ -= bytes_consumed;
  this->num_values_ -= max_values;
  return max_values;
}

// PLAIN decoder implementation for BOOLEAN

class PlainBooleanDecoder : public TypedDecoderImpl<BooleanType>, public BooleanDecoder {
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
  std::unique_ptr<::arrow::bit_util::BitReader> bit_reader_;
  int total_num_values_{0};
};

PlainBooleanDecoder::PlainBooleanDecoder(const ColumnDescriptor* descr)
    : TypedDecoderImpl<BooleanType>(descr, Encoding::PLAIN) {}

void PlainBooleanDecoder::SetData(int num_values, const uint8_t* data, int len) {
  DecoderImpl::SetData(num_values, data, len);
  total_num_values_ = num_values;
  bit_reader_ = std::make_unique<bit_util::BitReader>(data, len);
}

int PlainBooleanDecoder::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::Accumulator* builder) {
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(num_values_ < values_decoded)) {
    // A too large `num_values` was requested.
    ParquetException::EofException(
        "A too large `num_values` was requested in PlainBooleanDecoder: remain " +
        std::to_string(num_values_) + ", requested: " + std::to_string(values_decoded));
  }
  if (ARROW_PREDICT_FALSE(!bit_reader_->Advance(values_decoded))) {
    ParquetException::EofException("PlainDecoder doesn't have enough values in page");
  }

  if (null_count == 0) {
    // FastPath: can copy the data directly
    PARQUET_THROW_NOT_OK(builder->AppendValues(data_, values_decoded, NULLPTR,
                                               total_num_values_ - num_values_));
  } else {
    // Handle nulls by BitBlockCounter
    PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
    BitBlockCounter bit_counter(valid_bits, valid_bits_offset, num_values);
    int64_t value_position = 0;
    int64_t valid_bits_offset_position = valid_bits_offset;
    int64_t previous_value_offset = total_num_values_ - num_values_;
    while (value_position < num_values) {
      auto block = bit_counter.NextWord();
      if (block.AllSet()) {
        // GH-40978: We don't have UnsafeAppendValues for booleans currently,
        // so using `AppendValues` here.
        PARQUET_THROW_NOT_OK(
            builder->AppendValues(data_, block.length, NULLPTR, previous_value_offset));
        previous_value_offset += block.length;
      } else if (block.NoneSet()) {
        // GH-40978: We don't have UnsafeAppendNulls for booleans currently,
        // so using `AppendNulls` here.
        PARQUET_THROW_NOT_OK(builder->AppendNulls(block.length));
      } else {
        for (int64_t i = 0; i < block.length; ++i) {
          if (bit_util::GetBit(valid_bits, valid_bits_offset_position + i)) {
            bool value = bit_util::GetBit(data_, previous_value_offset);
            builder->UnsafeAppend(value);
            previous_value_offset += 1;
          } else {
            builder->UnsafeAppendNull();
          }
        }
      }
      value_position += block.length;
      valid_bits_offset_position += block.length;
    }
  }

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
  if (ARROW_PREDICT_FALSE(!bit_reader_->Advance(max_values))) {
    ParquetException::EofException();
  }
  // Copy the data directly
  // Parquet's boolean encoding is bit-packed using LSB. So
  // we can directly copy the data to the buffer.
  ::arrow::internal::CopyBitmap(this->data_, /*offset=*/total_num_values_ - num_values_,
                                /*length=*/max_values, /*dest=*/buffer,
                                /*dest_offset=*/0);
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

// PLAIN decoder implementation for FIXED_LEN_BYTE_ARRAY and BYTE_ARRAY

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
  const int byte_width = this->type_length_;
  const int values_decoded = num_values - null_count;
  CheckPageLargeEnough(len_, byte_width, values_decoded);

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  // 1. Copy directly into the FixedSizeBinary data buffer, packed to the right.
  uint8_t* decode_out = builder->GetMutableValue(builder->length() + null_count);
  memcpy(decode_out, data_, values_decoded * byte_width);

  // 2. Expand the values into their final positions.
  if (null_count == 0) {
    // No expansion required, and no need to append the bitmap
    builder->UnsafeAdvance(num_values);
  } else {
    ::arrow::util::internal::SpacedExpandLeftward(
        builder->GetMutableValue(builder->length()), byte_width, num_values, null_count,
        valid_bits, valid_bits_offset);
    builder->UnsafeAdvance(num_values, valid_bits, valid_bits_offset);
  }
  data_ += byte_width * values_decoded;
  num_values_ -= values_decoded;
  len_ -= byte_width * values_decoded;
  return values_decoded;
}

template <>
inline int PlainDecoder<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::DictAccumulator* builder) {
  const int byte_width = this->type_length_;
  const int values_decoded = num_values - null_count;
  CheckPageLargeEnough(len_, byte_width, values_decoded);

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));
  PARQUET_THROW_NOT_OK(
      VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                   [&](int64_t position, int64_t run_length, bool is_valid) {
                     if (is_valid) {
                       for (int64_t i = 0; i < run_length; ++i) {
                         RETURN_NOT_OK(builder->Append(data_));
                       }
                       data_ += run_length * byte_width;
                     } else {
                       RETURN_NOT_OK(builder->AppendNulls(run_length));
                     }
                     return Status::OK();
                   }));

  num_values_ -= values_decoded;
  len_ -= byte_width * values_decoded;
  return values_decoded;
}

class PlainByteArrayDecoder : public PlainDecoder<ByteArrayType> {
 public:
  using Base = PlainDecoder<ByteArrayType>;
  using Base::DecodeSpaced;
  using Base::PlainDecoder;

  // ----------------------------------------------------------------------
  // Dictionary read paths

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  ::arrow::BinaryDictionary32Builder* builder) override {
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
    auto visit_binary_helper = [&](auto* helper) {
      int values_decoded = 0;

      RETURN_NOT_OK(VisitBitRuns(
          valid_bits, valid_bits_offset, num_values,
          [&](int64_t position, int64_t run_length, bool is_valid) {
            if (is_valid) {
              for (int64_t i = 0; i < run_length; ++i) {
                if (ARROW_PREDICT_FALSE(len_ < 4)) {
                  return Status::Invalid(
                      "Invalid or truncated PLAIN-encoded BYTE_ARRAY data");
                }
                auto value_len = SafeLoadAs<int32_t>(data_);
                if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > len_ - 4)) {
                  return Status::Invalid(
                      "Invalid or truncated PLAIN-encoded BYTE_ARRAY data");
                }
                RETURN_NOT_OK(
                    helper->AppendValue(data_ + 4, value_len,
                                        /*estimated_remaining_data_length=*/len_));
                auto increment = value_len + 4;
                data_ += increment;
                len_ -= increment;
              }
              values_decoded += static_cast<int>(run_length);
              return Status::OK();
            } else {
              return helper->AppendNulls(run_length);
            }
          }));

      num_values_ -= values_decoded;
      *out_values_decoded = values_decoded;
      return Status::OK();
    };

    return DispatchArrowBinaryHelper<ByteArrayType>(out, num_values, len_,
                                                    visit_binary_helper);
  }

  template <typename BuilderType>
  Status DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset, BuilderType* builder,
                     int* out_values_decoded) {
    RETURN_NOT_OK(builder->Reserve(num_values));
    int values_decoded = 0;

    RETURN_NOT_OK(VisitBitRuns(
        valid_bits, valid_bits_offset, num_values,
        [&](int64_t position, int64_t run_length, bool is_valid) {
          if (is_valid) {
            for (int64_t i = 0; i < run_length; ++i) {
              if (ARROW_PREDICT_FALSE(len_ < 4)) {
                return Status::Invalid(
                    "Invalid or truncated PLAIN-encoded BYTE_ARRAY data");
              }
              auto value_len = SafeLoadAs<int32_t>(data_);
              if (ARROW_PREDICT_FALSE(value_len < 0 || value_len > len_ - 4)) {
                return Status::Invalid(
                    "Invalid or truncated PLAIN-encoded BYTE_ARRAY data");
              }
              RETURN_NOT_OK(builder->Append(data_ + 4, value_len));
              auto increment = value_len + 4;
              data_ += increment;
              len_ -= increment;
            }
            values_decoded += static_cast<int>(run_length);
            return Status::OK();
          } else {
            return builder->AppendNulls(run_length);
          }
        }));

    num_values_ -= values_decoded;
    *out_values_decoded = values_decoded;
    return Status::OK();
  }
};

class PlainFLBADecoder : public PlainDecoder<FLBAType>, public FLBADecoder {
 public:
  using Base = PlainDecoder<FLBAType>;
  using Base::PlainDecoder;
};

// ----------------------------------------------------------------------
// Dictionary decoding

template <typename Type>
class DictDecoderImpl : public TypedDecoderImpl<Type>, public DictDecoder<Type> {
 public:
  typedef typename Type::c_type T;

  // Initializes the dictionary with values from 'dictionary'. The data in
  // dictionary is not guaranteed to persist in memory after this call so the
  // dictionary decoder needs to copy the data out if necessary.
  explicit DictDecoderImpl(const ColumnDescriptor* descr,
                           MemoryPool* pool = ::arrow::default_memory_pool())
      : TypedDecoderImpl<Type>(descr, Encoding::RLE_DICTIONARY),
        dictionary_(AllocateBuffer(pool, 0)),
        dictionary_length_(0),
        byte_array_data_(AllocateBuffer(pool, 0)),
        byte_array_offsets_(AllocateBuffer(pool, 0)),
        indices_scratch_space_(AllocateBuffer(pool, 0)) {}

  // Perform type-specific initialization
  void SetDict(TypedDecoder<Type>* dictionary) override;

  void SetData(int num_values, const uint8_t* data, int len) override {
    this->num_values_ = num_values;
    if (len == 0) {
      // Initialize dummy decoder to avoid crashes later on
      idx_decoder_ = ::arrow::util::RleDecoder(data, len, /*bit_width=*/1);
      return;
    }
    uint8_t bit_width = *data;
    if (ARROW_PREDICT_FALSE(bit_width > 32)) {
      throw ParquetException("Invalid or corrupted bit_width " +
                             std::to_string(bit_width) + ". Maximum allowed is 32.");
    }
    idx_decoder_ = ::arrow::util::RleDecoder(++data, --len, bit_width);
  }

  int Decode(T* buffer, int num_values) override {
    num_values = std::min(num_values, this->num_values_);
    int decoded_values = idx_decoder_.GetBatchWithDict(
        dictionary_->data_as<T>(), dictionary_length_, buffer, num_values);
    if (decoded_values != num_values) {
      ParquetException::EofException();
    }
    this->num_values_ -= num_values;
    return num_values;
  }

  int DecodeSpaced(T* buffer, int num_values, int null_count, const uint8_t* valid_bits,
                   int64_t valid_bits_offset) override {
    num_values = std::min(num_values, this->num_values_);
    if (num_values != idx_decoder_.GetBatchWithDictSpaced(
                          dictionary_->data_as<T>(), dictionary_length_, buffer,
                          num_values, null_count, valid_bits, valid_bits_offset)) {
      ParquetException::EofException();
    }
    this->num_values_ -= num_values;
    return num_values;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<Type>::Accumulator* out) override;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<Type>::DictAccumulator* out) override;

  void InsertDictionary(::arrow::ArrayBuilder* builder) override;

  int DecodeIndicesSpaced(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          ::arrow::ArrayBuilder* builder) override {
    if (num_values > 0) {
      // TODO(wesm): Refactor to batch reads for improved memory use. It is not
      // trivial because the null_count is relative to the entire bitmap
      PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
          num_values, /*shrink_to_fit=*/false));
    }

    auto indices_buffer = indices_scratch_space_->mutable_data_as<int32_t>();

    if (num_values != idx_decoder_.GetBatchSpaced(num_values, null_count, valid_bits,
                                                  valid_bits_offset, indices_buffer)) {
      ParquetException::EofException();
    }

    // XXX(wesm): Cannot append "valid bits" directly to the builder
    std::vector<uint8_t> valid_bytes(num_values, 0);
    size_t i = 0;
    VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() { valid_bytes[i++] = 1; }, [&]() { ++i; });

    auto binary_builder = checked_cast<::arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(
        binary_builder->AppendIndices(indices_buffer, num_values, valid_bytes.data()));
    this->num_values_ -= num_values - null_count;
    return num_values - null_count;
  }

  int DecodeIndices(int num_values, ::arrow::ArrayBuilder* builder) override {
    num_values = std::min(num_values, this->num_values_);
    if (num_values > 0) {
      // TODO(wesm): Refactor to batch reads for improved memory use. This is
      // relatively simple here because we don't have to do any bookkeeping of
      // nulls
      PARQUET_THROW_NOT_OK(indices_scratch_space_->TypedResize<int32_t>(
          num_values, /*shrink_to_fit=*/false));
    }
    auto indices_buffer = indices_scratch_space_->mutable_data_as<int32_t>();
    if (num_values != idx_decoder_.GetBatch(indices_buffer, num_values)) {
      ParquetException::EofException();
    }
    auto binary_builder = checked_cast<::arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(binary_builder->AppendIndices(indices_buffer, num_values));
    this->num_values_ -= num_values;
    return num_values;
  }

  int DecodeIndices(int num_values, int32_t* indices) override {
    if (num_values != idx_decoder_.GetBatch(indices, num_values)) {
      ParquetException::EofException();
    }
    this->num_values_ -= num_values;
    return num_values;
  }

  void GetDictionary(const T** dictionary, int32_t* dictionary_length) override {
    *dictionary_length = dictionary_length_;
    *dictionary = dictionary_->mutable_data_as<T>();
  }

 protected:
  Status IndexInBounds(int32_t index) const {
    if (ARROW_PREDICT_TRUE(0 <= index && index < dictionary_length_)) {
      return Status::OK();
    }
    return Status::Invalid("Index not in dictionary bounds");
  }

  inline void DecodeDict(TypedDecoder<Type>* dictionary) {
    dictionary_length_ = static_cast<int32_t>(dictionary->values_left());
    PARQUET_THROW_NOT_OK(dictionary_->Resize(dictionary_length_ * sizeof(T),
                                             /*shrink_to_fit=*/false));
    dictionary->Decode(dictionary_->mutable_data_as<T>(), dictionary_length_);
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

  ::arrow::util::RleDecoder idx_decoder_;
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

  auto* dict_values = dictionary_->mutable_data_as<ByteArray>();

  int total_size = 0;
  for (int i = 0; i < dictionary_length_; ++i) {
    total_size += dict_values[i].len;
  }
  PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size,
                                                /*shrink_to_fit=*/false));
  PARQUET_THROW_NOT_OK(
      byte_array_offsets_->Resize((dictionary_length_ + 1) * sizeof(int32_t),
                                  /*shrink_to_fit=*/false));

  int32_t offset = 0;
  uint8_t* bytes_data = byte_array_data_->mutable_data();
  int32_t* bytes_offsets = byte_array_offsets_->mutable_data_as<int32_t>();
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

  auto* dict_values = dictionary_->mutable_data_as<FLBA>();

  int fixed_len = this->type_length_;
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

  const auto* dict_values = dictionary_->data_as<typename DType::c_type>();

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        int32_t index;
        if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
          throw ParquetException("");
        }
        PARQUET_THROW_NOT_OK(IndexInBounds(index));
        PARQUET_THROW_NOT_OK(builder->Append(dict_values[index]));
      },
      [&]() { PARQUET_THROW_NOT_OK(builder->AppendNull()); });

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
  if (builder->byte_width() != this->type_length_) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(builder->byte_width()) + " but decoder was " +
                           std::to_string(this->type_length_));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  const auto* dict_values = dictionary_->data_as<FLBA>();
  PARQUET_THROW_NOT_OK(
      VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                   [&](int64_t position, int64_t run_length, bool is_valid) {
                     if (is_valid) {
                       for (int64_t i = 0; i < run_length; ++i) {
                         int32_t index;
                         if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                           return Status::Invalid("Dict decoding failed");
                         }
                         RETURN_NOT_OK(IndexInBounds(index));
                         builder->UnsafeAppend(dict_values[index].ptr);
                       }
                       return Status::OK();
                     } else {
                       return builder->AppendNulls(run_length);
                     }
                   }));

  return num_values - null_count;
}

template <>
int DictDecoderImpl<FLBAType>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<FLBAType>::DictAccumulator* builder) {
  auto value_type =
      checked_cast<const ::arrow::DictionaryType&>(*builder->type()).value_type();
  auto byte_width =
      checked_cast<const ::arrow::FixedSizeBinaryType&>(*value_type).byte_width();
  if (byte_width != this->type_length_) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(byte_width) + " but decoder was " +
                           std::to_string(this->type_length_));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  const auto* dict_values = dictionary_->data_as<FLBA>();

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        int32_t index;
        if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
          throw ParquetException("");
        }
        PARQUET_THROW_NOT_OK(IndexInBounds(index));
        PARQUET_THROW_NOT_OK(builder->Append(dict_values[index].ptr));
      },
      [&]() { PARQUET_THROW_NOT_OK(builder->AppendNull()); });

  return num_values - null_count;
}

template <typename Type>
int DictDecoderImpl<Type>::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<Type>::Accumulator* builder) {
  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  using value_type = typename Type::c_type;
  const auto* dict_values = dictionary_->data_as<value_type>();

  PARQUET_THROW_NOT_OK(
      VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                   [&](int64_t position, int64_t run_length, bool is_valid) {
                     if (is_valid) {
                       for (int64_t i = 0; i < run_length; ++i) {
                         int32_t index;
                         if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
                           return Status::Invalid("Dict decoding failed");
                         }
                         RETURN_NOT_OK(IndexInBounds(index));
                         builder->UnsafeAppend(dict_values[index]);
                       }
                       return Status::OK();
                     } else {
                       return builder->AppendNulls(run_length);
                     }
                   }));

  return num_values - null_count;
}

template <typename Type>
void DictDecoderImpl<Type>::InsertDictionary(::arrow::ArrayBuilder* builder) {
  ParquetException::NYI("InsertDictionary only implemented for BYTE_ARRAY types");
}

template <>
void DictDecoderImpl<ByteArrayType>::InsertDictionary(::arrow::ArrayBuilder* builder) {
  auto binary_builder = checked_cast<::arrow::BinaryDictionary32Builder*>(builder);

  // Make a BinaryArray referencing the internal dictionary data
  auto arr = std::make_shared<::arrow::BinaryArray>(
      dictionary_length_, byte_array_offsets_, byte_array_data_);
  PARQUET_THROW_NOT_OK(binary_builder->InsertMemoValues(*arr));
}

class DictByteArrayDecoderImpl : public DictDecoderImpl<ByteArrayType> {
 public:
  using BASE = DictDecoderImpl<ByteArrayType>;
  using BASE::DictDecoderImpl;

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  ::arrow::BinaryDictionary32Builder* builder) override {
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
      PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count,
                                            /*valid_bits=*/nullptr, valid_bits_offset,
                                            out, &result));
    } else {
      PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                            valid_bits_offset, out, &result));
    }
    return result;
  }

 private:
  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_num_values) {
    constexpr int32_t kBufferSize = 1024;
    int32_t indices[kBufferSize];

    auto visit_binary_helper = [&](auto* helper) {
      const auto* dict_values = dictionary_->data_as<ByteArray>();
      const int values_to_decode = num_values - null_count;
      int values_decoded = 0;
      int num_indices = 0;
      int pos_indices = 0;

      auto visit_bit_run = [&](int64_t position, int64_t length, bool valid) {
        if (valid) {
          while (length > 0) {
            if (num_indices == pos_indices) {
              // Refill indices buffer
              const auto max_batch_size =
                  std::min<int32_t>(kBufferSize, values_to_decode - values_decoded);
              num_indices = idx_decoder_.GetBatch(indices, max_batch_size);
              if (ARROW_PREDICT_FALSE(num_indices < 1)) {
                return Status::Invalid("Invalid number of indices: ", num_indices);
              }
              pos_indices = 0;
            }
            const auto batch_size = std::min<int64_t>(num_indices - pos_indices, length);
            for (int64_t j = 0; j < batch_size; ++j) {
              const auto index = indices[pos_indices++];
              RETURN_NOT_OK(IndexInBounds(index));
              const auto& val = dict_values[index];
              RETURN_NOT_OK(helper->AppendValue(val.ptr, static_cast<int32_t>(val.len)));
            }
            values_decoded += static_cast<int32_t>(batch_size);
            length -= static_cast<int32_t>(batch_size);
          }
        } else {
          for (int64_t i = 0; i < length; ++i) {
            helper->UnsafeAppendNull();
          }
        }
        return Status::OK();
      };

      RETURN_NOT_OK(
          VisitBitRuns(valid_bits, valid_bits_offset, num_values, visit_bit_run));
      *out_num_values = values_decoded;
      return Status::OK();
    };
    // The `len_` in the ByteArrayDictDecoder is the total length of the
    // RLE/Bit-pack encoded data size, so, we cannot use `len_` to reserve
    // space for binary data.
    return DispatchArrowBinaryHelper<ByteArrayType>(
        out, num_values, /*estimated_data_length=*/{}, visit_binary_helper);
  }

  template <typename BuilderType>
  Status DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                     int64_t valid_bits_offset, BuilderType* builder,
                     int* out_num_values) {
    constexpr int32_t kBufferSize = 1024;
    int32_t indices[kBufferSize];

    RETURN_NOT_OK(builder->Reserve(num_values));
    ::arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

    const auto* dict_values = dictionary_->data_as<ByteArray>();

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

    const auto* dict_values = dictionary_->data_as<ByteArray>();

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
// DELTA_BINARY_PACKED decoder

template <typename DType>
class DeltaBitPackDecoder : public TypedDecoderImpl<DType> {
 public:
  using T = typename DType::c_type;
  using UT = std::make_unsigned_t<T>;

  explicit DeltaBitPackDecoder(const ColumnDescriptor* descr,
                               MemoryPool* pool = ::arrow::default_memory_pool())
      : TypedDecoderImpl<DType>(descr, Encoding::DELTA_BINARY_PACKED), pool_(pool) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  void SetData(int num_values, const uint8_t* data, int len) override {
    // num_values is equal to page's num_values, including null values in this page
    this->num_values_ = num_values;
    if (decoder_ == nullptr) {
      decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    } else {
      decoder_->Reset(data, len);
    }
    InitHeader();
  }

  // Set BitReader which is already initialized by DeltaLengthByteArrayDecoder or
  // DeltaByteArrayDecoder
  void SetDecoder(int num_values, std::shared_ptr<::arrow::bit_util::BitReader> decoder) {
    this->num_values_ = num_values;
    decoder_ = std::move(decoder);
    InitHeader();
  }

  int ValidValuesCount() {
    // total_values_remaining_ in header ignores of null values
    return static_cast<int>(total_values_remaining_);
  }

  int Decode(T* buffer, int max_values) override {
    return GetInternal(buffer, max_values);
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* out) override {
    if (null_count != 0) {
      // TODO(ARROW-34660): implement DecodeArrow with null slots.
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    int decoded_count = GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->AppendValues(values.data(), decoded_count));
    return decoded_count;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* out) override {
    if (null_count != 0) {
      // TODO(ARROW-34660): implement DecodeArrow with null slots.
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    int decoded_count = GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->Reserve(decoded_count));
    for (int i = 0; i < decoded_count; ++i) {
      PARQUET_THROW_NOT_OK(out->Append(values[i]));
    }
    return decoded_count;
  }

 private:
  static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);

  void InitHeader() {
    if (!decoder_->GetVlqInt(&values_per_block_) ||
        !decoder_->GetVlqInt(&mini_blocks_per_block_) ||
        !decoder_->GetVlqInt(&total_value_count_) ||
        !decoder_->GetZigZagVlqInt(&last_value_)) {
      ParquetException::EofException("InitHeader EOF");
    }

    if (values_per_block_ == 0) {
      throw ParquetException("cannot have zero value per block");
    }
    if (values_per_block_ % 128 != 0) {
      throw ParquetException(
          "the number of values in a block must be multiple of 128, but it's " +
          std::to_string(values_per_block_));
    }
    if (mini_blocks_per_block_ == 0) {
      throw ParquetException("cannot have zero miniblock per block");
    }
    values_per_mini_block_ = values_per_block_ / mini_blocks_per_block_;
    if (values_per_mini_block_ == 0) {
      throw ParquetException("cannot have zero value per miniblock");
    }
    if (values_per_mini_block_ % 32 != 0) {
      throw ParquetException(
          "the number of values in a miniblock must be multiple of 32, but it's " +
          std::to_string(values_per_mini_block_));
    }

    total_values_remaining_ = total_value_count_;
    if (delta_bit_widths_ == nullptr) {
      delta_bit_widths_ = AllocateBuffer(pool_, mini_blocks_per_block_);
    } else {
      PARQUET_THROW_NOT_OK(
          delta_bit_widths_->Resize(mini_blocks_per_block_, /*shrink_to_fit*/ false));
    }
    first_block_initialized_ = false;
    values_remaining_current_mini_block_ = 0;
  }

  void InitBlock() {
    DCHECK_GT(total_values_remaining_, 0) << "InitBlock called at EOF";

    if (!decoder_->GetZigZagVlqInt(&min_delta_))
      ParquetException::EofException("InitBlock EOF");

    // read the bitwidth of each miniblock
    uint8_t* bit_width_data = delta_bit_widths_->mutable_data();
    for (uint32_t i = 0; i < mini_blocks_per_block_; ++i) {
      if (!decoder_->GetAligned<uint8_t>(1, bit_width_data + i)) {
        ParquetException::EofException("Decode bit-width EOF");
      }
      // Note that non-conformant bitwidth entries are allowed by the Parquet spec
      // for extraneous miniblocks in the last block (GH-14923), so we check
      // the bitwidths when actually using them (see InitMiniBlock()).
    }

    mini_block_idx_ = 0;
    first_block_initialized_ = true;
    InitMiniBlock(bit_width_data[0]);
  }

  void InitMiniBlock(int bit_width) {
    if (ARROW_PREDICT_FALSE(bit_width > kMaxDeltaBitWidth)) {
      throw ParquetException("delta bit width larger than integer bit width");
    }
    delta_bit_width_ = bit_width;
    values_remaining_current_mini_block_ = values_per_mini_block_;
  }

  int GetInternal(T* buffer, int max_values) {
    max_values = static_cast<int>(std::min<int64_t>(max_values, total_values_remaining_));
    if (max_values == 0) {
      return 0;
    }

    int i = 0;

    if (ARROW_PREDICT_FALSE(!first_block_initialized_)) {
      // This is the first time we decode this data page, first output the
      // last value and initialize the first block.
      buffer[i++] = last_value_;
      if (ARROW_PREDICT_FALSE(i == max_values)) {
        // When i reaches max_values here we have two different possibilities:
        // 1. total_value_count_ == 1, which means that the page may have only
        //    one value (encoded in the header), and we should not initialize
        //    any block, nor should we skip any padding bits below.
        // 2. total_value_count_ != 1, which means we should initialize the
        //    incoming block for subsequent reads.
        if (total_value_count_ != 1) {
          InitBlock();
        }
        total_values_remaining_ -= max_values;
        this->num_values_ -= max_values;
        return max_values;
      }
      InitBlock();
    }

    DCHECK(first_block_initialized_);
    while (i < max_values) {
      // Ensure we have an initialized mini-block
      if (ARROW_PREDICT_FALSE(values_remaining_current_mini_block_ == 0)) {
        ++mini_block_idx_;
        if (mini_block_idx_ < mini_blocks_per_block_) {
          InitMiniBlock(delta_bit_widths_->data()[mini_block_idx_]);
        } else {
          InitBlock();
        }
      }

      int values_decode = std::min(values_remaining_current_mini_block_,
                                   static_cast<uint32_t>(max_values - i));
      if (decoder_->GetBatch(delta_bit_width_, buffer + i, values_decode) !=
          values_decode) {
        ParquetException::EofException();
      }
      for (int j = 0; j < values_decode; ++j) {
        // Addition between min_delta, packed int and last_value should be treated as
        // unsigned addition. Overflow is as expected.
        buffer[i + j] = static_cast<UT>(min_delta_) + static_cast<UT>(buffer[i + j]) +
                        static_cast<UT>(last_value_);
        last_value_ = buffer[i + j];
      }
      values_remaining_current_mini_block_ -= values_decode;
      i += values_decode;
    }
    total_values_remaining_ -= max_values;
    this->num_values_ -= max_values;

    if (ARROW_PREDICT_FALSE(total_values_remaining_ == 0)) {
      uint32_t padding_bits = values_remaining_current_mini_block_ * delta_bit_width_;
      // skip the padding bits
      if (!decoder_->Advance(padding_bits)) {
        ParquetException::EofException();
      }
      values_remaining_current_mini_block_ = 0;
    }
    return max_values;
  }

  MemoryPool* pool_;
  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  uint32_t values_per_block_;
  uint32_t mini_blocks_per_block_;
  uint32_t values_per_mini_block_;
  uint32_t total_value_count_;

  uint32_t total_values_remaining_;
  // Remaining values in current mini block. If the current block is the last mini block,
  // values_remaining_current_mini_block_ may greater than total_values_remaining_.
  uint32_t values_remaining_current_mini_block_;

  // If the page doesn't contain any block, `first_block_initialized_` will
  // always be false. Otherwise, it will be true when first block initialized.
  bool first_block_initialized_;
  T min_delta_;
  uint32_t mini_block_idx_;
  std::shared_ptr<ResizableBuffer> delta_bit_widths_;
  int delta_bit_width_;

  T last_value_;
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY decoder

class DeltaLengthByteArrayDecoder : public TypedDecoderImpl<ByteArrayType> {
 public:
  using Base = TypedDecoderImpl<ByteArrayType>;

  explicit DeltaLengthByteArrayDecoder(const ColumnDescriptor* descr,
                                       MemoryPool* pool = ::arrow::default_memory_pool())
      : Base(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY),
        len_decoder_(nullptr, pool),
        buffered_length_(AllocateBuffer(pool, 0)) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    Base::SetData(num_values, data, len);
    if (decoder_ == nullptr) {
      decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    } else {
      decoder_->Reset(data, len);
    }
    DecodeLengths();
  }

  int Decode(ByteArray* buffer, int max_values) override {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, num_valid_values_);
    DCHECK_GE(max_values, 0);
    if (max_values == 0) {
      return 0;
    }

    int32_t data_size = 0;
    const int32_t* length_ptr = buffered_length_->data_as<int32_t>() + length_idx_;
    int bytes_offset = len_ - decoder_->bytes_left();
    for (int i = 0; i < max_values; ++i) {
      int32_t len = length_ptr[i];
      if (ARROW_PREDICT_FALSE(len < 0)) {
        throw ParquetException("negative string delta length");
      }
      buffer[i].len = len;
      if (AddWithOverflow(data_size, len, &data_size)) {
        throw ParquetException("excess expansion in DELTA_(LENGTH_)BYTE_ARRAY");
      }
    }
    length_idx_ += max_values;
    if (ARROW_PREDICT_FALSE(!decoder_->Advance(8 * static_cast<int64_t>(data_size)))) {
      ParquetException::EofException();
    }
    const uint8_t* data_ptr = data_ + bytes_offset;
    for (int i = 0; i < max_values; ++i) {
      buffer[i].ptr = data_ptr;
      data_ptr += buffer[i].len;
    }
    this->num_values_ -= max_values;
    num_valid_values_ -= max_values;
    return max_values;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::Accumulator* out) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                          valid_bits_offset, out, &result));
    return result;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::DictAccumulator* out) override {
    ParquetException::NYI(
        "DecodeArrow of DictAccumulator for DeltaLengthByteArrayDecoder");
  }

 private:
  // Decode all the encoded lengths. The decoder_ will be at the start of the encoded data
  // after that.
  void DecodeLengths() {
    len_decoder_.SetDecoder(num_values_, decoder_);

    // get the number of encoded lengths
    int num_length = len_decoder_.ValidValuesCount();
    PARQUET_THROW_NOT_OK(buffered_length_->Resize(num_length * sizeof(int32_t)));

    // call len_decoder_.Decode to decode all the lengths.
    // all the lengths are buffered in buffered_length_.
    int ret =
        len_decoder_.Decode(buffered_length_->mutable_data_as<int32_t>(), num_length);
    DCHECK_EQ(ret, num_length);
    length_idx_ = 0;
    num_valid_values_ = num_length;
  }

  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_num_values) {
    std::vector<ByteArray> values(num_values - null_count);
    const int num_valid_values = Decode(values.data(), num_values - null_count);
    if (ARROW_PREDICT_FALSE(num_values - null_count != num_valid_values)) {
      throw ParquetException("Expected to decode ", num_values - null_count,
                             " values, but decoded ", num_valid_values, " values.");
    }

    auto visit_binary_helper = [&](auto* helper) {
      auto values_ptr = values.data();
      int value_idx = 0;

      RETURN_NOT_OK(
          VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                       [&](int64_t position, int64_t run_length, bool is_valid) {
                         if (is_valid) {
                           for (int64_t i = 0; i < run_length; ++i) {
                             const auto& val = values_ptr[value_idx];
                             RETURN_NOT_OK(helper->AppendValue(
                                 val.ptr, static_cast<int32_t>(val.len)));
                             ++value_idx;
                           }
                           return Status::OK();
                         } else {
                           return helper->AppendNulls(run_length);
                         }
                       }));
      *out_num_values = num_valid_values;
      return Status::OK();
    };
    return DispatchArrowBinaryHelper<ByteArrayType>(
        out, num_values, /*estimated_data_length=*/{}, visit_binary_helper);
  }

  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  DeltaBitPackDecoder<Int32Type> len_decoder_;
  int num_valid_values_{0};
  uint32_t length_idx_{0};
  std::shared_ptr<ResizableBuffer> buffered_length_;
};

// ----------------------------------------------------------------------
// RLE decoder for BOOLEAN

class RleBooleanDecoder : public TypedDecoderImpl<BooleanType>, public BooleanDecoder {
 public:
  explicit RleBooleanDecoder(const ColumnDescriptor* descr)
      : TypedDecoderImpl<BooleanType>(descr, Encoding::RLE) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    uint32_t num_bytes = 0;

    if (len < 4) {
      throw ParquetException("Received invalid length : " + std::to_string(len) +
                             " (corrupt data page?)");
    }
    // Load the first 4 bytes in little-endian, which indicates the length
    num_bytes = ::arrow::bit_util::FromLittleEndian(SafeLoadAs<uint32_t>(data));
    if (num_bytes < 0 || num_bytes > static_cast<uint32_t>(len - 4)) {
      throw ParquetException("Received invalid number of bytes : " +
                             std::to_string(num_bytes) + " (corrupt data page?)");
    }

    auto decoder_data = data + 4;
    if (decoder_ == nullptr) {
      decoder_ = std::make_shared<::arrow::util::RleDecoder>(decoder_data, num_bytes,
                                                             /*bit_width=*/1);
    } else {
      decoder_->Reset(decoder_data, num_bytes, /*bit_width=*/1);
    }
  }

  int Decode(bool* buffer, int max_values) override {
    max_values = std::min(max_values, num_values_);

    if (decoder_->GetBatch(buffer, max_values) != max_values) {
      ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

  int Decode(uint8_t* buffer, int max_values) override {
    ParquetException::NYI("Decode(uint8_t*, int) for RleBooleanDecoder");
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<BooleanType>::Accumulator* out) override {
    if (null_count == num_values) {
      PARQUET_THROW_NOT_OK(out->AppendNulls(null_count));
      return 0;
    }
    constexpr int kBatchSize = 1024;
    std::array<bool, kBatchSize> values;
    const int num_non_null_values = num_values - null_count;
    // Remaining non-null boolean values to read from decoder.
    // We decode from `decoder_` with maximum 1024 size batches.
    int num_remain_non_null_values = num_non_null_values;
    int current_index_in_batch = 0;
    int current_batch_size = 0;
    auto next_boolean_batch = [&]() {
      DCHECK_GT(num_remain_non_null_values, 0);
      DCHECK_EQ(current_index_in_batch, current_batch_size);
      current_batch_size = std::min(num_remain_non_null_values, kBatchSize);
      int decoded_count = decoder_->GetBatch(values.data(), current_batch_size);
      if (ARROW_PREDICT_FALSE(decoded_count != current_batch_size)) {
        // required values is more than values in decoder.
        ParquetException::EofException();
      }
      num_remain_non_null_values -= current_batch_size;
      current_index_in_batch = 0;
    };

    // Reserve all values including nulls first
    PARQUET_THROW_NOT_OK(out->Reserve(num_values));
    if (null_count == 0) {
      // Fast-path for not having nulls.
      do {
        next_boolean_batch();
        PARQUET_THROW_NOT_OK(
            out->AppendValues(values.begin(), values.begin() + current_batch_size));
        num_values -= current_batch_size;
        // set current_index_in_batch to current_batch_size means
        // the whole batch is totally consumed.
        current_index_in_batch = current_batch_size;
      } while (num_values > 0);
      return num_non_null_values;
    }
    auto next_value = [&]() -> bool {
      if (current_index_in_batch == current_batch_size) {
        next_boolean_batch();
        DCHECK_GT(current_batch_size, 0);
      }
      DCHECK_LT(current_index_in_batch, current_batch_size);
      bool value = values[current_index_in_batch];
      ++current_index_in_batch;
      return value;
    };
    VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() { out->UnsafeAppend(next_value()); }, [&]() { out->UnsafeAppendNull(); });
    return num_non_null_values;
  }

  int DecodeArrow(
      int num_values, int null_count, const uint8_t* valid_bits,
      int64_t valid_bits_offset,
      typename EncodingTraits<BooleanType>::DictAccumulator* builder) override {
    ParquetException::NYI("DecodeArrow for RleBooleanDecoder");
  }

 private:
  std::shared_ptr<::arrow::util::RleDecoder> decoder_;
};

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY decoder

template <typename DType>
class DeltaByteArrayDecoderImpl : public TypedDecoderImpl<DType> {
  using T = typename DType::c_type;

 public:
  explicit DeltaByteArrayDecoderImpl(const ColumnDescriptor* descr,
                                     MemoryPool* pool = ::arrow::default_memory_pool())
      : TypedDecoderImpl<DType>(descr, Encoding::DELTA_BYTE_ARRAY),
        pool_(pool),
        prefix_len_decoder_(nullptr, pool),
        suffix_decoder_(nullptr, pool),
        last_value_in_previous_page_(""),
        buffered_prefix_length_(AllocateBuffer(pool, 0)),
        buffered_data_(AllocateBuffer(pool, 0)) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    this->num_values_ = num_values;
    if (decoder_) {
      decoder_->Reset(data, len);
    } else {
      decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    }
    prefix_len_decoder_.SetDecoder(num_values, decoder_);

    // get the number of encoded prefix lengths
    int num_prefix = prefix_len_decoder_.ValidValuesCount();
    // call prefix_len_decoder_.Decode to decode all the prefix lengths.
    // all the prefix lengths are buffered in buffered_prefix_length_.
    PARQUET_THROW_NOT_OK(buffered_prefix_length_->Resize(num_prefix * sizeof(int32_t)));
    int ret = prefix_len_decoder_.Decode(
        buffered_prefix_length_->mutable_data_as<int32_t>(), num_prefix);
    DCHECK_EQ(ret, num_prefix);
    prefix_len_offset_ = 0;
    num_valid_values_ = num_prefix;

    int bytes_left = decoder_->bytes_left();
    // If len < bytes_left, prefix_len_decoder.Decode will throw exception.
    DCHECK_GE(len, bytes_left);
    int suffix_begins = len - bytes_left;
    // at this time, the decoder_ will be at the start of the encoded suffix data.
    suffix_decoder_.SetData(num_values, data + suffix_begins, bytes_left);

    // TODO: read corrupted files written with bug(PARQUET-246). last_value_ should be set
    // to last_value_in_previous_page_ when decoding a new page(except the first page)
    last_value_.clear();
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* out) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                          valid_bits_offset, out, &result));
    return result;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* builder) override {
    ParquetException::NYI("DecodeArrow of DictAccumulator for DeltaByteArrayDecoder");
  }

 protected:
  template <bool is_first_run>
  static void BuildBufferInternal(const int32_t* prefix_len_ptr, int i, ByteArray* buffer,
                                  std::string_view* prefix, uint8_t** data_ptr) {
    if (ARROW_PREDICT_FALSE(static_cast<size_t>(prefix_len_ptr[i]) > prefix->length())) {
      throw ParquetException("prefix length too large in DELTA_BYTE_ARRAY");
    }
    // For now, `buffer` points to string suffixes, and the suffix decoder
    // ensures that the suffix data has sufficient lifetime.
    if (prefix_len_ptr[i] == 0) {
      // prefix is empty: buffer[i] already points to the suffix.
      *prefix = std::string_view{buffer[i]};
      return;
    }
    DCHECK_EQ(is_first_run, i == 0);
    if constexpr (!is_first_run) {
      if (buffer[i].len == 0) {
        // suffix is empty: buffer[i] can simply point to the prefix.
        // This is not possible for the first run since the prefix
        // would point to the mutable `last_value_`.
        *prefix = prefix->substr(0, prefix_len_ptr[i]);
        buffer[i] = ByteArray(*prefix);
        return;
      }
    }
    // Both prefix and suffix are non-empty, so we need to decode the string
    // into `data_ptr`.
    // 1. Copy the prefix
    memcpy(*data_ptr, prefix->data(), prefix_len_ptr[i]);
    // 2. Copy the suffix.
    memcpy(*data_ptr + prefix_len_ptr[i], buffer[i].ptr, buffer[i].len);
    // 3. Point buffer[i] to the decoded string.
    buffer[i].ptr = *data_ptr;
    buffer[i].len += prefix_len_ptr[i];
    *data_ptr += buffer[i].len;
    *prefix = std::string_view{buffer[i]};
  }

  int GetInternal(ByteArray* buffer, int max_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, num_valid_values_);
    if (max_values == 0) {
      return max_values;
    }

    int suffix_read = suffix_decoder_.Decode(buffer, max_values);
    if (ARROW_PREDICT_FALSE(suffix_read != max_values)) {
      ParquetException::EofException("Read " + std::to_string(suffix_read) +
                                     ", expecting " + std::to_string(max_values) +
                                     " from suffix decoder");
    }

    int64_t data_size = 0;
    const int32_t* prefix_len_ptr =
        buffered_prefix_length_->data_as<int32_t>() + prefix_len_offset_;
    for (int i = 0; i < max_values; ++i) {
      if (prefix_len_ptr[i] == 0) {
        // We don't need to copy the suffix if the prefix length is 0.
        continue;
      }
      if (ARROW_PREDICT_FALSE(prefix_len_ptr[i] < 0)) {
        throw ParquetException("negative prefix length in DELTA_BYTE_ARRAY");
      }
      if (buffer[i].len == 0 && i != 0) {
        // We don't need to copy the prefix if the suffix length is 0
        // and this is not the first run (that is, the prefix doesn't point
        // to the mutable `last_value_`).
        continue;
      }
      if (ARROW_PREDICT_FALSE(AddWithOverflow(data_size, prefix_len_ptr[i], &data_size) ||
                              AddWithOverflow(data_size, buffer[i].len, &data_size))) {
        throw ParquetException("excess expansion in DELTA_BYTE_ARRAY");
      }
    }
    PARQUET_THROW_NOT_OK(buffered_data_->Resize(data_size));

    std::string_view prefix{last_value_};
    uint8_t* data_ptr = buffered_data_->mutable_data();
    if (max_values > 0) {
      BuildBufferInternal</*is_first_run=*/true>(prefix_len_ptr, 0, buffer, &prefix,
                                                 &data_ptr);
    }
    for (int i = 1; i < max_values; ++i) {
      BuildBufferInternal</*is_first_run=*/false>(prefix_len_ptr, i, buffer, &prefix,
                                                  &data_ptr);
    }
    DCHECK_EQ(data_ptr - buffered_data_->mutable_data(), data_size);
    prefix_len_offset_ += max_values;
    this->num_values_ -= max_values;
    num_valid_values_ -= max_values;
    last_value_ = std::string{prefix};

    if (num_valid_values_ == 0) {
      last_value_in_previous_page_ = last_value_;
    }
    return max_values;
  }

  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<DType>::Accumulator* out,
                          int* out_num_values) {
    std::vector<ByteArray> values(num_values);
    const int num_valid_values = GetInternal(values.data(), num_values - null_count);
    DCHECK_EQ(num_values - null_count, num_valid_values);

    auto visit_binary_helper = [&](auto* helper) {
      auto values_ptr = reinterpret_cast<const ByteArray*>(values.data());
      int value_idx = 0;

      PARQUET_THROW_NOT_OK(
          VisitBitRuns(valid_bits, valid_bits_offset, num_values,
                       [&](int64_t position, int64_t run_length, bool is_valid) {
                         if (is_valid) {
                           for (int64_t i = 0; i < run_length; ++i) {
                             const auto& val = values_ptr[value_idx];
                             RETURN_NOT_OK(helper->AppendValue(
                                 val.ptr, static_cast<int32_t>(val.len)));
                             ++value_idx;
                           }
                           return Status::OK();
                         } else {
                           return helper->AppendNulls(run_length);
                         }
                       }));

      *out_num_values = num_valid_values;
      return Status::OK();
    };
    return DispatchArrowBinaryHelper<DType>(out, num_values, /*estimated_data_length=*/{},
                                            visit_binary_helper);
  }

  MemoryPool* pool_;

 private:
  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  std::string last_value_;
  // string buffer for last value in previous page
  std::string last_value_in_previous_page_;
  int num_valid_values_{0};
  uint32_t prefix_len_offset_{0};
  std::shared_ptr<ResizableBuffer> buffered_prefix_length_;
  // buffer for decoded strings, which gurantees the lifetime of the decoded strings
  // until the next call of Decode.
  std::shared_ptr<ResizableBuffer> buffered_data_;
};

class DeltaByteArrayDecoder : public DeltaByteArrayDecoderImpl<ByteArrayType> {
 public:
  using Base = DeltaByteArrayDecoderImpl<ByteArrayType>;
  using Base::DeltaByteArrayDecoderImpl;

  int Decode(ByteArray* buffer, int max_values) override {
    return GetInternal(buffer, max_values);
  }
};

class DeltaByteArrayFLBADecoder : public DeltaByteArrayDecoderImpl<FLBAType>,
                                  public FLBADecoder {
 public:
  using Base = DeltaByteArrayDecoderImpl<FLBAType>;
  using Base::DeltaByteArrayDecoderImpl;
  using Base::pool_;

  int Decode(FixedLenByteArray* buffer, int max_values) override {
    // GetInternal currently only support ByteArray.
    std::vector<ByteArray> decode_byte_array(max_values);
    const int decoded_values_size = GetInternal(decode_byte_array.data(), max_values);
    const uint32_t type_length = static_cast<uint32_t>(this->type_length_);

    for (int i = 0; i < decoded_values_size; i++) {
      if (ARROW_PREDICT_FALSE(decode_byte_array[i].len != type_length)) {
        throw ParquetException("Fixed length byte array length mismatch");
      }
      buffer[i].ptr = decode_byte_array[i].ptr;
    }
    return decoded_values_size;
  }
};

// ----------------------------------------------------------------------
// BYTE_STREAM_SPLIT decoders

template <typename DType>
class ByteStreamSplitDecoderBase : public TypedDecoderImpl<DType> {
 public:
  using Base = TypedDecoderImpl<DType>;
  using T = typename DType::c_type;

  explicit ByteStreamSplitDecoderBase(const ColumnDescriptor* descr)
      : Base(descr, Encoding::BYTE_STREAM_SPLIT) {}

  void SetData(int num_values, const uint8_t* data, int len) final {
    // Check that the data size is consistent with the number of values
    // The spec requires that the data size is a multiple of the number of values,
    // see: https://github.com/apache/parquet-format/pull/192 .
    // GH-41562: passed in `num_values` may include nulls, so we need to check and
    // adjust the number of values.
    if (static_cast<int64_t>(num_values) * this->type_length_ < len) {
      throw ParquetException(
          "Data size (" + std::to_string(len) +
          ") is too small for the number of values in in BYTE_STREAM_SPLIT (" +
          std::to_string(num_values) + ")");
    }
    if (len % this->type_length_ != 0) {
      throw ParquetException("ByteStreamSplit data size " + std::to_string(len) +
                             " not aligned with type " + TypeToString(DType::type_num) +
                             " and byte width: " + std::to_string(this->type_length_));
    }
    num_values = len / this->type_length_;
    Base::SetData(num_values, data, len);
    stride_ = this->num_values_;
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::DictAccumulator* builder) override {
    ParquetException::NYI("DecodeArrow to DictAccumulator for BYTE_STREAM_SPLIT");
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator* builder) override {
    const int values_to_decode = num_values - null_count;
    if (ARROW_PREDICT_FALSE(this->num_values_ < values_to_decode)) {
      ParquetException::EofException();
    }

    PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

    // 1. Decode directly into the FixedSizeBinary data buffer, packed to the right.
    uint8_t* decode_out = reinterpret_cast<uint8_t*>(
        builder->GetMutableValue(builder->length() + null_count));
    const int num_decoded = this->DecodeRaw(decode_out, values_to_decode);
    DCHECK_EQ(num_decoded, values_to_decode);

    if (null_count == 0) {
      // No expansion required, and no need to append the bitmap
      builder->UnsafeAdvance(num_values);
      return values_to_decode;
    }

    // 2. Expand the decode values into their final positions.
    ::arrow::util::internal::SpacedExpandLeftward(
        reinterpret_cast<uint8_t*>(builder->GetMutableValue(builder->length())),
        this->type_length_, num_values, null_count, valid_bits, valid_bits_offset);
    builder->UnsafeAdvance(num_values, valid_bits, valid_bits_offset);
    return values_to_decode;
  }

 protected:
  int DecodeRaw(uint8_t* out_buffer, int max_values) {
    const int values_to_decode = std::min(this->num_values_, max_values);
    ::arrow::util::internal::ByteStreamSplitDecode(this->data_, this->type_length_,
                                                   values_to_decode, stride_, out_buffer);
    this->data_ += values_to_decode;
    this->num_values_ -= values_to_decode;
    this->len_ -= this->type_length_ * values_to_decode;
    return values_to_decode;
  }

  uint8_t* EnsureDecodeBuffer(int64_t min_values) {
    const int64_t size = this->type_length_ * min_values;
    if (!decode_buffer_ || decode_buffer_->size() < size) {
      const auto alloc_size = ::arrow::bit_util::NextPower2(size);
      PARQUET_ASSIGN_OR_THROW(decode_buffer_, ::arrow::AllocateBuffer(alloc_size));
    }
    return decode_buffer_->mutable_data();
  }

  int stride_{0};
  std::shared_ptr<Buffer> decode_buffer_;
};

// BYTE_STREAM_SPLIT decoder for FLOAT, DOUBLE, INT32, INT64

template <typename DType>
class ByteStreamSplitDecoder : public ByteStreamSplitDecoderBase<DType> {
 public:
  using Base = ByteStreamSplitDecoderBase<DType>;
  using T = typename DType::c_type;

  using Base::Base;

  int Decode(T* buffer, int max_values) override {
    return this->DecodeRaw(reinterpret_cast<uint8_t*>(buffer), max_values);
  }
};

// BYTE_STREAM_SPLIT decoder for FIXED_LEN_BYTE_ARRAY

template <>
class ByteStreamSplitDecoder<FLBAType> : public ByteStreamSplitDecoderBase<FLBAType>,
                                         public FLBADecoder {
 public:
  using Base = ByteStreamSplitDecoderBase<FLBAType>;
  using DType = FLBAType;
  using T = FixedLenByteArray;

  using Base::Base;

  int Decode(T* buffer, int max_values) override {
    // Decode into intermediate buffer.
    max_values = std::min(max_values, this->num_values_);
    uint8_t* decode_out = this->EnsureDecodeBuffer(max_values);
    const int num_decoded = this->DecodeRaw(decode_out, max_values);
    DCHECK_EQ(num_decoded, max_values);

    for (int i = 0; i < num_decoded; ++i) {
      buffer[i] =
          FixedLenByteArray(decode_out + static_cast<int64_t>(this->type_length_) * i);
    }
    return num_decoded;
  }
};

}  // namespace

// ----------------------------------------------------------------------
// Factory functions

std::unique_ptr<Decoder> MakeDecoder(Type::type type_num, Encoding::type encoding,
                                     const ColumnDescriptor* descr,
                                     ::arrow::MemoryPool* pool) {
  if (encoding == Encoding::PLAIN) {
    switch (type_num) {
      case Type::BOOLEAN:
        return std::make_unique<PlainBooleanDecoder>(descr);
      case Type::INT32:
        return std::make_unique<PlainDecoder<Int32Type>>(descr);
      case Type::INT64:
        return std::make_unique<PlainDecoder<Int64Type>>(descr);
      case Type::INT96:
        return std::make_unique<PlainDecoder<Int96Type>>(descr);
      case Type::FLOAT:
        return std::make_unique<PlainDecoder<FloatType>>(descr);
      case Type::DOUBLE:
        return std::make_unique<PlainDecoder<DoubleType>>(descr);
      case Type::BYTE_ARRAY:
        return std::make_unique<PlainByteArrayDecoder>(descr);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<PlainFLBADecoder>(descr);
      default:
        break;
    }
  } else if (encoding == Encoding::BYTE_STREAM_SPLIT) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<ByteStreamSplitDecoder<Int32Type>>(descr);
      case Type::INT64:
        return std::make_unique<ByteStreamSplitDecoder<Int64Type>>(descr);
      case Type::FLOAT:
        return std::make_unique<ByteStreamSplitDecoder<FloatType>>(descr);
      case Type::DOUBLE:
        return std::make_unique<ByteStreamSplitDecoder<DoubleType>>(descr);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<ByteStreamSplitDecoder<FLBAType>>(descr);
      default:
        throw ParquetException(
            "BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64 "
            "and FIXED_LEN_BYTE_ARRAY");
    }
  } else if (encoding == Encoding::DELTA_BINARY_PACKED) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<DeltaBitPackDecoder<Int32Type>>(descr, pool);
      case Type::INT64:
        return std::make_unique<DeltaBitPackDecoder<Int64Type>>(descr, pool);
      default:
        throw ParquetException(
            "DELTA_BINARY_PACKED decoder only supports INT32 and INT64");
    }
  } else if (encoding == Encoding::DELTA_BYTE_ARRAY) {
    switch (type_num) {
      case Type::BYTE_ARRAY:
        return std::make_unique<DeltaByteArrayDecoder>(descr, pool);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<DeltaByteArrayFLBADecoder>(descr, pool);
      default:
        throw ParquetException(
            "DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY");
    }
  } else if (encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) {
    if (type_num == Type::BYTE_ARRAY) {
      return std::make_unique<DeltaLengthByteArrayDecoder>(descr, pool);
    }
    throw ParquetException("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY");
  } else if (encoding == Encoding::RLE) {
    if (type_num == Type::BOOLEAN) {
      return std::make_unique<RleBooleanDecoder>(descr);
    }
    throw ParquetException("RLE encoding only supports BOOLEAN");
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
      return std::make_unique<DictDecoderImpl<Int32Type>>(descr, pool);
    case Type::INT64:
      return std::make_unique<DictDecoderImpl<Int64Type>>(descr, pool);
    case Type::INT96:
      return std::make_unique<DictDecoderImpl<Int96Type>>(descr, pool);
    case Type::FLOAT:
      return std::make_unique<DictDecoderImpl<FloatType>>(descr, pool);
    case Type::DOUBLE:
      return std::make_unique<DictDecoderImpl<DoubleType>>(descr, pool);
    case Type::BYTE_ARRAY:
      return std::make_unique<DictByteArrayDecoderImpl>(descr, pool);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_unique<DictDecoderImpl<FLBAType>>(descr, pool);
    default:
      break;
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
}

}  // namespace detail
}  // namespace parquet
