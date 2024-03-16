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
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_stream_utils_internal.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/byte_stream_split_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding_internal.h"
#include "arrow/util/spaced.h"
#include "arrow/util/ubsan.h"
#include "arrow/visit_data_inline.h"

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace bit_util = arrow::bit_util;

using arrow::Status;
using arrow::internal::AddWithOverflow;
using arrow::internal::checked_cast;
using arrow::internal::SafeSignedSubtract;
using arrow::util::SafeLoad;
using arrow::util::SafeLoadAs;

template <typename T>
using ArrowPoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

namespace parquet {
namespace {

// The Parquet spec isn't very clear whether ByteArray lengths are signed or
// unsigned, but the Java implementation uses signed ints.
constexpr size_t kMaxByteArraySize = std::numeric_limits<int32_t>::max();

class EncoderImpl : virtual public Encoder {
 public:
  EncoderImpl(const ColumnDescriptor* descr, Encoding::type encoding, MemoryPool* pool)
      : descr_(descr),
        encoding_(encoding),
        pool_(pool),
        type_length_(descr ? descr->type_length() : -1) {}

  Encoding::type encoding() const override { return encoding_; }

  MemoryPool* memory_pool() const override { return pool_; }

  int64_t ReportUnencodedDataBytes() override {
    if (descr_->physical_type() != Type::BYTE_ARRAY) {
      throw ParquetException("ReportUnencodedDataBytes is only supported for BYTE_ARRAY");
    }
    int64_t bytes = unencoded_byte_array_data_bytes_;
    unencoded_byte_array_data_bytes_ = 0;
    return bytes;
  }

 protected:
  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;
  const Encoding::type encoding_;
  MemoryPool* pool_;

  /// Type length from descr
  const int type_length_;
  /// Number of unencoded bytes written to the encoder. Used for ByteArray type only.
  int64_t unencoded_byte_array_data_bytes_ = 0;
};

// ----------------------------------------------------------------------
// PLAIN encoder

template <typename DType>
class PlainEncoder : public EncoderImpl, virtual public TypedEncoder<DType> {
 public:
  using T = typename DType::c_type;

  explicit PlainEncoder(const ColumnDescriptor* descr, MemoryPool* pool)
      : EncoderImpl(descr, Encoding::PLAIN, pool), sink_(pool) {}

  int64_t EstimatedDataEncodedSize() override { return sink_.length(); }

  std::shared_ptr<Buffer> FlushValues() override {
    std::shared_ptr<Buffer> buffer;
    PARQUET_THROW_NOT_OK(sink_.Finish(&buffer));
    return buffer;
  }

  using TypedEncoder<DType>::Put;

  void Put(const T* buffer, int num_values) override;

  void Put(const ::arrow::Array& values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = buffer->template mutable_data_as<T>();
      int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
          src, num_values, valid_bits, valid_bits_offset, data);
      Put(data, num_valid_values);
    } else {
      Put(src, num_values);
    }
  }

  void UnsafePutByteArray(const void* data, uint32_t length) {
    DCHECK(length == 0 || data != nullptr) << "Value ptr cannot be NULL";
    sink_.UnsafeAppend(&length, sizeof(uint32_t));
    sink_.UnsafeAppend(data, static_cast<int64_t>(length));
    unencoded_byte_array_data_bytes_ += length;
  }

  void Put(const ByteArray& val) {
    // Write the result to the output stream
    const int64_t increment = static_cast<int64_t>(val.len + sizeof(uint32_t));
    if (ARROW_PREDICT_FALSE(sink_.length() + increment > sink_.capacity())) {
      PARQUET_THROW_NOT_OK(sink_.Reserve(increment));
    }
    UnsafePutByteArray(val.ptr, val.len);
  }

 protected:
  template <typename ArrayType>
  void PutBinaryArray(const ArrayType& array) {
    const int64_t total_bytes =
        array.value_offset(array.length()) - array.value_offset(0);
    PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes + array.length() * sizeof(uint32_t)));

    PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](::std::string_view view) {
          if (ARROW_PREDICT_FALSE(view.size() > kMaxByteArraySize)) {
            return Status::Invalid(
                "Parquet cannot store strings with size 2GB or more, got: ", view.size());
          }
          UnsafePutByteArray(view.data(), static_cast<uint32_t>(view.size()));
          return Status::OK();
        },
        []() { return Status::OK(); }));
  }

  ::arrow::BufferBuilder sink_;
};

template <typename DType>
void PlainEncoder<DType>::Put(const T* buffer, int num_values) {
  if (num_values > 0) {
    PARQUET_THROW_NOT_OK(sink_.Append(buffer, num_values * sizeof(T)));
  }
}

template <>
inline void PlainEncoder<ByteArrayType>::Put(const ByteArray* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    Put(src[i]);
  }
}

template <typename ArrayType>
void DirectPutImpl(const ::arrow::Array& values, ::arrow::BufferBuilder* sink) {
  if (values.type_id() != ArrayType::TypeClass::type_id) {
    std::string type_name = ArrayType::TypeClass::type_name();
    throw ParquetException("direct put to " + type_name + " from " +
                           values.type()->ToString() + " not supported");
  }

  using value_type = typename ArrayType::value_type;
  constexpr auto value_size = sizeof(value_type);
  auto raw_values = checked_cast<const ArrayType&>(values).raw_values();

  if (values.null_count() == 0) {
    // no nulls, just dump the data
    PARQUET_THROW_NOT_OK(sink->Append(raw_values, values.length() * value_size));
  } else {
    PARQUET_THROW_NOT_OK(
        sink->Reserve((values.length() - values.null_count()) * value_size));

    for (int64_t i = 0; i < values.length(); i++) {
      if (values.IsValid(i)) {
        sink->UnsafeAppend(&raw_values[i], value_size);
      }
    }
  }
}

template <>
void PlainEncoder<Int32Type>::Put(const ::arrow::Array& values) {
  DirectPutImpl<::arrow::Int32Array>(values, &sink_);
}

template <>
void PlainEncoder<Int64Type>::Put(const ::arrow::Array& values) {
  DirectPutImpl<::arrow::Int64Array>(values, &sink_);
}

template <>
void PlainEncoder<Int96Type>::Put(const ::arrow::Array& values) {
  ParquetException::NYI("direct put to Int96");
}

template <>
void PlainEncoder<FloatType>::Put(const ::arrow::Array& values) {
  DirectPutImpl<::arrow::FloatArray>(values, &sink_);
}

template <>
void PlainEncoder<DoubleType>::Put(const ::arrow::Array& values) {
  DirectPutImpl<::arrow::DoubleArray>(values, &sink_);
}

template <typename DType>
void PlainEncoder<DType>::Put(const ::arrow::Array& values) {
  ParquetException::NYI("direct put of " + values.type()->ToString());
}

void AssertBaseBinary(const ::arrow::Array& values) {
  if (!::arrow::is_base_binary_like(values.type_id())) {
    throw ParquetException("Only BaseBinaryArray and subclasses supported");
  }
}

template <>
inline void PlainEncoder<ByteArrayType>::Put(const ::arrow::Array& values) {
  AssertBaseBinary(values);

  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else {
    DCHECK(::arrow::is_large_binary_like(values.type_id()));
    PutBinaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  }
}

void AssertFixedSizeBinary(const ::arrow::Array& values, int type_length) {
  if (!::arrow::is_fixed_size_binary(values.type_id())) {
    throw ParquetException("Only FixedSizeBinaryArray and subclasses supported");
  }
  if (checked_cast<const ::arrow::FixedSizeBinaryType&>(*values.type()).byte_width() !=
      type_length) {
    throw ParquetException("Size mismatch: " + values.type()->ToString() +
                           " should have been " + std::to_string(type_length) + " wide");
  }
}

template <>
inline void PlainEncoder<FLBAType>::Put(const ::arrow::Array& values) {
  AssertFixedSizeBinary(values, descr_->type_length());
  const auto& data = checked_cast<const ::arrow::FixedSizeBinaryArray&>(values);

  if (data.null_count() == 0) {
    // no nulls, just dump the data
    PARQUET_THROW_NOT_OK(
        sink_.Append(data.raw_values(), data.length() * data.byte_width()));
  } else {
    const int64_t total_bytes =
        data.length() * data.byte_width() - data.null_count() * data.byte_width();
    PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes));
    for (int64_t i = 0; i < data.length(); i++) {
      if (data.IsValid(i)) {
        sink_.UnsafeAppend(data.Value(i), data.byte_width());
      }
    }
  }
}

template <>
inline void PlainEncoder<FLBAType>::Put(const FixedLenByteArray* src, int num_values) {
  if (descr_->type_length() == 0) {
    return;
  }
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    DCHECK(src[i].ptr != nullptr) << "Value ptr cannot be NULL";
    PARQUET_THROW_NOT_OK(sink_.Append(src[i].ptr, descr_->type_length()));
  }
}

template <>
class PlainEncoder<BooleanType> : public EncoderImpl, virtual public BooleanEncoder {
 public:
  explicit PlainEncoder(const ColumnDescriptor* descr, MemoryPool* pool)
      : EncoderImpl(descr, Encoding::PLAIN, pool), sink_(pool) {}

  int64_t EstimatedDataEncodedSize() override;
  std::shared_ptr<Buffer> FlushValues() override;

  void Put(const bool* src, int num_values) override;

  void Put(const std::vector<bool>& src, int num_values) override;

  void PutSpaced(const bool* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = buffer->mutable_data_as<T>();
      int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
          src, num_values, valid_bits, valid_bits_offset, data);
      Put(data, num_valid_values);
    } else {
      Put(src, num_values);
    }
  }

  void Put(const ::arrow::Array& values) override {
    if (values.type_id() != ::arrow::Type::BOOL) {
      throw ParquetException("direct put to boolean from " + values.type()->ToString() +
                             " not supported");
    }
    const auto& data = checked_cast<const ::arrow::BooleanArray&>(values);

    if (data.null_count() == 0) {
      // no nulls, just dump the data
      PARQUET_THROW_NOT_OK(sink_.Reserve(data.length()));
      sink_.UnsafeAppend(data.data()->GetValues<uint8_t>(1, 0), data.offset(),
                         data.length());
    } else {
      PARQUET_THROW_NOT_OK(sink_.Reserve(data.length() - data.null_count()));
      for (int64_t i = 0; i < data.length(); i++) {
        if (data.IsValid(i)) {
          sink_.UnsafeAppend(data.Value(i));
        }
      }
    }
  }

 private:
  ::arrow::TypedBufferBuilder<bool> sink_;

  template <typename SequenceType>
  void PutImpl(const SequenceType& src, int num_values);
};

template <typename SequenceType>
void PlainEncoder<BooleanType>::PutImpl(const SequenceType& src, int num_values) {
  PARQUET_THROW_NOT_OK(sink_.Reserve(num_values));
  for (int i = 0; i < num_values; ++i) {
    sink_.UnsafeAppend(src[i]);
  }
}

int64_t PlainEncoder<BooleanType>::EstimatedDataEncodedSize() {
  return ::arrow::bit_util::BytesForBits(sink_.length());
}

std::shared_ptr<Buffer> PlainEncoder<BooleanType>::FlushValues() {
  std::shared_ptr<Buffer> buffer;
  PARQUET_THROW_NOT_OK(sink_.Finish(&buffer));
  return buffer;
}

void PlainEncoder<BooleanType>::Put(const bool* src, int num_values) {
  PutImpl(src, num_values);
}

void PlainEncoder<BooleanType>::Put(const std::vector<bool>& src, int num_values) {
  PutImpl(src, num_values);
}

// ----------------------------------------------------------------------
// DictEncoder<T> implementations

template <typename DType>
struct DictEncoderTraits {
  using c_type = typename DType::c_type;
  using MemoTableType = ::arrow::internal::ScalarMemoTable<c_type>;
};

template <>
struct DictEncoderTraits<ByteArrayType> {
  using MemoTableType = ::arrow::internal::BinaryMemoTable<::arrow::BinaryBuilder>;
};

template <>
struct DictEncoderTraits<FLBAType> {
  using MemoTableType = ::arrow::internal::BinaryMemoTable<::arrow::BinaryBuilder>;
};

// Initially 1024 elements
static constexpr int32_t kInitialHashTableSize = 1 << 10;

int RlePreserveBufferSize(int num_values, int bit_width) {
  // Note: because of the way RleEncoder::CheckBufferFull()
  // is called, we have to reserve an extra "RleEncoder::MinBufferSize"
  // bytes. These extra bytes won't be used but not reserving them
  // would cause the encoder to fail.
  return ::arrow::util::RleEncoder::MaxBufferSize(bit_width, num_values) +
         ::arrow::util::RleEncoder::MinBufferSize(bit_width);
}

/// See the dictionary encoding section of
/// https://github.com/Parquet/parquet-format.  The encoding supports
/// streaming encoding. Values are encoded as they are added while the
/// dictionary is being constructed. At any time, the buffered values
/// can be written out with the current dictionary size. More values
/// can then be added to the encoder, including new dictionary
/// entries.
template <typename DType>
class DictEncoderImpl : public EncoderImpl, virtual public DictEncoder<DType> {
  using MemoTableType = typename DictEncoderTraits<DType>::MemoTableType;

 public:
  typedef typename DType::c_type T;

  /// In data page, the bit width used to encode the entry
  /// ids stored as 1 byte (max bit width = 32).
  constexpr static int32_t kDataPageBitWidthBytes = 1;

  explicit DictEncoderImpl(const ColumnDescriptor* desc, MemoryPool* pool)
      : EncoderImpl(desc, Encoding::RLE_DICTIONARY, pool),
        buffered_indices_(::arrow::stl::allocator<int32_t>(pool)),
        dict_encoded_size_(0),
        memo_table_(pool, kInitialHashTableSize) {}

  ~DictEncoderImpl() override = default;

  int dict_encoded_size() const override { return dict_encoded_size_; }

  int WriteIndices(uint8_t* buffer, int buffer_len) override {
    // Write bit width in first byte
    *buffer = static_cast<uint8_t>(bit_width());
    ++buffer;
    --buffer_len;

    ::arrow::util::RleEncoder encoder(buffer, buffer_len, bit_width());

    for (int32_t index : buffered_indices_) {
      if (ARROW_PREDICT_FALSE(!encoder.Put(index))) return -1;
    }
    encoder.Flush();

    ClearIndices();
    return kDataPageBitWidthBytes + encoder.len();
  }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteIndices().
  int64_t EstimatedDataEncodedSize() override {
    return kDataPageBitWidthBytes +
           RlePreserveBufferSize(static_cast<int>(buffered_indices_.size()), bit_width());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const override {
    if (ARROW_PREDICT_FALSE(num_entries() == 0)) return 0;
    if (ARROW_PREDICT_FALSE(num_entries() == 1)) return 1;
    return bit_util::Log2(num_entries());
  }

  /// Encode value. Note that this does not actually write any data, just
  /// buffers the value's index to be written later.
  inline void Put(const T& value);

  // Not implemented for other data types
  inline void PutByteArray(const void* ptr, int32_t length);

  void Put(const T* src, int num_values) override {
    for (int32_t i = 0; i < num_values; i++) {
      Put(SafeLoad(src + i));
    }
  }

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    ::arrow::internal::VisitSetBitRunsVoid(valid_bits, valid_bits_offset, num_values,
                                           [&](int64_t position, int64_t length) {
                                             for (int64_t i = 0; i < length; i++) {
                                               Put(SafeLoad(src + i + position));
                                             }
                                           });
  }

  using TypedEncoder<DType>::Put;

  void Put(const ::arrow::Array& values) override;
  void PutDictionary(const ::arrow::Array& values) override;

  template <typename ArrowType, typename T = typename ArrowType::c_type>
  void PutIndicesTyped(const ::arrow::Array& data) {
    auto values = data.data()->GetValues<T>(1);
    size_t buffer_position = buffered_indices_.size();
    buffered_indices_.resize(buffer_position +
                             static_cast<size_t>(data.length() - data.null_count()));
    ::arrow::internal::VisitSetBitRunsVoid(
        data.null_bitmap_data(), data.offset(), data.length(),
        [&](int64_t position, int64_t length) {
          for (int64_t i = 0; i < length; ++i) {
            buffered_indices_[buffer_position++] =
                static_cast<int32_t>(values[i + position]);
          }
        });

    // Track unencoded bytes based on dictionary value type
    if constexpr (std::is_same_v<DType, ByteArrayType>) {
      // For ByteArray, need to look up actual lengths from dictionary
      for (size_t idx =
               buffer_position - static_cast<size_t>(data.length() - data.null_count());
           idx < buffer_position; ++idx) {
        memo_table_.VisitValue(buffered_indices_[idx], [&](std::string_view value) {
          unencoded_byte_array_data_bytes_ += value.length();
        });
      }
    }
  }

  void PutIndices(const ::arrow::Array& data) override {
    switch (data.type()->id()) {
      case ::arrow::Type::UINT8:
      case ::arrow::Type::INT8:
        return PutIndicesTyped<::arrow::UInt8Type>(data);
      case ::arrow::Type::UINT16:
      case ::arrow::Type::INT16:
        return PutIndicesTyped<::arrow::UInt16Type>(data);
      case ::arrow::Type::UINT32:
      case ::arrow::Type::INT32:
        return PutIndicesTyped<::arrow::UInt32Type>(data);
      case ::arrow::Type::UINT64:
      case ::arrow::Type::INT64:
        return PutIndicesTyped<::arrow::UInt64Type>(data);
      default:
        throw ParquetException("Passed non-integer array to PutIndices");
    }
  }

  std::shared_ptr<Buffer> FlushValues() override {
    std::shared_ptr<ResizableBuffer> buffer =
        AllocateBuffer(this->pool_, EstimatedDataEncodedSize());
    int result_size = WriteIndices(buffer->mutable_data(),
                                   static_cast<int>(EstimatedDataEncodedSize()));
    PARQUET_THROW_NOT_OK(buffer->Resize(result_size, false));
    return buffer;
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  void WriteDict(uint8_t* buffer) const override;

  /// The number of entries in the dictionary.
  int num_entries() const override { return memo_table_.size(); }

 private:
  /// Clears all the indices (but leaves the dictionary).
  void ClearIndices() { buffered_indices_.clear(); }

  /// Indices that have not yet be written out by WriteIndices().
  ArrowPoolVector<int32_t> buffered_indices_;

  template <typename ArrayType>
  void PutBinaryArray(const ArrayType& array) {
    PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](::std::string_view view) {
          if (ARROW_PREDICT_FALSE(view.size() > kMaxByteArraySize)) {
            return Status::Invalid(
                "Parquet cannot store strings with size 2GB or more, got: ", view.size());
          }
          PutByteArray(view.data(), static_cast<uint32_t>(view.size()));
          return Status::OK();
        },
        []() { return Status::OK(); }));
  }

  template <typename ArrayType>
  void PutBinaryDictionaryArray(const ArrayType& array) {
    DCHECK_EQ(array.null_count(), 0);
    for (int64_t i = 0; i < array.length(); i++) {
      auto v = array.GetView(i);
      if (ARROW_PREDICT_FALSE(v.size() > kMaxByteArraySize)) {
        throw ParquetException(
            "Parquet cannot store strings with size 2GB or more, got: ", v.size());
      }
      dict_encoded_size_ += static_cast<int>(v.size() + sizeof(uint32_t));
      int32_t unused_memo_index;
      PARQUET_THROW_NOT_OK(memo_table_.GetOrInsert(
          v.data(), static_cast<int32_t>(v.size()), &unused_memo_index));
    }
  }

  /// The number of bytes needed to encode the dictionary.
  int dict_encoded_size_;

  MemoTableType memo_table_;
};

template <typename DType>
void DictEncoderImpl<DType>::WriteDict(uint8_t* buffer) const {
  // For primitive types, only a memcpy
  DCHECK_EQ(static_cast<size_t>(dict_encoded_size_), sizeof(T) * memo_table_.size());
  memo_table_.CopyValues(0 /* start_pos */, reinterpret_cast<T*>(buffer));
}

// ByteArray and FLBA already have the dictionary encoded in their data heaps
template <>
void DictEncoderImpl<ByteArrayType>::WriteDict(uint8_t* buffer) const {
  memo_table_.VisitValues(0, [&buffer](::std::string_view v) {
    uint32_t len = static_cast<uint32_t>(v.length());
    memcpy(buffer, &len, sizeof(len));
    buffer += sizeof(len);
    memcpy(buffer, v.data(), len);
    buffer += len;
  });
}

template <>
void DictEncoderImpl<FLBAType>::WriteDict(uint8_t* buffer) const {
  memo_table_.VisitValues(0, [&](::std::string_view v) {
    DCHECK_EQ(v.length(), static_cast<size_t>(type_length_));
    memcpy(buffer, v.data(), type_length_);
    buffer += type_length_;
  });
}

template <typename DType>
inline void DictEncoderImpl<DType>::Put(const T& v) {
  // Put() implementation for primitive types
  auto on_found = [](int32_t memo_index) {};
  auto on_not_found = [this](int32_t memo_index) {
    dict_encoded_size_ += static_cast<int>(sizeof(T));
  };

  int32_t memo_index;
  PARQUET_THROW_NOT_OK(memo_table_.GetOrInsert(v, on_found, on_not_found, &memo_index));
  buffered_indices_.push_back(memo_index);
}

template <typename DType>
inline void DictEncoderImpl<DType>::PutByteArray(const void* ptr, int32_t length) {
  DCHECK(false);
}

template <>
inline void DictEncoderImpl<ByteArrayType>::PutByteArray(const void* ptr,
                                                         int32_t length) {
  static const uint8_t empty[] = {0};

  auto on_found = [](int32_t memo_index) {};
  auto on_not_found = [&](int32_t memo_index) {
    dict_encoded_size_ += static_cast<int>(length + sizeof(uint32_t));
  };

  DCHECK(ptr != nullptr || length == 0);
  ptr = (ptr != nullptr) ? ptr : empty;
  int32_t memo_index;
  PARQUET_THROW_NOT_OK(
      memo_table_.GetOrInsert(ptr, length, on_found, on_not_found, &memo_index));
  buffered_indices_.push_back(memo_index);
  unencoded_byte_array_data_bytes_ += length;
}

template <>
inline void DictEncoderImpl<ByteArrayType>::Put(const ByteArray& val) {
  return PutByteArray(val.ptr, static_cast<int32_t>(val.len));
}

template <>
inline void DictEncoderImpl<FLBAType>::Put(const FixedLenByteArray& v) {
  static const uint8_t empty[] = {0};

  auto on_found = [](int32_t memo_index) {};
  auto on_not_found = [this](int32_t memo_index) { dict_encoded_size_ += type_length_; };

  DCHECK(v.ptr != nullptr || type_length_ == 0);
  const void* ptr = (v.ptr != nullptr) ? v.ptr : empty;
  int32_t memo_index;
  PARQUET_THROW_NOT_OK(
      memo_table_.GetOrInsert(ptr, type_length_, on_found, on_not_found, &memo_index));
  buffered_indices_.push_back(memo_index);
}

template <>
void DictEncoderImpl<Int96Type>::Put(const ::arrow::Array& values) {
  ParquetException::NYI("Direct put to Int96");
}

template <>
void DictEncoderImpl<Int96Type>::PutDictionary(const ::arrow::Array& values) {
  ParquetException::NYI("Direct put to Int96");
}

template <typename DType>
void DictEncoderImpl<DType>::Put(const ::arrow::Array& values) {
  using ArrayType = typename ::arrow::CTypeTraits<typename DType::c_type>::ArrayType;
  const auto& data = checked_cast<const ArrayType&>(values);
  if (data.null_count() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < data.length(); i++) {
      Put(data.Value(i));
    }
  } else {
    for (int64_t i = 0; i < data.length(); i++) {
      if (data.IsValid(i)) {
        Put(data.Value(i));
      }
    }
  }
}

template <>
void DictEncoderImpl<FLBAType>::Put(const ::arrow::Array& values) {
  AssertFixedSizeBinary(values, type_length_);
  const auto& data = checked_cast<const ::arrow::FixedSizeBinaryArray&>(values);
  if (data.null_count() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < data.length(); i++) {
      Put(FixedLenByteArray(data.Value(i)));
    }
  } else {
    std::vector<uint8_t> empty(type_length_, 0);
    for (int64_t i = 0; i < data.length(); i++) {
      if (data.IsValid(i)) {
        Put(FixedLenByteArray(data.Value(i)));
      }
    }
  }
}

template <>
void DictEncoderImpl<ByteArrayType>::Put(const ::arrow::Array& values) {
  AssertBaseBinary(values);
  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else {
    DCHECK(::arrow::is_large_binary_like(values.type_id()));
    PutBinaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  }
}

template <typename DType>
void AssertCanPutDictionary(DictEncoderImpl<DType>* encoder, const ::arrow::Array& dict) {
  if (dict.null_count() > 0) {
    throw ParquetException("Inserted dictionary cannot contain nulls");
  }

  if (encoder->num_entries() > 0) {
    throw ParquetException("Can only call PutDictionary on an empty DictEncoder");
  }
}

template <typename DType>
void DictEncoderImpl<DType>::PutDictionary(const ::arrow::Array& values) {
  AssertCanPutDictionary(this, values);

  using ArrayType = typename ::arrow::CTypeTraits<typename DType::c_type>::ArrayType;
  const auto& data = checked_cast<const ArrayType&>(values);

  dict_encoded_size_ += static_cast<int>(sizeof(typename DType::c_type) * data.length());
  for (int64_t i = 0; i < data.length(); i++) {
    int32_t unused_memo_index;
    PARQUET_THROW_NOT_OK(memo_table_.GetOrInsert(data.Value(i), &unused_memo_index));
  }
}

template <>
void DictEncoderImpl<FLBAType>::PutDictionary(const ::arrow::Array& values) {
  AssertFixedSizeBinary(values, type_length_);
  AssertCanPutDictionary(this, values);

  const auto& data = checked_cast<const ::arrow::FixedSizeBinaryArray&>(values);

  dict_encoded_size_ += static_cast<int>(type_length_ * data.length());
  for (int64_t i = 0; i < data.length(); i++) {
    int32_t unused_memo_index;
    PARQUET_THROW_NOT_OK(
        memo_table_.GetOrInsert(data.Value(i), type_length_, &unused_memo_index));
  }
}

template <>
void DictEncoderImpl<ByteArrayType>::PutDictionary(const ::arrow::Array& values) {
  AssertBaseBinary(values);
  AssertCanPutDictionary(this, values);

  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryDictionaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else {
    DCHECK(::arrow::is_large_binary_like(values.type_id()));
    PutBinaryDictionaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  }
}

// ----------------------------------------------------------------------
// BYTE_STREAM_SPLIT encoder

// Common base class for all types

template <typename DType>
class ByteStreamSplitEncoderBase : public EncoderImpl,
                                   virtual public TypedEncoder<DType> {
 public:
  using T = typename DType::c_type;
  using TypedEncoder<DType>::Put;

  ByteStreamSplitEncoderBase(const ColumnDescriptor* descr, int byte_width,
                             ::arrow::MemoryPool* pool)
      : EncoderImpl(descr, Encoding::BYTE_STREAM_SPLIT, pool),
        sink_{pool},
        byte_width_(byte_width),
        num_values_in_buffer_{0} {}

  int64_t EstimatedDataEncodedSize() override { return sink_.length(); }

  std::shared_ptr<Buffer> FlushValues() override {
    if (byte_width_ == 1) {
      // Special-cased fast path
      PARQUET_ASSIGN_OR_THROW(auto buf, sink_.Finish());
      return buf;
    }
    auto output_buffer = AllocateBuffer(this->memory_pool(), EstimatedDataEncodedSize());
    uint8_t* output_buffer_raw = output_buffer->mutable_data();
    const uint8_t* raw_values = sink_.data();
    ::arrow::util::internal::ByteStreamSplitEncode(
        raw_values, /*width=*/byte_width_, num_values_in_buffer_, output_buffer_raw);
    sink_.Reset();
    num_values_in_buffer_ = 0;
    return output_buffer;
  }

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = buffer->template mutable_data_as<T>();
      int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
          src, num_values, valid_bits, valid_bits_offset, data);
      Put(data, num_valid_values);
    } else {
      Put(src, num_values);
    }
  }

 protected:
  ::arrow::BufferBuilder sink_;
  // Required because type_length_ is only filled in for FLBA
  const int byte_width_;
  int64_t num_values_in_buffer_;
};

// BYTE_STREAM_SPLIT encoder implementation for FLOAT, DOUBLE, INT32, INT64

template <typename DType>
class ByteStreamSplitEncoder : public ByteStreamSplitEncoderBase<DType> {
 public:
  using T = typename DType::c_type;
  using ArrowType = typename EncodingTraits<DType>::ArrowType;

  ByteStreamSplitEncoder(const ColumnDescriptor* descr,
                         ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : ByteStreamSplitEncoderBase<DType>(descr,
                                          /*byte_width=*/static_cast<int>(sizeof(T)),
                                          pool) {}

  // Inherit Put(const std::vector<T>&...)
  using TypedEncoder<DType>::Put;

  void Put(const T* buffer, int num_values) override {
    if (num_values > 0) {
      PARQUET_THROW_NOT_OK(
          this->sink_.Append(reinterpret_cast<const uint8_t*>(buffer),
                             num_values * static_cast<int64_t>(sizeof(T))));
      this->num_values_in_buffer_ += num_values;
    }
  }

  void Put(const ::arrow::Array& values) override {
    if (values.type_id() != ArrowType::type_id) {
      throw ParquetException(std::string() + "direct put from " +
                             values.type()->ToString() + " not supported");
    }
    const auto& data = *values.data();
    this->PutSpaced(data.GetValues<typename ArrowType::c_type>(1),
                    static_cast<int>(data.length), data.GetValues<uint8_t>(0, 0),
                    data.offset);
  }
};

// BYTE_STREAM_SPLIT encoder implementation for FLBA

template <>
class ByteStreamSplitEncoder<FLBAType> : public ByteStreamSplitEncoderBase<FLBAType> {
 public:
  using DType = FLBAType;
  using T = FixedLenByteArray;
  using ArrowType = ::arrow::FixedSizeBinaryArray;

  ByteStreamSplitEncoder(const ColumnDescriptor* descr,
                         ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : ByteStreamSplitEncoderBase<DType>(descr,
                                          /*byte_width=*/descr->type_length(), pool) {}

  // Inherit Put(const std::vector<T>&...)
  using TypedEncoder<DType>::Put;

  void Put(const T* buffer, int num_values) override {
    if (byte_width_ > 0) {
      const int64_t total_bytes = static_cast<int64_t>(num_values) * byte_width_;
      PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes));
      for (int i = 0; i < num_values; ++i) {
        // Write the result to the output stream
        DCHECK(buffer[i].ptr != nullptr) << "Value ptr cannot be NULL";
        sink_.UnsafeAppend(buffer[i].ptr, byte_width_);
      }
    }
    this->num_values_in_buffer_ += num_values;
  }

  void Put(const ::arrow::Array& values) override {
    AssertFixedSizeBinary(values, byte_width_);
    const auto& data = checked_cast<const ::arrow::FixedSizeBinaryArray&>(values);
    if (data.null_count() == 0) {
      // no nulls, just buffer the data
      PARQUET_THROW_NOT_OK(sink_.Append(data.raw_values(), data.length() * byte_width_));
      this->num_values_in_buffer_ += data.length();
    } else {
      const int64_t num_values = data.length() - data.null_count();
      const int64_t total_bytes = num_values * byte_width_;
      PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes));
      // TODO use VisitSetBitRunsVoid
      for (int64_t i = 0; i < data.length(); i++) {
        if (data.IsValid(i)) {
          sink_.UnsafeAppend(data.Value(i), byte_width_);
        }
      }
      this->num_values_in_buffer_ += num_values;
    }
  }
};

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED encoder

/// DeltaBitPackEncoder is an encoder for the DeltaBinary Packing format
/// as per the parquet spec. See:
/// https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5
///
/// Consists of a header followed by blocks of delta encoded values binary packed.
///
///  Format
///    [header] [block 1] [block 2] ... [block N]
///
///  Header
///    [block size] [number of mini blocks per block] [total value count] [first value]
///
///  Block
///    [min delta] [list of bitwidths of the mini blocks] [miniblocks]
///
/// Sets aside bytes at the start of the internal buffer where the header will be written,
/// and only writes the header when FlushValues is called before returning it.
///
/// To encode a block, we will:
///
/// 1. Compute the differences between consecutive elements. For the first element in the
/// block, use the last element in the previous block or, in the case of the first block,
/// use the first value of the whole sequence, stored in the header.
///
/// 2. Compute the frame of reference (the minimum of the deltas in the block). Subtract
/// this min delta from all deltas in the block. This guarantees that all values are
/// non-negative.
///
/// 3. Encode the frame of reference (min delta) as a zigzag ULEB128 int followed by the
/// bit widths of the mini blocks and the delta values (minus the min delta) bit packed
/// per mini block.
///
/// Supports only INT32 and INT64.

template <typename DType>
class DeltaBitPackEncoder : public EncoderImpl, virtual public TypedEncoder<DType> {
  // Maximum possible header size
  static constexpr uint32_t kMaxPageHeaderWriterSize = 32;
  static constexpr uint32_t kValuesPerBlock =
      std::is_same_v<int32_t, typename DType::c_type> ? 128 : 256;
  static constexpr uint32_t kMiniBlocksPerBlock = 4;

 public:
  using T = typename DType::c_type;
  using UT = std::make_unsigned_t<T>;
  using TypedEncoder<DType>::Put;

  explicit DeltaBitPackEncoder(const ColumnDescriptor* descr, MemoryPool* pool,
                               const uint32_t values_per_block = kValuesPerBlock,
                               const uint32_t mini_blocks_per_block = kMiniBlocksPerBlock)
      : EncoderImpl(descr, Encoding::DELTA_BINARY_PACKED, pool),
        values_per_block_(values_per_block),
        mini_blocks_per_block_(mini_blocks_per_block),
        values_per_mini_block_(values_per_block / mini_blocks_per_block),
        deltas_(values_per_block, ::arrow::stl::allocator<T>(pool)),
        bits_buffer_(
            AllocateBuffer(pool, (kMiniBlocksPerBlock + values_per_block) * sizeof(T))),
        sink_(pool),
        bit_writer_(bits_buffer_->mutable_data(),
                    static_cast<int>(bits_buffer_->size())) {
    if (values_per_block_ % 128 != 0) {
      throw ParquetException(
          "the number of values in a block must be multiple of 128, but it's " +
          std::to_string(values_per_block_));
    }
    if (values_per_mini_block_ % 32 != 0) {
      throw ParquetException(
          "the number of values in a miniblock must be multiple of 32, but it's " +
          std::to_string(values_per_mini_block_));
    }
    if (values_per_block % mini_blocks_per_block != 0) {
      throw ParquetException(
          "the number of values per block % number of miniblocks per block must be 0, "
          "but it's " +
          std::to_string(values_per_block % mini_blocks_per_block));
    }
    // Reserve enough space at the beginning of the buffer for largest possible header.
    PARQUET_THROW_NOT_OK(sink_.Advance(kMaxPageHeaderWriterSize));
  }

  std::shared_ptr<Buffer> FlushValues() override;

  int64_t EstimatedDataEncodedSize() override { return sink_.length(); }

  void Put(const ::arrow::Array& values) override;

  void Put(const T* buffer, int num_values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override;

  void FlushBlock();

 private:
  const uint32_t values_per_block_;
  const uint32_t mini_blocks_per_block_;
  const uint32_t values_per_mini_block_;
  uint32_t values_current_block_{0};
  uint32_t total_value_count_{0};
  T first_value_{0};
  T current_value_{0};
  ArrowPoolVector<T> deltas_;
  std::shared_ptr<ResizableBuffer> bits_buffer_;
  ::arrow::BufferBuilder sink_;
  ::arrow::bit_util::BitWriter bit_writer_;
};

template <typename DType>
void DeltaBitPackEncoder<DType>::Put(const T* src, int num_values) {
  if (num_values == 0) {
    return;
  }

  int idx = 0;
  if (total_value_count_ == 0) {
    current_value_ = src[0];
    first_value_ = current_value_;
    idx = 1;
  }
  total_value_count_ += num_values;

  while (idx < num_values) {
    T value = src[idx];
    // Calculate deltas. The possible overflow is handled by use of unsigned integers
    // making subtraction operations well-defined and correct even in case of overflow.
    // Encoded integers will wrap back around on decoding.
    // See http://en.wikipedia.org/wiki/Modular_arithmetic#Integers_modulo_n
    deltas_[values_current_block_] = SafeSignedSubtract(value, current_value_);
    current_value_ = value;
    idx++;
    values_current_block_++;
    if (values_current_block_ == values_per_block_) {
      FlushBlock();
    }
  }
}

template <typename DType>
void DeltaBitPackEncoder<DType>::FlushBlock() {
  if (values_current_block_ == 0) {
    return;
  }

  // Calculate the frame of reference for this miniblock. This value will be subtracted
  // from all deltas to guarantee all deltas are positive for encoding.
  const T min_delta =
      *std::min_element(deltas_.begin(), deltas_.begin() + values_current_block_);
  bit_writer_.PutZigZagVlqInt(min_delta);

  // Call to GetNextBytePtr reserves mini_blocks_per_block_ bytes of space to write
  // bit widths of miniblocks as they become known during the encoding.
  uint8_t* bit_width_data = bit_writer_.GetNextBytePtr(mini_blocks_per_block_);
  DCHECK(bit_width_data != nullptr);

  const uint32_t num_miniblocks =
      static_cast<uint32_t>(std::ceil(static_cast<double>(values_current_block_) /
                                      static_cast<double>(values_per_mini_block_)));
  for (uint32_t i = 0; i < num_miniblocks; i++) {
    const uint32_t values_current_mini_block =
        std::min(values_per_mini_block_, values_current_block_);

    const uint32_t start = i * values_per_mini_block_;
    const T max_delta = *std::max_element(
        deltas_.begin() + start, deltas_.begin() + start + values_current_mini_block);

    // The minimum number of bits required to write any of values in deltas_ vector.
    // See overflow comment above.
    const auto bit_width = bit_width_data[i] = bit_util::NumRequiredBits(
        static_cast<UT>(max_delta) - static_cast<UT>(min_delta));

    for (uint32_t j = start; j < start + values_current_mini_block; j++) {
      // Convert delta to frame of reference. See overflow comment above.
      const UT value = static_cast<UT>(deltas_[j]) - static_cast<UT>(min_delta);
      bit_writer_.PutValue(value, bit_width);
    }
    // If there are not enough values to fill the last mini block, we pad the mini block
    // with zeroes so that its length is the number of values in a full mini block
    // multiplied by the bit width.
    for (uint32_t j = values_current_mini_block; j < values_per_mini_block_; j++) {
      bit_writer_.PutValue(0, bit_width);
    }
    values_current_block_ -= values_current_mini_block;
  }

  // If, in the last block, less than <number of miniblocks in a block> miniblocks are
  // needed to store the values, the bytes storing the bit widths of the unneeded
  // miniblocks are still present, their value should be zero, but readers must accept
  // arbitrary values as well.
  for (uint32_t i = num_miniblocks; i < mini_blocks_per_block_; i++) {
    bit_width_data[i] = 0;
  }
  DCHECK_EQ(values_current_block_, 0);

  bit_writer_.Flush();
  PARQUET_THROW_NOT_OK(sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
  bit_writer_.Clear();
}

template <typename DType>
std::shared_ptr<Buffer> DeltaBitPackEncoder<DType>::FlushValues() {
  if (values_current_block_ > 0) {
    FlushBlock();
  }
  PARQUET_ASSIGN_OR_THROW(auto buffer, sink_.Finish(/*shrink_to_fit=*/true));

  uint8_t header_buffer_[kMaxPageHeaderWriterSize] = {};
  bit_util::BitWriter header_writer(header_buffer_, sizeof(header_buffer_));
  if (!header_writer.PutVlqInt(values_per_block_) ||
      !header_writer.PutVlqInt(mini_blocks_per_block_) ||
      !header_writer.PutVlqInt(total_value_count_) ||
      !header_writer.PutZigZagVlqInt(static_cast<T>(first_value_))) {
    throw ParquetException("header writing error");
  }
  header_writer.Flush();

  // We reserved enough space at the beginning of the buffer for largest possible header
  // and data was written immediately after. We now write the header data immediately
  // before the end of reserved space.
  const size_t offset_bytes = kMaxPageHeaderWriterSize - header_writer.bytes_written();
  std::memcpy(buffer->mutable_data() + offset_bytes, header_buffer_,
              header_writer.bytes_written());

  // Reset counter of cached values
  total_value_count_ = 0;
  // Reserve enough space at the beginning of the buffer for largest possible header.
  PARQUET_THROW_NOT_OK(sink_.Advance(kMaxPageHeaderWriterSize));

  // Excess bytes at the beginning are sliced off and ignored.
  return SliceBuffer(buffer, offset_bytes);
}

template <>
void DeltaBitPackEncoder<Int32Type>::Put(const ::arrow::Array& values) {
  const ::arrow::ArrayData& data = *values.data();
  if (values.type_id() != ::arrow::Type::INT32) {
    throw ParquetException("Expected Int32TArray, got ", values.type()->ToString());
  }
  if (data.length > std::numeric_limits<int32_t>::max()) {
    throw ParquetException("Array cannot be longer than ",
                           std::numeric_limits<int32_t>::max());
  }

  if (values.null_count() == 0) {
    Put(data.GetValues<int32_t>(1), static_cast<int>(data.length));
  } else {
    PutSpaced(data.GetValues<int32_t>(1), static_cast<int>(data.length),
              data.GetValues<uint8_t>(0, 0), data.offset);
  }
}

template <>
void DeltaBitPackEncoder<Int64Type>::Put(const ::arrow::Array& values) {
  const ::arrow::ArrayData& data = *values.data();
  if (values.type_id() != ::arrow::Type::INT64) {
    throw ParquetException("Expected Int64TArray, got ", values.type()->ToString());
  }
  if (data.length > std::numeric_limits<int32_t>::max()) {
    throw ParquetException("Array cannot be longer than ",
                           std::numeric_limits<int32_t>::max());
  }
  if (values.null_count() == 0) {
    Put(data.GetValues<int64_t>(1), static_cast<int>(data.length));
  } else {
    PutSpaced(data.GetValues<int64_t>(1), static_cast<int>(data.length),
              data.GetValues<uint8_t>(0, 0), data.offset);
  }
}

template <typename DType>
void DeltaBitPackEncoder<DType>::PutSpaced(const T* src, int num_values,
                                           const uint8_t* valid_bits,
                                           int64_t valid_bits_offset) {
  if (valid_bits != NULLPTR) {
    PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                 this->memory_pool()));
    T* data = buffer->template mutable_data_as<T>();
    int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
        src, num_values, valid_bits, valid_bits_offset, data);
    Put(data, num_valid_values);
  } else {
    Put(src, num_values);
  }
}

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY encoder

class DeltaLengthByteArrayEncoder : public EncoderImpl,
                                    virtual public TypedEncoder<ByteArrayType> {
 public:
  explicit DeltaLengthByteArrayEncoder(const ColumnDescriptor* descr, MemoryPool* pool)
      : EncoderImpl(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY,
                    pool = ::arrow::default_memory_pool()),
        sink_(pool),
        length_encoder_(nullptr, pool) {}

  std::shared_ptr<Buffer> FlushValues() override;

  int64_t EstimatedDataEncodedSize() override {
    return sink_.length() + length_encoder_.EstimatedDataEncodedSize();
  }

  using TypedEncoder<ByteArrayType>::Put;

  void Put(const ::arrow::Array& values) override;

  void Put(const T* buffer, int num_values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override;

 protected:
  template <typename ArrayType>
  void PutBinaryArray(const ArrayType& array) {
    PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](::std::string_view view) {
          if (ARROW_PREDICT_FALSE(view.size() > kMaxByteArraySize)) {
            return Status::Invalid(
                "Parquet cannot store strings with size 2GB or more, got: ", view.size());
          }
          if (ARROW_PREDICT_FALSE(
                  view.size() + sink_.length() >
                  static_cast<size_t>(std::numeric_limits<int32_t>::max()))) {
            return Status::Invalid("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
          }
          length_encoder_.Put({static_cast<int32_t>(view.length())}, 1);
          PARQUET_THROW_NOT_OK(sink_.Append(view.data(), view.length()));
          unencoded_byte_array_data_bytes_ += view.size();
          return Status::OK();
        },
        []() { return Status::OK(); }));
  }

  ::arrow::BufferBuilder sink_;
  DeltaBitPackEncoder<Int32Type> length_encoder_;
};

void DeltaLengthByteArrayEncoder::Put(const ::arrow::Array& values) {
  AssertBaseBinary(values);
  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else {
    PutBinaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  }
}

void DeltaLengthByteArrayEncoder::Put(const T* src, int num_values) {
  if (num_values == 0) {
    return;
  }

  constexpr int kBatchSize = 256;
  std::array<int32_t, kBatchSize> lengths;
  uint32_t total_increment_size = 0;
  for (int idx = 0; idx < num_values; idx += kBatchSize) {
    const int batch_size = std::min(kBatchSize, num_values - idx);
    for (int j = 0; j < batch_size; ++j) {
      const int32_t len = src[idx + j].len;
      if (ARROW_PREDICT_FALSE(
              AddWithOverflow(total_increment_size, len, &total_increment_size))) {
        throw ParquetException("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
      }
      lengths[j] = len;
    }
    length_encoder_.Put(lengths.data(), batch_size);
  }
  if (sink_.length() + total_increment_size > std::numeric_limits<int32_t>::max()) {
    throw ParquetException("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
  }
  PARQUET_THROW_NOT_OK(sink_.Reserve(total_increment_size));
  for (int idx = 0; idx < num_values; idx++) {
    sink_.UnsafeAppend(src[idx].ptr, src[idx].len);
  }
  unencoded_byte_array_data_bytes_ += total_increment_size;
}

void DeltaLengthByteArrayEncoder::PutSpaced(const T* src, int num_values,
                                            const uint8_t* valid_bits,
                                            int64_t valid_bits_offset) {
  if (valid_bits != NULLPTR) {
    PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                 this->memory_pool()));
    T* data = buffer->template mutable_data_as<T>();
    int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
        src, num_values, valid_bits, valid_bits_offset, data);
    Put(data, num_valid_values);
  } else {
    Put(src, num_values);
  }
}

std::shared_ptr<Buffer> DeltaLengthByteArrayEncoder::FlushValues() {
  std::shared_ptr<Buffer> encoded_lengths = length_encoder_.FlushValues();

  std::shared_ptr<Buffer> data;
  PARQUET_THROW_NOT_OK(sink_.Finish(&data));
  sink_.Reset();

  PARQUET_THROW_NOT_OK(sink_.Resize(encoded_lengths->size() + data->size()));
  PARQUET_THROW_NOT_OK(sink_.Append(encoded_lengths->data(), encoded_lengths->size()));
  PARQUET_THROW_NOT_OK(sink_.Append(data->data(), data->size()));

  std::shared_ptr<Buffer> buffer;
  PARQUET_THROW_NOT_OK(sink_.Finish(&buffer, true));
  return buffer;
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY encoder

/// Delta Byte Array encoding also known as incremental encoding or front compression:
/// for each element in a sequence of strings, store the prefix length of the previous
/// entry plus the suffix.
///
/// This is stored as a sequence of delta-encoded prefix lengths (DELTA_BINARY_PACKED),
/// followed by the suffixes encoded as delta length byte arrays
/// (DELTA_LENGTH_BYTE_ARRAY).

template <typename DType>
class DeltaByteArrayEncoder : public EncoderImpl, virtual public TypedEncoder<DType> {
  static constexpr std::string_view kEmpty = "";

 public:
  using T = typename DType::c_type;

  explicit DeltaByteArrayEncoder(const ColumnDescriptor* descr,
                                 MemoryPool* pool = ::arrow::default_memory_pool())
      : EncoderImpl(descr, Encoding::DELTA_BYTE_ARRAY, pool),
        sink_(pool),
        prefix_length_encoder_(/*descr=*/nullptr, pool),
        suffix_encoder_(descr, pool),
        last_value_(""),
        empty_(static_cast<uint32_t>(kEmpty.size()),
               reinterpret_cast<const uint8_t*>(kEmpty.data())) {}

  std::shared_ptr<Buffer> FlushValues() override;

  int64_t EstimatedDataEncodedSize() override {
    return prefix_length_encoder_.EstimatedDataEncodedSize() +
           suffix_encoder_.EstimatedDataEncodedSize();
  }

  using TypedEncoder<DType>::Put;

  void Put(const ::arrow::Array& values) override;

  void Put(const T* buffer, int num_values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != nullptr) {
      if (buffer_ == nullptr) {
        PARQUET_ASSIGN_OR_THROW(buffer_,
                                ::arrow::AllocateResizableBuffer(num_values * sizeof(T),
                                                                 this->memory_pool()));
      } else {
        PARQUET_THROW_NOT_OK(buffer_->Resize(num_values * sizeof(T), false));
      }
      T* data = buffer_->mutable_data_as<T>();
      int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
          src, num_values, valid_bits, valid_bits_offset, data);
      Put(data, num_valid_values);
    } else {
      Put(src, num_values);
    }
  }

 protected:
  template <typename VisitorType>
  void PutInternal(const T* src, int num_values, const VisitorType visitor) {
    if (num_values == 0) {
      return;
    }

    std::string_view last_value_view = last_value_;
    constexpr int kBatchSize = 256;
    std::array<int32_t, kBatchSize> prefix_lengths;
    std::array<ByteArray, kBatchSize> suffixes;

    for (int i = 0; i < num_values; i += kBatchSize) {
      const int batch_size = std::min(kBatchSize, num_values - i);

      for (int j = 0; j < batch_size; ++j) {
        const int idx = i + j;
        const auto view = visitor[idx];
        const auto len = static_cast<const uint32_t>(view.length());

        uint32_t common_prefix_length = 0;
        const uint32_t maximum_common_prefix_length =
            std::min(len, static_cast<uint32_t>(last_value_view.length()));
        while (common_prefix_length < maximum_common_prefix_length) {
          if (last_value_view[common_prefix_length] != view[common_prefix_length]) {
            break;
          }
          common_prefix_length++;
        }

        last_value_view = view;
        prefix_lengths[j] = common_prefix_length;
        const uint32_t suffix_length = len - common_prefix_length;
        const uint8_t* suffix_ptr = src[idx].ptr + common_prefix_length;

        // Convert to ByteArray, so it can be passed to the suffix_encoder_.
        const ByteArray suffix(suffix_length, suffix_ptr);
        suffixes[j] = suffix;

        unencoded_byte_array_data_bytes_ += len;
      }
      suffix_encoder_.Put(suffixes.data(), batch_size);
      prefix_length_encoder_.Put(prefix_lengths.data(), batch_size);
    }
    last_value_ = last_value_view;
  }

  template <typename ArrayType>
  void PutBinaryArray(const ArrayType& array) {
    auto previous_len = static_cast<uint32_t>(last_value_.length());
    std::string_view last_value_view = last_value_;

    PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<typename ArrayType::TypeClass>(
        *array.data(),
        [&](::std::string_view view) {
          if (ARROW_PREDICT_FALSE(view.size() >= kMaxByteArraySize)) {
            return Status::Invalid(
                "Parquet cannot store strings with size 2GB or more, got: ", view.size());
          }
          const ByteArray src{view};

          uint32_t common_prefix_length = 0;
          const uint32_t len = src.len;
          const uint32_t maximum_common_prefix_length = std::min(previous_len, len);
          while (common_prefix_length < maximum_common_prefix_length) {
            if (last_value_view[common_prefix_length] != view[common_prefix_length]) {
              break;
            }
            common_prefix_length++;
          }
          previous_len = len;
          prefix_length_encoder_.Put({static_cast<int32_t>(common_prefix_length)}, 1);

          last_value_view = view;
          const auto suffix_length = static_cast<uint32_t>(len - common_prefix_length);
          if (suffix_length == 0) {
            suffix_encoder_.Put(&empty_, 1);
            return Status::OK();
          }
          const uint8_t* suffix_ptr = src.ptr + common_prefix_length;
          // Convert to ByteArray, so it can be passed to the suffix_encoder_.
          const ByteArray suffix(suffix_length, suffix_ptr);
          suffix_encoder_.Put(&suffix, 1);

          unencoded_byte_array_data_bytes_ += len;
          return Status::OK();
        },
        []() { return Status::OK(); }));
    last_value_ = last_value_view;
  }

  ::arrow::BufferBuilder sink_;
  DeltaBitPackEncoder<Int32Type> prefix_length_encoder_;
  DeltaLengthByteArrayEncoder suffix_encoder_;
  std::string last_value_;
  const ByteArray empty_;
  std::unique_ptr<ResizableBuffer> buffer_;
};

struct ByteArrayVisitor {
  const ByteArray* src;

  std::string_view operator[](int i) const {
    if (ARROW_PREDICT_FALSE(src[i].len >= kMaxByteArraySize)) {
      throw ParquetException("Parquet cannot store strings with size 2GB or more, got: ",
                             src[i].len);
    }
    return std::string_view{src[i]};
  }

  uint32_t len(int i) const { return src[i].len; }
};

struct FLBAVisitor {
  const FLBA* src;
  const uint32_t type_length;

  std::string_view operator[](int i) const {
    return std::string_view{reinterpret_cast<const char*>(src[i].ptr), type_length};
  }

  uint32_t len(int i) const { return type_length; }
};

template <>
void DeltaByteArrayEncoder<ByteArrayType>::Put(const ByteArray* src, int num_values) {
  auto visitor = ByteArrayVisitor{src};
  PutInternal<ByteArrayVisitor>(src, num_values, visitor);
}

template <>
void DeltaByteArrayEncoder<FLBAType>::Put(const FLBA* src, int num_values) {
  auto visitor = FLBAVisitor{src, static_cast<uint32_t>(descr_->type_length())};
  PutInternal<FLBAVisitor>(src, num_values, visitor);
}

template <typename DType>
void DeltaByteArrayEncoder<DType>::Put(const ::arrow::Array& values) {
  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else if (::arrow::is_large_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  } else if (::arrow::is_fixed_size_binary(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::FixedSizeBinaryArray&>(values));
  } else {
    throw ParquetException("Only BaseBinaryArray and subclasses supported");
  }
}

template <typename DType>
std::shared_ptr<Buffer> DeltaByteArrayEncoder<DType>::FlushValues() {
  PARQUET_THROW_NOT_OK(sink_.Resize(EstimatedDataEncodedSize(), false));

  std::shared_ptr<Buffer> prefix_lengths = prefix_length_encoder_.FlushValues();
  PARQUET_THROW_NOT_OK(sink_.Append(prefix_lengths->data(), prefix_lengths->size()));

  std::shared_ptr<Buffer> suffixes = suffix_encoder_.FlushValues();
  PARQUET_THROW_NOT_OK(sink_.Append(suffixes->data(), suffixes->size()));

  std::shared_ptr<Buffer> buffer;
  PARQUET_THROW_NOT_OK(sink_.Finish(&buffer, true));
  last_value_.clear();
  return buffer;
}

// ----------------------------------------------------------------------
// RLE encoder for BOOLEAN

class RleBooleanEncoder final : public EncoderImpl, virtual public BooleanEncoder {
 public:
  explicit RleBooleanEncoder(const ColumnDescriptor* descr, ::arrow::MemoryPool* pool)
      : EncoderImpl(descr, Encoding::RLE, pool),
        buffered_append_values_(::arrow::stl::allocator<T>(pool)) {}

  int64_t EstimatedDataEncodedSize() override {
    return kRleLengthInBytes + MaxRleBufferSize();
  }

  std::shared_ptr<Buffer> FlushValues() override;

  void Put(const T* buffer, int num_values) override;
  void Put(const ::arrow::Array& values) override {
    if (values.type_id() != ::arrow::Type::BOOL) {
      throw ParquetException("RleBooleanEncoder expects BooleanArray, got ",
                             values.type()->ToString());
    }
    const auto& boolean_array = checked_cast<const ::arrow::BooleanArray&>(values);
    if (values.null_count() == 0) {
      for (int i = 0; i < boolean_array.length(); ++i) {
        // null_count == 0, so just call Value directly is ok.
        buffered_append_values_.push_back(boolean_array.Value(i));
      }
    } else {
      PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<::arrow::BooleanType>(
          *boolean_array.data(),
          [&](bool value) {
            buffered_append_values_.push_back(value);
            return Status::OK();
          },
          []() { return Status::OK(); }));
    }
  }

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = buffer->mutable_data_as<T>();
      int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
          src, num_values, valid_bits, valid_bits_offset, data);
      Put(data, num_valid_values);
    } else {
      Put(src, num_values);
    }
  }

  void Put(const std::vector<bool>& src, int num_values) override;

 protected:
  template <typename SequenceType>
  void PutImpl(const SequenceType& src, int num_values);

  int MaxRleBufferSize() const noexcept {
    return RlePreserveBufferSize(static_cast<int>(buffered_append_values_.size()),
                                 kBitWidth);
  }

  constexpr static int32_t kBitWidth = 1;
  /// 4 bytes in little-endian, which indicates the length.
  constexpr static int32_t kRleLengthInBytes = 4;

  // std::vector<bool> in C++ is tricky, because it's a bitmap.
  // Here RleBooleanEncoder will only append values into it, and
  // dump values into Buffer, so using it here is ok.
  ArrowPoolVector<bool> buffered_append_values_;
};

void RleBooleanEncoder::Put(const bool* src, int num_values) { PutImpl(src, num_values); }

void RleBooleanEncoder::Put(const std::vector<bool>& src, int num_values) {
  PutImpl(src, num_values);
}

template <typename SequenceType>
void RleBooleanEncoder::PutImpl(const SequenceType& src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    buffered_append_values_.push_back(src[i]);
  }
}

std::shared_ptr<Buffer> RleBooleanEncoder::FlushValues() {
  int rle_buffer_size_max = MaxRleBufferSize();
  std::shared_ptr<ResizableBuffer> buffer =
      AllocateBuffer(this->pool_, rle_buffer_size_max + kRleLengthInBytes);
  ::arrow::util::RleEncoder encoder(buffer->mutable_data() + kRleLengthInBytes,
                                    rle_buffer_size_max, /*bit_width*/ kBitWidth);

  for (bool value : buffered_append_values_) {
    encoder.Put(value ? 1 : 0);
  }
  encoder.Flush();
  ::arrow::util::SafeStore(buffer->mutable_data(),
                           ::arrow::bit_util::ToLittleEndian(encoder.len()));
  PARQUET_THROW_NOT_OK(buffer->Resize(kRleLengthInBytes + encoder.len()));
  buffered_append_values_.clear();
  return buffer;
}

}  // namespace

// ----------------------------------------------------------------------
// Factory function

std::unique_ptr<Encoder> MakeEncoder(Type::type type_num, Encoding::type encoding,
                                     bool use_dictionary, const ColumnDescriptor* descr,
                                     MemoryPool* pool) {
  if (use_dictionary) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<DictEncoderImpl<Int32Type>>(descr, pool);
      case Type::INT64:
        return std::make_unique<DictEncoderImpl<Int64Type>>(descr, pool);
      case Type::INT96:
        return std::make_unique<DictEncoderImpl<Int96Type>>(descr, pool);
      case Type::FLOAT:
        return std::make_unique<DictEncoderImpl<FloatType>>(descr, pool);
      case Type::DOUBLE:
        return std::make_unique<DictEncoderImpl<DoubleType>>(descr, pool);
      case Type::BYTE_ARRAY:
        return std::make_unique<DictEncoderImpl<ByteArrayType>>(descr, pool);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<DictEncoderImpl<FLBAType>>(descr, pool);
      default:
        DCHECK(false) << "Encoder not implemented";
        break;
    }
  } else if (encoding == Encoding::PLAIN) {
    switch (type_num) {
      case Type::BOOLEAN:
        return std::make_unique<PlainEncoder<BooleanType>>(descr, pool);
      case Type::INT32:
        return std::make_unique<PlainEncoder<Int32Type>>(descr, pool);
      case Type::INT64:
        return std::make_unique<PlainEncoder<Int64Type>>(descr, pool);
      case Type::INT96:
        return std::make_unique<PlainEncoder<Int96Type>>(descr, pool);
      case Type::FLOAT:
        return std::make_unique<PlainEncoder<FloatType>>(descr, pool);
      case Type::DOUBLE:
        return std::make_unique<PlainEncoder<DoubleType>>(descr, pool);
      case Type::BYTE_ARRAY:
        return std::make_unique<PlainEncoder<ByteArrayType>>(descr, pool);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<PlainEncoder<FLBAType>>(descr, pool);
      default:
        DCHECK(false) << "Encoder not implemented";
        break;
    }
  } else if (encoding == Encoding::BYTE_STREAM_SPLIT) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<ByteStreamSplitEncoder<Int32Type>>(descr, pool);
      case Type::INT64:
        return std::make_unique<ByteStreamSplitEncoder<Int64Type>>(descr, pool);
      case Type::FLOAT:
        return std::make_unique<ByteStreamSplitEncoder<FloatType>>(descr, pool);
      case Type::DOUBLE:
        return std::make_unique<ByteStreamSplitEncoder<DoubleType>>(descr, pool);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<ByteStreamSplitEncoder<FLBAType>>(descr, pool);
      default:
        throw ParquetException(
            "BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64 "
            "and FIXED_LEN_BYTE_ARRAY");
    }
  } else if (encoding == Encoding::DELTA_BINARY_PACKED) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<DeltaBitPackEncoder<Int32Type>>(descr, pool);
      case Type::INT64:
        return std::make_unique<DeltaBitPackEncoder<Int64Type>>(descr, pool);
      default:
        throw ParquetException(
            "DELTA_BINARY_PACKED encoder only supports INT32 and INT64");
    }
  } else if (encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) {
    switch (type_num) {
      case Type::BYTE_ARRAY:
        return std::make_unique<DeltaLengthByteArrayEncoder>(descr, pool);
      default:
        throw ParquetException("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY");
    }
  } else if (encoding == Encoding::RLE) {
    switch (type_num) {
      case Type::BOOLEAN:
        return std::make_unique<RleBooleanEncoder>(descr, pool);
      default:
        throw ParquetException("RLE only supports BOOLEAN");
    }
  } else if (encoding == Encoding::DELTA_BYTE_ARRAY) {
    switch (type_num) {
      case Type::BYTE_ARRAY:
        return std::make_unique<DeltaByteArrayEncoder<ByteArrayType>>(descr, pool);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_unique<DeltaByteArrayEncoder<FLBAType>>(descr, pool);
      default:
        throw ParquetException(
            "DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY");
    }
  } else {
    ParquetException::NYI("Selected encoding is not supported");
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
}

}  // namespace parquet
