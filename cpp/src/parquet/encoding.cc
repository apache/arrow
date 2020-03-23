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
#include "arrow/stl_allocator.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Status;
using arrow::internal::checked_cast;

template <typename T>
using ArrowPoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

namespace parquet {

constexpr int64_t kInMemoryDefaultCapacity = 1024;

class EncoderImpl : virtual public Encoder {
 public:
  EncoderImpl(const ColumnDescriptor* descr, Encoding::type encoding, MemoryPool* pool)
      : descr_(descr),
        encoding_(encoding),
        pool_(pool),
        type_length_(descr ? descr->type_length() : -1) {}

  Encoding::type encoding() const override { return encoding_; }

  MemoryPool* memory_pool() const override { return pool_; }

 protected:
  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;
  const Encoding::type encoding_;
  MemoryPool* pool_;

  /// Type length from descr
  int type_length_;
};

// ----------------------------------------------------------------------
// Plain encoder implementation

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

  void Put(const arrow::Array& values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    std::shared_ptr<ResizableBuffer> buffer;
    PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(this->memory_pool(),
                                                        num_values * sizeof(T), &buffer));
    int32_t num_valid_values = 0;
    arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
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

  void UnsafePutByteArray(const void* data, uint32_t length) {
    DCHECK(length == 0 || data != nullptr) << "Value ptr cannot be NULL";
    sink_.UnsafeAppend(&length, sizeof(uint32_t));
    sink_.UnsafeAppend(data, static_cast<int64_t>(length));
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
  arrow::BufferBuilder sink_;
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
void DirectPutImpl(const arrow::Array& values, arrow::BufferBuilder* sink) {
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
void PlainEncoder<Int32Type>::Put(const arrow::Array& values) {
  DirectPutImpl<arrow::Int32Array>(values, &sink_);
}

template <>
void PlainEncoder<Int64Type>::Put(const arrow::Array& values) {
  DirectPutImpl<arrow::Int64Array>(values, &sink_);
}

template <>
void PlainEncoder<Int96Type>::Put(const arrow::Array& values) {
  ParquetException::NYI("direct put to Int96");
}

template <>
void PlainEncoder<FloatType>::Put(const arrow::Array& values) {
  DirectPutImpl<arrow::FloatArray>(values, &sink_);
}

template <>
void PlainEncoder<DoubleType>::Put(const arrow::Array& values) {
  DirectPutImpl<arrow::DoubleArray>(values, &sink_);
}

template <typename DType>
void PlainEncoder<DType>::Put(const arrow::Array& values) {
  ParquetException::NYI("direct put of " + values.type()->ToString());
}

void AssertBinary(const arrow::Array& values) {
  if (values.type_id() != arrow::Type::BINARY &&
      values.type_id() != arrow::Type::STRING) {
    throw ParquetException("Only BinaryArray and subclasses supported");
  }
}

template <>
inline void PlainEncoder<ByteArrayType>::Put(const arrow::Array& values) {
  AssertBinary(values);
  const auto& data = checked_cast<const arrow::BinaryArray&>(values);
  const int64_t total_bytes = data.value_offset(data.length()) - data.value_offset(0);
  PARQUET_THROW_NOT_OK(sink_.Reserve(total_bytes + data.length() * sizeof(uint32_t)));

  if (data.null_count() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < data.length(); i++) {
      auto view = data.GetView(i);
      UnsafePutByteArray(view.data(), static_cast<uint32_t>(view.size()));
    }
  } else {
    for (int64_t i = 0; i < data.length(); i++) {
      if (data.IsValid(i)) {
        auto view = data.GetView(i);
        UnsafePutByteArray(view.data(), static_cast<uint32_t>(view.size()));
      }
    }
  }
}

void AssertFixedSizeBinary(const arrow::Array& values, int type_length) {
  if (values.type_id() != arrow::Type::FIXED_SIZE_BINARY &&
      values.type_id() != arrow::Type::DECIMAL) {
    throw ParquetException("Only FixedSizeBinaryArray and subclasses supported");
  }
  if (checked_cast<const arrow::FixedSizeBinaryType&>(*values.type()).byte_width() !=
      type_length) {
    throw ParquetException("Size mismatch: " + values.type()->ToString() +
                           " should have been " + std::to_string(type_length) + " wide");
  }
}

template <>
inline void PlainEncoder<FLBAType>::Put(const arrow::Array& values) {
  AssertFixedSizeBinary(values, descr_->type_length());
  const auto& data = checked_cast<const arrow::FixedSizeBinaryArray&>(values);

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
      : EncoderImpl(descr, Encoding::PLAIN, pool),
        bits_available_(kInMemoryDefaultCapacity * 8),
        bits_buffer_(AllocateBuffer(pool, kInMemoryDefaultCapacity)),
        sink_(pool),
        bit_writer_(bits_buffer_->mutable_data(),
                    static_cast<int>(bits_buffer_->size())) {}

  int64_t EstimatedDataEncodedSize() override;
  std::shared_ptr<Buffer> FlushValues() override;

  void Put(const bool* src, int num_values) override;

  void Put(const std::vector<bool>& src, int num_values) override;

  void PutSpaced(const bool* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    std::shared_ptr<ResizableBuffer> buffer;
    PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(this->memory_pool(),
                                                        num_values * sizeof(T), &buffer));
    int32_t num_valid_values = 0;
    arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
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

  void Put(const arrow::Array& values) override {
    if (values.type_id() != arrow::Type::BOOL) {
      throw ParquetException("direct put to boolean from " + values.type()->ToString() +
                             " not supported");
    }

    const auto& data = checked_cast<const arrow::BooleanArray&>(values);
    if (data.null_count() == 0) {
      PARQUET_THROW_NOT_OK(sink_.Reserve(BitUtil::BytesForBits(data.length())));
      // no nulls, just dump the data
      arrow::internal::CopyBitmap(data.data()->GetValues<uint8_t>(1), data.offset(),
                                  data.length(), sink_.mutable_data(), sink_.length());
      sink_.UnsafeAdvance(data.length());
    } else {
      auto n_valid = BitUtil::BytesForBits(data.length() - data.null_count());
      PARQUET_THROW_NOT_OK(sink_.Reserve(n_valid));
      arrow::internal::FirstTimeBitmapWriter writer(sink_.mutable_data(), sink_.length(),
                                                    n_valid);

      for (int64_t i = 0; i < data.length(); i++) {
        if (data.IsValid(i)) {
          if (data.Value(i)) {
            writer.Set();
          } else {
            writer.Clear();
          }
          writer.Next();
        }
      }
      writer.Finish();
    }
  }

 private:
  int bits_available_;
  std::shared_ptr<ResizableBuffer> bits_buffer_;
  arrow::BufferBuilder sink_;
  arrow::BitUtil::BitWriter bit_writer_;

  template <typename SequenceType>
  void PutImpl(const SequenceType& src, int num_values);
};

template <typename SequenceType>
void PlainEncoder<BooleanType>::PutImpl(const SequenceType& src, int num_values) {
  int bit_offset = 0;
  if (bits_available_ > 0) {
    int bits_to_write = std::min(bits_available_, num_values);
    for (int i = 0; i < bits_to_write; i++) {
      bit_writer_.PutValue(src[i], 1);
    }
    bits_available_ -= bits_to_write;
    bit_offset = bits_to_write;

    if (bits_available_ == 0) {
      bit_writer_.Flush();
      PARQUET_THROW_NOT_OK(
          sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
      bit_writer_.Clear();
    }
  }

  int bits_remaining = num_values - bit_offset;
  while (bit_offset < num_values) {
    bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;

    int bits_to_write = std::min(bits_available_, bits_remaining);
    for (int i = bit_offset; i < bit_offset + bits_to_write; i++) {
      bit_writer_.PutValue(src[i], 1);
    }
    bit_offset += bits_to_write;
    bits_available_ -= bits_to_write;
    bits_remaining -= bits_to_write;

    if (bits_available_ == 0) {
      bit_writer_.Flush();
      PARQUET_THROW_NOT_OK(
          sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
      bit_writer_.Clear();
    }
  }
}

int64_t PlainEncoder<BooleanType>::EstimatedDataEncodedSize() {
  int64_t position = sink_.length();
  return position + bit_writer_.bytes_written();
}

std::shared_ptr<Buffer> PlainEncoder<BooleanType>::FlushValues() {
  if (bits_available_ > 0) {
    bit_writer_.Flush();
    PARQUET_THROW_NOT_OK(sink_.Append(bit_writer_.buffer(), bit_writer_.bytes_written()));
    bit_writer_.Clear();
    bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;
  }

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
  using MemoTableType = arrow::internal::ScalarMemoTable<c_type>;
};

template <>
struct DictEncoderTraits<ByteArrayType> {
  using MemoTableType = arrow::internal::BinaryMemoTable<arrow::BinaryBuilder>;
};

template <>
struct DictEncoderTraits<FLBAType> {
  using MemoTableType = arrow::internal::BinaryMemoTable<arrow::BinaryBuilder>;
};

// Initially 1024 elements
static constexpr int32_t kInitialHashTableSize = 1 << 10;

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

  explicit DictEncoderImpl(const ColumnDescriptor* desc, MemoryPool* pool)
      : EncoderImpl(desc, Encoding::PLAIN_DICTIONARY, pool),
        buffered_indices_(::arrow::stl::allocator<int32_t>(pool)),
        dict_encoded_size_(0),
        memo_table_(pool, kInitialHashTableSize) {}

  ~DictEncoderImpl() override { DCHECK(buffered_indices_.empty()); }

  int dict_encoded_size() override { return dict_encoded_size_; }

  int WriteIndices(uint8_t* buffer, int buffer_len) override {
    // Write bit width in first byte
    *buffer = static_cast<uint8_t>(bit_width());
    ++buffer;
    --buffer_len;

    arrow::util::RleEncoder encoder(buffer, buffer_len, bit_width());

    for (int32_t index : buffered_indices_) {
      if (!encoder.Put(index)) return -1;
    }
    encoder.Flush();

    ClearIndices();
    return 1 + encoder.len();
  }

  void set_type_length(int type_length) { this->type_length_ = type_length; }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteIndices().
  int64_t EstimatedDataEncodedSize() override {
    // Note: because of the way RleEncoder::CheckBufferFull() is called, we have to
    // reserve
    // an extra "RleEncoder::MinBufferSize" bytes. These extra bytes won't be used
    // but not reserving them would cause the encoder to fail.
    return 1 +
           arrow::util::RleEncoder::MaxBufferSize(
               bit_width(), static_cast<int>(buffered_indices_.size())) +
           arrow::util::RleEncoder::MinBufferSize(bit_width());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const override {
    if (ARROW_PREDICT_FALSE(num_entries() == 0)) return 0;
    if (ARROW_PREDICT_FALSE(num_entries() == 1)) return 1;
    return BitUtil::Log2(num_entries());
  }

  /// Encode value. Note that this does not actually write any data, just
  /// buffers the value's index to be written later.
  inline void Put(const T& value);

  // Not implemented for other data types
  inline void PutByteArray(const void* ptr, int32_t length);

  void Put(const T* src, int num_values) override {
    for (int32_t i = 0; i < num_values; i++) {
      Put(src[i]);
    }
  }

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                    num_values);
    for (int32_t i = 0; i < num_values; i++) {
      if (valid_bits_reader.IsSet()) {
        Put(src[i]);
      }
      valid_bits_reader.Next();
    }
  }

  using TypedEncoder<DType>::Put;

  void Put(const arrow::Array& values) override;
  void PutDictionary(const arrow::Array& values) override;

  template <typename ArrowType>
  void PutIndicesTyped(const arrow::Array& data) {
    using ArrayType = typename arrow::TypeTraits<ArrowType>::ArrayType;
    const auto& indices = checked_cast<const ArrayType&>(data);
    auto values = indices.raw_values();

    size_t buffer_position = buffered_indices_.size();
    buffered_indices_.resize(
        buffer_position + static_cast<size_t>(indices.length() - indices.null_count()));
    if (indices.null_count() > 0) {
      arrow::internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(),
                                                      indices.offset(), indices.length());
      for (int64_t i = 0; i < indices.length(); ++i) {
        if (valid_bits_reader.IsSet()) {
          buffered_indices_[buffer_position++] = static_cast<int32_t>(values[i]);
        }
        valid_bits_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < indices.length(); ++i) {
        buffered_indices_[buffer_position++] = static_cast<int32_t>(values[i]);
      }
    }
  }

  void PutIndices(const arrow::Array& data) override {
    switch (data.type()->id()) {
      case arrow::Type::INT8:
        return PutIndicesTyped<arrow::Int8Type>(data);
      case arrow::Type::INT16:
        return PutIndicesTyped<arrow::Int16Type>(data);
      case arrow::Type::INT32:
        return PutIndicesTyped<arrow::Int32Type>(data);
      case arrow::Type::INT64:
        return PutIndicesTyped<arrow::Int64Type>(data);
      default:
        throw ParquetException("Dictionary indices were not signed integer");
    }
  }

  std::shared_ptr<Buffer> FlushValues() override {
    std::shared_ptr<ResizableBuffer> buffer =
        AllocateBuffer(this->pool_, EstimatedDataEncodedSize());
    int result_size = WriteIndices(buffer->mutable_data(),
                                   static_cast<int>(EstimatedDataEncodedSize()));
    PARQUET_THROW_NOT_OK(buffer->Resize(result_size, false));
    return std::move(buffer);
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  void WriteDict(uint8_t* buffer) override;

  /// The number of entries in the dictionary.
  int num_entries() const override { return memo_table_.size(); }

 private:
  /// Clears all the indices (but leaves the dictionary).
  void ClearIndices() { buffered_indices_.clear(); }

  /// Indices that have not yet be written out by WriteIndices().
  ArrowPoolVector<int32_t> buffered_indices_;

  /// The number of bytes needed to encode the dictionary.
  int dict_encoded_size_;

  MemoTableType memo_table_;
};

template <typename DType>
void DictEncoderImpl<DType>::WriteDict(uint8_t* buffer) {
  // For primitive types, only a memcpy
  DCHECK_EQ(static_cast<size_t>(dict_encoded_size_), sizeof(T) * memo_table_.size());
  memo_table_.CopyValues(0 /* start_pos */, reinterpret_cast<T*>(buffer));
}

// ByteArray and FLBA already have the dictionary encoded in their data heaps
template <>
void DictEncoderImpl<ByteArrayType>::WriteDict(uint8_t* buffer) {
  memo_table_.VisitValues(0, [&buffer](const arrow::util::string_view& v) {
    uint32_t len = static_cast<uint32_t>(v.length());
    memcpy(buffer, &len, sizeof(len));
    buffer += sizeof(len);
    memcpy(buffer, v.data(), len);
    buffer += len;
  });
}

template <>
void DictEncoderImpl<FLBAType>::WriteDict(uint8_t* buffer) {
  memo_table_.VisitValues(0, [&](const arrow::util::string_view& v) {
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
void DictEncoderImpl<Int96Type>::Put(const arrow::Array& values) {
  ParquetException::NYI("Direct put to Int96");
}

template <>
void DictEncoderImpl<Int96Type>::PutDictionary(const arrow::Array& values) {
  ParquetException::NYI("Direct put to Int96");
}

template <typename DType>
void DictEncoderImpl<DType>::Put(const arrow::Array& values) {
  using ArrayType = typename arrow::CTypeTraits<typename DType::c_type>::ArrayType;
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
void DictEncoderImpl<FLBAType>::Put(const arrow::Array& values) {
  AssertFixedSizeBinary(values, type_length_);
  const auto& data = checked_cast<const arrow::FixedSizeBinaryArray&>(values);
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
void DictEncoderImpl<ByteArrayType>::Put(const arrow::Array& values) {
  AssertBinary(values);
  const auto& data = checked_cast<const arrow::BinaryArray&>(values);
  if (data.null_count() == 0) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < data.length(); i++) {
      auto view = data.GetView(i);
      PutByteArray(view.data(), static_cast<int32_t>(view.size()));
    }
  } else {
    for (int64_t i = 0; i < data.length(); i++) {
      if (data.IsValid(i)) {
        auto view = data.GetView(i);
        PutByteArray(view.data(), static_cast<int32_t>(view.size()));
      }
    }
  }
}

template <typename DType>
void AssertCanPutDictionary(DictEncoderImpl<DType>* encoder, const arrow::Array& dict) {
  if (dict.null_count() > 0) {
    throw ParquetException("Inserted dictionary cannot cannot contain nulls");
  }

  if (encoder->num_entries() > 0) {
    throw ParquetException("Can only call PutDictionary on an empty DictEncoder");
  }
}

template <typename DType>
void DictEncoderImpl<DType>::PutDictionary(const arrow::Array& values) {
  AssertCanPutDictionary(this, values);

  using ArrayType = typename arrow::CTypeTraits<typename DType::c_type>::ArrayType;
  const auto& data = checked_cast<const ArrayType&>(values);

  dict_encoded_size_ += static_cast<int>(sizeof(typename DType::c_type) * data.length());
  for (int64_t i = 0; i < data.length(); i++) {
    int32_t unused_memo_index;
    PARQUET_THROW_NOT_OK(memo_table_.GetOrInsert(data.Value(i), &unused_memo_index));
  }
}

template <>
void DictEncoderImpl<FLBAType>::PutDictionary(const arrow::Array& values) {
  AssertFixedSizeBinary(values, type_length_);
  AssertCanPutDictionary(this, values);

  const auto& data = checked_cast<const arrow::FixedSizeBinaryArray&>(values);

  dict_encoded_size_ += static_cast<int>(type_length_ * data.length());
  for (int64_t i = 0; i < data.length(); i++) {
    int32_t unused_memo_index;
    PARQUET_THROW_NOT_OK(
        memo_table_.GetOrInsert(data.Value(i), type_length_, &unused_memo_index));
  }
}

template <>
void DictEncoderImpl<ByteArrayType>::PutDictionary(const arrow::Array& values) {
  AssertBinary(values);
  AssertCanPutDictionary(this, values);

  const auto& data = checked_cast<const arrow::BinaryArray&>(values);

  for (int64_t i = 0; i < data.length(); i++) {
    auto v = data.GetView(i);
    dict_encoded_size_ += static_cast<int>(v.size() + sizeof(uint32_t));
    int32_t unused_memo_index;
    PARQUET_THROW_NOT_OK(memo_table_.GetOrInsert(v.data(), static_cast<int32_t>(v.size()),
                                                 &unused_memo_index));
  }
}

// ----------------------------------------------------------------------
// ByteStreamSplitEncoder<T> implementations

template <typename DType>
class ByteStreamSplitEncoder : public EncoderImpl, virtual public TypedEncoder<DType> {
 public:
  using T = typename DType::c_type;
  using TypedEncoder<DType>::Put;

  explicit ByteStreamSplitEncoder(
      const ColumnDescriptor* descr,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  int64_t EstimatedDataEncodedSize() override;
  std::shared_ptr<Buffer> FlushValues() override;

  void Put(const T* buffer, int num_values) override;
  void Put(const arrow::Array& values) override;
  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override;

 protected:
  arrow::TypedBufferBuilder<T> values_;

 private:
  void PutArrowArray(const arrow::Array& values);
};

template <typename DType>
ByteStreamSplitEncoder<DType>::ByteStreamSplitEncoder(const ColumnDescriptor* descr,
                                                      ::arrow::MemoryPool* pool)
    : EncoderImpl(descr, Encoding::BYTE_STREAM_SPLIT, pool), values_{pool} {}

template <typename DType>
int64_t ByteStreamSplitEncoder<DType>::EstimatedDataEncodedSize() {
  return values_.length() * sizeof(T);
}

template <typename DType>
std::shared_ptr<Buffer> ByteStreamSplitEncoder<DType>::FlushValues() {
  constexpr size_t num_streams = sizeof(T);
  std::shared_ptr<ResizableBuffer> output_buffer =
      AllocateBuffer(this->memory_pool(), EstimatedDataEncodedSize());
  uint8_t* output_buffer_raw = output_buffer->mutable_data();
  const size_t num_values = values_.length();
  const uint8_t* raw_values = reinterpret_cast<const uint8_t*>(values_.data());
  for (size_t i = 0; i < num_values; ++i) {
    for (size_t j = 0U; j < num_streams; ++j) {
      const uint8_t byte_in_value = raw_values[i * num_streams + j];
      output_buffer_raw[j * num_values + i] = byte_in_value;
    }
  }
  values_.Reset();
  return std::move(output_buffer);
}

template <typename DType>
void ByteStreamSplitEncoder<DType>::Put(const T* buffer, int num_values) {
  PARQUET_THROW_NOT_OK(values_.Append(buffer, num_values));
}

template <typename DType>
void ByteStreamSplitEncoder<DType>::Put(const ::arrow::Array& values) {
  PutArrowArray(values);
}

template <>
void ByteStreamSplitEncoder<FloatType>::PutArrowArray(const ::arrow::Array& values) {
  DirectPutImpl<arrow::FloatArray>(values,
                                   reinterpret_cast<arrow::BufferBuilder*>(&values_));
}

template <>
void ByteStreamSplitEncoder<DoubleType>::PutArrowArray(const ::arrow::Array& values) {
  DirectPutImpl<arrow::DoubleArray>(values,
                                    reinterpret_cast<arrow::BufferBuilder*>(&values_));
}

template <typename DType>
void ByteStreamSplitEncoder<DType>::PutSpaced(const T* src, int num_values,
                                              const uint8_t* valid_bits,
                                              int64_t valid_bits_offset) {
  std::shared_ptr<ResizableBuffer> buffer;
  PARQUET_THROW_NOT_OK(arrow::AllocateResizableBuffer(this->memory_pool(),
                                                      num_values * sizeof(T), &buffer));
  int32_t num_valid_values = 0;
  arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
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

// ----------------------------------------------------------------------
// Encoder and decoder factory functions

std::unique_ptr<Encoder> MakeEncoder(Type::type type_num, Encoding::type encoding,
                                     bool use_dictionary, const ColumnDescriptor* descr,
                                     MemoryPool* pool) {
  if (use_dictionary) {
    switch (type_num) {
      case Type::INT32:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<Int32Type>(descr, pool));
      case Type::INT64:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<Int64Type>(descr, pool));
      case Type::INT96:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<Int96Type>(descr, pool));
      case Type::FLOAT:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<FloatType>(descr, pool));
      case Type::DOUBLE:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<DoubleType>(descr, pool));
      case Type::BYTE_ARRAY:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<ByteArrayType>(descr, pool));
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::unique_ptr<Encoder>(new DictEncoderImpl<FLBAType>(descr, pool));
      default:
        DCHECK(false) << "Encoder not implemented";
        break;
    }
  } else if (encoding == Encoding::PLAIN) {
    switch (type_num) {
      case Type::BOOLEAN:
        return std::unique_ptr<Encoder>(new PlainEncoder<BooleanType>(descr, pool));
      case Type::INT32:
        return std::unique_ptr<Encoder>(new PlainEncoder<Int32Type>(descr, pool));
      case Type::INT64:
        return std::unique_ptr<Encoder>(new PlainEncoder<Int64Type>(descr, pool));
      case Type::INT96:
        return std::unique_ptr<Encoder>(new PlainEncoder<Int96Type>(descr, pool));
      case Type::FLOAT:
        return std::unique_ptr<Encoder>(new PlainEncoder<FloatType>(descr, pool));
      case Type::DOUBLE:
        return std::unique_ptr<Encoder>(new PlainEncoder<DoubleType>(descr, pool));
      case Type::BYTE_ARRAY:
        return std::unique_ptr<Encoder>(new PlainEncoder<ByteArrayType>(descr, pool));
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::unique_ptr<Encoder>(new PlainEncoder<FLBAType>(descr, pool));
      default:
        DCHECK(false) << "Encoder not implemented";
        break;
    }
  } else if (encoding == Encoding::BYTE_STREAM_SPLIT) {
    switch (type_num) {
      case Type::FLOAT:
        return std::unique_ptr<Encoder>(
            new ByteStreamSplitEncoder<FloatType>(descr, pool));
      case Type::DOUBLE:
        return std::unique_ptr<Encoder>(
            new ByteStreamSplitEncoder<DoubleType>(descr, pool));
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

}  // namespace parquet
