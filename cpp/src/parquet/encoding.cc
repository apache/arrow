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
#include "arrow/array/builder_dict.h"
#include "arrow/stl_allocator.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_stream_utils.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/byte_stream_split.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/rle_encoding.h"
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
using arrow::internal::checked_cast;
using arrow::util::SafeLoad;
using arrow::util::SafeLoadAs;
using std::string_view;

template <typename T>
using ArrowPoolVector = std::vector<T, ::arrow::stl::allocator<T>>;

namespace parquet {
namespace {

constexpr int64_t kInMemoryDefaultCapacity = 1024;
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

  void Put(const ::arrow::Array& values) override;

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = reinterpret_cast<T*>(buffer->mutable_data());
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
            return Status::Invalid("Parquet cannot store strings with size 2GB or more");
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
  if (values.type_id() != ::arrow::Type::FIXED_SIZE_BINARY &&
      values.type_id() != ::arrow::Type::DECIMAL) {
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
    if (valid_bits != NULLPTR) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                   this->memory_pool()));
      T* data = reinterpret_cast<T*>(buffer->mutable_data());
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
      PARQUET_THROW_NOT_OK(sink_.Reserve(bit_util::BytesForBits(data.length())));
      // no nulls, just dump the data
      ::arrow::internal::CopyBitmap(data.data()->GetValues<uint8_t>(1), data.offset(),
                                    data.length(), sink_.mutable_data(), sink_.length());
    } else {
      auto n_valid = bit_util::BytesForBits(data.length() - data.null_count());
      PARQUET_THROW_NOT_OK(sink_.Reserve(n_valid));
      ::arrow::internal::FirstTimeBitmapWriter writer(sink_.mutable_data(),
                                                      sink_.length(), n_valid);

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
    sink_.UnsafeAdvance(data.length());
  }

 private:
  int bits_available_;
  std::shared_ptr<ResizableBuffer> bits_buffer_;
  ::arrow::BufferBuilder sink_;
  ::arrow::bit_util::BitWriter bit_writer_;

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

  int dict_encoded_size() const override { return dict_encoded_size_; }

  int WriteIndices(uint8_t* buffer, int buffer_len) override {
    // Write bit width in first byte
    *buffer = static_cast<uint8_t>(bit_width());
    ++buffer;
    --buffer_len;

    ::arrow::util::RleEncoder encoder(buffer, buffer_len, bit_width());

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
           ::arrow::util::RleEncoder::MaxBufferSize(
               bit_width(), static_cast<int>(buffered_indices_.size())) +
           ::arrow::util::RleEncoder::MinBufferSize(bit_width());
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
    return std::move(buffer);
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
            return Status::Invalid("Parquet cannot store strings with size 2GB or more");
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
        throw ParquetException("Parquet cannot store strings with size 2GB or more");
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
    throw ParquetException("Inserted dictionary cannot cannot contain nulls");
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
  void Put(const ::arrow::Array& values) override;
  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override;

 protected:
  template <typename ArrowType>
  void PutImpl(const ::arrow::Array& values) {
    if (values.type_id() != ArrowType::type_id) {
      throw ParquetException(std::string() + "direct put to " + ArrowType::type_name() +
                             " from " + values.type()->ToString() + " not supported");
    }
    const auto& data = *values.data();
    PutSpaced(data.GetValues<typename ArrowType::c_type>(1),
              static_cast<int>(data.length), data.GetValues<uint8_t>(0, 0), data.offset);
  }

  ::arrow::BufferBuilder sink_;
  int64_t num_values_in_buffer_;
};

template <typename DType>
ByteStreamSplitEncoder<DType>::ByteStreamSplitEncoder(const ColumnDescriptor* descr,
                                                      ::arrow::MemoryPool* pool)
    : EncoderImpl(descr, Encoding::BYTE_STREAM_SPLIT, pool),
      sink_{pool},
      num_values_in_buffer_{0} {}

template <typename DType>
int64_t ByteStreamSplitEncoder<DType>::EstimatedDataEncodedSize() {
  return sink_.length();
}

template <typename DType>
std::shared_ptr<Buffer> ByteStreamSplitEncoder<DType>::FlushValues() {
  std::shared_ptr<ResizableBuffer> output_buffer =
      AllocateBuffer(this->memory_pool(), EstimatedDataEncodedSize());
  uint8_t* output_buffer_raw = output_buffer->mutable_data();
  const uint8_t* raw_values = sink_.data();
  ::arrow::util::internal::ByteStreamSplitEncode<T>(raw_values, num_values_in_buffer_,
                                                    output_buffer_raw);
  sink_.Reset();
  num_values_in_buffer_ = 0;
  return std::move(output_buffer);
}

template <typename DType>
void ByteStreamSplitEncoder<DType>::Put(const T* buffer, int num_values) {
  if (num_values > 0) {
    PARQUET_THROW_NOT_OK(sink_.Append(buffer, num_values * sizeof(T)));
    num_values_in_buffer_ += num_values;
  }
}

template <>
void ByteStreamSplitEncoder<FloatType>::Put(const ::arrow::Array& values) {
  PutImpl<::arrow::FloatType>(values);
}

template <>
void ByteStreamSplitEncoder<DoubleType>::Put(const ::arrow::Array& values) {
  PutImpl<::arrow::DoubleType>(values);
}

template <typename DType>
void ByteStreamSplitEncoder<DType>::PutSpaced(const T* src, int num_values,
                                              const uint8_t* valid_bits,
                                              int64_t valid_bits_offset) {
  if (valid_bits != NULLPTR) {
    PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                 this->memory_pool()));
    T* data = reinterpret_cast<T*>(buffer->mutable_data());
    int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
        src, num_values, valid_bits, valid_bits_offset, data);
    Put(data, num_valid_values);
  } else {
    Put(src, num_values);
  }
}

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

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        builder->UnsafeAppend(SafeLoadAs<value_type>(data_));
        data_ += sizeof(value_type);
      },
      [&]() { builder->UnsafeAppendNull(); });

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

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        PARQUET_THROW_NOT_OK(builder->Append(SafeLoadAs<value_type>(data_)));
        data_ += sizeof(value_type);
      },
      [&]() { PARQUET_THROW_NOT_OK(builder->AppendNull()); });

  num_values_ -= values_decoded;
  len_ -= sizeof(value_type) * values_decoded;
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
    memcpy(out, data, bytes_to_decode);
  }
  return static_cast<int>(bytes_to_decode);
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
    out[i].ptr = data;
    data += type_length;
    data_size -= type_length;
  }
  return static_cast<int>(bytes_to_decode);
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

class PlainBooleanDecoder : public DecoderImpl, virtual public BooleanDecoder {
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
};

PlainBooleanDecoder::PlainBooleanDecoder(const ColumnDescriptor* descr)
    : DecoderImpl(descr, Encoding::PLAIN) {}

void PlainBooleanDecoder::SetData(int num_values, const uint8_t* data, int len) {
  num_values_ = num_values;
  bit_reader_ = std::make_unique<bit_util::BitReader>(data, len);
}

int PlainBooleanDecoder::DecodeArrow(
    int num_values, int null_count, const uint8_t* valid_bits, int64_t valid_bits_offset,
    typename EncodingTraits<BooleanType>::Accumulator* builder) {
  int values_decoded = num_values - null_count;
  if (ARROW_PREDICT_FALSE(num_values_ < values_decoded)) {
    ParquetException::EofException();
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        bool value;
        ARROW_IGNORE_EXPR(bit_reader_->GetValue(1, &value));
        builder->UnsafeAppend(value);
      },
      [&]() { builder->UnsafeAppendNull(); });

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
  ::arrow::internal::BitmapWriter bit_writer(buffer, 0, max_values);
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
  ::arrow::BinaryBuilder* builder;
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

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        builder->UnsafeAppend(data_);
        data_ += descr_->type_length();
      },
      [&]() { builder->UnsafeAppendNull(); });

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

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        PARQUET_THROW_NOT_OK(builder->Append(data_));
        data_ += descr_->type_length();
      },
      [&]() { PARQUET_THROW_NOT_OK(builder->AppendNull()); });

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
    ArrowBinaryHelper helper(out);
    int values_decoded = 0;

    RETURN_NOT_OK(helper.builder->Reserve(num_values));
    RETURN_NOT_OK(helper.builder->ReserveData(
        std::min<int64_t>(len_, helper.chunk_space_remaining)));

    int i = 0;
    RETURN_NOT_OK(VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() {
          if (ARROW_PREDICT_FALSE(len_ < 4)) {
            ParquetException::EofException();
          }
          auto value_len = SafeLoadAs<int32_t>(data_);
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
          ++i;
          return Status::OK();
        },
        [&]() {
          helper.UnsafeAppendNull();
          ++i;
          return Status::OK();
        }));

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

    RETURN_NOT_OK(VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() {
          if (ARROW_PREDICT_FALSE(len_ < 4)) {
            ParquetException::EofException();
          }
          auto value_len = SafeLoadAs<int32_t>(data_);
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
          return Status::OK();
        },
        [&]() { return builder->AppendNull(); }));

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
                           MemoryPool* pool = ::arrow::default_memory_pool())
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

    auto indices_buffer =
        reinterpret_cast<int32_t*>(indices_scratch_space_->mutable_data());

    if (num_values != idx_decoder_.GetBatchSpaced(num_values, null_count, valid_bits,
                                                  valid_bits_offset, indices_buffer)) {
      ParquetException::EofException();
    }

    // XXX(wesm): Cannot append "valid bits" directly to the builder
    std::vector<uint8_t> valid_bytes(num_values, 0);
    int64_t i = 0;
    VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() { valid_bytes[i++] = 1; }, [&]() { ++i; });

    auto binary_builder = checked_cast<::arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(
        binary_builder->AppendIndices(indices_buffer, num_values, valid_bytes.data()));
    num_values_ -= num_values - null_count;
    return num_values - null_count;
  }

  int DecodeIndices(int num_values, ::arrow::ArrayBuilder* builder) override {
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
    auto binary_builder = checked_cast<::arrow::BinaryDictionary32Builder*>(builder);
    PARQUET_THROW_NOT_OK(binary_builder->AppendIndices(indices_buffer, num_values));
    num_values_ -= num_values;
    return num_values;
  }

  int DecodeIndices(int num_values, int32_t* indices) override {
    if (num_values != idx_decoder_.GetBatch(indices, num_values)) {
      ParquetException::EofException();
    }
    num_values_ -= num_values;
    return num_values;
  }

  void GetDictionary(const T** dictionary, int32_t* dictionary_length) override {
    *dictionary_length = dictionary_length_;
    *dictionary = reinterpret_cast<T*>(dictionary_->mutable_data());
  }

 protected:
  Status IndexInBounds(int32_t index) {
    if (ARROW_PREDICT_TRUE(0 <= index && index < dictionary_length_)) {
      return Status::OK();
    }
    return Status::Invalid("Index not in dictionary bounds");
  }

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

  auto dict_values = reinterpret_cast<ByteArray*>(dictionary_->mutable_data());

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
  if (builder->byte_width() != descr_->type_length()) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(builder->byte_width()) + " but decoder was " +
                           std::to_string(descr_->type_length()));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto dict_values = reinterpret_cast<const FLBA*>(dictionary_->data());

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        int32_t index;
        if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
          throw ParquetException("");
        }
        PARQUET_THROW_NOT_OK(IndexInBounds(index));
        builder->UnsafeAppend(dict_values[index].ptr);
      },
      [&]() { builder->UnsafeAppendNull(); });

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
  if (byte_width != descr_->type_length()) {
    throw ParquetException("Byte width mismatch: builder was " +
                           std::to_string(byte_width) + " but decoder was " +
                           std::to_string(descr_->type_length()));
  }

  PARQUET_THROW_NOT_OK(builder->Reserve(num_values));

  auto dict_values = reinterpret_cast<const FLBA*>(dictionary_->data());

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
  auto dict_values = reinterpret_cast<const value_type*>(dictionary_->data());

  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        int32_t index;
        if (ARROW_PREDICT_FALSE(!idx_decoder_.Get(&index))) {
          throw ParquetException("");
        }
        PARQUET_THROW_NOT_OK(IndexInBounds(index));
        builder->UnsafeAppend(dict_values[index]);
      },
      [&]() { builder->UnsafeAppendNull(); });

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

class DictByteArrayDecoderImpl : public DictDecoderImpl<ByteArrayType>,
                                 virtual public ByteArrayDecoder {
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
      PARQUET_THROW_NOT_OK(DecodeArrowDenseNonNull(num_values, out, &result));
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

    ArrowBinaryHelper helper(out);

    auto dict_values = reinterpret_cast<const ByteArray*>(dictionary_->data());
    int values_decoded = 0;
    int num_indices = 0;
    int pos_indices = 0;

    auto visit_valid = [&](int64_t position) -> Status {
      if (num_indices == pos_indices) {
        // Refill indices buffer
        const auto batch_size =
            std::min<int32_t>(kBufferSize, num_values - null_count - values_decoded);
        num_indices = idx_decoder_.GetBatch(indices, batch_size);
        if (ARROW_PREDICT_FALSE(num_indices < 1)) {
          return Status::Invalid("Invalid number of indices: ", num_indices);
        }
        pos_indices = 0;
      }
      const auto index = indices[pos_indices++];
      RETURN_NOT_OK(IndexInBounds(index));
      const auto& val = dict_values[index];
      if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
        RETURN_NOT_OK(helper.PushChunk());
      }
      RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
      ++values_decoded;
      return Status::OK();
    };

    auto visit_null = [&]() -> Status {
      RETURN_NOT_OK(helper.AppendNull());
      return Status::OK();
    };

    ::arrow::internal::BitBlockCounter bit_blocks(valid_bits, valid_bits_offset,
                                                  num_values);
    int64_t position = 0;
    while (position < num_values) {
      const auto block = bit_blocks.NextWord();
      if (block.AllSet()) {
        for (int64_t i = 0; i < block.length; ++i, ++position) {
          ARROW_RETURN_NOT_OK(visit_valid(position));
        }
      } else if (block.NoneSet()) {
        for (int64_t i = 0; i < block.length; ++i, ++position) {
          ARROW_RETURN_NOT_OK(visit_null());
        }
      } else {
        for (int64_t i = 0; i < block.length; ++i, ++position) {
          if (bit_util::GetBit(valid_bits, valid_bits_offset + position)) {
            ARROW_RETURN_NOT_OK(visit_valid(position));
          } else {
            ARROW_RETURN_NOT_OK(visit_null());
          }
        }
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
    ::arrow::internal::BitmapReader bit_reader(valid_bits, valid_bits_offset, num_values);

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
// DeltaBitPackEncoder

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
  static constexpr uint32_t kValuesPerBlock = 128;
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
  UT first_value_{0};
  UT current_value_{0};
  ArrowPoolVector<UT> deltas_;
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
    UT value = static_cast<UT>(src[idx]);
    // Calculate deltas. The possible overflow is handled by use of unsigned integers
    // making subtraction operations well-defined and correct even in case of overflow.
    // Encoded integers will wrap back around on decoding.
    // See http://en.wikipedia.org/wiki/Modular_arithmetic#Integers_modulo_n
    deltas_[values_current_block_] = value - current_value_;
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

  const UT min_delta =
      *std::min_element(deltas_.begin(), deltas_.begin() + values_current_block_);
  bit_writer_.PutZigZagVlqInt(static_cast<T>(min_delta));

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
    const UT max_delta = *std::max_element(
        deltas_.begin() + start, deltas_.begin() + start + values_current_mini_block);

    // The minimum number of bits required to write any of values in deltas_ vector.
    // See overflow comment above.
    const auto bit_width = bit_width_data[i] =
        bit_util::NumRequiredBits(max_delta - min_delta);

    for (uint32_t j = start; j < start + values_current_mini_block; j++) {
      // See overflow comment above.
      const UT value = deltas_[j] - min_delta;
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
    T* data = reinterpret_cast<T*>(buffer->mutable_data());
    int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
        src, num_values, valid_bits, valid_bits_offset, data);
    Put(data, num_valid_values);
  } else {
    Put(src, num_values);
  }
}

// ----------------------------------------------------------------------
// DeltaBitPackDecoder

template <typename DType>
class DeltaBitPackDecoder : public DecoderImpl, virtual public TypedDecoder<DType> {
 public:
  typedef typename DType::c_type T;
  using UT = std::make_unsigned_t<T>;

  explicit DeltaBitPackDecoder(const ColumnDescriptor* descr,
                               MemoryPool* pool = ::arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BINARY_PACKED), pool_(pool) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  void SetData(int num_values, const uint8_t* data, int len) override {
    // num_values is equal to page's num_values, including null values in this page
    this->num_values_ = num_values;
    decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
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
    delta_bit_widths_ = AllocateBuffer(pool_, mini_blocks_per_block_);
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
    while (i < max_values) {
      if (ARROW_PREDICT_FALSE(values_remaining_current_mini_block_ == 0)) {
        if (ARROW_PREDICT_FALSE(!first_block_initialized_)) {
          buffer[i++] = last_value_;
          DCHECK_EQ(i, 1);  // we're at the beginning of the page
          if (ARROW_PREDICT_FALSE(i == max_values)) {
            // When block is uninitialized and i reaches max_values we have two
            // different possibilities:
            // 1. total_value_count_ == 1, which means that the page may have only
            // one value (encoded in the header), and we should not initialize
            // any block.
            // 2. total_value_count_ != 1, which means we should initialize the
            // incoming block for subsequent reads.
            if (total_value_count_ != 1) {
              InitBlock();
            }
            break;
          }
          InitBlock();
        } else {
          ++mini_block_idx_;
          if (mini_block_idx_ < mini_blocks_per_block_) {
            InitMiniBlock(delta_bit_widths_->data()[mini_block_idx_]);
          } else {
            InitBlock();
          }
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
// DELTA_LENGTH_BYTE_ARRAY

// ----------------------------------------------------------------------
// DeltaLengthByteArrayEncoder

template <typename DType>
class DeltaLengthByteArrayEncoder : public EncoderImpl,
                                    virtual public TypedEncoder<ByteArrayType> {
 public:
  explicit DeltaLengthByteArrayEncoder(const ColumnDescriptor* descr, MemoryPool* pool)
      : EncoderImpl(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY,
                    pool = ::arrow::default_memory_pool()),
        sink_(pool),
        length_encoder_(nullptr, pool),
        encoded_size_{0} {}

  std::shared_ptr<Buffer> FlushValues() override;

  int64_t EstimatedDataEncodedSize() override {
    return encoded_size_ + length_encoder_.EstimatedDataEncodedSize();
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
            return Status::Invalid("Parquet cannot store strings with size 2GB or more");
          }
          length_encoder_.Put({static_cast<int32_t>(view.length())}, 1);
          PARQUET_THROW_NOT_OK(sink_.Append(view.data(), view.length()));
          return Status::OK();
        },
        []() { return Status::OK(); }));
  }

  ::arrow::BufferBuilder sink_;
  DeltaBitPackEncoder<Int32Type> length_encoder_;
  uint32_t encoded_size_;
};

template <typename DType>
void DeltaLengthByteArrayEncoder<DType>::Put(const ::arrow::Array& values) {
  AssertBaseBinary(values);
  if (::arrow::is_binary_like(values.type_id())) {
    PutBinaryArray(checked_cast<const ::arrow::BinaryArray&>(values));
  } else {
    PutBinaryArray(checked_cast<const ::arrow::LargeBinaryArray&>(values));
  }
}

template <typename DType>
void DeltaLengthByteArrayEncoder<DType>::Put(const T* src, int num_values) {
  if (num_values == 0) {
    return;
  }

  constexpr int kBatchSize = 256;
  std::array<int32_t, kBatchSize> lengths;
  for (int idx = 0; idx < num_values; idx += kBatchSize) {
    const int batch_size = std::min(kBatchSize, num_values - idx);
    for (int j = 0; j < batch_size; ++j) {
      const int32_t len = src[idx + j].len;
      if (AddWithOverflow(encoded_size_, len, &encoded_size_)) {
        throw ParquetException("excess expansion in DELTA_LENGTH_BYTE_ARRAY");
      }
      lengths[j] = len;
    }
    length_encoder_.Put(lengths.data(), batch_size);
  }

  PARQUET_THROW_NOT_OK(sink_.Reserve(encoded_size_));
  for (int idx = 0; idx < num_values; idx++) {
    sink_.UnsafeAppend(src[idx].ptr, src[idx].len);
  }
}

template <typename DType>
void DeltaLengthByteArrayEncoder<DType>::PutSpaced(const T* src, int num_values,
                                                   const uint8_t* valid_bits,
                                                   int64_t valid_bits_offset) {
  if (valid_bits != NULLPTR) {
    PARQUET_ASSIGN_OR_THROW(auto buffer, ::arrow::AllocateBuffer(num_values * sizeof(T),
                                                                 this->memory_pool()));
    T* data = reinterpret_cast<T*>(buffer->mutable_data());
    int num_valid_values = ::arrow::util::internal::SpacedCompress<T>(
        src, num_values, valid_bits, valid_bits_offset, data);
    Put(data, num_valid_values);
  } else {
    Put(src, num_values);
  }
}

template <typename DType>
std::shared_ptr<Buffer> DeltaLengthByteArrayEncoder<DType>::FlushValues() {
  std::shared_ptr<Buffer> encoded_lengths = length_encoder_.FlushValues();

  std::shared_ptr<Buffer> data;
  PARQUET_THROW_NOT_OK(sink_.Finish(&data));
  sink_.Reset();

  PARQUET_THROW_NOT_OK(sink_.Resize(encoded_lengths->size() + data->size()));
  PARQUET_THROW_NOT_OK(sink_.Append(encoded_lengths->data(), encoded_lengths->size()));
  PARQUET_THROW_NOT_OK(sink_.Append(data->data(), data->size()));

  std::shared_ptr<Buffer> buffer;
  PARQUET_THROW_NOT_OK(sink_.Finish(&buffer, true));
  encoded_size_ = 0;
  return buffer;
}

// ----------------------------------------------------------------------
// DeltaLengthByteArrayDecoder

class DeltaLengthByteArrayDecoder : public DecoderImpl,
                                    virtual public TypedDecoder<ByteArrayType> {
 public:
  explicit DeltaLengthByteArrayDecoder(const ColumnDescriptor* descr,
                                       MemoryPool* pool = ::arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY),
        len_decoder_(nullptr, pool),
        buffered_length_(AllocateBuffer(pool, 0)),
        buffered_data_(AllocateBuffer(pool, 0)) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    if (len == 0) return;
    decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    DecodeLengths();
  }

  void SetDecoder(int num_values, std::shared_ptr<::arrow::bit_util::BitReader> decoder) {
    num_values_ = num_values;
    decoder_ = decoder;
    DecodeLengths();
  }

  int Decode(ByteArray* buffer, int max_values) override {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, num_valid_values_);
    if (max_values == 0) {
      return 0;
    }

    int32_t data_size = 0;
    const int32_t* length_ptr =
        reinterpret_cast<const int32_t*>(buffered_length_->data()) + length_idx_;
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

    PARQUET_THROW_NOT_OK(buffered_data_->Resize(data_size));
    if (decoder_->GetBatch(8, buffered_data_->mutable_data(), data_size) != data_size) {
      ParquetException::EofException();
    }
    const uint8_t* data_ptr = buffered_data_->data();

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
    int ret = len_decoder_.Decode(
        reinterpret_cast<int32_t*>(buffered_length_->mutable_data()), num_length);
    DCHECK_EQ(ret, num_length);
    length_idx_ = 0;
    num_valid_values_ = num_length;
  }

  Status DecodeArrowDense(int num_values, int null_count, const uint8_t* valid_bits,
                          int64_t valid_bits_offset,
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_num_values) {
    ArrowBinaryHelper helper(out);

    std::vector<ByteArray> values(num_values - null_count);
    const int num_valid_values = Decode(values.data(), num_values - null_count);
    if (ARROW_PREDICT_FALSE(num_values - null_count != num_valid_values)) {
      throw ParquetException("Expected to decode ", num_values - null_count,
                             " values, but decoded ", num_valid_values, " values.");
    }

    auto values_ptr = values.data();
    int value_idx = 0;

    RETURN_NOT_OK(VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() {
          const auto& val = values_ptr[value_idx];
          if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
            RETURN_NOT_OK(helper.PushChunk());
          }
          RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
          ++value_idx;
          return Status::OK();
        },
        [&]() {
          RETURN_NOT_OK(helper.AppendNull());
          --null_count;
          return Status::OK();
        }));

    DCHECK_EQ(null_count, 0);
    *out_num_values = num_valid_values;
    return Status::OK();
  }

  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  DeltaBitPackDecoder<Int32Type> len_decoder_;
  int num_valid_values_;
  uint32_t length_idx_;
  std::shared_ptr<ResizableBuffer> buffered_length_;
  std::shared_ptr<ResizableBuffer> buffered_data_;
};

// ----------------------------------------------------------------------
// RLE_BOOLEAN_DECODER

class RleBooleanDecoder : public DecoderImpl, virtual public BooleanDecoder {
 public:
  explicit RleBooleanDecoder(const ColumnDescriptor* descr)
      : DecoderImpl(descr, Encoding::RLE) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    uint32_t num_bytes = 0;

    if (len < 4) {
      throw ParquetException("Received invalid length : " + std::to_string(len) +
                             " (corrupt data page?)");
    }
    // Load the first 4 bytes in little-endian, which indicates the length
    num_bytes = ::arrow::bit_util::ToLittleEndian(SafeLoadAs<uint32_t>(data));
    if (num_bytes < 0 || num_bytes > static_cast<uint32_t>(len - 4)) {
      throw ParquetException("Received invalid number of bytes : " +
                             std::to_string(num_bytes) + " (corrupt data page?)");
    }

    auto decoder_data = data + 4;
    decoder_ = std::make_shared<::arrow::util::RleDecoder>(decoder_data, num_bytes,
                                                           /*bit_width=*/1);
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
    ParquetException::NYI("DecodeArrow for RleBooleanDecoder");
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
// DELTA_BYTE_ARRAY

class DeltaByteArrayDecoder : public DecoderImpl,
                              virtual public TypedDecoder<ByteArrayType> {
 public:
  explicit DeltaByteArrayDecoder(const ColumnDescriptor* descr,
                                 MemoryPool* pool = ::arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BYTE_ARRAY),
        prefix_len_decoder_(nullptr, pool),
        suffix_decoder_(nullptr, pool),
        last_value_in_previous_page_(""),
        buffered_prefix_length_(AllocateBuffer(pool, 0)),
        buffered_data_(AllocateBuffer(pool, 0)) {}

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    prefix_len_decoder_.SetDecoder(num_values, decoder_);

    // get the number of encoded prefix lengths
    int num_prefix = prefix_len_decoder_.ValidValuesCount();
    // call prefix_len_decoder_.Decode to decode all the prefix lengths.
    // all the prefix lengths are buffered in buffered_prefix_length_.
    PARQUET_THROW_NOT_OK(buffered_prefix_length_->Resize(num_prefix * sizeof(int32_t)));
    int ret = prefix_len_decoder_.Decode(
        reinterpret_cast<int32_t*>(buffered_prefix_length_->mutable_data()), num_prefix);
    DCHECK_EQ(ret, num_prefix);
    prefix_len_offset_ = 0;
    num_valid_values_ = num_prefix;

    // at this time, the decoder_ will be at the start of the encoded suffix data.
    suffix_decoder_.SetDecoder(num_values, decoder_);

    // TODO: read corrupted files written with bug(PARQUET-246). last_value_ should be set
    // to last_value_in_previous_page_ when decoding a new page(except the first page)
    last_value_ = "";
  }

  int Decode(ByteArray* buffer, int max_values) override {
    return GetInternal(buffer, max_values);
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t* valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<ByteArrayType>::Accumulator* out) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                          valid_bits_offset, out, &result));
    return result;
  }

  int DecodeArrow(
      int num_values, int null_count, const uint8_t* valid_bits,
      int64_t valid_bits_offset,
      typename EncodingTraits<ByteArrayType>::DictAccumulator* builder) override {
    ParquetException::NYI("DecodeArrow of DictAccumulator for DeltaByteArrayDecoder");
  }

 private:
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
        reinterpret_cast<const int32_t*>(buffered_prefix_length_->data()) +
        prefix_len_offset_;
    for (int i = 0; i < max_values; ++i) {
      if (ARROW_PREDICT_FALSE(prefix_len_ptr[i] < 0)) {
        throw ParquetException("negative prefix length in DELTA_BYTE_ARRAY");
      }
      if (ARROW_PREDICT_FALSE(AddWithOverflow(data_size, prefix_len_ptr[i], &data_size) ||
                              AddWithOverflow(data_size, buffer[i].len, &data_size))) {
        throw ParquetException("excess expansion in DELTA_BYTE_ARRAY");
      }
    }
    PARQUET_THROW_NOT_OK(buffered_data_->Resize(data_size));

    string_view prefix{last_value_};
    uint8_t* data_ptr = buffered_data_->mutable_data();
    for (int i = 0; i < max_values; ++i) {
      if (ARROW_PREDICT_FALSE(static_cast<size_t>(prefix_len_ptr[i]) > prefix.length())) {
        throw ParquetException("prefix length too large in DELTA_BYTE_ARRAY");
      }
      memcpy(data_ptr, prefix.data(), prefix_len_ptr[i]);
      // buffer[i] currently points to the string suffix
      memcpy(data_ptr + prefix_len_ptr[i], buffer[i].ptr, buffer[i].len);
      buffer[i].ptr = data_ptr;
      buffer[i].len += prefix_len_ptr[i];
      data_ptr += buffer[i].len;
      prefix = string_view{reinterpret_cast<const char*>(buffer[i].ptr), buffer[i].len};
    }
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
                          typename EncodingTraits<ByteArrayType>::Accumulator* out,
                          int* out_num_values) {
    ArrowBinaryHelper helper(out);

    std::vector<ByteArray> values(num_values);
    const int num_valid_values = GetInternal(values.data(), num_values - null_count);
    DCHECK_EQ(num_values - null_count, num_valid_values);

    auto values_ptr = reinterpret_cast<const ByteArray*>(values.data());
    int value_idx = 0;

    RETURN_NOT_OK(VisitNullBitmapInline(
        valid_bits, valid_bits_offset, num_values, null_count,
        [&]() {
          const auto& val = values_ptr[value_idx];
          if (ARROW_PREDICT_FALSE(!helper.CanFit(val.len))) {
            RETURN_NOT_OK(helper.PushChunk());
          }
          RETURN_NOT_OK(helper.Append(val.ptr, static_cast<int32_t>(val.len)));
          ++value_idx;
          return Status::OK();
        },
        [&]() {
          RETURN_NOT_OK(helper.AppendNull());
          --null_count;
          return Status::OK();
        }));

    DCHECK_EQ(null_count, 0);
    *out_num_values = num_valid_values;
    return Status::OK();
  }

  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  std::string last_value_;
  // string buffer for last value in previous page
  std::string last_value_in_previous_page_;
  int num_valid_values_;
  uint32_t prefix_len_offset_;
  std::shared_ptr<ResizableBuffer> buffered_prefix_length_;
  std::shared_ptr<ResizableBuffer> buffered_data_;
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

  T* EnsureDecodeBuffer(int64_t min_values) {
    const int64_t size = sizeof(T) * min_values;
    if (!decode_buffer_ || decode_buffer_->size() < size) {
      PARQUET_ASSIGN_OR_THROW(decode_buffer_, ::arrow::AllocateBuffer(size));
    }
    return reinterpret_cast<T*>(decode_buffer_->mutable_data());
  }

 private:
  int num_values_in_buffer_{0};
  std::shared_ptr<Buffer> decode_buffer_;

  static constexpr size_t kNumStreams = sizeof(T);
};

template <typename DType>
ByteStreamSplitDecoder<DType>::ByteStreamSplitDecoder(const ColumnDescriptor* descr)
    : DecoderImpl(descr, Encoding::BYTE_STREAM_SPLIT) {}

template <typename DType>
void ByteStreamSplitDecoder<DType>::SetData(int num_values, const uint8_t* data,
                                            int len) {
  if (num_values * static_cast<int64_t>(sizeof(T)) < len) {
    throw ParquetException(
        "Data size too large for number of values (padding in byte stream split data "
        "page?)");
  }
  if (len % sizeof(T) != 0) {
    throw ParquetException("ByteStreamSplit data size " + std::to_string(len) +
                           " not aligned with type " + TypeToString(DType::type_num));
  }
  num_values = len / sizeof(T);
  DecoderImpl::SetData(num_values, data, len);
  num_values_in_buffer_ = num_values_;
}

template <typename DType>
int ByteStreamSplitDecoder<DType>::Decode(T* buffer, int max_values) {
  const int values_to_decode = std::min(num_values_, max_values);
  const int num_decoded_previously = num_values_in_buffer_ - num_values_;
  const uint8_t* data = data_ + num_decoded_previously;

  ::arrow::util::internal::ByteStreamSplitDecode<T>(data, values_to_decode,
                                                    num_values_in_buffer_, buffer);
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

  const int num_decoded_previously = num_values_in_buffer_ - num_values_;
  const uint8_t* data = data_ + num_decoded_previously;
  int offset = 0;

#if defined(ARROW_HAVE_SIMD_SPLIT)
  // Use fast decoding into intermediate buffer.  This will also decode
  // some null values, but it's fast enough that we don't care.
  T* decode_out = EnsureDecodeBuffer(values_decoded);
  ::arrow::util::internal::ByteStreamSplitDecode<T>(data, values_decoded,
                                                    num_values_in_buffer_, decode_out);

  // XXX If null_count is 0, we could even append in bulk or decode directly into
  // builder
  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        builder->UnsafeAppend(decode_out[offset]);
        ++offset;
      },
      [&]() { builder->UnsafeAppendNull(); });

#else
  VisitNullBitmapInline(
      valid_bits, valid_bits_offset, num_values, null_count,
      [&]() {
        uint8_t gathered_byte_data[kNumStreams];
        for (size_t b = 0; b < kNumStreams; ++b) {
          const size_t byte_index = b * num_values_in_buffer_ + offset;
          gathered_byte_data[b] = data[byte_index];
        }
        builder->UnsafeAppend(SafeLoadAs<T>(&gathered_byte_data[0]));
        ++offset;
      },
      [&]() { builder->UnsafeAppendNull(); });
#endif

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

}  // namespace

// ----------------------------------------------------------------------
// Encoder and decoder factory functions

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
      case Type::FLOAT:
        return std::make_unique<ByteStreamSplitEncoder<FloatType>>(descr, pool);
      case Type::DOUBLE:
        return std::make_unique<ByteStreamSplitEncoder<DoubleType>>(descr, pool);
      default:
        throw ParquetException("BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE");
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
        return std::make_unique<DeltaLengthByteArrayEncoder<ByteArrayType>>(descr, pool);
      default:
        throw ParquetException("DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY");
    }
  } else {
    ParquetException::NYI("Selected encoding is not supported");
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
}

std::unique_ptr<Decoder> MakeDecoder(Type::type type_num, Encoding::type encoding,
                                     const ColumnDescriptor* descr) {
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
      case Type::FLOAT:
        return std::make_unique<ByteStreamSplitDecoder<FloatType>>(descr);
      case Type::DOUBLE:
        return std::make_unique<ByteStreamSplitDecoder<DoubleType>>(descr);
      default:
        throw ParquetException("BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE");
    }
  } else if (encoding == Encoding::DELTA_BINARY_PACKED) {
    switch (type_num) {
      case Type::INT32:
        return std::make_unique<DeltaBitPackDecoder<Int32Type>>(descr);
      case Type::INT64:
        return std::make_unique<DeltaBitPackDecoder<Int64Type>>(descr);
      default:
        throw ParquetException(
            "DELTA_BINARY_PACKED decoder only supports INT32 and INT64");
    }
  } else if (encoding == Encoding::DELTA_BYTE_ARRAY) {
    if (type_num == Type::BYTE_ARRAY) {
      return std::make_unique<DeltaByteArrayDecoder>(descr);
    }
    throw ParquetException("DELTA_BYTE_ARRAY only supports BYTE_ARRAY");
  } else if (encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) {
    if (type_num == Type::BYTE_ARRAY) {
      return std::make_unique<DeltaLengthByteArrayDecoder>(descr);
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
