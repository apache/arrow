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

#ifndef PARQUET_ENCODING_INTERNAL_H
#define PARQUET_ENCODING_INTERNAL_H

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/util/bit-stream-utils.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/cpu-info.h"
#include "arrow/util/hash-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/rle-encoding.h"

#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

namespace BitUtil = ::arrow::BitUtil;
using HashUtil = ::arrow::HashUtil;

class ColumnDescriptor;

// ----------------------------------------------------------------------
// Encoding::PLAIN decoder implementation

template <typename DType>
class PlainDecoder : public Decoder<DType> {
 public:
  typedef typename DType::c_type T;
  using Decoder<DType>::num_values_;

  explicit PlainDecoder(const ColumnDescriptor* descr)
      : Decoder<DType>(descr, Encoding::PLAIN), data_(nullptr), len_(0) {
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
inline int DecodePlain(const uint8_t* data, int64_t data_size, int num_values,
                       int type_length, T* out) {
  int bytes_to_decode = num_values * static_cast<int>(sizeof(T));
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
    increment = static_cast<int>(sizeof(uint32_t) + len);
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
    bit_reader_ = ::arrow::BitReader(data, len);
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
    if (bit_reader_.GetBatch(1, buffer, max_values) != max_values) {
      ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  ::arrow::BitReader bit_reader_;
};

// ----------------------------------------------------------------------
// Encoding::PLAIN encoder implementation

template <typename DType>
class PlainEncoder : public Encoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit PlainEncoder(const ColumnDescriptor* descr,
                        ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Encoder<DType>(descr, Encoding::PLAIN, pool) {
    values_sink_.reset(new InMemoryOutputStream(pool));
  }

  int64_t EstimatedDataEncodedSize() override { return values_sink_->Tell(); }

  std::shared_ptr<Buffer> FlushValues() override;
  void Put(const T* src, int num_values) override;

 protected:
  std::unique_ptr<InMemoryOutputStream> values_sink_;
};

template <>
class PlainEncoder<BooleanType> : public Encoder<BooleanType> {
 public:
  explicit PlainEncoder(const ColumnDescriptor* descr,
                        ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Encoder<BooleanType>(descr, Encoding::PLAIN, pool),
        bits_available_(kInMemoryDefaultCapacity * 8),
        bits_buffer_(AllocateBuffer(pool, kInMemoryDefaultCapacity)),
        values_sink_(new InMemoryOutputStream(pool)) {
    bit_writer_.reset(new ::arrow::BitWriter(bits_buffer_->mutable_data(),
                                             static_cast<int>(bits_buffer_->size())));
  }

  int64_t EstimatedDataEncodedSize() override {
    return values_sink_->Tell() + bit_writer_->bytes_written();
  }

  std::shared_ptr<Buffer> FlushValues() override {
    if (bits_available_ > 0) {
      bit_writer_->Flush();
      values_sink_->Write(bit_writer_->buffer(), bit_writer_->bytes_written());
      bit_writer_->Clear();
      bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;
    }

    std::shared_ptr<Buffer> buffer = values_sink_->GetBuffer();
    values_sink_.reset(new InMemoryOutputStream(this->pool_));
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
      bits_available_ = static_cast<int>(bits_buffer_->size()) * 8;               \
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
  std::unique_ptr<::arrow::BitWriter> bit_writer_;
  std::shared_ptr<PoolBuffer> bits_buffer_;
  std::unique_ptr<InMemoryOutputStream> values_sink_;
};

template <typename DType>
inline std::shared_ptr<Buffer> PlainEncoder<DType>::FlushValues() {
  std::shared_ptr<Buffer> buffer = values_sink_->GetBuffer();
  values_sink_.reset(new InMemoryOutputStream(this->pool_));
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
    if (src[i].len > 0) {
      DCHECK(nullptr != src[i].ptr) << "Value ptr cannot be NULL";
    }
    values_sink_->Write(reinterpret_cast<const uint8_t*>(src[i].ptr), src[i].len);
  }
}

template <>
inline void PlainEncoder<FLBAType>::Put(const FixedLenByteArray* src, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    // Write the result to the output stream
    if (descr_->type_length() > 0) {
      DCHECK(nullptr != src[i].ptr) << "Value ptr cannot be NULL";
    }
    values_sink_->Write(reinterpret_cast<const uint8_t*>(src[i].ptr),
                        descr_->type_length());
  }
}

// ----------------------------------------------------------------------
// Dictionary encoding and decoding

template <typename Type>
class DictionaryDecoder : public Decoder<Type> {
 public:
  typedef typename Type::c_type T;

  // Initializes the dictionary with values from 'dictionary'. The data in
  // dictionary is not guaranteed to persist in memory after this call so the
  // dictionary decoder needs to copy the data out if necessary.
  explicit DictionaryDecoder(const ColumnDescriptor* descr,
                             ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Decoder<Type>(descr, Encoding::RLE_DICTIONARY),
        dictionary_(0, pool),
        byte_array_data_(AllocateBuffer(pool, 0)) {}

  // Perform type-specific initiatialization
  void SetDict(Decoder<Type>* dictionary);

  void SetData(int num_values, const uint8_t* data, int len) override {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    ++data;
    --len;
    idx_decoder_ = ::arrow::RleDecoder(data, len, bit_width);
  }

  int Decode(T* buffer, int max_values) override {
    max_values = std::min(max_values, num_values_);
    int decoded_values =
        idx_decoder_.GetBatchWithDict(dictionary_.data(), buffer, max_values);
    if (decoded_values != max_values) {
      ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

  int DecodeSpaced(T* buffer, int num_values, int null_count, const uint8_t* valid_bits,
                   int64_t valid_bits_offset) override {
    int decoded_values =
        idx_decoder_.GetBatchWithDictSpaced(dictionary_.data(), buffer, num_values,
                                            null_count, valid_bits, valid_bits_offset);
    if (decoded_values != num_values) {
      ParquetException::EofException();
    }
    return decoded_values;
  }

 private:
  using Decoder<Type>::num_values_;

  // Only one is set.
  Vector<T> dictionary_;

  // Data that contains the byte array data (byte_array_dictionary_ just has the
  // pointers).
  std::shared_ptr<PoolBuffer> byte_array_data_;

  ::arrow::RleDecoder idx_decoder_;
};

template <typename Type>
inline void DictionaryDecoder<Type>::SetDict(Decoder<Type>* dictionary) {
  int num_dictionary_values = dictionary->values_left();
  dictionary_.Resize(num_dictionary_values);
  dictionary->Decode(&dictionary_[0], num_dictionary_values);
}

template <>
inline void DictionaryDecoder<BooleanType>::SetDict(Decoder<BooleanType>* dictionary) {
  ParquetException::NYI("Dictionary encoding is not implemented for boolean values");
}

template <>
inline void DictionaryDecoder<ByteArrayType>::SetDict(
    Decoder<ByteArrayType>* dictionary) {
  int num_dictionary_values = dictionary->values_left();
  dictionary_.Resize(num_dictionary_values);
  dictionary->Decode(&dictionary_[0], num_dictionary_values);

  int total_size = 0;
  for (int i = 0; i < num_dictionary_values; ++i) {
    total_size += dictionary_[i].len;
  }
  PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size, false));
  int offset = 0;

  uint8_t* bytes_data = byte_array_data_->mutable_data();
  for (int i = 0; i < num_dictionary_values; ++i) {
    memcpy(bytes_data + offset, dictionary_[i].ptr, dictionary_[i].len);
    dictionary_[i].ptr = bytes_data + offset;
    offset += dictionary_[i].len;
  }
}

template <>
inline void DictionaryDecoder<FLBAType>::SetDict(Decoder<FLBAType>* dictionary) {
  int num_dictionary_values = dictionary->values_left();
  dictionary_.Resize(num_dictionary_values);
  dictionary->Decode(&dictionary_[0], num_dictionary_values);

  int fixed_len = descr_->type_length();
  int total_size = num_dictionary_values * fixed_len;

  PARQUET_THROW_NOT_OK(byte_array_data_->Resize(total_size, false));
  uint8_t* bytes_data = byte_array_data_->mutable_data();
  for (int32_t i = 0, offset = 0; i < num_dictionary_values; ++i, offset += fixed_len) {
    memcpy(bytes_data + offset, dictionary_[i].ptr, fixed_len);
    dictionary_[i].ptr = bytes_data + offset;
  }
}

// ----------------------------------------------------------------------
// Dictionary encoder

// Initially imported from Apache Impala on 2016-02-22, and has been modified
// since for parquet-cpp

// Initially 1024 elements
static constexpr int INITIAL_HASH_TABLE_SIZE = 1 << 10;

typedef int32_t hash_slot_t;
static constexpr hash_slot_t HASH_SLOT_EMPTY = std::numeric_limits<int32_t>::max();

// The maximum load factor for the hash table before resizing.
static constexpr double MAX_HASH_LOAD = 0.7;

/// See the dictionary encoding section of https://github.com/Parquet/parquet-format.
/// The encoding supports streaming encoding. Values are encoded as they are added while
/// the dictionary is being constructed. At any time, the buffered values can be
/// written out with the current dictionary size. More values can then be added to
/// the encoder, including new dictionary entries.
template <typename DType>
class DictEncoder : public Encoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit DictEncoder(const ColumnDescriptor* desc, ChunkedAllocator* pool = nullptr,
                       ::arrow::MemoryPool* allocator = ::arrow::default_memory_pool())
      : Encoder<DType>(desc, Encoding::PLAIN_DICTIONARY, allocator),
        allocator_(allocator),
        pool_(pool),
        hash_table_size_(INITIAL_HASH_TABLE_SIZE),
        mod_bitmask_(hash_table_size_ - 1),
        hash_slots_(0, allocator),
        dict_encoded_size_(0),
        type_length_(desc->type_length()) {
    hash_slots_.Assign(hash_table_size_, HASH_SLOT_EMPTY);
    if (!::arrow::CpuInfo::initialized()) {
      ::arrow::CpuInfo::Init();
    }
  }

  ~DictEncoder() override { DCHECK(buffered_indices_.empty()); }

  // TODO(wesm): think about how to address the construction semantics in
  // encodings/dictionary-encoding.h
  void set_mem_pool(ChunkedAllocator* pool) { pool_ = pool; }

  void set_type_length(int type_length) { type_length_ = type_length; }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteIndices().
  int64_t EstimatedDataEncodedSize() override {
    // Note: because of the way RleEncoder::CheckBufferFull() is called, we have to
    // reserve
    // an extra "RleEncoder::MinBufferSize" bytes. These extra bytes won't be used
    // but not reserving them would cause the encoder to fail.
    return 1 +
           ::arrow::RleEncoder::MaxBufferSize(
               bit_width(), static_cast<int>(buffered_indices_.size())) +
           ::arrow::RleEncoder::MinBufferSize(bit_width());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const {
    if (ARROW_PREDICT_FALSE(num_entries() == 0)) return 0;
    if (ARROW_PREDICT_FALSE(num_entries() == 1)) return 1;
    return BitUtil::Log2(num_entries());
  }

  /// Writes out any buffered indices to buffer preceded by the bit width of this data.
  /// Returns the number of bytes written.
  /// If the supplied buffer is not big enough, returns -1.
  /// buffer must be preallocated with buffer_len bytes. Use EstimatedDataEncodedSize()
  /// to size buffer.
  int WriteIndices(uint8_t* buffer, int buffer_len);

  int hash_table_size() { return hash_table_size_; }
  int dict_encoded_size() { return dict_encoded_size_; }
  /// Clears all the indices (but leaves the dictionary).
  void ClearIndices() { buffered_indices_.clear(); }

  /// Encode value. Note that this does not actually write any data, just
  /// buffers the value's index to be written later.
  void Put(const T& value);

  std::shared_ptr<Buffer> FlushValues() override {
    std::shared_ptr<PoolBuffer> buffer =
        AllocateBuffer(this->allocator_, EstimatedDataEncodedSize());
    int result_size = WriteIndices(buffer->mutable_data(),
                                   static_cast<int>(EstimatedDataEncodedSize()));
    ClearIndices();
    PARQUET_THROW_NOT_OK(buffer->Resize(result_size, false));
    return buffer;
  }

  void Put(const T* values, int num_values) override {
    for (int i = 0; i < num_values; i++) {
      Put(values[i]);
    }
  }

  void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
                 int64_t valid_bits_offset) override {
    ::arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                      num_values);
    for (int32_t i = 0; i < num_values; i++) {
      if (valid_bits_reader.IsSet()) {
        Put(src[i]);
      }
      valid_bits_reader.Next();
    }
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  void WriteDict(uint8_t* buffer);

  ChunkedAllocator* mem_pool() { return pool_; }

  /// The number of entries in the dictionary.
  int num_entries() const { return static_cast<int>(uniques_.size()); }

 private:
  ::arrow::MemoryPool* allocator_;

  // For ByteArray / FixedLenByteArray data. Not owned
  ChunkedAllocator* pool_;

  /// Size of the table. Must be a power of 2.
  int hash_table_size_;

  // Store hash_table_size_ - 1, so that j & mod_bitmask_ is equivalent to j %
  // hash_table_size_, but uses far fewer CPU cycles
  int mod_bitmask_;

  // We use a fixed-size hash table with linear probing
  //
  // These values correspond to the uniques_ array
  Vector<hash_slot_t> hash_slots_;

  /// Indices that have not yet be written out by WriteIndices().
  std::vector<int> buffered_indices_;

  /// The number of bytes needed to encode the dictionary.
  int dict_encoded_size_;

  // The unique observed values
  std::vector<T> uniques_;

  bool SlotDifferent(const T& v, hash_slot_t slot);
  void DoubleTableSize();

  /// Size of each encoded dictionary value. -1 for variable-length types.
  int type_length_;

  /// Hash function for mapping a value to a bucket.
  inline int Hash(const T& value) const;

  /// Adds value to the hash table and updates dict_encoded_size_
  void AddDictKey(const T& value);
};

template <typename DType>
inline int DictEncoder<DType>::Hash(const typename DType::c_type& value) const {
  return HashUtil::Hash(&value, sizeof(value), 0);
}

template <>
inline int DictEncoder<ByteArrayType>::Hash(const ByteArray& value) const {
  if (value.len > 0) {
    DCHECK_NE(nullptr, value.ptr) << "Value ptr cannot be NULL";
  }
  return HashUtil::Hash(value.ptr, value.len, 0);
}

template <>
inline int DictEncoder<FLBAType>::Hash(const FixedLenByteArray& value) const {
  if (type_length_ > 0) {
    DCHECK_NE(nullptr, value.ptr) << "Value ptr cannot be NULL";
  }
  return HashUtil::Hash(value.ptr, type_length_, 0);
}

template <typename DType>
inline bool DictEncoder<DType>::SlotDifferent(const typename DType::c_type& v,
                                              hash_slot_t slot) {
  return v != uniques_[slot];
}

template <>
inline bool DictEncoder<FLBAType>::SlotDifferent(const FixedLenByteArray& v,
                                                 hash_slot_t slot) {
  return 0 != memcmp(v.ptr, uniques_[slot].ptr, type_length_);
}

template <typename DType>
inline void DictEncoder<DType>::Put(const typename DType::c_type& v) {
  int j = Hash(v) & mod_bitmask_;
  hash_slot_t index = hash_slots_[j];

  // Find an empty slot
  while (HASH_SLOT_EMPTY != index && SlotDifferent(v, index)) {
    // Linear probing
    ++j;
    if (j == hash_table_size_) j = 0;
    index = hash_slots_[j];
  }

  if (index == HASH_SLOT_EMPTY) {
    // Not in the hash table, so we insert it now
    index = static_cast<hash_slot_t>(uniques_.size());
    hash_slots_[j] = index;
    AddDictKey(v);

    if (ARROW_PREDICT_FALSE(static_cast<int>(uniques_.size()) >
                            hash_table_size_ * MAX_HASH_LOAD)) {
      DoubleTableSize();
    }
  }

  buffered_indices_.push_back(index);
}

template <typename DType>
inline void DictEncoder<DType>::DoubleTableSize() {
  int new_size = hash_table_size_ * 2;
  Vector<hash_slot_t> new_hash_slots(0, allocator_);
  new_hash_slots.Assign(new_size, HASH_SLOT_EMPTY);
  hash_slot_t index, slot;
  int j;
  for (int i = 0; i < hash_table_size_; ++i) {
    index = hash_slots_[i];

    if (index == HASH_SLOT_EMPTY) {
      continue;
    }

    // Compute the hash value mod the new table size to start looking for an
    // empty slot
    const typename DType::c_type& v = uniques_[index];

    // Find an empty slot in the new hash table
    j = Hash(v) & (new_size - 1);
    slot = new_hash_slots[j];
    while (HASH_SLOT_EMPTY != slot && SlotDifferent(v, slot)) {
      ++j;
      if (j == new_size) j = 0;
      slot = new_hash_slots[j];
    }

    // Copy the old slot index to the new hash table
    new_hash_slots[j] = index;
  }

  hash_table_size_ = new_size;
  mod_bitmask_ = new_size - 1;

  hash_slots_.Swap(new_hash_slots);
}

template <typename DType>
inline void DictEncoder<DType>::AddDictKey(const typename DType::c_type& v) {
  uniques_.push_back(v);
  dict_encoded_size_ += static_cast<int>(sizeof(typename DType::c_type));
}

template <>
inline void DictEncoder<ByteArrayType>::AddDictKey(const ByteArray& v) {
  uint8_t* heap = pool_->Allocate(v.len);
  if (ARROW_PREDICT_FALSE(v.len > 0 && heap == nullptr)) {
    throw ParquetException("out of memory");
  }
  memcpy(heap, v.ptr, v.len);
  uniques_.push_back(ByteArray(v.len, heap));
  dict_encoded_size_ += static_cast<int>(v.len + sizeof(uint32_t));
}

template <>
inline void DictEncoder<FLBAType>::AddDictKey(const FixedLenByteArray& v) {
  uint8_t* heap = pool_->Allocate(type_length_);
  if (ARROW_PREDICT_FALSE(type_length_ > 0 && heap == nullptr)) {
    throw ParquetException("out of memory");
  }
  memcpy(heap, v.ptr, type_length_);

  uniques_.push_back(FixedLenByteArray(heap));
  dict_encoded_size_ += type_length_;
}

template <typename DType>
inline void DictEncoder<DType>::WriteDict(uint8_t* buffer) {
  // For primitive types, only a memcpy
  memcpy(buffer, uniques_.data(), sizeof(typename DType::c_type) * uniques_.size());
}

template <>
inline void DictEncoder<BooleanType>::WriteDict(uint8_t* buffer) {
  // For primitive types, only a memcpy
  // memcpy(buffer, uniques_.data(), sizeof(typename DType::c_type) * uniques_.size());
  for (size_t i = 0; i < uniques_.size(); i++) {
    buffer[i] = uniques_[i];
  }
}

// ByteArray and FLBA already have the dictionary encoded in their data heaps
template <>
inline void DictEncoder<ByteArrayType>::WriteDict(uint8_t* buffer) {
  for (const ByteArray& v : uniques_) {
    memcpy(buffer, reinterpret_cast<const void*>(&v.len), sizeof(uint32_t));
    buffer += sizeof(uint32_t);
    if (v.len > 0) {
      DCHECK(nullptr != v.ptr) << "Value ptr cannot be NULL";
    }
    memcpy(buffer, v.ptr, v.len);
    buffer += v.len;
  }
}

template <>
inline void DictEncoder<FLBAType>::WriteDict(uint8_t* buffer) {
  for (const FixedLenByteArray& v : uniques_) {
    if (type_length_ > 0) {
      DCHECK(nullptr != v.ptr) << "Value ptr cannot be NULL";
    }
    memcpy(buffer, v.ptr, type_length_);
    buffer += type_length_;
  }
}

template <typename DType>
inline int DictEncoder<DType>::WriteIndices(uint8_t* buffer, int buffer_len) {
  // Write bit width in first byte
  *buffer = static_cast<uint8_t>(bit_width());
  ++buffer;
  --buffer_len;

  ::arrow::RleEncoder encoder(buffer, buffer_len, bit_width());
  for (int index : buffered_indices_) {
    if (!encoder.Put(index)) return -1;
  }
  encoder.Flush();

  ClearIndices();
  return 1 + encoder.len();
}

// ----------------------------------------------------------------------
// DeltaBitPackDecoder

template <typename DType>
class DeltaBitPackDecoder : public Decoder<DType> {
 public:
  typedef typename DType::c_type T;

  explicit DeltaBitPackDecoder(const ColumnDescriptor* descr,
                               ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Decoder<DType>(descr, Encoding::DELTA_BINARY_PACKED),
        delta_bit_widths_(new PoolBuffer(pool)) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException("Delta bit pack encoding should only be for integer data.");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = ::arrow::BitReader(data, len);
    values_current_block_ = 0;
    values_current_mini_block_ = 0;
  }

  virtual int Decode(T* buffer, int max_values) {
    return GetInternal(buffer, max_values);
  }

 private:
  using Decoder<DType>::num_values_;

  void InitBlock() {
    int32_t block_size;
    if (!decoder_.GetVlqInt(&block_size)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&num_mini_blocks_)) ParquetException::EofException();
    if (!decoder_.GetVlqInt(&values_current_block_)) {
      ParquetException::EofException();
    }
    if (!decoder_.GetZigZagVlqInt(&last_value_)) ParquetException::EofException();
    PARQUET_THROW_NOT_OK(delta_bit_widths_->Resize(num_mini_blocks_, false));

    uint8_t* bit_width_data = delta_bit_widths_->mutable_data();

    if (!decoder_.GetZigZagVlqInt(&min_delta_)) ParquetException::EofException();
    for (int i = 0; i < num_mini_blocks_; ++i) {
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
    max_values = std::min(max_values, num_values_);
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
    num_values_ -= max_values;
    return max_values;
  }

  ::arrow::BitReader decoder_;
  int32_t values_current_block_;
  int32_t num_mini_blocks_;
  uint64_t values_per_mini_block_;
  uint64_t values_current_mini_block_;

  int32_t min_delta_;
  size_t mini_block_idx_;
  std::unique_ptr<PoolBuffer> delta_bit_widths_;
  int delta_bit_width_;

  int32_t last_value_;
};

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY

class DeltaLengthByteArrayDecoder : public Decoder<ByteArrayType> {
 public:
  explicit DeltaLengthByteArrayDecoder(
      const ColumnDescriptor* descr,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Decoder<ByteArrayType>(descr, Encoding::DELTA_LENGTH_BYTE_ARRAY),
        len_decoder_(nullptr, pool) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int total_lengths_len = *reinterpret_cast<const int*>(data);
    data += 4;
    len_decoder_.SetData(num_values, data, total_lengths_len);
    data_ = data + total_lengths_len;
    len_ = len - 4 - total_lengths_len;
  }

  virtual int Decode(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    std::vector<int> lengths(max_values);
    len_decoder_.Decode(lengths.data(), max_values);
    for (int i = 0; i < max_values; ++i) {
      buffer[i].len = lengths[i];
      buffer[i].ptr = data_;
      data_ += lengths[i];
      len_ -= lengths[i];
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  using Decoder<ByteArrayType>::num_values_;
  DeltaBitPackDecoder<Int32Type> len_decoder_;
  const uint8_t* data_;
  int len_;
};

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY

class DeltaByteArrayDecoder : public Decoder<ByteArrayType> {
 public:
  explicit DeltaByteArrayDecoder(
      const ColumnDescriptor* descr,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : Decoder<ByteArrayType>(descr, Encoding::DELTA_BYTE_ARRAY),
        prefix_len_decoder_(nullptr, pool),
        suffix_decoder_(nullptr, pool),
        last_value_(0, nullptr) {}

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int prefix_len_length = *reinterpret_cast<const int*>(data);
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
    max_values = std::min(max_values, num_values_);
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
    num_values_ -= max_values;
    return max_values;
  }

 private:
  using Decoder<ByteArrayType>::num_values_;

  DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  ByteArray last_value_;
};

}  // namespace parquet

#endif  // PARQUET_ENCODING_INTERNAL_H
