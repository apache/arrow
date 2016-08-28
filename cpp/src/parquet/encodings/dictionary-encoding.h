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

#ifndef PARQUET_DICTIONARY_ENCODING_H
#define PARQUET_DICTIONARY_ENCODING_H

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <vector>

#include "parquet/encodings/decoder.h"
#include "parquet/encodings/encoder.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/util/buffer.h"
#include "parquet/util/cpu-info.h"
#include "parquet/util/hash-util.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/mem-pool.h"
#include "parquet/util/rle-encoding.h"

namespace parquet {

template <typename Type>
class DictionaryDecoder : public Decoder<Type> {
 public:
  typedef typename Type::c_type T;

  // Initializes the dictionary with values from 'dictionary'. The data in
  // dictionary is not guaranteed to persist in memory after this call so the
  // dictionary decoder needs to copy the data out if necessary.
  explicit DictionaryDecoder(
      const ColumnDescriptor* descr, MemoryAllocator* allocator = default_allocator())
      : Decoder<Type>(descr, Encoding::RLE_DICTIONARY),
        dictionary_(0, allocator),
        byte_array_data_(0, allocator) {}

  // Perform type-specific initiatialization
  void SetDict(Decoder<Type>* dictionary);

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    ++data;
    --len;
    idx_decoder_ = RleDecoder(data, len, bit_width);
  }

  virtual int Decode(T* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    int decoded_values = idx_decoder_.GetBatchWithDict(dictionary_, buffer, max_values);
    if (decoded_values != max_values) { ParquetException::EofException(); }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  using Decoder<Type>::num_values_;

  // Only one is set.
  Vector<T> dictionary_;

  // Data that contains the byte array data (byte_array_dictionary_ just has the
  // pointers).
  OwnedMutableBuffer byte_array_data_;

  RleDecoder idx_decoder_;
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
  byte_array_data_.Resize(total_size);
  int offset = 0;
  for (int i = 0; i < num_dictionary_values; ++i) {
    memcpy(&byte_array_data_[offset], dictionary_[i].ptr, dictionary_[i].len);
    dictionary_[i].ptr = &byte_array_data_[offset];
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

  byte_array_data_.Resize(total_size);
  int offset = 0;
  for (int i = 0; i < num_dictionary_values; ++i) {
    memcpy(&byte_array_data_[offset], dictionary_[i].ptr, fixed_len);
    dictionary_[i].ptr = &byte_array_data_[offset];
    offset += fixed_len;
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

  explicit DictEncoder(const ColumnDescriptor* desc, MemPool* pool = nullptr,
      MemoryAllocator* allocator = default_allocator())
      : Encoder<DType>(desc, Encoding::PLAIN_DICTIONARY, allocator),
        allocator_(allocator),
        pool_(pool),
        hash_table_size_(INITIAL_HASH_TABLE_SIZE),
        mod_bitmask_(hash_table_size_ - 1),
        hash_slots_(0, allocator),
        dict_encoded_size_(0),
        type_length_(desc->type_length()) {
    hash_slots_.Assign(hash_table_size_, HASH_SLOT_EMPTY);
    if (!CpuInfo::initialized()) { CpuInfo::Init(); }
  }

  virtual ~DictEncoder() { DCHECK(buffered_indices_.empty()); }

  // TODO(wesm): think about how to address the construction semantics in
  // encodings/dictionary-encoding.h
  void set_mem_pool(MemPool* pool) { pool_ = pool; }

  void set_type_length(int type_length) { type_length_ = type_length; }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteIndices().
  int64_t EstimatedDataEncodedSize() override {
    // Note: because of the way RleEncoder::CheckBufferFull() is called, we have to
    // reserve
    // an extra "RleEncoder::MinBufferSize" bytes. These extra bytes won't be used
    // but not reserving them would cause the encoder to fail.
    return 1 + RleEncoder::MaxBufferSize(bit_width(), buffered_indices_.size()) +
           RleEncoder::MinBufferSize(bit_width());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const {
    if (UNLIKELY(num_entries() == 0)) return 0;
    if (UNLIKELY(num_entries() == 1)) return 1;
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
    auto buffer = std::make_shared<OwnedMutableBuffer>(
        EstimatedDataEncodedSize(), this->allocator_);
    int result_size = WriteIndices(buffer->mutable_data(), EstimatedDataEncodedSize());
    ClearIndices();
    buffer->Resize(result_size);
    return buffer;
  };

  void Put(const T* values, int num_values) override {
    for (int i = 0; i < num_values; i++) {
      Put(values[i]);
    }
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  void WriteDict(uint8_t* buffer);

  MemPool* mem_pool() { return pool_; }

  /// The number of entries in the dictionary.
  int num_entries() const { return uniques_.size(); }

 private:
  MemoryAllocator* allocator_;

  // For ByteArray / FixedLenByteArray data. Not owned
  MemPool* pool_;

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
  return HashUtil::Hash(value.ptr, value.len, 0);
}

template <>
inline int DictEncoder<FLBAType>::Hash(const FixedLenByteArray& value) const {
  return HashUtil::Hash(value.ptr, type_length_, 0);
}

template <typename DType>
inline bool DictEncoder<DType>::SlotDifferent(
    const typename DType::c_type& v, hash_slot_t slot) {
  return v != uniques_[slot];
}

template <>
inline bool DictEncoder<FLBAType>::SlotDifferent(
    const FixedLenByteArray& v, hash_slot_t slot) {
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
    index = uniques_.size();
    hash_slots_[j] = index;
    AddDictKey(v);

    if (UNLIKELY(static_cast<int>(uniques_.size()) > hash_table_size_ * MAX_HASH_LOAD)) {
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

    if (index == HASH_SLOT_EMPTY) { continue; }

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
  dict_encoded_size_ += sizeof(typename DType::c_type);
}

template <>
inline void DictEncoder<ByteArrayType>::AddDictKey(const ByteArray& v) {
  uint8_t* heap = pool_->Allocate(v.len);
  if (UNLIKELY(v.len > 0 && heap == nullptr)) { throw ParquetException("out of memory"); }
  memcpy(heap, v.ptr, v.len);
  uniques_.push_back(ByteArray(v.len, heap));
  dict_encoded_size_ += v.len + sizeof(uint32_t);
}

template <>
inline void DictEncoder<FLBAType>::AddDictKey(const FixedLenByteArray& v) {
  uint8_t* heap = pool_->Allocate(type_length_);
  if (UNLIKELY(type_length_ > 0 && heap == nullptr)) {
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
    memcpy(buffer, v.ptr, v.len);
    buffer += v.len;
  }
}

template <>
inline void DictEncoder<FLBAType>::WriteDict(uint8_t* buffer) {
  for (const FixedLenByteArray& v : uniques_) {
    memcpy(buffer, v.ptr, type_length_);
    buffer += type_length_;
  }
}

template <typename DType>
inline int DictEncoder<DType>::WriteIndices(uint8_t* buffer, int buffer_len) {
  // Write bit width in first byte
  *buffer = bit_width();
  ++buffer;
  --buffer_len;

  RleEncoder encoder(buffer, buffer_len, bit_width());
  for (int index : buffered_indices_) {
    if (!encoder.Put(index)) return -1;
  }
  encoder.Flush();

  ClearIndices();
  return 1 + encoder.len();
}

}  // namespace parquet

#endif
