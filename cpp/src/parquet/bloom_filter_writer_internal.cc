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

#include "parquet/bloom_filter_writer_internal.h"

#include "parquet/exception.h"
#include "parquet/schema.h"

#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/unreachable.h"
#include "arrow/visit_data_inline.h"

namespace parquet::internal {

constexpr int64_t kHashBatchSize = 256;

template <typename ParquetType>
BloomFilterWriterImpl<ParquetType>::BloomFilterWriterImpl(const ColumnDescriptor* descr,
                                                          BloomFilter* bloom_filter)
    : descr_(descr), bloom_filter_(bloom_filter) {}

template <typename ParquetType>
bool BloomFilterWriterImpl<ParquetType>::HasBloomFilter() const {
  return bloom_filter_ != nullptr;
}

template <typename ParquetType>
void BloomFilterWriterImpl<ParquetType>::UpdateBloomFilter(const T* values,
                                                           int64_t num_values) {
  if (bloom_filter_ == nullptr) {
    return;
  }
  std::array<uint64_t, kHashBatchSize> hashes;
  for (int64_t i = 0; i < num_values; i += kHashBatchSize) {
    int64_t current_hash_batch_size = std::min(kHashBatchSize, num_values - i);
    bloom_filter_->Hashes(values, static_cast<int>(current_hash_batch_size),
                          hashes.data());
    bloom_filter_->InsertHashes(hashes.data(), static_cast<int>(current_hash_batch_size));
  }
}

template <>
void BloomFilterWriterImpl<FLBAType>::UpdateBloomFilter(const FLBA* values,
                                                        int64_t num_values) {
  if (bloom_filter_ == nullptr) {
    return;
  }
  std::array<uint64_t, kHashBatchSize> hashes;
  for (int64_t i = 0; i < num_values; i += kHashBatchSize) {
    int64_t current_hash_batch_size = std::min(kHashBatchSize, num_values - i);
    bloom_filter_->Hashes(values, descr_->type_length(),
                          static_cast<int>(current_hash_batch_size), hashes.data());
    bloom_filter_->InsertHashes(hashes.data(), static_cast<int>(current_hash_batch_size));
  }
}

template <>
void BloomFilterWriterImpl<BooleanType>::UpdateBloomFilter(const bool*, int64_t) {
  if (ARROW_PREDICT_FALSE(bloom_filter_ != nullptr)) {
    throw ParquetException("BooleanType does not support bloom filters");
  }
}

template <typename ParquetType>
void BloomFilterWriterImpl<ParquetType>::UpdateBloomFilterSpaced(
    const T* values, int64_t num_values, const uint8_t* valid_bits,
    int64_t valid_bits_offset) {
  if (bloom_filter_ == nullptr) {
    // No bloom filter to update
    return;
  }
  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      valid_bits, valid_bits_offset, num_values, [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto current_hash_batch_size = std::min(kHashBatchSize, length - i);
          bloom_filter_->Hashes(values + i + position,
                                static_cast<int>(current_hash_batch_size), hashes.data());
          bloom_filter_->InsertHashes(hashes.data(),
                                      static_cast<int>(current_hash_batch_size));
        }
      });
}

template <>
void BloomFilterWriterImpl<BooleanType>::UpdateBloomFilterSpaced(const bool*, int64_t,
                                                                 const uint8_t*,
                                                                 int64_t) {}

template <>
void BloomFilterWriterImpl<FLBAType>::UpdateBloomFilterSpaced(const FLBA* values,
                                                              int64_t num_values,
                                                              const uint8_t* valid_bits,
                                                              int64_t valid_bits_offset) {
  if (bloom_filter_ == nullptr) {
    return;
  }
  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      valid_bits, valid_bits_offset, num_values, [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto current_hash_batch_size = std::min(kHashBatchSize, length - i);
          bloom_filter_->Hashes(values + i + position, descr_->type_length(),
                                static_cast<int>(current_hash_batch_size), hashes.data());
          bloom_filter_->InsertHashes(hashes.data(),
                                      static_cast<int>(current_hash_batch_size));
        }
      });
}

template <typename ArrayType>
void UpdateBinaryBloomFilter(BloomFilter& bloom_filter, const ArrayType& array) {
  // Using a smaller size because an extra `byte_arrays` is used.
  constexpr int64_t kBinaryHashBatchSize = 64;
  std::array<ByteArray, kBinaryHashBatchSize> byte_arrays;
  std::array<uint64_t, kBinaryHashBatchSize> hashes;
  int hashes_idx = 0;
  auto flush_hashes = [&]() {
    ARROW_DCHECK_NE(0, hashes_idx);
    bloom_filter.Hashes(byte_arrays.data(), static_cast<int>(hashes_idx), hashes.data());
    bloom_filter.InsertHashes(hashes.data(), static_cast<int>(hashes_idx));
    hashes_idx = 0;
  };
  PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<typename ArrayType::TypeClass>(
      *array.data(),
      [&](std::string_view view) {
        if (hashes_idx == kHashBatchSize) {
          flush_hashes();
        }
        byte_arrays[hashes_idx] = view;
        ++hashes_idx;
        return ::arrow::Status::OK();
      },
      []() { return ::arrow::Status::OK(); }));
  if (hashes_idx != 0) {
    flush_hashes();
  }
}

template <>
void BloomFilterWriterImpl<ByteArrayType>::UpdateBloomFilterArray(
    const ::arrow::Array& values) {
  if (bloom_filter_ == nullptr) {
    return;
  }
  if (::arrow::is_binary_view_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::BinaryViewArray&>(values));
  } else if (::arrow::is_binary_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::BinaryArray&>(values));
  } else if (::arrow::is_large_binary_like(values.type_id())) {
    UpdateBinaryBloomFilter(
        *bloom_filter_,
        ::arrow::internal::checked_cast<const ::arrow::LargeBinaryArray&>(values));
  } else {
    throw ParquetException("Bloom filter is not supported for this Arrow type: " +
                           values.type()->ToString());
  }
}

template <typename ParquetType>
void BloomFilterWriterImpl<ParquetType>::UpdateBloomFilterArray(
    const ::arrow::Array& values) {
  // Only ByteArray type would write ::arrow::Array directly.
  ::arrow::Unreachable("UpdateBloomFilterArray for non ByteArray type is unreachable");
}

template class PARQUET_EXPORT BloomFilterWriterImpl<BooleanType>;
template class PARQUET_EXPORT BloomFilterWriterImpl<Int32Type>;
template class PARQUET_EXPORT BloomFilterWriterImpl<Int64Type>;
template class PARQUET_EXPORT BloomFilterWriterImpl<Int96Type>;
template class PARQUET_EXPORT BloomFilterWriterImpl<FloatType>;
template class PARQUET_EXPORT BloomFilterWriterImpl<DoubleType>;
template class PARQUET_EXPORT BloomFilterWriterImpl<ByteArrayType>;
template class PARQUET_EXPORT BloomFilterWriterImpl<FLBAType>;

}  // namespace parquet::internal