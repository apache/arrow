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

#include <algorithm>
#include <cstring>

#include "parquet/encoding-internal.h"
#include "parquet/exception.h"
#include "parquet/statistics.h"
#include "parquet/util/comparison.h"
#include "parquet/util/memory.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(
    const ColumnDescriptor* schema, MemoryPool* pool)
    : pool_(pool),
      min_buffer_(AllocateBuffer(pool_, 0)),
      max_buffer_(AllocateBuffer(pool_, 0)) {
  SetDescr(schema);
  Reset();
}

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(const typename DType::c_type& min,
    const typename DType::c_type& max, int64_t num_values, int64_t null_count,
    int64_t distinct_count)
    : pool_(default_memory_pool()),
      min_buffer_(AllocateBuffer(pool_, 0)),
      max_buffer_(AllocateBuffer(pool_, 0)) {
  IncrementNumValues(num_values);
  IncrementNullCount(null_count);
  IncrementDistinctCount(distinct_count);

  Copy(min, &min_, min_buffer_.get());
  Copy(max, &max_, max_buffer_.get());
  has_min_max_ = true;
}

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(const ColumnDescriptor* schema,
    const std::string& encoded_min, const std::string& encoded_max, int64_t num_values,
    int64_t null_count, int64_t distinct_count, bool has_min_max, MemoryPool* pool)
    : pool_(pool),
      min_buffer_(AllocateBuffer(pool_, 0)),
      max_buffer_(AllocateBuffer(pool_, 0)) {
  IncrementNumValues(num_values);
  IncrementNullCount(null_count);
  IncrementDistinctCount(distinct_count);

  SetDescr(schema);

  if (!encoded_min.empty()) { PlainDecode(encoded_min, &min_); }
  if (!encoded_max.empty()) { PlainDecode(encoded_max, &max_); }
  has_min_max_ = has_min_max;
}

template <typename DType>
bool TypedRowGroupStatistics<DType>::HasMinMax() const {
  return has_min_max_;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Reset() {
  ResetCounts();
  has_min_max_ = false;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Update(
    const T* values, int64_t num_not_null, int64_t num_null) {
  DCHECK(num_not_null >= 0);
  DCHECK(num_null >= 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);
  // TODO: support distinct count?
  if (num_not_null == 0) return;

  Compare<T> compare(descr_);
  auto batch_minmax = std::minmax_element(values, values + num_not_null, compare);
  if (!has_min_max_) {
    has_min_max_ = true;
    Copy(*batch_minmax.first, &min_, min_buffer_.get());
    Copy(*batch_minmax.second, &max_, max_buffer_.get());
  } else {
    Copy(std::min(min_, *batch_minmax.first, compare), &min_, min_buffer_.get());
    Copy(std::max(max_, *batch_minmax.second, compare), &max_, max_buffer_.get());
  }
}

template <typename DType>
void TypedRowGroupStatistics<DType>::UpdateSpaced(const T* values,
    const uint8_t* valid_bits, int64_t valid_bits_offset, int64_t num_not_null,
    int64_t num_null) {
  DCHECK(num_not_null >= 0);
  DCHECK(num_null >= 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);
  // TODO: support distinct count?
  if (num_not_null == 0) return;

  Compare<T> compare(descr_);
  INIT_BITSET(valid_bits, static_cast<int>(valid_bits_offset));
  // Find first valid entry and use that for min/max
  // As (num_not_null != 0) there must be one
  int64_t length = num_null + num_not_null;
  int64_t i = 0;
  for (; i < length; i++) {
    if (bitset_valid_bits & (1 << bit_offset_valid_bits)) { break; }
    READ_NEXT_BITSET(valid_bits);
  }
  T min = values[i];
  T max = values[i];
  for (; i < length; i++) {
    if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
      if (compare(values[i], min)) {
        min = values[i];
      } else if (compare(max, values[i])) {
        max = values[i];
      }
    }
    READ_NEXT_BITSET(valid_bits);
  }
  if (!has_min_max_) {
    has_min_max_ = true;
    Copy(min, &min_, min_buffer_.get());
    Copy(max, &max_, max_buffer_.get());
  } else {
    Copy(std::min(min_, min, compare), &min_, min_buffer_.get());
    Copy(std::max(max_, max, compare), &max_, max_buffer_.get());
  }
}

template <typename DType>
const typename DType::c_type& TypedRowGroupStatistics<DType>::min() const {
  return min_;
}

template <typename DType>
const typename DType::c_type& TypedRowGroupStatistics<DType>::max() const {
  return max_;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Merge(const TypedRowGroupStatistics<DType>& other) {
  this->MergeCounts(other);

  if (!other.HasMinMax()) return;

  if (!has_min_max_) {
    Copy(other.min_, &this->min_, min_buffer_.get());
    Copy(other.max_, &this->max_, max_buffer_.get());
    has_min_max_ = true;
    return;
  }

  Compare<T> compare(descr_);
  Copy(std::min(this->min_, other.min_, compare), &this->min_, min_buffer_.get());
  Copy(std::max(this->max_, other.max_, compare), &this->max_, max_buffer_.get());
}

template <typename DType>
std::string TypedRowGroupStatistics<DType>::EncodeMin() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(min_, &s);
  return s;
}

template <typename DType>
std::string TypedRowGroupStatistics<DType>::EncodeMax() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(max_, &s);
  return s;
}

template <typename DType>
EncodedStatistics TypedRowGroupStatistics<DType>::Encode() {
  EncodedStatistics s;
  if (HasMinMax()) {
    s.set_min(this->EncodeMin());
    s.set_max(this->EncodeMax());
  }
  s.set_null_count(this->null_count());
  return s;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::PlainEncode(const T& src, std::string* dst) {
  PlainEncoder<DType> encoder(descr(), pool_);
  encoder.Put(&src, 1);
  auto buffer = encoder.FlushValues();
  auto ptr = reinterpret_cast<const char*>(buffer->data());
  dst->assign(ptr, buffer->size());
}

template <typename DType>
void TypedRowGroupStatistics<DType>::PlainDecode(const std::string& src, T* dst) {
  PlainDecoder<DType> decoder(descr());
  decoder.SetData(
      1, reinterpret_cast<const uint8_t*>(src.c_str()), static_cast<int>(src.size()));
  decoder.Decode(dst, 1);
}

template <>
void TypedRowGroupStatistics<ByteArrayType>::PlainEncode(const T& src, std::string* dst) {
  dst->assign(reinterpret_cast<const char*>(src.ptr), src.len);
}

template <>
void TypedRowGroupStatistics<ByteArrayType>::PlainDecode(const std::string& src, T* dst) {
  dst->len = static_cast<uint32_t>(src.size());
  dst->ptr = reinterpret_cast<const uint8_t*>(src.c_str());
}

template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<BooleanType>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<Int32Type>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<Int64Type>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<Int96Type>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<FloatType>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<DoubleType>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<ByteArrayType>;
template class PARQUET_TEMPLATE_EXPORT TypedRowGroupStatistics<FLBAType>;

}  // namespace parquet
