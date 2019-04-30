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
#include <cmath>
#include <cstring>
#include <type_traits>

#include "arrow/util/logging.h"

#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/statistics.h"
#include "parquet/util/memory.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

// ----------------------------------------------------------------------
// Comparator implementations

template <typename DType>
struct SignedCompare {
  typedef typename DType::c_type T;
  static inline bool Compare(const ColumnDescriptor* descr, const T& a,
                             const T& b) {
    return a < b;
  }
};

template <>
struct SignedCompare<Int96Type> {
  static inline bool Compare(const ColumnDescriptor* descr, const Int96& a,
                             const Int96& b) {
    // Only the MSB bit is by Signed comparison
    // For little-endian, this is the last bit of Int96 type
    const int32_t amsb = static_cast<const int32_t>(a.value[2]);
    const int32_t bmsb = static_cast<const int32_t>(b.value[2]);
    if (amsb != bmsb) {
      return (amsb < bmsb);
    } else if (a.value[1] != b.value[1]) {
      return (a.value[1] < b.value[1]);
    }
    return (a.value[0] < b.value[0]);
  }
};

template <>
struct SignedCompare<ByteArrayType> {
  static inline bool Compare(const ColumnDescriptor* descr, const ByteArray& a,
                             const ByteArray& b) {
    const int8_t* aptr = reinterpret_cast<const int8_t*>(a.ptr);
    const int8_t* bptr = reinterpret_cast<const int8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
  }
};

template <>
struct SignedCompare<FLBAType> {
  static inline bool Compare(const ColumnDescriptor* descr, const FLBA& a,
                             const FLBA& b) {
    const int32_t type_length = descr->type_length();
    const int8_t* aptr = reinterpret_cast<const int8_t*>(a.ptr);
    const int8_t* bptr = reinterpret_cast<const int8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + type_length, bptr,
                                        bptr + type_length);
  }
};

template <typename DType>
struct UnsignedCompare {};

template <>
struct UnsignedCompare<Int32Type> {
  static inline bool Compare(const ColumnDescriptor* descr, int32_t a,
                             int32_t b) {
    const uint32_t ua = a;
    const uint32_t ub = b;
    return ua < ub;
  }
};

template <>
struct UnsignedCompare<Int64Type> {
  static inline bool Compare(const ColumnDescriptor* descr, int64_t a,
                             int64_t b) {
    const uint64_t ua = a;
    const uint64_t ub = b;
    return ua < ub;
  }
};

template <>
struct UnsignedCompare<Int96Type> {
  static inline bool Compare(const ColumnDescriptor* descr, const Int96& a,
                             const Int96& b) {
    if (a.value[2] != b.value[2]) {
      return (a.value[2] < b.value[2]);
    } else if (a.value[1] != b.value[1]) {
      return (a.value[1] < b.value[1]);
    }
    return (a.value[0] < b.value[0]);
  }
};

template <>
struct UnsignedCompare<ByteArrayType> {
  static inline bool Compare(const ColumnDescriptor* descr, const ByteArray& a,
                             const ByteArray& b) {
    const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
    const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
  }
};

template <>
struct UnsignedCompare<FLBAType> {
  static inline bool Compare(const ColumnDescriptor* descr, const FLBA& a,
                             const FLBA& b) {
    const int32_t type_length = descr->type_length();
    const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
    const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + type_length_, bptr,
                                        bptr + type_length_);
  }
};

template <typename DType>
class SignedComparator : public TypedComparator<DType> {
 public:
  typedef typename DType::c_type T;

  SignedComparator(const ColumnDescriptor* descr = nullptr)
      : descr_(descr) {}

  bool CompareInline(const T& a, const T& b) {
    return SignedCompare<DType>::Compare(descr_, a, b);
  }

  bool Compare(const T& a, const T& b) override {
    return CompareInline(a, b);
  }

 private:
  const ColumnDescriptor* descr_;
};

template <typename DType>
class UnsignedComparator : public TypedComparator<DType> {
 public:
  typedef typename DType::c_type T;

  UnsignedComparator(const ColumnDescriptor* descr = nullptr)
      : descr_(descr) {}

  bool CompareInline(const T& a, const T& b) {
    return UnsignedCompare<DType>::Compare(descr_, a, b);
  }

  bool Compare(const T& a, const T& b) override {
    return CompareInline(a, b);
  }

 private:
  const ColumnDescriptor* descr_;
};

std::shared_ptr<Comparator> Comparator::Make(const ColumnDescriptor* descr) {
  if (SortOrder::SIGNED == descr->sort_order()) {
    switch (descr->physical_type()) {
      case Type::BOOLEAN:
        return std::make_shared<SignedComparator<BooleanType>>();
      case Type::INT32:
        return std::make_shared<SignedComparator<Int32Type>>();
      case Type::INT64:
        return std::make_shared<SignedComparator<Int64Type>>();
      case Type::FLOAT:
        return std::make_shared<SignedComparator<FloatType>>();
      case Type::DOUBLE:
        return std::make_shared<SignedComparator<DoubleType>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<SignedComparator<ByteArrayType>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<SignedComparator<FLBAType>>();
      default:
        ParquetException::NYI("Signed Compare not implemented");
    }
  } else if (SortOrder::UNSIGNED == descr->sort_order()) {
    switch (descr->physical_type()) {
      case Type::INT32:
        return std::make_shared<UnsignedComparator<Int32Type>>();
      case Type::INT64:
        return std::make_shared<UnsignedComparator<Int64Type>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<UnsignedComparator<ByteArrayType>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<UnsignedComparator<FLBAType>>(descr);
      default:
        ParquetException::NYI("Unsigned Compare not implemented");
    }
  } else {
    throw ParquetException("UNKNOWN Sort Order");
  }
  return nullptr;
}

// ----------------------------------------------------------------------

template <typename DType>
class TypedStatisticsImpl : public TypedStatistics<DType> {
 public:
  using T = typename DType::c_type;

  TypedStatistics(const ColumnDescriptor* schema, MemoryPool* pool)
      : pool_(pool),
        min_buffer_(AllocateBuffer(pool_, 0)),
        max_buffer_(AllocateBuffer(pool_, 0)) {
    descr_ = schema;

    auto comp = Comparator::Make(schema);
    comparator_ = std::static_pointer_cast<TypedComparator<DType>>(comp);
    Reset();
  }

  TypedStatistics(const T& min, const T& max,
                  int64_t num_values, int64_t null_count,
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

  TypedStatistics(const ColumnDescriptor* schema, const std::string& encoded_min,
                  const std::string& encoded_max, int64_t num_values, int64_t null_count,
                  int64_t distinct_count, bool has_min_max, MemoryPool* pool)
      : TypedStatistics(schema, pool) {
    IncrementNumValues(num_values);
    IncrementNullCount(null_count);
    IncrementDistinctCount(distinct_count);

    if (!encoded_min.empty()) {
      PlainDecode(encoded_min, &min_);
    }
    if (!encoded_max.empty()) {
      PlainDecode(encoded_max, &max_);
    }
    has_min_max_ = has_min_max;
  }

  bool HasMinMax() const {
    return has_min_max_;
  }

  void Reset() {
    ResetCounts();
    has_min_max_ = false;
  }

  void SetMinMax(const T& min, const T& max) {
    if (!has_min_max_) {
      has_min_max_ = true;
      Copy(min, &min_, min_buffer_.get());
      Copy(max, &max_, max_buffer_.get());
    } else {
      Copy(comparator_->Compare(min_, min) ? min_ : min, &min_,
           min_buffer_.get());
      Copy(comparator_->Compare(max_, max) ? max : max_,
           &max_, max_buffer_.get());
    }
  }

  void Merge(const TypedStatistics<DType>& other);

  void Update(const T* values, int64_t num_not_null, int64_t num_null);
  void UpdateSpaced(const T* values, const uint8_t* valid_bits, int64_t valid_bits_spaced,
                    int64_t num_not_null, int64_t num_null);
  void SetMinMax(const T& min, const T& max);

  const T& min() const;
  const T& max() const;

  std::string EncodeMin() override;
  std::string EncodeMax() override;
  EncodedStatistics Encode() override;

 private:
  bool has_min_max_ = false;
  T min_;
  T max_;
  ::arrow::MemoryPool* pool_;

  void PlainEncode(const T& src, std::string* dst);
  void PlainDecode(const std::string& src, T* dst);
  void Copy(const T& src, T* dst, ResizableBuffer* buffer);

  std::shared_ptr<ResizableBuffer> min_buffer_, max_buffer_;
};

template <typename DType>
inline void TypedStatistics<DType>::Copy(const T& src, T* dst, ResizableBuffer*) {
  *dst = src;
}

template <>
inline void TypedStatistics<FLBAType>::Copy(const FLBA& src, FLBA* dst,
                                                    ResizableBuffer* buffer) {
  if (dst->ptr == src.ptr) return;
  uint32_t len = descr_->type_length();
  PARQUET_THROW_NOT_OK(buffer->Resize(len, false));
  std::memcpy(buffer->mutable_data(), src.ptr, len);
  *dst = FLBA(buffer->data());
}

template <>
inline void TypedStatistics<ByteArrayType>::Copy(const ByteArray& src,
                                                 ByteArray* dst,
                                                 ResizableBuffer* buffer) {
  if (dst->ptr == src.ptr) return;
  PARQUET_THROW_NOT_OK(buffer->Resize(src.len, false));
  std::memcpy(buffer->mutable_data(), src.ptr, src.len);
  *dst = ByteArray(src.len, buffer->data());
}

template <typename T, typename Enable = void>
struct StatsHelper {
  inline int64_t GetValueBeginOffset(const T* values, int64_t count) { return 0; }

  inline int64_t GetValueEndOffset(const T* values, int64_t count) { return count; }

  inline bool IsNaN(const T value) { return false; }
};

template <typename T>
struct StatsHelper<T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
  inline int64_t GetValueBeginOffset(const T* values, int64_t count) {
    // Skip NaNs
    for (int64_t i = 0; i < count; i++) {
      if (!std::isnan(values[i])) {
        return i;
      }
    }
    return count;
  }

  inline int64_t GetValueEndOffset(const T* values, int64_t count) {
    // Skip NaNs
    for (int64_t i = (count - 1); i >= 0; i--) {
      if (!std::isnan(values[i])) {
        return (i + 1);
      }
    }
    return 0;
  }

  inline bool IsNaN(const T value) { return std::isnan(value); }
};

template <typename T>
void SetNaN(T* value) {
  // no-op
}

template <>
void SetNaN<float>(float* value) {
  *value = std::nanf("");
}

template <>
void SetNaN<double>(double* value) {
  *value = std::nan("");
}

template <typename DType>
void TypedStatistics<DType>::Update(const T* values, int64_t num_not_null,
                                            int64_t num_null) {
  DCHECK_GE(num_not_null, 0);
  DCHECK_GE(num_null, 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);
  // TODO: support distinct count?
  if (num_not_null == 0) return;

  // PARQUET-1225: Handle NaNs
  // The problem arises only if the starting/ending value(s)
  // of the values-buffer contain NaN
  StatsHelper<T> helper;
  int64_t begin_offset = helper.GetValueBeginOffset(values, num_not_null);
  int64_t end_offset = helper.GetValueEndOffset(values, num_not_null);

  // All values are NaN
  if (end_offset < begin_offset) {
    // Set min/max to NaNs in this case.
    // Don't set has_min_max flag since
    // these values must be over-written by valid stats later
    if (!has_min_max_) {
      SetNaN(&min_);
      SetNaN(&max_);
    }
    return;
  }

  auto batch_minmax = std::minmax_element(values + begin_offset, values + end_offset,
                                          std::ref(*(this->comparator_)));

  SetMinMax(*batch_minmax.first, *batch_minmax.second);
}

template <typename DType>
void TypedStatistics<DType>::UpdateSpaced(const T* values,
                                                  const uint8_t* valid_bits,
                                                  int64_t valid_bits_offset,
                                                  int64_t num_not_null,
                                                  int64_t num_null) {
  DCHECK_GE(num_not_null, 0);
  DCHECK_GE(num_null, 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);
  // TODO: support distinct count?
  if (num_not_null == 0) return;

  // Find first valid entry and use that for min/max
  // As (num_not_null != 0) there must be one
  int64_t length = num_null + num_not_null;
  int64_t i = 0;
  ::arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                    length);
  StatsHelper<T> helper;
  for (; i < length; i++) {
    // PARQUET-1225: Handle NaNs
    if (valid_bits_reader.IsSet() && !helper.IsNaN(values[i])) {
      break;
    }
    valid_bits_reader.Next();
  }

  // All are NaNs and stats are not set yet
  if ((i == length) && helper.IsNaN(values[i - 1])) {
    // Don't set has_min_max flag since
    // these values must be over-written by valid stats later
    if (!has_min_max_) {
      SetNaN(&min_);
      SetNaN(&max_);
    }
    return;
  }

  T min = values[i];
  T max = values[i];
  for (; i < length; i++) {
    if (valid_bits_reader.IsSet()) {
      if ((std::ref(*(this->comparator_)))(values[i], min)) {
        min = values[i];
      } else if ((std::ref(*(this->comparator_)))(max, values[i])) {
        max = values[i];
      }
    }
    valid_bits_reader.Next();
  }

  SetMinMax(min, max);
}

template <typename DType>
const typename DType::c_type& TypedStatistics<DType>::min() const {
  return min_;
}

template <typename DType>
const typename DType::c_type& TypedStatistics<DType>::max() const {
  return max_;
}

template <typename DType>
void TypedStatistics<DType>::Merge(const TypedStatistics<DType>& other) {
  this->MergeCounts(other);

  if (!other.HasMinMax()) return;

  SetMinMax(other.min_, other.max_);
}

template <typename DType>
std::string TypedStatistics<DType>::EncodeMin() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(min_, &s);
  return s;
}

template <typename DType>
std::string TypedStatistics<DType>::EncodeMax() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(max_, &s);
  return s;
}

template <typename DType>
EncodedStatistics TypedStatistics<DType>::Encode() {
  EncodedStatistics s;
  if (HasMinMax()) {
    s.set_min(this->EncodeMin());
    s.set_max(this->EncodeMax());
  }
  s.set_null_count(this->null_count());
  return s;
}

template <typename DType>
void TypedStatistics<DType>::PlainEncode(const T& src, std::string* dst) {
  auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr(), pool_);
  encoder->Put(&src, 1);
  auto buffer = encoder->FlushValues();
  auto ptr = reinterpret_cast<const char*>(buffer->data());
  dst->assign(ptr, buffer->size());
}

template <typename DType>
void TypedStatistics<DType>::PlainDecode(const std::string& src, T* dst) {
  auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr());
  decoder->SetData(1, reinterpret_cast<const uint8_t*>(src.c_str()),
                   static_cast<int>(src.size()));
  decoder->Decode(dst, 1);
}

template <>
void TypedStatistics<ByteArrayType>::PlainEncode(const T& src, std::string* dst) {
  dst->assign(reinterpret_cast<const char*>(src.ptr), src.len);
}

template <>
void TypedStatistics<ByteArrayType>::PlainDecode(const std::string& src, T* dst) {
  dst->len = static_cast<uint32_t>(src.size());
  dst->ptr = reinterpret_cast<const uint8_t*>(src.c_str());
}

// ----------------------------------------------------------------------
// Public factory functions

std::shared_ptr<Statistics> Statistics::Make(
    const ColumnDescriptor* schema,
    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool()) {

}

std::shared_ptr<Statistics> Statistics::Make(
    const ColumnDescriptor* schema,
    const std::string& encoded_min,
    const std::string& encoded_max, int64_t num_values,
    int64_t null_count, int64_t distinct_count,
    bool has_min_max,
    ::arrow::MemoryPool* pool) {

}

}  // namespace parquet
