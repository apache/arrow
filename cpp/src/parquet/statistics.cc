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

template <typename DType, bool is_signed>
struct CompareHelper {
  typedef typename DType::c_type T;
  static inline bool Compare(int type_length, const T& a, const T& b) { return a < b; }
};

template <>
struct CompareHelper<Int96Type, true> {
  static inline bool Compare(int type_length, const Int96& a, const Int96& b) {
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
struct CompareHelper<ByteArrayType, true> {
  static inline bool Compare(int type_length, const ByteArray& a, const ByteArray& b) {
    const int8_t* aptr = reinterpret_cast<const int8_t*>(a.ptr);
    const int8_t* bptr = reinterpret_cast<const int8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
  }
};

template <>
struct CompareHelper<FLBAType, true> {
  static inline bool Compare(int type_length, const FLBA& a, const FLBA& b) {
    const int8_t* aptr = reinterpret_cast<const int8_t*>(a.ptr);
    const int8_t* bptr = reinterpret_cast<const int8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + type_length, bptr,
                                        bptr + type_length);
  }
};

template <>
struct CompareHelper<Int32Type, false> {
  static inline bool Compare(int type_length, int32_t a, int32_t b) {
    const uint32_t ua = a;
    const uint32_t ub = b;
    return ua < ub;
  }
};

template <>
struct CompareHelper<Int64Type, false> {
  static inline bool Compare(int type_length, int64_t a, int64_t b) {
    const uint64_t ua = a;
    const uint64_t ub = b;
    return ua < ub;
  }
};

template <>
struct CompareHelper<Int96Type, false> {
  static inline bool Compare(int type_length, const Int96& a, const Int96& b) {
    if (a.value[2] != b.value[2]) {
      return (a.value[2] < b.value[2]);
    } else if (a.value[1] != b.value[1]) {
      return (a.value[1] < b.value[1]);
    }
    return (a.value[0] < b.value[0]);
  }
};

template <>
struct CompareHelper<ByteArrayType, false> {
  static inline bool Compare(int type_length, const ByteArray& a, const ByteArray& b) {
    const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
    const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
  }
};

template <>
struct CompareHelper<FLBAType, false> {
  static inline bool Compare(int type_length, const FLBA& a, const FLBA& b) {
    const uint8_t* aptr = reinterpret_cast<const uint8_t*>(a.ptr);
    const uint8_t* bptr = reinterpret_cast<const uint8_t*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + type_length, bptr,
                                        bptr + type_length);
  }
};

template <typename DType, bool is_signed = true>
class TypedComparatorImpl : public TypedComparator<DType> {
 public:
  typedef typename DType::c_type T;

  explicit TypedComparatorImpl(int type_length = -1) : type_length_(type_length) {}

  bool CompareInline(const T& a, const T& b) {
    return CompareHelper<DType, is_signed>::Compare(type_length_, a, b);
  }

  bool Compare(const T& a, const T& b) override { return CompareInline(a, b); }

  void GetMinMax(const T* values, int64_t length, T* out_min, T* out_max) override {
    T min = values[0];
    T max = values[0];
    for (int64_t i = 1; i < length; i++) {
      if (CompareInline(values[i], min)) {
        min = values[i];
      } else if (CompareInline(max, values[i])) {
        max = values[i];
      }
    }
    *out_min = min;
    *out_max = max;
  }

  void GetMinMaxSpaced(const T* values, int64_t length, const uint8_t* valid_bits,
                       int64_t valid_bits_offset, T* out_min, T* out_max) override {
    ::arrow::internal::BitmapReader valid_bits_reader(valid_bits, valid_bits_offset,
                                                      length);
    T min = values[0];
    T max = values[0];
    for (int64_t i = 0; i < length; i++) {
      if (valid_bits_reader.IsSet()) {
        if (CompareInline(values[i], min)) {
          min = values[i];
        } else if (CompareInline(max, values[i])) {
          max = values[i];
        }
      }
      valid_bits_reader.Next();
    }
    *out_min = min;
    *out_max = max;
  }

 private:
  int type_length_;
};

std::shared_ptr<Comparator> Comparator::Make(Type::type physical_type,
                                             SortOrder::type sort_order,
                                             int type_length) {
  if (SortOrder::SIGNED == sort_order) {
    switch (physical_type) {
      case Type::BOOLEAN:
        return std::make_shared<TypedComparatorImpl<BooleanType>>();
      case Type::INT32:
        return std::make_shared<TypedComparatorImpl<Int32Type>>();
      case Type::INT64:
        return std::make_shared<TypedComparatorImpl<Int64Type>>();
      case Type::INT96:
        return std::make_shared<TypedComparatorImpl<Int96Type>>();
      case Type::FLOAT:
        return std::make_shared<TypedComparatorImpl<FloatType>>();
      case Type::DOUBLE:
        return std::make_shared<TypedComparatorImpl<DoubleType>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<ByteArrayType>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<FLBAType>>(type_length);
      default:
        ParquetException::NYI("Signed Compare not implemented");
    }
  } else if (SortOrder::UNSIGNED == sort_order) {
    switch (physical_type) {
      case Type::INT32:
        return std::make_shared<TypedComparatorImpl<Int32Type, false>>();
      case Type::INT64:
        return std::make_shared<TypedComparatorImpl<Int64Type, false>>();
      case Type::INT96:
        return std::make_shared<TypedComparatorImpl<Int96Type, false>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<ByteArrayType, false>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<FLBAType, false>>(type_length);
      default:
        ParquetException::NYI("Unsigned Compare not implemented");
    }
  } else {
    throw ParquetException("UNKNOWN Sort Order");
  }
  return nullptr;
}

std::shared_ptr<Comparator> Comparator::Make(const ColumnDescriptor* descr) {
  return Make(descr->physical_type(), descr->sort_order(), descr->type_length());
}

// ----------------------------------------------------------------------

template <typename DType>
class TypedStatisticsImpl : public TypedStatistics<DType> {
 public:
  using T = typename DType::c_type;

  TypedStatisticsImpl(const ColumnDescriptor* descr, MemoryPool* pool)
      : descr_(descr),
        pool_(pool),
        min_buffer_(AllocateBuffer(pool_, 0)),
        max_buffer_(AllocateBuffer(pool_, 0)) {
    auto comp = Comparator::Make(descr);
    comparator_ = std::static_pointer_cast<TypedComparator<DType>>(comp);
    Reset();
  }

  TypedStatisticsImpl(const T& min, const T& max, int64_t num_values, int64_t null_count,
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

  TypedStatisticsImpl(const ColumnDescriptor* descr, const std::string& encoded_min,
                      const std::string& encoded_max, int64_t num_values,
                      int64_t null_count, int64_t distinct_count, bool has_min_max,
                      MemoryPool* pool)
      : TypedStatisticsImpl(descr, pool) {
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

  bool HasMinMax() const override { return has_min_max_; }

  void Reset() override {
    ResetCounts();
    has_min_max_ = false;
  }

  void SetMinMax(const T& arg_min, const T& arg_max) override {
    if (!has_min_max_) {
      has_min_max_ = true;
      Copy(arg_min, &min_, min_buffer_.get());
      Copy(arg_max, &max_, max_buffer_.get());
    } else {
      Copy(comparator_->Compare(min_, arg_min) ? min_ : arg_min, &min_,
           min_buffer_.get());
      Copy(comparator_->Compare(max_, arg_max) ? arg_max : max_, &max_,
           max_buffer_.get());
    }
  }

  void Merge(const TypedStatistics<DType>& other) override {
    this->MergeCounts(other);
    if (!other.HasMinMax()) return;
    SetMinMax(other.min(), other.max());
  }

  void Update(const T* values, int64_t num_not_null, int64_t num_null) override;
  void UpdateSpaced(const T* values, const uint8_t* valid_bits, int64_t valid_bits_spaced,
                    int64_t num_not_null, int64_t num_null) override;

  const T& min() const override { return min_; }

  const T& max() const override { return max_; }

  Type::type physical_type() const override { return descr_->physical_type(); }

  std::string EncodeMin() override {
    std::string s;
    if (HasMinMax()) this->PlainEncode(min_, &s);
    return s;
  }

  std::string EncodeMax() override {
    std::string s;
    if (HasMinMax()) this->PlainEncode(max_, &s);
    return s;
  }

  EncodedStatistics Encode() override {
    EncodedStatistics s;
    if (HasMinMax()) {
      s.set_min(this->EncodeMin());
      s.set_max(this->EncodeMax());
    }
    s.set_null_count(this->null_count());
    return s;
  }

  int64_t null_count() const override { return statistics_.null_count; }
  int64_t distinct_count() const override { return statistics_.distinct_count; }
  int64_t num_values() const override { return num_values_; }

 private:
  const ColumnDescriptor* descr_;
  bool has_min_max_ = false;
  T min_;
  T max_;
  ::arrow::MemoryPool* pool_;
  int64_t num_values_ = 0;
  EncodedStatistics statistics_;
  std::shared_ptr<TypedComparator<DType>> comparator_;
  std::shared_ptr<ResizableBuffer> min_buffer_, max_buffer_;

  void PlainEncode(const T& src, std::string* dst);
  void PlainDecode(const std::string& src, T* dst);

  void Copy(const T& src, T* dst, ResizableBuffer*) { *dst = src; }

  void IncrementNullCount(int64_t n) { statistics_.null_count += n; }

  void IncrementNumValues(int64_t n) { num_values_ += n; }

  void IncrementDistinctCount(int64_t n) { statistics_.distinct_count += n; }

  void MergeCounts(const Statistics& other) {
    this->statistics_.null_count += other.null_count();
    this->statistics_.distinct_count += other.distinct_count();
    this->num_values_ += other.num_values();
  }

  void ResetCounts() {
    this->statistics_.null_count = 0;
    this->statistics_.distinct_count = 0;
    this->num_values_ = 0;
  }
};

template <>
inline void TypedStatisticsImpl<FLBAType>::Copy(const FLBA& src, FLBA* dst,
                                                ResizableBuffer* buffer) {
  if (dst->ptr == src.ptr) return;
  uint32_t len = descr_->type_length();
  PARQUET_THROW_NOT_OK(buffer->Resize(len, false));
  std::memcpy(buffer->mutable_data(), src.ptr, len);
  *dst = FLBA(buffer->data());
}

template <>
inline void TypedStatisticsImpl<ByteArrayType>::Copy(const ByteArray& src, ByteArray* dst,
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
void TypedStatisticsImpl<DType>::Update(const T* values, int64_t num_not_null,
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

  T batch_min, batch_max;
  comparator_->GetMinMax(values + begin_offset, end_offset - begin_offset, &batch_min,
                         &batch_max);
  SetMinMax(batch_min, batch_max);
}

template <typename DType>
void TypedStatisticsImpl<DType>::UpdateSpaced(const T* values, const uint8_t* valid_bits,
                                              int64_t valid_bits_offset,
                                              int64_t num_not_null, int64_t num_null) {
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

  // Find min and max values from remaining non-NaN values
  T batch_min, batch_max;
  comparator_->GetMinMaxSpaced(values + i, length - i, valid_bits, valid_bits_offset + i,
                               &batch_min, &batch_max);
  SetMinMax(batch_min, batch_max);
}

template <typename DType>
void TypedStatisticsImpl<DType>::PlainEncode(const T& src, std::string* dst) {
  auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr_, pool_);
  encoder->Put(&src, 1);
  auto buffer = encoder->FlushValues();
  auto ptr = reinterpret_cast<const char*>(buffer->data());
  dst->assign(ptr, buffer->size());
}

template <typename DType>
void TypedStatisticsImpl<DType>::PlainDecode(const std::string& src, T* dst) {
  auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
  decoder->SetData(1, reinterpret_cast<const uint8_t*>(src.c_str()),
                   static_cast<int>(src.size()));
  decoder->Decode(dst, 1);
}

template <>
void TypedStatisticsImpl<ByteArrayType>::PlainEncode(const T& src, std::string* dst) {
  dst->assign(reinterpret_cast<const char*>(src.ptr), src.len);
}

template <>
void TypedStatisticsImpl<ByteArrayType>::PlainDecode(const std::string& src, T* dst) {
  dst->len = static_cast<uint32_t>(src.size());
  dst->ptr = reinterpret_cast<const uint8_t*>(src.c_str());
}

// ----------------------------------------------------------------------
// Public factory functions

std::shared_ptr<Statistics> Statistics::Make(const ColumnDescriptor* descr,
                                             ::arrow::MemoryPool* pool) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<TypedStatisticsImpl<BooleanType>>(descr, pool);
    case Type::INT32:
      return std::make_shared<TypedStatisticsImpl<Int32Type>>(descr, pool);
    case Type::INT64:
      return std::make_shared<TypedStatisticsImpl<Int64Type>>(descr, pool);
    case Type::FLOAT:
      return std::make_shared<TypedStatisticsImpl<FloatType>>(descr, pool);
    case Type::DOUBLE:
      return std::make_shared<TypedStatisticsImpl<DoubleType>>(descr, pool);
    case Type::BYTE_ARRAY:
      return std::make_shared<TypedStatisticsImpl<ByteArrayType>>(descr, pool);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<TypedStatisticsImpl<FLBAType>>(descr, pool);
    default:
      ParquetException::NYI("Statistics not implemented");
  }
}

std::shared_ptr<Statistics> Statistics::Make(Type::type physical_type, const void* min,
                                             const void* max, int64_t num_values,
                                             int64_t null_count, int64_t distinct_count) {
#define MAKE_STATS(CAP_TYPE, KLASS)                                                    \
  case Type::CAP_TYPE:                                                                 \
    return std::make_shared<TypedStatisticsImpl<KLASS>>(                               \
        *reinterpret_cast<const typename KLASS::c_type*>(min),                         \
        *reinterpret_cast<const typename KLASS::c_type*>(max), num_values, null_count, \
        distinct_count)

  switch (physical_type) {
    MAKE_STATS(BOOLEAN, BooleanType);
    MAKE_STATS(INT32, Int32Type);
    MAKE_STATS(INT64, Int64Type);
    MAKE_STATS(FLOAT, FloatType);
    MAKE_STATS(DOUBLE, DoubleType);
    MAKE_STATS(BYTE_ARRAY, ByteArrayType);
    MAKE_STATS(FIXED_LEN_BYTE_ARRAY, FLBAType);
    default:
      break;
  }
#undef MAKE_STATS
  DCHECK(false) << "Cannot reach here";
  return nullptr;
}

std::shared_ptr<Statistics> Statistics::Make(const ColumnDescriptor* descr,
                                             const std::string& encoded_min,
                                             const std::string& encoded_max,
                                             int64_t num_values, int64_t null_count,
                                             int64_t distinct_count, bool has_min_max,
                                             ::arrow::MemoryPool* pool) {
#define MAKE_STATS(CAP_TYPE, KLASS)                                              \
  case Type::CAP_TYPE:                                                           \
    return std::make_shared<TypedStatisticsImpl<KLASS>>(                         \
        descr, encoded_min, encoded_max, num_values, null_count, distinct_count, \
        has_min_max, pool)

  switch (descr->physical_type()) {
    MAKE_STATS(BOOLEAN, BooleanType);
    MAKE_STATS(INT32, Int32Type);
    MAKE_STATS(INT64, Int64Type);
    MAKE_STATS(FLOAT, FloatType);
    MAKE_STATS(DOUBLE, DoubleType);
    MAKE_STATS(BYTE_ARRAY, ByteArrayType);
    MAKE_STATS(FIXED_LEN_BYTE_ARRAY, FLBAType);
    default:
      break;
  }
#undef MAKE_STATS
  DCHECK(false) << "Cannot reach here";
  return nullptr;
}

}  // namespace parquet
