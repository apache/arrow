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

#include "parquet/statistics.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/ubsan.h"
#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;
using arrow::internal::checked_cast;

namespace parquet {

// ----------------------------------------------------------------------
// Comparator implementations

template <typename DType, bool is_signed>
struct CompareHelper {
  using T = typename DType::c_type;

  constexpr static T DefaultMin() { return std::numeric_limits<T>::max(); }
  constexpr static T DefaultMax() { return std::numeric_limits<T>::lowest(); }

  // MSVC17 fix, isnan is not overloaded for IntegralType as per C++11
  // standard requirements.
  template <typename T1 = T>
  static arrow::enable_if_t<std::is_floating_point<T1>::value, T> Coalesce(T val,
                                                                           T fallback) {
    return std::isnan(val) ? fallback : val;
  }

  template <typename T1 = T>
  static arrow::enable_if_t<!std::is_floating_point<T1>::value, T> Coalesce(T val,
                                                                            T fallback) {
    return val;
  }

  static inline bool Compare(int type_length, const T& a, const T& b) { return a < b; }

  static T Min(int type_length, T a, T b) { return a < b ? a : b; }
  static T Max(int type_length, T a, T b) { return a < b ? b : a; }
};

template <typename DType>
struct UnsignedCompareHelperBase {
  using T = typename DType::c_type;
  using UCType = typename std::make_unsigned<T>::type;

  constexpr static T DefaultMin() { return std::numeric_limits<UCType>::max(); }
  constexpr static T DefaultMax() { return std::numeric_limits<UCType>::lowest(); }
  static T Coalesce(T val, T fallback) { return val; }

  static inline bool Compare(int type_length, T a, T b) {
    return arrow::util::SafeCopy<UCType>(a) < arrow::util::SafeCopy<UCType>(b);
  }

  static T Min(int type_length, T a, T b) { return Compare(type_length, a, b) ? a : b; }
  static T Max(int type_length, T a, T b) { return Compare(type_length, a, b) ? b : a; }
};

template <>
struct CompareHelper<Int32Type, false> : public UnsignedCompareHelperBase<Int32Type> {};

template <>
struct CompareHelper<Int64Type, false> : public UnsignedCompareHelperBase<Int64Type> {};

template <bool is_signed>
struct CompareHelper<Int96Type, is_signed> {
  using T = typename Int96Type::c_type;
  using msb_type = typename std::conditional<is_signed, int32_t, uint32_t>::type;

  static T DefaultMin() {
    uint32_t kMsbMax = std::numeric_limits<msb_type>::max();
    uint32_t kMax = std::numeric_limits<uint32_t>::max();
    return {kMax, kMax, kMsbMax};
  }
  static T DefaultMax() {
    uint32_t kMsbMin = std::numeric_limits<msb_type>::min();
    uint32_t kMin = std::numeric_limits<uint32_t>::min();
    return {kMin, kMin, kMsbMin};
  }
  static T Coalesce(T val, T fallback) { return val; }

  static inline bool Compare(int type_length, const T& a, const T& b) {
    if (a.value[2] != b.value[2]) {
      // Only the MSB bit is by Signed comparison. For little-endian, this is the
      // last bit of Int96 type.
      return arrow::util::SafeCopy<msb_type>(a.value[2]) <
             arrow::util::SafeCopy<msb_type>(b.value[2]);
    } else if (a.value[1] != b.value[1]) {
      return (a.value[1] < b.value[1]);
    }
    return (a.value[0] < b.value[0]);
  }

  static T Min(int type_length, const T& a, const T& b) {
    return Compare(0, a, b) ? a : b;
  }
  static T Max(int type_length, const T& a, const T& b) {
    return Compare(0, a, b) ? b : a;
  }
};  // namespace parquet

template <typename DType, bool is_signed>
struct ByteLikeCompareHelperBase {
  using T = ByteArrayType::c_type;
  using PtrType = typename std::conditional<is_signed, int8_t, uint8_t>::type;

  static T DefaultMin() { return {}; }
  static T DefaultMax() { return {}; }
  static T Coalesce(T val, T fallback) { return val; }

  static inline bool Compare(int type_length, const T& a, const T& b) {
    const auto* aptr = reinterpret_cast<const PtrType*>(a.ptr);
    const auto* bptr = reinterpret_cast<const PtrType*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + a.len, bptr, bptr + b.len);
  }

  static T Min(int type_length, const T& a, const T& b) {
    if (a.ptr == nullptr) return b;
    if (b.ptr == nullptr) return a;
    return Compare(type_length, a, b) ? a : b;
  }

  static T Max(int type_length, const T& a, const T& b) {
    if (a.ptr == nullptr) return b;
    if (b.ptr == nullptr) return a;
    return Compare(type_length, a, b) ? b : a;
  }
};

template <typename DType, bool is_signed>
struct BinaryLikeCompareHelperBase {
  using T = typename DType::c_type;
  using PtrType = typename std::conditional<is_signed, int8_t, uint8_t>::type;

  static T DefaultMin() { return {}; }
  static T DefaultMax() { return {}; }
  static T Coalesce(T val, T fallback) { return val; }

  static int value_length(int value_length, const ByteArray& value) { return value.len; }

  static int value_length(int type_length, const FLBA& value) { return type_length; }

  static inline bool Compare(int type_length, const T& a, const T& b) {
    const auto* aptr = reinterpret_cast<const PtrType*>(a.ptr);
    const auto* bptr = reinterpret_cast<const PtrType*>(b.ptr);
    return std::lexicographical_compare(aptr, aptr + value_length(type_length, a), bptr,
                                        bptr + value_length(type_length, b));
  }

  static T Min(int type_length, const T& a, const T& b) {
    if (a.ptr == nullptr) return b;
    if (b.ptr == nullptr) return a;
    return Compare(type_length, a, b) ? a : b;
  }

  static T Max(int type_length, const T& a, const T& b) {
    if (a.ptr == nullptr) return b;
    if (b.ptr == nullptr) return a;
    return Compare(type_length, a, b) ? b : a;
  }
};

template <bool is_signed>
struct CompareHelper<ByteArrayType, is_signed>
    : public BinaryLikeCompareHelperBase<ByteArrayType, is_signed> {};

template <bool is_signed>
struct CompareHelper<FLBAType, is_signed>
    : public BinaryLikeCompareHelperBase<FLBAType, is_signed> {};

using ::arrow::util::optional;

template <typename T>
::arrow::enable_if_t<std::is_integral<T>::value, optional<std::pair<T, T>>>
CleanStatistic(std::pair<T, T> min_max) {
  return min_max;
}

// In case of floating point types, the following rules are applied (as per
// upstream parquet-mr):
// - If any of min/max is NaN, return nothing.
// - If min is 0.0f, replace with -0.0f
// - If max is -0.0f, replace with 0.0f
template <typename T>
::arrow::enable_if_t<std::is_floating_point<T>::value, optional<std::pair<T, T>>>
CleanStatistic(std::pair<T, T> min_max) {
  T min = min_max.first;
  T max = min_max.second;

  // Ignore if one of the value is nan.
  if (std::isnan(min) || std::isnan(max)) {
    return nonstd::nullopt;
  }

  if (min == std::numeric_limits<T>::max() && max == std::numeric_limits<T>::lowest()) {
    return nonstd::nullopt;
  }

  T zero{};

  if (min == zero && !std::signbit(min)) {
    min = -min;
  }

  if (max == zero && std::signbit(max)) {
    max = -max;
  }

  return {{min, max}};
}

optional<std::pair<FLBA, FLBA>> CleanStatistic(std::pair<FLBA, FLBA> min_max) {
  if (min_max.first.ptr == nullptr || min_max.second.ptr == nullptr) {
    return nonstd::nullopt;
  }
  return min_max;
}

optional<std::pair<ByteArray, ByteArray>> CleanStatistic(
    std::pair<ByteArray, ByteArray> min_max) {
  if (min_max.first.ptr == nullptr || min_max.second.ptr == nullptr) {
    return nonstd::nullopt;
  }
  return min_max;
}

template <bool is_signed, typename DType>
class TypedComparatorImpl : virtual public TypedComparator<DType> {
 public:
  using T = typename DType::c_type;
  using Helper = CompareHelper<DType, is_signed>;

  explicit TypedComparatorImpl(int type_length = -1) : type_length_(type_length) {}

  bool CompareInline(const T& a, const T& b) const {
    return Helper::Compare(type_length_, a, b);
  }

  bool Compare(const T& a, const T& b) override { return CompareInline(a, b); }

  std::pair<T, T> GetMinMax(const T* values, int64_t length) override {
    DCHECK_GT(length, 0);

    T min = Helper::DefaultMin();
    T max = Helper::DefaultMax();

    for (int64_t i = 0; i < length; i++) {
      auto val = values[i];
      min = Helper::Min(type_length_, min, Helper::Coalesce(val, Helper::DefaultMin()));
      max = Helper::Max(type_length_, max, Helper::Coalesce(val, Helper::DefaultMax()));
    }

    return {min, max};
  }

  std::pair<T, T> GetMinMaxSpaced(const T* values, int64_t length,
                                  const uint8_t* valid_bits,
                                  int64_t valid_bits_offset) override {
    DCHECK_GT(length, 0);

    T min = Helper::DefaultMin();
    T max = Helper::DefaultMax();

    ::arrow::internal::BitmapReader reader(valid_bits, valid_bits_offset, length);
    for (int64_t i = 0; i < length; i++, reader.Next()) {
      if (reader.IsSet()) {
        auto val = values[i];
        min = Helper::Min(type_length_, min, Helper::Coalesce(val, Helper::DefaultMin()));
        max = Helper::Max(type_length_, max, Helper::Coalesce(val, Helper::DefaultMax()));
      }
    }

    return {min, max};
  }

  std::pair<T, T> GetMinMax(const ::arrow::Array& values) override;

 private:
  int type_length_;
};

template <bool is_signed, typename DType>
std::pair<typename DType::c_type, typename DType::c_type>
TypedComparatorImpl<is_signed, DType>::GetMinMax(const ::arrow::Array& values) {
  ParquetException::NYI(values.type()->ToString());
}

template <bool is_signed>
std::pair<ByteArray, ByteArray> GetMinMaxBinaryHelper(
    const TypedComparatorImpl<is_signed, ByteArrayType>& comparator,
    const ::arrow::Array& values) {
  const auto& data = checked_cast<const ::arrow::BinaryArray&>(values);

  ByteArray min, max;
  if (data.null_count() > 0) {
    ::arrow::internal::BitmapReader reader(data.null_bitmap_data(), data.offset(),
                                           data.length());
    int64_t i = 0;
    while (!reader.IsSet()) {
      ++i;
      reader.Next();
    }

    min = data.GetView(i);
    max = data.GetView(i);
    for (; i < data.length(); i++, reader.Next()) {
      ByteArray val = data.GetView(i);
      if (reader.IsSet()) {
        min = comparator.CompareInline(val, min) ? val : min;
        max = comparator.CompareInline(max, val) ? val : max;
      }
    }
  } else {
    min = data.GetView(0);
    max = data.GetView(0);
    for (int64_t i = 0; i < data.length(); i++) {
      ByteArray val = data.GetView(i);
      min = comparator.CompareInline(val, min) ? val : min;
      max = comparator.CompareInline(max, val) ? val : max;
    }
  }

  return {min, max};
}

template <>
std::pair<ByteArray, ByteArray> TypedComparatorImpl<true, ByteArrayType>::GetMinMax(
    const ::arrow::Array& values) {
  return GetMinMaxBinaryHelper<true>(*this, values);
}

template <>
std::pair<ByteArray, ByteArray> TypedComparatorImpl<false, ByteArrayType>::GetMinMax(
    const ::arrow::Array& values) {
  return GetMinMaxBinaryHelper<false>(*this, values);
}

std::shared_ptr<Comparator> Comparator::Make(Type::type physical_type,
                                             SortOrder::type sort_order,
                                             int type_length) {
  if (SortOrder::SIGNED == sort_order) {
    switch (physical_type) {
      case Type::BOOLEAN:
        return std::make_shared<TypedComparatorImpl<true, BooleanType>>();
      case Type::INT32:
        return std::make_shared<TypedComparatorImpl<true, Int32Type>>();
      case Type::INT64:
        return std::make_shared<TypedComparatorImpl<true, Int64Type>>();
      case Type::INT96:
        return std::make_shared<TypedComparatorImpl<true, Int96Type>>();
      case Type::FLOAT:
        return std::make_shared<TypedComparatorImpl<true, FloatType>>();
      case Type::DOUBLE:
        return std::make_shared<TypedComparatorImpl<true, DoubleType>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<true, ByteArrayType>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<true, FLBAType>>(type_length);
      default:
        ParquetException::NYI("Signed Compare not implemented");
    }
  } else if (SortOrder::UNSIGNED == sort_order) {
    switch (physical_type) {
      case Type::INT32:
        return std::make_shared<TypedComparatorImpl<false, Int32Type>>();
      case Type::INT64:
        return std::make_shared<TypedComparatorImpl<false, Int64Type>>();
      case Type::INT96:
        return std::make_shared<TypedComparatorImpl<false, Int96Type>>();
      case Type::BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<false, ByteArrayType>>();
      case Type::FIXED_LEN_BYTE_ARRAY:
        return std::make_shared<TypedComparatorImpl<false, FLBAType>>(type_length);
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
    SetMinMaxPair({arg_min, arg_max});
  }

  void Merge(const TypedStatistics<DType>& other) override {
    this->MergeCounts(other);
    if (!other.HasMinMax()) return;
    SetMinMax(other.min(), other.max());
  }

  void Update(const T* values, int64_t num_not_null, int64_t num_null) override;
  void UpdateSpaced(const T* values, const uint8_t* valid_bits, int64_t valid_bits_spaced,
                    int64_t num_not_null, int64_t num_null) override;

  void Update(const ::arrow::Array& values) override {
    IncrementNullCount(values.null_count());
    IncrementNumValues(values.length() - values.null_count());

    if (values.null_count() == values.length()) {
      return;
    }

    SetMinMaxPair(comparator_->GetMinMax(values));
  }

  const T& min() const override { return min_; }

  const T& max() const override { return max_; }

  Type::type physical_type() const override { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const override { return descr_; }

  std::string EncodeMin() const override {
    std::string s;
    if (HasMinMax()) this->PlainEncode(min_, &s);
    return s;
  }

  std::string EncodeMax() const override {
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

  void PlainEncode(const T& src, std::string* dst) const;
  void PlainDecode(const std::string& src, T* dst) const;

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

  void SetMinMaxPair(std::pair<T, T> min_max) {
    // CleanStatistic can return a nullopt in case of erroneous values, e.g. NaN
    auto maybe_min_max = CleanStatistic(min_max);
    if (!maybe_min_max) return;

    auto min = maybe_min_max.value().first;
    auto max = maybe_min_max.value().second;

    if (!has_min_max_) {
      has_min_max_ = true;
      Copy(min, &min_, min_buffer_.get());
      Copy(max, &max_, max_buffer_.get());
    } else {
      Copy(comparator_->Compare(min_, min) ? min_ : min, &min_, min_buffer_.get());
      Copy(comparator_->Compare(max_, max) ? max : max_, &max_, max_buffer_.get());
    }
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

template <typename DType>
void TypedStatisticsImpl<DType>::Update(const T* values, int64_t num_not_null,
                                        int64_t num_null) {
  DCHECK_GE(num_not_null, 0);
  DCHECK_GE(num_null, 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);

  if (num_not_null == 0) return;
  SetMinMaxPair(comparator_->GetMinMax(values, num_not_null));
}

template <typename DType>
void TypedStatisticsImpl<DType>::UpdateSpaced(const T* values, const uint8_t* valid_bits,
                                              int64_t valid_bits_offset,
                                              int64_t num_not_null, int64_t num_null) {
  DCHECK_GE(num_not_null, 0);
  DCHECK_GE(num_null, 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);

  if (num_not_null == 0) return;

  int64_t length = num_null + num_not_null;
  SetMinMaxPair(
      comparator_->GetMinMaxSpaced(values, length, valid_bits, valid_bits_offset));
}

template <typename DType>
void TypedStatisticsImpl<DType>::PlainEncode(const T& src, std::string* dst) const {
  auto encoder = MakeTypedEncoder<DType>(Encoding::PLAIN, false, descr_, pool_);
  encoder->Put(&src, 1);
  auto buffer = encoder->FlushValues();
  auto ptr = reinterpret_cast<const char*>(buffer->data());
  dst->assign(ptr, buffer->size());
}

template <typename DType>
void TypedStatisticsImpl<DType>::PlainDecode(const std::string& src, T* dst) const {
  auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
  decoder->SetData(1, reinterpret_cast<const uint8_t*>(src.c_str()),
                   static_cast<int>(src.size()));
  decoder->Decode(dst, 1);
}

template <>
void TypedStatisticsImpl<ByteArrayType>::PlainEncode(const T& src,
                                                     std::string* dst) const {
  dst->assign(reinterpret_cast<const char*>(src.ptr), src.len);
}

template <>
void TypedStatisticsImpl<ByteArrayType>::PlainDecode(const std::string& src,
                                                     T* dst) const {
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
