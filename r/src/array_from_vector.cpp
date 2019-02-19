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

#include "arrow_types.h"

using namespace Rcpp;
using namespace arrow;

namespace arrow {
namespace r {

template <int RTYPE>
inline bool isna(typename Vector<RTYPE>::stored_type x) {
  return Vector<RTYPE>::is_na(x);
}

template <>
inline bool isna<REALSXP>(double x) {
  return ISNA(x);
}

template <int RTYPE, typename Type>
std::shared_ptr<Array> SimpleArray(SEXP x) {
  Rcpp::Vector<RTYPE> vec(x);
  auto n = vec.size();
  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
    std::make_shared<RBuffer<RTYPE>>(vec)};

  int null_count = 0;
  if (RTYPE != RAWSXP) {
    std::shared_ptr<Buffer> null_bitmap;

    auto first_na = std::find_if(vec.begin(), vec.end(), isna<RTYPE> );
    if (first_na < vec.end()) {
      STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_bitmap));
      internal::FirstTimeBitmapWriter bitmap_writer(null_bitmap->mutable_data(), 0, n);

      // first loop to clear all the bits before the first NA
      auto j = std::distance(vec.begin(), first_na);
      int i = 0;
      for (; i < j; i++, bitmap_writer.Next()) {
        bitmap_writer.Set();
      }

      // then finish
      for (; i < n; i++, bitmap_writer.Next()) {
        if (isna<RTYPE>(vec[i])) {
          bitmap_writer.Clear();
          null_count++;
        } else {
          bitmap_writer.Set();
        }
      }

      bitmap_writer.Finish();
      buffers[0] = std::move(null_bitmap);
    }
  }

  auto data = ArrayData::Make(
    std::make_shared<Type>(), LENGTH(x), std::move(buffers), null_count, 0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<typename TypeTraits<Type>::ArrayType>(data);
}

std::shared_ptr<arrow::Array> Int64Array(SEXP x) {
  auto p_vec_start = reinterpret_cast<int64_t*>(REAL(x));
  auto n = Rf_xlength(x);
  int64_t null_count = 0;

  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
    std::make_shared<RBuffer<REALSXP>>(x)};

  auto p_vec = std::find(p_vec_start, p_vec_start + n, NA_INT64);
  auto first_na = p_vec - p_vec_start;
  if (first_na < n) {
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &buffers[0]));
    internal::FirstTimeBitmapWriter bitmap_writer(buffers[0]->mutable_data(), 0, n);

    // first loop to clear all the bits before the first NA
    int i = 0;
    for (; i < first_na; i++, bitmap_writer.Next()) {
      bitmap_writer.Set();
    }

    // then finish
    for (; i < n; i++, bitmap_writer.Next(), ++p_vec) {
      if (*p_vec == NA_INT64) {
        bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
      }
    }

    bitmap_writer.Finish();
  }

  auto data = ArrayData::Make(
    std::make_shared<Int64Type>(), n, std::move(buffers), null_count, 0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<typename TypeTraits<Int64Type>::ArrayType>(data);
}




std::shared_ptr<arrow::Array> MakeBooleanArray(LogicalVector_ vec) {
  R_xlen_t n = vec.size();

  // allocate a buffer for the data
  std::shared_ptr<Buffer> data_bitmap;
  STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &data_bitmap));
  auto data_bitmap_data = data_bitmap->mutable_data();
  internal::FirstTimeBitmapWriter bitmap_writer(data_bitmap_data, 0, n);
  R_xlen_t null_count = 0;

  // loop until the first no null
  R_xlen_t i = 0;
  for (; i < n; i++, bitmap_writer.Next()) {
    if (vec[i] == 0) {
      bitmap_writer.Clear();
    } else if (vec[i] == NA_LOGICAL) {
      break;
    } else {
      bitmap_writer.Set();
    }
  }

  std::shared_ptr<arrow::Buffer> null_bitmap(nullptr);
  if (i < n) {
    // there has been a null before the end, so we need
    // to collect that information in a null bitmap
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_bitmap));
    auto null_bitmap_data = null_bitmap->mutable_data();
    internal::FirstTimeBitmapWriter null_bitmap_writer(null_bitmap_data, 0, n);

    // catch up on the initial `i` bits
    for (R_xlen_t j = 0; j < i; j++, null_bitmap_writer.Next()) {
      null_bitmap_writer.Set();
    }

    // finish both bitmaps
    for (; i < n; i++, bitmap_writer.Next(), null_bitmap_writer.Next()) {
      if (vec[i] == 0) {
        bitmap_writer.Clear();
        null_bitmap_writer.Set();
      } else if (vec[i] == NA_LOGICAL) {
        null_bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
        null_bitmap_writer.Set();
      }
    }
    null_bitmap_writer.Finish();
  }
  bitmap_writer.Finish();

  auto data =
    ArrayData::Make(boolean(), n, {std::move(null_bitmap), std::move(data_bitmap)},
      null_count, 0 /*offset*/
    );

  // return the right Array class
  return MakeArray(data);
}

std::shared_ptr<Array> MakeStringArray(StringVector_ vec) {
  R_xlen_t n = vec.size();

  std::shared_ptr<Buffer> null_buffer;
  std::shared_ptr<Buffer> offset_buffer;
  std::shared_ptr<Buffer> value_buffer;

  // there is always an offset buffer
  STOP_IF_NOT_OK(AllocateBuffer((n + 1) * sizeof(int32_t), &offset_buffer));

  R_xlen_t i = 0;
  int current_offset = 0;
  int64_t null_count = 0;
  auto p_offset = reinterpret_cast<int32_t*>(offset_buffer->mutable_data());
  *p_offset = 0;
  for (++p_offset; i < n; i++, ++p_offset) {
    SEXP s = STRING_ELT(vec, i);
    if (s == NA_STRING) {
      // break as we are going to need a null_bitmap buffer
      break;
    }

    *p_offset = current_offset += LENGTH(s);
  }

  if (i < n) {
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_buffer));
    internal::FirstTimeBitmapWriter null_bitmap_writer(null_buffer->mutable_data(), 0, n);

    // catch up
    for (R_xlen_t j = 0; j < i; j++, null_bitmap_writer.Next()) {
      null_bitmap_writer.Set();
    }

    // resume offset filling
    for (; i < n; i++, ++p_offset, null_bitmap_writer.Next()) {
      SEXP s = STRING_ELT(vec, i);
      if (s == NA_STRING) {
        null_bitmap_writer.Clear();
        *p_offset = current_offset;
        null_count++;
      } else {
        null_bitmap_writer.Set();
        *p_offset = current_offset += LENGTH(s);
      }
    }

    null_bitmap_writer.Finish();
  }

  // ----- data buffer
  if (current_offset > 0) {
    STOP_IF_NOT_OK(AllocateBuffer(current_offset, &value_buffer));
    p_offset = reinterpret_cast<int32_t*>(offset_buffer->mutable_data());
    auto p_data = reinterpret_cast<char*>(value_buffer->mutable_data());

    for (R_xlen_t i = 0; i < n; i++) {
      SEXP s = STRING_ELT(vec, i);
      if (s != NA_STRING) {
        auto ni = LENGTH(s);
        std::copy_n(CHAR(s), ni, p_data);
        p_data += ni;
      }
    }
  }

  auto data = ArrayData::Make(arrow::utf8(), n,
  {null_buffer, offset_buffer, value_buffer}, null_count, 0);
  return MakeArray(data);
}

template <typename Type>
std::shared_ptr<Array> MakeFactorArrayImpl(Rcpp::IntegerVector_ factor) {
  using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
  auto dict_values = MakeStringArray(Rf_getAttrib(factor, R_LevelsSymbol));
  auto dict_type =
    dictionary(std::make_shared<Type>(), dict_values, Rf_inherits(factor, "ordered"));

  auto n = factor.size();

  std::shared_ptr<Buffer> indices_buffer;
  STOP_IF_NOT_OK(AllocateBuffer(n * sizeof(value_type), &indices_buffer));

  std::vector<std::shared_ptr<Buffer>> buffers{nullptr, indices_buffer};

  int64_t null_count = 0;
  R_xlen_t i = 0;
  auto p_factor = factor.begin();
  auto p_indices = reinterpret_cast<value_type*>(indices_buffer->mutable_data());
  for (; i < n; i++, ++p_indices, ++p_factor) {
    if (*p_factor == NA_INTEGER) break;
    *p_indices = *p_factor - 1;
  }

  if (i < n) {
    // there are NA's so we need a null buffer
    std::shared_ptr<Buffer> null_buffer;
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_buffer));
    internal::FirstTimeBitmapWriter null_bitmap_writer(null_buffer->mutable_data(), 0, n);

    // catch up
    for (R_xlen_t j = 0; j < i; j++, null_bitmap_writer.Next()) {
      null_bitmap_writer.Set();
    }

    // resume offset filling
    for (; i < n; i++, ++p_indices, ++p_factor, null_bitmap_writer.Next()) {
      if (*p_factor == NA_INTEGER) {
        null_bitmap_writer.Clear();
        null_count++;
      } else {
        null_bitmap_writer.Set();
        *p_indices = *p_factor - 1;
      }
    }

    null_bitmap_writer.Finish();
    buffers[0] = std::move(null_buffer);
  }

  auto array_indices_data =
    ArrayData::Make(std::make_shared<Type>(), n, std::move(buffers), null_count, 0);
  auto array_indices = MakeArray(array_indices_data);

  std::shared_ptr<Array> out;
  STOP_IF_NOT_OK(DictionaryArray::FromArrays(dict_type, array_indices, &out));
  return out;
}

std::shared_ptr<Array> MakeFactorArray(Rcpp::IntegerVector_ factor) {
  SEXP levels = factor.attr("levels");
  int n = Rf_length(levels);
  if (n < 128) {
    return MakeFactorArrayImpl<arrow::Int8Type>(factor);
  } else if (n < 32768) {
    return MakeFactorArrayImpl<arrow::Int16Type>(factor);
  } else {
    return MakeFactorArrayImpl<arrow::Int32Type>(factor);
  }
}

template <typename T>
int64_t time_cast(T value);

template <>
inline int64_t time_cast<int>(int value) {
  return static_cast<int64_t>(value) * 1000;
}

template <>
inline int64_t time_cast<double>(double value) {
  return static_cast<int64_t>(value * 1000);
}

inline int64_t timestamp_cast(int value) { return static_cast<int64_t>(value) * 1000000; }

template <int RTYPE>
std::shared_ptr<Array> TimeStampArray_From_POSIXct(SEXP x) {
  Rcpp::Vector<RTYPE> vec(x);
  auto p_vec = vec.begin();
  auto n = vec.size();

  std::shared_ptr<Buffer> values_buffer;
  STOP_IF_NOT_OK(AllocateBuffer(n * sizeof(int64_t), &values_buffer));
  auto p_values = reinterpret_cast<int64_t*>(values_buffer->mutable_data());

  std::vector<std::shared_ptr<Buffer>> buffers{nullptr, values_buffer};

  int null_count = 0;
  R_xlen_t i = 0;
  for (; i < n; i++, ++p_vec, ++p_values) {
    if (Rcpp::Vector<RTYPE>::is_na(*p_vec)) break;
    *p_values = timestamp_cast(*p_vec);
  }
  if (i < n) {
    std::shared_ptr<Buffer> null_buffer;
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_buffer));
    internal::FirstTimeBitmapWriter bitmap_writer(null_buffer->mutable_data(), 0, n);

    // catch up
    for (R_xlen_t j = 0; j < i; j++, bitmap_writer.Next()) {
      bitmap_writer.Set();
    }

    // finish
    for (; i < n; i++, ++p_vec, ++p_values, bitmap_writer.Next()) {
      if (Rcpp::Vector<RTYPE>::is_na(*p_vec)) {
        bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
        *p_values = timestamp_cast(*p_vec);
      }
    }

    bitmap_writer.Finish();
    buffers[0] = std::move(null_buffer);
  }

  auto data = ArrayData::Make(std::make_shared<TimestampType>(TimeUnit::MICRO, "GMT"), n,
    std::move(buffers), null_count, 0);

  return std::make_shared<TimestampArray>(data);
}

inline int difftime_unit_multiplier(SEXP x) {
  std::string unit(CHAR(STRING_ELT(Rf_getAttrib(x, symbols::units), 0)));
  if (unit == "secs") {
    return 1;
  } else if (unit == "mins") {
    return 60;
  } else if (unit == "hours") {
    return 3600;
  } else if (unit == "days") {
    return 86400;
  } else if (unit == "weeks") {
    return 604800;
  }
  Rcpp::stop("unknown difftime unit");
  return 0;
}

std::shared_ptr<arrow::Array> Time32Array_From_difftime(SEXP x) {
  // number of seconds as a double
  auto p_vec_start = REAL(x);
  auto n = Rf_xlength(x);
  int64_t null_count = 0;

  int multiplier = difftime_unit_multiplier(x);
  std::vector<std::shared_ptr<Buffer>> buffers(2);

  STOP_IF_NOT_OK(AllocateBuffer(n * sizeof(int32_t), &buffers[1]));
  auto p_values = reinterpret_cast<int32_t*>(buffers[1]->mutable_data());

  R_xlen_t i = 0;
  auto p_vec = p_vec_start;
  for (; i < n; i++, ++p_vec, ++p_values) {
    if (NumericVector::is_na(*p_vec)) {
      break;
    }
    *p_values = static_cast<int32_t>(*p_vec * multiplier);
  }

  if (i < n) {
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &buffers[0]));
    internal::FirstTimeBitmapWriter bitmap_writer(buffers[0]->mutable_data(), 0, n);

    // first loop to clear all the bits before the first NA
    for (R_xlen_t j = 0; j < i; j++, bitmap_writer.Next()) {
      bitmap_writer.Set();
    }

    // then finish
    for (; i < n; i++, bitmap_writer.Next(), ++p_vec, ++p_values) {
      if (NumericVector::is_na(*p_vec)) {
        bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
        *p_values = static_cast<int32_t>(*p_vec * multiplier);
      }
    }

    bitmap_writer.Finish();
  }

  auto data = ArrayData::Make(
    time32(TimeUnit::SECOND), n, std::move(buffers), null_count, 0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<Time32Array>(data);
}

}
}

// ---------------- new api





namespace arrow{
using internal::checked_cast;

namespace internal{

template <typename T, typename Target>
Status int_cast(T x, Target* out) {
  if (x < std::numeric_limits<Target>::min() || x > std::numeric_limits<Target>::max()) {
    return Status::Invalid("Value is too large to fit in C integer type");
  }
  *out = static_cast<Target>(x);
  return Status::OK();
}

template <typename Int>
Status double_cast(Int x, double* out) {
  *out = static_cast<double>(x);
  return Status::OK();
}

template <>
Status double_cast<int64_t>(int64_t x, double* out) {
  constexpr int64_t kDoubleMax = 1LL << 53;
  constexpr int64_t kDoubleMin = -(1LL << 53);

  if (x < kDoubleMin || x > kDoubleMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
      " representable by a IEEE 754 double precision value");
  }
  *out = static_cast<double>(x);
  return Status::OK();
}

// used for int and int64_t
template <typename T>
Status float_cast(T x, float* out) {
  constexpr int64_t kHalfFloatMax = 1LL << 24;
  constexpr int64_t kHalfFloatMin = -(1LL << 24);

  int64_t x64 = static_cast<int64_t>(x);
  if (x64 < kHalfFloatMin || x64 > kHalfFloatMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
      " representable by a IEEE 754 half precision value");
  }

  *out = static_cast<float>(x);
  return Status::OK();
}

template <>
Status float_cast<double>(double x, float* out) {
  //  TODO: is there some sort of floating point overflow ?
  *out = static_cast<float>(x);
  return Status::OK();
}

}

namespace r{

class VectorConverter;

Status GetConverter(const std::shared_ptr<DataType>& type, std::unique_ptr<VectorConverter>* out);

class VectorConverter {
public:
  virtual ~VectorConverter() = default;

  virtual Status Init(ArrayBuilder* builder) = 0;

  virtual Status Ingest(SEXP obj) = 0;

  virtual Status GetResult(std::vector<std::shared_ptr<arrow::Array>>* chunks) {
    *chunks = chunks_;

    // Still some accumulated data in the builder. If there are no chunks, we
    // always call Finish to deal with the edge case where a size-0 sequence
    // was converted with a specific output type, like array([], type=t)
    if (chunks_.size() == 0 || builder_->length() > 0) {
      std::shared_ptr<Array> last_chunk;
      RETURN_NOT_OK(builder_->Finish(&last_chunk));
      chunks->emplace_back(std::move(last_chunk));
    }
    return Status::OK();
  }

  ArrayBuilder* builder() const { return builder_; }

protected:
  ArrayBuilder* builder_;
  std::vector<std::shared_ptr<Array>> chunks_;

};

template <typename Type, typename Enable = void>
struct Unbox {};

// unboxer for int type
template <typename Type>
struct Unbox<Type, enable_if_integer<Type>>  {
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using CType = typename TypeTraits<Type>::CType;

  static inline Status Ingest(BuilderType* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    case INTSXP:
      return IngestRange<int>(builder, INTEGER(obj), XLENGTH(obj), NA_INTEGER);
    case REALSXP:
      if (Rf_inherits(obj, "integer64")) {
        return IngestRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)), XLENGTH(obj), NA_INT64);
      }
    // TODO: handle aw and logical
    default:
      break;
    }

    // TODO: include more information about the R object and the target type
    return Status::Invalid("Cannot convert R object to integer type");
  }

  template <typename T>
  static inline Status IngestRange(BuilderType* builder, T* p, R_xlen_t n, T na) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(*p == na) {
        builder->UnsafeAppendNull();
      } else {
        CType value;
        RETURN_NOT_OK(internal::int_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }
};

template<>
struct Unbox<DoubleType> {

  static inline Status Ingest(DoubleBuilder* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    // TODO: handle RAW
    case INTSXP:
    return IngestIntRange<int>(builder, INTEGER(obj), XLENGTH(obj), NA_INTEGER);
    case REALSXP:
      if(Rf_inherits(obj, "integer64")) {
        return IngestIntRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)), XLENGTH(obj), NA_INT64);
      }
      return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
    }
    return Status::Invalid("Cannot convert R object to double type");
  }

  template <typename T>
  static inline Status IngestIntRange(DoubleBuilder* builder, T* p, R_xlen_t n, T na) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        double value;
        RETURN_NOT_OK(internal::double_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(DoubleBuilder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p);
      }
    }
    return Status::OK();
  }

};

template<>
struct Unbox<FloatType> {

  static inline Status Ingest(FloatBuilder* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    // TODO: handle RAW
    case INTSXP:
      return IngestIntRange<int>(builder, INTEGER(obj), XLENGTH(obj), NA_INTEGER);
    case REALSXP:
      if(Rf_inherits(obj, "integer64")) {
        return IngestIntRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)), XLENGTH(obj), NA_INT64);
      }
      return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
    }
    return Status::Invalid("Cannot convert R object to double type");
  }

  template <typename T>
  static inline Status IngestIntRange(FloatBuilder* builder, T* p, R_xlen_t n, T na) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        float value;
        RETURN_NOT_OK(internal::float_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(FloatBuilder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        float value;
        RETURN_NOT_OK(internal::float_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }

};

template <>
struct Unbox<BooleanType> {

  static inline Status Ingest(BooleanBuilder* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    case LGLSXP:
    {
      R_xlen_t n = XLENGTH(obj);
      RETURN_NOT_OK(builder->Resize(n));
      int* p = LOGICAL(obj);
      for (R_xlen_t i=0; i<n; i++, ++p) {
        if(*p == NA_LOGICAL) {
          builder->UnsafeAppendNull();
        } else {
          builder->UnsafeAppend(*p == 1);
        }
      }
      return Status::OK();
    }

    default: break;
    }

    // TODO: include more information about the R object and the target type
    return Status::Invalid("Cannot convert R object to boolean type");
  }

};

template <>
struct Unbox<Date32Type> {

  static inline Status Ingest(Date32Builder* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    case INTSXP:
      if (Rf_inherits(obj, "Date")) {
        return IngestIntRange(builder, INTEGER(obj), XLENGTH(obj));
      }
      break;
    case REALSXP:
      if (Rf_inherits(obj, "Date")) {
        return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
      }
      break;
    default:
      break;
    }
    return Status::Invalid("Cannot convert R object to date32 type");
  }

  static inline Status IngestIntRange(Date32Builder* builder, int* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(Date32Builder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(static_cast<int>(*p));
      }
    }
    return Status::OK();
  }

};

template <>
struct Unbox<Date64Type> {

  constexpr static int64_t kMillisecondsPerDay = 86400000;

  static inline Status Ingest(Date64Builder* builder, SEXP obj) {
    switch(TYPEOF(obj)) {
    case INTSXP:
      // number of days since epoch
      if (Rf_inherits(obj, "Date")) {
        return IngestDateInt32Range(builder, INTEGER(obj), XLENGTH(obj));
      }
      break;

    case REALSXP:
      // (fractional number of days since epoch)
      if (Rf_inherits(obj, "Date")) {
        return IngestDateDoubleRange<kMillisecondsPerDay>(builder, REAL(obj), XLENGTH(obj));
      }

      // number of seconds since epoch
      if (Rf_inherits(obj, "POSIXct")) {
        return IngestDateDoubleRange<1000>(builder, REAL(obj), XLENGTH(obj));
      }
    }
    return Status::Invalid("Cannot convert R object to date64 type");
  }

  // ingest a integer vector that represents number of days since epoch
  static inline Status IngestDateInt32Range(Date64Builder* builder, int* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p * kMillisecondsPerDay);
      }
    }
    return Status::OK();
  }

  // ingest a numeric vector that represents (fractional) number of days since epoch
  template <int64_t MULTIPLIER>
  static inline Status IngestDateDoubleRange(Date64Builder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));

    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(static_cast<int64_t>(*p * MULTIPLIER));
      }
    }
    return Status::OK();
  }

};

template <typename Type, class Derived>
class TypedVectorConverter : public VectorConverter {
public:
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = checked_cast<BuilderType*>(builder_);
    return Status::OK();
  }

  Status Ingest(SEXP obj) override {
    return Unbox<Type>::Ingest(typed_builder_, obj);
  }

protected:
  BuilderType* typed_builder_;
};

template <typename Type>
class NumericVectorConverter : public TypedVectorConverter<Type, NumericVectorConverter<Type>>{};

class BooleanVectorConverter : public TypedVectorConverter<BooleanType, BooleanVectorConverter>{};

class Date32Converter : public TypedVectorConverter<Date32Type, Date32Converter> {};
class Date64Converter : public TypedVectorConverter<Date64Type, Date64Converter> {};

inline int64_t get_time_multiplier(TimeUnit::type unit){
  switch(unit){
  case TimeUnit::SECOND: return 1;
  case TimeUnit::MILLI: return 1000;
  case TimeUnit::MICRO: return 1000000;
  case TimeUnit::NANO: return 1000000000;
  }
}

template <typename Type>
class TimeConverter : public VectorConverter {
  using BuilderType = typename TypeTraits<Type>::BuilderType;

public:
  TimeConverter(TimeUnit::type unit) : unit_(unit), multiplier_(get_time_multiplier(unit)){}

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = checked_cast<BuilderType*>(builder);
    return Status::OK();
  }

  Status Ingest(SEXP obj) override {

    if(valid_R_object(obj)) {
      return Ingest_POSIXct(REAL(obj), XLENGTH(obj));
    }

    return Status::Invalid("Cannot convert R object to timestamp type");
  }

protected:
  TimeUnit::type unit_;
  BuilderType* typed_builder_;
  int64_t multiplier_;

  Status Ingest_POSIXct(double* p, R_xlen_t n) {
    RETURN_NOT_OK(typed_builder_->Resize(n));

    for (R_xlen_t i=0; i<n; i++, ++p) {
      if(ISNA(*p)) {
        typed_builder_->UnsafeAppendNull();
      } else {
        typed_builder_->UnsafeAppend(static_cast<int64_t>(*p * multiplier_));
      }
    }
    return Status::OK();
  }

  virtual bool valid_R_object(SEXP obj)  = 0 ;

};

class TimestampConverter : public TimeConverter<TimestampType> {
public:
  TimestampConverter(TimeUnit::type unit) : TimeConverter<TimestampType>(unit){}

protected:
  virtual bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "POSIXct");
  }
};

class Time32Converter : public TimeConverter<Time32Type> {
public:
  Time32Converter(TimeUnit::type unit) : TimeConverter<Time32Type>(unit){}

protected:
  virtual bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "difftime");
  }
};

class Time64Converter : public TimeConverter<Time64Type> {
public:
  Time64Converter(TimeUnit::type unit) : TimeConverter<Time64Type>(unit){}

protected:
  virtual bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "difftime");
  }
};


#define NUMERIC_CONVERTER(TYPE_ENUM, TYPE)                     \
case Type::TYPE_ENUM:                                                \
  *out = std::unique_ptr<NumericVectorConverter<TYPE>>(new NumericVectorConverter<TYPE>); \
  return Status::OK()

#define SIMPLE_CONVERTER_CASE(TYPE_ENUM, TYPE)                      \
case Type::TYPE_ENUM:                                               \
  *out = std::unique_ptr<TYPE>(new TYPE);                      \
  return Status::OK()

#define TIME_CONVERTER_CASE(TYPE_ENUM, DATA_TYPE, TYPE)                      \
case Type::TYPE_ENUM:                                               \
  *out = std::unique_ptr<TYPE>(new TYPE(checked_cast<DATA_TYPE*>(type.get())->unit()));                           \
  return Status::OK()

Status GetConverter(const std::shared_ptr<DataType>& type, std::unique_ptr<VectorConverter>* out) {

  switch(type->id()){
  SIMPLE_CONVERTER_CASE(BOOL, BooleanVectorConverter);
  NUMERIC_CONVERTER(INT8  , Int8Type);
  NUMERIC_CONVERTER(INT16 , Int16Type);
  NUMERIC_CONVERTER(INT32 , Int32Type);
  NUMERIC_CONVERTER(INT64 , Int64Type);
  NUMERIC_CONVERTER(UINT8 , UInt8Type);
  NUMERIC_CONVERTER(UINT16, UInt16Type);
  NUMERIC_CONVERTER(UINT32, UInt32Type);
  NUMERIC_CONVERTER(UINT64, UInt64Type);

  // TODO: not sure how to handle half floats
  //       the python code uses npy_half
  // NUMERIC_CONVERTER(HALF_FLOAT, HalfFloatType);
  NUMERIC_CONVERTER(FLOAT, FloatType);
  NUMERIC_CONVERTER(DOUBLE, DoubleType);

  SIMPLE_CONVERTER_CASE(DATE32, Date32Converter);
  SIMPLE_CONVERTER_CASE(DATE64, Date64Converter);

  case Type::DECIMAL:
  case Type::DICTIONARY:

  TIME_CONVERTER_CASE(TIME32, Time32Type, Time32Converter);
  TIME_CONVERTER_CASE(TIME64, Time64Type, Time64Converter);
  TIME_CONVERTER_CASE(TIMESTAMP, TimestampType, TimestampConverter);

  default:
    break;
  }
  return Status::NotImplemented("type not implemented");
}


template <typename Type>
std::shared_ptr<arrow::DataType> GetFactorTypeImpl(Rcpp::IntegerVector_ factor) {
  auto dict_values = MakeStringArray(Rf_getAttrib(factor, R_LevelsSymbol));
  auto dict_type =
    dictionary(std::make_shared<Type>(), dict_values, Rf_inherits(factor, "ordered"));
  return dict_type;
}

std::shared_ptr<arrow::DataType> GetFactorType(SEXP factor) {
  SEXP levels = Rf_getAttrib(factor, R_LevelsSymbol);
  int n = Rf_length(levels);
  if (n < 128) {
    return GetFactorTypeImpl<arrow::Int8Type>(factor);
  } else if (n < 32768) {
    return GetFactorTypeImpl<arrow::Int16Type>(factor);
  } else {
    return GetFactorTypeImpl<arrow::Int32Type>(factor);
  }
}

std::shared_ptr<arrow::DataType> InferType(SEXP x) {
  switch (TYPEOF(x)) {
  case LGLSXP:
    return boolean();
  case INTSXP:
    if (Rf_isFactor(x)) {
      return GetFactorType(x);
    }
    if (Rf_inherits(x, "Date")) {
      return date32();
    }
    if (Rf_inherits(x, "POSIXct")) {
      return timestamp(TimeUnit::MICRO, "GMT");
    }
    return int32();
  case REALSXP:
    if (Rf_inherits(x, "Date")) {
      return date32();
    }
    if (Rf_inherits(x, "POSIXct")) {
      return timestamp(TimeUnit::MICRO, "GMT");
    }
    if (Rf_inherits(x, "integer64")) {
      return int64();
    }
    if (Rf_inherits(x, "difftime")) {
      return time32(TimeUnit::SECOND);
    }
    return float64();
  case RAWSXP:
    return int8();
  case STRSXP:
    return utf8();
  default:
    break;
  }

  Rcpp::stop("cannot infer type from data");
}

// in some situations we can just use the memory of the R object in an RBuffer
// instead of going through ArrayBuilder, etc ...
bool can_reuse_memory(SEXP x, const std::shared_ptr<arrow::DataType>& type) {
  switch(type->id()) {
  case Type::INT32: return TYPEOF(x) == INTSXP && !OBJECT(x);
  case Type::DOUBLE: return TYPEOF(x) == REALSXP && !OBJECT(x);
  case Type::INT8: return TYPEOF(x) == RAWSXP && !OBJECT(x);
  case Type::INT64: return TYPEOF(x) == REALSXP && Rf_inherits(x, "integer64");
  default:
    break;
  }
  return false;
}

template <typename T>
inline bool is_na(T value) {
  return false;
}

template <>
inline bool is_na<int64_t>(int64_t value){
  return value == NA_INT64;
}

template <>
inline bool is_na<double>(double value){
  return ISNA(value);
}

template <>
inline bool is_na<int>(int value){
  return value == NA_INTEGER;
}
// this is only used on some special cases when the arrow Array can just use the memory of the R
// object, via an RBuffer, hence be zero copy
template <int RTYPE, typename Type>
std::shared_ptr<Array> MakeSimpleArray(SEXP x) {
  using value_type = typename arrow::TypeTraits<Type>::CType;
  Rcpp::Vector<RTYPE, NoProtectStorage> vec(x);
  auto n = vec.size();
  auto p_vec_start = reinterpret_cast<value_type*>(vec.begin());
  auto p_vec_end = p_vec_start + n;
  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
    std::make_shared<RBuffer<RTYPE>>(vec)};

  int null_count = 0;
  std::shared_ptr<Buffer> null_bitmap;

  auto first_na = std::find_if(p_vec_start, p_vec_end, is_na<value_type>);
  if (first_na < p_vec_start) {
    STOP_IF_NOT_OK(AllocateBuffer(BitUtil::BytesForBits(n), &null_bitmap));
    internal::FirstTimeBitmapWriter bitmap_writer(null_bitmap->mutable_data(), 0, n);

    // first loop to clear all the bits before the first NA
    auto j = std::distance(p_vec_start, first_na);
    int i = 0;
    for (; i < j; i++, bitmap_writer.Next()) {
      bitmap_writer.Set();
    }

    auto p_vec = first_na;
    // then finish
    for (; i < n; i++, bitmap_writer.Next(), ++p_vec) {
      if (is_na<value_type>(*p_vec)) {
        bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
      }
    }

    bitmap_writer.Finish();
    buffers[0] = std::move(null_bitmap);
  }

  auto data = ArrayData::Make(
    std::make_shared<Type>(), LENGTH(x), std::move(buffers), null_count, 0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<typename TypeTraits<Type>::ArrayType>(data);
}

std::shared_ptr<arrow::Array> Array__from_vector_reuse_memory(SEXP x) {
  switch(TYPEOF(x)) {
  case INTSXP:
    return MakeSimpleArray<INTSXP, Int32Type>(x);
  case REALSXP:
    if (Rf_inherits(x, "integer64")) {
      return MakeSimpleArray<REALSXP, Int64Type>(x);
    }
    return MakeSimpleArray<REALSXP, DoubleType>(x);
  case RAWSXP:
    return MakeSimpleArray<RAWSXP, Int8Type>(x);
  default:
    break;
  }

  Rcpp::stop("not implemented");
}

}  // namespace r
}  // namespace arrow

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Array__infer_type(SEXP x) {
  return arrow::r::InferType(x);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x, SEXP s_type) {

  // the type might be NULL, in which case we need to infer it from the data
  // we keep track of whether it was infered or supplied
  bool type_infered = Rf_isNull(s_type);
  std::shared_ptr<arrow::DataType> type;
  if (type_infered) {
    type = arrow::r::InferType(x);
  } else {
    type = arrow::r::extract<arrow::DataType>(s_type);
  }

  // special case when we can just use the data from the R vector
  // directly. This still needs to handle the null bitmap
  if (arrow::r::can_reuse_memory(x, type)) {
    return arrow::r::Array__from_vector_reuse_memory(x);
  }

  // treat strings separately for now
  if (type->id() == Type::STRING) {
    STOP_IF_NOT(TYPEOF(x) == STRSXP, "Cannot convert R object to string array");
    return arrow::r::MakeStringArray(x);
  }

  // general conversion with converter and builder
  std::unique_ptr<arrow::r::VectorConverter> converter;
  STOP_IF_NOT_OK(arrow::r::GetConverter(type, &converter));

  // Create ArrayBuilder for type
  std::unique_ptr<arrow::ArrayBuilder> type_builder;
  STOP_IF_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), type, &type_builder));
  STOP_IF_NOT_OK(converter->Init(type_builder.get()));

  // ingest x
  STOP_IF_NOT_OK(converter->Ingest(x));

  std::vector<std::shared_ptr<arrow::Array>> chunks;
  STOP_IF_NOT_OK(converter->GetResult(&chunks));

  return chunks[0];
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__from_vector_old(SEXP x, const std::shared_ptr<arrow::DataType>& type) {
  switch (TYPEOF(x)) {
  case LGLSXP:
    return arrow::r::MakeBooleanArray(x);
  case INTSXP:
    if (Rf_isFactor(x)) {
      return arrow::r::MakeFactorArray(x);
    }
    if (Rf_inherits(x, "Date")) {
      return arrow::r::SimpleArray<INTSXP, arrow::Date32Type>(x);
    }
    if (Rf_inherits(x, "POSIXct")) {
      return arrow::r::TimeStampArray_From_POSIXct<INTSXP>(x);
    }
    return arrow::r::SimpleArray<INTSXP, arrow::Int32Type>(x);
  case REALSXP:
    if (Rf_inherits(x, "Date")) {
      return arrow::r::SimpleArray<INTSXP, arrow::Date32Type>(x);
    }
    if (Rf_inherits(x, "POSIXct")) {
      return arrow::r::TimeStampArray_From_POSIXct<REALSXP>(x);
    }
    if (Rf_inherits(x, "integer64")) {
      return arrow::r::Int64Array(x);
    }
    if (Rf_inherits(x, "difftime")) {
      return arrow::r::Time32Array_From_difftime(x);
    }
    return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
  case RAWSXP:
    return arrow::r::SimpleArray<RAWSXP, arrow::Int8Type>(x);
  case STRSXP:
    return arrow::r::MakeStringArray(x);
  default:
    break;
  }

  stop("not handled");
  return nullptr;
}
