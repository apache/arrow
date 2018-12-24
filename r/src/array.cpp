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

// the integer64 sentinel
constexpr int64_t NA_INT64 = std::numeric_limits<int64_t>::min();

template <int RTYPE, typename Type>
std::shared_ptr<Array> SimpleArray(SEXP x) {
  Rcpp::Vector<RTYPE> vec(x);
  auto n = vec.size();
  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
                                               std::make_shared<RBuffer<RTYPE>>(vec)};

  int null_count = 0;
  if (RTYPE != RAWSXP) {
    std::shared_ptr<Buffer> null_bitmap;

    auto first_na = std::find_if(vec.begin(), vec.end(), Rcpp::Vector<RTYPE>::is_na);
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

}  // namespace r
}  // namespace arrow

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x) {
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

// ---------------------------- Array -> R vector

namespace arrow {
namespace r {

template <typename Converter, typename... Args>
SEXP ArrayVector_To_Vector(int64_t n, const ArrayVector& arrays, Args... args) {
  Converter converter(n, std::forward<Args>(args)...);

  R_xlen_t k = 0;
  for (const auto& array : arrays) {
    auto n_chunk = array->length();
    converter.Ingest(array, k, n_chunk);
    k += n_chunk;
  }
  return converter.data;
}

template <int RTYPE>
struct Converter_SimpleArray {
  using Vector = Rcpp::Vector<RTYPE>;

  Converter_SimpleArray(R_xlen_t n) : data(no_init(n)) {}

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    using value_type = typename Vector::stored_type;
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, default_value<RTYPE>());
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      STOP_IF_NULL(p_values);

      // first copy all the data
      std::copy_n(p_values, n, data.begin() + start);

      if (null_count) {
        // then set the sentinel NA
        arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                                    array->offset(), n);

        for (size_t i = 0; i < n; i++, bitmap_reader.Next()) {
          if (bitmap_reader.IsNotSet()) {
            data[i + start] = default_value<RTYPE>();
          }
        }
      }
    }
  }

  Vector data;
};

struct Converter_String {
  Converter_String(R_xlen_t n) : data(n) {}

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();

    if (null_count == n) {
      std::fill_n(data.begin(), n, NA_STRING);
    } else {
      auto p_offset = array->data()->GetValues<int32_t>(1);
      STOP_IF_NULL(p_offset);
      auto p_data = array->data()->GetValues<char>(2, *p_offset);
      if (!p_data) {
        // There is an offset buffer, but the data buffer is null
        // There is at least one value in the array and not all the values are null
        // That means all values are empty strings so there is nothing to do
        return;
      }

      if (null_count) {
        // need to watch for nulls
        arrow::internal::BitmapReader null_reader(array->null_bitmap_data(),
                                                  array->offset(), n);
        for (int i = 0; i < n; i++, null_reader.Next()) {
          if (null_reader.IsSet()) {
            auto diff = p_offset[i + 1] - p_offset[i];
            SET_STRING_ELT(data, start + i, Rf_mkCharLenCE(p_data, diff, CE_UTF8));
            p_data += diff;
          } else {
            SET_STRING_ELT(data, start + i, NA_STRING);
          }
        }

      } else {
        // no need to check for nulls
        // TODO: altrep mark this as no na
        for (int i = 0; i < n; i++) {
          auto diff = p_offset[i + 1] - p_offset[i];
          SET_STRING_ELT(data, start + i, Rf_mkCharLenCE(p_data, diff, CE_UTF8));
          p_data += diff;
        }
      }
    }
  }

  CharacterVector data;
};

struct Converter_Boolean {
  Converter_Boolean(R_xlen_t n) : data(n) {}

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_LOGICAL);
    } else {
      // process the data
      auto p_data = array->data()->GetValues<uint8_t>(1, 0);
      STOP_IF_NULL(p_data);

      arrow::internal::BitmapReader data_reader(p_data, array->offset(), n);
      for (size_t i = 0; i < n; i++, data_reader.Next()) {
        data[start + i] = data_reader.IsSet();
      }

      // then the null bitmap if needed
      if (null_count) {
        arrow::internal::BitmapReader null_reader(array->null_bitmap()->data(),
                                                  array->offset(), n);
        for (size_t i = 0; i < n; i++, null_reader.Next()) {
          if (null_reader.IsNotSet()) {
            data[start + i] = NA_LOGICAL;
          }
        }
      }
    }
  }

  LogicalVector data;
};

template <typename Type>
struct Converter_Dictionary_Int32Indices {
  Converter_Dictionary_Int32Indices(R_xlen_t n, const std::shared_ptr<arrow::Array>& dict,
                                    bool ordered)
      : data(no_init(n)) {
    data.attr("levels") = ArrayVector_To_Vector<Converter_String>(dict->length(), {dict});
    if (ordered) {
      data.attr("class") = CharacterVector::create("ordered", "factor");
    } else {
      data.attr("class") = "factor";
    }
  }

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    DictionaryArray* dict_array = static_cast<DictionaryArray*>(array.get());
    using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_INTEGER);
    } else {
      std::shared_ptr<Array> indices = dict_array->indices();
      auto p_array = indices->data()->GetValues<value_type>(1);
      STOP_IF_NULL(p_array);

      if (array->null_count()) {
        arrow::internal::BitmapReader bitmap_reader(indices->null_bitmap()->data(),
                                                    indices->offset(), n);
        for (size_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_array) {
          data[start + i] =
              bitmap_reader.IsNotSet() ? NA_INTEGER : (static_cast<int>(*p_array) + 1);
        }
      } else {
        std::transform(
            p_array, p_array + n, data.begin() + start,
            [](const value_type value) { return static_cast<int>(value) + 1; });
      }
    }
  }

  IntegerVector data;
};

struct Converter_Date64 {
  Converter_Date64(R_xlen_t n) : data(n) {
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
  }

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();
    if (null_count == n) {
      std::fill_n(data.begin() + start, n, NA_REAL);
    } else {
      auto p_values = array->data()->GetValues<int64_t>(1);
      STOP_IF_NULL(p_values);
      auto p_vec = data.begin() + start;

      // convert DATE64 milliseconds to R seconds (stored as double)
      auto seconds = [](int64_t ms) { return static_cast<double>(ms / 1000); };

      if (null_count) {
        arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                                    array->offset(), n);
        for (size_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_vec, ++p_values) {
          *p_vec = bitmap_reader.IsSet() ? seconds(*p_values) : NA_REAL;
        }
      } else {
        std::transform(p_values, p_values + n, p_vec, seconds);
      }
    }
  }

  NumericVector data;
};

template <int RTYPE, typename Type>
struct Converter_Promotion {
  using r_stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

  Converter_Promotion(R_xlen_t n) : data(no_init(n)) {}

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();
    if (null_count == n) {
      std::fill_n(data.begin() + start, n, default_value<RTYPE>());
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      STOP_IF_NULL(p_values);

      auto value_convert = [](value_type value) {
        return static_cast<r_stored_type>(value);
      };
      if (null_count) {
        internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                             array->offset(), n);
        for (size_t i = 0; i < n; i++, bitmap_reader.Next()) {
          data[start + i] = bitmap_reader.IsNotSet() ? Rcpp::Vector<RTYPE>::get_na()
                                                     : value_convert(p_values[i]);
        }
      } else {
        std::transform(p_values, p_values + n, data.begin(), value_convert);
      }
    }
  }

  Rcpp::Vector<RTYPE> data;
};

template <typename value_type>
struct Converter_Time {
  Converter_Time(int64_t n, int32_t multiplier, CharacterVector classes)
      : data(no_init(n)), multiplier_(multiplier) {
    data.attr("class") = classes;
  }

  Converter_Time(int64_t n, int32_t multiplier)
      : data(no_init(n)), multiplier_(multiplier) {
    data.attr("class") = CharacterVector::create("hms", "difftime");
    data.attr("units") = "secs";
  }

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();
    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_REAL);
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      STOP_IF_NULL(p_values);
      auto p_vec = data.begin() + start;
      auto convert = [this](value_type value) {
        return static_cast<double>(value) / multiplier_;
      };
      if (null_count) {
        arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                                    array->offset(), n);
        for (size_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_vec, ++p_values) {
          *p_vec = bitmap_reader.IsSet() ? convert(*p_values) : NA_REAL;
        }
      } else {
        std::transform(p_values, p_values + n, p_vec, convert);
      }
    }
  }

  NumericVector data;
  int32_t multiplier_;
};

template <typename value_type>
struct Converter_TimeStamp : Converter_Time<value_type> {
  Converter_TimeStamp(int64_t n, int32_t multiplier)
      : Converter_Time<value_type>(n, multiplier,
                                   CharacterVector::create("POSIXct", "POSIXt")) {}
};

struct Converter_Int64 {
  Converter_Int64(R_xlen_t n) : data(no_init(n)) { data.attr("class") = "integer64"; }

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();
    if (null_count == n) {
      std::fill_n(reinterpret_cast<int64_t*>(data.begin()) + start, n, NA_INT64);
    } else {
      auto p_values = array->data()->GetValues<int64_t>(1);
      STOP_IF_NULL(p_values);
      auto p_vec = reinterpret_cast<int64_t*>(data.begin()) + start;

      if (array->null_count()) {
        internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                             array->offset(), n);
        for (size_t i = 0; i < n; i++, bitmap_reader.Next()) {
          p_vec[i] = bitmap_reader.IsNotSet() ? NA_INT64 : p_values[i];
        }
      } else {
        std::copy_n(p_values, n, p_vec);
      }
    }
  }

  NumericVector data;
};

SEXP DictionaryArrays_to_Vector(int64_t n, const ArrayVector& arrays) {
  DictionaryArray* dict_array = static_cast<DictionaryArray*>(arrays[0].get());
  auto dict = dict_array->dictionary();
  auto indices = dict_array->indices();

  if (dict->type_id() != Type::STRING) {
    stop("Cannot convert Dictionary Array of type `%s` to R",
         dict_array->type()->ToString());
  }
  bool ordered = dict_array->dict_type()->ordered();
  switch (indices->type_id()) {
    case Type::UINT8:
      return ArrayVector_To_Vector<Converter_Dictionary_Int32Indices<arrow::UInt8Type>>(
          n, arrays, dict, ordered);

    case Type::INT8:
      return ArrayVector_To_Vector<Converter_Dictionary_Int32Indices<arrow::Int8Type>>(
          n, arrays, dict, ordered);

    case Type::UINT16:
      return ArrayVector_To_Vector<Converter_Dictionary_Int32Indices<arrow::UInt16Type>>(
          n, arrays, dict, ordered);

    case Type::INT16:
      return ArrayVector_To_Vector<Converter_Dictionary_Int32Indices<arrow::Int16Type>>(
          n, arrays, dict, ordered);

    case Type::INT32:
      return ArrayVector_To_Vector<Converter_Dictionary_Int32Indices<arrow::Int32Type>>(
          n, arrays, dict, ordered);

    default:
      stop("Cannot convert Dictionary Array of type `%s` to R",
           dict_array->type()->ToString());
  }
  return R_NilValue;
}

SEXP Date32ArrayVector_to_Vector(int64_t n, const ArrayVector& arrays) {
  IntegerVector out(
      arrow::r::ArrayVector_To_Vector<Converter_SimpleArray<INTSXP>>(n, arrays));
  out.attr("class") = "Date";
  return out;
}

struct Converter_Decimal {
  Converter_Decimal(R_xlen_t n) : data(no_init(n)) {}

  void Ingest(const std::shared_ptr<arrow::Array>& array, R_xlen_t start, R_xlen_t n) {
    auto null_count = array->null_count();
    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_REAL);
    } else {
      auto p_vec = reinterpret_cast<double*>(data.begin()) + start;
      const auto& decimals_arr =
          internal::checked_cast<const arrow::Decimal128Array&>(*array);

      if (array->null_count()) {
        internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                             array->offset(), n);

        for (size_t i = 0; i < n; i++, bitmap_reader.Next()) {
          p_vec[i] = bitmap_reader.IsNotSet()
                         ? NA_REAL
                         : std::stod(decimals_arr.FormatValue(i).c_str());
        }
      } else {
        for (size_t i = 0; i < n; i++) {
          p_vec[i] = std::stod(decimals_arr.FormatValue(i).c_str());
        }
      }
    }
  }

  NumericVector data;
};

}  // namespace r
}  // namespace arrow

SEXP ArrayVector__as_vector(int64_t n, const ArrayVector& arrays) {
  using namespace arrow::r;

  switch (arrays[0]->type_id()) {
    // direct support
    case Type::INT8:
      return ArrayVector_To_Vector<Converter_SimpleArray<RAWSXP>>(n, arrays);
    case Type::INT32:
      return ArrayVector_To_Vector<Converter_SimpleArray<INTSXP>>(n, arrays);
    case Type::DOUBLE:
      return ArrayVector_To_Vector<Converter_SimpleArray<REALSXP>>(n, arrays);

    // need to handle 1-bit case
    case Type::BOOL:
      return ArrayVector_To_Vector<Converter_Boolean>(n, arrays);

      // handle memory dense strings
    case Type::STRING:
      return ArrayVector_To_Vector<Converter_String>(n, arrays);
    case Type::DICTIONARY:
      return DictionaryArrays_to_Vector(n, arrays);

    case Type::DATE32:
      return Date32ArrayVector_to_Vector(n, arrays);
    case Type::DATE64:
      return ArrayVector_To_Vector<Converter_Date64>(n, arrays);

      // promotions to integer vector
    case Type::UINT8:
      return ArrayVector_To_Vector<Converter_Promotion<INTSXP, arrow::UInt8Type>>(n,
                                                                                  arrays);
    case Type::INT16:
      return ArrayVector_To_Vector<Converter_Promotion<INTSXP, arrow::Int16Type>>(n,
                                                                                  arrays);
    case Type::UINT16:
      return ArrayVector_To_Vector<Converter_Promotion<INTSXP, arrow::UInt16Type>>(
          n, arrays);

      // promotions to numeric vector
    case Type::UINT32:
      return ArrayVector_To_Vector<Converter_Promotion<REALSXP, arrow::UInt32Type>>(
          n, arrays);
    case Type::HALF_FLOAT:
      return ArrayVector_To_Vector<Converter_Promotion<REALSXP, arrow::HalfFloatType>>(
          n, arrays);
    case Type::FLOAT:
      return ArrayVector_To_Vector<Converter_Promotion<REALSXP, arrow::FloatType>>(
          n, arrays);

      // time32 ane time64
    case Type::TIME32:
      return ArrayVector_To_Vector<Converter_Time<int32_t>>(
          n, arrays,
          static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::SECOND
              ? 1
              : 1000);

    case Type::TIME64:
      return ArrayVector_To_Vector<Converter_Time<int64_t>>(
          n, arrays,
          static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::MICRO
              ? 1000000
              : 1000000000);

    case Type::TIMESTAMP:
      return ArrayVector_To_Vector<Converter_TimeStamp<int64_t>>(
          n, arrays,
          static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::MICRO
              ? 1000000
              : 1000000000);

    case Type::INT64:
      return ArrayVector_To_Vector<Converter_Int64>(n, arrays);
    case Type::DECIMAL:
      return ArrayVector_To_Vector<Converter_Decimal>(n, arrays);

    default:
      break;
  }

  stop(tfm::format("cannot handle Array of type %s", arrays[0]->type()->name()));
  return R_NilValue;
}

// [[Rcpp::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  return ArrayVector__as_vector(array->length(), {array});
}

// [[Rcpp::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return ArrayVector__as_vector(chunked_array->length(), chunked_array->chunks());
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice1(const std::shared_ptr<arrow::Array>& array,
                                            int offset) {
  return array->Slice(offset);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> Array__Slice2(const std::shared_ptr<arrow::Array>& array,
                                            int offset, int length) {
  return array->Slice(offset, length);
}

// [[Rcpp::export]]
bool Array__IsNull(const std::shared_ptr<arrow::Array>& x, int i) { return x->IsNull(i); }

// [[Rcpp::export]]
bool Array__IsValid(const std::shared_ptr<arrow::Array>& x, int i) {
  return x->IsValid(i);
}

// [[Rcpp::export]]
int Array__length(const std::shared_ptr<arrow::Array>& x) { return x->length(); }

// [[Rcpp::export]]
int Array__offset(const std::shared_ptr<arrow::Array>& x) { return x->offset(); }

// [[Rcpp::export]]
int Array__null_count(const std::shared_ptr<arrow::Array>& x) { return x->null_count(); }

// [[Rcpp::export]]
std::shared_ptr<arrow::DataType> Array__type(const std::shared_ptr<arrow::Array>& x) {
  return x->type();
}

// [[Rcpp::export]]
std::string Array__ToString(const std::shared_ptr<arrow::Array>& x) {
  return x->ToString();
}

// [[Rcpp::export]]
arrow::Type::type Array__type_id(const std::shared_ptr<arrow::Array>& x) {
  return x->type_id();
}

// [[Rcpp::export]]
bool Array__Equals(const std::shared_ptr<arrow::Array>& lhs,
                   const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->Equals(rhs);
}

// [[Rcpp::export]]
bool Array__ApproxEquals(const std::shared_ptr<arrow::Array>& lhs,
                         const std::shared_ptr<arrow::Array>& rhs) {
  return lhs->ApproxEquals(rhs);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::ArrayData> Array__data(
    const std::shared_ptr<arrow::Array>& array) {
  return array->data();
}

// [[Rcpp::export]]
bool Array__RangeEquals(const std::shared_ptr<arrow::Array>& self,
                        const std::shared_ptr<arrow::Array>& other, int start_idx,
                        int end_idx, int other_start_idx) {
  return self->RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

// [[Rcpp::export]]
LogicalVector Array__Mask(const std::shared_ptr<arrow::Array>& array) {
  if (array->null_count() == 0) {
    return LogicalVector(array->length(), true);
  }

  auto n = array->length();
  LogicalVector res(no_init(n));
  arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                              array->offset(), n);
  for (size_t i = 0; i < array->length(); i++, bitmap_reader.Next()) {
    res[i] = bitmap_reader.IsSet();
  }
  return res;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> DictionaryArray__indices(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->indices();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> DictionaryArray__dictionary(
    const std::shared_ptr<arrow::DictionaryArray>& array) {
  return array->dictionary();
}
