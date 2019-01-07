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

template <typename Converter, typename... Args>
SEXP ArrayVector_To_Vector(int64_t n, const ArrayVector& arrays, Args... args) {
  Shield<SEXP> data(Converter::Allocate(n, std::forward<Args>(args)...));

  R_xlen_t k = 0;
  for (const auto& array : arrays) {
    auto n_chunk = array->length();
    STOP_IF_NOT_OK(Converter::Ingest(data, array, k, n_chunk));
    k += n_chunk;
  }

  return data;
}

template <int RTYPE>
struct Converter_SimpleArray {
  using Vector = Rcpp::Vector<RTYPE, Rcpp::NoProtectStorage>;

  static SEXP Allocate(R_xlen_t n) { return Vector(no_init(n)); }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    Vector data(data_);
    using value_type = typename Vector::stored_type;
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, default_value<RTYPE>());
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      if (!p_values) {
        return Status::Invalid("Invalid data buffer");
      }

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
    return Status::OK();
  }
};

struct Converter_Date32 {
  static SEXP Allocate(R_xlen_t n) {
    IntegerVector data(no_init(n));
    data.attr("class") = "Date";
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    return Converter_SimpleArray<INTSXP>::Ingest(data_, array, start, n);
  }
};

struct Converter_String {
  static SEXP Allocate(R_xlen_t n) { return StringVector_(no_init(n)); }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    StringVector_ data(data_);
    auto null_count = array->null_count();

    if (null_count == n) {
      std::fill_n(data.begin(), n, NA_STRING);
    } else {
      auto p_offset = array->data()->GetValues<int32_t>(1);
      if (!p_offset) {
        return Status::Invalid("Invalid offset buffer");
      }
      auto p_data = array->data()->GetValues<char>(2, *p_offset);
      if (!p_data) {
        // There is an offset buffer, but the data buffer is null
        // There is at least one value in the array and not all the values are null
        // That means all values are empty strings so there is nothing to do
        return Status::OK();
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
    return Status::OK();
  }
};

struct Converter_Boolean {
  static SEXP Allocate(R_xlen_t n) { return LogicalVector_(no_init(n)); }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    LogicalVector_ data(data_);
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_LOGICAL);
    } else {
      // process the data
      auto p_data = array->data()->GetValues<uint8_t>(1, 0);
      if (!p_data) {
        return Status::Invalid("Invalid data buffer");
      }

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
    return Status::OK();
  }
};

struct Converter_Dictionary {
  static SEXP Allocate(R_xlen_t n, const ArrayVector& arrays) {
    IntegerVector data(no_init(n));
    auto dict_array = static_cast<DictionaryArray*>(arrays[0].get());
    auto dict = dict_array->dictionary();
    auto indices = dict_array->indices();
    switch (indices->type_id()) {
      case Type::UINT8:
      case Type::INT8:
      case Type::UINT16:
      case Type::INT16:
      case Type::INT32:
        break;
      default:
        stop("Cannot convert Dictionary Array of type `%s` to R",
             dict_array->type()->ToString());
    }

    if (dict->type_id() != Type::STRING) {
      stop("Cannot convert Dictionary Array of type `%s` to R",
           dict_array->type()->ToString());
    }
    bool ordered = dict_array->dict_type()->ordered();

    data.attr("levels") = ArrayVector_To_Vector<Converter_String>(dict->length(), {dict});
    if (ordered) {
      data.attr("class") = CharacterVector::create("ordered", "factor");
    } else {
      data.attr("class") = "factor";
    }
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    DictionaryArray* dict_array = static_cast<DictionaryArray*>(array.get());
    auto indices = dict_array->indices();
    switch (indices->type_id()) {
      case Type::UINT8:
        return Ingest_Impl<arrow::UInt8Type>(data_, array, start, n);
      case Type::INT8:
        return Ingest_Impl<arrow::Int8Type>(data_, array, start, n);
      case Type::UINT16:
        return Ingest_Impl<arrow::UInt16Type>(data_, array, start, n);
      case Type::INT16:
        return Ingest_Impl<arrow::Int16Type>(data_, array, start, n);
      case Type::INT32:
        return Ingest_Impl<arrow::Int32Type>(data_, array, start, n);
      default:
        break;
    }
    return Status::OK();
  }

  template <typename Type>
  static Status Ingest_Impl(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                            R_xlen_t start, R_xlen_t n) {
    IntegerVector_ data(data_);

    DictionaryArray* dict_array = static_cast<DictionaryArray*>(array.get());
    using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_INTEGER);
    } else {
      std::shared_ptr<Array> indices = dict_array->indices();
      auto p_array = indices->data()->GetValues<value_type>(1);
      if (!p_array) {
        return Status::Invalid("invalid data buffer");
      }
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
    return Status::OK();
  }
};

struct Converter_Date64 {
  static SEXP Allocate(R_xlen_t n) {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    NumericVector_ data(data_);
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
    return Status::OK();
  }
};

template <int RTYPE, typename Type>
struct Converter_Promotion {
  using r_stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

  static SEXP Allocate(R_xlen_t n) { return Rcpp::Vector<RTYPE>(no_init(n)); }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    Rcpp::Vector<RTYPE, NoProtectStorage> data(data_);
    auto null_count = array->null_count();
    if (null_count == n) {
      std::fill_n(data.begin() + start, n, default_value<RTYPE>());
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      if (!p_values) {
        return Status::Invalid("Invalid values buffer");
      }

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
    return Status::OK();
  }
};

template <typename value_type, int32_t multiplier>
struct Converter_Time {
  static SEXP Allocate(R_xlen_t n) {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("hms", "difftime");
    data.attr("units") = CharacterVector::create("secs");
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    NumericVector_ data(data_);

    auto null_count = array->null_count();
    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_REAL);
    } else {
      auto p_values = array->data()->GetValues<value_type>(1);
      if (!p_values) {
        return Status::Invalid("Invalid data buffer");
      }

      auto p_vec = data.begin() + start;
      auto convert = [](value_type value) {
        return static_cast<double>(value) / multiplier;
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
    return Status::OK();
  }
};

template <typename value_type, int32_t multiplier>
struct Converter_Timestamp {
  static SEXP Allocate(R_xlen_t n) {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
    R_xlen_t start, R_xlen_t n) {
    return Converter_Time<value_type, multiplier>::Ingest(data_, array, start, n);
  }
};

struct Converter_Int64 {
  static SEXP Allocate(R_xlen_t n) {
    NumericVector data(no_init(n));
    data.attr("class") = "integer64";
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    NumericVector_ data(data_);
    auto null_count = array->null_count();
    if (null_count == n) {
      std::fill_n(reinterpret_cast<int64_t*>(data.begin()) + start, n, NA_INT64);
    } else {
      auto p_values = array->data()->GetValues<int64_t>(1);
      if (!p_values) {
        return Status::Invalid("Invalid data buffer");
      }

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
    return Status::OK();
  }
};

struct Converter_Decimal {
  static SEXP Allocate(R_xlen_t n) { return NumericVector_(no_init(n)); }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    NumericVector_ data(data_);
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
    return Status::OK();
  }
};

// Most converter can ingest in parallel
template <typename Converter>
constexpr bool parallel_ingest() {
  return true;
}

// but not the string converter
template <>
constexpr bool parallel_ingest<Converter_String>() {
  return false;
}

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
      return ArrayVector_To_Vector<Converter_Dictionary>(n, arrays, arrays);

    case Type::DATE32:
      return ArrayVector_To_Vector<Converter_Date32>(n, arrays);
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
    case Type::TIME32: {
      if (static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::SECOND) {
        return ArrayVector_To_Vector<Converter_Time<int32_t, 1>>(n, arrays);
      } else {
        return ArrayVector_To_Vector<Converter_Time<int32_t, 1000>>(n, arrays);
      }
    }

    case Type::TIME64: {
      if (static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::MICRO) {
        return ArrayVector_To_Vector<Converter_Time<int64_t, 1000000>>(n, arrays);
      } else {
        return ArrayVector_To_Vector<Converter_Time<int64_t, 1000000000>>(n, arrays);
      }
    }

    case Type::TIMESTAMP: {
      if (static_cast<TimeType*>(arrays[0]->type().get())->unit() == TimeUnit::MICRO) {
        return ArrayVector_To_Vector<Converter_Timestamp<int64_t, 1000000>>(n, arrays);
      } else {
        return ArrayVector_To_Vector<Converter_Timestamp<int64_t, 1000000000>>(
            n, arrays);
      }
    }

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
List RecordBatch__to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  auto nr = batch->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for (int i = 0; i < nc; i++) {
    tbl[i] = Array__as_vector(batch->column(i));
    names[i] = batch->column_name(i);
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}
