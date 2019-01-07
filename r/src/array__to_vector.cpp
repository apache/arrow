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

#include <arrow/util/parallel.h>
#include <arrow/util/task-group.h>
#include "arrow_types.h"

using namespace Rcpp;
using namespace arrow;

namespace arrow {
namespace r {

template <typename Converter>
SEXP ArrayVector_To_Vector(int64_t n, const ArrayVector& arrays) {
  Shield<SEXP> data(Converter::Allocate(n, arrays));

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

  static SEXP Allocate(R_xlen_t n, const ArrayVector&) { return Vector(no_init(n)); }

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
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
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
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
    return StringVector_(no_init(n));
  }

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
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
    return LogicalVector_(no_init(n));
  }

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

    // convert the 0-based indices from the arrow Array
    // to 1-based indices used in R factors
    auto to_r_index = [](value_type value){ return static_cast<int>(value) + 1;};

    auto null_count = array->null_count();

    if (n == null_count) {
      std::fill_n(data.begin() + start, n, NA_INTEGER);
    } else {
      std::shared_ptr<Array> indices = dict_array->indices();
      auto p_array = indices->data()->GetValues<value_type>(1);
      if (!p_array) {
        return Status::Invalid("invalid data buffer");
      }

      if (null_count) {
        arrow::internal::BitmapReader bitmap_reader(indices->null_bitmap()->data(),
                                                    indices->offset(), n);
        auto p_data = data.begin() + start;
        for (size_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_array, ++p_data) {
          *p_data = bitmap_reader.IsSet() ? to_r_index(*p_array) : NA_INTEGER ;
        }
      } else {
        std::transform(p_array, p_array + n, data.begin() + start, to_r_index);
      }
    }
    return Status::OK();
  }
};

struct Converter_Date64 {
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
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

  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
    return Rcpp::Vector<RTYPE>(no_init(n));
  }

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

static int TimeUnit_multiplier(const std::shared_ptr<Array>& array) {
  switch (static_cast<TimeType*>(array->type().get())->unit()) {
    case TimeUnit::SECOND:
      return 1;
    case TimeUnit::MILLI:
      return 1000;
    case TimeUnit::MICRO:
      return 1000000;
    case TimeUnit::NANO:
      return 1000000000;
  }
}

template <typename value_type>
struct Converter_Time {
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
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
      int multiplier = TimeUnit_multiplier(array);
      auto convert = [=](value_type value) {
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

template <typename value_type>
struct Converter_Timestamp {
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    return Converter_Time<value_type>::Ingest(data_, array, start, n);
  }
};

struct Converter_Int64 {
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
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

      if (null_count) {
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
  static SEXP Allocate(R_xlen_t n, const ArrayVector&) {
    return NumericVector_(no_init(n));
  }

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

      if (null_count) {
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

struct Converter_NotHandled {
  static SEXP Allocate(R_xlen_t n, const ArrayVector& arrays) {
    stop(tfm::format("cannot handle Array of type %s", arrays[0]->type()->name()));
    return R_NilValue;
  }

  static Status Ingest(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                       R_xlen_t start, R_xlen_t n) {
    return Status::RError("Not handled");
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

template <typename What, typename... Args>
auto ArrayVector__Dispatch(const ArrayVector& arrays, Args... args) ->
    typename What::OUT {
  using namespace arrow::r;

  switch (arrays[0]->type_id()) {
    // direct support
    case Type::INT8:
      return What::template Do<Converter_SimpleArray<RAWSXP>>(
          arrays, std::forward<Args>(args)...);

    case Type::INT32:
      return What::template Do<Converter_SimpleArray<INTSXP>>(
          arrays, std::forward<Args>(args)...);

    case Type::DOUBLE:
      return What::template Do<Converter_SimpleArray<REALSXP>>(
          arrays, std::forward<Args>(args)...);

      // need to handle 1-bit case
    case Type::BOOL:
      return What::template Do<Converter_Boolean>(arrays, std::forward<Args>(args)...);

    // handle memory dense strings
    case Type::STRING:
      return What::template Do<Converter_String>(arrays, std::forward<Args>(args)...);

    case Type::DICTIONARY:
      return What::template Do<Converter_Dictionary>(arrays, std::forward<Args>(args)...);

    case Type::DATE32:
      return What::template Do<Converter_Date32>(arrays, std::forward<Args>(args)...);

    case Type::DATE64:
      return What::template Do<Converter_Date64>(arrays, std::forward<Args>(args)...);

      // promotions to integer vector
    case Type::UINT8:
      return What::template Do<Converter_Promotion<INTSXP, arrow::UInt8Type>>(
          arrays, std::forward<Args>(args)...);

    case Type::INT16:
      return What::template Do<Converter_Promotion<INTSXP, arrow::Int16Type>>(
          arrays, std::forward<Args>(args)...);

    case Type::UINT16:
      return What::template Do<Converter_Promotion<INTSXP, arrow::UInt16Type>>(
          arrays, std::forward<Args>(args)...);

      // promotions to numeric vector
    case Type::UINT32:
      return What::template Do<Converter_Promotion<REALSXP, arrow::UInt32Type>>(
          arrays, std::forward<Args>(args)...);

    case Type::HALF_FLOAT:
      return What::template Do<Converter_Promotion<REALSXP, arrow::HalfFloatType>>(
          arrays, std::forward<Args>(args)...);

    case Type::FLOAT:
      return What::template Do<Converter_Promotion<REALSXP, arrow::FloatType>>(
          arrays, std::forward<Args>(args)...);

      // time32 ane time64
    case Type::TIME32:
      return What::template Do<Converter_Time<int32_t>>(arrays,
                                                        std::forward<Args>(args)...);

    case Type::TIME64:
      return What::template Do<Converter_Time<int64_t>>(arrays,
                                                        std::forward<Args>(args)...);

    case Type::TIMESTAMP:
      return What::template Do<Converter_Timestamp<int64_t>>(arrays,
                                                             std::forward<Args>(args)...);

    case Type::INT64:
      return What::template Do<Converter_Int64>(arrays, std::forward<Args>(args)...);

    case Type::DECIMAL:
      return What::template Do<Converter_Decimal>(arrays, std::forward<Args>(args)...);

    default:
      break;
  }

  return What::template Do<Converter_NotHandled>(arrays, std::forward<Args>(args)...);
}

struct Allocator {
  using OUT = SEXP;

  template <typename Converter>
  static SEXP Do(const ArrayVector& arrays, R_xlen_t n) {
    return Converter::Allocate(n, arrays);
  }
};

struct Ingester {
  using OUT = Status;

  template <typename Converter>
  static Status Do(const ArrayVector& arrays, SEXP data) {
    R_xlen_t k = 0;
    for (const auto& array : arrays) {
      auto n_chunk = array->length();
      RETURN_NOT_OK(Converter::Ingest(data, array, k, n_chunk));
      k += n_chunk;
    }
    return Status::OK();
  }
};

struct CanParallel {
  using OUT = bool;

  template <typename Converter>
  static bool Do(const ArrayVector& arrays) {
    return parallel_ingest<Converter>();
  }
};

// Only allocate an R vector to host the arrays
SEXP ArrayVector__Allocate(R_xlen_t n, const ArrayVector& arrays) {
  return ArrayVector__Dispatch<arrow::r::Allocator, R_xlen_t>(arrays, n);
}

// Ingest data from arrays to previously allocated R vector
// For most vector types, this can be done in a task
// in some other thread
Status ArrayVector__Ingest(SEXP data, const ArrayVector& arrays) {
  return ArrayVector__Dispatch<arrow::r::Ingester, SEXP>(arrays, data);
}

bool ArrayVector__Parallel(const ArrayVector& arrays) {
  return ArrayVector__Dispatch<arrow::r::CanParallel>(arrays);
}

// Allocate + Ingest
SEXP ArrayVector__as_vector(R_xlen_t n, const ArrayVector& arrays) {
  Shield<SEXP> data(ArrayVector__Allocate(n, arrays));
  STOP_IF_NOT_OK(ArrayVector__Ingest(data, arrays));
  return data;
}

}  // namespace r
}  // namespace arrow

// [[Rcpp::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  return arrow::r::ArrayVector__as_vector(array->length(), {array});
}

// [[Rcpp::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return arrow::r::ArrayVector__as_vector(chunked_array->length(),
                                          chunked_array->chunks());
}

List RecordBatch__to_dataframe_serial(const std::shared_ptr<arrow::RecordBatch>& batch) {
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

List RecordBatch__to_dataframe_parallel(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto nc = batch->num_columns();
  auto nr = batch->num_rows();
  List tbl(nc);
  CharacterVector names(nc);

  // task group to ingest data in parallel
  auto tg = arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool());

  // allocate and start ingesting immediately the columns that
  // can be ingested in parallel, i.e. when ingestion no longer
  // need to happen on the main thread
  for (int i = 0; i < nc; i++) {
    ArrayVector arrays{batch->column(i)};
    // allocate data for column i
    SEXP column = tbl[i] = arrow::r::ArrayVector__Allocate(nr, arrays);

    // add a task to ingest data of that column if that can be done in parallel
    if (arrow::r::ArrayVector__Parallel(arrays)) {
      tg->Append([=] { return arrow::r::ArrayVector__Ingest(column, arrays); });
    }
  }

  arrow::Status status = arrow::Status::OK();

  // ingest the other columns
  for (int i = 0; i < nc; i++) {
    // ingest if cannot ingest in parallel
    ArrayVector arrays{batch->column(i)};
    if (!arrow::r::ArrayVector__Parallel(arrays)) {
      status &= arrow::r::ArrayVector__Ingest(tbl[i], arrays);
    }

    names[i] = batch->column_name(i);
  }

  // wait for the ingestion to be finished
  status &= tg->Finish();

  STOP_IF_NOT_OK(status);

  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);

  return tbl;
}

// [[Rcpp::export]]
List RecordBatch__to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch,
                               bool use_threads) {
  if (use_threads) {
    return RecordBatch__to_dataframe_parallel(batch);
  } else {
    return RecordBatch__to_dataframe_serial(batch);
  }
}

List Table__to_dataframe_parallel(const std::shared_ptr<arrow::Table>& table) {
  auto nc = table->num_columns();
  auto nr = table->num_rows();
  List tbl(nc);
  CharacterVector names(nc);

  // task group to ingest data in parallel
  auto tg = arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool());

  // allocate and start ingesting immediately the columns that
  // can be ingested in parallel, i.e. when ingestion no longer
  // need to happen on the main thread
  for (int i = 0; i < nc; i++) {
    const ArrayVector& arrays = table->column(i)->data()->chunks();
    // allocate data for column i
    SEXP column = tbl[i] = arrow::r::ArrayVector__Allocate(nr, arrays);

    // add a task to ingest data of that column if that can be done in parallel
    if (arrow::r::ArrayVector__Parallel(arrays)) {
      tg->Append([=] { return arrow::r::ArrayVector__Ingest(column, arrays); });
    }
  }

  arrow::Status status = arrow::Status::OK();

  // ingest the other columns
  for (int i = 0; i < nc; i++) {
    // ingest if cannot ingest in parallel
    const ArrayVector& arrays = table->column(i)->data()->chunks();
    if (!arrow::r::ArrayVector__Parallel(arrays)) {
      status &= arrow::r::ArrayVector__Ingest(tbl[i], arrays);
    }

    names[i] = table->column(i)->name();
  }

  // wait for the ingestion to be finished
  status &= tg->Finish();

  STOP_IF_NOT_OK(status);

  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);

  return tbl;
}

List Table__to_dataframe_serial(const std::shared_ptr<arrow::Table>& table) {
  int nc = table->num_columns();
  int nr = table->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for (int i = 0; i < nc; i++) {
    auto column = table->column(i);
    tbl[i] = ChunkedArray__as_vector(column->data());
    names[i] = column->name();
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

// [[Rcpp::export]]
List Table__to_dataframe(const std::shared_ptr<arrow::Table>& table, bool use_threads) {
  if (use_threads) {
    return Table__to_dataframe_parallel(table);
  } else {
    return Table__to_dataframe_serial(table);
  }
  int nc = table->num_columns();
  int nr = table->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for (int i = 0; i < nc; i++) {
    auto column = table->column(i);
    tbl[i] = ChunkedArray__as_vector(column->data());
    names[i] = column->name();
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}
