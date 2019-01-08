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

class Converter {
 public:
  Converter(const ArrayVector& arrays) : arrays_(arrays) {}

  virtual ~Converter() {}

  virtual SEXP Allocate(R_xlen_t n) const = 0;
  virtual Status IngestOne(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const = 0;

  virtual bool Parallel() const { return true; }

  Status Ingest(SEXP data) {
    R_xlen_t k = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      RETURN_NOT_OK(IngestOne(data, array, k, n_chunk));
      k += n_chunk;
    }
    return Status::OK();
  }

  void IngestParallel(SEXP data, const std::shared_ptr<arrow::internal::TaskGroup>& tg) {
    R_xlen_t k = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      tg->Append([=] { return IngestOne(data, array, k, n_chunk); });
      k += n_chunk;
    }
  }

  SEXP Convert(R_xlen_t n) {
    Shield<SEXP> column(Allocate(n));
    STOP_IF_NOT_OK(Ingest(column));
    return column;
  }

  static std::shared_ptr<Converter> Make(const ArrayVector& arrays);

 protected:
  const ArrayVector& arrays_;
};

// Allocate + Ingest
SEXP ArrayVector__as_vector(R_xlen_t n, const ArrayVector& arrays) {
  auto converter = Converter::Make(arrays);
  Shield<SEXP> data(converter->Allocate(n));
  STOP_IF_NOT_OK(converter->Ingest(data));
  return data;
}

template <int RTYPE>
class Converter_SimpleArray : public Converter {
  using Vector = Rcpp::Vector<RTYPE, Rcpp::NoProtectStorage>;

 public:
  Converter_SimpleArray(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Vector(no_init(n)); }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

class Converter_Date32 : public Converter_SimpleArray<INTSXP> {
 public:
  Converter_Date32(const ArrayVector& arrays) : Converter_SimpleArray<INTSXP>(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    IntegerVector data(no_init(n));
    data.attr("class") = "Date";
    return data;
  }
};

struct Converter_String : public Converter {
 public:
  Converter_String(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return StringVector_(no_init(n)); }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

  bool Parallel() const { return false; }
};

class Converter_Boolean : public Converter {
 public:
  Converter_Boolean(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return LogicalVector_(no_init(n)); }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

class Converter_Dictionary : public Converter {
 public:
  Converter_Dictionary(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    IntegerVector data(no_init(n));
    auto dict_array = static_cast<DictionaryArray*>(Converter::arrays_[0].get());
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

    data.attr("levels") = ArrayVector__as_vector(dict->length(), {dict});
    if (ordered) {
      data.attr("class") = CharacterVector::create("ordered", "factor");
    } else {
      data.attr("class") = "factor";
    }
    return data;
  }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

 private:
  template <typename Type>
  Status Ingest_Impl(SEXP data_, const std::shared_ptr<arrow::Array>& array,
                     R_xlen_t start, R_xlen_t n) const {
    IntegerVector_ data(data_);

    DictionaryArray* dict_array = static_cast<DictionaryArray*>(array.get());
    using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;

    // convert the 0-based indices from the arrow Array
    // to 1-based indices used in R factors
    auto to_r_index = [](value_type value) { return static_cast<int>(value) + 1; };

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
          *p_data = bitmap_reader.IsSet() ? to_r_index(*p_array) : NA_INTEGER;
        }
      } else {
        std::transform(p_array, p_array + n, data.begin() + start, to_r_index);
      }
    }
    return Status::OK();
  }
};

class Converter_Date64 : public Converter {
 public:
  Converter_Date64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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
class Converter_Promotion : public Converter {
  using r_stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

 public:
  Converter_Promotion(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rcpp::Vector<RTYPE>(no_init(n)); }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

template <typename value_type>
class Converter_Time : public Converter {
 public:
  Converter_Time(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("hms", "difftime");
    data.attr("units") = CharacterVector::create("secs");
    return data;
  }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

 private:
  int TimeUnit_multiplier(const std::shared_ptr<Array>& array) const {
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
};

template <typename value_type>
class Converter_Timestamp : public Converter_Time<value_type> {
 public:
  Converter_Timestamp(const ArrayVector& arrays) : Converter_Time<value_type>(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    NumericVector data(no_init(n));
    data.attr("class") = CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }
};

class Converter_Int64 : public Converter {
 public:
  Converter_Int64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    NumericVector data(no_init(n));
    data.attr("class") = "integer64";
    return data;
  }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

class Converter_Decimal : public Converter {
 public:
  Converter_Decimal(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return NumericVector_(no_init(n)); }

  Status IngestOne(SEXP data_, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
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

std::shared_ptr<Converter> Converter::Make(const ArrayVector& arrays) {
  using namespace arrow::r;

  switch (arrays[0]->type_id()) {
    // direct support
    case Type::INT8:
      return std::make_shared<Converter_SimpleArray<RAWSXP>>(arrays);

    case Type::INT32:
      return std::make_shared<Converter_SimpleArray<INTSXP>>(arrays);

    case Type::DOUBLE:
      return std::make_shared<Converter_SimpleArray<REALSXP>>(arrays);

      // need to handle 1-bit case
    case Type::BOOL:
      return std::make_shared<Converter_Boolean>(arrays);

      // handle memory dense strings
    case Type::STRING:
      return std::make_shared<Converter_String>(arrays);

    case Type::DICTIONARY:
      return std::make_shared<Converter_Dictionary>(arrays);

    case Type::DATE32:
      return std::make_shared<Converter_Date32>(arrays);

    case Type::DATE64:
      return std::make_shared<Converter_Date64>(arrays);

      // promotions to integer vector
    case Type::UINT8:
      return std::make_shared<Converter_Promotion<INTSXP, arrow::UInt8Type>>(arrays);

    case Type::INT16:
      return std::make_shared<Converter_Promotion<INTSXP, arrow::Int16Type>>(arrays);

    case Type::UINT16:
      return std::make_shared<Converter_Promotion<INTSXP, arrow::UInt16Type>>(arrays);

      // promotions to numeric vector
    case Type::UINT32:
      return std::make_shared<Converter_Promotion<REALSXP, arrow::UInt32Type>>(arrays);

    case Type::HALF_FLOAT:
      return std::make_shared<Converter_Promotion<REALSXP, arrow::HalfFloatType>>(arrays);

    case Type::FLOAT:
      return std::make_shared<Converter_Promotion<REALSXP, arrow::FloatType>>(arrays);

      // time32 ane time64
    case Type::TIME32:
      return std::make_shared<Converter_Time<int32_t>>(arrays);

    case Type::TIME64:
      return std::make_shared<Converter_Time<int64_t>>(arrays);

    case Type::TIMESTAMP:
      return std::make_shared<Converter_Timestamp<int64_t>>(arrays);

    case Type::INT64:
      return std::make_shared<Converter_Int64>(arrays);

    case Type::DECIMAL:
      return std::make_shared<Converter_Decimal>(arrays);

    default:
      break;
  }

  stop(tfm::format("cannot handle Array of type %s", arrays[0]->type()->name()));
  return nullptr;
}

List to_dataframe_serial(int64_t nr, int64_t nc, const CharacterVector& names,
                         const std::vector<std::shared_ptr<Converter>>& converters) {
  List tbl(nc);

  for (int i = 0; i < nc; i++) {
    tbl[i] = converters[i]->Convert(nr);
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

List to_dataframe_parallel(int64_t nr, int64_t nc, const CharacterVector& names,
                           const std::vector<std::shared_ptr<Converter>>& converters) {
  List tbl(nc);

  // task group to ingest data in parallel
  auto tg = arrow::internal::TaskGroup::MakeThreaded(arrow::internal::GetCpuThreadPool());

  // allocate and start ingesting immediately the columns that
  // can be ingested in parallel, i.e. when ingestion no longer
  // need to happen on the main thread
  for (int i = 0; i < nc; i++) {
    // allocate data for column i
    SEXP column = tbl[i] = converters[i]->Allocate(nr);

    // add a task to ingest data of that column if that can be done in parallel
    if (converters[i]->Parallel()) {
      converters[i]->IngestParallel(column, tg);
    }
  }

  arrow::Status status = arrow::Status::OK();

  // ingest the columns that cannot be dealt with in parallel
  for (int i = 0; i < nc; i++) {
    if (!converters[i]->Parallel()) {
      status &= converters[i]->Ingest(tbl[i]);
    }
  }

  // wait for the ingestion to be finished
  status &= tg->Finish();

  STOP_IF_NOT_OK(status);

  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);

  return tbl;
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

// [[Rcpp::export]]
List RecordBatch__to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch,
                               bool use_threads) {
  int64_t nc = batch->num_columns();
  int64_t nr = batch->num_rows();
  CharacterVector names(nc);
  std::vector<ArrayVector> arrays(nc);
  std::vector<std::shared_ptr<arrow::r::Converter>> converters(nc);

  for (int64_t i = 0; i < nc; i++) {
    names[i] = batch->column_name(i);
    arrays[i] = {batch->column(i)};
    converters[i] = arrow::r::Converter::Make(arrays[i]);
  }

  if (use_threads) {
    return arrow::r::to_dataframe_parallel(nr, nc, names, converters);
  } else {
    return arrow::r::to_dataframe_serial(nr, nc, names, converters);
  }
}

// [[Rcpp::export]]
List Table__to_dataframe(const std::shared_ptr<arrow::Table>& table, bool use_threads) {
  int64_t nc = table->num_columns();
  int64_t nr = table->num_rows();
  CharacterVector names(nc);
  std::vector<std::shared_ptr<arrow::r::Converter>> converters(nc);

  for (int64_t i = 0; i < nc; i++) {
    converters[i] = arrow::r::Converter::Make(table->column(i)->data()->chunks());
    names[i] = table->column(i)->name();
  }

  if (use_threads) {
    return arrow::r::to_dataframe_parallel(nr, nc, names, converters);
  } else {
    return arrow::r::to_dataframe_serial(nr, nc, names, converters);
  }
}
