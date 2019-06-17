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

#include "./arrow_types.h"
#if defined(ARROW_R_WITH_ARROW)

#include <arrow/util/parallel.h>
#include <arrow/util/task-group.h>

namespace arrow {
namespace r {

using Rcpp::default_value;
using Rcpp::IntegerVector;
using Rcpp::LogicalVector_;
using Rcpp::no_init;
using Rcpp::Shield;
using Rcpp::StringVector_;

class Converter {
 public:
  explicit Converter(const ArrayVector& arrays) : arrays_(arrays) {}

  virtual ~Converter() {}

  // Allocate a vector of the right R type for this converter
  virtual SEXP Allocate(R_xlen_t n) const = 0;

  // data[ start:(start + n) ] = NA
  virtual Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const = 0;

  // ingest the values from the array into data[ start : (start + n)]
  virtual Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                                   R_xlen_t start, R_xlen_t n) const = 0;

  // ingest one array
  Status IngestOne(SEXP data, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n) const {
    if (array->null_count() == n) {
      return Ingest_all_nulls(data, start, n);
    } else {
      return Ingest_some_nulls(data, array, start, n);
    }
  }

  // can this run in parallel ?
  virtual bool Parallel() const { return true; }

  // Ingest all the arrays serially
  Status IngestSerial(SEXP data) {
    R_xlen_t k = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      RETURN_NOT_OK(IngestOne(data, array, k, n_chunk));
      k += n_chunk;
    }
    return Status::OK();
  }

  // ingest the arrays in parallel
  //
  // for each array, add a task to the task group
  //
  // The task group is Finish() iun the caller
  void IngestParallel(SEXP data, const std::shared_ptr<arrow::internal::TaskGroup>& tg) {
    R_xlen_t k = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      tg->Append([=] { return IngestOne(data, array, k, n_chunk); });
      k += n_chunk;
    }
  }

  // Converter factory
  static std::shared_ptr<Converter> Make(const ArrayVector& arrays);

 protected:
  const ArrayVector& arrays_;
};

// data[start:(start+n)] = NA
template <int RTYPE>
Status AllNull_Ingest(SEXP data, R_xlen_t start, R_xlen_t n) {
  auto p_data = Rcpp::internal::r_vector_start<RTYPE>(data) + start;
  std::fill_n(p_data, n, Rcpp::default_value<RTYPE>());
  return Status::OK();
}

// ingest the data from `array` into a slice of `data`
//
// each element goes through `lambda` when some conversion is needed
template <int RTYPE, typename array_value_type, typename Lambda>
Status SomeNull_Ingest(SEXP data, R_xlen_t start, R_xlen_t n,
                       const array_value_type* p_values,
                       const std::shared_ptr<arrow::Array>& array, Lambda lambda) {
  if (!p_values) {
    return Status::Invalid("Invalid data buffer");
  }
  auto p_data = Rcpp::internal::r_vector_start<RTYPE>(data) + start;

  if (array->null_count()) {
    arrow::internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                                array->offset(), n);
    for (R_xlen_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_data, ++p_values) {
      *p_data = bitmap_reader.IsSet() ? lambda(*p_values) : default_value<RTYPE>();
    }
  } else {
    std::transform(p_values, p_values + n, p_data, lambda);
  }

  return Status::OK();
}

// Allocate + Ingest
SEXP ArrayVector__as_vector(R_xlen_t n, const ArrayVector& arrays) {
  auto converter = Converter::Make(arrays);
  Shield<SEXP> data(converter->Allocate(n));
  STOP_IF_NOT_OK(converter->IngestSerial(data));
  return data;
}

template <int RTYPE>
class Converter_SimpleArray : public Converter {
  using Vector = Rcpp::Vector<RTYPE, Rcpp::NoProtectStorage>;
  using value_type = typename Vector::stored_type;

 public:
  explicit Converter_SimpleArray(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Vector(no_init(n)); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<RTYPE>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto p_values = array->data()->GetValues<value_type>(1);
    auto echo = [](value_type value) { return value; };
    return SomeNull_Ingest<RTYPE, value_type>(data, start, n, p_values, array, echo);
  }
};

class Converter_Date32 : public Converter_SimpleArray<INTSXP> {
 public:
  explicit Converter_Date32(const ArrayVector& arrays)
      : Converter_SimpleArray<INTSXP>(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    IntegerVector data(no_init(n));
    data.attr("class") = "Date";
    return data;
  }
};

struct Converter_String : public Converter {
 public:
  explicit Converter_String(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return StringVector_(no_init(n)); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<STRSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto p_offset = array->data()->GetValues<int32_t>(1);
    if (!p_offset) {
      return Status::Invalid("Invalid offset buffer");
    }
    auto p_strings = array->data()->GetValues<char>(2, *p_offset);
    if (!p_strings) {
      // There is an offset buffer, but the data buffer is null
      // There is at least one value in the array and not all the values are null
      // That means all values are either empty strings or nulls so there is nothing to do

      if (array->null_count()) {
        arrow::internal::BitmapReader null_reader(array->null_bitmap_data(),
                                                  array->offset(), n);
        for (int i = 0; i < n; i++, null_reader.Next()) {
          if (null_reader.IsNotSet()) {
            SET_STRING_ELT(data, start + i, NA_STRING);
          }
        }
      }
      return Status::OK();
    }

    arrow::StringArray* string_array = static_cast<arrow::StringArray*>(array.get());
    if (array->null_count()) {
      // need to watch for nulls
      arrow::internal::BitmapReader null_reader(array->null_bitmap_data(),
                                                array->offset(), n);
      for (int i = 0; i < n; i++, null_reader.Next()) {
        if (null_reader.IsSet()) {
          SET_STRING_ELT(data, start + i, r_string(string_array->GetString(i)));
        } else {
          SET_STRING_ELT(data, start + i, NA_STRING);
        }
      }

    } else {
      for (int i = 0; i < n; i++) {
        SET_STRING_ELT(data, start + i, r_string(string_array->GetString(i)));
      }
    }

    return Status::OK();
  }

  bool Parallel() const { return false; }

  inline SEXP r_string(const arrow::util::string_view& view) const {
    return Rf_mkCharLenCE(view.data(), view.size(), CE_UTF8);
  }
};

class Converter_Boolean : public Converter {
 public:
  explicit Converter_Boolean(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return LogicalVector_(no_init(n)); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<LGLSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto p_data = Rcpp::internal::r_vector_start<LGLSXP>(data) + start;
    auto p_bools = array->data()->GetValues<uint8_t>(1, 0);
    if (!p_bools) {
      return Status::Invalid("Invalid data buffer");
    }

    arrow::internal::BitmapReader data_reader(p_bools, array->offset(), n);
    if (array->null_count()) {
      arrow::internal::BitmapReader null_reader(array->null_bitmap()->data(),
                                                array->offset(), n);

      for (R_xlen_t i = 0; i < n; i++, data_reader.Next(), null_reader.Next(), ++p_data) {
        *p_data = null_reader.IsSet() ? data_reader.IsSet() : NA_LOGICAL;
      }
    } else {
      for (R_xlen_t i = 0; i < n; i++, data_reader.Next(), ++p_data) {
        *p_data = data_reader.IsSet();
      }
    }

    return Status::OK();
  }
};

class Converter_Dictionary : public Converter {
 public:
  explicit Converter_Dictionary(const ArrayVector& arrays) : Converter(arrays) {}

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
        Rcpp::stop("Cannot convert Dictionary Array of type `%s` to R",
                   dict_array->type()->ToString());
    }

    if (dict->type_id() != Type::STRING) {
      Rcpp::stop("Cannot convert Dictionary Array of type `%s` to R",
                 dict_array->type()->ToString());
    }
    bool ordered = dict_array->dict_type()->ordered();

    data.attr("levels") = ArrayVector__as_vector(dict->length(), {dict});
    if (ordered) {
      data.attr("class") = Rcpp::CharacterVector::create("ordered", "factor");
    } else {
      data.attr("class") = "factor";
    }
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<INTSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    DictionaryArray* dict_array = static_cast<DictionaryArray*>(array.get());
    auto indices = dict_array->indices();
    switch (indices->type_id()) {
      case Type::UINT8:
        return Ingest_some_nulls_Impl<arrow::UInt8Type>(data, array, start, n);
      case Type::INT8:
        return Ingest_some_nulls_Impl<arrow::Int8Type>(data, array, start, n);
      case Type::UINT16:
        return Ingest_some_nulls_Impl<arrow::UInt16Type>(data, array, start, n);
      case Type::INT16:
        return Ingest_some_nulls_Impl<arrow::Int16Type>(data, array, start, n);
      case Type::INT32:
        return Ingest_some_nulls_Impl<arrow::Int32Type>(data, array, start, n);
      default:
        break;
    }
    return Status::OK();
  }

 private:
  template <typename Type>
  Status Ingest_some_nulls_Impl(SEXP data, const std::shared_ptr<arrow::Array>& array,
                                R_xlen_t start, R_xlen_t n) const {
    using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;

    std::shared_ptr<Array> indices =
        static_cast<DictionaryArray*>(array.get())->indices();

    // convert the 0-based indices from the arrow Array
    // to 1-based indices used in R factors
    auto to_r_index = [](value_type value) { return static_cast<int>(value) + 1; };

    return SomeNull_Ingest<INTSXP, value_type>(
        data, start, n, indices->data()->GetValues<value_type>(1), indices, to_r_index);
  }
};

class Converter_Struct : public Converter {
 public:
  explicit Converter_Struct(const ArrayVector& arrays) : Converter(arrays), converters() {
    auto first_array =
        internal::checked_cast<arrow::StructArray*>(Converter::arrays_[0].get());
    int nf = first_array->num_fields();
    for (int i = 0; i < nf; i++) {
      converters.push_back(Converter::Make({first_array->field(i)}));
    }
  }

  SEXP Allocate(R_xlen_t n) const {
    // allocate a data frame column to host each array
    auto first_array =
        internal::checked_cast<arrow::StructArray*>(Converter::arrays_[0].get());
    auto type = first_array->struct_type();
    int nf = first_array->num_fields();
    Rcpp::List out(nf);
    Rcpp::CharacterVector colnames(nf);
    for (int i = 0; i < nf; i++) {
      out[i] = converters[i]->Allocate(n);
      colnames[i] = type->child(i)->name();
    }
    IntegerVector rn(2);
    rn[0] = NA_INTEGER;
    rn[1] = -n;
    Rf_setAttrib(out, symbols::row_names, rn);
    Rf_setAttrib(out, R_NamesSymbol, colnames);
    Rf_setAttrib(out, R_ClassSymbol, Rf_mkString("data.frame"));
    return out;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    int nf = converters.size();
    for (int i = 0; i < nf; i++) {
      STOP_IF_NOT_OK(converters[i]->Ingest_all_nulls(VECTOR_ELT(data, i), start, n));
    }
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto struct_array = internal::checked_cast<arrow::StructArray*>(array.get());
    int nf = converters.size();
    // Flatten() deals with merging of nulls
    ArrayVector arrays(nf);
    STOP_IF_NOT_OK(struct_array->Flatten(default_memory_pool(), &arrays));
    for (int i = 0; i < nf; i++) {
      STOP_IF_NOT_OK(
          converters[i]->Ingest_some_nulls(VECTOR_ELT(data, i), arrays[i], start, n));
    }

    return Status::OK();
  }

 private:
  std::vector<std::shared_ptr<Converter>> converters;
};

double ms_to_seconds(int64_t ms) { return static_cast<double>(ms / 1000); }

class Converter_Date64 : public Converter {
 public:
  explicit Converter_Date64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    Rcpp::NumericVector data(no_init(n));
    data.attr("class") = Rcpp::CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<REALSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto convert = [](int64_t ms) { return static_cast<double>(ms / 1000); };
    return SomeNull_Ingest<REALSXP, int64_t>(
        data, start, n, array->data()->GetValues<int64_t>(1), array, convert);
  }
};

template <int RTYPE, typename Type>
class Converter_Promotion : public Converter {
  using r_stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

 public:
  explicit Converter_Promotion(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    return Rcpp::Vector<RTYPE, Rcpp::NoProtectStorage>(no_init(n));
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<RTYPE>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto convert = [](value_type value) { return static_cast<r_stored_type>(value); };
    return SomeNull_Ingest<RTYPE, value_type>(
        data, start, n, array->data()->GetValues<value_type>(1), array, convert);
  }

 private:
  static r_stored_type value_convert(value_type value) {
    return static_cast<r_stored_type>(value);
  }
};

template <typename value_type>
class Converter_Time : public Converter {
 public:
  explicit Converter_Time(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    Rcpp::NumericVector data(no_init(n));
    data.attr("class") = Rcpp::CharacterVector::create("hms", "difftime");
    data.attr("units") = Rcpp::CharacterVector::create("secs");
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<REALSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    int multiplier = TimeUnit_multiplier(array);
    auto convert = [=](value_type value) {
      return static_cast<double>(value) / multiplier;
    };
    return SomeNull_Ingest<REALSXP, value_type>(
        data, start, n, array->data()->GetValues<value_type>(1), array, convert);
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
      default:
        return 0;
    }
  }
};

template <typename value_type>
class Converter_Timestamp : public Converter_Time<value_type> {
 public:
  explicit Converter_Timestamp(const ArrayVector& arrays)
      : Converter_Time<value_type>(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    Rcpp::NumericVector data(no_init(n));
    data.attr("class") = Rcpp::CharacterVector::create("POSIXct", "POSIXt");
    return data;
  }
};

class Converter_Decimal : public Converter {
 public:
  explicit Converter_Decimal(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rcpp::NumericVector_(no_init(n)); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return AllNull_Ingest<REALSXP>(data, start, n);
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto p_data = Rcpp::internal::r_vector_start<REALSXP>(data) + start;
    const auto& decimals_arr =
        internal::checked_cast<const arrow::Decimal128Array&>(*array);

    if (array->null_count()) {
      internal::BitmapReader bitmap_reader(array->null_bitmap()->data(), array->offset(),
                                           n);

      for (R_xlen_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_data) {
        *p_data = bitmap_reader.IsSet() ? std::stod(decimals_arr.FormatValue(i).c_str())
                                        : NA_REAL;
      }
    } else {
      for (R_xlen_t i = 0; i < n; i++, ++p_data) {
        *p_data = std::stod(decimals_arr.FormatValue(i).c_str());
      }
    }

    return Status::OK();
  }
};

class Converter_Int64 : public Converter {
 public:
  explicit Converter_Int64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    Rcpp::NumericVector data(no_init(n));
    data.attr("class") = "integer64";
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    auto p_data = reinterpret_cast<int64_t*>(REAL(data)) + start;
    std::fill_n(p_data, n, NA_INT64);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n) const {
    auto p_values = array->data()->GetValues<int64_t>(1);
    if (!p_values) {
      return Status::Invalid("Invalid data buffer");
    }

    auto p_data = reinterpret_cast<int64_t*>(REAL(data)) + start;

    if (array->null_count()) {
      internal::BitmapReader bitmap_reader(array->null_bitmap()->data(), array->offset(),
                                           n);
      for (R_xlen_t i = 0; i < n; i++, bitmap_reader.Next(), ++p_data) {
        *p_data = bitmap_reader.IsSet() ? p_values[i] : NA_INT64;
      }
    } else {
      std::copy_n(p_values, n, p_data);
    }

    return Status::OK();
  }
};

std::shared_ptr<Converter> Converter::Make(const ArrayVector& arrays) {
  switch (arrays[0]->type_id()) {
    // direct support
    case Type::INT32:
      return std::make_shared<arrow::r::Converter_SimpleArray<INTSXP>>(arrays);

    case Type::DOUBLE:
      return std::make_shared<arrow::r::Converter_SimpleArray<REALSXP>>(arrays);

      // need to handle 1-bit case
    case Type::BOOL:
      return std::make_shared<arrow::r::Converter_Boolean>(arrays);

      // handle memory dense strings
    case Type::STRING:
      return std::make_shared<arrow::r::Converter_String>(arrays);

    case Type::DICTIONARY:
      return std::make_shared<arrow::r::Converter_Dictionary>(arrays);

    case Type::DATE32:
      return std::make_shared<arrow::r::Converter_Date32>(arrays);

    case Type::DATE64:
      return std::make_shared<arrow::r::Converter_Date64>(arrays);

      // promotions to integer vector
    case Type::INT8:
      return std::make_shared<arrow::r::Converter_Promotion<INTSXP, arrow::Int8Type>>(
          arrays);

    case Type::UINT8:
      return std::make_shared<arrow::r::Converter_Promotion<INTSXP, arrow::UInt8Type>>(
          arrays);

    case Type::INT16:
      return std::make_shared<arrow::r::Converter_Promotion<INTSXP, arrow::Int16Type>>(
          arrays);

    case Type::UINT16:
      return std::make_shared<arrow::r::Converter_Promotion<INTSXP, arrow::UInt16Type>>(
          arrays);

      // promotions to numeric vector
    case Type::UINT32:
      return std::make_shared<arrow::r::Converter_Promotion<REALSXP, arrow::UInt32Type>>(
          arrays);

    case Type::HALF_FLOAT:
      return std::make_shared<
          arrow::r::Converter_Promotion<REALSXP, arrow::HalfFloatType>>(arrays);

    case Type::FLOAT:
      return std::make_shared<arrow::r::Converter_Promotion<REALSXP, arrow::FloatType>>(
          arrays);

      // time32 ane time64
    case Type::TIME32:
      return std::make_shared<arrow::r::Converter_Time<int32_t>>(arrays);

    case Type::TIME64:
      return std::make_shared<arrow::r::Converter_Time<int64_t>>(arrays);

    case Type::TIMESTAMP:
      return std::make_shared<arrow::r::Converter_Timestamp<int64_t>>(arrays);

    case Type::INT64:
      return std::make_shared<arrow::r::Converter_Int64>(arrays);

    case Type::DECIMAL:
      return std::make_shared<arrow::r::Converter_Decimal>(arrays);

    case Type::STRUCT:
      return std::make_shared<arrow::r::Converter_Struct>(arrays);

    default:
      break;
  }

  Rcpp::stop(tfm::format("cannot handle Array of type %s", arrays[0]->type()->name()));
  return nullptr;
}

Rcpp::List to_dataframe_serial(
    int64_t nr, int64_t nc, const Rcpp::CharacterVector& names,
    const std::vector<std::shared_ptr<Converter>>& converters) {
  Rcpp::List tbl(nc);

  for (int i = 0; i < nc; i++) {
    SEXP column = tbl[i] = converters[i]->Allocate(nr);
    STOP_IF_NOT_OK(converters[i]->IngestSerial(column));
  }
  tbl.attr("names") = names;
  tbl.attr("class") = Rcpp::CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = Rcpp::IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

Rcpp::List to_dataframe_parallel(
    int64_t nr, int64_t nc, const Rcpp::CharacterVector& names,
    const std::vector<std::shared_ptr<Converter>>& converters) {
  Rcpp::List tbl(nc);

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
      status &= converters[i]->IngestSerial(tbl[i]);
    }
  }

  // wait for the ingestion to be finished
  status &= tg->Finish();

  STOP_IF_NOT_OK(status);

  tbl.attr("names") = names;
  tbl.attr("class") = Rcpp::CharacterVector::create("tbl_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);

  return tbl;
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  return arrow::r::ArrayVector__as_vector(array->length(), {array});
}

// [[arrow::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return arrow::r::ArrayVector__as_vector(chunked_array->length(),
                                          chunked_array->chunks());
}

// [[arrow::export]]
Rcpp::List RecordBatch__to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     bool use_threads) {
  int64_t nc = batch->num_columns();
  int64_t nr = batch->num_rows();
  Rcpp::CharacterVector names(nc);
  std::vector<arrow::ArrayVector> arrays(nc);
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

// [[arrow::export]]
Rcpp::List Table__to_dataframe(const std::shared_ptr<arrow::Table>& table,
                               bool use_threads) {
  int64_t nc = table->num_columns();
  int64_t nr = table->num_rows();
  Rcpp::CharacterVector names(nc);
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

#endif
