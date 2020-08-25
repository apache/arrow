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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <arrow/util/bitmap_reader.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/util/int_util.h>
#include <arrow/util/parallel.h>
#include <arrow/util/task_group.h>

namespace arrow {

using internal::checked_cast;
using internal::IntegersCanFit;

namespace r {

class Converter {
 public:
  explicit Converter(ArrayVector arrays) : arrays_(std::move(arrays)) {}

  virtual ~Converter() {}

  // Allocate a vector of the right R type for this converter
  virtual SEXP Allocate(R_xlen_t n) const = 0;

  // data[ start:(start + n) ] = NA
  virtual Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const = 0;

  // ingest the values from the array into data[ start : (start + n)]
  //
  // chunk_index indicates which of the chunk is being ingested into data. This is
  //             ignored by most implementations and currently only used with Dictionary
  //             arrays.
  virtual Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                                   R_xlen_t start, R_xlen_t n,
                                   size_t chunk_index) const = 0;

  // ingest one array
  Status IngestOne(SEXP data, const std::shared_ptr<arrow::Array>& array, R_xlen_t start,
                   R_xlen_t n, size_t chunk_index) const {
    if (array->null_count() == n) {
      return Ingest_all_nulls(data, start, n);
    } else {
      return Ingest_some_nulls(data, array, start, n, chunk_index);
    }
  }

  // can this run in parallel ?
  virtual bool Parallel() const { return true; }

  // Ingest all the arrays serially
  Status IngestSerial(SEXP data) {
    R_xlen_t k = 0, i = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      RETURN_NOT_OK(IngestOne(data, array, k, n_chunk, i));
      k += n_chunk;
      i++;
    }
    return Status::OK();
  }

  // ingest the arrays in parallel
  //
  // for each array, add a task to the task group
  //
  // The task group is Finish() in the caller
  void IngestParallel(SEXP data, const std::shared_ptr<arrow::internal::TaskGroup>& tg) {
    R_xlen_t k = 0, i = 0;
    for (const auto& array : arrays_) {
      auto n_chunk = array->length();
      tg->Append([=] { return IngestOne(data, array, k, n_chunk, i); });
      k += n_chunk;
      i++;
    }
  }

  // Converter factory
  static std::shared_ptr<Converter> Make(const std::shared_ptr<DataType>& type,
                                         ArrayVector arrays);

 protected:
  ArrayVector arrays_;
};

template <typename SetNonNull, typename SetNull>
Status IngestSome(const std::shared_ptr<arrow::Array>& array, R_xlen_t n,
                  SetNonNull&& set_non_null, SetNull&& set_null) {
  if (array->null_count()) {
    internal::BitmapReader bitmap_reader(array->null_bitmap()->data(), array->offset(),
                                         n);

    for (R_xlen_t i = 0; i < n; i++, bitmap_reader.Next()) {
      if (bitmap_reader.IsSet()) {
        RETURN_NOT_OK(set_non_null(i));
      } else {
        RETURN_NOT_OK(set_null(i));
      }
    }

  } else {
    for (R_xlen_t i = 0; i < n; i++) {
      RETURN_NOT_OK(set_non_null(i));
    }
  }

  return Status::OK();
}

template <typename SetNonNull>
Status IngestSome(const std::shared_ptr<arrow::Array>& array, R_xlen_t n,
                  SetNonNull&& set_non_null) {
  auto nothing = [](R_xlen_t i) { return Status::OK(); };
  return IngestSome(array, n, std::forward<SetNonNull>(set_non_null), nothing);
}

// Allocate + Ingest
SEXP ArrayVector__as_vector(R_xlen_t n, const std::shared_ptr<DataType>& type,
                            const ArrayVector& arrays) {
  auto converter = Converter::Make(type, arrays);
  SEXP data = PROTECT(converter->Allocate(n));
  StopIfNotOk(converter->IngestSerial(data));
  UNPROTECT(1);
  return data;
}

template <typename Type>
class Converter_Int : public Converter {
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

 public:
  explicit Converter_Int(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(INTSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(INTEGER(data) + start, n, NA_INTEGER);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_values = array->data()->GetValues<value_type>(1);
    if (!p_values) {
      return Status::Invalid("Invalid data buffer");
    }
    auto p_data = INTEGER(data) + start;
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = static_cast<int>(p_values[i]);
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_INTEGER;
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one, null_one);
  }
};

template <typename Type>
class Converter_Double : public Converter {
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

 public:
  explicit Converter_Double(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(REALSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_values = array->data()->GetValues<value_type>(1);
    if (!p_values) {
      return Status::Invalid("Invalid data buffer");
    }
    auto p_data = REAL(data) + start;
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = static_cast<value_type>(p_values[i]);
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_REAL;
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one, null_one);
  }
};

class Converter_Date32 : public Converter {
 public:
  explicit Converter_Date32(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    SEXP data = PROTECT(Rf_allocVector(REALSXP, n));
    Rf_classgets(data, Rf_mkString("Date"));
    UNPROTECT(1);
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_values = array->data()->GetValues<int>(1);
    if (!p_values) {
      return Status::Invalid("Invalid data buffer");
    }
    auto p_data = REAL(data) + start;
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = static_cast<double>(p_values[i]);
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_REAL;
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one, null_one);
  }
};

template <typename StringArrayType>
struct Converter_String : public Converter {
 public:
  explicit Converter_String(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(STRSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    for (R_xlen_t i = 0; i < n; i++) {
      SET_STRING_ELT(data, i + start, NA_STRING);
    }
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
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

    StringArrayType* string_array = static_cast<StringArrayType*>(array.get());
    if (array->null_count()) {
      // need to watch for nulls
      arrow::internal::BitmapReader null_reader(array->null_bitmap_data(),
                                                array->offset(), n);
      for (int i = 0; i < n; i++, null_reader.Next()) {
        if (null_reader.IsSet()) {
          SET_STRING_ELT(data, start + i, cpp11::r_string(string_array->GetString(i)));
        } else {
          SET_STRING_ELT(data, start + i, NA_STRING);
        }
      }

    } else {
      for (int i = 0; i < n; i++) {
        SET_STRING_ELT(data, start + i, cpp11::r_string(string_array->GetString(i)));
      }
    }

    return Status::OK();
  }

  bool Parallel() const { return false; }
};

class Converter_Boolean : public Converter {
 public:
  explicit Converter_Boolean(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(LGLSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(LOGICAL(data) + start, n, NA_LOGICAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_data = LOGICAL(data) + start;
    auto p_bools = array->data()->GetValues<uint8_t>(1, 0);
    if (!p_bools) {
      return Status::Invalid("Invalid data buffer");
    }

    arrow::internal::BitmapReader data_reader(p_bools, array->offset(), n);
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = data_reader.IsSet();
      data_reader.Next();
      return Status::OK();
    };

    auto null_one = [&](R_xlen_t i) {
      data_reader.Next();
      p_data[i] = NA_LOGICAL;
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one, null_one);
  }
};

template <typename ArrayType>
class Converter_Binary : public Converter {
 public:
  using offset_type = typename ArrayType::offset_type;
  explicit Converter_Binary(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    SEXP res = PROTECT(Rf_allocVector(VECSXP, n));
    if (std::is_same<ArrayType, BinaryArray>::value) {
      Rf_classgets(res, data::classes_arrow_binary);
    } else {
      Rf_classgets(res, data::classes_arrow_large_binary);
    }
    UNPROTECT(1);
    return res;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    const ArrayType* binary_array = checked_cast<const ArrayType*>(array.get());

    auto ingest_one = [&](R_xlen_t i) {
      offset_type ni;
      auto value = binary_array->GetValue(i, &ni);
      if (ni > R_XLEN_T_MAX) {
        return Status::RError("Array too big to be represented as a raw vector");
      }
      SEXP raw = PROTECT(Rf_allocVector(RAWSXP, ni));
      std::copy(value, value + ni, RAW(raw));

      SET_VECTOR_ELT(data, i + start, raw);
      UNPROTECT(1);

      return Status::OK();
    };

    return IngestSome(array, n, ingest_one);
  }
};

class Converter_FixedSizeBinary : public Converter {
 public:
  explicit Converter_FixedSizeBinary(const ArrayVector& arrays, int byte_width)
      : Converter(arrays), byte_width_(byte_width) {}

  SEXP Allocate(R_xlen_t n) const {
    SEXP res = PROTECT(Rf_allocVector(VECSXP, n));
    Rf_classgets(res, data::classes_arrow_fixed_size_binary);
    Rf_setAttrib(res, symbols::byte_width, Rf_ScalarInteger(byte_width_));
    UNPROTECT(1);
    return res;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    const FixedSizeBinaryArray* binary_array =
        checked_cast<const FixedSizeBinaryArray*>(array.get());

    int byte_width = binary_array->byte_width();
    auto ingest_one = [&, byte_width](R_xlen_t i) {
      auto value = binary_array->GetValue(i);
      SEXP raw = PROTECT(Rf_allocVector(RAWSXP, byte_width));
      std::copy(value, value + byte_width, RAW(raw));

      SET_VECTOR_ELT(data, i + start, raw);
      UNPROTECT(1);

      return Status::OK();
    };

    return IngestSome(array, n, ingest_one);
  }

 private:
  int byte_width_;
};

class Converter_Dictionary : public Converter {
 private:
  bool need_unification_;
  std::unique_ptr<arrow::DictionaryUnifier> unifier_;
  std::vector<std::shared_ptr<Buffer>> arrays_transpose_;
  std::shared_ptr<DataType> out_type_;
  std::shared_ptr<Array> dictionary_;

 public:
  explicit Converter_Dictionary(const ArrayVector& arrays)
      : Converter(arrays), need_unification_(NeedUnification()) {
    if (need_unification_) {
      const auto& arr_first = checked_cast<const DictionaryArray&>(*arrays[0]);
      const auto& arr_type = checked_cast<const DictionaryType&>(*arr_first.type());
      unifier_ = ValueOrStop(DictionaryUnifier::Make(arr_type.value_type()));

      size_t n_arrays = arrays.size();
      arrays_transpose_.resize(n_arrays);

      for (size_t i = 0; i < n_arrays; i++) {
        const auto& dict_i =
            *checked_cast<const DictionaryArray&>(*arrays[i]).dictionary();
        StopIfNotOk(unifier_->Unify(dict_i, &arrays_transpose_[i]));
      }

      StopIfNotOk(unifier_->GetResult(&out_type_, &dictionary_));
    } else {
      const auto& dict_array = checked_cast<const DictionaryArray&>(*arrays_[0]);

      auto indices = dict_array.indices();
      switch (indices->type_id()) {
        case Type::UINT8:
        case Type::INT8:
        case Type::UINT16:
        case Type::INT16:
        case Type::INT32:
          // TODO: also add int64, uint32, uint64 downcasts, if possible
          break;
        default:
          cpp11::stop("Cannot convert Dictionary Array of type `%s` to R",
                      dict_array.type()->ToString().c_str());
      }

      dictionary_ = dict_array.dictionary();
    }
  }

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::integers data(n);
    data.attr("levels") = GetLevels();
    if (GetOrdered()) {
      Rf_classgets(data, arrow::r::data::classes_ordered);
    } else {
      Rf_classgets(data, arrow::r::data::classes_factor);
    }
    return data;
  }

  virtual bool Parallel() const { return false; }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(INTEGER(data) + start, n, NA_INTEGER);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    const DictionaryArray& dict_array =
        checked_cast<const DictionaryArray&>(*array.get());
    auto indices = dict_array.indices();
    switch (indices->type_id()) {
      case Type::UINT8:
        return Ingest_some_nulls_Impl<arrow::UInt8Type>(data, array, start, n,
                                                        chunk_index);
      case Type::INT8:
        return Ingest_some_nulls_Impl<arrow::Int8Type>(data, array, start, n,
                                                       chunk_index);
      case Type::UINT16:
        return Ingest_some_nulls_Impl<arrow::UInt16Type>(data, array, start, n,
                                                         chunk_index);
      case Type::INT16:
        return Ingest_some_nulls_Impl<arrow::Int16Type>(data, array, start, n,
                                                        chunk_index);
      case Type::INT32:
        return Ingest_some_nulls_Impl<arrow::Int32Type>(data, array, start, n,
                                                        chunk_index);
      default:
        break;
    }
    return Status::OK();
  }

 private:
  template <typename Type>
  Status Ingest_some_nulls_Impl(SEXP data, const std::shared_ptr<arrow::Array>& array,
                                R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    using index_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
    auto indices = checked_cast<const DictionaryArray&>(*array).indices();
    auto raw_indices = indices->data()->GetValues<index_type>(1);

    auto p_data = INTEGER(data) + start;
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_INTEGER;
      return Status::OK();
    };

    // convert the 0-based indices from the arrow Array
    // to 1-based indices used in R factors
    if (need_unification_) {
      // transpose the indices before converting
      auto transposed =
          reinterpret_cast<const int32_t*>(arrays_transpose_[chunk_index]->data());

      auto ingest_one = [&](R_xlen_t i) {
        p_data[i] = transposed[raw_indices[i]] + 1;
        return Status::OK();
      };

      return IngestSome(array, n, ingest_one, null_one);
    } else {
      auto ingest_one = [&](R_xlen_t i) {
        p_data[i] = static_cast<int>(raw_indices[i]) + 1;
        return Status::OK();
      };
      return IngestSome(array, n, ingest_one, null_one);
    }
  }

  bool NeedUnification() {
    int n = arrays_.size();
    if (n < 2) {
      return false;
    }
    const auto& arr_first = checked_cast<const DictionaryArray&>(*arrays_[0]);
    for (int i = 1; i < n; i++) {
      const auto& arr = checked_cast<const DictionaryArray&>(*arrays_[i]);
      if (!(arr_first.dictionary()->Equals(arr.dictionary()))) {
        return true;
      }
    }
    return false;
  }

  bool GetOrdered() const {
    return checked_cast<const DictionaryArray&>(*arrays_[0]).dict_type()->ordered();
  }

  SEXP GetLevels() const {
    // R factor levels must be type "character" so coerce `dict` to STRSXP
    // TODO (npr): this coercion should be optional, "dictionariesAsFactors" ;)
    // Alternative: preserve the logical type of the dictionary values
    // (e.g. if dict is timestamp, return a POSIXt R vector, not factor)
    if (dictionary_->type_id() != Type::STRING) {
      cpp11::warning("Coercing dictionary values to R character factor levels");
    }

    SEXP vec = PROTECT(ArrayVector__as_vector(dictionary_->length(), dictionary_->type(),
                                              {dictionary_}));
    SEXP strings_vec = PROTECT(Rf_coerceVector(vec, STRSXP));
    UNPROTECT(2);
    return strings_vec;
  }
};

class Converter_Struct : public Converter {
 public:
  explicit Converter_Struct(const ArrayVector& arrays) : Converter(arrays), converters() {
    auto first_array = checked_cast<const arrow::StructArray*>(this->arrays_[0].get());
    int nf = first_array->num_fields();
    for (int i = 0; i < nf; i++) {
      converters.push_back(
          Converter::Make(first_array->field(i)->type(), {first_array->field(i)}));
    }
  }

  SEXP Allocate(R_xlen_t n) const {
    // allocate a data frame column to host each array
    auto first_array = checked_cast<const arrow::StructArray*>(this->arrays_[0].get());
    auto type = first_array->struct_type();
    auto out =
        arrow::r::to_r_list(converters, [n](const std::shared_ptr<Converter>& converter) {
          return converter->Allocate(n);
        });
    auto colnames = arrow::r::to_r_strings(
        type->fields(),
        [](const std::shared_ptr<Field>& field) { return field->name(); });
    out.attr(symbols::row_names) = arrow::r::short_row_names(n);
    out.attr(R_NamesSymbol) = colnames;
    out.attr(R_ClassSymbol) = arrow::r::data::classes_tbl_df;

    return out;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    int nf = converters.size();
    for (int i = 0; i < nf; i++) {
      StopIfNotOk(converters[i]->Ingest_all_nulls(VECTOR_ELT(data, i), start, n));
    }
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto struct_array = checked_cast<const arrow::StructArray*>(array.get());
    int nf = converters.size();
    // Flatten() deals with merging of nulls
    auto arrays = ValueOrStop(struct_array->Flatten(default_memory_pool()));
    for (int i = 0; i < nf; i++) {
      StopIfNotOk(converters[i]->Ingest_some_nulls(VECTOR_ELT(data, i), arrays[i], start,
                                                   n, chunk_index));
    }

    return Status::OK();
  }

 private:
  std::vector<std::shared_ptr<Converter>> converters;
};

double ms_to_seconds(int64_t ms) { return static_cast<double>(ms) / 1000; }

class Converter_Date64 : public Converter {
 public:
  explicit Converter_Date64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    Rf_classgets(data, arrow::r::data::classes_POSIXct);
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_data = REAL(data) + start;
    auto p_values = array->data()->GetValues<int64_t>(1);
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = static_cast<double>(p_values[i] / 1000);
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_REAL;
      return Status::OK();
    };
    return IngestSome(array, n, ingest_one, null_one);
  }
};

template <typename value_type, typename unit_type = TimeType>
class Converter_Time : public Converter {
 public:
  explicit Converter_Time(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    data.attr("class") = cpp11::writable::strings({"hms", "difftime"});

    // hms difftime is always stored as "seconds"
    data.attr("units") = cpp11::writable::strings({"secs"});
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    int multiplier = TimeUnit_multiplier(array);

    auto p_data = REAL(data) + start;
    auto p_values = array->data()->GetValues<value_type>(1);
    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = static_cast<double>(p_values[i]) / multiplier;
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_REAL;
      return Status::OK();
    };
    return IngestSome(array, n, ingest_one, null_one);
  }

 private:
  int TimeUnit_multiplier(const std::shared_ptr<Array>& array) const {
    // hms difftime is always "seconds", so multiply based on the Array's TimeUnit
    switch (static_cast<unit_type*>(array->type().get())->unit()) {
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
class Converter_Timestamp : public Converter_Time<value_type, TimestampType> {
 public:
  explicit Converter_Timestamp(const ArrayVector& arrays)
      : Converter_Time<value_type, TimestampType>(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    Rf_classgets(data, arrow::r::data::classes_POSIXct);
    auto array = checked_cast<const TimestampArray*>(this->arrays_[0].get());
    auto array_type = checked_cast<const TimestampType*>(array->type().get());
    std::string tzone = array_type->timezone();
    if (tzone.size() > 0) {
      data.attr("tzone") = tzone;
    }
    return data;
  }
};

class Converter_Decimal : public Converter {
 public:
  explicit Converter_Decimal(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(REALSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto p_data = REAL(data) + start;
    const auto& decimals_arr = checked_cast<const arrow::Decimal128Array&>(*array);

    auto ingest_one = [&](R_xlen_t i) {
      p_data[i] = std::stod(decimals_arr.FormatValue(i).c_str());
      return Status::OK();
    };
    auto null_one = [&](R_xlen_t i) {
      p_data[i] = NA_REAL;
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one, null_one);
  }
};

template <typename ListArrayType>
class Converter_List : public Converter {
 private:
  std::shared_ptr<arrow::DataType> value_type_;

 public:
  explicit Converter_List(const ArrayVector& arrays,
                          const std::shared_ptr<arrow::DataType>& value_type)
      : Converter(arrays), value_type_(value_type) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::list res(n);
    res.attr(R_ClassSymbol) = std::is_same<ListArrayType, ListArray>::value
                                  ? arrow::r::data::classes_arrow_list
                                  : arrow::r::data::classes_arrow_large_list;

    // Build an empty array to match value_type
    std::unique_ptr<arrow::ArrayBuilder> builder;
    StopIfNotOk(arrow::MakeBuilder(arrow::default_memory_pool(), value_type_, &builder));

    std::shared_ptr<arrow::Array> array;
    StopIfNotOk(builder->Finish(&array));

    // convert to an R object to store as the list' ptype
    res.attr(arrow::r::symbols::ptype) = Array__as_vector(array);

    return res;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    // nothing to do, list contain NULL by default
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto list_array = checked_cast<const ListArrayType*>(array.get());
    auto values_array = list_array->values();

    auto ingest_one = [&](R_xlen_t i) {
      auto slice = list_array->value_slice(i);
      SET_VECTOR_ELT(data, i + start, Array__as_vector(slice));
      return Status::OK();
    };

    return IngestSome(array, n, ingest_one);
  }

  bool Parallel() const { return false; }
};

class Converter_FixedSizeList : public Converter {
 private:
  std::shared_ptr<arrow::DataType> value_type_;
  int list_size_;

 public:
  explicit Converter_FixedSizeList(const ArrayVector& arrays,
                                   const std::shared_ptr<arrow::DataType>& value_type,
                                   int list_size)
      : Converter(arrays), value_type_(value_type), list_size_(list_size) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::list res(n);
    Rf_classgets(res, arrow::r::data::classes_arrow_fixed_size_list);
    res.attr(arrow::r::symbols::list_size) = Rf_ScalarInteger(list_size_);

    // Build an empty array to match value_type
    std::unique_ptr<arrow::ArrayBuilder> builder;
    StopIfNotOk(arrow::MakeBuilder(arrow::default_memory_pool(), value_type_, &builder));

    std::shared_ptr<arrow::Array> array;
    StopIfNotOk(builder->Finish(&array));

    // convert to an R object to store as the list' ptype
    res.attr(arrow::r::symbols::ptype) = Array__as_vector(array);

    return res;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    // nothing to do, list contain NULL by default
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    const auto& fixed_size_list_array = checked_cast<const FixedSizeListArray&>(*array);
    auto values_array = fixed_size_list_array.values();

    auto ingest_one = [&](R_xlen_t i) {
      auto slice = fixed_size_list_array.value_slice(i);
      SET_VECTOR_ELT(data, i + start, Array__as_vector(slice));
      return Status::OK();
    };
    return IngestSome(array, n, ingest_one);
  }

  bool Parallel() const { return false; }
};

class Converter_Int64 : public Converter {
 public:
  explicit Converter_Int64(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    data.attr("class") = "integer64";
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    auto p_data = reinterpret_cast<int64_t*>(REAL(data)) + start;
    std::fill_n(p_data, n, NA_INT64);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
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

class Converter_Null : public Converter {
 public:
  explicit Converter_Null(const ArrayVector& arrays) : Converter(arrays) {}

  SEXP Allocate(R_xlen_t n) const {
    SEXP data = PROTECT(Rf_allocVector(LGLSXP, n));
    std::fill_n(LOGICAL(data), n, NA_LOGICAL);
    Rf_classgets(data, Rf_mkString("vctrs_unspecified"));
    UNPROTECT(1);
    return data;
  }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    return Status::OK();
  }
};

bool ArraysCanFitInteger(ArrayVector arrays) {
  bool out = false;
  auto i32 = arrow::int32();
  for (const auto& array : arrays) {
    if (!out) {
      out = arrow::IntegersCanFit(arrow::Datum(array), *i32).ok();
    }
  }
  return out;
}

std::shared_ptr<Converter> Converter::Make(const std::shared_ptr<DataType>& type,
                                           ArrayVector arrays) {
  if (arrays.empty()) {
    // slight hack for the 0-row case since the converters expect at least one
    // chunk to process.
    arrays.push_back(ValueOrStop(arrow::MakeArrayOfNull(type, 0)));
  }

  switch (type->id()) {
    // direct support
    case Type::INT32:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int32Type>>(
          std::move(arrays));

    case Type::DOUBLE:
      return std::make_shared<arrow::r::Converter_Double<arrow::DoubleType>>(
          std::move(arrays));

      // need to handle 1-bit case
    case Type::BOOL:
      return std::make_shared<arrow::r::Converter_Boolean>(std::move(arrays));

    case Type::BINARY:
      return std::make_shared<arrow::r::Converter_Binary<arrow::BinaryArray>>(
          std::move(arrays));

    case Type::LARGE_BINARY:
      return std::make_shared<arrow::r::Converter_Binary<arrow::LargeBinaryArray>>(
          std::move(arrays));

    case Type::FIXED_SIZE_BINARY:
      return std::make_shared<arrow::r::Converter_FixedSizeBinary>(
          std::move(arrays),
          checked_cast<const FixedSizeBinaryType&>(*type).byte_width());

      // handle memory dense strings
    case Type::STRING:
      return std::make_shared<arrow::r::Converter_String<arrow::StringArray>>(
          std::move(arrays));

    case Type::LARGE_STRING:
      return std::make_shared<arrow::r::Converter_String<arrow::LargeStringArray>>(
          std::move(arrays));

    case Type::DICTIONARY:
      return std::make_shared<arrow::r::Converter_Dictionary>(std::move(arrays));

    case Type::DATE32:
      return std::make_shared<arrow::r::Converter_Date32>(std::move(arrays));

    case Type::DATE64:
      return std::make_shared<arrow::r::Converter_Date64>(std::move(arrays));

      // promotions to integer vector
    case Type::INT8:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int8Type>>(
          std::move(arrays));

    case Type::UINT8:
      return std::make_shared<arrow::r::Converter_Int<arrow::UInt8Type>>(
          std::move(arrays));

    case Type::INT16:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int16Type>>(
          std::move(arrays));

    case Type::UINT16:
      return std::make_shared<arrow::r::Converter_Int<arrow::UInt16Type>>(
          std::move(arrays));

      // promotions to numeric vector, if they don't fit into int32
    case Type::UINT32:
      if (ArraysCanFitInteger(arrays)) {
        return std::make_shared<arrow::r::Converter_Int<arrow::UInt32Type>>(
            std::move(arrays));
      } else {
        return std::make_shared<arrow::r::Converter_Double<arrow::UInt32Type>>(
            std::move(arrays));
      }

    case Type::UINT64:
      if (ArraysCanFitInteger(arrays)) {
        return std::make_shared<arrow::r::Converter_Int<arrow::UInt64Type>>(
            std::move(arrays));
      } else {
        return std::make_shared<arrow::r::Converter_Double<arrow::UInt64Type>>(
            std::move(arrays));
      }

    case Type::HALF_FLOAT:
      return std::make_shared<arrow::r::Converter_Double<arrow::HalfFloatType>>(
          std::move(arrays));

    case Type::FLOAT:
      return std::make_shared<arrow::r::Converter_Double<arrow::FloatType>>(
          std::move(arrays));

      // time32 and time64
    case Type::TIME32:
      return std::make_shared<arrow::r::Converter_Time<int32_t>>(std::move(arrays));

    case Type::TIME64:
      return std::make_shared<arrow::r::Converter_Time<int64_t>>(std::move(arrays));

    case Type::TIMESTAMP:
      return std::make_shared<arrow::r::Converter_Timestamp<int64_t>>(std::move(arrays));

    case Type::INT64:
      // Prefer integer if it fits
      if (ArraysCanFitInteger(arrays)) {
        return std::make_shared<arrow::r::Converter_Int<arrow::Int64Type>>(
            std::move(arrays));
      } else {
        return std::make_shared<arrow::r::Converter_Int64>(std::move(arrays));
      }

    case Type::DECIMAL:
      return std::make_shared<arrow::r::Converter_Decimal>(std::move(arrays));

      // nested
    case Type::STRUCT:
      return std::make_shared<arrow::r::Converter_Struct>(std::move(arrays));

    case Type::LIST:
      return std::make_shared<arrow::r::Converter_List<arrow::ListArray>>(
          std::move(arrays),
          checked_cast<const arrow::ListType*>(type.get())->value_type());

    case Type::LARGE_LIST:
      return std::make_shared<arrow::r::Converter_List<arrow::LargeListArray>>(
          std::move(arrays),
          checked_cast<const arrow::LargeListType*>(type.get())->value_type());

    case Type::FIXED_SIZE_LIST:
      return std::make_shared<arrow::r::Converter_FixedSizeList>(
          std::move(arrays),
          checked_cast<const arrow::FixedSizeListType&>(*type).value_type(),
          checked_cast<const arrow::FixedSizeListType&>(*type).list_size());

    case Type::NA:
      return std::make_shared<arrow::r::Converter_Null>(std::move(arrays));

    default:
      break;
  }

  cpp11::stop("cannot handle Array of type ", type->name().c_str());
}

cpp11::writable::list to_dataframe_serial(
    int64_t nr, int64_t nc, const cpp11::writable::strings& names,
    const std::vector<std::shared_ptr<Converter>>& converters) {
  cpp11::writable::list tbl(nc);
  for (int i = 0; i < nc; i++) {
    SEXP column = tbl[i] = converters[i]->Allocate(nr);
    StopIfNotOk(converters[i]->IngestSerial(column));
  }
  tbl.attr(R_NamesSymbol) = names;
  tbl.attr(R_ClassSymbol) = arrow::r::data::classes_tbl_df;
  tbl.attr(R_RowNamesSymbol) = arrow::r::short_row_names(nr);
  return tbl;
}

cpp11::writable::list to_dataframe_parallel(
    int64_t nr, int64_t nc, const cpp11::writable::strings& names,
    const std::vector<std::shared_ptr<Converter>>& converters) {
  cpp11::writable::list tbl(nc);

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

  StopIfNotOk(status);

  tbl.attr(R_NamesSymbol) = names;
  tbl.attr(R_ClassSymbol) = arrow::r::data::classes_tbl_df;
  tbl.attr(R_RowNamesSymbol) = arrow::r::short_row_names(nr);

  return tbl;
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  return arrow::r::ArrayVector__as_vector(array->length(), array->type(), {array});
}

// [[arrow::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array) {
  return arrow::r::ArrayVector__as_vector(chunked_array->length(), chunked_array->type(),
                                          chunked_array->chunks());
}

// [[arrow::export]]
cpp11::writable::list RecordBatch__to_dataframe(
    const std::shared_ptr<arrow::RecordBatch>& batch, bool use_threads) {
  int64_t nc = batch->num_columns();
  int64_t nr = batch->num_rows();
  cpp11::writable::strings names(nc);
  std::vector<arrow::ArrayVector> arrays(nc);
  std::vector<std::shared_ptr<arrow::r::Converter>> converters(nc);

  for (R_xlen_t i = 0; i < nc; i++) {
    names[i] = batch->column_name(i);
    arrays[i] = {batch->column(i)};
    converters[i] = arrow::r::Converter::Make(batch->column(i)->type(), arrays[i]);
  }

  if (use_threads) {
    return arrow::r::to_dataframe_parallel(nr, nc, names, converters);
  } else {
    return arrow::r::to_dataframe_serial(nr, nc, names, converters);
  }
}

// [[arrow::export]]
cpp11::writable::list Table__to_dataframe(const std::shared_ptr<arrow::Table>& table,
                                          bool use_threads) {
  int64_t nc = table->num_columns();
  int64_t nr = table->num_rows();
  cpp11::writable::strings names(nc);
  std::vector<std::shared_ptr<arrow::r::Converter>> converters(nc);

  for (R_xlen_t i = 0; i < nc; i++) {
    converters[i] =
        arrow::r::Converter::Make(table->column(i)->type(), table->column(i)->chunks());
    names[i] = table->field(i)->name();
  }

  if (use_threads) {
    return arrow::r::to_dataframe_parallel(nr, nc, names, converters);
  } else {
    return arrow::r::to_dataframe_serial(nr, nc, names, converters);
  }
}

#endif
