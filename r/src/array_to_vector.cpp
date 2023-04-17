// Licensed to the Apache Software Foundation (ASF) under one
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// or more contributor license agreements.  See the NOTICE file
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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/datum.h>
#include <arrow/table.h>
#include <arrow/util/bitmap_reader.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/util/int_util.h>

#include <cpp11/altrep.hpp>
#include <type_traits>

#include "./extension.h"
#include "./r_task_group.h"

namespace arrow {

using internal::checked_cast;
using internal::IntegersCanFit;

namespace r {

class Converter {
 public:
  explicit Converter(const std::shared_ptr<ChunkedArray>& chunked_array)
      : chunked_array_(std::move(chunked_array)) {}

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

  // can this run in parallel ?
  virtual bool Parallel() const { return true; }

  // converter is passed as self to outlive the scope of Converter::Convert()
  SEXP ScheduleConvertTasks(RTasks& tasks, std::shared_ptr<Converter> self) {
    // try altrep first
    SEXP alt = altrep::MakeAltrepVector(chunked_array_);
    if (!Rf_isNull(alt)) {
      return alt;
    }

    // otherwise use the Converter api:

    // allocating the R vector upfront
    SEXP out = PROTECT(Allocate(chunked_array_->length()));

    // for each array, fill the relevant slice of `out`, potentially in parallel
    R_xlen_t k = 0, i = 0;
    for (const auto& array : chunked_array_->chunks()) {
      auto n_chunk = array->length();

      tasks.Append(Parallel(), [=] {
        if (array->null_count() == n_chunk) {
          return self->Ingest_all_nulls(out, k, n_chunk);
        } else {
          return self->Ingest_some_nulls(out, array, k, n_chunk, i);
        }
      });

      k += n_chunk;
      i++;
    }

    UNPROTECT(1);
    return out;
  }

  // Converter factory
  static std::shared_ptr<Converter> Make(
      const std::shared_ptr<ChunkedArray>& chunked_array);

  static SEXP LazyConvert(const std::shared_ptr<ChunkedArray>& chunked_array,
                          RTasks& tasks) {
    auto converter = Make(chunked_array);
    return converter->ScheduleConvertTasks(tasks, converter);
  }

  static SEXP Convert(const std::shared_ptr<ChunkedArray>& chunked_array,
                      bool use_threads) {
    RTasks tasks(use_threads);
    SEXP out = PROTECT(Converter::LazyConvert(chunked_array, tasks));
    StopIfNotOk(tasks.Finish());

    UNPROTECT(1);
    return out;
  }

  static SEXP Convert(const std::shared_ptr<Array>& array) {
    return Convert(std::make_shared<ChunkedArray>(array), false);
  }

  SEXP MaybeAltrep() { return altrep::MakeAltrepVector(chunked_array_); }

 protected:
  std::shared_ptr<ChunkedArray> chunked_array_;
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

std::shared_ptr<Array> CreateEmptyArray(const std::shared_ptr<DataType>& array_type) {
  std::unique_ptr<arrow::ArrayBuilder> builder;
  StopIfNotOk(arrow::MakeBuilder(gc_memory_pool(), array_type, &builder));

  std::shared_ptr<arrow::Array> array;
  StopIfNotOk(builder->Finish(&array));
  return array;
}

template <typename Type>
class Converter_Int : public Converter {
  using value_type = typename TypeTraits<Type>::ArrayType::value_type;

 public:
  explicit Converter_Int(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_Double(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_Date32(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_String(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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

    const bool all_valid = array->null_count() == 0;
    const bool strip_out_nuls = GetBoolOption("arrow.skip_nul", false);

    bool nul_was_stripped = false;

    if (all_valid) {
      // no need to watch for missing strings
      cpp11::unwind_protect([&] {
        if (strip_out_nuls) {
          for (int i = 0; i < n; i++) {
            SET_STRING_ELT(data, start + i,
                           r_string_from_view_strip_nul(string_array->GetView(i),
                                                        &nul_was_stripped));
          }
          return;
        }

        for (int i = 0; i < n; i++) {
          SET_STRING_ELT(data, start + i, r_string_from_view(string_array->GetView(i)));
        }
      });
    } else {
      cpp11::unwind_protect([&] {
        arrow::internal::BitmapReader validity_reader(array->null_bitmap_data(),
                                                      array->offset(), n);

        if (strip_out_nuls) {
          for (int i = 0; i < n; i++, validity_reader.Next()) {
            if (validity_reader.IsSet()) {
              SET_STRING_ELT(data, start + i,
                             r_string_from_view_strip_nul(string_array->GetView(i),
                                                          &nul_was_stripped));
            } else {
              SET_STRING_ELT(data, start + i, NA_STRING);
            }
          }
          return;
        }

        for (int i = 0; i < n; i++, validity_reader.Next()) {
          if (validity_reader.IsSet()) {
            SET_STRING_ELT(data, start + i, r_string_from_view(string_array->GetView(i)));
          } else {
            SET_STRING_ELT(data, start + i, NA_STRING);
          }
        }
      });
    }

    if (nul_was_stripped) {
      cpp11::safe[Rf_warning]("Stripping '\\0' (nul) from character vector");
    }

    return Status::OK();
  }

  bool Parallel() const { return false; }

 private:
  static SEXP r_string_from_view(std::string_view view) {
    return Rf_mkCharLenCE(view.data(), view.size(), CE_UTF8);
  }

  static SEXP r_string_from_view_strip_nul(std::string_view view,
                                           bool* nul_was_stripped) {
    const char* old_string = view.data();

    std::string stripped_string;
    size_t stripped_len = 0, nul_count = 0;

    for (size_t i = 0; i < view.size(); i++) {
      if (old_string[i] == '\0') {
        ++nul_count;

        if (nul_count == 1) {
          // first nul spotted: allocate stripped string storage
          stripped_string = std::string(view);
          stripped_len = i;
        }

        // don't copy old_string[i] (which is \0) into stripped_string
        continue;
      }

      if (nul_count > 0) {
        stripped_string[stripped_len++] = old_string[i];
      }
    }

    if (nul_count > 0) {
      *nul_was_stripped = true;
      stripped_string.resize(stripped_len);
      return r_string_from_view(stripped_string);
    }

    return r_string_from_view(view);
  }
};

class Converter_Boolean : public Converter {
 public:
  explicit Converter_Boolean(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_Binary(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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

  virtual bool Parallel() const { return false; }
};

class Converter_FixedSizeBinary : public Converter {
 public:
  explicit Converter_FixedSizeBinary(const std::shared_ptr<ChunkedArray>& chunked_array,
                                     int byte_width)
      : Converter(chunked_array), byte_width_(byte_width) {}

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

  virtual bool Parallel() const { return false; }

 private:
  int byte_width_;
};

bool DictionaryChunkArrayNeedUnification(
    const std::shared_ptr<ChunkedArray>& chunked_array) {
  int n = chunked_array->num_chunks();
  if (n < 2) {
    return false;
  }
  const auto& arr_first =
      internal::checked_cast<const DictionaryArray&>(*chunked_array->chunk(0));
  for (int i = 1; i < n; i++) {
    const auto& arr =
        internal::checked_cast<const DictionaryArray&>(*chunked_array->chunk(i));
    if (!(arr_first.dictionary()->Equals(arr.dictionary()))) {
      return true;
    }
  }
  return false;
}

class Converter_Dictionary : public Converter {
 private:
  bool need_unification_;
  std::unique_ptr<arrow::DictionaryUnifier> unifier_;
  std::vector<std::shared_ptr<Buffer>> arrays_transpose_;
  std::shared_ptr<DataType> out_type_;
  std::shared_ptr<Array> dictionary_;

 public:
  explicit Converter_Dictionary(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array),
        need_unification_(DictionaryChunkArrayNeedUnification(chunked_array)) {
    if (need_unification_) {
      const auto& arr_type = checked_cast<const DictionaryType&>(*chunked_array->type());
      unifier_ = ValueOrStop(DictionaryUnifier::Make(arr_type.value_type()));

      size_t n_arrays = chunked_array->num_chunks();
      arrays_transpose_.resize(n_arrays);

      for (size_t i = 0; i < n_arrays; i++) {
        const auto& dict_i =
            *checked_cast<const DictionaryArray&>(*chunked_array->chunk(i)).dictionary();
        StopIfNotOk(unifier_->Unify(dict_i, &arrays_transpose_[i]));
      }

      StopIfNotOk(unifier_->GetResult(&out_type_, &dictionary_));
    } else {
      const auto& dict_type = checked_cast<const DictionaryType&>(*chunked_array->type());

      const auto& indices_type = *dict_type.index_type();
      switch (indices_type.id()) {
        case Type::UINT8:
        case Type::INT8:
        case Type::UINT16:
        case Type::INT16:
        case Type::INT32:
          // TODO: also add int64, uint32, uint64 downcasts, if possible
          break;
        default:
          cpp11::stop("Cannot convert Dictionary Array of type `%s` to R",
                      dict_type.ToString().c_str());
      }

      if (chunked_array->num_chunks() > 0) {
        // DictionaryChunkArrayNeedUnification() returned false so we can safely assume
        // the dictionary of the first chunk applies everywhere
        const auto& dict_array =
            checked_cast<const DictionaryArray&>(*chunked_array->chunk(0));
        dictionary_ = dict_array.dictionary();
      } else {
        dictionary_ = CreateEmptyArray(dict_type.value_type());
      }
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

  bool GetOrdered() const {
    return checked_cast<const DictionaryType&>(*chunked_array_->type()).ordered();
  }

  SEXP GetLevels() const {
    // R factor levels must be type "character" so coerce `dict` to STRSXP
    // TODO (npr): this coercion should be optional, "dictionariesAsFactors" ;)
    // Alternative: preserve the logical type of the dictionary values
    // (e.g. if dict is timestamp, return a POSIXt R vector, not factor)
    if (dictionary_->type_id() != Type::STRING) {
      cpp11::safe[Rf_warning]("Coercing dictionary values to R character factor levels");
    }

    SEXP vec = PROTECT(Converter::Convert(dictionary_));
    SEXP strings_vec = PROTECT(Rf_coerceVector(vec, STRSXP));
    UNPROTECT(2);
    return strings_vec;
  }
};

class Converter_Struct : public Converter {
 public:
  explicit Converter_Struct(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array), converters() {
    const auto& struct_type =
        checked_cast<const arrow::StructType&>(*chunked_array->type());

    int nf = struct_type.num_fields();

    std::shared_ptr<arrow::Table> array_as_table =
        ValueOrStop(arrow::Table::FromChunkedStructArray(chunked_array));
    for (int i = 0; i < nf; i++) {
      converters.push_back(Converter::Make(array_as_table->column(i)));
    }
  }

  SEXP Allocate(R_xlen_t n) const {
    // allocate a data frame column to host each array
    // If possible, a column is dealt with directly with altrep
    auto type =
        checked_cast<const arrow::StructType*>(this->chunked_array_->type().get());
    auto out =
        arrow::r::to_r_list(converters, [n](const std::shared_ptr<Converter>& converter) {
          SEXP out = converter->MaybeAltrep();
          if (Rf_isNull(out)) {
            out = converter->Allocate(n);
          }
          return out;
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
      SEXP data_i = VECTOR_ELT(data, i);

      // only ingest if the column is not altrep
      if (!altrep::is_unmaterialized_arrow_altrep(data_i)) {
        StopIfNotOk(converters[i]->Ingest_all_nulls(data_i, start, n));
      }
    }
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    auto struct_array = checked_cast<const arrow::StructArray*>(array.get());
    int nf = converters.size();
    // Flatten() deals with merging of nulls
    auto arrays = ValueOrStop(struct_array->Flatten(gc_memory_pool()));
    for (int i = 0; i < nf; i++) {
      SEXP data_i = VECTOR_ELT(data, i);

      // only ingest if the column is not altrep
      if (!altrep::is_unmaterialized_arrow_altrep(data_i)) {
        StopIfNotOk(converters[i]->Ingest_some_nulls(VECTOR_ELT(data, i), arrays[i],
                                                     start, n, chunk_index));
      }
    }

    return Status::OK();
  }

  virtual bool Parallel() const {
    // this can only run in parallel if all the
    // inner converters can
    for (const auto& converter : converters) {
      if (!converter->Parallel()) return false;
    }
    return true;
  }

 private:
  std::vector<std::shared_ptr<Converter>> converters;
};

double ms_to_seconds(int64_t ms) { return static_cast<double>(ms) / 1000; }

class Converter_Date64 : public Converter {
 public:
  explicit Converter_Date64(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_Time(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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

template <typename value_type, typename unit_type = DurationType>
class Converter_Duration : public Converter {
 public:
  explicit Converter_Duration(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    data.attr("class") = "difftime";

    // difftime is always stored as "seconds"
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
    // difftime is always "seconds", so multiply based on the Array's TimeUnit
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
  explicit Converter_Timestamp(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter_Time<value_type, TimestampType>(chunked_array) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::doubles data(n);
    Rf_classgets(data, arrow::r::data::classes_POSIXct);
    auto array_type =
        checked_cast<const TimestampType*>(this->chunked_array_->type().get());
    std::string tzone = array_type->timezone();
    if (tzone.size() > 0) {
      data.attr("tzone") = tzone;
    }
    return data;
  }
};

template <typename Type>
class Converter_Decimal : public Converter {
 public:
  explicit Converter_Decimal(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

  SEXP Allocate(R_xlen_t n) const { return Rf_allocVector(REALSXP, n); }

  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    std::fill_n(REAL(data) + start, n, NA_REAL);
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    using DecimalArray = typename TypeTraits<Type>::ArrayType;
    auto p_data = REAL(data) + start;
    const auto& decimals_arr = checked_cast<const DecimalArray&>(*array);

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
  explicit Converter_List(const std::shared_ptr<ChunkedArray>& chunked_array,
                          const std::shared_ptr<arrow::DataType>& value_type)
      : Converter(chunked_array), value_type_(value_type) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::list res(n);

    if (std::is_same<ListArrayType, MapArray>::value) {
      res.attr(R_ClassSymbol) = arrow::r::data::classes_arrow_list;
    } else if (std::is_same<ListArrayType, ListArray>::value) {
      res.attr(R_ClassSymbol) = arrow::r::data::classes_arrow_list;
    } else {
      res.attr(R_ClassSymbol) = arrow::r::data::classes_arrow_large_list;
    }

    std::shared_ptr<arrow::Array> array = CreateEmptyArray(value_type_);

    // convert to an R object to store as the list' ptype
    res.attr(arrow::r::symbols::ptype) = Converter::Convert(array);

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
      SET_VECTOR_ELT(data, i + start, Converter::Convert(slice));
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
  explicit Converter_FixedSizeList(const std::shared_ptr<ChunkedArray>& chunked_array,
                                   const std::shared_ptr<arrow::DataType>& value_type,
                                   int list_size)
      : Converter(chunked_array), value_type_(value_type), list_size_(list_size) {}

  SEXP Allocate(R_xlen_t n) const {
    cpp11::writable::list res(n);
    Rf_classgets(res, arrow::r::data::classes_arrow_fixed_size_list);
    res.attr(arrow::r::symbols::list_size) = Rf_ScalarInteger(list_size_);

    std::shared_ptr<arrow::Array> array = CreateEmptyArray(value_type_);

    // convert to an R object to store as the list' ptype
    res.attr(arrow::r::symbols::ptype) = Converter::Convert(array);

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
      SET_VECTOR_ELT(data, i + start, Converter::Convert(slice));
      return Status::OK();
    };
    return IngestSome(array, n, ingest_one);
  }

  bool Parallel() const { return false; }
};

class Converter_Int64 : public Converter {
 public:
  explicit Converter_Int64(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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
  explicit Converter_Null(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

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

// Unlike other types, conversion of ExtensionType (chunked) arrays occurs at
// R level via the ExtensionType (or subclass) R6 instance. We do this via Allocate,
// since it is called once per ChunkedArray.
class Converter_Extension : public Converter {
 public:
  explicit Converter_Extension(const std::shared_ptr<ChunkedArray>& chunked_array)
      : Converter(chunked_array) {}

  SEXP Allocate(R_xlen_t n) const {
    auto extension_type =
        dynamic_cast<const RExtensionType*>(chunked_array_->type().get());
    if (extension_type == nullptr) {
      Rf_error("Converter_Extension can't be used with a non-R extension type");
    }

    return extension_type->Convert(chunked_array_);
  }

  // At this point we have already done the conversion
  Status Ingest_all_nulls(SEXP data, R_xlen_t start, R_xlen_t n) const {
    return Status::OK();
  }

  Status Ingest_some_nulls(SEXP data, const std::shared_ptr<arrow::Array>& array,
                           R_xlen_t start, R_xlen_t n, size_t chunk_index) const {
    return Status::OK();
  }
};

bool ArraysCanFitInteger(ArrayVector arrays) {
  bool all_can_fit = true;
  auto i32 = arrow::int32();
  for (const auto& array : arrays) {
    if (all_can_fit) {
      all_can_fit = arrow::IntegersCanFit(*array->data(), *i32).ok();
    }
  }
  return all_can_fit;
}

bool GetBoolOption(const std::string& name, bool default_) {
  SEXP getOption = Rf_install("getOption");
  cpp11::sexp call = Rf_lang2(getOption, Rf_mkString(name.c_str()));
  cpp11::sexp res = Rf_eval(call, R_BaseEnv);
  if (TYPEOF(res) == LGLSXP) {
    return LOGICAL(res)[0] == TRUE;
  } else {
    return default_;
  }
}

std::shared_ptr<Converter> Converter::Make(
    const std::shared_ptr<ChunkedArray>& chunked_array) {
  const auto& type = chunked_array->type();
  switch (type->id()) {
    // direct support
    case Type::INT32:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int32Type>>(chunked_array);

    case Type::DOUBLE:
      return std::make_shared<arrow::r::Converter_Double<arrow::DoubleType>>(
          chunked_array);

      // need to handle 1-bit case
    case Type::BOOL:
      return std::make_shared<arrow::r::Converter_Boolean>(chunked_array);

    case Type::BINARY:
      return std::make_shared<arrow::r::Converter_Binary<arrow::BinaryArray>>(
          chunked_array);

    case Type::LARGE_BINARY:
      return std::make_shared<arrow::r::Converter_Binary<arrow::LargeBinaryArray>>(
          chunked_array);

    case Type::FIXED_SIZE_BINARY:
      return std::make_shared<arrow::r::Converter_FixedSizeBinary>(
          chunked_array, checked_cast<const FixedSizeBinaryType&>(*type).byte_width());

      // handle memory dense strings
    case Type::STRING:
      return std::make_shared<arrow::r::Converter_String<arrow::StringArray>>(
          chunked_array);

    case Type::LARGE_STRING:
      return std::make_shared<arrow::r::Converter_String<arrow::LargeStringArray>>(
          chunked_array);

    case Type::DICTIONARY:
      return std::make_shared<arrow::r::Converter_Dictionary>(chunked_array);

    case Type::DATE32:
      return std::make_shared<arrow::r::Converter_Date32>(chunked_array);

    case Type::DATE64:
      return std::make_shared<arrow::r::Converter_Date64>(chunked_array);

      // promotions to integer vector
    case Type::INT8:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int8Type>>(chunked_array);

    case Type::UINT8:
      return std::make_shared<arrow::r::Converter_Int<arrow::UInt8Type>>(chunked_array);

    case Type::INT16:
      return std::make_shared<arrow::r::Converter_Int<arrow::Int16Type>>(chunked_array);

    case Type::UINT16:
      return std::make_shared<arrow::r::Converter_Int<arrow::UInt16Type>>(chunked_array);

      // promotions to numeric vector, if they don't fit into int32
    case Type::UINT32:
      if (ArraysCanFitInteger(chunked_array->chunks())) {
        return std::make_shared<arrow::r::Converter_Int<arrow::UInt32Type>>(
            chunked_array);
      } else {
        return std::make_shared<arrow::r::Converter_Double<arrow::UInt32Type>>(
            chunked_array);
      }

    case Type::UINT64:
      if (ArraysCanFitInteger(chunked_array->chunks())) {
        return std::make_shared<arrow::r::Converter_Int<arrow::UInt64Type>>(
            chunked_array);
      } else {
        return std::make_shared<arrow::r::Converter_Double<arrow::UInt64Type>>(
            chunked_array);
      }

    case Type::HALF_FLOAT:
      return std::make_shared<arrow::r::Converter_Double<arrow::HalfFloatType>>(
          chunked_array);

    case Type::FLOAT:
      return std::make_shared<arrow::r::Converter_Double<arrow::FloatType>>(
          chunked_array);

      // time32 and time64
    case Type::TIME32:
      return std::make_shared<arrow::r::Converter_Time<int32_t>>(chunked_array);

    case Type::TIME64:
      return std::make_shared<arrow::r::Converter_Time<int64_t>>(chunked_array);

    case Type::DURATION:
      return std::make_shared<arrow::r::Converter_Duration<int64_t>>(chunked_array);

    case Type::TIMESTAMP:
      return std::make_shared<arrow::r::Converter_Timestamp<int64_t>>(chunked_array);

    case Type::INT64:
      // Prefer integer if it fits, unless option arrow.int64_downcast is `false`
      if (GetBoolOption("arrow.int64_downcast", true) &&
          ArraysCanFitInteger(chunked_array->chunks())) {
        return std::make_shared<arrow::r::Converter_Int<arrow::Int64Type>>(chunked_array);
      } else {
        return std::make_shared<arrow::r::Converter_Int64>(chunked_array);
      }

    case Type::DECIMAL128:
      return std::make_shared<arrow::r::Converter_Decimal<Decimal128Type>>(chunked_array);

    case Type::DECIMAL256:
      return std::make_shared<arrow::r::Converter_Decimal<Decimal256Type>>(chunked_array);

      // nested
    case Type::STRUCT:
      return std::make_shared<arrow::r::Converter_Struct>(chunked_array);

    case Type::LIST:
      return std::make_shared<arrow::r::Converter_List<arrow::ListArray>>(
          chunked_array, checked_cast<const arrow::ListType*>(type.get())->value_type());

    case Type::LARGE_LIST:
      return std::make_shared<arrow::r::Converter_List<arrow::LargeListArray>>(
          chunked_array,
          checked_cast<const arrow::LargeListType*>(type.get())->value_type());

    case Type::FIXED_SIZE_LIST:
      return std::make_shared<arrow::r::Converter_FixedSizeList>(
          chunked_array,
          checked_cast<const arrow::FixedSizeListType&>(*type).value_type(),
          checked_cast<const arrow::FixedSizeListType&>(*type).list_size());

    case Type::MAP:
      return std::make_shared<arrow::r::Converter_List<arrow::MapArray>>(
          chunked_array, checked_cast<const arrow::MapType&>(*type).value_type());

    case Type::NA:
      return std::make_shared<arrow::r::Converter_Null>(chunked_array);

    case Type::EXTENSION:
      return std::make_shared<arrow::r::Converter_Extension>(chunked_array);

    default:
      break;
  }

  cpp11::stop("cannot handle Array of type <%s>", type->name().c_str());
}

std::shared_ptr<ChunkedArray> to_chunks(const std::shared_ptr<Array>& array) {
  return std::make_shared<ChunkedArray>(array);
}

std::shared_ptr<ChunkedArray> to_chunks(
    const std::shared_ptr<ChunkedArray>& chunked_array) {
  return chunked_array;
}

template <typename Rectangle>
cpp11::writable::list to_data_frame(const std::shared_ptr<Rectangle>& data,
                                    bool use_threads) {
  int64_t nc = data->num_columns();
  int64_t nr = data->num_rows();
  cpp11::writable::strings names(nc);

  arrow::r::RTasks tasks(use_threads);

  cpp11::writable::list tbl(nc);

  for (int i = 0; i < nc; i++) {
    names[i] = data->schema()->field(i)->name();
    tbl[i] = Converter::LazyConvert(to_chunks(data->column(i)), tasks);
  }

  StopIfNotOk(tasks.Finish());

  tbl.attr(R_NamesSymbol) = names;
  tbl.attr(R_ClassSymbol) = arrow::r::data::classes_tbl_df;
  tbl.attr(R_RowNamesSymbol) = arrow::r::short_row_names(nr);

  return tbl;
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array) {
  return arrow::r::Converter::Convert(array);
}

// [[arrow::export]]
SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
                             bool use_threads = false) {
  return arrow::r::Converter::Convert(chunked_array, use_threads);
}

// [[arrow::export]]
cpp11::writable::list RecordBatch__to_dataframe(
    const std::shared_ptr<arrow::RecordBatch>& batch, bool use_threads) {
  return arrow::r::to_data_frame(batch, use_threads);
}

// [[arrow::export]]
cpp11::writable::list Table__to_dataframe(const std::shared_ptr<arrow::Table>& table,
                                          bool use_threads) {
  return arrow::r::to_data_frame(table, use_threads);
}
