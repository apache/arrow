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
#include <arrow/chunked_array.h>
#include <arrow/compute/api.h>
#include <arrow/util/bitmap_reader.h>

#include <cpp11/altrep.hpp>
#include <cpp11/declarations.hpp>
#if defined(HAS_ALTREP)

#if R_VERSION < R_Version(3, 6, 0)

// workaround because R's <R_ext/Altrep.h> not so conveniently uses `class`
// as a variable name, and C++ is not happy about that
//
// SEXP R_new_altrep(R_altrep_class_t class, SEXP data1, SEXP data2);
//
#define class klass

// Because functions declared in <R_ext/Altrep.h> have C linkage
extern "C" {
#include <R_ext/Altrep.h>
}

// undo the workaround
#undef class

#else
#include <R_ext/Altrep.h>
#endif

#include "./r_task_group.h"

namespace arrow {
namespace r {
namespace altrep {

namespace {
template <typename c_type>
R_xlen_t Standard_Get_region(SEXP data2, R_xlen_t i, R_xlen_t n, c_type* buf);

template <>
R_xlen_t Standard_Get_region<double>(SEXP data2, R_xlen_t i, R_xlen_t n, double* buf) {
  return REAL_GET_REGION(data2, i, n, buf);
}

template <>
R_xlen_t Standard_Get_region<int>(SEXP data2, R_xlen_t i, R_xlen_t n, int* buf) {
  return INTEGER_GET_REGION(data2, i, n, buf);
}

void DeleteChunkedArray(std::shared_ptr<ChunkedArray>* ptr) { delete ptr; }
using Pointer =
    cpp11::external_pointer<std::shared_ptr<ChunkedArray>, DeleteChunkedArray>;

// the ChunkedArray that is being wrapped by the altrep object
const std::shared_ptr<ChunkedArray>& GetChunkedArray(SEXP alt) {
  return *Pointer(R_altrep_data1(alt));
}

struct ArrayResolve {
  // TODO: ARROW-11989
  ArrayResolve(const std::shared_ptr<ChunkedArray>& chunked_array, int64_t i) {
    for (int idx_chunk = 0; idx_chunk < chunked_array->num_chunks(); idx_chunk++) {
      std::shared_ptr<Array> chunk = chunked_array->chunk(idx_chunk);
      auto chunk_size = chunk->length();
      if (i < chunk_size) {
        index_ = i;
        array_ = chunk;
        break;
      }

      i -= chunk_size;
    }
  }

  std::shared_ptr<Array> array_;
  int64_t index_ = 0;
};

// base class for all altrep vectors
//
// data1: the Array as an external pointer.
// data2: starts as NULL, and becomes a standard R vector with the same
//        data if necessary: if materialization is needed, e.g. if we need
//        to access its data pointer, with DATAPTR().
template <typename Impl>
struct AltrepVectorBase {
  // store the Array as an external pointer in data1, mark as immutable
  static SEXP Make(const std::shared_ptr<ChunkedArray>& chunked_array) {
    SEXP alt = R_new_altrep(Impl::class_t,
                            Pointer(new std::shared_ptr<ChunkedArray>(chunked_array)),
                            R_NilValue);
    MARK_NOT_MUTABLE(alt);

    return alt;
  }

  // Is the vector materialized, i.e. does the data2 slot contain a
  // standard R vector with the same data as the array.
  static bool IsMaterialized(SEXP alt) { return !Rf_isNull(R_altrep_data2(alt)); }

  static R_xlen_t Length(SEXP alt) { return GetChunkedArray(alt)->length(); }

  static int No_NA(SEXP alt) { return GetChunkedArray(alt)->null_count() == 0; }

  static int Is_sorted(SEXP alt) { return UNKNOWN_SORTEDNESS; }

  // What gets printed on .Internal(inspect(<the altrep object>))
  static Rboolean Inspect(SEXP alt, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& chunked_array = GetChunkedArray(alt);
    Rprintf("arrow::ChunkedArray<%p, %s, %d chunks, %d nulls> len=%d\n",
            chunked_array.get(), chunked_array->type()->ToString().c_str(),
            chunked_array->num_chunks(), chunked_array->null_count(),
            chunked_array->length());
    return TRUE;
  }

  // Duplication is done by first materializing the vector and
  // then make a lazy duplicate of data2
  static SEXP Duplicate(SEXP alt, Rboolean /* deep */) {
    return Rf_lazy_duplicate(Impl::Materialize(alt));
  }

  static SEXP Coerce(SEXP alt, int type) {
    return Rf_coerceVector(Impl::Materialize(alt), type);
  }

  static SEXP Serialized_state(SEXP alt) { return Impl::Materialize(alt); }

  static SEXP Unserialize(SEXP /* class_ */, SEXP state) { return state; }
};

// altrep R vector shadowing an primitive (int or double) Array.
//
// This tries as much as possible to directly use the data
// from the Array and minimize data copies.
template <int sexp_type>
struct AltrepVectorPrimitive : public AltrepVectorBase<AltrepVectorPrimitive<sexp_type>> {
  using Base = AltrepVectorBase<AltrepVectorPrimitive<sexp_type>>;

  // singleton altrep class description
  static R_altrep_class_t class_t;

  using c_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  // Force materialization. After calling this, the data2 slot of the altrep
  // object contains a standard R vector with the same data, with
  // R sentinels where the Array has nulls.
  //
  // The Array remains available so that it can be used by Length(), Min(), etc ...
  static SEXP Materialize(SEXP alt) {
    if (!Base::IsMaterialized(alt)) {
      auto size = Base::Length(alt);

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(sexp_type, size));

      // copy the data from the array, through Get_region
      Get_region(alt, 0, size, reinterpret_cast<c_type*>(DATAPTR(copy)));

      // store as data2, this is now considered materialized
      R_set_altrep_data2(alt, copy);
      MARK_NOT_MUTABLE(copy);

      UNPROTECT(1);
    }
    return R_altrep_data2(alt);
  }

  // R calls this to get a pointer to the start of the vector data
  // but only if this is possible without allocating (in the R sense).
  static const void* Dataptr_or_null(SEXP alt) {
    // data2 has been created, and so the R sentinels are in place where the array has
    // nulls
    if (Base::IsMaterialized(alt)) {
      return DATAPTR_RO(R_altrep_data2(alt));
    }

    // there is only one chunk with no nulls, we can directly return the start of its data
    auto chunked_array = GetChunkedArray(alt);
    if (chunked_array->num_chunks() == 1 && chunked_array->null_count() == 0) {
      return reinterpret_cast<const void*>(
          chunked_array->chunk(0)->data()->template GetValues<c_type>(1));
    }

    // Otherwise: if the array has nulls and data2 has not been generated: give up
    return nullptr;
  }

  // R calls this to get a pointer to the start of the data, R allocations are allowed.
  static void* Dataptr(SEXP alt, Rboolean writeable) {
    // If the object hasn't been materialized, and the array has no
    // nulls we can directly point to the array data.
    if (!Base::IsMaterialized(alt)) {
      const auto& chunked_array = GetChunkedArray(alt);

      if (chunked_array->num_chunks() == 1 && chunked_array->null_count() == 0) {
        return reinterpret_cast<void*>(const_cast<c_type*>(
            chunked_array->chunk(0)->data()->template GetValues<c_type>(1)));
      }
    }

    // Otherwise we have to materialize and hand the pointer to data2
    //
    // NOTE: this returns the DATAPTR() of data2 even in the case writeable = TRUE
    //
    // which is risky because C(++) clients of this object might
    // modify data2, and therefore make it diverge from the data of the Array,
    // but the object was marked as immutable on creation, so doing this is
    // disregarding the R api.
    //
    // Simply stop() when `writeable = TRUE` is too strong, e.g. this fails
    // identical() which calls DATAPTR() even though DATAPTR_RO() would
    // be enough
    return DATAPTR(Materialize(alt));
  }

  // The value at position i
  static c_type Elt(SEXP alt, R_xlen_t i) {
    ArrayResolve resolve(GetChunkedArray(alt), i);
    auto array = resolve.array_;
    auto j = resolve.index_;

    return array->IsNull(j) ? cpp11::na<c_type>()
                            : array->data()->template GetValues<c_type>(1)[j];
  }

  // R calls this when it wants data from position `i` to `i + n` copied into `buf`
  // The returned value is the number of values that were really copied
  // (this can be lower than n)
  static R_xlen_t Get_region(SEXP alt, R_xlen_t i, R_xlen_t n, c_type* buf) {
    // If we have data2, we can just copy the region into buf
    // using the standard Get_region for this R type
    if (Base::IsMaterialized(alt)) {
      return Standard_Get_region<c_type>(R_altrep_data2(alt), i, n, buf);
    }

    // The vector was not materialized, aka we don't have data2
    //
    // In that case, we copy the data from the Array, and then
    // do a second pass to force the R sentinels for where the
    // array has nulls
    //
    // This only materialize the region, into buf. Not the entire vector.
    auto slice = GetChunkedArray(alt)->Slice(i, n);
    R_xlen_t ncopy = slice->length();

    c_type* out = buf;
    for (const auto& array : slice->chunks()) {
      auto n_i = array->length();

      // first copy the data buffer
      memcpy(out, array->data()->template GetValues<c_type>(1), n_i * sizeof(c_type));

      // then set the R NA sentinels if needed
      if (array->null_count() > 0) {
        internal::BitmapReader bitmap_reader(array->null_bitmap()->data(),
                                             array->offset(), n_i);

        for (R_xlen_t j = 0; j < n_i; j++, bitmap_reader.Next()) {
          if (bitmap_reader.IsNotSet()) {
            out[j] = cpp11::na<c_type>();
          }
        }
      }

      out += n_i;
    }

    return ncopy;
  }

  static std::shared_ptr<arrow::compute::ScalarAggregateOptions> NaRmOptions(bool na_rm) {
    auto options = std::make_shared<arrow::compute::ScalarAggregateOptions>(
        arrow::compute::ScalarAggregateOptions::Defaults());
    options->min_count = 0;
    options->skip_nulls = na_rm;
    return options;
  }

  template <bool Min>
  static SEXP MinMax(SEXP alt, Rboolean narm) {
    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
    using scalar_type =
        typename std::conditional<sexp_type == INTSXP, Int32Scalar, DoubleScalar>::type;

    const auto& chunked_array = GetChunkedArray(alt);
    bool na_rm = narm == TRUE;
    auto n = chunked_array->length();
    auto null_count = chunked_array->null_count();
    if ((na_rm || n == 0) && null_count == n) {
      return Rf_ScalarReal(Min ? R_PosInf : R_NegInf);
    }
    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }

    auto options = NaRmOptions(na_rm);

    const auto& minmax = ValueOrStop(
        arrow::compute::CallFunction("min_max", {chunked_array}, options.get()));
    const auto& minmax_scalar =
        internal::checked_cast<const StructScalar&>(*minmax.scalar());

    const auto& result_scalar = internal::checked_cast<const scalar_type&>(
        *ValueOrStop(minmax_scalar.field(Min ? "min" : "max")));
    return cpp11::as_sexp(result_scalar.value);
  }

  static SEXP Min(SEXP alt, Rboolean narm) { return MinMax<true>(alt, narm); }

  static SEXP Max(SEXP alt, Rboolean narm) { return MinMax<false>(alt, narm); }

  static SEXP Sum(SEXP alt, Rboolean narm) {
    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

    const auto& chunked_array = GetChunkedArray(alt);
    bool na_rm = narm == TRUE;
    auto null_count = chunked_array->null_count();

    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }
    auto options = NaRmOptions(na_rm);

    const auto& sum =
        ValueOrStop(arrow::compute::CallFunction("sum", {chunked_array}, options.get()));

    if (sexp_type == INTSXP) {
      // When calling the "sum" function on an int32 array, we get an Int64 scalar
      // in case of overflow, make it a double like R
      int64_t value = internal::checked_cast<const Int64Scalar&>(*sum.scalar()).value;
      if (value <= INT32_MIN || value > INT32_MAX) {
        return Rf_ScalarReal(static_cast<double>(value));
      } else {
        return Rf_ScalarInteger(static_cast<int>(value));
      }
    } else {
      return Rf_ScalarReal(
          internal::checked_cast<const DoubleScalar&>(*sum.scalar()).value);
    }
  }
};
template <int sexp_type>
R_altrep_class_t AltrepVectorPrimitive<sexp_type>::class_t;

// Implementation for string arrays
template <typename Type>
struct AltrepVectorString : public AltrepVectorBase<AltrepVectorString<Type>> {
  using Base = AltrepVectorBase<AltrepVectorString<Type>>;

  static R_altrep_class_t class_t;
  using StringArrayType = typename TypeTraits<Type>::ArrayType;

  // Helper class to convert to R strings
  struct RStringViewer {
    RStringViewer()
        : strip_out_nuls_(GetBoolOption("arrow.skip_nul", false)),
          nul_was_stripped_(false) {}

    // convert the i'th string of the Array to an R string (CHARSXP)
    SEXP Convert(size_t i) {
      if (array_->IsNull(i)) {
        return NA_STRING;
      }

      view_ = string_array_->GetView(i);
      bool no_nul = std::find(view_.begin(), view_.end(), '\0') == view_.end();

      if (no_nul) {
        return Rf_mkCharLenCE(view_.data(), view_.size(), CE_UTF8);
      } else if (strip_out_nuls_) {
        return ConvertStripNul();
      } else {
        Error();

        // not reached
        return R_NilValue;
      }
    }

    // strip the nuls and then convert to R string
    SEXP ConvertStripNul() {
      const char* old_string = view_.data();

      size_t stripped_len = 0, nul_count = 0;

      for (size_t i = 0; i < view_.size(); i++) {
        if (old_string[i] == '\0') {
          ++nul_count;

          if (nul_count == 1) {
            // first nul spotted: allocate stripped string storage
            stripped_string_.assign(view_.begin(), view_.end());
            stripped_len = i;
          }

          // don't copy old_string[i] (which is \0) into stripped_string
          continue;
        }

        if (nul_count > 0) {
          stripped_string_[stripped_len++] = old_string[i];
        }
      }

      nul_was_stripped_ = true;
      return Rf_mkCharLenCE(stripped_string_.data(), stripped_len, CE_UTF8);
    }

    bool nul_was_stripped() const { return nul_was_stripped_; }

    // throw R error about embedded nul
    void Error() {
      stripped_string_ = "embedded nul in string: '";
      for (char c : view_) {
        if (c) {
          stripped_string_ += c;
        } else {
          stripped_string_ += "\\0";
        }
      }

      stripped_string_ +=
          "'; to strip nuls when converting from Arrow to R, set options(arrow.skip_nul "
          "= TRUE)";

      Rf_error(stripped_string_.c_str());
    }

    void SetArray(const std::shared_ptr<Array>& array) {
      array_ = array;
      string_array_ = internal::checked_cast<const StringArrayType*>(array.get());
    }

    std::shared_ptr<Array> array_;
    const StringArrayType* string_array_;
    std::string stripped_string_;
    const bool strip_out_nuls_;
    bool nul_was_stripped_;
    util::string_view view_;
  };

  // Get a single string, as a CHARSXP SEXP,
  // either from data2 or directly from the Array
  static SEXP Elt(SEXP alt, R_xlen_t i) {
    if (Base::IsMaterialized(alt)) {
      return STRING_ELT(R_altrep_data2(alt), i);
    }
    BEGIN_CPP11

    ArrayResolve resolve(GetChunkedArray(alt), i);
    auto array = resolve.array_;
    auto j = resolve.index_;

    RStringViewer r_string_viewer;
    r_string_viewer.SetArray(array);

    // r_string_viewer.Convert() might jump so it's wrapped
    // in cpp11::unwind_protect() so that string_viewer
    // can be properly destructed before the unwinding continues
    SEXP s = NA_STRING;
    cpp11::unwind_protect([&]() {
      s = r_string_viewer.Convert(j);
      if (r_string_viewer.nul_was_stripped()) {
        cpp11::warning("Stripping '\\0' (nul) from character vector");
      }
    });
    return s;

    END_CPP11
  }

  static void* Dataptr(SEXP alt, Rboolean writeable) { return DATAPTR(Materialize(alt)); }

  static SEXP Materialize(SEXP alt) {
    if (Base::IsMaterialized(alt)) {
      return R_altrep_data2(alt);
    }

    BEGIN_CPP11

    const auto& chunked_array = GetChunkedArray(alt);
    SEXP data2 = PROTECT(Rf_allocVector(STRSXP, chunked_array->length()));
    MARK_NOT_MUTABLE(data2);

    RStringViewer r_string_viewer;

    // r_string_viewer.Convert() might jump so we have to
    // wrap it in unwind_protect() to:
    // - correctly destruct the C++ objects
    // - resume the unwinding
    cpp11::unwind_protect([&]() {
      R_xlen_t i = 0;
      for (const auto& array : chunked_array->chunks()) {
        r_string_viewer.SetArray(array);

        auto ni = array->length();
        for (R_xlen_t j = 0; j < ni; j++, i++) {
          SET_STRING_ELT(data2, i, r_string_viewer.Convert(j));
        }
      }

      if (r_string_viewer.nul_was_stripped()) {
        cpp11::warning("Stripping '\\0' (nul) from character vector");
      }
    });

    // only set to data2 if all the values have been converted
    R_set_altrep_data2(alt, data2);
    UNPROTECT(1);  // data2

    return data2;

    END_CPP11
  }

  static const void* Dataptr_or_null(SEXP alt) {
    if (Base::IsMaterialized(alt)) return DATAPTR(R_altrep_data2(alt));

    // otherwise give up
    return nullptr;
  }

  static void Set_elt(SEXP alt, R_xlen_t i, SEXP v) {
    Rf_error("ALTSTRING objects of type <arrow::array_string_vector> are immutable");
  }
};

template <typename Type>
R_altrep_class_t AltrepVectorString<Type>::class_t;

// initialize altrep, altvec, altreal, and altinteger methods
template <typename AltrepClass>
void InitAltrepMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altrep_Length_method(class_t, AltrepClass::Length);
  R_set_altrep_Inspect_method(class_t, AltrepClass::Inspect);
  R_set_altrep_Duplicate_method(class_t, AltrepClass::Duplicate);
  R_set_altrep_Serialized_state_method(class_t, AltrepClass::Serialized_state);
  R_set_altrep_Unserialize_method(class_t, AltrepClass::Unserialize);
  R_set_altrep_Coerce_method(class_t, AltrepClass::Coerce);
}

template <typename AltrepClass>
void InitAltvecMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altvec_Dataptr_method(class_t, AltrepClass::Dataptr);
  R_set_altvec_Dataptr_or_null_method(class_t, AltrepClass::Dataptr_or_null);
}

template <typename AltrepClass>
void InitAltRealMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altreal_No_NA_method(class_t, AltrepClass::No_NA);
  R_set_altreal_Is_sorted_method(class_t, AltrepClass::Is_sorted);

  R_set_altreal_Sum_method(class_t, AltrepClass::Sum);
  R_set_altreal_Min_method(class_t, AltrepClass::Min);
  R_set_altreal_Max_method(class_t, AltrepClass::Max);

  R_set_altreal_Elt_method(class_t, AltrepClass::Elt);
  R_set_altreal_Get_region_method(class_t, AltrepClass::Get_region);
}

template <typename AltrepClass>
void InitAltIntegerMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altinteger_No_NA_method(class_t, AltrepClass::No_NA);
  R_set_altinteger_Is_sorted_method(class_t, AltrepClass::Is_sorted);

  R_set_altinteger_Sum_method(class_t, AltrepClass::Sum);
  R_set_altinteger_Min_method(class_t, AltrepClass::Min);
  R_set_altinteger_Max_method(class_t, AltrepClass::Max);

  R_set_altinteger_Elt_method(class_t, AltrepClass::Elt);
  R_set_altinteger_Get_region_method(class_t, AltrepClass::Get_region);
}

template <typename AltrepClass>
void InitAltRealClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altreal_class(name, "arrow", dll);
  InitAltrepMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltvecMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltRealMethods<AltrepClass>(AltrepClass::class_t, dll);
}

template <typename AltrepClass>
void InitAltIntegerClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altinteger_class(name, "arrow", dll);
  InitAltrepMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltvecMethods<AltrepClass>(AltrepClass::class_t, dll);
  InitAltIntegerMethods<AltrepClass>(AltrepClass::class_t, dll);
}

template <typename AltrepClass>
void InitAltStringClass(DllInfo* dll, const char* name) {
  AltrepClass::class_t = R_make_altstring_class(name, "arrow", dll);
  R_set_altrep_Length_method(AltrepClass::class_t, AltrepClass::Length);
  R_set_altrep_Inspect_method(AltrepClass::class_t, AltrepClass::Inspect);
  R_set_altrep_Duplicate_method(AltrepClass::class_t, AltrepClass::Duplicate);
  R_set_altrep_Serialized_state_method(AltrepClass::class_t,
                                       AltrepClass::Serialized_state);
  R_set_altrep_Unserialize_method(AltrepClass::class_t, AltrepClass::Unserialize);
  R_set_altrep_Coerce_method(AltrepClass::class_t, AltrepClass::Coerce);

  R_set_altvec_Dataptr_method(AltrepClass::class_t, AltrepClass::Dataptr);
  R_set_altvec_Dataptr_or_null_method(AltrepClass::class_t, AltrepClass::Dataptr_or_null);

  R_set_altstring_Elt_method(AltrepClass::class_t, AltrepClass::Elt);
  R_set_altstring_Set_elt_method(AltrepClass::class_t, AltrepClass::Set_elt);
  R_set_altstring_No_NA_method(AltrepClass::class_t, AltrepClass::No_NA);
  R_set_altstring_Is_sorted_method(AltrepClass::class_t, AltrepClass::Is_sorted);
}

}  // namespace

// initialize the altrep classes
void Init_Altrep_classes(DllInfo* dll) {
  InitAltRealClass<AltrepVectorPrimitive<REALSXP>>(dll, "arrow::array_dbl_vector");
  InitAltIntegerClass<AltrepVectorPrimitive<INTSXP>>(dll, "arrow::array_int_vector");

  InitAltStringClass<AltrepVectorString<StringType>>(dll, "arrow::array_string_vector");
  InitAltStringClass<AltrepVectorString<LargeStringType>>(
      dll, "arrow::array_large_string_vector");
}

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<ChunkedArray>& chunked_array) {
  // using altrep if
  // - the arrow.use_altrep is set to TRUE or unset (implicit TRUE)
  // - the chunked array has at least one element
  if (arrow::r::GetBoolOption("arrow.use_altrep", true) && chunked_array->length() > 0) {
    switch (chunked_array->type()->id()) {
      case arrow::Type::DOUBLE:
        return altrep::AltrepVectorPrimitive<REALSXP>::Make(chunked_array);

      case arrow::Type::INT32:
        return altrep::AltrepVectorPrimitive<INTSXP>::Make(chunked_array);

      case arrow::Type::STRING:
        return altrep::AltrepVectorString<StringType>::Make(chunked_array);

      case arrow::Type::LARGE_STRING:
        return altrep::AltrepVectorString<LargeStringType>::Make(chunked_array);

      default:
        break;
    }
  }

  return R_NilValue;
}

bool is_arrow_altrep(SEXP x) {
  if (ALTREP(x)) {
    SEXP info = ALTREP_CLASS_SERIALIZED_CLASS(ALTREP_CLASS(x));
    SEXP pkg = ALTREP_SERIALIZED_CLASS_PKGSYM(info);

    if (pkg == symbols::arrow) return true;
  }

  return false;
}

std::shared_ptr<ChunkedArray> vec_to_arrow_altrep_bypass(SEXP x) {
  if (is_arrow_altrep(x)) {
    return GetChunkedArray(x);
  }

  return nullptr;
}

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#else  // HAS_ALTREP

namespace arrow {
namespace r {
namespace altrep {

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<ChunkedArray>& chunked_array) {
  return R_NilValue;
}

bool is_arrow_altrep(SEXP) { return false; }

std::shared_ptr<Array> vec_to_arrow_altrep_bypass(SEXP x) { return nullptr; }

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
void test_SET_STRING_ELT(SEXP s) { SET_STRING_ELT(s, 0, Rf_mkChar("forbidden")); }

// [[arrow::export]]
bool is_arrow_altrep(SEXP x) { return arrow::r::altrep::is_arrow_altrep(x); }

#endif
