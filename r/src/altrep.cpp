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

void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

// base class for all altrep vectors
//
// data1: the Array as an external pointer.
// data2: starts as NULL, and becomes a standard R vector with the same
//        data if necessary: if materialization is needed, e.g. if we need
//        to access its data pointer, with DATAPTR().
template <typename Impl>
struct AltrepVectorBase {
  // store the Array as an external pointer in data1, mark as immutable
  static SEXP Make(const std::shared_ptr<Array>& array) {
    SEXP alt = R_new_altrep(Impl::class_t, Pointer(new std::shared_ptr<Array>(array)),
                             R_NilValue);
    MARK_NOT_MUTABLE(alt);

    return alt;
  }

  // the Array that is being wrapped by the altrep object
  static const std::shared_ptr<Array>& GetArray(SEXP alt) {
    return *Pointer(R_altrep_data1(alt));
  }

  // Is the vector materialized, i.e. does the data2 slot contain a
  // standard R vector with the same data as the array.
  static bool IsMaterialized(SEXP alt) { return !Rf_isNull(R_altrep_data2(alt)); }

  static R_xlen_t Length(SEXP alt) { return GetArray(alt)->length(); }

  static int No_NA(SEXP alt) { return GetArray(alt)->null_count() == 0; }

  static int Is_sorted(SEXP alt) { return UNKNOWN_SORTEDNESS; }

  // What gets printed on .Internal(inspect(<the altrep object>))
  static Rboolean Inspect(SEXP alt, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = GetArray(alt);
    Rprintf("arrow::Array<%s, %d nulls> len=%d, Array=<%p>\n",
            array->type()->ToString().c_str(), array->null_count(), array->length(),
            array.get());
    inspect_subtree(R_altrep_data1(alt), pre, deep + 1, pvec);
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

    // the Array has no nulls, we can directly return the start of its data
    const auto& array = Base::GetArray(alt);
    if (array->null_count() == 0) {
      return reinterpret_cast<const void*>(array->data()->template GetValues<c_type>(1));
    }

    // Otherwise: if the array has nulls and data2 has not been generated: give up
    return nullptr;
  }

  // R calls this to get a pointer to the start of the data, R allocations are allowed.
  static void* Dataptr(SEXP alt, Rboolean writeable) {
    // If the object hasn't been materialized, and the array has no
    // nulls we can directly point to the array data.
    if (!Base::IsMaterialized(alt)) {
      const auto& array = Base::GetArray(alt);

      if (array->null_count() == 0) {
        return reinterpret_cast<void*>(
            const_cast<c_type*>(array->data()->template GetValues<c_type>(1)));
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
    const auto& array = Base::GetArray(alt);
    return array->IsNull(i) ? cpp11::na<c_type>()
                             : array->data()->template GetValues<c_type>(1)[i];
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
    auto slice = Base::GetArray(alt)->Slice(i, n);
    R_xlen_t ncopy = slice->length();

    // first copy the data buffer
    memcpy(buf, slice->data()->template GetValues<c_type>(1), ncopy * sizeof(c_type));

    // then set the R NA sentinels if needed
    if (slice->null_count() > 0) {
      internal::BitmapReader bitmap_reader(slice->null_bitmap()->data(), slice->offset(),
                                           ncopy);

      for (R_xlen_t j = 0; j < ncopy; j++, bitmap_reader.Next()) {
        if (bitmap_reader.IsNotSet()) {
          buf[j] = cpp11::na<c_type>();
        }
      }
    }

    return ncopy;
  }

  static std::shared_ptr<arrow::compute::ScalarAggregateOptions> NaRmOptions(
      const std::shared_ptr<Array>& array, bool na_rm) {
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

    const auto& array = Base::GetArray(alt);
    bool na_rm = narm == TRUE;
    auto n = array->length();
    auto null_count = array->null_count();
    if ((na_rm || n == 0) && null_count == n) {
      return Rf_ScalarReal(Min ? R_PosInf : R_NegInf);
    }
    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }

    auto options = NaRmOptions(array, na_rm);

    const auto& minmax =
        ValueOrStop(arrow::compute::CallFunction("min_max", {array}, options.get()));
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

    const auto& array = Base::GetArray(alt);
    bool na_rm = narm == TRUE;
    auto null_count = array->null_count();

    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }
    auto options = NaRmOptions(array, na_rm);

    const auto& sum =
        ValueOrStop(arrow::compute::CallFunction("sum", {array}, options.get()));

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

  // Get a single string, as a CHARSXP SEXP
  // data2 is initialized, the CHARSXP is generated from the Array data
  // and stored in data2, so that this only needs to expand a given string once
  static SEXP Elt(SEXP alt, R_xlen_t i) {
    if (Base::IsMaterialized(alt)) {
      return STRING_ELT(R_altrep_data2(alt), i);
    }

    // nul -> to NA_STRING
    if (Base::GetArray(alt)->IsNull(i)) {
      return NA_STRING;
    }

    // not nul, but we need care about embedded nuls
    // this needs to call an R api function: Rf_mkCharLenCE() that
    // might jump, i.e. throw an R error, which is dealt with using
    // BEGIN_CPP11/END_CPP11/cpp11::unwind_protect()

    BEGIN_CPP11

    // C++ objects that will properly be destroyed by END_CPP11
    // before it resumes the unwinding - and perhaps let
    // the R error pass through
    const auto& array = Base::GetArray(alt);
    auto view = internal::checked_cast<StringArrayType*>(array.get())->GetView(i);
    const bool strip_out_nuls = GetBoolOption("arrow.skip_nul", false);
    bool nul_was_stripped = false;
    std::string stripped_string;

    // both cases might jump, although it's less likely when
    // nuls are stripped, but still we need the unwind protection
    // so that C++ objects here are correctly destructed, whilst errors
    // properly pass through to the R side
    SEXP s;
    cpp11::unwind_protect([&]() {
      if (strip_out_nuls) {
        s = r_string_from_view_strip_nul(view, stripped_string, &nul_was_stripped);
      } else {
        s = r_string_from_view_keep_nul(view, stripped_string);
      }

      if (nul_was_stripped) {
        cpp11::warning("Stripping '\\0' (nul) from character vector");
      }
    });
    return s;

    END_CPP11
  }

  static void* Dataptr(SEXP alt, Rboolean writeable) {
    return DATAPTR(Materialize(alt));
  }

  static SEXP Materialize(SEXP alt) {
    if (Base::IsMaterialized(alt)) {
      return R_altrep_data2(alt);
    }

    BEGIN_CPP11

    auto array = Base::GetArray(alt);
    R_xlen_t n = array->length();
    SEXP data2 = PROTECT(Rf_allocVector(STRSXP, n));
    MARK_NOT_MUTABLE(data2);

    std::string stripped_string;
    const bool strip_out_nuls = GetBoolOption("arrow.skip_nul", false);
    auto* string_array = internal::checked_cast<StringArrayType*>(array.get());
    util::string_view view;

    cpp11::unwind_protect([&]() {
      if (strip_out_nuls) {
        bool nul_was_stripped = false;

        for (R_xlen_t i = 0; i < n; i++) {
          // nul, so materialize to NA_STRING
          if (array->IsNull(i)) {
            SET_STRING_ELT(data2, i, NA_STRING);
            continue;
          }

          // strip nul and materialize
          view = string_array->GetView(i);
          SET_STRING_ELT(
              data2, i,
              r_string_from_view_strip_nul(view, stripped_string, &nul_was_stripped));
          if (nul_was_stripped) {
            cpp11::warning("Stripping '\\0' (nul) from character vector");
          }
        }
      } else {
        for (R_xlen_t i = 0; i < n; i++) {
          // nul, so materialize to NA_STRING
          if (array->IsNull(i)) {
            SET_STRING_ELT(data2, i, NA_STRING);
            continue;
          }

          // try to materialize, this will error if the string has a nul
          view = string_array->GetView(i);
          SET_STRING_ELT(data2, i, r_string_from_view_keep_nul(view, stripped_string));
        }
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

  // this is called from an unwind_protect() block because
  // r_string_from_view might jump
  static SEXP r_string_from_view_strip_nul(arrow::util::string_view view,
                                           std::string& stripped_string,
                                           bool* nul_was_stripped) {
    const char* old_string = view.data();

    size_t stripped_len = 0, nul_count = 0;

    for (size_t i = 0; i < view.size(); i++) {
      if (old_string[i] == '\0') {
        ++nul_count;

        if (nul_count == 1) {
          // first nul spotted: allocate stripped string storage
          stripped_string = view.to_string();
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

  static SEXP r_string_from_view_keep_nul(arrow::util::string_view view,
                                          std::string& buffer) {
    bool has_nul = std::find(view.begin(), view.end(), '\0') != view.end();
    if (has_nul) {
      buffer = "embedded nul in string: '";
      for (char c : view) {
        if (c) {
          buffer += c;
        } else {
          buffer += "\\0";
        }
      }

      buffer +=
          "'; to strip nuls when converting from Arrow to R, set options(arrow.skip_nul "
          "= TRUE)";

      Rf_error(buffer.c_str());
    }

    return r_string_from_view(view);
  }

  static SEXP r_string_from_view(arrow::util::string_view view) {
    return Rf_mkCharLenCE(view.data(), view.size(), CE_UTF8);
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
  // special case when there is only one array
  if (chunked_array->num_chunks() == 1) {
    const auto& array = chunked_array->chunk(0);
    // using altrep if
    // - the arrow.use_altrep is set to TRUE or unset (implicit TRUE)
    // - the array has at least one element
    if (arrow::r::GetBoolOption("arrow.use_altrep", true) && array->length() > 0) {
      switch (array->type()->id()) {
      case arrow::Type::DOUBLE:
        return altrep::AltrepVectorPrimitive<REALSXP>::Make(array);

      case arrow::Type::INT32:
        return altrep::AltrepVectorPrimitive<INTSXP>::Make(array);

      case arrow::Type::STRING:
        return altrep::AltrepVectorString<StringType>::Make(array);

      case arrow::Type::LARGE_STRING:
        return altrep::AltrepVectorString<LargeStringType>::Make(array);

      default:
        break;
      }
    }
  }
  return R_NilValue;
}

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#else // HAS_ALTREP

namespace arrow {
namespace r {
namespace altrep {

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<ChunkedArray>& chunked_array) {
  return R_NilValue;
}

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
void test_SET_STRING_ELT(SEXP s) { SET_STRING_ELT(s, 0, Rf_mkChar("forbidden")); }

#endif
