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
// The altrep vector stores the Array as an external pointer in data1
// Implementation classes AltrepVectorPrimitive<> and AltrepVectorString
// also use data2
struct AltrepVectorBase {
  // store the Array as an external pointer in data1, mark as immutable
  static SEXP Make(R_altrep_class_t class_t, const std::shared_ptr<Array>& array) {
    SEXP alt_ =
        R_new_altrep(class_t, Pointer(new std::shared_ptr<Array>(array)), R_NilValue);
    MARK_NOT_MUTABLE(alt_);

    return alt_;
  }

  // the Array that is being wrapped by the altrep object
  static const std::shared_ptr<Array>& array(SEXP alt_) {
    return *Pointer(R_altrep_data1(alt_));
  }

  static R_xlen_t Length(SEXP alt_) { return array(alt_)->length(); }

  static int No_NA(SEXP alt_) { return array(alt_)->null_count() == 0; }

  static int Is_sorted(SEXP alt_) { return UNKNOWN_SORTEDNESS; }

  // What gets printed on .Internal(inspect(<the altrep object>))
  static Rboolean Inspect(SEXP alt_, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array_ = array(alt_);
    Rprintf("arrow::Array<%s, %d nulls> len=%d, Array=<%p>\n",
            array_->type()->ToString().c_str(), array_->null_count(), array_->length(),
            array_.get());
    inspect_subtree(R_altrep_data1(alt_), pre, deep + 1, pvec);
    return TRUE;
  }
};

// altrep R vector shadowing an primitive (int or double) Array.
//
// This tries as much as possible to directly use the data
// from the Array and minimize data copies.
//
// data2 starts as NULL, and becomes a standard R vector with the same
// data if necessary: if materialization is needed, e.g. if we need
// to access its data pointer
template <int sexp_type>
struct AltrepVectorPrimitive : public AltrepVectorBase {
  // singleton altrep class description
  static R_altrep_class_t class_t;

  using c_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  static SEXP Make(const std::shared_ptr<Array>& array) {
    return AltrepVectorBase::Make(class_t, array);
  }

  // Is the vector materialized, i.e. does the data2 slot contain a
  // standard R vector with the same data as the array.
  static bool IsMaterialized(SEXP alt_) { return !Rf_isNull(R_altrep_data2(alt_)); }

  // Force materialization. After calling this, the data2 slot of the altrep
  // object contains a standard R vector with the same data, with
  // R sentinels where the Array has nulls.
  //
  // The Array remains available so that it can be used by Length(), Min(), etc ...
  static SEXP Materialize(SEXP alt_) {
    if (!IsMaterialized(alt_)) {
      auto size = Length(alt_);

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(sexp_type, size));

      // copy the data from the array, through Get_region
      Get_region(alt_, 0, size, reinterpret_cast<c_type*>(DATAPTR(copy)));

      // store as data2, this is now considered materialized
      R_set_altrep_data2(alt_, copy);
      MARK_NOT_MUTABLE(copy);

      UNPROTECT(1);
    }
    return R_altrep_data2(alt_);
  }

  // Duplication is done by first materializing the vector and
  // then make a lazy duplicate of data2
  static SEXP Duplicate(SEXP alt_, Rboolean /* deep */) {
    return Rf_lazy_duplicate(Materialize(alt_));
  }

  // R calls this to get a pointer to the start of the vector data
  // but only if this is possible without allocating (in the R sense).
  static const void* Dataptr_or_null(SEXP alt_) {
    // data2 has been created, and so the R sentinels are in place where the array has
    // nulls
    if (IsMaterialized(alt_)) {
      return DATAPTR_RO(R_altrep_data2(alt_));
    }

    // the Array has no nulls, we can directly return the start of its data
    const auto& array_ = array(alt_);
    if (array_->null_count() == 0) {
      return reinterpret_cast<const void*>(array_->data()->template GetValues<c_type>(1));
    }

    // Otherwise: if the array has nulls and data2 has not been generated: give up
    return NULL;
  }

  // R calls this to get a pointer to the start of the data, R allocations are allowed.
  static void* Dataptr(SEXP alt_, Rboolean writeable) {
    // If the object hasn't been materialized, and the array has no
    // nulls we can directly point to the array data.
    if (!IsMaterialized(alt_)) {
      const auto& array_ = array(alt_);

      if (array_->null_count() == 0) {
        return reinterpret_cast<void*>(
            const_cast<c_type*>(array_->data()->template GetValues<c_type>(1)));
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
    return DATAPTR(Materialize(alt_));
  }

  // The value at position i
  static c_type Elt(SEXP alt_, R_xlen_t i) {
    const auto& array_ = array(alt_);
    return array_->IsNull(i) ? cpp11::na<c_type>()
                             : array_->data()->template GetValues<c_type>(1)[i];
  }

  // R calls this when it wants data from position `i` to `i + n` copied into `buf`
  // The returned value is the number of values that were really copied
  // (this can be lower than n)
  static R_xlen_t Get_region(SEXP alt_, R_xlen_t i, R_xlen_t n, c_type* buf) {
    // If we have data2, we can just copy the region into buf
    // using the standard Get_region for this R type
    if (IsMaterialized(alt_)) {
      return Standard_Get_region<c_type>(R_altrep_data2(alt_), i, n, buf);
    }

    // The vector was not materialized, aka we don't have data2
    //
    // In that case, we copy the data from the Array, and then
    // do a second pass to force the R sentinels for where the
    // array has nulls
    //
    // This only materialize the region, into buf. Not the entire vector.
    auto slice = array(alt_)->Slice(i, n);
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

  // This cannot keep the external pointer to an Arrow object through
  // R serialization, so return the materialized
  static SEXP Serialized_state(SEXP alt_) { return R_altrep_data2(Materialize(alt_)); }

  static SEXP Unserialize(SEXP /* class_ */, SEXP state) { return state; }

  static SEXP Coerce(SEXP alt_, int type) {
    return Rf_coerceVector(Materialize(alt_), type);
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
  static SEXP MinMax(SEXP alt_, Rboolean narm) {
    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
    using scalar_type =
        typename std::conditional<sexp_type == INTSXP, Int32Scalar, DoubleScalar>::type;

    const auto& array_ = array(alt_);
    bool na_rm = narm == TRUE;
    auto n = array_->length();
    auto null_count = array_->null_count();
    if ((na_rm || n == 0) && null_count == n) {
      return Rf_ScalarReal(Min ? R_PosInf : R_NegInf);
    }
    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }

    auto options = NaRmOptions(array_, na_rm);

    const auto& minmax =
        ValueOrStop(arrow::compute::CallFunction("min_max", {array_}, options.get()));
    const auto& minmax_scalar =
        internal::checked_cast<const StructScalar&>(*minmax.scalar());

    const auto& result_scalar = internal::checked_cast<const scalar_type&>(
        *ValueOrStop(minmax_scalar.field(Min ? "min" : "max")));
    return cpp11::as_sexp(result_scalar.value);
  }

  static SEXP Min(SEXP alt_, Rboolean narm) { return MinMax<true>(alt_, narm); }

  static SEXP Max(SEXP alt_, Rboolean narm) { return MinMax<false>(alt_, narm); }

  static SEXP Sum(SEXP alt_, Rboolean narm) {
    using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

    const auto& array_ = array(alt_);
    bool na_rm = narm == TRUE;
    auto null_count = array_->null_count();

    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }
    auto options = NaRmOptions(array_, na_rm);

    const auto& sum =
        ValueOrStop(arrow::compute::CallFunction("sum", {array_}, options.get()));

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
struct AltrepVectorString : public AltrepVectorBase {
  static R_altrep_class_t class_t;
  using StringArrayType = typename TypeTraits<Type>::ArrayType;

  static SEXP Make(const std::shared_ptr<Array>& array) {
    SEXP alt_ = AltrepVectorBase::Make(class_t, array);

    // using the @tag of the external pointer to count the
    // number of strings that have been expanded
    //
    // When this is equal to the length of the array,
    // it means that the data2 is complete
    SEXP count = PROTECT(Rf_ScalarInteger(0));
    R_SetExternalPtrTag(R_altrep_data1(alt_), count);
    UNPROTECT(1);

    return alt_;
  }

  // true when all the strings have been expanded in data2
  static bool IsComplete(SEXP alt_) {
    return INTEGER_ELT(R_ExternalPtrTag(R_altrep_data1(alt_)), 0) == Length(alt_);
  }

  // increment the complete count, that tracks the number of
  // strings from the Array that have been expanded to R strings
  static void IncrementComplete(SEXP alt_) {
    SEXP count = R_ExternalPtrTag(R_altrep_data1(alt_));
    ++INTEGER(count)[0];
  }

  static SEXP Initialize(SEXP alt_) {
    if (Rf_isNull(R_altrep_data2(alt_))) {
      auto array_ = array(alt_);
      R_xlen_t n = array_->length();
      SEXP data2_ = PROTECT(Rf_allocVector(STRSXP, n));
      if (n > 0) {
        // set individual strings to NULL (not yet materialized)
        memset(STDVEC_DATAPTR(data2_), 0, n * sizeof(SEXP));
      }
      R_set_altrep_data2(alt_, data2_);
      UNPROTECT(1);
    }

    return R_altrep_data2(alt_);
  }

  // Get a single string, as a CHARSXP SEXP
  // data2 is initialized, the CHARSXP is generated from the Array data
  // and stored in data2, so that this only needs to expand a given string once
  static SEXP Elt(SEXP alt_, R_xlen_t i) {
    Initialize(alt_);
    SEXP data2_ = R_altrep_data2(alt_);

    // data2[i] was already generated - nothing to do
    SEXP s = STRING_ELT(data2_, i);
    if (s != NULL) {
      return s;
    }

    // data2[i] is nul, expand to NA_STRING
    if (array(alt_)->IsNull(i)) {
      SET_STRING_ELT(data2_, i, NA_STRING);
      IncrementComplete(alt_);
      return NA_STRING;
    }

    // data2[i] is a string, but we need care about embedded nuls
    // this needs to call an R api function: Rf_mkCharLenCE() that
    // might jump, i.e. throw an R error, which is dealt with using
    // BEGIN_CPP11/END_CPP11/cpp11::unwind_protect()

    BEGIN_CPP11

    // C++ objects that will properly be destroyed by END_CPP11
    // before it resumes the unwinding - and perhaps let
    // the R error pass through
    auto array_ = array(alt_);
    auto view = internal::checked_cast<StringArrayType*>(array_.get())->GetView(i);
    const bool strip_out_nuls = GetBoolOption("arrow.skip_nul", false);
    bool nul_was_stripped = false;
    std::string stripped_string;

    // both cases might jump, although it's less likely when
    // nuls are stripped, but still we need the unwind protection
    // so that C++ objects here are correctly destructed, whilst errors
    // properly pass through to the R side
    cpp11::unwind_protect([&]() {
      if (strip_out_nuls) {
        s = r_string_from_view_strip_nul(view, stripped_string, &nul_was_stripped);
      } else {
        s = r_string_from_view(view);
      }

      if (nul_was_stripped) {
        cpp11::warning("Stripping '\\0' (nul) from character vector");
      }
    });
    SET_STRING_ELT(data2_, i, s);
    IncrementComplete(alt_);
    return s;

    END_CPP11
  }

  static void* Dataptr(SEXP alt_, Rboolean writeable) { return DATAPTR(Complete(alt_)); }

  static SEXP Complete(SEXP alt_) {
    if (IsComplete(alt_)) {
      return R_altrep_data2(alt_);
    }

    BEGIN_CPP11

    Initialize(alt_);
    auto array_ = array(alt_);
    SEXP data2_ = R_altrep_data2(alt_);
    R_xlen_t n = XLENGTH(data2_);

    std::string stripped_string;
    const bool strip_out_nuls = GetBoolOption("arrow.skip_nul", false);
    bool nul_was_stripped = false;
    auto* string_array = internal::checked_cast<StringArrayType*>(array_.get());
    util::string_view view;

    // keeping track of how many strings have been materialized
    SEXP count = R_ExternalPtrTag(R_altrep_data1(alt_));
    int* p_count = INTEGER(count);

    cpp11::unwind_protect([&]() {
      for (R_xlen_t i = 0; i < n; i++) {
        SEXP s = STRING_ELT(data2_, i);

        // already materialized, nothing to do
        if (s != NULL) {
          continue;
        }

        // nul, so materialize to NA_STRING
        if (array_->IsNull(i)) {
          SET_STRING_ELT(data2_, i, NA_STRING);
          ++p_count;
          continue;
        }

        // materialize a real string, with care about potential jump
        // from Rf_mkCharLenCE()
        view = string_array->GetView(i);
        if (strip_out_nuls) {
          s = r_string_from_view_strip_nul(view, stripped_string, &nul_was_stripped);
        } else {
          s = r_string_from_view(view);
        }
        SET_STRING_ELT(data2_, i, s);
        ++p_count;
      }

      if (nul_was_stripped) {
        cpp11::warning("Stripping '\\0' (nul) from character vector");
      }
    });

    return R_altrep_data2(alt_);
    END_CPP11
  }

  static const void* Dataptr_or_null(SEXP alt_) {
    // only valid if all strings have been materialized
    // i.e. it is not enough for data2 to be not NULL
    if (IsComplete(alt_)) return DATAPTR(R_altrep_data2(alt_));

    // otherwise give up
    return NULL;
  }

  static SEXP Coerce(SEXP alt_, int type) {
    return Rf_coerceVector(Complete(alt_), type);
  }

  static SEXP Serialized_state(SEXP alt_) { return Complete(alt_); }

  static SEXP Unserialize(SEXP /* class_ */, SEXP state) { return state; }

  static SEXP Duplicate(SEXP alt_, Rboolean /* deep */) {
    return Rf_lazy_duplicate(Complete(alt_));
  }

  // static method so that this can error without concerns of
  // destruction for the
  static void Set_elt(SEXP alt_, R_xlen_t i, SEXP v) {
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

// initialize the altrep classes
void Init_Altrep_classes(DllInfo* dll) {
  InitAltRealClass<AltrepVectorPrimitive<REALSXP>>(dll, "arrow::array_dbl_vector");
  InitAltIntegerClass<AltrepVectorPrimitive<INTSXP>>(dll, "arrow::array_int_vector");

  InitAltStringClass<AltrepVectorString<StringType>>(dll, "arrow::array_string_vector");
  InitAltStringClass<AltrepVectorString<LargeStringType>>(
      dll, "arrow::array_large_string_vector");
}

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepVector(const std::shared_ptr<Array>& array) {
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

  return R_NilValue;
}

}  // namespace altrep
}  // namespace r
}  // namespace arrow

#endif  // HAS_ALTREP

// [[arrow::export]]
bool is_altrep(SEXP x) {
#if defined(HAS_ALTREP)
  return ALTREP(x);
#else
  return false;
#endif
}

// [[arrow::export]]
void test_SET_STRING_ELT(SEXP s) { SET_STRING_ELT(s, 0, Rf_mkChar("forbidden")); }

#endif
