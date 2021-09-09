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

// altrep R vector shadowing an Array.
//
// This tries as much as possible to directly use the data
// from the Array and minimize data copies.
//
// Both slots of the altrep object (data1 and data2) are used:
//
// data1: always used, stores an R external pointer to a
//        shared pointer of the Array
// data2: starts as NULL, and becomes a standard R vector with the same
//        data if necessary (if materialization is needed)
template <int sexp_type>
struct AltrepArrayPrimitive {
  static void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
  using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

  using c_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  // singleton altrep class description
  static R_altrep_class_t class_t;

  // the altrep R object
  SEXP alt_;

  // This constructor is used to create the altrep object from
  // an Array. Used by MakeAltrepArrayPrimitive() which is used
  // in array_to_vector.cpp
  explicit AltrepArrayPrimitive(const std::shared_ptr<Array>& array)
      : alt_(R_new_altrep(class_t, Pointer(new std::shared_ptr<Array>(array)),
                          R_NilValue)) {
    // force duplicate on modify
    MARK_NOT_MUTABLE(alt_);
  }

  // This constructor is used when R calls altrep methods.
  //
  // For example in the Length() method below:
  //
  // template <typename AltrepClass>
  // R_xlen_t Length(SEXP alt) {
  //   return AltrepClass(alt).Length();
  // }
  explicit AltrepArrayPrimitive(SEXP alt) : alt_(alt) {}

  // the arrow::Array that is being wrapped by the altrep object
  // this is only valid before data2 has been materialized
  const std::shared_ptr<Array>& array() const { return *Pointer(R_altrep_data1(alt_)); }

  R_xlen_t Length() { return array()->length(); }

  // Does the data2 slot of the altrep object contain a
  // standard R vector with the same data as the array
  bool IsMaterialized() const { return !Rf_isNull(R_altrep_data2(alt_)); }

  // Force materialization. After calling this, the data2 slot of the altrep
  // object contains a standard R vector with the same data, with
  // R sentinels where the Array has nulls.
  void Materialize() {
    if (!IsMaterialized()) {
      auto size = array()->length();

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(sexp_type, size));

      // copy the data from the array, through Get_region
      Get_region(0, size, reinterpret_cast<c_type*>(DATAPTR(copy)));

      // store as data2, this is now considered materialized
      R_set_altrep_data2(alt_, copy);
      MARK_NOT_MUTABLE(copy);

      UNPROTECT(1);
    }
  }

  // Duplication is done by first materializing the vector and
  // then make a lazy duplicate of data2
  SEXP Duplicate(Rboolean /* deep */) {
    Materialize();
    return Rf_lazy_duplicate(R_altrep_data2(alt_));
  }

  // What gets printed on .Internal(inspect(<the altrep object>))
  Rboolean Inspect(int pre, int deep, int pvec,
                   void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array_ = array();
    Rprintf("arrow::Array<%s, %d nulls, %s> len=%d, Array=<%p>\n",
            array_->type()->ToString().c_str(), array_->null_count(),
            IsMaterialized() ? "materialized" : "not materialized", array_->length(),
            array_.get());
    inspect_subtree(R_altrep_data1(alt_), pre, deep + 1, pvec);
    if (IsMaterialized()) {
      inspect_subtree(R_altrep_data2(alt_), pre, deep + 1, pvec);
    }

    return TRUE;
  }

  // R calls this to get a pointer to the start of the vector data
  // but only if this is possible without allocating (in the R sense).
  //
  // For this implementation we can return the data in these cases
  // - data2 has been created, and so the R sentinels are in place where the array has
  // nulls
  // - the Array has no nulls, we can directly return the start of its data
  //
  // Otherwise: if the array has nulls and data2 has not been generated: give up
  const void* Dataptr_or_null() {
    if (IsMaterialized()) {
      return DATAPTR_RO(R_altrep_data2(alt_));
    }

    const auto& array_ = array();
    if (array_->null_count() == 0) {
      return reinterpret_cast<const void*>(array_->data()->template GetValues<c_type>(1));
    }

    return NULL;
  }

  // R calls this to get a pointer to the start of the data, R allocations are allowed.
  //
  // If the object hasn't been materialized, and the array has no
  // nulls we can directly point to the array data.
  //
  // Otherwise, the object is materialized DATAPTR(data2) is returned.
  void* Dataptr(Rboolean writeable) {
    if (!IsMaterialized()) {
      const auto& array_ = array();

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
    Materialize();
    return DATAPTR(R_altrep_data2(alt_));
  }

  // Does the Array have no nulls ?
  int No_NA() const { return array()->null_count() != 0; }

  int Is_sorted() const { return UNKNOWN_SORTEDNESS; }

  // The value at position i
  c_type Elt(R_xlen_t i) {
    const auto& array_ = array();
    return array_->IsNull(i) ? cpp11::na<c_type>()
                             : array_->data()->template GetValues<c_type>(1)[i];
  }

  // R calls this when it wants data from position `i` to `i + n` copied into `buf`
  // The returned value is the number of values that were really copied
  // (this can be lower than n)
  R_xlen_t Get_region(R_xlen_t i, R_xlen_t n, c_type* buf) {
    // If we have data2, we can just copy the region into buf
    // using the standard Get_region for this R type
    if (IsMaterialized()) {
      return Standard_Get_region<c_type>(R_altrep_data2(alt_), i, n, buf);
    }

    // The vector was not materialized, aka we don't have data2
    //
    // In that case, we copy the data from the Array, and then
    // do a second pass to force the R sentinels for where the
    // array has nulls
    //
    // This only materialize the region, into buf. Not the entire vector.
    auto slice = array()->Slice(i, n);
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
  SEXP Serialized_state() {
    Materialize();
    return R_altrep_data2(alt_);
  }

  static SEXP Unserialize(SEXP /* class_ */, SEXP state) { return state; }

  SEXP Coerce(int type) {
    // Just let R handle it for now
    return NULL;
  }
};
template <int sexp_type>
R_altrep_class_t AltrepArrayPrimitive<sexp_type>::class_t;

// The methods below are how R interacts with the altrep objects.
//
// They all use the same pattern: create a C++ object of the
// class parameter, and then call the method.
template <typename AltrepClass>
R_xlen_t Length(SEXP alt) {
  return AltrepClass(alt).Length();
}

template <typename AltrepClass>
Rboolean Inspect(SEXP alt, int pre, int deep, int pvec,
                 void (*inspect_subtree)(SEXP, int, int, int)) {
  return AltrepClass(alt).Inspect(pre, deep, pvec, inspect_subtree);
}

template <typename AltrepClass>
const void* Dataptr_or_null(SEXP alt) {
  return AltrepClass(alt).Dataptr_or_null();
}

template <typename AltrepClass>
void* Dataptr(SEXP alt, Rboolean writeable) {
  return AltrepClass(alt).Dataptr(writeable);
}

template <typename AltrepClass>
SEXP Duplicate(SEXP alt, Rboolean deep) {
  return AltrepClass(alt).Duplicate(deep);
}

template <typename AltrepClass>
auto Elt(SEXP alt, R_xlen_t i) -> decltype(AltrepClass(alt).Elt(i)) {
  return AltrepClass(alt).Elt(i);
}

template <typename AltrepClass>
int No_NA(SEXP alt) {
  return AltrepClass(alt).No_NA();
}

template <typename AltrepClass>
int Is_sorted(SEXP alt) {
  return AltrepClass(alt).Is_sorted();
}

template <typename AltrepClass>
R_xlen_t Get_region(SEXP alt, R_xlen_t i, R_xlen_t n, typename AltrepClass::c_type* buf) {
  return AltrepClass(alt).Get_region(i, n, buf);
}

template <typename AltrepClass>
SEXP Serialized_state(SEXP alt) {
  return AltrepClass(alt).Serialized_state();
}

template <typename AltrepClass>
SEXP Unserialize(SEXP class_, SEXP state) {
  return AltrepClass::Unserialize(class_, state);
}

template <typename AltrepClass>
SEXP Coerce(SEXP alt, int type) {
  return AltrepClass(alt).Coerce(type);
}

static std::shared_ptr<arrow::compute::ScalarAggregateOptions> NaRmOptions(
    const std::shared_ptr<Array>& array, bool na_rm) {
  auto options = std::make_shared<arrow::compute::ScalarAggregateOptions>(
      arrow::compute::ScalarAggregateOptions::Defaults());
  options->min_count = 0;
  options->skip_nulls = na_rm;
  return options;
}

template <int sexp_type, bool Min>
SEXP MinMax(SEXP alt, Rboolean narm) {
  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
  using scalar_type =
      typename std::conditional<sexp_type == INTSXP, Int32Scalar, DoubleScalar>::type;

  AltrepArrayPrimitive<sexp_type> alt_(alt);

  const auto& array = alt_.array();
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

template <int sexp_type>
SEXP Min(SEXP alt, Rboolean narm) {
  return MinMax<sexp_type, true>(alt, narm);
}

template <int sexp_type>
SEXP Max(SEXP alt, Rboolean narm) {
  return MinMax<sexp_type, false>(alt, narm);
}

template <int sexp_type>
static SEXP Sum(SEXP alt, Rboolean narm) {
  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  AltrepArrayPrimitive<sexp_type> alt_(alt);

  const auto& array = alt_.array();
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

// initialize altrep, altvec, altreal, and altinteger methods
template <typename AltrepClass>
void InitAltrepMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altrep_Length_method(class_t, Length<AltrepClass>);
  R_set_altrep_Inspect_method(class_t, Inspect<AltrepClass>);
  R_set_altrep_Duplicate_method(class_t, Duplicate<AltrepClass>);
  R_set_altrep_Serialized_state_method(class_t, Serialized_state<AltrepClass>);
  R_set_altrep_Unserialize_method(class_t, Unserialize<AltrepClass>);
  R_set_altrep_Coerce_method(class_t, Coerce<AltrepClass>);
}

template <typename AltrepClass>
void InitAltvecMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altvec_Dataptr_method(class_t, Dataptr<AltrepClass>);
  R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null<AltrepClass>);
}

template <typename AltrepClass>
void InitAltRealMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altreal_No_NA_method(class_t, No_NA<AltrepClass>);
  R_set_altreal_Is_sorted_method(class_t, Is_sorted<AltrepClass>);

  R_set_altreal_Sum_method(class_t, Sum<REALSXP>);
  R_set_altreal_Min_method(class_t, Min<REALSXP>);
  R_set_altreal_Max_method(class_t, Max<REALSXP>);

  R_set_altreal_Elt_method(class_t, Elt<AltrepClass>);
  R_set_altreal_Get_region_method(class_t, Get_region<AltrepClass>);
}

template <typename AltrepClass>
void InitAltIntegerMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altinteger_No_NA_method(class_t, No_NA<AltrepClass>);
  R_set_altinteger_Is_sorted_method(class_t, Is_sorted<AltrepClass>);

  R_set_altinteger_Sum_method(class_t, Sum<INTSXP>);
  R_set_altinteger_Min_method(class_t, Min<INTSXP>);
  R_set_altinteger_Max_method(class_t, Max<INTSXP>);

  R_set_altinteger_Elt_method(class_t, Elt<AltrepClass>);
  R_set_altinteger_Get_region_method(class_t, Get_region<AltrepClass>);
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

// initialize the altrep classes
void Init_Altrep_classes(DllInfo* dll) {
  InitAltRealClass<AltrepArrayPrimitive<REALSXP>>(dll, "array_dbl_vector");
  InitAltIntegerClass<AltrepArrayPrimitive<INTSXP>>(dll, "array_int_vector");
}

// return an altrep R vector that shadows the array if possible
SEXP MakeAltrepArrayPrimitive(const std::shared_ptr<Array>& array) {
  switch (array->type()->id()) {
    case arrow::Type::DOUBLE:
      return altrep::AltrepArrayPrimitive<REALSXP>(array).alt_;

    case arrow::Type::INT32:
      return altrep::AltrepArrayPrimitive<INTSXP>(array).alt_;

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

#endif
