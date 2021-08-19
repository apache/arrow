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

// defined in array_to_vector.cpp
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);

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

// specialized altrep vector for when there are some nulls
template <int sexp_type>
struct AltrepArrayPrimitive {
  static void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
  using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

  static R_altrep_class_t class_t;

  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
  constexpr static int r_type = sexp_type;

  explicit AltrepArrayPrimitive(SEXP alt)
      : alt_(alt), array_(*Pointer(R_altrep_data1(alt_))) {}

  explicit AltrepArrayPrimitive(const std::shared_ptr<Array>& array)
      : alt_(R_new_altrep(class_t, Pointer(new std::shared_ptr<Array>(array)),
                          R_NilValue)),
        array_(array) {
    MARK_NOT_MUTABLE(alt_);
  }

  SEXP data1() { return R_altrep_data1(alt_); }

  SEXP data2() { return R_altrep_data2(alt_); }

  R_xlen_t Length() { return array_->length(); }

  bool IsMaterialized() { return !Rf_isNull(data2()); }

  void Materialize() {
    if (!IsMaterialized()) {
      auto size = array_->length();

      // create a standard R vector
      SEXP copy = PROTECT(Rf_allocVector(sexp_type, array_->length()));

      // copy the data from the array, through Get_region
      Get_region(0, size, reinterpret_cast<data_type*>(DATAPTR(copy)));

      // store as data2
      MARK_NOT_MUTABLE(copy);

      R_set_altrep_data2(alt_, copy);
      UNPROTECT(1);
    }
  }

  SEXP Duplicate(Rboolean deep) {
    Materialize();
    return Rf_lazy_duplicate(data2());
  }

  Rboolean Inspect(int pre, int deep, int pvec,
                   void (*inspect_subtree)(SEXP, int, int, int)) {
    Rprintf("arrow::Array<%s, %d nulls, %s> len=%d, Array=<%p>\n",
            array_->type()->ToString().c_str(), array_->null_count(),
            IsMaterialized() ? "materialized" : "not materialized", array_->length(),
            array_.get());
    inspect_subtree(data1(), pre, deep + 1, pvec);
    if (IsMaterialized()) {
      inspect_subtree(data2(), pre, deep + 1, pvec);
    }
    return TRUE;
  }

  const void* Dataptr_or_null() {
    if (array_->null_count() == 0) {
      return reinterpret_cast<const void*>(const_array_data());
    }

    if (IsMaterialized()) {
      return DATAPTR_RO(data2());
    }

    return NULL;
  }

  // force materialization, and then return data ptr
  void* Dataptr(Rboolean writeable) {
    if (array_->null_count() == 0) {
      return reinterpret_cast<void*>(array_data());
    }

    Materialize();
    return DATAPTR(data2());
  }

  // by design, there are missing values in this vector
  int No_NA() { return array_->null_count() != 0; }

  // There are no guarantee that the data is the R sentinel,
  // so we have to treat it specially
  data_type Elt(R_xlen_t i) {
    return array_->IsNull(i) ? cpp11::na<data_type>()
                             : array_->data()->template GetValues<data_type>(1)[i];
  }

  SEXP alt_;
  const std::shared_ptr<Array>& array_;

  const data_type* const_array_data() const {
    return array_->data()->template GetValues<data_type>(1);
  }

  data_type* array_data() const {
    return const_cast<data_type*>(array_->data()->template GetValues<data_type>(1));
  }

  R_xlen_t Get_region(R_xlen_t i, R_xlen_t n, data_type* buf) {
    const auto& slice = array_->Slice(i, n);

    R_xlen_t ncopy = slice->length();

    if (IsMaterialized()) {
      // just use data2
      memcpy(buf, reinterpret_cast<data_type*>(DATAPTR(data2())) + i,
             ncopy * sizeof(data_type));
    } else {
      // first copy the data buffer
      memcpy(buf, slice->data()->template GetValues<data_type>(1),
             ncopy * sizeof(data_type));

      // then set the R NA sentinels if needed
      if (slice->null_count() > 0) {
        internal::BitmapReader bitmap_reader(slice->null_bitmap()->data(),
                                             slice->offset(), ncopy);

        for (R_xlen_t j = 0; j < ncopy; j++, bitmap_reader.Next()) {
          if (bitmap_reader.IsNotSet()) {
            buf[j] = cpp11::na<data_type>();
          }
        }
      }
    }

    return ncopy;
  }
};
template <int sexp_type>
R_altrep_class_t AltrepArrayPrimitive<sexp_type>::class_t;

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
R_xlen_t Get_region(SEXP alt, R_xlen_t i, R_xlen_t n,
                    typename AltrepClass::data_type* buf) {
  return AltrepClass(alt).Get_region(i, n, buf);
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

  const auto& array = AltrepArrayPrimitive<sexp_type>(alt).array_;
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

  const auto& array = AltrepArrayPrimitive<sexp_type>(alt).array_;
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

template <typename AltrepClass>
void InitAltrepMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altrep_Length_method(class_t, Length<AltrepClass>);
  R_set_altrep_Inspect_method(class_t, Inspect<AltrepClass>);
  R_set_altrep_Duplicate_method(class_t, Duplicate<AltrepClass>);
}

template <typename AltrepClass>
void InitAltvecMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altvec_Dataptr_method(class_t, Dataptr<AltrepClass>);
  R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null<AltrepClass>);
}

template <typename AltrepClass>
void InitAltRealMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altreal_No_NA_method(class_t, No_NA<AltrepClass>);
  R_set_altreal_Sum_method(class_t, Sum<REALSXP>);
  R_set_altreal_Min_method(class_t, Min<REALSXP>);
  R_set_altreal_Max_method(class_t, Max<REALSXP>);

  R_set_altreal_Elt_method(class_t, Elt<AltrepClass>);
  R_set_altreal_Get_region_method(class_t, Get_region<AltrepClass>);
}

template <typename AltrepClass>
void InitAltIntegerMethods(R_altrep_class_t class_t, DllInfo* dll) {
  R_set_altinteger_No_NA_method(class_t, No_NA<AltrepClass>);
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

void Init_Altrep_classes(DllInfo* dll) {
  InitAltRealClass<AltrepArrayPrimitive<REALSXP>>(dll, "array_dbl_vector");
  InitAltIntegerClass<AltrepArrayPrimitive<INTSXP>>(dll, "array_int_vector");
}

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
