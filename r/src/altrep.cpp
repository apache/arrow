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

#include <arrow/compute/api.h>

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

#include <arrow/array.h>
#include <arrow/util/bitmap_reader.h>

#include "./r_task_group.h"

namespace arrow {
namespace r {

struct AltrepArrayBase {
  static void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
  using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

  SEXP alt_;

  AltrepArrayBase(SEXP alt) : alt_(alt){}

  SEXP data1() {
    return R_altrep_data1(alt_);
  }

  SEXP data2() {
    return R_altrep_data2(alt_);
  }

  const std::shared_ptr<Array>& Get() {
    return *Pointer(data1());
  }

  R_xlen_t Length() {
    return Get()->length();
  }

};

namespace altrep {

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
R_xlen_t Get_region(SEXP alt, R_xlen_t i, R_xlen_t n, typename AltrepClass::data_type* buf) {
  return AltrepClass(alt).Get_region(i, n, buf);
}

template <typename AltrepClass>
int No_NA(SEXP alt) {
  return AltrepClass(alt).No_NA();
}

inline SEXP Make(R_altrep_class_t class_t, const std::shared_ptr<Array>& array, RTasks& tasks) {
  // we don't need the whole r6 object, just an external pointer that retain the Array
  AltrepArrayBase::Pointer xp(new std::shared_ptr<Array>(array));

  SEXP res = R_new_altrep(class_t, xp, R_NilValue);
  MARK_NOT_MUTABLE(res);

  return res;
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

  const auto& array = AltrepArrayBase(alt).Get();
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

  const auto& array = AltrepArrayBase(alt).Get();
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

} // namespace altrep


template <int sexp_type>
struct AltrepArrayNoNulls : public AltrepArrayBase {
  static R_altrep_class_t class_t;

  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  AltrepArrayNoNulls(SEXP alt) : AltrepArrayBase(alt){}

  Rboolean Inspect(int pre, int deep, int pvec,
                   void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = Get();
    Rprintf("arrow::Array<%s, no nulls> len=%d, Array=<%p>\n",
            array->type()->ToString().c_str(), array->length(),
            array.get());
    inspect_subtree(data1(), pre, deep + 1, pvec);
    return TRUE;
  }

  const void* Dataptr_or_null() {
    return Get()->data()->template GetValues<data_type>(1);
  }

  void* Dataptr(Rboolean writeable) {
    return const_cast<void*>(Dataptr_or_null());
  }

  virtual SEXP Duplicate(Rboolean deep) {
    const auto& array = Get();
    auto size = array->length();

    SEXP copy = PROTECT(Rf_allocVector(sexp_type, array->length()));

    memcpy(DATAPTR(copy), Dataptr_or_null(), size * sizeof(data_type));

    UNPROTECT(1);
    return copy;
  }

  int No_NA() {
    return true;
  }

  static void Init(DllInfo* dll) {
    // altrep
    R_set_altrep_Length_method(class_t, altrep::Length<AltrepArrayNoNulls>);
    R_set_altrep_Inspect_method(class_t, altrep::Inspect<AltrepArrayNoNulls>);
    R_set_altrep_Duplicate_method(class_t, altrep::Duplicate<AltrepArrayNoNulls>);

    // altvec
    R_set_altvec_Dataptr_method(class_t, altrep::Dataptr<AltrepArrayNoNulls>);
    R_set_altvec_Dataptr_or_null_method(class_t, altrep::Dataptr_or_null<AltrepArrayNoNulls>);
  }

};

template <int sexp_type>
struct AltrepArrayWithNulls : public AltrepArrayBase {
  static R_altrep_class_t class_t;

  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;

  AltrepArrayWithNulls(SEXP alt) : AltrepArrayBase(alt){}

  Rboolean Inspect(int pre, int deep, int pvec,
                   void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = Get();
    Rprintf("arrow::Array<%s, %d nulls, %s> len=%d, Array=<%p>\n",
            array->type()->ToString().c_str(), array->null_count(),
            Rf_isNull(data2()) ? "not materialized" : "materialized",
            array->length(),
            array.get());
    inspect_subtree(data1(), pre, deep + 1, pvec);
    if (IsMaterialized()) {
      inspect_subtree(data2(), pre, deep + 1, pvec);
    }
    return TRUE;
  }

  const void* Dataptr_or_null() {
    if (!IsMaterialized()) {
      return NULL;
    }

    return DATAPTR_RO(data2());
  }

  void* Dataptr(Rboolean writeable) {
    Materialize();
    return DATAPTR(data2());
  }

  virtual SEXP Duplicate(Rboolean) {
    Materialize();
    return data2();
  }

  int No_NA() {
    return false;
  }

  data_type Elt(R_xlen_t i) {
    const auto& array = Get();

    if (array->IsNull(i)) {
      return cpp11::na<data_type>();
    }

    return array->data()->template GetValues<data_type>(1)[i];
  }

  R_xlen_t Get_region(R_xlen_t i, R_xlen_t n, data_type* buf) {
    const auto& slice = Get()->Slice(i, n);

    R_xlen_t ncopy = slice->length();

    // first copy the data buffer
    memcpy(buf, slice->data()->template GetValues<data_type>(1), ncopy * sizeof(data_type));

    // then set the R NA sentinels if needed
    if (slice->null_count() > 0) {
      internal::BitmapReader bitmap_reader(slice->null_bitmap()->data(), slice->offset(),
                                           ncopy);

      for (R_xlen_t j = 0; j < ncopy; j++, bitmap_reader.Next()) {
        if (bitmap_reader.IsNotSet()) {
          buf[j] = cpp11::na<data_type>();
        }
      }
    }

    return ncopy;
  }

  static void Init(DllInfo* dll) {
    // altrep
    R_set_altrep_Length_method(class_t, altrep::Length<AltrepArrayWithNulls>);
    R_set_altrep_Inspect_method(class_t, altrep::Inspect<AltrepArrayWithNulls>);
    R_set_altrep_Duplicate_method(class_t, altrep::Duplicate<AltrepArrayWithNulls>);

    // altvec
    R_set_altvec_Dataptr_method(class_t, altrep::Dataptr<AltrepArrayWithNulls>);
    R_set_altvec_Dataptr_or_null_method(class_t, altrep::Dataptr_or_null<AltrepArrayWithNulls>);
  }

private:

  bool IsMaterialized() {
    return !Rf_isNull(data2());
  }

  void Materialize() {
    if (!IsMaterialized()) {
      const auto& array = Get();
      auto size = array->length();

      SEXP copy = PROTECT(Rf_allocVector(sexp_type, array->length()));

      // copy the data from the buffer
      memcpy(DATAPTR(copy), array->data()->template GetValues<data_type>(1), size * sizeof(data_type));

      // then set the NAs to the R sentinel
      internal::BitmapReader bitmap_reader(array->null_bitmap()->data(), array->offset(),
                                           size);

      data_type* p = reinterpret_cast<data_type*>(DATAPTR(copy));
      for (R_xlen_t i = 0; i < size; i++, bitmap_reader.Next(), p++) {
        if (bitmap_reader.IsNotSet()) {
          *p = cpp11::na<data_type>();
        }
      }

      MARK_NOT_MUTABLE(copy);
      R_set_altrep_data2(alt_, copy);

      UNPROTECT(1);
    }
  }

};


template <int sexp_type>
R_altrep_class_t AltrepArrayNoNulls<sexp_type>::class_t;

template <int sexp_type>
R_altrep_class_t AltrepArrayWithNulls<sexp_type>::class_t;

void InitAltrepArrayNoNullClasses(DllInfo* dll) {
  // double
  R_altrep_class_t class_t = R_make_altreal_class("array_dbl_vector_no_nulls", "arrow", dll);

  AltrepArrayNoNulls<REALSXP>::class_t = class_t;
  AltrepArrayNoNulls<REALSXP>::Init(dll);

  R_set_altreal_No_NA_method(class_t, altrep::No_NA<AltrepArrayNoNulls<REALSXP>>);
  R_set_altreal_Sum_method(class_t, altrep::Sum<REALSXP>);
  R_set_altreal_Min_method(class_t, altrep::Min<REALSXP>);
  R_set_altreal_Max_method(class_t, altrep::Max<REALSXP>);

  // int
  class_t = R_make_altinteger_class("array_int_vector_no_nulls", "arrow", dll);

  AltrepArrayNoNulls<INTSXP>::class_t = class_t;
  AltrepArrayNoNulls<INTSXP>::Init(dll);

  R_set_altinteger_No_NA_method(class_t, altrep::No_NA<AltrepArrayNoNulls<INTSXP>>);
  R_set_altinteger_Sum_method(class_t, altrep::Sum<INTSXP>);
  R_set_altinteger_Min_method(class_t, altrep::Min<INTSXP>);
  R_set_altinteger_Max_method(class_t, altrep::Max<INTSXP>);
}

void InitAltrepArrayWithNullClasses(DllInfo* dll) {
  // double
  R_altrep_class_t class_t = R_make_altreal_class("array_dbl_vector_with_nulls", "arrow", dll);

  AltrepArrayWithNulls<REALSXP>::class_t = class_t;
  AltrepArrayWithNulls<REALSXP>::Init(dll);

  R_set_altreal_No_NA_method(class_t, altrep::No_NA<AltrepArrayWithNulls<REALSXP>>);
  R_set_altreal_Sum_method(class_t, altrep::Sum<REALSXP>);
  R_set_altreal_Min_method(class_t, altrep::Min<REALSXP>);
  R_set_altreal_Max_method(class_t, altrep::Max<REALSXP>);

  R_set_altreal_Elt_method(class_t, altrep::Elt<AltrepArrayWithNulls<REALSXP>>);
  R_set_altreal_Get_region_method(class_t, altrep::Get_region<AltrepArrayWithNulls<REALSXP>>);

  // int
  class_t = R_make_altinteger_class("array_int_vector_with_nulls", "arrow", dll);

  AltrepArrayWithNulls<INTSXP>::class_t = class_t;
  AltrepArrayWithNulls<INTSXP>::Init(dll);

  R_set_altinteger_No_NA_method(class_t, altrep::No_NA<AltrepArrayWithNulls<INTSXP>>);
  R_set_altinteger_Sum_method(class_t, altrep::Sum<INTSXP>);
  R_set_altinteger_Min_method(class_t, altrep::Min<INTSXP>);
  R_set_altinteger_Max_method(class_t, altrep::Max<INTSXP>);

  R_set_altinteger_Elt_method(class_t, altrep::Elt<AltrepArrayWithNulls<INTSXP>>);
  R_set_altinteger_Get_region_method(class_t, altrep::Get_region<AltrepArrayWithNulls<INTSXP>>);
}

template <int RTYPE>
SEXP MakeAltrepArray(const std::shared_ptr<Array>& array, RTasks& tasks) {
  if (array->null_count() > 0) {
    return altrep::Make(AltrepArrayWithNulls<RTYPE>::class_t, array, tasks);
  } else {
    return altrep::Make(AltrepArrayNoNulls<RTYPE>::class_t, array, tasks);
  }
}
template SEXP MakeAltrepArray<INTSXP>(const std::shared_ptr<Array>& array, RTasks& tasks);
template SEXP MakeAltrepArray<REALSXP>(const std::shared_ptr<Array>& array, RTasks& tasks);

void Init_Altrep_classes(DllInfo* dll) {
  arrow::r::InitAltrepArrayNoNullClasses(dll);
  arrow::r::InitAltrepArrayWithNullClasses(dll);
}

}  // namespace r
}  // namespace arrow

#endif // HAS_ALTREP

// [[arrow::export]]
bool is_altrep(SEXP x) {
#if defined(HAS_ALTREP)
  return ALTREP(x);
#else
  return false;
#endif
}

#endif
