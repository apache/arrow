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

template <typename T>
void UseSentinel(const std::shared_ptr<Array>& array) {
  auto n = array->length();
  auto null_count = array->null_count();
  internal::BitmapReader bitmap_reader(array->null_bitmap()->data(), array->offset(), n);

  auto* data = array->data()->GetMutableValues<T>(1);

  for (R_xlen_t i = 0, k = 0; k < null_count; i++, bitmap_reader.Next()) {
    if (bitmap_reader.IsNotSet()) {
      k++;
      data[i] = cpp11::na<T>();
    }
  }
}

template <int sexp_type>
struct AltrepVector {
  using data_type = typename std::conditional<sexp_type == REALSXP, double, int>::type;
  using scalar_type =
      typename std::conditional<sexp_type == INTSXP, Int32Scalar, DoubleScalar>::type;

  static void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
  using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

  // altrep object around an Array
  // data1: an external pointer to a shared pointer to the Array
  // data2: not used

  static SEXP Make(R_altrep_class_t class_t, const std::shared_ptr<Array>& array,
                   RTasks& tasks) {
    // we don't need the whole r6 object, just an external pointer
    // that retain the Array
    Pointer xp(new std::shared_ptr<Array>(array));

    // we only get here if the Array data buffer is mutable
    // UseSentinel() puts the R sentinel where the data is null
    auto null_count = array->null_count();
    if (null_count > 0) {
      tasks.Append(true, [array]() {
        UseSentinel<data_type>(array);
        return Status::OK();
      });
    }

    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    MARK_NOT_MUTABLE(res);

    return res;
  }

  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = Get(x);
    Rprintf("arrow::Array<%s, nulls=%d> len=%d, Array=<%p>\n",
            array->type()->ToString().c_str(), array->null_count(), array->length(),
            array.get());
    inspect_subtree(R_altrep_data1(x), pre, deep + 1, pvec);
    return TRUE;
  }

  static const std::shared_ptr<Array>& Get(SEXP vec) {
    return *Pointer(R_altrep_data1(vec));
  }

  static R_xlen_t Length(SEXP vec) { return Get(vec)->length(); }

  static const void* Dataptr_or_null(SEXP vec) {
    return Get(vec)->data()->template GetValues<data_type>(1);
  }

  static SEXP Duplicate(SEXP vec, Rboolean) {
    const auto& array = Get(vec);
    auto size = array->length();

    SEXP copy = PROTECT(Rf_allocVector(sexp_type, array->length()));

    memcpy(DATAPTR(copy), Dataptr_or_null(vec), size * sizeof(data_type));

    UNPROTECT(1);
    return copy;
  }

  static void* Dataptr(SEXP vec, Rboolean writeable) {
    return const_cast<void*>(Dataptr_or_null(vec));
  }

  static int No_NA(SEXP vec) { return Get(vec)->null_count() == 0; }

  static SEXP Min(SEXP x, Rboolean narm) { return MinMax(x, narm, "min", R_PosInf); }

  static SEXP Max(SEXP x, Rboolean narm) { return MinMax(x, narm, "max", R_NegInf); }

  static SEXP MinMax(SEXP x, Rboolean narm, const std::string& field, double inf) {
    const auto& array = Get(x);
    bool na_rm = narm == TRUE;
    auto n = array->length();
    auto null_count = array->null_count();
    if ((na_rm || n == 0) && null_count == n) {
      return Rf_ScalarReal(inf);
    }
    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }

    auto options = Options(array, na_rm);

    const auto& minmax =
        ValueOrStop(arrow::compute::CallFunction("min_max", {array}, options.get()));
    const auto& minmax_scalar =
        internal::checked_cast<const StructScalar&>(*minmax.scalar());

    const auto& result_scalar = internal::checked_cast<const scalar_type&>(
        *ValueOrStop(minmax_scalar.field(field)));
    return cpp11::as_sexp(result_scalar.value);
  }

  static SEXP Sum(SEXP x, Rboolean narm) {
    const auto& array = Get(x);
    bool na_rm = narm == TRUE;
    auto null_count = array->null_count();

    if (!na_rm && null_count > 0) {
      return cpp11::as_sexp(cpp11::na<data_type>());
    }
    auto options = Options(array, na_rm);

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

  static std::shared_ptr<arrow::compute::ScalarAggregateOptions> Options(
      const std::shared_ptr<Array>& array, bool na_rm) {
    auto options = std::make_shared<arrow::compute::ScalarAggregateOptions>(
        arrow::compute::ScalarAggregateOptions::Defaults());
    options->min_count = 0;
    options->skip_nulls = na_rm;
    return options;
  }

  static void Init(R_altrep_class_t class_t, DllInfo* dll) {
    // altrep
    R_set_altrep_Length_method(class_t, AltrepVector::Length);
    R_set_altrep_Inspect_method(class_t, AltrepVector::Inspect);
    R_set_altrep_Duplicate_method(class_t, AltrepVector::Duplicate);

    // altvec
    R_set_altvec_Dataptr_method(class_t, AltrepVector::Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, AltrepVector::Dataptr_or_null);
  }
};

struct AltrepVectorDouble {
  using Base = AltrepVector<REALSXP>;
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altreal_class("array_dbl_vector", "arrow", dll);
    Base::Init(class_t, dll);

    R_set_altreal_No_NA_method(class_t, Base::No_NA);

    R_set_altreal_Sum_method(class_t, Base::Sum);
    R_set_altreal_Min_method(class_t, Base::Min);
    R_set_altreal_Max_method(class_t, Base::Max);
  }

  static SEXP Make(const std::shared_ptr<Array>& array, RTasks& tasks) {
    return Base::Make(class_t, array, tasks);
  }
};

struct AltrepVectorInt32 {
  using Base = AltrepVector<INTSXP>;
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altinteger_class("array_int_vector", "arrow", dll);
    Base::Init(class_t, dll);
    R_set_altinteger_No_NA_method(class_t, Base::No_NA);

    R_set_altinteger_Sum_method(class_t, Base::Sum);
    R_set_altinteger_Min_method(class_t, Base::Min);
    R_set_altinteger_Max_method(class_t, Base::Max);
  }

  static SEXP Make(const std::shared_ptr<Array>& array, RTasks& tasks) {
    return Base::Make(class_t, array, tasks);
  }
};

R_altrep_class_t AltrepVectorInt32::class_t;
R_altrep_class_t AltrepVectorDouble::class_t;

void Init_Altrep_classes(DllInfo* dll) {
  AltrepVectorDouble::Init(dll);
  AltrepVectorInt32::Init(dll);
}

SEXP MakeAltrepVectorDouble(const std::shared_ptr<Array>& array, RTasks& tasks) {
  return AltrepVectorDouble::Make(array, tasks);
}

SEXP MakeAltrepVectorInt32(const std::shared_ptr<Array>& array, RTasks& tasks) {
  return AltrepVectorInt32::Make(array, tasks);
}

}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
bool is_altrep(SEXP x) {
#if defined(HAS_ALTREP)
  return ALTREP(x);
#else
  return false;
#endif
}

#endif
