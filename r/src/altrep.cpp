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

#include <cpp11/altrep.hpp>

#include "./arrow_types.h"

#if defined(HAS_ALTREP)

#include <R_ext/Altrep.h>
#include <arrow/array.h>

namespace arrow {
namespace r {

template <int sexp_type>
struct array_nonull {
  using data_type = typename std::conditional<sexp_type == INTSXP, int, double>::type;

  // altrep object around an Array with no nulls
  // data1: an external pointer to a shared pointer to the Array
  // data2: not used

  static SEXP Make(R_altrep_class_t class_t, const std::shared_ptr<Array>& array) {
    // we don't need the whole r6 object, just an external pointer
    // that retain the array
    cpp11::external_pointer<std::shared_ptr<Array>> xp(new std::shared_ptr<Array>(array));

    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    MARK_NOT_MUTABLE(res);

    return res;
  }

  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = Get(x);
    Rprintf("std::shared_ptr<arrow::Array, %s, NONULL> (len=%d, ptr=%p)\n",
            array->type()->ToString().c_str(), array->length(), array.get());
    return TRUE;
  }

  static const std::shared_ptr<Array>& Get(SEXP vec) {
    return *cpp11::external_pointer<std::shared_ptr<Array>>(R_altrep_data1(vec));
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

  // by definition, there are no NA
  static int No_NA(SEXP vec) { return 1; }

  static void Init(R_altrep_class_t class_t, DllInfo* dll) {
    // altrep
    R_set_altrep_Length_method(class_t, array_nonull::Length);
    R_set_altrep_Inspect_method(class_t, array_nonull::Inspect);
    R_set_altrep_Duplicate_method(class_t, array_nonull::Duplicate);

    // altvec
    R_set_altvec_Dataptr_method(class_t, array_nonull::Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, array_nonull::Dataptr_or_null);
  }
};

struct array_nonull_dbl_vector {
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altreal_class("array_nonull_dbl_vector", "arrow", dll);
    array_nonull<REALSXP>::Init(class_t, dll);
    R_set_altreal_No_NA_method(class_t, array_nonull<REALSXP>::No_NA);
  }

  static SEXP Make(const std::shared_ptr<Array>& array) {
    return array_nonull<REALSXP>::Make(class_t, array);
  }
};

struct array_nonull_int_vector {
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altinteger_class("array_nonull_int_vector", "arrow", dll);
    array_nonull<INTSXP>::Init(class_t, dll);
    R_set_altinteger_No_NA_method(class_t, array_nonull<INTSXP>::No_NA);
  }

  static SEXP Make(const std::shared_ptr<Array>& array) {
    return array_nonull<INTSXP>::Make(class_t, array);
  }
};

R_altrep_class_t array_nonull_int_vector::class_t;
R_altrep_class_t array_nonull_dbl_vector::class_t;

void Init_Altrep_classes(DllInfo* dll) {
  array_nonull_dbl_vector::Init(dll);
  array_nonull_int_vector::Init(dll);
}

SEXP Make_array_nonull_dbl_vector(const std::shared_ptr<Array>& array) {
  return array_nonull_dbl_vector::Make(array);
}

SEXP Make_array_nonull_int_vector(const std::shared_ptr<Array>& array) {
  return array_nonull_int_vector::Make(array);
}

}  // namespace r
}  // namespace arrow

// TODO: when arrow depends on R 3.5 we can eliminate this
#else

namespace arrow {
namespace r {

void Init_Altrep_classes(DllInfo* dll) {
  // nothing
}

SEXP Make_array_nonull_dbl_vector(const std::shared_ptr<Array>& array) {
  return R_NilValue;
}

SEXP Make_array_nonull_int_vector(const std::shared_ptr<Array>& array) {
  return R_NilValue;
}

}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
bool is_altrep_int_nonull(SEXP x) {
#if defined(HAS_ALTREP)
  return R_altrep_inherits(x, arrow::r::array_nonull_int_vector::class_t);
#else
  return false;
#endif
}

// [[arrow::export]]
bool is_altrep_dbl_nonull(SEXP x) {
#if defined(HAS_ALTREP)
  return R_altrep_inherits(x, arrow::r::array_nonull_dbl_vector::class_t);
#else
  return false;
#endif
}
