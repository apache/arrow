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

#include <arrow/array.h>

namespace arrow {
namespace r {

template <int sexp_type>
struct ArrayNoNull {
  using data_type = typename std::conditional<sexp_type == INTSXP, int, double>::type;
  static void DeleteArray(std::shared_ptr<Array>* ptr) { delete ptr; }
  using Pointer = cpp11::external_pointer<std::shared_ptr<Array>, DeleteArray>;

  // altrep object around an Array with no nulls
  // data1: an external pointer to a shared pointer to the Array
  // data2: not used

  static SEXP Make(R_altrep_class_t class_t, const std::shared_ptr<Array>& array) {
    // we don't need the whole r6 object, just an external pointer
    // that retain the array
    Pointer xp(new std::shared_ptr<Array>(array));

    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    MARK_NOT_MUTABLE(res);

    return res;
  }

  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    const auto& array = Get(x);
    Rprintf("arrow::Array<%s, NONULL> len=%d, Array=<%p>\n",
            array->type()->ToString().c_str(), array->length(), array.get());
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

  // by definition, there are no NA
  static int No_NA(SEXP vec) { return 1; }

  static void Init(R_altrep_class_t class_t, DllInfo* dll) {
    // altrep
    R_set_altrep_Length_method(class_t, ArrayNoNull::Length);
    R_set_altrep_Inspect_method(class_t, ArrayNoNull::Inspect);
    R_set_altrep_Duplicate_method(class_t, ArrayNoNull::Duplicate);

    // altvec
    R_set_altvec_Dataptr_method(class_t, ArrayNoNull::Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, ArrayNoNull::Dataptr_or_null);
  }
};

struct DoubleArrayNoNull {
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altreal_class("array_nonull_dbl_vector", "arrow", dll);
    ArrayNoNull<REALSXP>::Init(class_t, dll);
    R_set_altreal_No_NA_method(class_t, ArrayNoNull<REALSXP>::No_NA);
  }

  static SEXP Make(const std::shared_ptr<Array>& array) {
    return ArrayNoNull<REALSXP>::Make(class_t, array);
  }
};

struct Int32ArrayNoNull {
  static R_altrep_class_t class_t;

  static void Init(DllInfo* dll) {
    class_t = R_make_altinteger_class("array_nonull_int_vector", "arrow", dll);
    ArrayNoNull<INTSXP>::Init(class_t, dll);
    R_set_altinteger_No_NA_method(class_t, ArrayNoNull<INTSXP>::No_NA);
  }

  static SEXP Make(const std::shared_ptr<Array>& array) {
    return ArrayNoNull<INTSXP>::Make(class_t, array);
  }
};

R_altrep_class_t Int32ArrayNoNull::class_t;
R_altrep_class_t DoubleArrayNoNull::class_t;

void Init_Altrep_classes(DllInfo* dll) {
  DoubleArrayNoNull::Init(dll);
  Int32ArrayNoNull::Init(dll);
}

SEXP MakeDoubleArrayNoNull(const std::shared_ptr<Array>& array) {
  return DoubleArrayNoNull::Make(array);
}

SEXP MakeInt32ArrayNoNull(const std::shared_ptr<Array>& array) {
  return Int32ArrayNoNull::Make(array);
}

}  // namespace r
}  // namespace arrow

#endif

// [[arrow::export]]
bool is_altrep_int_nonull(SEXP x) {
#if defined(HAS_ALTREP)
  return R_altrep_inherits(x, arrow::r::Int32ArrayNoNull::class_t);
#else
  return false;
#endif
}

// [[arrow::export]]
bool is_altrep_dbl_nonull(SEXP x) {
#if defined(HAS_ALTREP)
  return R_altrep_inherits(x, arrow::r::DoubleArrayNoNull::class_t);
#else
  return false;
#endif
}

#endif
