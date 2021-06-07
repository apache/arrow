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
#include <arrow/builder.h>

// [[arrow::export]]
std::shared_ptr<arrow::Array> Test_array_nonull_dbl_vector() {
  // TODO: maybe there is a simpler way to build this array ?
  arrow::NumericBuilder<arrow::DoubleType> builder(arrow::float64(),
                                                   arrow::default_memory_pool());
  StopIfNotOk(builder.AppendValues({1.0, 2.0, 3.0}));
  auto array = builder.Finish();

  return array.ValueUnsafe();
}

namespace arrow {
namespace r {

struct array_nonull_dbl_vector {
  // altrep object around an Array of type Double with no nulls
  // data1: an external pointer to a shared pointer to the Array
  // data2: not used

  static R_altrep_class_t class_t;

  static SEXP Make(const std::shared_ptr<Array>& array) {
    // we don't need the whole r6 object, just an external pointer
    // that retain the array
    cpp11::external_pointer<std::shared_ptr<Array>> xp(new std::shared_ptr<Array>(array));

    SEXP res = R_new_altrep(class_t, xp, R_NilValue);
    return res;
  }

  static std::shared_ptr<Array>& Get(SEXP vec) {
    return *cpp11::external_pointer<std::shared_ptr<Array>>(R_altrep_data1(vec));
  }

  static R_xlen_t Length(SEXP vec) { return Get(vec)->length(); }

  static Rboolean Inspect(SEXP x, int pre, int deep, int pvec,
                          void (*inspect_subtree)(SEXP, int, int, int)) {
    Rprintf("std::shared_ptr<arrow::Array, DOUBLE, NONULL> (len=%d, ptr=%p)\n", Length(x),
            Get(x).get());
    return TRUE;
  }

  static const void* Dataptr_or_null(SEXP vec) {
    return reinterpret_cast<const void*>(Get(vec)->data()->buffers[1]->data());
  }

  static void* Dataptr(SEXP vec, Rboolean) {
    return const_cast<void*>(Dataptr_or_null(vec));
  }

  // by definition, there are no NA
  static int No_NA(SEXP vec) { return 1; }

  static void Init(DllInfo* dll) {
    class_t = R_make_altreal_class("array_nonull_dbl_vector", "arrow", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    R_set_altvec_Dataptr_method(class_t, Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);

    // altreal
    R_set_altreal_No_NA_method(class_t, No_NA);
  }
};

R_altrep_class_t array_nonull_dbl_vector::class_t;

void Init_Altrep_classes(DllInfo* dll) { array_nonull_dbl_vector::Init(dll); }

SEXP Make_array_nonull_dbl_vector(const std::shared_ptr<Array>& array) {
  return array_nonull_dbl_vector::Make(array);
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

}  // namespace r
}  // namespace arrow

#endif
