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

#include "arrow/python/pyarrow_acero.h"

#include <memory>
#include <utility>

#include "arrow/acero/exec_plan.h"
#include "arrow/array.h"
#include "arrow/python/lib_api.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/config.h"

#include "arrow/python/common.h"
#include "arrow/python/datetime.h"

namespace {
#include "arrow/python/lib_acero_api.h"
}

namespace arrow {
namespace py {

static Status UnwrapError(PyObject* obj, const char* expected_type) {
  return Status::TypeError("Could not unwrap ", expected_type,
                           " from Python object of type '", Py_TYPE(obj)->tp_name, "'");
}

int import_pyarrow_acero() {
#ifdef PYPY_VERSION
  PyDateTime_IMPORT;
#else
  internal::InitDatetime();
#endif
  return ::import_pyarrow__lib_acero();
}

#define DEFINE_WRAP_FUNCTIONS(FUNC_SUFFIX, TYPE_NAME)                                   \
  bool is_##FUNC_SUFFIX(PyObject* obj) { return ::pyarrow_is_##FUNC_SUFFIX(obj) != 0; } \
                                                                                        \
  PyObject* wrap_##FUNC_SUFFIX(const TYPE_NAME& src) {                                  \
    return ::pyarrow_wrap_##FUNC_SUFFIX(src);                                           \
  }                                                                                     \
  Result<TYPE_NAME> unwrap_##FUNC_SUFFIX(PyObject* obj) {                               \
    auto out = ::pyarrow_unwrap_##FUNC_SUFFIX(obj);                                     \
    if (IS_VALID(out)) {                                                                \
      return std::move(out);                                                            \
    } else {                                                                            \
      return UnwrapError(obj, #TYPE_NAME);                                              \
    }                                                                                   \
  }

#define IS_VALID(OUT) OUT

// DEFINE_WRAP_FUNCTIONS(buffer, std::shared_ptr<Buffer>)
//
// DEFINE_WRAP_FUNCTIONS(data_type, std::shared_ptr<DataType>)
// DEFINE_WRAP_FUNCTIONS(field, std::shared_ptr<Field>)
// DEFINE_WRAP_FUNCTIONS(schema, std::shared_ptr<Schema>)
//
//
// DEFINE_WRAP_FUNCTIONS(scalar, std::shared_ptr<Scalar>)
// DEFINE_WRAP_FUNCTIONS(array, std::shared_ptr<Array>)
// DEFINE_WRAP_FUNCTIONS(chunked_array, std::shared_ptr<ChunkedArray>)
//
// DEFINE_WRAP_FUNCTIONS(sparse_coo_tensor, std::shared_ptr<SparseCOOTensor>)
// DEFINE_WRAP_FUNCTIONS(sparse_csc_matrix, std::shared_ptr<SparseCSCMatrix>)
// DEFINE_WRAP_FUNCTIONS(sparse_csf_tensor, std::shared_ptr<SparseCSFTensor>)
// DEFINE_WRAP_FUNCTIONS(sparse_csr_matrix, std::shared_ptr<SparseCSRMatrix>)
// DEFINE_WRAP_FUNCTIONS(tensor, std::shared_ptr<Tensor>)
//
// DEFINE_WRAP_FUNCTIONS(batch, std::shared_ptr<RecordBatch>)
// DEFINE_WRAP_FUNCTIONS(table, std::shared_ptr<Table>)

#undef IS_VALID

#define IS_VALID(OUT) OUT.IsValid()
DEFINE_WRAP_FUNCTIONS(declaration, acero::Declaration)
#undef IS_VALID

#undef DEFINE_WRAP_FUNCTIONS

}  // namespace py
}  // namespace arrow
