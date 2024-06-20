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
#include <memory>
#include <utility>

#include "arrow/array.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/datetime.h"
#include "arrow/python/lib_api.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/wrap_macros.h"

namespace {
#include "arrow/python/pyarrow_api.h"
}

namespace arrow {
namespace py {

int import_pyarrow() {
#ifdef PYPY_VERSION
  PyDateTime_IMPORT;
#else
  internal::InitDatetime();
#endif
  return ::import_pyarrow__lib();
}

DEFINE_WRAP_FUNCTIONS(buffer, std::shared_ptr<Buffer>, out)

DEFINE_WRAP_FUNCTIONS(data_type, std::shared_ptr<DataType>, out)
DEFINE_WRAP_FUNCTIONS(field, std::shared_ptr<Field>, out)
DEFINE_WRAP_FUNCTIONS(schema, std::shared_ptr<Schema>, out)

DEFINE_WRAP_FUNCTIONS(scalar, std::shared_ptr<Scalar>, out)

DEFINE_WRAP_FUNCTIONS(array, std::shared_ptr<arrow::Array>, out)
DEFINE_WRAP_FUNCTIONS(chunked_array, std::shared_ptr<ChunkedArray>, out)

DEFINE_WRAP_FUNCTIONS(sparse_coo_tensor, std::shared_ptr<SparseCOOTensor>, out)
DEFINE_WRAP_FUNCTIONS(sparse_csc_matrix, std::shared_ptr<SparseCSCMatrix>, out)
DEFINE_WRAP_FUNCTIONS(sparse_csf_tensor, std::shared_ptr<SparseCSFTensor>, out)
DEFINE_WRAP_FUNCTIONS(sparse_csr_matrix, std::shared_ptr<SparseCSRMatrix>, out)
DEFINE_WRAP_FUNCTIONS(tensor, std::shared_ptr<Tensor>, out)

DEFINE_WRAP_FUNCTIONS(batch, std::shared_ptr<RecordBatch>, out)
DEFINE_WRAP_FUNCTIONS(table, std::shared_ptr<Table>, out)

namespace internal {

int check_status(const Status& status) { return ::pyarrow_internal_check_status(status); }

PyObject* convert_status(const Status& status) {
  DCHECK(!status.ok());
  return ::pyarrow_internal_convert_status(status);
}

}  // namespace internal

}  // namespace py
}  // namespace arrow
