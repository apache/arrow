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

#pragma once

#include "arrow/python/platform.h"
#include "arrow/python/wrap_macros.h"

#include <memory>

#include "arrow/python/visibility.h"

#include "arrow/sparse_tensor.h"

// Work around ARROW-2317 (C linkage warning from Cython)
extern "C++" {

namespace arrow {

class Array;
class Buffer;
class DataType;
class Field;
class RecordBatch;
class Schema;
class Status;
class Table;
class Tensor;

namespace compute {
class Expression;
}

namespace py {

// Returns 0 on success, -1 on error.
ARROW_PYTHON_EXPORT int import_pyarrow();

DECLARE_WRAP_FUNCTIONS(buffer, std::shared_ptr<Buffer>)

DECLARE_WRAP_FUNCTIONS(data_type, std::shared_ptr<DataType>)
DECLARE_WRAP_FUNCTIONS(field, std::shared_ptr<Field>)
DECLARE_WRAP_FUNCTIONS(schema, std::shared_ptr<Schema>)

DECLARE_WRAP_FUNCTIONS(scalar, std::shared_ptr<Scalar>)

DECLARE_WRAP_FUNCTIONS(array, std::shared_ptr<Array>)
DECLARE_WRAP_FUNCTIONS(chunked_array, std::shared_ptr<ChunkedArray>)

DECLARE_WRAP_FUNCTIONS(sparse_coo_tensor, std::shared_ptr<SparseCOOTensor>)
DECLARE_WRAP_FUNCTIONS(sparse_csc_matrix, std::shared_ptr<SparseCSCMatrix>)
DECLARE_WRAP_FUNCTIONS(sparse_csf_tensor, std::shared_ptr<SparseCSFTensor>)
DECLARE_WRAP_FUNCTIONS(sparse_csr_matrix, std::shared_ptr<SparseCSRMatrix>)
DECLARE_WRAP_FUNCTIONS(tensor, std::shared_ptr<Tensor>)

DECLARE_WRAP_FUNCTIONS(batch, std::shared_ptr<RecordBatch>)
DECLARE_WRAP_FUNCTIONS(table, std::shared_ptr<Table>)

DECLARE_WRAP_FUNCTIONS(expression, compute::Expression)

namespace internal {

// If status is ok, return 0.
// If status is not ok, set Python error indicator and return -1.
ARROW_PYTHON_EXPORT int check_status(const Status& status);

// Convert status to a Python exception object.  Status must not be ok.
ARROW_PYTHON_EXPORT PyObject* convert_status(const Status& status);

}  // namespace internal
}  // namespace py
}  // namespace arrow

}  // extern "C++"
