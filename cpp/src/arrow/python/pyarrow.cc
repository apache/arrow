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

#include "arrow/python/pyarrow.h"

#include <memory>

#include "arrow/array.h"
#include "arrow/table.h"
#include "arrow/tensor.h"
#include "arrow/type.h"

namespace {
#include "arrow/python/pyarrow_api.h"
}

namespace arrow {
namespace py {

int import_pyarrow() { return ::import_pyarrow__lib(); }

bool is_buffer(PyObject* buffer) { return ::pyarrow_is_buffer(buffer) != 0; }

Status unwrap_buffer(PyObject* buffer, std::shared_ptr<Buffer>* out) {
  *out = ::pyarrow_unwrap_buffer(buffer);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Buffer from the passed Python object.");
  }
}

PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer) {
  return ::pyarrow_wrap_buffer(buffer);
}

bool is_data_type(PyObject* data_type) { return ::pyarrow_is_data_type(data_type) != 0; }

Status unwrap_data_type(PyObject* object, std::shared_ptr<DataType>* out) {
  *out = ::pyarrow_unwrap_data_type(object);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap DataType from the passed Python object.");
  }
}

PyObject* wrap_data_type(const std::shared_ptr<DataType>& type) {
  return ::pyarrow_wrap_data_type(type);
}

bool is_field(PyObject* field) { return ::pyarrow_is_field(field) != 0; }

Status unwrap_field(PyObject* field, std::shared_ptr<Field>* out) {
  *out = ::pyarrow_unwrap_field(field);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Field from the passed Python object.");
  }
}

PyObject* wrap_field(const std::shared_ptr<Field>& field) {
  return ::pyarrow_wrap_field(field);
}

bool is_schema(PyObject* schema) { return ::pyarrow_is_schema(schema) != 0; }

Status unwrap_schema(PyObject* schema, std::shared_ptr<Schema>* out) {
  *out = ::pyarrow_unwrap_schema(schema);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Schema from the passed Python object.");
  }
}

PyObject* wrap_schema(const std::shared_ptr<Schema>& schema) {
  return ::pyarrow_wrap_schema(schema);
}

bool is_array(PyObject* array) { return ::pyarrow_is_array(array) != 0; }

Status unwrap_array(PyObject* array, std::shared_ptr<Array>* out) {
  *out = ::pyarrow_unwrap_array(array);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Array from the passed Python object.");
  }
}

PyObject* wrap_array(const std::shared_ptr<Array>& array) {
  return ::pyarrow_wrap_array(array);
}

bool is_tensor(PyObject* tensor) { return ::pyarrow_is_tensor(tensor) != 0; }

Status unwrap_tensor(PyObject* tensor, std::shared_ptr<Tensor>* out) {
  *out = ::pyarrow_unwrap_tensor(tensor);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Tensor from the passed Python object.");
  }
}

PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor) {
  return ::pyarrow_wrap_tensor(tensor);
}

bool is_sparse_tensor_csr(PyObject* sparse_tensor) {
  return ::pyarrow_is_sparse_tensor_csr(sparse_tensor) != 0;
}

Status unwrap_sparse_tensor_csr(PyObject* sparse_tensor,
                                std::shared_ptr<SparseTensorCSR>* out) {
  *out = ::pyarrow_unwrap_sparse_tensor_csr(sparse_tensor);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid(
        "Could not unwrap SparseTensorCSR from the passed Python object.");
  }
}

PyObject* wrap_sparse_tensor_csr(const std::shared_ptr<SparseTensorCSR>& sparse_tensor) {
  return ::pyarrow_wrap_sparse_tensor_csr(sparse_tensor);
}

bool is_sparse_tensor_coo(PyObject* sparse_tensor) {
  return ::pyarrow_is_sparse_tensor_coo(sparse_tensor) != 0;
}

Status unwrap_sparse_tensor_coo(PyObject* sparse_tensor,
                                std::shared_ptr<SparseTensorCOO>* out) {
  *out = ::pyarrow_unwrap_sparse_tensor_coo(sparse_tensor);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid(
        "Could not unwrap SparseTensorCOO from the passed Python object.");
  }
}

PyObject* wrap_sparse_tensor_coo(const std::shared_ptr<SparseTensorCOO>& sparse_tensor) {
  return ::pyarrow_wrap_sparse_tensor_coo(sparse_tensor);
}

bool is_column(PyObject* column) { return ::pyarrow_is_column(column) != 0; }

Status unwrap_column(PyObject* column, std::shared_ptr<Column>* out) {
  *out = ::pyarrow_unwrap_column(column);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Column from the passed Python object.");
  }
}

PyObject* wrap_column(const std::shared_ptr<Column>& column) {
  return ::pyarrow_wrap_column(column);
}

bool is_table(PyObject* table) { return ::pyarrow_is_table(table) != 0; }

Status unwrap_table(PyObject* table, std::shared_ptr<Table>* out) {
  *out = ::pyarrow_unwrap_table(table);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap Table from the passed Python object.");
  }
}

PyObject* wrap_table(const std::shared_ptr<Table>& table) {
  return ::pyarrow_wrap_table(table);
}

bool is_record_batch(PyObject* batch) { return ::pyarrow_is_batch(batch) != 0; }

Status unwrap_record_batch(PyObject* batch, std::shared_ptr<RecordBatch>* out) {
  *out = ::pyarrow_unwrap_batch(batch);
  if (*out) {
    return Status::OK();
  } else {
    return Status::Invalid("Could not unwrap RecordBatch from the passed Python object.");
  }
}

PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch) {
  return ::pyarrow_wrap_batch(batch);
}

namespace internal {

int check_status(const Status& status) { return ::pyarrow_internal_check_status(status); }

}  // namespace internal
}  // namespace py
}  // namespace arrow
