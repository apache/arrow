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

int import_pyarrow() {
  return ::import_pyarrow__lib();
}

PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer) {
  return ::pyarrow_wrap_buffer(buffer);
}

PyObject* wrap_data_type(const std::shared_ptr<DataType>& type) {
  return ::pyarrow_wrap_data_type(type);
}

PyObject* wrap_field(const std::shared_ptr<Field>& field) {
  return ::pyarrow_wrap_field(field);
}

PyObject* wrap_schema(const std::shared_ptr<Schema>& schema) {
  return ::pyarrow_wrap_schema(schema);
}

PyObject* wrap_array(const std::shared_ptr<Array>& array) {
  return ::pyarrow_wrap_array(array);
}

PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor) {
  return ::pyarrow_wrap_tensor(tensor);
}

PyObject* wrap_column(const std::shared_ptr<Column>& column) {
  return ::pyarrow_wrap_column(column);
}

PyObject* wrap_table(const std::shared_ptr<Table>& table) {
  return ::pyarrow_wrap_table(table);
}

PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch) {
  return ::pyarrow_wrap_batch(batch);
}

}  // namespace py
}  // namespace arrow
