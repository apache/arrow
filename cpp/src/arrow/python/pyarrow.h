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

#ifndef ARROW_PYTHON_PYARROW_H
#define ARROW_PYTHON_PYARROW_H

#include "arrow/python/platform.h"

#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class Column;
class DataType;
class Field;
class RecordBatch;
class Schema;
class Status;
class Table;
class Tensor;

namespace py {

ARROW_EXPORT int import_pyarrow();

ARROW_EXPORT bool is_buffer(PyObject* buffer);
ARROW_EXPORT Status unwrap_buffer(PyObject* buffer, std::shared_ptr<Buffer>* out);
ARROW_EXPORT PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer);

ARROW_EXPORT bool is_data_type(PyObject* data_type);
ARROW_EXPORT Status unwrap_data_type(PyObject* data_type, std::shared_ptr<DataType>* out);
ARROW_EXPORT PyObject* wrap_data_type(const std::shared_ptr<DataType>& type);

ARROW_EXPORT bool is_field(PyObject* field);
ARROW_EXPORT Status unwrap_field(PyObject* field, std::shared_ptr<Field>* out);
ARROW_EXPORT PyObject* wrap_field(const std::shared_ptr<Field>& field);

ARROW_EXPORT bool is_schema(PyObject* schema);
ARROW_EXPORT Status unwrap_schema(PyObject* schema, std::shared_ptr<Schema>* out);
ARROW_EXPORT PyObject* wrap_schema(const std::shared_ptr<Schema>& schema);

ARROW_EXPORT bool is_array(PyObject* array);
ARROW_EXPORT Status unwrap_array(PyObject* array, std::shared_ptr<Array>* out);
ARROW_EXPORT PyObject* wrap_array(const std::shared_ptr<Array>& array);

ARROW_EXPORT bool is_tensor(PyObject* tensor);
ARROW_EXPORT Status unwrap_tensor(PyObject* tensor, std::shared_ptr<Tensor>* out);
ARROW_EXPORT PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor);

ARROW_EXPORT bool is_column(PyObject* column);
ARROW_EXPORT Status unwrap_column(PyObject* column, std::shared_ptr<Column>* out);
ARROW_EXPORT PyObject* wrap_column(const std::shared_ptr<Column>& column);

ARROW_EXPORT bool is_table(PyObject* table);
ARROW_EXPORT Status unwrap_table(PyObject* table, std::shared_ptr<Table>* out);
ARROW_EXPORT PyObject* wrap_table(const std::shared_ptr<Table>& table);

ARROW_EXPORT bool is_record_batch(PyObject* batch);
ARROW_EXPORT Status unwrap_record_batch(
    PyObject* batch, std::shared_ptr<RecordBatch>* out);
ARROW_EXPORT PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYARROW_H
