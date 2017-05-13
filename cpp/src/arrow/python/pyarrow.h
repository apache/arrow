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
class Table;
class Tensor;

namespace py {

ARROW_EXPORT int import_pyarrow();
ARROW_EXPORT PyObject* wrap_buffer(const std::shared_ptr<Buffer>& buffer);
ARROW_EXPORT PyObject* wrap_data_type(const std::shared_ptr<DataType>& type);
ARROW_EXPORT PyObject* wrap_field(const std::shared_ptr<Field>& field);
ARROW_EXPORT PyObject* wrap_schema(const std::shared_ptr<Schema>& schema);
ARROW_EXPORT PyObject* wrap_array(const std::shared_ptr<Array>& array);
ARROW_EXPORT PyObject* wrap_tensor(const std::shared_ptr<Tensor>& tensor);
ARROW_EXPORT PyObject* wrap_column(const std::shared_ptr<Column>& column);
ARROW_EXPORT PyObject* wrap_table(const std::shared_ptr<Table>& table);
ARROW_EXPORT PyObject* wrap_record_batch(const std::shared_ptr<RecordBatch>& batch);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_PYARROW_H
