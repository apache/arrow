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

// Functions for converting between CPython built-in data structures and Arrow
// data structures

#ifndef ARROW_PYTHON_ADAPTERS_BUILTIN_H
#define ARROW_PYTHON_ADAPTERS_BUILTIN_H

#include <Python.h>

#include <memory>

#include "arrow/type.h"

#include "arrow/util/visibility.h"

#include "arrow/python/common.h"

namespace arrow {

class Array;
class Status;

namespace py {

ARROW_EXPORT arrow::Status InferArrowTypeAndSize(
    PyObject* obj, int64_t* size, std::shared_ptr<arrow::DataType>* out_type);
ARROW_EXPORT arrow::Status InferArrowSize(PyObject* obj, int64_t* size);

ARROW_EXPORT arrow::Status AppendPySequence(PyObject* obj,
    const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<arrow::ArrayBuilder>& builder);

// Type and size inference
ARROW_EXPORT
Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out);

// Size inference
ARROW_EXPORT
Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out,
    const std::shared_ptr<DataType>& type);

// No inference
ARROW_EXPORT
Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out,
    const std::shared_ptr<DataType>& type, int64_t size);

ARROW_EXPORT Status CheckPythonBytesAreFixedLength(
    PyObject* obj, Py_ssize_t expected_length);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ADAPTERS_BUILTIN_H
