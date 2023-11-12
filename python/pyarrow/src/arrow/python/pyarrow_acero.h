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

namespace acero {
struct Declaration;
}

namespace py {

// Returns 0 on success, -1 on error.
ARROW_PYTHON_EXPORT int import_pyarrow_acero();

#define DECLARE_WRAP_FUNCTIONS(FUNC_SUFFIX, TYPE_NAME)                   \
  ARROW_PYTHON_EXPORT bool is_##FUNC_SUFFIX(PyObject*);                  \
  ARROW_PYTHON_EXPORT Result<TYPE_NAME> unwrap_##FUNC_SUFFIX(PyObject*); \
  ARROW_PYTHON_EXPORT PyObject* wrap_##FUNC_SUFFIX(const TYPE_NAME&);

DECLARE_WRAP_FUNCTIONS(declaration, acero::Declaration)

#undef DECLARE_WRAP_FUNCTIONS

}  // namespace py
}  // namespace arrow

}  // extern "C++"
