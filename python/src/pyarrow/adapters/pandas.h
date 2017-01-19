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

// Functions for converting between pandas's NumPy-based data representation
// and Arrow data structures

#ifndef PYARROW_ADAPTERS_PANDAS_H
#define PYARROW_ADAPTERS_PANDAS_H

#include <Python.h>

#include <memory>

#include "pyarrow/visibility.h"

namespace arrow {

class Array;
class Column;
class Field;
class MemoryPool;
class Status;
class Table;

}  // namespace arrow

namespace pyarrow {

PYARROW_EXPORT
arrow::Status ConvertArrayToPandas(
    const std::shared_ptr<arrow::Array>& arr, PyObject* py_ref, PyObject** out);

PYARROW_EXPORT
arrow::Status ConvertColumnToPandas(
    const std::shared_ptr<arrow::Column>& col, PyObject* py_ref, PyObject** out);

struct PandasOptions {
  bool strings_to_categorical;
};

// Convert a whole table as efficiently as possible to a pandas.DataFrame.
//
// The returned Python object is a list of tuples consisting of the exact 2D
// BlockManager structure of the pandas.DataFrame used as of pandas 0.19.x.
//
// tuple item: (indices: ndarray[int32], block: ndarray[TYPE, ndim=2])
PYARROW_EXPORT
arrow::Status ConvertTableToPandas(
    const std::shared_ptr<arrow::Table>& table, int nthreads, PyObject** out);

PYARROW_EXPORT
arrow::Status PandasToArrow(arrow::MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<arrow::Field>& field, std::shared_ptr<arrow::Array>* out);

}  // namespace pyarrow

#endif  // PYARROW_ADAPTERS_PANDAS_H
