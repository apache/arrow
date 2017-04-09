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

#ifndef ARROW_PYTHON_ADAPTERS_PANDAS_H
#define ARROW_PYTHON_ADAPTERS_PANDAS_H

#include <Python.h>

#include <memory>
#include <string>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Column;
struct DataType;
class MemoryPool;
class Status;
class Table;

namespace py {

ARROW_EXPORT
Status ConvertArrayToPandas(
    const std::shared_ptr<Array>& arr, PyObject* py_ref, PyObject** out);

ARROW_EXPORT
Status ConvertColumnToPandas(
    const std::shared_ptr<Column>& col, PyObject* py_ref, PyObject** out);

struct PandasOptions {
  bool strings_to_categorical;
};

// Convert a whole table as efficiently as possible to a pandas.DataFrame.
//
// The returned Python object is a list of tuples consisting of the exact 2D
// BlockManager structure of the pandas.DataFrame used as of pandas 0.19.x.
//
// tuple item: (indices: ndarray[int32], block: ndarray[TYPE, ndim=2])
ARROW_EXPORT
Status ConvertTableToPandas(
    const std::shared_ptr<Table>& table, int nthreads, PyObject** out);

ARROW_EXPORT
Status PandasToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out);

/// Convert dtype=object arrays. If target data type is not known, pass a type
/// with nullptr
ARROW_EXPORT
Status PandasObjectsToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ADAPTERS_PANDAS_H
