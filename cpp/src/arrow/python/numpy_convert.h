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

#ifndef ARROW_PYTHON_NUMPY_CONVERT_H
#define ARROW_PYTHON_NUMPY_CONVERT_H

#include <Python.h>

#include <memory>
#include <string>

#include "arrow/buffer.h"
#include "arrow/util/visibility.h"

namespace arrow {

struct DataType;
class MemoryPool;
class Status;
class Tensor;

namespace py {

class ARROW_EXPORT NumPyBuffer : public Buffer {
 public:
  explicit NumPyBuffer(PyObject* arr);
  virtual ~NumPyBuffer();

 private:
  PyObject* arr_;
};

// Handle misbehaved types like LONGLONG and ULONGLONG
int cast_npy_type_compat(int type_num);

bool is_contiguous(PyObject* array);

ARROW_EXPORT
Status NumPyDtypeToArrow(PyObject* dtype, std::shared_ptr<DataType>* out);

Status GetTensorType(PyObject* dtype, std::shared_ptr<DataType>* out);
Status GetNumPyType(const DataType& type, int* type_num);

ARROW_EXPORT Status NdarrayToTensor(
    MemoryPool* pool, PyObject* ao, std::shared_ptr<Tensor>* out);

ARROW_EXPORT Status TensorToNdarray(const Tensor& tensor, PyObject* base, PyObject** out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_NUMPY_CONVERT_H
