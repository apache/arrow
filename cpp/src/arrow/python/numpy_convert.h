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

#include "arrow/python/platform.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/python/visibility.h"
#include "arrow/sparse_tensor.h"

namespace arrow {

class DataType;
class MemoryPool;
class Status;
class Tensor;

namespace py {

class ARROW_PYTHON_EXPORT NumPyBuffer : public Buffer {
 public:
  explicit NumPyBuffer(PyObject* arr);
  virtual ~NumPyBuffer();

 private:
  PyObject* arr_;
};

ARROW_PYTHON_EXPORT
Status NumPyDtypeToArrow(PyObject* dtype, std::shared_ptr<DataType>* out);
ARROW_PYTHON_EXPORT
Status NumPyDtypeToArrow(PyArray_Descr* descr, std::shared_ptr<DataType>* out);

ARROW_PYTHON_EXPORT Status NdarrayToTensor(MemoryPool* pool, PyObject* ao,
                                           const std::vector<std::string>& dim_names,
                                           std::shared_ptr<Tensor>* out);

ARROW_PYTHON_EXPORT Status TensorToNdarray(const std::shared_ptr<Tensor>& tensor,
                                           PyObject* base, PyObject** out);

ARROW_PYTHON_EXPORT Status
SparseTensorCOOToNdarray(const std::shared_ptr<SparseTensorCOO>& sparse_tensor,
                         PyObject* base, PyObject** out_data, PyObject** out_coords);

ARROW_PYTHON_EXPORT Status SparseTensorCSRToNdarray(
    const std::shared_ptr<SparseTensorCSR>& sparse_tensor, PyObject* base,
    PyObject** out_data, PyObject** out_indptr, PyObject** out_indices);

ARROW_PYTHON_EXPORT Status NdarraysToSparseTensorCOO(
    MemoryPool* pool, PyObject* data_ao, PyObject* coords_ao,
    const std::vector<int64_t>& shape, const std::vector<std::string>& dim_names,
    std::shared_ptr<SparseTensorCOO>* out);

ARROW_PYTHON_EXPORT Status NdarraysToSparseTensorCSR(
    MemoryPool* pool, PyObject* data_ao, PyObject* indptr_ao, PyObject* indices_ao,
    const std::vector<int64_t>& shape, const std::vector<std::string>& dim_names,
    std::shared_ptr<SparseTensorCSR>* out);

ARROW_PYTHON_EXPORT Status
TensorToSparseTensorCOO(const std::shared_ptr<Tensor>& tensor,
                        std::shared_ptr<SparseTensorCOO>* csparse_tensor);

ARROW_PYTHON_EXPORT Status
TensorToSparseTensorCSR(const std::shared_ptr<Tensor>& tensor,
                        std::shared_ptr<SparseTensorCSR>* csparse_tensor);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_NUMPY_CONVERT_H
