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

#ifndef ARROW_PYTHON_ARROW_TO_PYTHON_H
#define ARROW_PYTHON_ARROW_TO_PYTHON_H

#include <Python.h>

#include "arrow/api.h"

#include <vector>

extern "C" {
extern PyObject* pyarrow_serialize_callback;
extern PyObject* pyarrow_deserialize_callback;
}

namespace arrow {
namespace py {

Status CallCustomCallback(PyObject* callback, PyObject* elem, PyObject** result);

arrow::Status DeserializeList(std::shared_ptr<arrow::Array> array, int32_t start_idx,
                              int32_t stop_idx, PyObject* base,
                              const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                              PyObject** out);

arrow::Status DeserializeTuple(std::shared_ptr<arrow::Array> array, int32_t start_idx,
                               int32_t stop_idx, PyObject* base,
                               const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                               PyObject** out);

arrow::Status DeserializeDict(std::shared_ptr<arrow::Array> array, int32_t start_idx,
                              int32_t stop_idx, PyObject* base,
                              const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                              PyObject** out);

arrow::Status DeserializeArray(std::shared_ptr<arrow::Array> array, int32_t offset,
                               PyObject* base,
                               const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                               PyObject** out);

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ARROW_TO_PYTHON_H
