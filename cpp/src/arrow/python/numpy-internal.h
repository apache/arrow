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

// Internal utilities for dealing with NumPy

#ifndef ARROW_PYTHON_NUMPY_INTERNAL_H
#define ARROW_PYTHON_NUMPY_INTERNAL_H

#include "arrow/python/numpy_interop.h"

#include "arrow/python/platform.h"

#include <cstdint>

namespace arrow {
namespace py {

/// Indexing convenience for interacting with strided 1-dim ndarray objects
template <typename T>
class Ndarray1DIndexer {
 public:
  typedef int64_t size_type;

  Ndarray1DIndexer() : arr_(nullptr), data_(nullptr) {}

  explicit Ndarray1DIndexer(PyArrayObject* arr) : Ndarray1DIndexer() { Init(arr); }

  void Init(PyArrayObject* arr) {
    arr_ = arr;
    DCHECK_EQ(1, PyArray_NDIM(arr)) << "Only works with 1-dimensional arrays";
    Py_INCREF(arr);
    data_ = reinterpret_cast<T*>(PyArray_DATA(arr));
    stride_ = PyArray_STRIDES(arr)[0] / sizeof(T);
  }

  ~Ndarray1DIndexer() { Py_XDECREF(arr_); }

  int64_t size() const { return PyArray_SIZE(arr_); }

  T& operator[](size_type index) { return *(data_ + index * stride_); }

 private:
  PyArrayObject* arr_;
  T* data_;
  int64_t stride_;
};

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_NUMPY_INTERNAL_H
