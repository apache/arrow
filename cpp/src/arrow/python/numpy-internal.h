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
#include <string>

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

  T* data() const { return data_; }

  T* begin() const { return data(); }
  T* end() const { return begin() + size() * stride_; }

  bool is_strided() const { return stride_ == 1; }

  T& operator[](size_type index) { return data_[index * stride_]; }
  T& operator[](size_type index) const { return data_[index * stride_]; }

 private:
  PyArrayObject* arr_;
  T* data_;
  int64_t stride_;
};

static inline std::string GetNumPyTypeName(int npy_type) {
#define TYPE_CASE(TYPE, NAME) \
  case NPY_##TYPE:            \
    return NAME;

  switch (npy_type) {
    TYPE_CASE(BOOL, "bool")
    TYPE_CASE(INT8, "int8")
    TYPE_CASE(INT16, "int16")
    TYPE_CASE(INT32, "int32")
    TYPE_CASE(INT64, "int64")
#if (NPY_INT64 != NPY_LONGLONG)
    TYPE_CASE(LONGLONG, "longlong")
#endif
    TYPE_CASE(UINT8, "uint8")
    TYPE_CASE(UINT16, "uint16")
    TYPE_CASE(UINT32, "uint32")
    TYPE_CASE(UINT64, "uint64")
#if (NPY_UINT64 != NPY_ULONGLONG)
    TYPE_CASE(ULONGLONG, "ulonglong")
#endif
    TYPE_CASE(FLOAT16, "float16")
    TYPE_CASE(FLOAT32, "float32")
    TYPE_CASE(FLOAT64, "float64")
    TYPE_CASE(DATETIME, "datetime64")
    TYPE_CASE(OBJECT, "object")
    TYPE_CASE(VOID, "void")
    default:
      break;
  }

#undef TYPE_CASE
  return "unrecognized type in GetNumPyTypeName";
}

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_NUMPY_INTERNAL_H
