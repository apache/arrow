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

#include "arrow/status.h"

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

  Ndarray1DIndexer() : arr_(NULLPTR), data_(NULLPTR) {}

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

  bool is_strided() const { return stride_ != 1; }

  T& operator[](size_type index) { return data_[index * stride_]; }
  T& operator[](size_type index) const { return data_[index * stride_]; }

 private:
  PyArrayObject* arr_;
  T* data_;
  int64_t stride_;
};

// Handling of Numpy Types by their static numbers
// (the NPY_TYPES enum and related defines)

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
#if !NPY_INT32_IS_INT
    TYPE_CASE(INT, "intc")
#endif
#if !NPY_INT64_IS_LONG_LONG
    TYPE_CASE(LONGLONG, "longlong")
#endif
    TYPE_CASE(UINT8, "uint8")
    TYPE_CASE(UINT16, "uint16")
    TYPE_CASE(UINT32, "uint32")
    TYPE_CASE(UINT64, "uint64")
#if !NPY_INT32_IS_INT
    TYPE_CASE(UINT, "uintc")
#endif
#if !NPY_INT64_IS_LONG_LONG
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
  std::stringstream ss;
  ss << "unrecognized type (" << npy_type << ") in GetNumPyTypeName";
  return ss.str();
}

#define TYPE_VISIT_INLINE(TYPE) \
  case NPY_##TYPE:              \
    return visitor->template Visit<NPY_##TYPE>(arr);

template <typename VISITOR>
inline Status VisitNumpyArrayInline(PyArrayObject* arr, VISITOR* visitor) {
  switch (PyArray_TYPE(arr)) {
    TYPE_VISIT_INLINE(BOOL);
    TYPE_VISIT_INLINE(INT8);
    TYPE_VISIT_INLINE(UINT8);
    TYPE_VISIT_INLINE(INT16);
    TYPE_VISIT_INLINE(UINT16);
    TYPE_VISIT_INLINE(INT32);
    TYPE_VISIT_INLINE(UINT32);
    TYPE_VISIT_INLINE(INT64);
    TYPE_VISIT_INLINE(UINT64);
#if !NPY_INT32_IS_INT
    TYPE_VISIT_INLINE(INT);
    TYPE_VISIT_INLINE(UINT);
#endif
#if !NPY_INT64_IS_LONG_LONG
    TYPE_VISIT_INLINE(LONGLONG);
    TYPE_VISIT_INLINE(ULONGLONG);
#endif
    TYPE_VISIT_INLINE(FLOAT16);
    TYPE_VISIT_INLINE(FLOAT32);
    TYPE_VISIT_INLINE(FLOAT64);
    TYPE_VISIT_INLINE(DATETIME);
    TYPE_VISIT_INLINE(OBJECT);
  }
  std::stringstream ss;
  ss << "NumPy type not implemented: " << GetNumPyTypeName(PyArray_TYPE(arr));
  return Status::NotImplemented(ss.str());
}

#undef TYPE_VISIT_INLINE

namespace internal {

inline bool PyFloatScalar_Check(PyObject* obj) {
  return PyFloat_Check(obj) || PyArray_IsScalar(obj, Floating);
}

inline bool PyIntScalar_Check(PyObject* obj) {
#if PY_MAJOR_VERSION < 3
  if (PyInt_Check(obj)) {
    return true;
  }
#endif
  return PyLong_Check(obj) || PyArray_IsScalar(obj, Integer);
}

}  // namespace internal

// Form a consensus dtype to use for Arrow conversion for a collection of dtype
// objects observed one at a time

#define _NUMPY_UNIFY_NOOP(DTYPE) \
  case NPY_##DTYPE:              \
    return NOOP;

#define _NUMPY_UNIFY_PROMOTE(DTYPE) \
  case NPY_##DTYPE:                 \
    return PROMOTE;

class NumPyDtypeUnifier {
 public:
  enum Action { NOOP, PROMOTE, INVALID };

  NumPyDtypeUnifier() : current_type_num_(-1), current_dtype_(NULLPTR) {}

  Status InvalidMix(int new_dtype) {
    std::stringstream ss;
    ss << "Cannot mix NumPy dtypes " << GetNumPyTypeName(current_type_num_) << " and "
       << GetNumPyTypeName(new_dtype);
    return Status::Invalid(ss.str());
  }

  int Observe_BOOL(PyArray_Descr* descr, int dtype) { return INVALID; }

  int Observe_INT8(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_PROMOTE(INT16);
      _NUMPY_UNIFY_PROMOTE(INT32);
      _NUMPY_UNIFY_PROMOTE(INT64);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_INT16(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(INT8);
      _NUMPY_UNIFY_PROMOTE(INT32);
      _NUMPY_UNIFY_PROMOTE(INT64);
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_INT32(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(INT8);
      _NUMPY_UNIFY_NOOP(INT16);
      _NUMPY_UNIFY_PROMOTE(INT32);
      _NUMPY_UNIFY_PROMOTE(INT64);
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_INT64(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(INT8);
      _NUMPY_UNIFY_NOOP(INT16);
      _NUMPY_UNIFY_NOOP(INT32);
      _NUMPY_UNIFY_NOOP(INT64);
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_NOOP(UINT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_UINT8(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_PROMOTE(UINT16);
      _NUMPY_UNIFY_PROMOTE(UINT32);
      _NUMPY_UNIFY_PROMOTE(UINT64);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_UINT16(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_PROMOTE(UINT32);
      _NUMPY_UNIFY_PROMOTE(UINT64);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_UINT32(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_PROMOTE(UINT64);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_UINT64(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_NOOP(UINT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_FLOAT16(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_PROMOTE(FLOAT32);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_FLOAT32(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(INT8);
      _NUMPY_UNIFY_NOOP(INT16);
      _NUMPY_UNIFY_NOOP(INT32);
      _NUMPY_UNIFY_NOOP(INT64);
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_NOOP(UINT32);
      _NUMPY_UNIFY_NOOP(UINT64);
      _NUMPY_UNIFY_PROMOTE(FLOAT64);
      default:
        return INVALID;
    }
  }

  int Observe_FLOAT64(PyArray_Descr* descr, int dtype) {
    switch (dtype) {
      _NUMPY_UNIFY_NOOP(INT8);
      _NUMPY_UNIFY_NOOP(INT16);
      _NUMPY_UNIFY_NOOP(INT32);
      _NUMPY_UNIFY_NOOP(INT64);
      _NUMPY_UNIFY_NOOP(UINT8);
      _NUMPY_UNIFY_NOOP(UINT16);
      _NUMPY_UNIFY_NOOP(UINT32);
      _NUMPY_UNIFY_NOOP(UINT64);
      default:
        return INVALID;
    }
  }

  int Observe_DATETIME(PyArray_Descr* dtype_obj) {
    // TODO: check that units are all the same
    // current_dtype_ = dtype_obj->type_num;
    return NOOP;
  }

  Status Observe(PyArray_Descr* descr) {
    const int dtype = fix_numpy_type_num(descr->type_num);

    if (current_type_num_ == -1) {
      current_dtype_ = descr;
      current_type_num_ = dtype;
      return Status::OK();
    } else if (current_type_num_ == dtype) {
      return Status::OK();
    }

#define OBSERVE_CASE(DTYPE)                 \
  case NPY_##DTYPE:                         \
    action = Observe_##DTYPE(descr, dtype); \
    break;

    int action = NOOP;
    switch (current_type_num_) {
      OBSERVE_CASE(BOOL);
      OBSERVE_CASE(INT8);
      OBSERVE_CASE(INT16);
      OBSERVE_CASE(INT32);
      OBSERVE_CASE(INT64);
      OBSERVE_CASE(UINT8);
      OBSERVE_CASE(UINT16);
      OBSERVE_CASE(UINT32);
      OBSERVE_CASE(UINT64);
      OBSERVE_CASE(FLOAT16);
      OBSERVE_CASE(FLOAT32);
      OBSERVE_CASE(FLOAT64);
      case NPY_DATETIME:
        action = Observe_DATETIME(descr);
        break;
      default:
        std::stringstream ss;
        ss << "Unsupported numpy type " << GetNumPyTypeName(dtype) << std::endl;
        return Status::NotImplemented(ss.str());
    }

    if (action == INVALID) {
      return InvalidMix(dtype);
    } else if (action == PROMOTE) {
      current_type_num_ = dtype;
      current_dtype_ = descr;
    }
    return Status::OK();
  }

  bool dtype_was_observed() const { return current_type_num_ != -1; }

  PyArray_Descr* current_dtype() const { return current_dtype_; }

 private:
  int current_type_num_;
  PyArray_Descr* current_dtype_;
};

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_NUMPY_INTERNAL_H
