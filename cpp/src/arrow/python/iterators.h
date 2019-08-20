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

#ifndef ARROW_PYTHON_ITERATORS_H
#define ARROW_PYTHON_ITERATORS_H

#include <utility>

#include "arrow/python/common.h"
#include "arrow/python/numpy_internal.h"

namespace arrow {
namespace py {
namespace internal {

// Visit the Python sequence, calling the given callable on each element.  If
// the callable returns a non-OK status, iteration stops and the status is
// returned.
//
// The call signature for Visitor must be
//
// Visit(PyObject* obj, int64_t index, bool* keep_going)
//
// If keep_going is set to false, the iteration terminates
template <class VisitorFunc>
inline Status VisitSequenceGeneric(PyObject* obj, VisitorFunc&& func) {
  // VisitorFunc may set to false to terminate iteration
  bool keep_going = true;

  if (PyArray_Check(obj)) {
    PyArrayObject* arr_obj = reinterpret_cast<PyArrayObject*>(obj);
    if (PyArray_NDIM(arr_obj) != 1) {
      return Status::Invalid("Only 1D arrays accepted");
    }

    if (PyArray_DESCR(arr_obj)->type_num == NPY_OBJECT) {
      // It's an array object, we can fetch object pointers directly
      const Ndarray1DIndexer<PyObject*> objects(arr_obj);
      for (int64_t i = 0; keep_going && i < objects.size(); ++i) {
        RETURN_NOT_OK(func(objects[i], i, &keep_going));
      }
      return Status::OK();
    }
    // It's a non-object array, fall back on regular sequence access.
    // (note PyArray_GETITEM() is slightly different: it returns standard
    //  Python types, not Numpy scalar types)
    // This code path is inefficient: callers should implement dedicated
    // logic for non-object arrays.
  }
  if (PySequence_Check(obj)) {
    if (PyList_Check(obj) || PyTuple_Check(obj)) {
      // Use fast item access
      const Py_ssize_t size = PySequence_Fast_GET_SIZE(obj);
      for (Py_ssize_t i = 0; keep_going && i < size; ++i) {
        PyObject* value = PySequence_Fast_GET_ITEM(obj, i);
        RETURN_NOT_OK(func(value, static_cast<int64_t>(i), &keep_going));
      }
    } else {
      // Regular sequence: avoid making a potentially large copy
      const Py_ssize_t size = PySequence_Size(obj);
      RETURN_IF_PYERROR();
      for (Py_ssize_t i = 0; keep_going && i < size; ++i) {
        OwnedRef value_ref(PySequence_ITEM(obj, i));
        RETURN_IF_PYERROR();
        RETURN_NOT_OK(func(value_ref.obj(), static_cast<int64_t>(i), &keep_going));
      }
    }
  } else {
    return Status::TypeError("Object is not a sequence");
  }
  return Status::OK();
}

// Visit sequence with no null mask
template <class VisitorFunc>
inline Status VisitSequence(PyObject* obj, VisitorFunc&& func) {
  return VisitSequenceGeneric(
      obj, [&func](PyObject* value, int64_t i /* unused */, bool* keep_going) {
        return func(value, keep_going);
      });
}

/// Visit sequence with null mask
template <class VisitorFunc>
inline Status VisitSequenceMasked(PyObject* obj, PyObject* mo, VisitorFunc&& func) {
  if (mo == nullptr || !PyArray_Check(mo)) {
    return Status::Invalid("Null mask must be NumPy array");
  }

  PyArrayObject* mask = reinterpret_cast<PyArrayObject*>(mo);
  if (PyArray_NDIM(mask) != 1) {
    return Status::Invalid("Mask must be 1D array");
  }

  const Py_ssize_t obj_size = PySequence_Size(obj);
  if (PyArray_SIZE(mask) != static_cast<int64_t>(obj_size)) {
    return Status::Invalid("Mask was a different length from sequence being converted");
  }

  const int dtype = fix_numpy_type_num(PyArray_DESCR(mask)->type_num);
  if (dtype == NPY_BOOL) {
    Ndarray1DIndexer<uint8_t> mask_values(mask);

    return VisitSequenceGeneric(
        obj, [&func, &mask_values](PyObject* value, int64_t i, bool* keep_going) {
          return func(value, mask_values[i], keep_going);
        });
  } else {
    return Status::Invalid("Mask must be boolean dtype");
  }
}

// Like IterateSequence, but accepts any generic iterable (including
// non-restartable iterators, e.g. generators).
//
// The call signature for VisitorFunc must be Visit(PyObject*, bool*
// keep_going). If keep_going is set to false, the iteration terminates
template <class VisitorFunc>
inline Status VisitIterable(PyObject* obj, VisitorFunc&& func) {
  if (PySequence_Check(obj)) {
    // Numpy arrays fall here as well
    return VisitSequence(obj, std::forward<VisitorFunc>(func));
  }
  // Fall back on the iterator protocol
  OwnedRef iter_ref(PyObject_GetIter(obj));
  PyObject* iter = iter_ref.obj();
  RETURN_IF_PYERROR();
  PyObject* value;

  bool keep_going = true;
  while (keep_going && (value = PyIter_Next(iter))) {
    OwnedRef value_ref(value);
    RETURN_NOT_OK(func(value_ref.obj(), &keep_going));
  }
  RETURN_IF_PYERROR();  // __next__() might have raised
  return Status::OK();
}

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_ITERATORS_H
