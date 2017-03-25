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

#ifndef PYARROW_COMMON_H
#define PYARROW_COMMON_H

#include "pyarrow/config.h"

#include "arrow/buffer.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

namespace py {

class PyAcquireGIL {
 public:
  PyAcquireGIL() { state_ = PyGILState_Ensure(); }

  ~PyAcquireGIL() { PyGILState_Release(state_); }

 private:
  PyGILState_STATE state_;
  DISALLOW_COPY_AND_ASSIGN(PyAcquireGIL);
};

#define PYARROW_IS_PY2 PY_MAJOR_VERSION <= 2

class OwnedRef {
 public:
  OwnedRef() : obj_(nullptr) {}

  OwnedRef(PyObject* obj) : obj_(obj) {}

  ~OwnedRef() {
    PyAcquireGIL lock;
    Py_XDECREF(obj_);
  }

  void reset(PyObject* obj) {
    if (obj_ != nullptr) { Py_XDECREF(obj_); }
    obj_ = obj;
  }

  void release() { obj_ = nullptr; }

  PyObject* obj() const { return obj_; }

 private:
  PyObject* obj_;
};

struct PyObjectStringify {
  OwnedRef tmp_obj;
  const char* bytes;

  PyObjectStringify(PyObject* obj) {
    PyObject* bytes_obj;
    if (PyUnicode_Check(obj)) {
      bytes_obj = PyUnicode_AsUTF8String(obj);
      tmp_obj.reset(bytes_obj);
    } else {
      bytes_obj = obj;
    }
    bytes = PyBytes_AsString(bytes_obj);
  }
};

// TODO(wesm): We can just let errors pass through. To be explored later
#define RETURN_IF_PYERROR()                         \
  if (PyErr_Occurred()) {                           \
    PyObject *exc_type, *exc_value, *traceback;     \
    PyErr_Fetch(&exc_type, &exc_value, &traceback); \
    PyObjectStringify stringified(exc_value);       \
    std::string message(stringified.bytes);         \
    Py_DECREF(exc_type);                            \
    Py_XDECREF(exc_value);                          \
    Py_XDECREF(traceback);                          \
    PyErr_Clear();                                  \
    return Status::UnknownError(message);           \
  }

// Return the common PyArrow memory pool
ARROW_EXPORT void set_default_memory_pool(MemoryPool* pool);
ARROW_EXPORT MemoryPool* get_memory_pool();

class ARROW_EXPORT NumPyBuffer : public Buffer {
 public:
  NumPyBuffer(PyArrayObject* arr) : Buffer(nullptr, 0) {
    arr_ = arr;
    Py_INCREF(arr);

    data_ = reinterpret_cast<const uint8_t*>(PyArray_DATA(arr_));
    size_ = PyArray_SIZE(arr_) * PyArray_DESCR(arr_)->elsize;
    capacity_ = size_;
  }

  virtual ~NumPyBuffer() { Py_XDECREF(arr_); }

 private:
  PyArrayObject* arr_;
};

class ARROW_EXPORT PyBuffer : public Buffer {
 public:
  /// Note that the GIL must be held when calling the PyBuffer constructor.
  ///
  /// While memoryview objects support multi-demensional buffers, PyBuffer only supports
  /// one-dimensional byte buffers.
  PyBuffer(PyObject* obj);
  ~PyBuffer();

 private:
  PyObject* obj_;
};

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_COMMON_H
