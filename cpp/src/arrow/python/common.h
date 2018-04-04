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

#ifndef ARROW_PYTHON_COMMON_H
#define ARROW_PYTHON_COMMON_H

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/python/config.h"

#include "arrow/buffer.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

namespace py {

Status CheckPyError(StatusCode code = StatusCode::UnknownError);

Status PassPyError();

// TODO(wesm): We can just let errors pass through. To be explored later
#define RETURN_IF_PYERROR() RETURN_NOT_OK(CheckPyError());

#define PY_RETURN_IF_ERROR(CODE) RETURN_NOT_OK(CheckPyError(CODE));

class ARROW_EXPORT PyAcquireGIL {
 public:
  PyAcquireGIL() : acquired_gil_(false) { acquire(); }

  ~PyAcquireGIL() { release(); }

  void acquire() {
    if (!acquired_gil_) {
      state_ = PyGILState_Ensure();
      acquired_gil_ = true;
    }
  }

  // idempotent
  void release() {
    if (acquired_gil_) {
      PyGILState_Release(state_);
      acquired_gil_ = false;
    }
  }

 private:
  bool acquired_gil_;
  PyGILState_STATE state_;
  ARROW_DISALLOW_COPY_AND_ASSIGN(PyAcquireGIL);
};

#define PYARROW_IS_PY2 PY_MAJOR_VERSION <= 2

// A RAII primitive that DECREFs the underlying PyObject* when it
// goes out of scope.
class ARROW_EXPORT OwnedRef {
 public:
  OwnedRef() : obj_(NULLPTR) {}
  OwnedRef(OwnedRef&& other) : OwnedRef(other.detach()) {}
  explicit OwnedRef(PyObject* obj) : obj_(obj) {}

  OwnedRef& operator=(OwnedRef&& other) {
    obj_ = other.detach();
    return *this;
  }

  ~OwnedRef() { reset(); }

  void reset(PyObject* obj) {
    Py_XDECREF(obj_);
    obj_ = obj;
  }

  void reset() { reset(NULLPTR); }

  PyObject* detach() {
    PyObject* result = obj_;
    obj_ = NULLPTR;
    return result;
  }

  PyObject* obj() const { return obj_; }

  PyObject** ref() { return &obj_; }

  operator bool() const { return obj_ != NULLPTR; }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(OwnedRef);

  PyObject* obj_;
};

// Same as OwnedRef, but ensures the GIL is taken when it goes out of scope.
// This is for situations where the GIL is not always known to be held
// (e.g. if it is released in the middle of a function for performance reasons)
class ARROW_EXPORT OwnedRefNoGIL : public OwnedRef {
 public:
  OwnedRefNoGIL() : OwnedRef() {}
  OwnedRefNoGIL(OwnedRefNoGIL&& other) : OwnedRef(other.detach()) {}
  explicit OwnedRefNoGIL(PyObject* obj) : OwnedRef(obj) {}

  ~OwnedRefNoGIL() {
    PyAcquireGIL lock;
    reset();
  }
};

// A temporary conversion of a Python object to a bytes area.
struct ARROW_EXPORT PyBytesView {
  const char* bytes;
  Py_ssize_t size;

  PyBytesView() : bytes(nullptr), size(0), ref(nullptr) {}

  // View the given Python object as binary-like, i.e. bytes
  static Status FromBinary(PyObject* obj, PyBytesView* out) {
    return FromBinary(obj, out, "a bytes object");
  }

  // View the given Python object as string-like, i.e. str or (utf8) bytes
  // XXX make this non-static?
  static Status FromString(PyObject* obj, PyBytesView* out, bool check_valid = false) {
    if (PyUnicode_Check(obj)) {
#if PY_MAJOR_VERSION >= 3
      Py_ssize_t size;
      // The utf-8 representation is cached on the unicode object
      const char* data = PyUnicode_AsUTF8AndSize(obj, &size);
      RETURN_IF_PYERROR();
      *out = PyBytesView(data, size);
      return Status::OK();
#else
      PyObject* converted = PyUnicode_AsUTF8String(obj);
      RETURN_IF_PYERROR();
      *out = PyBytesView(PyBytes_AS_STRING(converted), PyBytes_GET_SIZE(converted),
                         converted);
      return Status::OK();
#endif
    } else {
      PyBytesView view;
      RETURN_NOT_OK(FromBinary(obj, &view, "a string or bytes object"));
      if (check_valid) {
        // Check the bytes are valid utf-8
        OwnedRef decoded(PyUnicode_FromStringAndSize(view.bytes, view.size));
        RETURN_IF_PYERROR();
      }
      *out = std::move(view);
      return Status::OK();
    }
  }

 protected:
  PyBytesView(const char* b, Py_ssize_t s, PyObject* obj = nullptr)
      : bytes(b), size(s), ref(obj) {}

  static Status FromBinary(PyObject* obj, PyBytesView* out,
                           const std::string& expected_msg) {
    if (PyBytes_Check(obj)) {
      *out = PyBytesView(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj));
      return Status::OK();
    } else if (PyByteArray_Check(obj)) {
      *out = PyBytesView(PyByteArray_AS_STRING(obj), PyByteArray_GET_SIZE(obj));
      return Status::OK();
    } else {
      std::stringstream ss;
      ss << "Expected " << expected_msg << ", got a '" << Py_TYPE(obj)->tp_name
         << "' object";
      return Status::TypeError(ss.str());
    }
  }

  OwnedRef ref;
};

// Return the common PyArrow memory pool
ARROW_EXPORT void set_default_memory_pool(MemoryPool* pool);
ARROW_EXPORT MemoryPool* get_memory_pool();

class ARROW_EXPORT PyBuffer : public Buffer {
 public:
  /// While memoryview objects support multi-dimensional buffers, PyBuffer only supports
  /// one-dimensional byte buffers.
  ~PyBuffer();

  static Status FromPyObject(PyObject* obj, std::shared_ptr<Buffer>* out);

 private:
  PyBuffer();
  Status Init(PyObject*);

  Py_buffer py_buf_;
};

}  // namespace py
}  // namespace arrow

#endif  // ARROW_PYTHON_COMMON_H
