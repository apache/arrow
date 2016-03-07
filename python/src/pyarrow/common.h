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

#include <Python.h>

namespace arrow { class MemoryPool; }

namespace pyarrow {

#define PYARROW_IS_PY2 PY_MAJOR_VERSION < 2

#define RETURN_ARROW_NOT_OK(s) do {             \
    arrow::Status _s = (s);                     \
    if (!_s.ok()) {                             \
      return Status::ArrowError(s.ToString());  \
    }                                           \
  } while (0);

class OwnedRef {
 public:
  OwnedRef() : obj_(nullptr) {}

  OwnedRef(PyObject* obj) :
      obj_(obj) {}

  ~OwnedRef() {
    Py_XDECREF(obj_);
  }

  void reset(PyObject* obj) {
    if (obj_ != nullptr) {
      Py_XDECREF(obj_);
    }
    obj_ = obj;
  }

  PyObject* obj() const{
    return obj_;
  }

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
    Py_DECREF(exc_value);                           \
    Py_DECREF(traceback);                           \
    return Status::UnknownError(message);           \
  }

arrow::MemoryPool* GetMemoryPool();

} // namespace pyarrow

#endif // PYARROW_COMMON_H
