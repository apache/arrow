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

#define RETURN_IF_PYERROR()                             \
  if (PyErr_Occurred()) {                               \
    PyObject *exc_type, *exc_value, *traceback;         \
    PyErr_Fetch(&exc_type, &exc_value, &traceback);     \
    std::string message(PyString_AsString(exc_value));  \
    Py_DECREF(exc_type);                                \
    Py_DECREF(exc_value);                               \
    Py_DECREF(traceback);                               \
    return Status::UnknownError(message);               \
  }

class OwnedRef {
 public:
  OwnedRef(PyObject* obj) :
      obj_(obj) {}

  ~OwnedRef() {
    Py_XDECREF(obj_);
  }

  PyObject* obj() const{
    return obj_;
  }

 private:
  PyObject* obj_;
};

arrow::MemoryPool* GetMemoryPool();

} // namespace pyarrow

#endif // PYARROW_COMMON_H
