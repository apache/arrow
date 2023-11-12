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

#pragma once

#include "arrow/python/common.h"
#include "arrow/status.h"

namespace arrow {

namespace py {

static Status UnwrapError(PyObject* obj, const char* expected_type) {
  return Status::TypeError("Could not unwrap ", expected_type,
                           " from Python object of type '", Py_TYPE(obj)->tp_name, "'");
}

#define DECLARE_WRAP_FUNCTIONS(FUNC_SUFFIX, TYPE_NAME)                   \
  ARROW_PYTHON_EXPORT bool is_##FUNC_SUFFIX(PyObject*);                  \
  ARROW_PYTHON_EXPORT Result<TYPE_NAME> unwrap_##FUNC_SUFFIX(PyObject*); \
  ARROW_PYTHON_EXPORT PyObject* wrap_##FUNC_SUFFIX(const TYPE_NAME&);

#define DEFINE_WRAP_FUNCTIONS(FUNC_SUFFIX, TYPE_NAME)                                   \
  bool is_##FUNC_SUFFIX(PyObject* obj) { return ::pyarrow_is_##FUNC_SUFFIX(obj) != 0; } \
                                                                                        \
  PyObject* wrap_##FUNC_SUFFIX(const TYPE_NAME& src) {                                  \
    return ::pyarrow_wrap_##FUNC_SUFFIX(src);                                           \
  }                                                                                     \
  Result<TYPE_NAME> unwrap_##FUNC_SUFFIX(PyObject* obj) {                               \
    auto out = ::pyarrow_unwrap_##FUNC_SUFFIX(obj);                                     \
    if (IS_VALID(out)) {                                                                \
      return std::move(out);                                                            \
    } else {                                                                            \
      return UnwrapError(obj, #TYPE_NAME);                                              \
    }                                                                                   \
  }
}  // namespace py
}  // namespace arrow
