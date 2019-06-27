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

#include "arrow/python/common.h"

#include <cstdlib>
#include <mutex>
#include <string>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/python/helpers.h"

namespace arrow {
namespace py {

static std::mutex memory_pool_mutex;
static MemoryPool* default_python_pool = nullptr;

void set_default_memory_pool(MemoryPool* pool) {
  std::lock_guard<std::mutex> guard(memory_pool_mutex);
  default_python_pool = pool;
}

MemoryPool* get_memory_pool() {
  std::lock_guard<std::mutex> guard(memory_pool_mutex);
  if (default_python_pool) {
    return default_python_pool;
  } else {
    return default_memory_pool();
  }
}

// ----------------------------------------------------------------------
// PyBuffer

PyBuffer::PyBuffer() : Buffer(nullptr, 0) {}

Status PyBuffer::Init(PyObject* obj) {
  if (!PyObject_GetBuffer(obj, &py_buf_, PyBUF_ANY_CONTIGUOUS)) {
    data_ = reinterpret_cast<const uint8_t*>(py_buf_.buf);
    ARROW_CHECK_NE(data_, nullptr) << "Null pointer in Py_buffer";
    size_ = py_buf_.len;
    capacity_ = py_buf_.len;
    is_mutable_ = !py_buf_.readonly;
    if (is_mutable_) {
      mutable_data_ = reinterpret_cast<uint8_t*>(py_buf_.buf);
    }
    return Status::OK();
  } else {
    return Status(StatusCode::PythonError, "");
  }
}

Status PyBuffer::FromPyObject(PyObject* obj, std::shared_ptr<Buffer>* out) {
  PyBuffer* buf = new PyBuffer();
  std::shared_ptr<Buffer> res(buf);
  RETURN_NOT_OK(buf->Init(obj));
  *out = res;
  return Status::OK();
}

PyBuffer::~PyBuffer() {
  if (data_ != nullptr) {
    PyAcquireGIL lock;
    PyBuffer_Release(&py_buf_);
  }
}

// ----------------------------------------------------------------------
// Python exception -> Status

Status ConvertPyError(StatusCode code) {
  PyObject* exc_type = nullptr;
  PyObject* exc_value = nullptr;
  PyObject* traceback = nullptr;

  PyErr_Fetch(&exc_type, &exc_value, &traceback);
  PyErr_NormalizeException(&exc_type, &exc_value, &traceback);

  DCHECK_NE(exc_type, nullptr) << "ConvertPyError called without an exception set";

  OwnedRef exc_type_ref(exc_type);
  OwnedRef exc_value_ref(exc_value);
  OwnedRef traceback_ref(traceback);

  std::string message;
  RETURN_NOT_OK(internal::PyObject_StdStringStr(exc_value, &message));

  if (code == StatusCode::UnknownError) {
    // Try to match the Python exception type with an appropriate Status code
    if (PyErr_GivenExceptionMatches(exc_type, PyExc_MemoryError)) {
      code = StatusCode::OutOfMemory;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_IndexError)) {
      code = StatusCode::IndexError;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_KeyError)) {
      code = StatusCode::KeyError;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_TypeError)) {
      code = StatusCode::TypeError;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_ValueError) ||
               PyErr_GivenExceptionMatches(exc_type, PyExc_OverflowError)) {
      code = StatusCode::Invalid;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_EnvironmentError)) {
      code = StatusCode::IOError;
    } else if (PyErr_GivenExceptionMatches(exc_type, PyExc_NotImplementedError)) {
      code = StatusCode::NotImplemented;
    }
  }
  return Status(code, message);
}

Status PassPyError() {
  if (PyErr_Occurred()) {
    // Do not call PyErr_Clear, the assumption is that someone further
    // up the call stack will want to deal with the Python error.
    return Status(StatusCode::PythonError, "");
  }
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
