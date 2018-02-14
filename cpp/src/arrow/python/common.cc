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
#include <sstream>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

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
    DCHECK(data_ != nullptr);
    size_ = py_buf_.len;
    capacity_ = py_buf_.len;
    is_mutable_ = !py_buf_.readonly;
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

Status CheckPyError(StatusCode code) {
  if (PyErr_Occurred()) {
    PyObject* exc_type = nullptr;
    PyObject* exc_value = nullptr;
    PyObject* traceback = nullptr;

    OwnedRef exc_type_ref(exc_type);
    OwnedRef exc_value_ref(exc_value);
    OwnedRef traceback_ref(traceback);

    PyErr_Fetch(&exc_type, &exc_value, &traceback);

    PyErr_NormalizeException(&exc_type, &exc_value, &traceback);

    OwnedRef exc_value_str(PyObject_Str(exc_value));
    PyObjectStringify stringified(exc_value_str.obj());
    std::string message(stringified.bytes);

    PyErr_Clear();
    return Status(code, message);
  }
  return Status::OK();
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
