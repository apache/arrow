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

PyBuffer::PyBuffer(PyObject* obj) : Buffer(nullptr, 0), obj_(nullptr) {
  if (PyObject_CheckBuffer(obj)) {
    obj_ = PyMemoryView_FromObject(obj);
    Py_buffer* buffer = PyMemoryView_GET_BUFFER(obj_);
    data_ = reinterpret_cast<const uint8_t*>(buffer->buf);
    size_ = buffer->len;
    capacity_ = buffer->len;
    is_mutable_ = false;
  }
}

PyBuffer::~PyBuffer() {
  PyAcquireGIL lock;
  Py_XDECREF(obj_);
}

Status CheckPyError(StatusCode code) {
  if (PyErr_Occurred()) {
    PyObject *exc_type, *exc_value, *traceback;
    PyErr_Fetch(&exc_type, &exc_value, &traceback);
    PyErr_NormalizeException(&exc_type, &exc_value, &traceback);
    PyObject* exc_value_str = PyObject_Str(exc_value);
    PyObjectStringify stringified(exc_value_str);
    std::string message(stringified.bytes);
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_value_str);
    Py_XDECREF(traceback);
    PyErr_Clear();
    return Status(code, message);
  }
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
