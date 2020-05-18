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
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include "arrow/python/helpers.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

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
// PythonErrorDetail

namespace {
const char kErrorDetailTypeId[] = "arrow::py::PythonErrorDetail";

// Try to match the Python exception type with an appropriate Status code
StatusCode MapPyError(PyObject* exc_type) {
  StatusCode code;

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
  } else {
    code = StatusCode::UnknownError;
  }
  return code;
}

// PythonErrorDetail indicates a Python exception was raised.
class PythonErrorDetail : public StatusDetail {
 public:
  explicit PythonErrorDetail(PyError error) : error_(std::move(error)) {}

  const char* type_id() const override { return kErrorDetailTypeId; }

  std::string ToString() const override {
    // This is simple enough not to need the GIL
    const auto type = reinterpret_cast<const PyTypeObject*>(error_.type.obj());

    // XXX Should we also print traceback?
    return std::string("Python exception: ") + type->tp_name;
  }

  PyError error_;
};

}  // namespace

// ----------------------------------------------------------------------
// PyError

PyError::PyError(PyObject* e) : exception(e) {
  ARROW_CHECK(exception);
  exception.incref();
  type.reset(PyObject_Type(exception.obj()));
  traceback.reset(PyException_GetTraceback(exception.obj()));
  Normalize();
}

PyError PyError::Fetch() {
  PyError out;
  PyErr_Fetch(out.type.ref(), out.exception.ref(), out.traceback.ref());
  if (out) {
    out.Normalize();
  }
  return out;
}

void PyError::Restore() && {
  if (exception) {
    PyErr_Restore(type.detach(), exception.detach(), traceback.detach());
  }
}

PyError::operator bool() const { return exception; }

void PyError::SetContext(PyError context) {
  ARROW_CHECK(*this);
  PyException_SetContext(exception.obj(), nullptr);
  PyException_SetContext(exception.obj(), context.exception.detach());
}

Status PyError::ToStatus(StatusCode code) && {
  if (!exception) {
    return Status::OK();
  }

  if (code == StatusCode::UnknownError) {
    code = MapPyError(type.obj());
  }

  std::string message;
  RETURN_NOT_OK(internal::PyObject_StdStringStr(exception.obj(), &message));

  auto detail = std::make_shared<PythonErrorDetail>(std::move(*this));
  return Status(code, std::move(message), std::move(detail));
}

PyError PyError::Get(Status status) {
  ARROW_CHECK(IsPyError(status));
  auto detail = checked_pointer_cast<PythonErrorDetail>(std::move(status).detail());
  return std::move(detail->error_);
}

void PyError::Normalize() {
  PyErr_NormalizeException(type.ref(), exception.ref(), traceback.ref());
  DCHECK(PyType_Check(type.obj()));
  DCHECK(exception);  // Ensured by PyErr_NormalizeException, double-check
  if (!traceback) {
    // Needed by PyErr_Restore()
    traceback.reset(Py_None);
    traceback.incref();
  }
}

Status ConvertPyError(StatusCode code) { return PyError::Fetch().ToStatus(code); }

bool IsPyError(const Status& status) {
  if (status.ok()) {
    return false;
  }
  const auto& detail = status.detail();
  bool result = detail != nullptr && detail->type_id() == kErrorDetailTypeId;
  return result;
}

void RestorePyError(Status status) { PyError::Get(std::move(status)).Restore(); }

Status ExceptionToStatus(PyObject* exception) { return PyError(exception).ToStatus(); }

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
    return ConvertPyError(StatusCode::Invalid);
  }
}

Result<std::shared_ptr<Buffer>> PyBuffer::FromPyObject(PyObject* obj) {
  PyBuffer* buf = new PyBuffer();
  std::shared_ptr<Buffer> res(buf);
  RETURN_NOT_OK(buf->Init(obj));
  return res;
}

PyBuffer::~PyBuffer() {
  if (data_ != nullptr) {
    PyAcquireGIL lock;
    PyBuffer_Release(&py_buf_);
  }
}

}  // namespace py
}  // namespace arrow
