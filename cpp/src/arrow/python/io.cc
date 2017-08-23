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

#include "arrow/python/io.h"

#include <cstdint>
#include <cstdlib>
#include <string>

#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "arrow/python/common.h"

namespace arrow {
namespace py {

// ----------------------------------------------------------------------
// Python file

PythonFile::PythonFile(PyObject* file) : file_(file) { Py_INCREF(file_); }

PythonFile::~PythonFile() { Py_DECREF(file_); }

// This is annoying: because C++11 does not allow implicit conversion of string
// literals to non-const char*, we need to go through some gymnastics to use
// PyObject_CallMethod without a lot of pain (its arguments are non-const
// char*)
template <typename... ArgTypes>
static inline PyObject* cpp_PyObject_CallMethod(PyObject* obj, const char* method_name,
                                                const char* argspec, ArgTypes... args) {
  return PyObject_CallMethod(obj, const_cast<char*>(method_name),
                             const_cast<char*>(argspec), args...);
}

Status PythonFile::Close() {
  // whence: 0 for relative to start of file, 2 for end of file
  PyObject* result = cpp_PyObject_CallMethod(file_, "close", "()");
  Py_XDECREF(result);
  PY_RETURN_IF_ERROR(StatusCode::IOError);
  return Status::OK();
}

Status PythonFile::Seek(int64_t position, int whence) {
  // whence: 0 for relative to start of file, 2 for end of file
  PyObject* result = cpp_PyObject_CallMethod(file_, "seek", "(ii)", position, whence);
  Py_XDECREF(result);
  PY_RETURN_IF_ERROR(StatusCode::IOError);
  return Status::OK();
}

Status PythonFile::Read(int64_t nbytes, PyObject** out) {
  PyObject* result = cpp_PyObject_CallMethod(file_, "read", "(i)", nbytes);
  PY_RETURN_IF_ERROR(StatusCode::IOError);
  *out = result;
  return Status::OK();
}

Status PythonFile::Write(const uint8_t* data, int64_t nbytes) {
  PyObject* py_data =
      PyBytes_FromStringAndSize(reinterpret_cast<const char*>(data), nbytes);
  PY_RETURN_IF_ERROR(StatusCode::IOError);

  PyObject* result = cpp_PyObject_CallMethod(file_, "write", "(O)", py_data);
  Py_XDECREF(py_data);
  Py_XDECREF(result);
  PY_RETURN_IF_ERROR(StatusCode::IOError);
  return Status::OK();
}

Status PythonFile::Tell(int64_t* position) {
  PyObject* result = cpp_PyObject_CallMethod(file_, "tell", "()");
  PY_RETURN_IF_ERROR(StatusCode::IOError);

  *position = PyLong_AsLongLong(result);
  Py_DECREF(result);

  // PyLong_AsLongLong can raise OverflowError
  PY_RETURN_IF_ERROR(StatusCode::IOError);

  return Status::OK();
}

// ----------------------------------------------------------------------
// Seekable input stream

PyReadableFile::PyReadableFile(PyObject* file) { file_.reset(new PythonFile(file)); }

PyReadableFile::~PyReadableFile() {}

Status PyReadableFile::Close() {
  PyAcquireGIL lock;
  return file_->Close();
}

Status PyReadableFile::Seek(int64_t position) {
  PyAcquireGIL lock;
  return file_->Seek(position, 0);
}

Status PyReadableFile::Tell(int64_t* position) const {
  PyAcquireGIL lock;
  return file_->Tell(position);
}

Status PyReadableFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  PyAcquireGIL lock;
  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(file_->Read(nbytes, &bytes_obj));

  *bytes_read = PyBytes_GET_SIZE(bytes_obj);
  std::memcpy(out, PyBytes_AS_STRING(bytes_obj), *bytes_read);
  Py_DECREF(bytes_obj);

  return Status::OK();
}

Status PyReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  PyAcquireGIL lock;

  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(file_->Read(nbytes, &bytes_obj));

  *out = std::make_shared<PyBuffer>(bytes_obj);
  Py_DECREF(bytes_obj);

  return Status::OK();
}

Status PyReadableFile::GetSize(int64_t* size) {
  PyAcquireGIL lock;

  int64_t current_position;

  ARROW_RETURN_NOT_OK(file_->Tell(&current_position));

  ARROW_RETURN_NOT_OK(file_->Seek(0, 2));

  int64_t file_size;
  ARROW_RETURN_NOT_OK(file_->Tell(&file_size));

  // Restore previous file position
  ARROW_RETURN_NOT_OK(file_->Seek(current_position, 0));

  *size = file_size;
  return Status::OK();
}

bool PyReadableFile::supports_zero_copy() const { return false; }

// ----------------------------------------------------------------------
// Output stream

PyOutputStream::PyOutputStream(PyObject* file) : position_(0) {
  file_.reset(new PythonFile(file));
}

PyOutputStream::~PyOutputStream() {}

Status PyOutputStream::Close() {
  PyAcquireGIL lock;
  return file_->Close();
}

Status PyOutputStream::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

Status PyOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  PyAcquireGIL lock;
  position_ += nbytes;
  return file_->Write(data, nbytes);
}

// ----------------------------------------------------------------------
// A readable file that is backed by a PyBuffer

PyBytesReader::PyBytesReader(PyObject* obj)
    : io::BufferReader(std::make_shared<PyBuffer>(obj)) {}

PyBytesReader::~PyBytesReader() {}

}  // namespace py
}  // namespace arrow
