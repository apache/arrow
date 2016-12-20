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

#include "pyarrow/io.h"

#include <cstdint>
#include <cstdlib>

#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "pyarrow/common.h"

using arrow::Status;

namespace pyarrow {

// ----------------------------------------------------------------------
// Python file

PythonFile::PythonFile(PyObject* file) : file_(file) {
  Py_INCREF(file_);
}

PythonFile::~PythonFile() {
  Py_DECREF(file_);
}

static Status CheckPyError() {
  if (PyErr_Occurred()) {
    PyObject *exc_type, *exc_value, *traceback;
    PyErr_Fetch(&exc_type, &exc_value, &traceback);
    PyObjectStringify stringified(exc_value);
    std::string message(stringified.bytes);
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(traceback);
    PyErr_Clear();
    return Status::IOError(message);
  }
  return Status::OK();
}

Status PythonFile::Close() {
  // whence: 0 for relative to start of file, 2 for end of file
  PyObject* result = PyObject_CallMethod(file_, "close", "()");
  Py_XDECREF(result);
  ARROW_RETURN_NOT_OK(CheckPyError());
  return Status::OK();
}

Status PythonFile::Seek(int64_t position, int whence) {
  // whence: 0 for relative to start of file, 2 for end of file
  PyObject* result = PyObject_CallMethod(file_, "seek", "(ii)", position, whence);
  Py_XDECREF(result);
  ARROW_RETURN_NOT_OK(CheckPyError());
  return Status::OK();
}

Status PythonFile::Read(int64_t nbytes, PyObject** out) {
  PyObject* result = PyObject_CallMethod(file_, "read", "(i)", nbytes);
  ARROW_RETURN_NOT_OK(CheckPyError());
  *out = result;
  return Status::OK();
}

Status PythonFile::Write(const uint8_t* data, int64_t nbytes) {
  PyObject* py_data =
      PyBytes_FromStringAndSize(reinterpret_cast<const char*>(data), nbytes);
  ARROW_RETURN_NOT_OK(CheckPyError());

  PyObject* result = PyObject_CallMethod(file_, "write", "(O)", py_data);
  Py_XDECREF(py_data);
  Py_XDECREF(result);
  ARROW_RETURN_NOT_OK(CheckPyError());
  return Status::OK();
}

Status PythonFile::Tell(int64_t* position) {
  PyObject* result = PyObject_CallMethod(file_, "tell", "()");
  ARROW_RETURN_NOT_OK(CheckPyError());

  *position = PyLong_AsLongLong(result);
  Py_DECREF(result);

  // PyLong_AsLongLong can raise OverflowError
  ARROW_RETURN_NOT_OK(CheckPyError());

  return Status::OK();
}

// ----------------------------------------------------------------------
// Seekable input stream

PyReadableFile::PyReadableFile(PyObject* file) {
  file_.reset(new PythonFile(file));
}

PyReadableFile::~PyReadableFile() {}

Status PyReadableFile::Close() {
  PyGILGuard lock;
  return file_->Close();
}

Status PyReadableFile::Seek(int64_t position) {
  PyGILGuard lock;
  return file_->Seek(position, 0);
}

Status PyReadableFile::Tell(int64_t* position) {
  PyGILGuard lock;
  return file_->Tell(position);
}

Status PyReadableFile::Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  PyGILGuard lock;
  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(file_->Read(nbytes, &bytes_obj));

  *bytes_read = PyBytes_GET_SIZE(bytes_obj);
  std::memcpy(out, PyBytes_AS_STRING(bytes_obj), *bytes_read);
  Py_DECREF(bytes_obj);

  return Status::OK();
}

Status PyReadableFile::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {
  PyGILGuard lock;

  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(file_->Read(nbytes, &bytes_obj));

  *out = std::make_shared<PyBytesBuffer>(bytes_obj);
  Py_DECREF(bytes_obj);

  return Status::OK();
}

Status PyReadableFile::GetSize(int64_t* size) {
  PyGILGuard lock;

  int64_t current_position;
  ;
  ARROW_RETURN_NOT_OK(file_->Tell(&current_position));

  ARROW_RETURN_NOT_OK(file_->Seek(0, 2));

  int64_t file_size;
  ARROW_RETURN_NOT_OK(file_->Tell(&file_size));

  // Restore previous file position
  ARROW_RETURN_NOT_OK(file_->Seek(current_position, 0));

  *size = file_size;
  return Status::OK();
}

bool PyReadableFile::supports_zero_copy() const {
  return false;
}

// ----------------------------------------------------------------------
// Output stream

PyOutputStream::PyOutputStream(PyObject* file) {
  file_.reset(new PythonFile(file));
}

PyOutputStream::~PyOutputStream() {}

Status PyOutputStream::Close() {
  PyGILGuard lock;
  return file_->Close();
}

Status PyOutputStream::Tell(int64_t* position) {
  PyGILGuard lock;
  return file_->Tell(position);
}

Status PyOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  PyGILGuard lock;
  return file_->Write(data, nbytes);
}

// ----------------------------------------------------------------------
// A readable file that is backed by a PyBytes

PyBytesReader::PyBytesReader(PyObject* obj)
    : arrow::io::BufferReader(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(obj)),
          PyBytes_GET_SIZE(obj)),
      obj_(obj) {
  Py_INCREF(obj_);
}

PyBytesReader::~PyBytesReader() {
  Py_DECREF(obj_);
}

}  // namespace pyarrow
