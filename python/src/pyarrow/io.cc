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

#include <cstdlib>
#include <sstream>

#include <arrow/util/memory-pool.h>
#include <arrow/util/status.h>

#include "pyarrow/common.h"
#include "pyarrow/status.h"

namespace pyarrow {

// ----------------------------------------------------------------------
// Seekable input stream

PyReadableFile::PyReadableFile(PyObject* file)
    : file_(file) {
  Py_INCREF(file_);
}

PyReadableFile::~PyReadableFile() {
  Py_DECREF(file_);
}

#define ARROW_RETURN_IF_PYERROR()                   \
  if (PyErr_Occurred()) {                           \
    PyObject *exc_type, *exc_value, *traceback;     \
    PyErr_Fetch(&exc_type, &exc_value, &traceback); \
    PyObjectStringify stringified(exc_value);       \
    std::string message(stringified.bytes);         \
    Py_DECREF(exc_type);                            \
    Py_DECREF(exc_value);                           \
    Py_DECREF(traceback);                           \
    PyErr_Clear();                                  \
    return arrow::Status::IOError(message);    \
  }

static arrow::Status SeekNoGIL(PyObject* file, int64_t position, int whence) {
  // whence: 0 for relative to start of file, 2 for end of file
  PyObject* result = PyObject_CallMethod(file, "seek", "(i)", position);
  Py_XDECREF(result);
  ARROW_RETURN_IF_PYERROR();
  return arrow::Status::OK();
}

static arrow::Status ReadNoGIL(PyObject* file, int64_t nbytes, PyObject** out) {
  PyObject* result = PyObject_CallMethod(file, "read", "(i)", nbytes);
  ARROW_RETURN_IF_PYERROR();
  *out = result;
  return arrow::Status::OK();
}

static arrow::Status TellNoGIL(PyObject* file, int64_t* position) {
  PyObject* result = PyObject_CallMethod(file, "tell", "()");
  ARROW_RETURN_IF_PYERROR();

  *position = PyLong_AsLongLong(result);
  Py_DECREF(result);

  // PyLong_AsLongLong can raise OverflowError
  ARROW_RETURN_IF_PYERROR();

 return arrow::Status::OK();
}

arrow::Status PyReadableFile::Seek(int64_t position) {
  PyAcquireGIL_RAII lock;
  return SeekNoGIL(file_, position, 0);
}

arrow::Status PyReadableFile::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  PyAcquireGIL_RAII lock;
  ARROW_RETURN_NOT_OK(SeekNoGIL(file_, position, 0));

  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(ReadNoGIL(file_, nbytes, &bytes_obj));

  *bytes_read = PyBytes_GET_SIZE(bytes_obj);
  std::memcpy(out, PyBytes_AS_STRING(bytes_obj), *bytes_read);
  Py_DECREF(bytes_obj);

  return arrow::Status::OK();
}

arrow::Status PyReadableFile::GetSize(int64_t* size) {
  PyAcquireGIL_RAII lock;

  int64_t current_position;;
  ARROW_RETURN_NOT_OK(TellNoGIL(file_, &current_position));

  ARROW_RETURN_NOT_OK(SeekNoGIL(file_, 0, 2));

  int64_t file_size;
  ARROW_RETURN_NOT_OK(TellNoGIL(file_, &file_size));

  // Restore previous file position
  ARROW_RETURN_NOT_OK(SeekNoGIL(file_, current_position, 0));

  *size = file_size;
  return arrow::Status::OK();
}

// Does not copy if not necessary
arrow::Status PyReadableFile::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {
  PyAcquireGIL_RAII lock;
  ARROW_RETURN_NOT_OK(SeekNoGIL(file_, position, 0));

  PyObject* bytes_obj;
  ARROW_RETURN_NOT_OK(ReadNoGIL(file_, nbytes, &bytes_obj));

  *out = std::make_shared<PyBytesBuffer>(bytes_obj);
  Py_DECREF(bytes_obj);

  return arrow::Status::OK();
}

bool PyReadableFile::supports_zero_copy() const {
  return false;
}

// ----------------------------------------------------------------------
// Output stream

PyOutputStream::PyOutputStream(PyObject* file)
    : file_(file) {
  Py_INCREF(file);
}

PyOutputStream::~PyOutputStream() {
  Py_DECREF(file_);
}

arrow::Status PyOutputStream::Close() {
  return arrow::Status::OK();
}

arrow::Status PyOutputStream::Tell(int64_t* position) {
  return arrow::Status::OK();
}

arrow::Status PyOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  return arrow::Status::OK();
}

} // namespace pyarrow
