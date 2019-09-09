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
#include <memory>
#include <mutex>
#include <string>

#include "arrow/io/memory.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"

namespace arrow {
namespace py {

// ----------------------------------------------------------------------
// Python file

// A common interface to a Python file-like object. Must acquire GIL before
// calling any methods
class PythonFile {
 public:
  explicit PythonFile(PyObject* file) : file_(file) { Py_INCREF(file); }

  Status CheckClosed() const {
    if (!file_) {
      return Status::Invalid("operation on closed Python file");
    }
    return Status::OK();
  }

  Status Close() {
    if (file_) {
      PyObject* result = cpp_PyObject_CallMethod(file_.obj(), "close", "()");
      Py_XDECREF(result);
      file_.reset();
      PY_RETURN_IF_ERROR(StatusCode::IOError);
    }
    return Status::OK();
  }

  Status Abort() {
    file_.reset();
    return Status::OK();
  }

  bool closed() const {
    if (!file_) {
      return true;
    }
    PyObject* result = PyObject_GetAttrString(file_.obj(), "closed");
    if (result == NULL) {
      // Can't propagate the error, so write it out and return an arbitrary value
      PyErr_WriteUnraisable(NULL);
      return true;
    }
    int ret = PyObject_IsTrue(result);
    Py_XDECREF(result);
    if (ret < 0) {
      PyErr_WriteUnraisable(NULL);
      return true;
    }
    return ret != 0;
  }

  Status Seek(int64_t position, int whence) {
    RETURN_NOT_OK(CheckClosed());

    // whence: 0 for relative to start of file, 2 for end of file
    PyObject* result = cpp_PyObject_CallMethod(file_.obj(), "seek", "(ni)",
                                               static_cast<Py_ssize_t>(position), whence);
    Py_XDECREF(result);
    PY_RETURN_IF_ERROR(StatusCode::IOError);
    return Status::OK();
  }

  Status Read(int64_t nbytes, PyObject** out) {
    RETURN_NOT_OK(CheckClosed());

    PyObject* result = cpp_PyObject_CallMethod(file_.obj(), "read", "(n)",
                                               static_cast<Py_ssize_t>(nbytes));
    PY_RETURN_IF_ERROR(StatusCode::IOError);
    *out = result;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    RETURN_NOT_OK(CheckClosed());

    PyObject* py_data =
        PyBytes_FromStringAndSize(reinterpret_cast<const char*>(data), nbytes);
    PY_RETURN_IF_ERROR(StatusCode::IOError);

    PyObject* result = cpp_PyObject_CallMethod(file_.obj(), "write", "(O)", py_data);
    Py_XDECREF(py_data);
    Py_XDECREF(result);
    PY_RETURN_IF_ERROR(StatusCode::IOError);
    return Status::OK();
  }

  Status Tell(int64_t* position) {
    RETURN_NOT_OK(CheckClosed());

    PyObject* result = cpp_PyObject_CallMethod(file_.obj(), "tell", "()");
    PY_RETURN_IF_ERROR(StatusCode::IOError);

    *position = PyLong_AsLongLong(result);
    Py_DECREF(result);

    // PyLong_AsLongLong can raise OverflowError
    PY_RETURN_IF_ERROR(StatusCode::IOError);

    return Status::OK();
  }

  std::mutex& lock() { return lock_; }

 private:
  std::mutex lock_;
  OwnedRefNoGIL file_;
};

// ----------------------------------------------------------------------
// Seekable input stream

PyReadableFile::PyReadableFile(PyObject* file) { file_.reset(new PythonFile(file)); }

// The destructor does not close the underlying Python file object, as
// there may be multiple references to it.  Instead let the Python
// destructor do its job.
PyReadableFile::~PyReadableFile() {}

Status PyReadableFile::Abort() {
  return SafeCallIntoPython([this]() { return file_->Abort(); });
}

Status PyReadableFile::Close() {
  return SafeCallIntoPython([this]() { return file_->Close(); });
}

bool PyReadableFile::closed() const {
  bool res;
  Status st = SafeCallIntoPython([this, &res]() {
    res = file_->closed();
    return Status::OK();
  });
  return res;
}

Status PyReadableFile::Seek(int64_t position) {
  return SafeCallIntoPython([=] { return file_->Seek(position, 0); });
}

Status PyReadableFile::Tell(int64_t* position) const {
  return SafeCallIntoPython([=]() { return file_->Tell(position); });
}

Status PyReadableFile::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  return SafeCallIntoPython([=]() {
    OwnedRef bytes;
    RETURN_NOT_OK(file_->Read(nbytes, bytes.ref()));
    PyObject* bytes_obj = bytes.obj();
    DCHECK(bytes_obj != NULL);

    if (!PyBytes_Check(bytes_obj)) {
      return Status::TypeError(
          "Python file read() should have returned a bytes object, got '",
          Py_TYPE(bytes_obj)->tp_name, "' (did you open the file in binary mode?)");
    }

    *bytes_read = PyBytes_GET_SIZE(bytes_obj);
    std::memcpy(out, PyBytes_AS_STRING(bytes_obj), *bytes_read);
    return Status::OK();
  });
}

Status PyReadableFile::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  return SafeCallIntoPython([=]() {
    OwnedRef bytes_obj;
    RETURN_NOT_OK(file_->Read(nbytes, bytes_obj.ref()));
    DCHECK(bytes_obj.obj() != NULL);

    return PyBuffer::FromPyObject(bytes_obj.obj(), out);
  });
}

Status PyReadableFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                              void* out) {
  std::lock_guard<std::mutex> guard(file_->lock());
  return SafeCallIntoPython([=]() {
    RETURN_NOT_OK(Seek(position));
    return Read(nbytes, bytes_read, out);
  });
}

Status PyReadableFile::ReadAt(int64_t position, int64_t nbytes,
                              std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> guard(file_->lock());
  return SafeCallIntoPython([=]() {
    RETURN_NOT_OK(Seek(position));
    return Read(nbytes, out);
  });
}

Status PyReadableFile::GetSize(int64_t* size) {
  return SafeCallIntoPython([=]() {
    int64_t current_position = -1;

    RETURN_NOT_OK(file_->Tell(&current_position));
    RETURN_NOT_OK(file_->Seek(0, 2));

    int64_t file_size = -1;
    RETURN_NOT_OK(file_->Tell(&file_size));
    // Restore previous file position
    RETURN_NOT_OK(file_->Seek(current_position, 0));

    *size = file_size;
    return Status::OK();
  });
}

// ----------------------------------------------------------------------
// Output stream

PyOutputStream::PyOutputStream(PyObject* file) : position_(0) {
  file_.reset(new PythonFile(file));
}

// The destructor does not close the underlying Python file object, as
// there may be multiple references to it.  Instead let the Python
// destructor do its job.
PyOutputStream::~PyOutputStream() {}

Status PyOutputStream::Abort() {
  return SafeCallIntoPython([=]() { return file_->Abort(); });
}

Status PyOutputStream::Close() {
  return SafeCallIntoPython([=]() { return file_->Close(); });
}

bool PyOutputStream::closed() const {
  bool res;
  Status st = SafeCallIntoPython([this, &res]() {
    res = file_->closed();
    return Status::OK();
  });
  return res;
}

Status PyOutputStream::Tell(int64_t* position) const {
  *position = position_;
  return Status::OK();
}

Status PyOutputStream::Write(const void* data, int64_t nbytes) {
  return SafeCallIntoPython([=]() {
    position_ += nbytes;
    return file_->Write(data, nbytes);
  });
}

// ----------------------------------------------------------------------
// Foreign buffer

Status PyForeignBuffer::Make(const uint8_t* data, int64_t size, PyObject* base,
                             std::shared_ptr<Buffer>* out) {
  PyForeignBuffer* buf = new PyForeignBuffer(data, size, base);
  if (buf == NULL) {
    return Status::OutOfMemory("could not allocate foreign buffer object");
  } else {
    *out = std::shared_ptr<Buffer>(buf);
    return Status::OK();
  }
}

}  // namespace py
}  // namespace arrow
