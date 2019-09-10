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

#ifndef PYARROW_IO_H
#define PYARROW_IO_H

#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/python/visibility.h"

#include "arrow/python/config.h"

#include "arrow/python/common.h"

namespace arrow {

class MemoryPool;

namespace py {

class ARROW_NO_EXPORT PythonFile;

class ARROW_PYTHON_EXPORT PyReadableFile : public io::RandomAccessFile {
 public:
  explicit PyReadableFile(PyObject* file);
  ~PyReadableFile() override;

  Status Close() override;
  Status Abort() override;
  bool closed() const override;

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override;
  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  // Thread-safe version
  Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                void* out) override;

  // Thread-safe version
  Status ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  Status GetSize(int64_t* size) override;

  Status Seek(int64_t position) override;

  Status Tell(int64_t* position) const override;

 private:
  std::unique_ptr<PythonFile> file_;
};

class ARROW_PYTHON_EXPORT PyOutputStream : public io::OutputStream {
 public:
  explicit PyOutputStream(PyObject* file);
  ~PyOutputStream() override;

  Status Close() override;
  Status Abort() override;
  bool closed() const override;
  Status Tell(int64_t* position) const override;
  Status Write(const void* data, int64_t nbytes) override;

 private:
  std::unique_ptr<PythonFile> file_;
  int64_t position_;
};

// TODO(wesm): seekable output files

// A Buffer subclass that keeps a PyObject reference throughout its
// lifetime, such that the Python object is kept alive as long as the
// C++ buffer is still needed.
// Keeping the reference in a Python wrapper would be incorrect as
// the Python wrapper can get destroyed even though the wrapped C++
// buffer is still alive (ARROW-2270).
class ARROW_PYTHON_EXPORT PyForeignBuffer : public Buffer {
 public:
  static Status Make(const uint8_t* data, int64_t size, PyObject* base,
                     std::shared_ptr<Buffer>* out);

 private:
  PyForeignBuffer(const uint8_t* data, int64_t size, PyObject* base)
      : Buffer(data, size) {
    Py_INCREF(base);
    base_.reset(base);
  }

  OwnedRefNoGIL base_;
};

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_IO_H
