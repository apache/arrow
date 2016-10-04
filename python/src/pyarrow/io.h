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

#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"

#include "pyarrow/config.h"
#include "pyarrow/visibility.h"

namespace arrow { class MemoryPool; }

namespace pyarrow {

// A common interface to a Python file-like object. Must acquire GIL before
// calling any methods
class PythonFile {
 public:
  PythonFile(PyObject* file);
  ~PythonFile();

  arrow::Status Close();
  arrow::Status Seek(int64_t position, int whence);
  arrow::Status Read(int64_t nbytes, PyObject** out);
  arrow::Status Tell(int64_t* position);
  arrow::Status Write(const uint8_t* data, int64_t nbytes);

 private:
  PyObject* file_;
};

class PYARROW_EXPORT PyReadableFile : public arrow::io::ReadableFileInterface {
 public:
  explicit PyReadableFile(PyObject* file);
  virtual ~PyReadableFile();

  arrow::Status Close() override;

  arrow::Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;
  arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override;

  arrow::Status GetSize(int64_t* size) override;

  arrow::Status Seek(int64_t position) override;

  arrow::Status Tell(int64_t* position) override;

  bool supports_zero_copy() const override;

 private:
  std::unique_ptr<PythonFile> file_;
};

class PYARROW_EXPORT PyOutputStream : public arrow::io::OutputStream {
 public:
  explicit PyOutputStream(PyObject* file);
  virtual ~PyOutputStream();

  arrow::Status Close() override;
  arrow::Status Tell(int64_t* position) override;
  arrow::Status Write(const uint8_t* data, int64_t nbytes) override;

 private:
  std::unique_ptr<PythonFile> file_;
};

// A zero-copy reader backed by a PyBytes object
class PYARROW_EXPORT PyBytesReader : public arrow::io::BufferReader {
 public:
  explicit PyBytesReader(PyObject* obj);
  virtual ~PyBytesReader();

 private:
  PyObject* obj_;
};

// TODO(wesm): seekable output files

} // namespace pyarrow

#endif  // PYARROW_IO_H
