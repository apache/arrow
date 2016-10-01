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

#ifndef PYARROW_COMMON_H
#define PYARROW_COMMON_H

#include "arrow/io/interfaces.h"

#include "pyarrow/config.h"
#include "pyarrow/visibility.h"

namespace arrow { class MemoryPool; }

namespace pyarrow {

class PYARROW_EXPORT PyReadableFile : public arrow::io::ReadableFileInterface {
public:
  explicit PyReadableFile(PyObject* file);
  ~PyReadableFile();

  arrow::Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) override;

  arrow::Status GetSize(int64_t* size) override;

  // Does not copy if not necessary
  arrow::Status ReadAt(
      int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override;

  bool supports_zero_copy() const override;

private:
  PyObject* file_;
};

class PYARROW_EXPORT PyOutputStream : public arrow::io::OutputStream {
public:
  explicit PyOutputStream(PyObject* file);
  ~PyOutputStream();

  arrow::Status Close() override;
  arrow::Status Tell(int64_t* position) override;
  arrow::Status Write(const uint8_t* data, int64_t nbytes) override;

private:
  PyObject* file_;
};

} // namespace pyarrow
