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

#include "pyarrow/common.h"

#include <cstdlib>
#include <sstream>

#include <arrow/util/memory-pool.h>
#include <arrow/util/status.h>
#include "pyarrow/status.h"

namespace pyarrow {

// ----------------------------------------------------------------------
// Seekable input stream

PyReadableFile::PyReadableFile(PyObject* file)
    : file_(file) {
  Py_INCREF(file);
}

PyReadableFile::~PyReadableFile() {
  Py_DECREF();
}

arrow::Status PyReadableFile::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) override {

  return Status::OK();
}

arrow::Status PyReadableFile::GetSize(int64_t* size) override {
  return Status::OK();
}

  // Does not copy if not necessary
Status PyReadableFile::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) override {

  return Status::OK();
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

Status PyOutputStream::Close() override {
  return arrow::Status::OK();
}

Status PyOutputStream::Tell(int64_t* position) {
  return arrow::Status::OK();
}

Status PyOutputStream::Write(const uint8_t* data, int64_t nbytes) {
  return arrow::Status::OK();
}

private:
  PyObject* file_;
};

} // namespace pyarrow
