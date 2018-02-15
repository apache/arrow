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
#include "arrow/util/visibility.h"

#include "arrow/python/config.h"

#include "arrow/python/common.h"

namespace arrow {

class MemoryPool;

namespace py {

class ARROW_NO_EXPORT PythonFile;

class ARROW_EXPORT PyReadableFile : public io::RandomAccessFile {
 public:
  explicit PyReadableFile(PyObject* file);
  virtual ~PyReadableFile();

  Status Close() override;

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

  bool supports_zero_copy() const override;

 private:
  std::unique_ptr<PythonFile> file_;
};

class ARROW_EXPORT PyOutputStream : public io::OutputStream {
 public:
  explicit PyOutputStream(PyObject* file);
  virtual ~PyOutputStream();

  Status Close() override;
  Status Tell(int64_t* position) const override;
  Status Write(const void* data, int64_t nbytes) override;

 private:
  std::unique_ptr<PythonFile> file_;
  int64_t position_;
};

// TODO(wesm): seekable output files

}  // namespace py
}  // namespace arrow

#endif  // PYARROW_IO_H
