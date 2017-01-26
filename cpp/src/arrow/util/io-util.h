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

#ifndef ARROW_UTIL_IO_UTIL_H
#define ARROW_UTIL_IO_UTIL_H

#include "arrow/buffer.h"
#include <iostream>

namespace arrow {
namespace io {

// Output stream that just writes to stdout.
class StdoutStream : public OutputStream {
 public:
  StdoutStream() : pos_(0) { set_mode(FileMode::WRITE); }
  virtual ~StdoutStream() {}

  Status Close() { return Status::OK(); }
  Status Tell(int64_t* position) {
    *position = pos_;
    return Status::OK();
  }

  Status Write(const uint8_t* data, int64_t nbytes) {
    pos_ += nbytes;
    std::cout.write(reinterpret_cast<const char*>(data), nbytes);
    return Status::OK();
  }

 private:
  int64_t pos_;
};

// Input stream that just reads from stdin.
class StdinStream : public InputStream {
 public:
  StdinStream() : pos_(0) { set_mode(FileMode::READ); }
  virtual ~StdinStream() {}

  Status Close() { return Status::OK(); }
  Status Tell(int64_t* position) {
    *position = pos_;
    return Status::OK();
  }

  virtual Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
    std::cin.read(reinterpret_cast<char*>(out), nbytes);
    if (std::cin) {
      *bytes_read = nbytes;
      pos_ += nbytes;
    } else {
      *bytes_read = 0;
    }
    return Status::OK();
  }

  virtual Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
    auto buffer = std::make_shared<PoolBuffer>(nullptr);
    RETURN_NOT_OK(buffer->Resize(nbytes));
    int64_t bytes_read;
    RETURN_NOT_OK(Read(nbytes, &bytes_read, buffer->mutable_data()));
    RETURN_NOT_OK(buffer->Resize(bytes_read, false));
    *out = buffer;
    return Status::OK();
  }

 private:
  int64_t pos_;
};

}  // namespace io
}  // namespace arrow

#endif  // ARROW_UTIL_IO_UTIL_H
