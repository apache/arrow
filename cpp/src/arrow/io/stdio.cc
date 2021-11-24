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

#include "arrow/io/stdio.h"

#include <iostream>

#include "arrow/buffer.h"
#include "arrow/result.h"

namespace arrow {
namespace io {

//
// StdoutStream implementation
//

StdoutStream::StdoutStream() : pos_(0) { set_mode(FileMode::WRITE); }

Status StdoutStream::Close() { return Status::OK(); }

bool StdoutStream::closed() const { return false; }

Result<int64_t> StdoutStream::Tell() const { return pos_; }

Status StdoutStream::Write(const void* data, int64_t nbytes) {
  pos_ += nbytes;
  std::cout.write(reinterpret_cast<const char*>(data), nbytes);
  return Status::OK();
}

//
// StderrStream implementation
//

StderrStream::StderrStream() : pos_(0) { set_mode(FileMode::WRITE); }

Status StderrStream::Close() { return Status::OK(); }

bool StderrStream::closed() const { return false; }

Result<int64_t> StderrStream::Tell() const { return pos_; }

Status StderrStream::Write(const void* data, int64_t nbytes) {
  pos_ += nbytes;
  std::cerr.write(reinterpret_cast<const char*>(data), nbytes);
  return Status::OK();
}

//
// StdinStream implementation
//

StdinStream::StdinStream() : pos_(0) { set_mode(FileMode::READ); }

Status StdinStream::Close() { return Status::OK(); }

bool StdinStream::closed() const { return false; }

Result<int64_t> StdinStream::Tell() const { return pos_; }

Result<int64_t> StdinStream::Read(int64_t nbytes, void* out) {
  std::cin.read(reinterpret_cast<char*>(out), nbytes);
  if (std::cin) {
    pos_ += nbytes;
    return nbytes;
  } else {
    return 0;
  }
}

Result<std::shared_ptr<Buffer>> StdinStream::Read(int64_t nbytes) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
  ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read, false));
  buffer->ZeroPadding();
  return std::move(buffer);
}

}  // namespace io
}  // namespace arrow
