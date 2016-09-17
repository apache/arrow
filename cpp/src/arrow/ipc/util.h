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

#ifndef ARROW_IPC_UTIL_H
#define ARROW_IPC_UTIL_H

#include <cstdint>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/util/status.h"

namespace arrow {
namespace ipc {

// A helper class to tracks the size of allocations
class MockOutputStream : public io::OutputStream {
 public:
  MockOutputStream() : extent_bytes_written_(0) {}

  Status Close() override { return Status::OK(); }

  Status Write(const uint8_t* data, int64_t nbytes) override {
    extent_bytes_written_ += nbytes;
    return Status::OK();
  }

  Status Tell(int64_t* position) override {
    *position = extent_bytes_written_;
    return Status::OK();
  }

  int64_t GetExtentBytesWritten() const { return extent_bytes_written_; }

 private:
  int64_t extent_bytes_written_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_UTIL_H
