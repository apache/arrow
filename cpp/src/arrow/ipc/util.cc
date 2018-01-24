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

#include "arrow/ipc/util.h"

#include <cstdint>

#include "arrow/io/interfaces.h"
#include "arrow/status.h"

namespace arrow {
namespace ipc {
namespace internal {

// Adds padding bytes if necessary to ensure all memory blocks are written on
// 8-byte boundaries.
Status AlignOutputStream(io::OutputStream* stream, int64_t alignment) {
  int64_t position;
  RETURN_NOT_OK(stream->Tell(&position));
  int64_t remainder = PaddedLength(position, alignment) - position;
  if (remainder > 0) {
    return stream->Write(kPaddingBytes, remainder);
  }
  return Status::OK();
}

Status AlignInputStream(io::InputStream* stream, int64_t alignment) {
  int64_t position;
  RETURN_NOT_OK(stream->Tell(&position));
  int64_t remainder = PaddedLength(position, alignment) - position;

  static uint8_t kScratchBuffer[8] = {0};
  if (remainder > 0) {
    int64_t bytes_read = 0;
    RETURN_NOT_OK(stream->Read(remainder, &bytes_read, kScratchBuffer));
    if (bytes_read != remainder) {
      return Status::IOError("Unable to align InputStream");
    }
  }
  return Status::OK();
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
