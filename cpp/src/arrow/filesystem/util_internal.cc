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

#include "arrow/filesystem/util_internal.h"
#include "arrow/buffer.h"

namespace arrow {
namespace fs {
namespace internal {

Status CopyStream(const std::shared_ptr<io::InputStream>& src,
                  const std::shared_ptr<io::OutputStream>& dest, int64_t chunk_size) {
  std::shared_ptr<Buffer> chunk;
  int64_t bytes_read;

  RETURN_NOT_OK(AllocateBuffer(chunk_size, &chunk));
  while (true) {
    RETURN_NOT_OK(src->Read(chunk_size, &bytes_read, chunk->mutable_data()));
    if (bytes_read == 0) {
      // EOF
      break;
    }
    RETURN_NOT_OK(dest->Write(chunk->data(), bytes_read));
  }

  return Status::OK();
}

}  // namespace internal
}  // namespace fs
}  // namespace arrow
