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

#include "arrow/ipc/options.h"

#include "arrow/status.h"

namespace arrow {
namespace ipc {

IpcWriteOptions IpcWriteOptions::Defaults() { return IpcWriteOptions(); }

IpcReadOptions IpcReadOptions::Defaults() { return IpcReadOptions(); }

Status IpcReadOptions::Validate() const {
  if (max_recursion_depth < 0) {
    return Status::Invalid("max_recursion_depth must be non-negative, got ",
                           max_recursion_depth);
  }
  if (max_metadata_length < -1) {
    return Status::Invalid("max_metadata_length must be -1 or non-negative, got ",
                           max_metadata_length);
  }
  if (max_body_length < -1) {
    return Status::Invalid("max_body_length must be -1 or non-negative, got ",
                           max_body_length);
  }
  if (max_decompression_ratio < -1) {
    return Status::Invalid("max_decompression_ratio must be -1 or non-negative, got ",
                           max_decompression_ratio);
  }
  return Status::OK();
}

Status IpcReadOptions::CheckMetadataLength(int64_t length, const char* prefix) const {
  if (max_metadata_length >= 0 && length > max_metadata_length) {
    return Status::Invalid(prefix, length, " exceeds maximum allowed metadata length ",
                           max_metadata_length);
  }
  return Status::OK();
}

Status IpcReadOptions::CheckBodyLength(int64_t length) const {
  if (max_body_length >= 0 && length > max_body_length) {
    return Status::Invalid("IPC body length ", length,
                           " exceeds maximum allowed body length ", max_body_length);
  }
  return Status::OK();
}

namespace internal {

Status CheckCompressionSupported(Compression::type codec) {
  if (!(codec == Compression::LZ4_FRAME || codec == Compression::ZSTD)) {
    return Status::Invalid("Only LZ4_FRAME and ZSTD compression allowed");
  }
  return Status::OK();
}

}  // namespace internal

}  // namespace ipc
}  // namespace arrow
