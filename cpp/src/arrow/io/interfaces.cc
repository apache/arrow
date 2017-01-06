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

#include "arrow/io/interfaces.h"

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/status.h"

namespace arrow {
namespace io {

FileInterface::~FileInterface() {}

ReadableFileInterface::ReadableFileInterface() {
  set_mode(FileMode::READ);
}

Status ReadableFileInterface::ReadAt(
    int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* out) {
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status ReadableFileInterface::ReadAt(
    int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, out);
}

Status Writeable::Write(const std::string& data) {
  return Write(
      reinterpret_cast<const uint8_t*>(data.c_str()), static_cast<int64_t>(data.size()));
}

}  // namespace io
}  // namespace arrow
