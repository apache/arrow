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

#include "parquet/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/io/memory.h"

#include "parquet/exception.h"

namespace parquet {

std::shared_ptr<::arrow::io::BufferOutputStream> CreateOutputStream(MemoryPool* pool) {
  PARQUET_ASSIGN_OR_THROW(auto stream, ::arrow::io::BufferOutputStream::Create(
                                           kDefaultOutputStreamSize, pool));
  return stream;
}

std::shared_ptr<ResizableBuffer> AllocateBuffer(MemoryPool* pool, int64_t size) {
  PARQUET_ASSIGN_OR_THROW(auto result, ::arrow::AllocateResizableBuffer(size, pool));
  return result;
}

void CopyStream(std::shared_ptr<ArrowInputStream> from,
                std::shared_ptr<ArrowOutputStream> to, int64_t size,
                ::arrow::MemoryPool* pool) {
  int64_t bytes_copied = 0;
  if (from->supports_zero_copy()) {
    while (bytes_copied < size) {
      PARQUET_ASSIGN_OR_THROW(auto buffer, from->Read(size - bytes_copied));
      if (buffer->size() == 0) {
        throw ParquetException("Unexpected end of stream at ", bytes_copied);
      }
      PARQUET_THROW_NOT_OK(to->Write(buffer->data(), buffer->size()));
      bytes_copied += buffer->size();
    }
    return;
  }

  std::shared_ptr<ResizableBuffer> buffer =
      AllocateBuffer(pool, kDefaultOutputStreamSize);
  while (bytes_copied < size) {
    PARQUET_ASSIGN_OR_THROW(auto read_size, from->Read(size - bytes_copied, &buffer));
    if (read_size == 0) {
      throw ParquetException("Unexpected end of stream at ", bytes_copied);
    }
    PARQUET_THROW_NOT_OK(to->Write(buffer->data(), read_size));
    bytes_copied += read_size;
  }
}

}  // namespace parquet
