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
#include <utility>

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
  return std::move(result);
}

}  // namespace parquet
