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

// Bridges Arrow's IO interfaces and Parquet-cpp's IO interfaces

#ifndef PARQUET_ARROW_IO_H
#define PARQUET_ARROW_IO_H

#include <cstdint>
#include <memory>

#include "parquet/api/io.h"

#include "arrow/io/interfaces.h"
#include "arrow/util/memory-pool.h"

namespace parquet {

namespace arrow {

// An implementation of the Parquet MemoryAllocator API that plugs into an
// existing Arrow memory pool. This way we can direct all allocations to a
// single place rather than tracking allocations in different locations (for
// example: without utilizing parquet-cpp's default allocator)
class PARQUET_EXPORT ParquetAllocator : public MemoryAllocator {
 public:
  // Uses the default memory pool
  ParquetAllocator();

  explicit ParquetAllocator(::arrow::MemoryPool* pool);
  virtual ~ParquetAllocator();

  uint8_t* Malloc(int64_t size) override;
  void Free(uint8_t* buffer, int64_t size) override;

  void set_pool(::arrow::MemoryPool* pool) { pool_ = pool; }

  ::arrow::MemoryPool* pool() const { return pool_; }

 private:
  ::arrow::MemoryPool* pool_;
};

class PARQUET_EXPORT ParquetReadSource : public RandomAccessSource {
 public:
  explicit ParquetReadSource(ParquetAllocator* allocator);

  // We need to ask for the file size on opening the file, and this can fail
  ::arrow::Status Open(const std::shared_ptr<::arrow::io::RandomAccessFile>& file);

  void Close() override;
  int64_t Tell() const override;
  void Seek(int64_t pos) override;
  int64_t Read(int64_t nbytes, uint8_t* out) override;
  std::shared_ptr<Buffer> Read(int64_t nbytes) override;

 private:
  // An Arrow readable file of some kind
  std::shared_ptr<::arrow::io::RandomAccessFile> file_;

  // The allocator is required for creating managed buffers
  ParquetAllocator* allocator_;
};

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_IO_H
