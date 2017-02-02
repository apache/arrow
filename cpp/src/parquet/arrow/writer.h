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

#ifndef PARQUET_ARROW_WRITER_H
#define PARQUET_ARROW_WRITER_H

#include <memory>

#include "parquet/api/schema.h"
#include "parquet/api/writer.h"

#include "arrow/io/interfaces.h"

namespace arrow {

class Array;
class MemoryPool;
class PrimitiveArray;
class RowBatch;
class Status;
class StringArray;
class Table;
}

namespace parquet {

namespace arrow {

/**
 * Iterative API:
 *  Start a new RowGroup/Chunk with NewRowGroup
 *  Write column-by-column the whole column chunk
 */
class PARQUET_EXPORT FileWriter {
 public:
  FileWriter(::arrow::MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer);

  ::arrow::Status NewRowGroup(int64_t chunk_size);
  ::arrow::Status WriteColumnChunk(
      const ::arrow::Array* data, int64_t offset = 0, int64_t length = -1);
  ::arrow::Status Close();

  virtual ~FileWriter();

  ::arrow::MemoryPool* memory_pool() const;

 private:
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * Write a Table to Parquet.
 *
 * The table shall only consist of columns of primitive type or of primitive lists.
 */
::arrow::Status PARQUET_EXPORT WriteTable(const ::arrow::Table* table,
    ::arrow::MemoryPool* pool, const std::shared_ptr<OutputStream>& sink,
    int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties = default_writer_properties());

::arrow::Status PARQUET_EXPORT WriteTable(const ::arrow::Table* table,
    ::arrow::MemoryPool* pool, const std::shared_ptr<::arrow::io::OutputStream>& sink,
    int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties = default_writer_properties());

}  // namespace arrow

}  // namespace parquet

#endif  // PARQUET_ARROW_WRITER_H
