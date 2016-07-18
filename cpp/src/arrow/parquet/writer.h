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

#ifndef ARROW_PARQUET_WRITER_H
#define ARROW_PARQUET_WRITER_H

#include <memory>

#include "parquet/api/schema.h"
#include "parquet/api/writer.h"

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class MemoryPool;
class PrimitiveArray;
class RowBatch;
class Status;
class StringArray;
class Table;

namespace parquet {

/**
 * Iterative API:
 *  Start a new RowGroup/Chunk with NewRowGroup
 *  Write column-by-column the whole column chunk
 */
class ARROW_EXPORT FileWriter {
 public:
  FileWriter(MemoryPool* pool, std::unique_ptr<::parquet::ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  Status WriteFlatColumnChunk(const Array* data, int64_t offset = 0, int64_t length = -1);
  Status Close();

  virtual ~FileWriter();

  MemoryPool* memory_pool() const;

 private:
  class ARROW_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * Write a flat Table to Parquet.
 *
 * The table shall only consist of nullable, non-repeated columns of primitive type.
 */
Status ARROW_EXPORT WriteFlatTable(const Table* table, MemoryPool* pool,
    const std::shared_ptr<::parquet::OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<::parquet::WriterProperties>& properties =
        ::parquet::default_writer_properties());

}  // namespace parquet

}  // namespace arrow

#endif  // ARROW_PARQUET_WRITER_H
