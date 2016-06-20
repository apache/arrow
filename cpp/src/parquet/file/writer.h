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

#ifndef PARQUET_FILE_WRITER_H
#define PARQUET_FILE_WRITER_H

#include <cstdint>
#include <memory>

#include "parquet/column/properties.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class ColumnWriter;
class PageWriter;
class OutputStream;

class RowGroupWriter {
 public:
  struct Contents {
    virtual int num_columns() const = 0;
    virtual int64_t num_rows() const = 0;

    // TODO: PARQUET-579
    // virtual void WriteRowGroupStatitics();
    virtual ColumnWriter* NextColumn() = 0;
    virtual void Close() = 0;

    // Return const-pointer to make it clear that this object is not to be copied
    virtual const SchemaDescriptor* schema() const = 0;
  };

  RowGroupWriter(std::unique_ptr<Contents> contents, MemoryAllocator* allocator);

  /**
   * Construct a ColumnWriter for the indicated row group-relative column.
   *
   * Ownership is solely within the RowGroupWriter. The ColumnWriter is only valid
   * until the next call to NextColumn or Close. As the contents are directly written to
   * the sink, once a new column is started, the contents of the previous one cannot be
   * modified anymore.
   */
  ColumnWriter* NextColumn();
  void Close();

  int num_columns() const;

  /**
   * Number of rows that shall be written as part of this RowGroup.
   */
  int64_t num_rows() const;

  // TODO: PARQUET-579
  // virtual void WriteRowGroupStatitics();

 private:
  // Owned by the parent ParquetFileWriter
  const SchemaDescriptor* schema_;

  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  MemoryAllocator* allocator_;
};

class ParquetFileWriter {
 public:
  struct Contents {
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;

    virtual RowGroupWriter* AppendRowGroup(int64_t num_rows) = 0;

    virtual int64_t num_rows() const = 0;
    virtual int num_columns() const = 0;
    virtual int num_row_groups() const = 0;

    virtual const std::shared_ptr<WriterProperties>& properties() const = 0;

    // Return const-poitner to make it clear that this object is not to be copied
    const SchemaDescriptor* schema() const { return &schema_; }
    SchemaDescriptor schema_;
  };

  ParquetFileWriter();
  ~ParquetFileWriter();

  static std::unique_ptr<ParquetFileWriter> Open(std::shared_ptr<OutputStream> sink,
      std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties());

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  /**
   * Construct a RowGroupWriter for the indicated number of rows.
   *
   * Ownership is solely within the ParquetFileWriter. The RowGroupWriter is only valid
   * until the next call to AppendRowGroup or Close.
   *
   * @param num_rows The number of rows that are stored in the new RowGroup
   */
  RowGroupWriter* AppendRowGroup(int64_t num_rows);

  /**
   * Number of columns.
   *
   * This number is fixed during the lifetime of the writer as it is determined via
   * the schema.
   */
  int num_columns() const;

  /**
   * Number of rows in the yet started RowGroups.
   *
   * Changes on the addition of a new RowGroup.
   */
  int64_t num_rows() const;

  /**
   * Number of started RowGroups.
   */
  int num_row_groups() const;

  /**
   * Configuration passed to the writer, e.g. the used Parquet format version.
   */
  const std::shared_ptr<WriterProperties>& properties() const;

  /**
   * Returns the file schema descriptor
   */
  const SchemaDescriptor* descr() { return schema_; }

  const ColumnDescriptor* column_schema(int i) const { return schema_->Column(i); }

 private:
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  // The SchemaDescriptor is provided by the Contents impl
  const SchemaDescriptor* schema_;
};

}  // namespace parquet

#endif  // PARQUET_FILE_WRITER_H
