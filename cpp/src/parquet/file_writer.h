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

#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace parquet {

class ColumnWriter;
class PageWriter;
class OutputStream;

namespace schema {

class GroupNode;

}  // namespace schema

class PARQUET_EXPORT RowGroupWriter {
 public:
  // Forward declare a virtual class 'Contents' to aid dependency injection and more
  // easily create test fixtures
  // An implementation of the Contents class is defined in the .cc file
  struct Contents {
    virtual ~Contents() = default;
    virtual int num_columns() const = 0;
    virtual int64_t num_rows() const = 0;

    virtual ColumnWriter* NextColumn() = 0;
    virtual int current_column() const = 0;
    virtual void Close() = 0;
  };

  explicit RowGroupWriter(std::unique_ptr<Contents> contents);

  /// Construct a ColumnWriter for the indicated row group-relative column.
  ///
  /// Ownership is solely within the RowGroupWriter. The ColumnWriter is only
  /// valid until the next call to NextColumn or Close. As the contents are
  /// directly written to the sink, once a new column is started, the contents
  /// of the previous one cannot be modified anymore.
  ColumnWriter* NextColumn();
  /// Index of currently written column
  int current_column();
  void Close();

  int num_columns() const;

  /**
   * Number of rows that shall be written as part of this RowGroup.
   */
  int64_t num_rows() const;

 private:
  // Holds a pointer to an instance of Contents implementation
  std::unique_ptr<Contents> contents_;
};

class PARQUET_EXPORT ParquetFileWriter {
 public:
  // Forward declare a virtual class 'Contents' to aid dependency injection and more
  // easily create test fixtures
  // An implementation of the Contents class is defined in the .cc file
  struct Contents {
    Contents(const std::shared_ptr<::parquet::schema::GroupNode>& schema,
             const std::shared_ptr<const KeyValueMetadata>& key_value_metadata)
        : schema_(), key_value_metadata_(key_value_metadata) {
      schema_.Init(schema);
    }
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;

    /// \deprecated Since 1.3.0
    RowGroupWriter* AppendRowGroup(int64_t num_rows);

    virtual RowGroupWriter* AppendRowGroup() = 0;

    virtual int64_t num_rows() const = 0;
    virtual int num_columns() const = 0;
    virtual int num_row_groups() const = 0;

    virtual const std::shared_ptr<WriterProperties>& properties() const = 0;

    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata() const {
      return key_value_metadata_;
    }

    // Return const-pointer to make it clear that this object is not to be copied
    const SchemaDescriptor* schema() const { return &schema_; }

    SchemaDescriptor schema_;

    /// This should be the only place this is stored. Everything else is a const reference
    std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
  };

  ParquetFileWriter();
  ~ParquetFileWriter();

  static std::unique_ptr<ParquetFileWriter> Open(
      const std::shared_ptr<::arrow::io::OutputStream>& sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata = nullptr);

  static std::unique_ptr<ParquetFileWriter> Open(
      const std::shared_ptr<OutputStream>& sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata = nullptr);

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  // Construct a RowGroupWriter for the indicated number of rows.
  //
  // Ownership is solely within the ParquetFileWriter. The RowGroupWriter is only valid
  // until the next call to AppendRowGroup or Close.
  // @param num_rows The number of rows that are stored in the new RowGroup
  //
  // \deprecated Since 1.3.0
  RowGroupWriter* AppendRowGroup(int64_t num_rows);

  /// Construct a RowGroupWriter with an arbitrary number of rows.
  ///
  /// Ownership is solely within the ParquetFileWriter. The RowGroupWriter is only valid
  /// until the next call to AppendRowGroup or Close.
  RowGroupWriter* AppendRowGroup();

  /// Number of columns.
  ///
  /// This number is fixed during the lifetime of the writer as it is determined via
  /// the schema.
  int num_columns() const;

  /// Number of rows in the yet started RowGroups.
  ///
  /// Changes on the addition of a new RowGroup.
  int64_t num_rows() const;

  /// Number of started RowGroups.
  int num_row_groups() const;

  /// Configuration passed to the writer, e.g. the used Parquet format version.
  const std::shared_ptr<WriterProperties>& properties() const;

  /// Returns the file schema descriptor
  const SchemaDescriptor* schema() const;

  /// Returns a column descriptor in schema
  const ColumnDescriptor* descr(int i) const;

  /// Returns the file custom metadata
  const std::shared_ptr<const KeyValueMetadata>& key_value_metadata() const;

 private:
  // Holds a pointer to an instance of Contents implementation
  std::unique_ptr<Contents> contents_;
};

}  // namespace parquet

#endif  // PARQUET_FILE_WRITER_H
