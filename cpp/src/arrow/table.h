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

#ifndef ARROW_TABLE_H
#define ARROW_TABLE_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class KeyValueMetadata;
class Status;

using ArrayVector = std::vector<std::shared_ptr<Array>>;

/// \class ChunkedArray
/// \brief A data structure managing a list of primitive Arrow arrays logically
/// as one large array
class ARROW_EXPORT ChunkedArray {
 public:
  explicit ChunkedArray(const ArrayVector& chunks);

  /// \return the total length of the chunked array; computed on construction
  int64_t length() const { return length_; }

  int64_t null_count() const { return null_count_; }

  int num_chunks() const { return static_cast<int>(chunks_.size()); }

  /// \return chunk a particular chunk from the chunked array
  std::shared_ptr<Array> chunk(int i) const { return chunks_[i]; }

  const ArrayVector& chunks() const { return chunks_; }

  std::shared_ptr<DataType> type() const;

  bool Equals(const ChunkedArray& other) const;
  bool Equals(const std::shared_ptr<ChunkedArray>& other) const;

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ChunkedArray);
};

/// \brief An immutable column data structure consisting of a field (type
/// metadata) and a logical chunked data array
class ARROW_EXPORT Column {
 public:
  Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks);
  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<ChunkedArray>& data);

  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data);

  // Construct from name and array
  Column(const std::string& name, const std::shared_ptr<Array>& data);

  int64_t length() const { return data_->length(); }

  int64_t null_count() const { return data_->null_count(); }

  std::shared_ptr<Field> field() const { return field_; }

  /// \brief The column name
  /// \return the column's name in the passed metadata
  const std::string& name() const { return field_->name(); }

  /// \brief The column type
  /// \return the column's type according to the metadata
  std::shared_ptr<DataType> type() const { return field_->type(); }

  /// \brief The column data as a chunked array
  /// \return the column's data as a chunked logical array
  std::shared_ptr<ChunkedArray> data() const { return data_; }

  bool Equals(const Column& other) const;
  bool Equals(const std::shared_ptr<Column>& other) const;

  /// \brief Verify that the column's array data is consistent with the passed
  /// field's metadata
  Status ValidateData();

 protected:
  std::shared_ptr<Field> field_;
  std::shared_ptr<ChunkedArray> data_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Column);
};

/// \class RecordBatch
/// \brief Collection of equal-length arrays matching a particular Schema
///
/// A record batch is table-like data structure consisting of an internal
/// sequence of fields, each a contiguous Arrow array
class ARROW_EXPORT RecordBatch {
 public:
  /// \param[in] schema The record batch schema
  /// \param[in] num_rows length of fields in the record batch. Each array
  /// should have the same length as num_rows
  /// \param[in] columns the record batch fields as vector of arrays
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
              const std::vector<std::shared_ptr<Array>>& columns);

  /// \brief Move-based constructor for a vector of Array instances
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
              std::vector<std::shared_ptr<Array>>&& columns);

  /// \brief Construct record batch from vector of internal data structures
  /// \since 0.5.0
  ///
  /// This class is only provided with an rvalue-reference for the input data,
  /// and is intended for internal use, or advanced users.
  ///
  /// \param schema the record batch schema
  /// \param num_rows the number of semantic rows in the record batch. This
  /// should be equal to the length of each field
  /// \param columns the data for the batch's columns
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
              std::vector<std::shared_ptr<ArrayData>>&& columns);

  /// \brief Construct record batch by copying vector of array data
  /// \since 0.5.0
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
              const std::vector<std::shared_ptr<ArrayData>>& columns);

  /// \brief Determine if two record batches are exactly equal
  /// \return true if batches are equal
  bool Equals(const RecordBatch& other) const;

  /// \brief Determine if two record batches are approximately equal
  bool ApproxEquals(const RecordBatch& other) const;

  // \return the table's schema
  /// \return true if batches are equal
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \brief Retrieve an array from the record batch
  /// \param[in] i field index, does not boundscheck
  /// \return an Array object
  std::shared_ptr<Array> column(int i) const;

  std::shared_ptr<ArrayData> column_data(int i) const { return columns_[i]; }

  /// \brief Name in i-th column
  const std::string& column_name(int i) const;

  /// \return the number of columns in the table
  int num_columns() const { return static_cast<int>(columns_.size()); }

  /// \return the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Replace schema key-value metadata with new metadata (EXPERIMENTAL)
  /// \since 0.5.0
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new RecordBatch
  std::shared_ptr<RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice, through end of batch
  /// \return new record batch
  std::shared_ptr<RecordBatch> Slice(int64_t offset) const;

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice
  /// \param[in] length the number of elements to slice from offset
  /// \return new record batch
  std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length) const;

  /// \brief Check for schema or length inconsistencies
  /// \return Status
  Status Validate() const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(RecordBatch);

  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows);

  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;
  std::vector<std::shared_ptr<ArrayData>> columns_;

  // Caching boxed array data
  mutable std::vector<std::shared_ptr<Array>> boxed_columns_;
};

/// \class Table
/// \brief Logical table as sequence of chunked arrays
class ARROW_EXPORT Table {
 public:
  /// \brief Construct Table from schema and columns
  /// If columns is zero-length, the table's number of rows is zero
  /// \param schema The table schema (column types)
  /// \param columns The table's columns
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  Table(const std::shared_ptr<Schema>& schema,
        const std::vector<std::shared_ptr<Column>>& columns, int64_t num_rows = -1);

  /// \brief Construct Table from schema and arrays
  /// \param schema The table schema (column types)
  /// \param arrays The table's columns as arrays
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  Table(const std::shared_ptr<Schema>& schema,
        const std::vector<std::shared_ptr<Array>>& arrays, int64_t num_rows = -1);

  // Construct table from RecordBatch, but only if all of the batch schemas are
  // equal. Returns Status::Invalid if there is some problem
  static Status FromRecordBatches(
      const std::vector<std::shared_ptr<RecordBatch>>& batches,
      std::shared_ptr<Table>* table);

  /// \return the table's schema
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \param[in] i column index, does not boundscheck
  /// \return the i-th column
  std::shared_ptr<Column> column(int i) const { return columns_[i]; }

  /// \brief Remove column from the table, producing a new Table
  Status RemoveColumn(int i, std::shared_ptr<Table>* out) const;

  /// \brief Add column to the table, producing a new Table
  Status AddColumn(int i, const std::shared_ptr<Column>& column,
                   std::shared_ptr<Table>* out) const;

  /// \brief Replace schema key-value metadata with new metadata (EXPERIMENTAL)
  /// \since 0.5.0
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new Table
  std::shared_ptr<Table> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;

  /// \return the number of columns in the table
  int num_columns() const { return static_cast<int>(columns_.size()); }

  /// \return the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Determine if semantic contents of tables are exactly equal
  bool Equals(const Table& other) const;

  /// \brief Perform any checks to validate the input arguments
  Status ValidateColumns() const;

  /// \brief Return true if any column has multiple chunks
  bool IsChunked() const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Table);

  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<Column>> columns_;

  int64_t num_rows_;
};

/// \brief Abstract interface for reading stream of record batches
class ARROW_EXPORT RecordBatchReader {
 public:
  virtual ~RecordBatchReader();

  /// \return the shared schema of the record batches in the stream
  virtual std::shared_ptr<Schema> schema() const = 0;

  /// Read the next record batch in the stream. Return null for batch when
  /// reaching end of stream
  ///
  /// \param[out] batch the next loaded batch, null at end of stream
  /// \return Status
  virtual Status ReadNext(std::shared_ptr<RecordBatch>* batch) = 0;
};

/// \brief Compute a sequence of record batches from a (possibly chunked) Table
class ARROW_EXPORT TableBatchReader : public RecordBatchReader {
 public:
  ~TableBatchReader();

  /// \brief Read batches with the maximum possible size
  explicit TableBatchReader(const Table& table);

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override;

 private:
  class TableBatchReaderImpl;
  std::unique_ptr<TableBatchReaderImpl> impl_;
};

/// \brief Construct table from multiple input tables.
/// \return Status, fails if any schemas are different
ARROW_EXPORT
Status ConcatenateTables(const std::vector<std::shared_ptr<Table>>& tables,
                         std::shared_ptr<Table>* table);

/// \brief Construct table from multiple input tables.
/// \return Status, fails if any schemas are different
ARROW_EXPORT
Status MakeTable(const std::shared_ptr<Schema>& schema,
                 const std::vector<std::shared_ptr<Array>>& arrays,
                 std::shared_ptr<Table>* table);

}  // namespace arrow

#endif  // ARROW_TABLE_H
