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

#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Column;
class Schema;
class Status;

using ArrayVector = std::vector<std::shared_ptr<Array>>;

// A data structure managing a list of primitive Arrow arrays logically as one
// large array
class ARROW_EXPORT ChunkedArray {
 public:
  explicit ChunkedArray(const ArrayVector& chunks);

  // @returns: the total length of the chunked array; computed on construction
  int64_t length() const { return length_; }

  int64_t null_count() const { return null_count_; }

  int num_chunks() const { return static_cast<int>(chunks_.size()); }

  std::shared_ptr<Array> chunk(int i) const { return chunks_[i]; }

  const ArrayVector& chunks() const { return chunks_; }

  bool Equals(const ChunkedArray& other) const;
  bool Equals(const std::shared_ptr<ChunkedArray>& other) const;

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;
};

// An immutable column data structure consisting of a field (type metadata) and
// a logical chunked data array (which can be validated as all being the same
// type).
class ARROW_EXPORT Column {
 public:
  Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks);
  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<ChunkedArray>& data);

  Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data);

  /// Construct from name and array
  Column(const std::string& name, const std::shared_ptr<Array>& data);

  int64_t length() const { return data_->length(); }

  int64_t null_count() const { return data_->null_count(); }

  std::shared_ptr<Field> field() const { return field_; }

  // @returns: the column's name in the passed metadata
  const std::string& name() const { return field_->name(); }

  // @returns: the column's type according to the metadata
  std::shared_ptr<DataType> type() const { return field_->type(); }

  // @returns: the column's data as a chunked logical array
  std::shared_ptr<ChunkedArray> data() const { return data_; }

  bool Equals(const Column& other) const;
  bool Equals(const std::shared_ptr<Column>& other) const;

  // Verify that the column's array data is consistent with the passed field's
  // metadata
  Status ValidateData();

 protected:
  std::shared_ptr<Field> field_;
  std::shared_ptr<ChunkedArray> data_;
};

// A record batch is a simpler and more rigid table data structure intended for
// use primarily in shared memory IPC. It contains a schema (metadata) and a
// corresponding sequence of equal-length Arrow arrays
class ARROW_EXPORT RecordBatch {
 public:
  // num_rows is a parameter to allow for record batches of a particular size not
  // having any materialized columns. Each array should have the same length as
  // num_rows
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
      const std::vector<std::shared_ptr<Array>>& columns);

  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
      std::vector<std::shared_ptr<Array>>&& columns);

  bool Equals(const RecordBatch& other) const;

  bool ApproxEquals(const RecordBatch& other) const;

  // @returns: the table's schema
  std::shared_ptr<Schema> schema() const { return schema_; }

  // @returns: the i-th column
  // Note: Does not boundscheck
  std::shared_ptr<Array> column(int i) const { return columns_[i]; }

  const std::vector<std::shared_ptr<Array>>& columns() const { return columns_; }

  const std::string& column_name(int i) const;

  // @returns: the number of columns in the table
  int num_columns() const { return static_cast<int>(columns_.size()); }

  // @returns: the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// Slice each of the arrays in the record batch and construct a new RecordBatch object
  std::shared_ptr<RecordBatch> Slice(int64_t offset);
  std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length);

  /// Returns error status is there is something wrong with the record batch
  /// contents, like a schema/array mismatch or inconsistent lengths
  Status Validate() const;

 private:
  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;
  std::vector<std::shared_ptr<Array>> columns_;
};

// Immutable container of fixed-length columns conforming to a particular schema
class ARROW_EXPORT Table {
 public:
  // If columns is zero-length, the table's number of rows is zero
  Table(const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<Column>>& columns);

  // num_rows is a parameter to allow for tables of a particular size not
  // having any materialized columns. Each column should therefore have the
  // same length as num_rows -- you can validate this using
  // Table::ValidateColumns
  Table(const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<Column>>& columns, int64_t num_rows);

  // Construct table from RecordBatch, but only if all of the batch schemas are
  // equal. Returns Status::Invalid if there is some problem
  static Status FromRecordBatches(
      const std::vector<std::shared_ptr<RecordBatch>>& batches,
      std::shared_ptr<Table>* table);

  // @returns: the table's schema
  std::shared_ptr<Schema> schema() const { return schema_; }

  // Note: Does not boundscheck
  // @returns: the i-th column
  std::shared_ptr<Column> column(int i) const { return columns_[i]; }

  /// Remove column from the table, producing a new Table (because tables and
  /// schemas are immutable)
  Status RemoveColumn(int i, std::shared_ptr<Table>* out) const;

  /// Add column to the table, producing a new Table
  Status AddColumn(
      int i, const std::shared_ptr<Column>& column, std::shared_ptr<Table>* out) const;

  // @returns: the number of columns in the table
  int num_columns() const { return static_cast<int>(columns_.size()); }

  // @returns: the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  bool Equals(const Table& other) const;

  // After construction, perform any checks to validate the input arguments
  Status ValidateColumns() const;

 private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<Column>> columns_;

  int64_t num_rows_;
};

// Construct table from multiple input tables. Return Status::Invalid if
// schemas are not equal
Status ARROW_EXPORT ConcatenateTables(
    const std::vector<std::shared_ptr<Table>>& tables, std::shared_ptr<Table>* table);

}  // namespace arrow

#endif  // ARROW_TABLE_H
