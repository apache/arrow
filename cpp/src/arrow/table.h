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
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class KeyValueMetadata;
class Status;

/// \class ChunkedArray
/// \brief A data structure managing a list of primitive Arrow arrays logically
/// as one large array
class ARROW_EXPORT ChunkedArray {
 public:
  explicit ChunkedArray(const ArrayVector& chunks);

  /// \return the total length of the chunked array; computed on construction
  int64_t length() const { return length_; }

  /// \return the total number of nulls among all chunks
  int64_t null_count() const { return null_count_; }

  int num_chunks() const { return static_cast<int>(chunks_.size()); }

  /// \return chunk a particular chunk from the chunked array
  std::shared_ptr<Array> chunk(int i) const { return chunks_[i]; }

  const ArrayVector& chunks() const { return chunks_; }

  /// \brief Construct a zero-copy slice of the chunked array with the
  /// indicated offset and length
  ///
  /// \param[in] offset the position of the first element in the constructed
  /// slice
  /// \param[in] length the length of the slice. If there are not enough
  /// elements in the chunked array, the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<ChunkedArray>
  std::shared_ptr<ChunkedArray> Slice(int64_t offset, int64_t length) const;

  /// \brief Slice from offset until end of the chunked array
  std::shared_ptr<ChunkedArray> Slice(int64_t offset) const;

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

/// \class Column
/// \brief An immutable column data structure consisting of a field (type
/// metadata) and a chunked data array
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

  /// \brief Construct a zero-copy slice of the column with the indicated
  /// offset and length
  ///
  /// \param[in] offset the position of the first element in the constructed
  /// slice
  /// \param[in] length the length of the slice. If there are not enough
  /// elements in the column, the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<Column>
  std::shared_ptr<Column> Slice(int64_t offset, int64_t length) const {
    return std::make_shared<Column>(field_, data_->Slice(offset, length));
  }

  /// \brief Slice from offset until end of the column
  std::shared_ptr<Column> Slice(int64_t offset) const {
    return std::make_shared<Column>(field_, data_->Slice(offset));
  }

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

/// \class Table
/// \brief Logical table as sequence of chunked arrays
class ARROW_EXPORT Table {
 public:
  virtual ~Table() = default;

  /// \brief Construct Table from schema and columns
  /// If columns is zero-length, the table's number of rows is zero
  /// \param schema The table schema (column types)
  /// \param columns The table's columns
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  static std::shared_ptr<Table> Make(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::shared_ptr<Column>>& columns,
                                     int64_t num_rows = -1);

  /// \brief Construct Table from schema and arrays
  /// \param schema The table schema (column types)
  /// \param arrays The table's columns as arrays
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  static std::shared_ptr<Table> Make(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::shared_ptr<Array>>& arrays,
                                     int64_t num_rows = -1);

  // Construct table from RecordBatch, but only if all of the batch schemas are
  // equal. Returns Status::Invalid if there is some problem
  static Status FromRecordBatches(
      const std::vector<std::shared_ptr<RecordBatch>>& batches,
      std::shared_ptr<Table>* table);

  /// \return the table's schema
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \param[in] i column index, does not boundscheck
  /// \return the i-th column
  virtual std::shared_ptr<Column> column(int i) const = 0;

  /// \brief Remove column from the table, producing a new Table
  virtual Status RemoveColumn(int i, std::shared_ptr<Table>* out) const = 0;

  /// \brief Add column to the table, producing a new Table
  virtual Status AddColumn(int i, const std::shared_ptr<Column>& column,
                           std::shared_ptr<Table>* out) const = 0;

  /// \brief Replace schema key-value metadata with new metadata (EXPERIMENTAL)
  /// \since 0.5.0
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new Table
  virtual std::shared_ptr<Table> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const = 0;

  /// \brief Perform any checks to validate the input arguments
  virtual Status Validate() const = 0;

  /// \return the number of columns in the table
  int num_columns() const { return schema_->num_fields(); }

  /// \return the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Determine if semantic contents of tables are exactly equal
  bool Equals(const Table& other) const;

 protected:
  Table();

  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Table);
};

/// \brief Compute a sequence of record batches from a (possibly chunked) Table
class ARROW_EXPORT TableBatchReader : public RecordBatchReader {
 public:
  ~TableBatchReader();

  /// \brief Read batches with the maximum possible size
  explicit TableBatchReader(const Table& table);

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override;

  void set_chunksize(int64_t chunksize);

 private:
  class TableBatchReaderImpl;
  std::unique_ptr<TableBatchReaderImpl> impl_;
};

/// \brief Construct table from multiple input tables.
/// \return Status, fails if any schemas are different
ARROW_EXPORT
Status ConcatenateTables(const std::vector<std::shared_ptr<Table>>& tables,
                         std::shared_ptr<Table>* table);

#ifndef ARROW_NO_DEPRECATED_API

/// \brief Construct table from multiple input tables.
/// \return Status, fails if any schemas are different
/// \note Deprecated since 0.8.0
ARROW_EXPORT
Status MakeTable(const std::shared_ptr<Schema>& schema,
                 const std::vector<std::shared_ptr<Array>>& arrays,
                 std::shared_ptr<Table>* table);

#endif

}  // namespace arrow

#endif  // ARROW_TABLE_H
