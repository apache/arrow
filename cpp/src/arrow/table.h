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

#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

using ArrayVector = std::vector<std::shared_ptr<Array>>;

/// \class ChunkedArray
/// \brief A data structure managing a list of primitive Arrow arrays logically
/// as one large array
class ARROW_EXPORT ChunkedArray {
 public:
  /// \brief Construct a chunked array from a vector of arrays
  ///
  /// The vector must be non-empty and all its elements must have the same
  /// data type.
  explicit ChunkedArray(const ArrayVector& chunks);

  /// \brief Construct a chunked array from a single Array
  explicit ChunkedArray(const std::shared_ptr<Array>& chunk)
      : ChunkedArray(ArrayVector({chunk})) {}

  /// \brief Construct a chunked array from a vector of arrays and a data type
  ///
  /// As the data type is passed explicitly, the vector may be empty.
  ChunkedArray(const ArrayVector& chunks, const std::shared_ptr<DataType>& type);

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

  /// \brief Flatten this chunked array as a vector of chunked arrays, one
  /// for each struct field
  ///
  /// \param[in] pool The pool for buffer allocations, if any
  /// \param[out] out The resulting vector of arrays
  Status Flatten(MemoryPool* pool, std::vector<std::shared_ptr<ChunkedArray>>* out) const;

  /// Construct a zero-copy view of this chunked array with the given
  /// type. Calls Array::View on each constituent chunk. Always succeeds if
  /// there are zero chunks
  Status View(const std::shared_ptr<DataType>& type,
              std::shared_ptr<ChunkedArray>* out) const;

  std::shared_ptr<DataType> type() const { return type_; }

  /// \brief Determine if two chunked arrays are equal.
  ///
  /// Two chunked arrays can be equal only if they have equal datatypes.
  /// However, they may be equal even if they have different chunkings.
  bool Equals(const ChunkedArray& other) const;
  /// \brief Determine if two chunked arrays are equal.
  bool Equals(const std::shared_ptr<ChunkedArray>& other) const;

  /// \brief Perform cheap validation checks to determine obvious inconsistencies
  /// within the chunk array's internal data.
  ///
  /// This is O(k*m) where k is the number of array descendents,
  /// and m is the number of chunks.
  ///
  /// \return Status
  Status Validate() const;

  /// \brief Perform extensive validation checks to determine inconsistencies
  /// within the chunk array's internal data.
  ///
  /// This is O(k*n) where k is the number of array descendents,
  /// and n is the length in elements.
  ///
  /// \return Status
  Status ValidateFull() const;

 protected:
  ArrayVector chunks_;
  int64_t length_;
  int64_t null_count_;
  std::shared_ptr<DataType> type_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ChunkedArray);
};

/// \class Table
/// \brief Logical table as sequence of chunked arrays
class ARROW_EXPORT Table {
 public:
  virtual ~Table() = default;

  /// \brief Construct a Table from schema and columns
  /// If columns is zero-length, the table's number of rows is zero
  /// \param schema The table schema (column types)
  /// \param columns The table's columns as chunked arrays
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  static std::shared_ptr<Table> Make(
      const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<ChunkedArray>>& columns, int64_t num_rows = -1);

  /// \brief Construct a Table from schema and arrays
  /// \param schema The table schema (column types)
  /// \param arrays The table's columns as arrays
  /// \param num_rows number of rows in table, -1 (default) to infer from columns
  static std::shared_ptr<Table> Make(const std::shared_ptr<Schema>& schema,
                                     const std::vector<std::shared_ptr<Array>>& arrays,
                                     int64_t num_rows = -1);

  /// \brief Construct a Table from RecordBatches, using schema supplied by the first
  /// RecordBatch.
  ///
  /// \param[in] batches a std::vector of record batches
  /// \param[out] table the returned table
  /// \return Status Returns Status::Invalid if there is some problem
  static Status FromRecordBatches(
      const std::vector<std::shared_ptr<RecordBatch>>& batches,
      std::shared_ptr<Table>* table);

  /// \brief Construct a Table from RecordBatches, using supplied schema. There may be
  /// zero record batches
  ///
  /// \param[in] schema the arrow::Schema for each batch
  /// \param[in] batches a std::vector of record batches
  /// \param[out] table the returned table
  /// \return Status
  static Status FromRecordBatches(
      const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<RecordBatch>>& batches,
      std::shared_ptr<Table>* table);

  /// \brief Construct a Table from a chunked StructArray. One column will be produced
  /// for each field of the StructArray.
  ///
  /// \param[in] array a chunked StructArray
  /// \param[out] table the returned table
  /// \return Status
  static Status FromChunkedStructArray(const std::shared_ptr<ChunkedArray>& array,
                                       std::shared_ptr<Table>* table);

  /// \brief Return the table schema
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \brief Return a column by index
  virtual std::shared_ptr<ChunkedArray> column(int i) const = 0;

  /// \brief Return vector of all columns for table
  std::vector<std::shared_ptr<ChunkedArray>> columns() const;

  /// Return a column's field by index
  std::shared_ptr<Field> field(int i) const { return schema_->field(i); }

  /// \brief Return vector of all fields for table
  std::vector<std::shared_ptr<Field>> fields() const;

  /// \brief Construct a zero-copy slice of the table with the
  /// indicated offset and length
  ///
  /// \param[in] offset the index of the first row in the constructed
  /// slice
  /// \param[in] length the number of rows of the slice. If there are not enough
  /// rows in the table, the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<Table>
  virtual std::shared_ptr<Table> Slice(int64_t offset, int64_t length) const = 0;

  /// \brief Slice from first row at offset until end of the table
  std::shared_ptr<Table> Slice(int64_t offset) const { return Slice(offset, num_rows_); }

  /// \brief Return a column by name
  /// \param[in] name field name
  /// \return an Array or null if no field was found
  std::shared_ptr<ChunkedArray> GetColumnByName(const std::string& name) const {
    auto i = schema_->GetFieldIndex(name);
    return i == -1 ? NULLPTR : column(i);
  }

  /// \brief Remove column from the table, producing a new Table
  virtual Status RemoveColumn(int i, std::shared_ptr<Table>* out) const = 0;

  /// \brief Add column to the table, producing a new Table
  virtual Status AddColumn(int i, std::shared_ptr<Field> field_arg,
                           std::shared_ptr<ChunkedArray> column,
                           std::shared_ptr<Table>* out) const = 0;

  /// \brief Replace a column in the table, producing a new Table
  virtual Status SetColumn(int i, std::shared_ptr<Field> field_arg,
                           std::shared_ptr<ChunkedArray> column,
                           std::shared_ptr<Table>* out) const = 0;

  /// \brief Return names of all columns
  std::vector<std::string> ColumnNames() const;

  /// \brief Rename columns with provided names
  Status RenameColumns(const std::vector<std::string>& names,
                       std::shared_ptr<Table>* out) const;

  /// \brief Replace schema key-value metadata with new metadata (EXPERIMENTAL)
  /// \since 0.5.0
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new Table
  virtual std::shared_ptr<Table> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const = 0;

  /// \brief Flatten the table, producing a new Table.  Any column with a
  /// struct type will be flattened into multiple columns
  ///
  /// \param[in] pool The pool for buffer allocations, if any
  /// \param[out] out The returned table
  virtual Status Flatten(MemoryPool* pool, std::shared_ptr<Table>* out) const = 0;

  /// \brief Perform cheap validation checks to determine obvious inconsistencies
  /// within the table's schema and internal data.
  ///
  /// This is O(k*m) where k is the total number of field descendents,
  /// and m is the number of chunks.
  ///
  /// \return Status
  virtual Status Validate() const = 0;

  /// \brief Perform extensive validation checks to determine inconsistencies
  /// within the table's schema and internal data.
  ///
  /// This is O(k*n) where k is the total number of field descendents,
  /// and n is the number of rows.
  ///
  /// \return Status
  virtual Status ValidateFull() const = 0;

  /// \brief Return the number of columns in the table
  int num_columns() const { return schema_->num_fields(); }

  /// \brief Return the number of rows (equal to each column's logical length)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Determine if tables are equal
  ///
  /// Two tables can be equal only if they have equal schemas.
  /// However, they may be equal even if they have different chunkings.
  bool Equals(const Table& other) const;

  /// \brief Make a new table by combining the chunks this table has.
  ///
  /// All the underlying chunks in the ChunkedArray of each column are
  /// concatenated into zero or one chunk.
  ///
  /// \param[in] pool The pool for buffer allocations
  /// \param[out] out The table with chunks combined
  Status CombineChunks(MemoryPool* pool, std::shared_ptr<Table>* out) const;

 protected:
  Table();

  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Table);
};

/// \brief Compute a stream of record batches from a (possibly chunked) Table
///
/// The conversion is zero-copy: each record batch is a view over a slice
/// of the table's columns.
class ARROW_EXPORT TableBatchReader : public RecordBatchReader {
 public:
  /// \brief Construct a TableBatchReader for the given table
  explicit TableBatchReader(const Table& table);

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override;

  /// \brief Set the desired maximum chunk size of record batches
  ///
  /// The actual chunk size of each record batch may be smaller, depending
  /// on actual chunking characteristics of each table column.
  void set_chunksize(int64_t chunksize);

 private:
  const Table& table_;
  std::vector<ChunkedArray*> column_data_;
  std::vector<int> chunk_numbers_;
  std::vector<int64_t> chunk_offsets_;
  int64_t absolute_row_position_;
  int64_t max_chunksize_;
};

/// \brief Construct table from multiple input tables.
///
/// The tables are concatenated vertically.  Therefore, all tables should
/// have the same schema.  Each column in the output table is the result
/// of concatenating the corresponding columns in all input tables.
ARROW_EXPORT
Status ConcatenateTables(const std::vector<std::shared_ptr<Table>>& tables,
                         std::shared_ptr<Table>* table);

/// \brief Promotes a table to conform to the given schema.
///
/// If a field in the schema does not have a corresponding column in the
/// table, a column of nulls will be added to the resulting table.
/// If the corresponding column is of type Null, it will be promoted to
/// the type specified by schema, with null values filled.
/// Returns an error:
/// - if the corresponding column's type is not compatible with the
///   schema.
/// - if there is a column in the table that does not exist in the schema.
///
/// \param[in] table the input Table
/// \param[in] schema the target schema to promote to
/// \param[in] pool The memory pool to be used if null-filled arrays need to
/// be created.
ARROW_EXPORT
Result<std::shared_ptr<Table>> PromoteTableToSchema(
    const std::shared_ptr<Table>& table, const std::shared_ptr<Schema>& schema,
    MemoryPool* pool = default_memory_pool());

/// \brief Concatenate tables with null-filling and type promotion.
///
/// Columns of the same name will be concatenated. They should be of the
/// same type, or be of type NULL, in which case it will be promoted to
/// the type of other corresponding columns with null values filled.
/// If a table is missing a particular field, null values of the appropriate
//  type will be generated to take the place of the missing field
/// The new schema will share the metadata with the first table. Each field in
/// the new schema will share the metadata with the first table which has the
/// field defined.
///
/// \param[in] tables the tables to be concatenated
/// \param[in] pool The memory pool to be used if null-filled arrays need to
/// be created.
ARROW_EXPORT
Result<std::shared_ptr<Table>> ConcatenateTablesWithPromotion(
    const std::vector<std::shared_ptr<Table>>& tables,
    MemoryPool* pool = default_memory_pool());

}  // namespace arrow

#endif  // ARROW_TABLE_H
