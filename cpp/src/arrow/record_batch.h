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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// \class RecordBatch
/// \brief Collection of equal-length arrays matching a particular Schema
///
/// A record batch is table-like data structure that is semantically a sequence
/// of fields, each a contiguous Arrow array
class ARROW_EXPORT RecordBatch {
 public:
  virtual ~RecordBatch() = default;

  /// \param[in] schema The record batch schema
  /// \param[in] num_rows length of fields in the record batch. Each array
  /// should have the same length as num_rows
  /// \param[in] columns the record batch fields as vector of arrays
  static std::shared_ptr<RecordBatch> Make(std::shared_ptr<Schema> schema,
                                           int64_t num_rows,
                                           std::vector<std::shared_ptr<Array>> columns);

  /// \brief Construct record batch from vector of internal data structures
  /// \since 0.5.0
  ///
  /// This class is intended for internal use, or advanced users.
  ///
  /// \param schema the record batch schema
  /// \param num_rows the number of semantic rows in the record batch. This
  /// should be equal to the length of each field
  /// \param columns the data for the batch's columns
  static std::shared_ptr<RecordBatch> Make(
      std::shared_ptr<Schema> schema, int64_t num_rows,
      std::vector<std::shared_ptr<ArrayData>> columns);

  /// \brief Convert record batch to struct array
  ///
  /// Create a struct array whose child arrays are the record batch's columns.
  /// Note that the record batch's top-level field metadata cannot be reflected
  /// in the resulting struct array.
  Result<std::shared_ptr<StructArray>> ToStructArray() const;

  /// \brief Construct record batch from struct array
  ///
  /// This constructs a record batch using the child arrays of the given
  /// array, which must be a struct array.  Note that the struct array's own
  /// null bitmap is not reflected in the resulting record batch.
  static Result<std::shared_ptr<RecordBatch>> FromStructArray(
      const std::shared_ptr<Array>& array);

  /// \brief Determine if two record batches are exactly equal
  ///
  /// \param[in] other the RecordBatch to compare with
  /// \param[in] check_metadata if true, check that Schema metadata is the same
  /// \return true if batches are equal
  bool Equals(const RecordBatch& other, bool check_metadata = false) const;

  /// \brief Determine if two record batches are approximately equal
  bool ApproxEquals(const RecordBatch& other) const;

  // \return the table's schema
  /// \return true if batches are equal
  std::shared_ptr<Schema> schema() const { return schema_; }

  /// \brief Retrieve all columns at once
  std::vector<std::shared_ptr<Array>> columns() const;

  /// \brief Retrieve an array from the record batch
  /// \param[in] i field index, does not boundscheck
  /// \return an Array object
  virtual std::shared_ptr<Array> column(int i) const = 0;

  /// \brief Retrieve an array from the record batch
  /// \param[in] name field name
  /// \return an Array or null if no field was found
  std::shared_ptr<Array> GetColumnByName(const std::string& name) const;

  /// \brief Retrieve an array's internal data from the record batch
  /// \param[in] i field index, does not boundscheck
  /// \return an internal ArrayData object
  virtual std::shared_ptr<ArrayData> column_data(int i) const = 0;

  /// \brief Retrieve all arrays' internal data from the record batch.
  virtual ArrayDataVector column_data() const = 0;

  /// \brief Add column to the record batch, producing a new RecordBatch
  ///
  /// \param[in] i field index, which will be boundschecked
  /// \param[in] field field to be added
  /// \param[in] column column to be added
  virtual Result<std::shared_ptr<RecordBatch>> AddColumn(
      int i, const std::shared_ptr<Field>& field,
      const std::shared_ptr<Array>& column) const = 0;

  /// \brief Add new nullable column to the record batch, producing a new
  /// RecordBatch.
  ///
  /// For non-nullable columns, use the Field-based version of this method.
  ///
  /// \param[in] i field index, which will be boundschecked
  /// \param[in] field_name name of field to be added
  /// \param[in] column column to be added
  virtual Result<std::shared_ptr<RecordBatch>> AddColumn(
      int i, std::string field_name, const std::shared_ptr<Array>& column) const;

  /// \brief Remove column from the record batch, producing a new RecordBatch
  ///
  /// \param[in] i field index, does boundscheck
  virtual Result<std::shared_ptr<RecordBatch>> RemoveColumn(int i) const = 0;

  virtual std::shared_ptr<RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const = 0;

  /// \brief Name in i-th column
  const std::string& column_name(int i) const;

  /// \return the number of columns in the table
  int num_columns() const;

  /// \return the number of rows (the corresponding length of each column)
  int64_t num_rows() const { return num_rows_; }

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice, through end of batch
  /// \return new record batch
  virtual std::shared_ptr<RecordBatch> Slice(int64_t offset) const;

  /// \brief Slice each of the arrays in the record batch
  /// \param[in] offset the starting offset to slice
  /// \param[in] length the number of elements to slice from offset
  /// \return new record batch
  virtual std::shared_ptr<RecordBatch> Slice(int64_t offset, int64_t length) const = 0;

  /// \return PrettyPrint representation suitable for debugging
  std::string ToString() const;

  /// \brief Perform cheap validation checks to determine obvious inconsistencies
  /// within the record batch's schema and internal data.
  ///
  /// This is O(k) where k is the total number of fields and array descendents.
  ///
  /// \return Status
  virtual Status Validate() const;

  /// \brief Perform extensive validation checks to determine inconsistencies
  /// within the record batch's schema and internal data.
  ///
  /// This is potentially O(k*n) where n is the number of rows.
  ///
  /// \return Status
  virtual Status ValidateFull() const;

 protected:
  RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows);

  std::shared_ptr<Schema> schema_;
  int64_t num_rows_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(RecordBatch);
};

/// \brief Abstract interface for reading stream of record batches
class ARROW_EXPORT RecordBatchReader {
 public:
  virtual ~RecordBatchReader() = default;

  /// \return the shared schema of the record batches in the stream
  virtual std::shared_ptr<Schema> schema() const = 0;

  /// \brief Read the next record batch in the stream. Return null for batch
  /// when reaching end of stream
  ///
  /// \param[out] batch the next loaded batch, null at end of stream
  /// \return Status
  virtual Status ReadNext(std::shared_ptr<RecordBatch>* batch) = 0;

  /// \brief Iterator interface
  Result<std::shared_ptr<RecordBatch>> Next() {
    std::shared_ptr<RecordBatch> batch;
    ARROW_RETURN_NOT_OK(ReadNext(&batch));
    return batch;
  }

  /// \brief Consume entire stream as a vector of record batches
  Status ReadAll(RecordBatchVector* batches);

  /// \brief Read all batches and concatenate as arrow::Table
  Status ReadAll(std::shared_ptr<Table>* table);

  /// \brief Create a RecordBatchReader from a vector of RecordBatch.
  ///
  /// \param[in] batches the vector of RecordBatch to read from
  /// \param[in] schema schema to conform to. Will be inferred from the first
  ///            element if not provided.
  static Result<std::shared_ptr<RecordBatchReader>> Make(
      RecordBatchVector batches, std::shared_ptr<Schema> schema = NULLPTR);
};

}  // namespace arrow
