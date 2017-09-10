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

#include "arrow/table.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <sstream>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

namespace arrow {

using internal::ArrayData;

// ----------------------------------------------------------------------
// ChunkedArray and Column methods

ChunkedArray::ChunkedArray(const ArrayVector& chunks) : chunks_(chunks) {
  length_ = 0;
  null_count_ = 0;
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

bool ChunkedArray::Equals(const ChunkedArray& other) const {
  if (length_ != other.length()) {
    return false;
  }
  if (null_count_ != other.null_count()) {
    return false;
  }

  // Check contents of the underlying arrays. This checks for equality of
  // the underlying data independently of the chunk size.
  int this_chunk_idx = 0;
  int64_t this_start_idx = 0;
  int other_chunk_idx = 0;
  int64_t other_start_idx = 0;

  int64_t elements_compared = 0;
  while (elements_compared < length_) {
    const std::shared_ptr<Array> this_array = chunks_[this_chunk_idx];
    const std::shared_ptr<Array> other_array = other.chunk(other_chunk_idx);
    int64_t common_length = std::min(this_array->length() - this_start_idx,
                                     other_array->length() - other_start_idx);
    if (!this_array->RangeEquals(this_start_idx, this_start_idx + common_length,
                                 other_start_idx, other_array)) {
      return false;
    }

    elements_compared += common_length;

    // If we have exhausted the current chunk, proceed to the next one individually.
    if (this_start_idx + common_length == this_array->length()) {
      this_chunk_idx++;
      this_start_idx = 0;
    } else {
      this_start_idx += common_length;
    }

    if (other_start_idx + common_length == other_array->length()) {
      other_chunk_idx++;
      other_start_idx = 0;
    } else {
      other_start_idx += common_length;
    }
  }
  return true;
}

bool ChunkedArray::Equals(const std::shared_ptr<ChunkedArray>& other) const {
  if (this == other.get()) {
    return true;
  }
  if (!other) {
    return false;
  }
  return Equals(*other.get());
}

Column::Column(const std::shared_ptr<Field>& field, const ArrayVector& chunks)
    : field_(field) {
  data_ = std::make_shared<ChunkedArray>(chunks);
}

Column::Column(const std::shared_ptr<Field>& field, const std::shared_ptr<Array>& data)
    : field_(field) {
  if (data) {
    data_ = std::make_shared<ChunkedArray>(ArrayVector({data}));
  } else {
    data_ = std::make_shared<ChunkedArray>(ArrayVector({}));
  }
}

Column::Column(const std::string& name, const std::shared_ptr<Array>& data)
    : Column(::arrow::field(name, data->type()), data) {}

Column::Column(const std::shared_ptr<Field>& field,
               const std::shared_ptr<ChunkedArray>& data)
    : field_(field), data_(data) {}

bool Column::Equals(const Column& other) const {
  if (!field_->Equals(other.field())) {
    return false;
  }
  return data_->Equals(other.data());
}

bool Column::Equals(const std::shared_ptr<Column>& other) const {
  if (this == other.get()) {
    return true;
  }
  if (!other) {
    return false;
  }

  return Equals(*other.get());
}

Status Column::ValidateData() {
  for (int i = 0; i < data_->num_chunks(); ++i) {
    std::shared_ptr<DataType> type = data_->chunk(i)->type();
    if (!this->type()->Equals(type)) {
      std::stringstream ss;
      ss << "In chunk " << i << " expected type " << this->type()->ToString()
         << " but saw " << type->ToString();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// RecordBatch methods

void AssertBatchValid(const RecordBatch& batch) {
  Status s = batch.Validate();
  if (!s.ok()) {
    DCHECK(false) << s.ToString();
  }
}

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows)
    : schema_(schema), num_rows_(num_rows) {
  boxed_columns_.resize(schema->num_fields());
}

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                         const std::vector<std::shared_ptr<Array>>& columns)
    : RecordBatch(schema, num_rows) {
  columns_.resize(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    columns_[i] = columns[i]->data();
  }
}

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                         std::vector<std::shared_ptr<Array>>&& columns)
    : RecordBatch(schema, num_rows) {
  columns_.resize(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    columns_[i] = columns[i]->data();
  }
}

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                         std::vector<std::shared_ptr<ArrayData>>&& columns)
    : RecordBatch(schema, num_rows) {
  columns_ = std::move(columns);
}

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows,
                         const std::vector<std::shared_ptr<ArrayData>>& columns)
    : RecordBatch(schema, num_rows) {
  columns_ = columns;
}

std::shared_ptr<Array> RecordBatch::column(int i) const {
  if (!boxed_columns_[i]) {
    DCHECK(internal::MakeArray(columns_[i], &boxed_columns_[i]).ok());
  }
  return boxed_columns_[i];
}

const std::string& RecordBatch::column_name(int i) const {
  return schema_->field(i)->name();
}

bool RecordBatch::Equals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->Equals(other.column(i))) {
      return false;
    }
  }

  return true;
}

bool RecordBatch::ApproxEquals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->ApproxEquals(other.column(i))) {
      return false;
    }
  }

  return true;
}

std::shared_ptr<RecordBatch> RecordBatch::ReplaceSchemaMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  auto new_schema = schema_->AddMetadata(metadata);
  return std::make_shared<RecordBatch>(new_schema, num_rows_, columns_);
}

std::shared_ptr<RecordBatch> RecordBatch::Slice(int64_t offset) const {
  return Slice(offset, this->num_rows() - offset);
}

std::shared_ptr<RecordBatch> RecordBatch::Slice(int64_t offset, int64_t length) const {
  std::vector<std::shared_ptr<ArrayData>> arrays;
  arrays.reserve(num_columns());
  for (const auto& field : columns_) {
    int64_t col_length = std::min(field->length - offset, length);
    int64_t col_offset = field->offset + offset;

    auto new_data = std::make_shared<ArrayData>(*field);
    new_data->length = col_length;
    new_data->offset = col_offset;
    new_data->null_count = kUnknownNullCount;
    arrays.emplace_back(new_data);
  }
  int64_t num_rows = std::min(num_rows_ - offset, length);
  return std::make_shared<RecordBatch>(schema_, num_rows, std::move(arrays));
}

Status RecordBatch::Validate() const {
  for (int i = 0; i < num_columns(); ++i) {
    const ArrayData& arr = *columns_[i];
    if (arr.length != num_rows_) {
      std::stringstream ss;
      ss << "Number of rows in column " << i << " did not match batch: " << arr.length
         << " vs " << num_rows_;
      return Status::Invalid(ss.str());
    }
    const auto& schema_type = *schema_->field(i)->type();
    if (!arr.type->Equals(schema_type)) {
      std::stringstream ss;
      ss << "Column " << i << " type not match schema: " << arr.type->ToString() << " vs "
         << schema_type.ToString();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Table methods

Table::Table(const std::shared_ptr<Schema>& schema,
             const std::vector<std::shared_ptr<Column>>& columns, int64_t num_rows)
    : schema_(schema), columns_(columns) {
  if (num_rows < 0) {
    if (columns.size() == 0) {
      num_rows_ = 0;
    } else {
      num_rows_ = columns[0]->length();
    }
  } else {
    num_rows_ = num_rows;
  }
}

Table::Table(const std::shared_ptr<Schema>& schema,
             const std::vector<std::shared_ptr<Array>>& columns, int64_t num_rows)
    : schema_(schema) {
  if (num_rows < 0) {
    if (columns.size() == 0) {
      num_rows_ = 0;
    } else {
      num_rows_ = columns[0]->length();
    }
  } else {
    num_rows_ = num_rows;
  }

  columns_.resize(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    columns_[i] =
        std::make_shared<Column>(schema->field(static_cast<int>(i)), columns[i]);
  }
}

std::shared_ptr<Table> Table::ReplaceSchemaMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  auto new_schema = schema_->AddMetadata(metadata);
  return std::make_shared<Table>(new_schema, columns_);
}

Status Table::FromRecordBatches(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                                std::shared_ptr<Table>* table) {
  if (batches.size() == 0) {
    return Status::Invalid("Must pass at least one record batch");
  }

  std::shared_ptr<Schema> schema = batches[0]->schema();

  const int nbatches = static_cast<int>(batches.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 1; i < nbatches; ++i) {
    if (!batches[i]->schema()->Equals(*schema)) {
      std::stringstream ss;
      ss << "Schema at index " << static_cast<int>(i) << " was different: \n"
         << schema->ToString() << "\nvs\n"
         << batches[i]->schema()->ToString();
      return Status::Invalid(ss.str());
    }
  }

  std::vector<std::shared_ptr<Column>> columns(ncolumns);
  std::vector<std::shared_ptr<Array>> column_arrays(nbatches);

  for (int i = 0; i < ncolumns; ++i) {
    for (int j = 0; j < nbatches; ++j) {
      column_arrays[j] = batches[j]->column(i);
    }
    columns[i] = std::make_shared<Column>(schema->field(i), column_arrays);
  }

  *table = std::make_shared<Table>(schema, columns);
  return Status::OK();
}

Status ConcatenateTables(const std::vector<std::shared_ptr<Table>>& tables,
                         std::shared_ptr<Table>* table) {
  if (tables.size() == 0) {
    return Status::Invalid("Must pass at least one table");
  }

  std::shared_ptr<Schema> schema = tables[0]->schema();

  const int ntables = static_cast<int>(tables.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 1; i < ntables; ++i) {
    if (!tables[i]->schema()->Equals(*schema)) {
      std::stringstream ss;
      ss << "Schema at index " << static_cast<int>(i) << " was different: \n"
         << schema->ToString() << "\nvs\n"
         << tables[i]->schema()->ToString();
      return Status::Invalid(ss.str());
    }
  }

  std::vector<std::shared_ptr<Column>> columns(ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    std::vector<std::shared_ptr<Array>> column_arrays;
    for (int j = 0; j < ntables; ++j) {
      const std::vector<std::shared_ptr<Array>>& chunks =
          tables[j]->column(i)->data()->chunks();
      for (const auto& chunk : chunks) {
        column_arrays.push_back(chunk);
      }
    }
    columns[i] = std::make_shared<Column>(schema->field(i), column_arrays);
  }
  *table = std::make_shared<Table>(schema, columns);
  return Status::OK();
}

bool Table::Equals(const Table& other) const {
  if (this == &other) {
    return true;
  }
  if (!schema_->Equals(*other.schema())) {
    return false;
  }
  if (static_cast<int64_t>(columns_.size()) != other.num_columns()) {
    return false;
  }

  for (int i = 0; i < static_cast<int>(columns_.size()); i++) {
    if (!columns_[i]->Equals(other.column(i))) {
      return false;
    }
  }
  return true;
}

Status Table::RemoveColumn(int i, std::shared_ptr<Table>* out) const {
  std::shared_ptr<Schema> new_schema;
  RETURN_NOT_OK(schema_->RemoveField(i, &new_schema));

  *out = std::make_shared<Table>(new_schema, internal::DeleteVectorElement(columns_, i));
  return Status::OK();
}

Status Table::AddColumn(int i, const std::shared_ptr<Column>& col,
                        std::shared_ptr<Table>* out) const {
  if (i < 0 || i > num_columns() + 1) {
    return Status::Invalid("Invalid column index.");
  }
  if (col == nullptr) {
    std::stringstream ss;
    ss << "Column " << i << " was null";
    return Status::Invalid(ss.str());
  }
  if (col->length() != num_rows_) {
    std::stringstream ss;
    ss << "Added column's length must match table's length. Expected length " << num_rows_
       << " but got length " << col->length();
    return Status::Invalid(ss.str());
  }

  std::shared_ptr<Schema> new_schema;
  RETURN_NOT_OK(schema_->AddField(i, col->field(), &new_schema));

  *out =
      std::make_shared<Table>(new_schema, internal::AddVectorElement(columns_, i, col));
  return Status::OK();
}

Status Table::ValidateColumns() const {
  if (num_columns() != schema_->num_fields()) {
    return Status::Invalid("Number of columns did not match schema");
  }

  // Make sure columns are all the same length
  for (size_t i = 0; i < columns_.size(); ++i) {
    const Column* col = columns_[i].get();
    if (col == nullptr) {
      std::stringstream ss;
      ss << "Column " << i << " was null";
      return Status::Invalid(ss.str());
    }
    if (col->length() != num_rows_) {
      std::stringstream ss;
      ss << "Column " << i << " named " << col->name() << " expected length " << num_rows_
         << " but got length " << col->length();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

bool Table::IsChunked() const {
  for (size_t i = 0; i < columns_.size(); ++i) {
    if (columns_[i]->data()->num_chunks() > 1) {
      return true;
    }
  }
  return false;
}

Status MakeTable(const std::shared_ptr<Schema>& schema,
                 const std::vector<std::shared_ptr<Array>>& arrays,
                 std::shared_ptr<Table>* table) {
  // Make sure the length of the schema corresponds to the length of the vector
  if (schema->num_fields() != static_cast<int>(arrays.size())) {
    std::stringstream ss;
    ss << "Schema and Array vector have different lengths: " << schema->num_fields()
       << " != " << arrays.size();
    return Status::Invalid(ss.str());
  }

  std::vector<std::shared_ptr<Column>> columns;
  columns.reserve(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); i++) {
    columns.emplace_back(std::make_shared<Column>(schema->field(i), arrays[i]));
  }

  *table = std::make_shared<Table>(schema, columns);

  return Status::OK();
}

// ----------------------------------------------------------------------
// Base record batch reader

RecordBatchReader::~RecordBatchReader() {}

#ifndef ARROW_NO_DEPRECATED_API
Status RecordBatchReader::ReadNextRecordBatch(std::shared_ptr<RecordBatch>* batch) {
  return ReadNext(batch);
}
#endif

// ----------------------------------------------------------------------
// Convert a table to a sequence of record batches

class TableBatchReader::TableBatchReaderImpl {
 public:
  explicit TableBatchReaderImpl(const Table& table)
      : table_(table),
        column_data_(table.num_columns()),
        chunk_numbers_(table.num_columns(), 0),
        chunk_offsets_(table.num_columns(), 0),
        absolute_row_position_(0) {
    for (int i = 0; i < table.num_columns(); ++i) {
      column_data_[i] = table.column(i)->data().get();
    }
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) {
    if (absolute_row_position_ == table_.num_rows()) {
      *out = nullptr;
      return Status::OK();
    }

    // Determine the minimum contiguous slice across all columns
    int64_t chunksize = table_.num_rows();
    std::vector<const Array*> chunks(table_.num_columns());
    for (int i = 0; i < table_.num_columns(); ++i) {
      auto chunk = column_data_[i]->chunk(chunk_numbers_[i]).get();
      int64_t chunk_remaining = chunk->length() - chunk_offsets_[i];

      if (chunk_remaining < chunksize) {
        chunksize = chunk_remaining;
      }

      chunks[i] = chunk;
    }

    // Slice chunks and advance chunk index as appropriate
    std::vector<std::shared_ptr<ArrayData>> batch_data;
    batch_data.reserve(table_.num_columns());

    for (int i = 0; i < table_.num_columns(); ++i) {
      // Exhausted chunk
      const Array* chunk = chunks[i];
      const int64_t offset = chunk_offsets_[i];
      std::shared_ptr<ArrayData> slice_data;
      if ((chunk->length() - offset) == chunksize) {
        ++chunk_numbers_[i];
        chunk_offsets_[i] = 0;
        if (chunk_offsets_[i] > 0) {
          // Need to slice
          slice_data = chunk->Slice(offset, chunksize)->data();
        } else {
          // No slice
          slice_data = chunk->data();
        }
      } else {
        slice_data = chunk->Slice(offset, chunksize)->data();
      }
      batch_data.emplace_back(std::move(slice_data));
    }

    absolute_row_position_ += chunksize;
    *out =
        std::make_shared<RecordBatch>(table_.schema(), chunksize, std::move(batch_data));

    return Status::OK();
  }

  std::shared_ptr<Schema> schema() const { return table_.schema(); }

 private:
  const Table& table_;
  std::vector<ChunkedArray*> column_data_;
  std::vector<int> chunk_numbers_;
  std::vector<int64_t> chunk_offsets_;
  int64_t absolute_row_position_;
};

TableBatchReader::TableBatchReader(const Table& table) {
  impl_.reset(new TableBatchReaderImpl(table));
}

TableBatchReader::~TableBatchReader() {}

std::shared_ptr<Schema> TableBatchReader::schema() const { return impl_->schema(); }

Status TableBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  return impl_->ReadNext(out);
}

}  // namespace arrow
