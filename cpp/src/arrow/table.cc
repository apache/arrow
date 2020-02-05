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
#include <limits>
#include <memory>
#include <sstream>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/validate.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// ChunkedArray methods

ChunkedArray::ChunkedArray(const ArrayVector& chunks) : chunks_(chunks) {
  length_ = 0;
  null_count_ = 0;

  ARROW_CHECK_GT(chunks.size(), 0)
      << "cannot construct ChunkedArray from empty vector and omitted type";
  type_ = chunks[0]->type();
  for (const std::shared_ptr<Array>& chunk : chunks) {
    length_ += chunk->length();
    null_count_ += chunk->null_count();
  }
}

ChunkedArray::ChunkedArray(const ArrayVector& chunks,
                           const std::shared_ptr<DataType>& type)
    : chunks_(chunks), type_(type) {
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
  if (length_ == 0) {
    return type_->Equals(other.type_);
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

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset, int64_t length) const {
  ARROW_CHECK_LE(offset, length_) << "Slice offset greater than array length";

  int curr_chunk = 0;
  while (curr_chunk < num_chunks() && offset >= chunk(curr_chunk)->length()) {
    offset -= chunk(curr_chunk)->length();
    curr_chunk++;
  }

  ArrayVector new_chunks;
  while (curr_chunk < num_chunks() && length > 0) {
    new_chunks.push_back(chunk(curr_chunk)->Slice(offset, length));
    length -= chunk(curr_chunk)->length() - offset;
    offset = 0;
    curr_chunk++;
  }

  return std::make_shared<ChunkedArray>(new_chunks, type_);
}

std::shared_ptr<ChunkedArray> ChunkedArray::Slice(int64_t offset) const {
  return Slice(offset, length_);
}

Status ChunkedArray::Flatten(MemoryPool* pool,
                             std::vector<std::shared_ptr<ChunkedArray>>* out) const {
  out->clear();
  if (type()->id() != Type::STRUCT) {
    // Emulate non-existent copy constructor
    *out = {std::make_shared<ChunkedArray>(chunks_, type_)};
    return Status::OK();
  }

  std::vector<ArrayVector> flattened_chunks(type()->num_children());
  for (const auto& chunk : chunks_) {
    ArrayVector res;
    RETURN_NOT_OK(checked_cast<const StructArray&>(*chunk).Flatten(pool, &res));
    DCHECK_EQ(res.size(), flattened_chunks.size());
    for (size_t i = 0; i < res.size(); ++i) {
      flattened_chunks[i].push_back(res[i]);
    }
  }

  out->resize(type()->num_children());
  for (size_t i = 0; i < out->size(); ++i) {
    auto child_type = type()->child(static_cast<int>(i))->type();
    out->at(i) = std::make_shared<ChunkedArray>(flattened_chunks[i], child_type);
  }
  return Status::OK();
}

Status ChunkedArray::View(const std::shared_ptr<DataType>& type,
                          std::shared_ptr<ChunkedArray>* out) const {
  ArrayVector out_chunks(this->num_chunks());
  for (int i = 0; i < this->num_chunks(); ++i) {
    RETURN_NOT_OK(chunks_[i]->View(type, &out_chunks[i]));
  }
  *out = std::make_shared<ChunkedArray>(out_chunks, type);
  return Status::OK();
}

Status ChunkedArray::Validate() const {
  if (chunks_.size() == 0) {
    return Status::OK();
  }

  const auto& type = *chunks_[0]->type();
  // Make sure chunks all have the same type
  for (size_t i = 1; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    if (!chunk.type()->Equals(type)) {
      return Status::Invalid("In chunk ", i, " expected type ", type.ToString(),
                             " but saw ", chunk.type()->ToString());
    }
  }
  // Validate the chunks themselves
  for (size_t i = 0; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    const Status st = internal::ValidateArray(chunk);
    if (!st.ok()) {
      return Status::Invalid("In chunk ", i, ": ", st.ToString());
    }
  }
  return Status::OK();
}

Status ChunkedArray::ValidateFull() const {
  RETURN_NOT_OK(Validate());
  for (size_t i = 0; i < chunks_.size(); ++i) {
    const Array& chunk = *chunks_[i];
    const Status st = internal::ValidateArrayData(chunk);
    if (!st.ok()) {
      return Status::Invalid("In chunk ", i, ": ", st.ToString());
    }
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Table methods

/// \class SimpleTable
/// \brief A basic, non-lazy in-memory table, like SimpleRecordBatch
class SimpleTable : public Table {
 public:
  SimpleTable(const std::shared_ptr<Schema>& schema,
              const std::vector<std::shared_ptr<ChunkedArray>>& columns,
              int64_t num_rows = -1)
      : columns_(columns) {
    schema_ = schema;
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

  SimpleTable(const std::shared_ptr<Schema>& schema,
              const std::vector<std::shared_ptr<Array>>& columns, int64_t num_rows = -1) {
    schema_ = schema;
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
      columns_[i] = std::make_shared<ChunkedArray>(columns[i]);
    }
  }

  std::shared_ptr<ChunkedArray> column(int i) const override { return columns_[i]; }

  std::shared_ptr<Table> Slice(int64_t offset, int64_t length) const override {
    auto sliced = columns_;
    int64_t num_rows = length;
    for (auto& column : sliced) {
      column = column->Slice(offset, length);
      num_rows = column->length();
    }
    return Table::Make(schema_, sliced, num_rows);
  }

  Status RemoveColumn(int i, std::shared_ptr<Table>* out) const override {
    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->RemoveField(i, &new_schema));

    *out = Table::Make(new_schema, internal::DeleteVectorElement(columns_, i),
                       this->num_rows());
    return Status::OK();
  }

  Status AddColumn(int i, std::shared_ptr<Field> field_arg,
                   std::shared_ptr<ChunkedArray> col,
                   std::shared_ptr<Table>* out) const override {
    DCHECK(col != nullptr);

    if (col->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match table's length. Expected length ", num_rows_,
          " but got length ", col->length());
    }

    if (!field_arg->type()->Equals(col->type())) {
      return Status::Invalid("Field type did not match data type");
    }

    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->AddField(i, field_arg, &new_schema));

    *out =
        Table::Make(new_schema, internal::AddVectorElement(columns_, i, std::move(col)));
    return Status::OK();
  }

  Status SetColumn(int i, std::shared_ptr<Field> field_arg,
                   std::shared_ptr<ChunkedArray> col,
                   std::shared_ptr<Table>* out) const override {
    DCHECK(col != nullptr);

    if (col->length() != num_rows_) {
      return Status::Invalid(
          "Added column's length must match table's length. Expected length ", num_rows_,
          " but got length ", col->length());
    }

    if (!field_arg->type()->Equals(col->type())) {
      return Status::Invalid("Field type did not match data type");
    }

    std::shared_ptr<Schema> new_schema;
    RETURN_NOT_OK(schema_->SetField(i, field_arg, &new_schema));
    *out = Table::Make(new_schema,
                       internal::ReplaceVectorElement(columns_, i, std::move(col)));
    return Status::OK();
  }

  std::shared_ptr<Table> ReplaceSchemaMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const override {
    auto new_schema = schema_->WithMetadata(metadata);
    return Table::Make(new_schema, columns_);
  }

  Status Flatten(MemoryPool* pool, std::shared_ptr<Table>* out) const override {
    std::vector<std::shared_ptr<Field>> flattened_fields;
    std::vector<std::shared_ptr<ChunkedArray>> flattened_columns;
    for (int i = 0; i < num_columns(); ++i) {
      std::vector<std::shared_ptr<ChunkedArray>> new_columns;
      std::vector<std::shared_ptr<Field>> new_fields = field(i)->Flatten();
      RETURN_NOT_OK(column(i)->Flatten(pool, &new_columns));
      DCHECK_EQ(new_columns.size(), new_fields.size());
      for (size_t j = 0; j < new_columns.size(); ++j) {
        flattened_fields.push_back(new_fields[j]);
        flattened_columns.push_back(new_columns[j]);
      }
    }
    auto flattened_schema =
        std::make_shared<Schema>(flattened_fields, schema_->metadata());
    *out = Table::Make(flattened_schema, flattened_columns);
    return Status::OK();
  }

  Status Validate() const override {
    RETURN_NOT_OK(ValidateMeta());
    for (int i = 0; i < num_columns(); ++i) {
      const ChunkedArray* col = columns_[i].get();
      Status st = col->Validate();
      if (!st.ok()) {
        std::stringstream ss;
        ss << "Column " << i << ": " << st.message();
        return st.WithMessage(ss.str());
      }
    }
    return Status::OK();
  }

  Status ValidateFull() const override {
    RETURN_NOT_OK(ValidateMeta());
    for (int i = 0; i < num_columns(); ++i) {
      const ChunkedArray* col = columns_[i].get();
      Status st = col->ValidateFull();
      if (!st.ok()) {
        std::stringstream ss;
        ss << "Column " << i << ": " << st.message();
        return st.WithMessage(ss.str());
      }
    }
    return Status::OK();
  }

 protected:
  Status ValidateMeta() const {
    // Make sure columns and schema are consistent
    if (static_cast<int>(columns_.size()) != schema_->num_fields()) {
      return Status::Invalid("Number of columns did not match schema");
    }
    for (int i = 0; i < num_columns(); ++i) {
      const ChunkedArray* col = columns_[i].get();
      if (col == nullptr) {
        return Status::Invalid("Column ", i, " was null");
      }
      if (!col->type()->Equals(*schema_->field(i)->type())) {
        return Status::Invalid("Column data for field ", i, " with type ",
                               col->type()->ToString(), " is inconsistent with schema ",
                               schema_->field(i)->type()->ToString());
      }
    }

    // Make sure columns are all the same length, and validate them
    for (int i = 0; i < num_columns(); ++i) {
      const ChunkedArray* col = columns_[i].get();
      if (col->length() != num_rows_) {
        return Status::Invalid("Column ", i, " named ", field(i)->name(),
                               " expected length ", num_rows_, " but got length ",
                               col->length());
      }
      Status st = col->Validate();
      if (!st.ok()) {
        std::stringstream ss;
        ss << "Column " << i << ": " << st.message();
        return st.WithMessage(ss.str());
      }
    }
    return Status::OK();
  }

 private:
  std::vector<std::shared_ptr<ChunkedArray>> columns_;
};

Table::Table() : num_rows_(0) {}

std::vector<std::shared_ptr<ChunkedArray>> Table::columns() const {
  std::vector<std::shared_ptr<ChunkedArray>> result;
  for (int i = 0; i < this->num_columns(); ++i) {
    result.emplace_back(this->column(i));
  }
  return result;
}

std::vector<std::shared_ptr<Field>> Table::fields() const {
  std::vector<std::shared_ptr<Field>> result;
  for (int i = 0; i < this->num_columns(); ++i) {
    result.emplace_back(this->field(i));
  }
  return result;
}

std::shared_ptr<Table> Table::Make(
    const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<ChunkedArray>>& columns, int64_t num_rows) {
  return std::make_shared<SimpleTable>(schema, columns, num_rows);
}

std::shared_ptr<Table> Table::Make(const std::shared_ptr<Schema>& schema,
                                   const std::vector<std::shared_ptr<Array>>& arrays,
                                   int64_t num_rows) {
  return std::make_shared<SimpleTable>(schema, arrays, num_rows);
}

Status Table::FromRecordBatches(const std::shared_ptr<Schema>& schema,
                                const std::vector<std::shared_ptr<RecordBatch>>& batches,
                                std::shared_ptr<Table>* table) {
  const int nbatches = static_cast<int>(batches.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 0; i < nbatches; ++i) {
    if (!batches[i]->schema()->Equals(*schema, false)) {
      return Status::Invalid("Schema at index ", static_cast<int>(i),
                             " was different: \n", schema->ToString(), "\nvs\n",
                             batches[i]->schema()->ToString());
    }
  }

  std::vector<std::shared_ptr<ChunkedArray>> columns(ncolumns);
  std::vector<std::shared_ptr<Array>> column_arrays(nbatches);

  for (int i = 0; i < ncolumns; ++i) {
    for (int j = 0; j < nbatches; ++j) {
      column_arrays[j] = batches[j]->column(i);
    }
    columns[i] = std::make_shared<ChunkedArray>(column_arrays, schema->field(i)->type());
  }

  *table = Table::Make(schema, columns);
  return Status::OK();
}

Status Table::FromRecordBatches(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                                std::shared_ptr<Table>* table) {
  if (batches.size() == 0) {
    return Status::Invalid("Must pass at least one record batch");
  }

  return FromRecordBatches(batches[0]->schema(), batches, table);
}

Status Table::FromChunkedStructArray(const std::shared_ptr<ChunkedArray>& array,
                                     std::shared_ptr<Table>* table) {
  auto type = array->type();
  if (type->id() != Type::STRUCT) {
    return Status::Invalid("Expected a chunked struct array, got ", *type);
  }
  int num_columns = type->num_children();
  int num_chunks = array->num_chunks();

  const auto& struct_chunks = array->chunks();
  std::vector<std::shared_ptr<ChunkedArray>> columns(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    ArrayVector chunks(num_chunks);
    std::transform(struct_chunks.begin(), struct_chunks.end(), chunks.begin(),
                   [i](const std::shared_ptr<Array>& struct_chunk) {
                     return static_cast<const StructArray&>(*struct_chunk).field(i);
                   });
    columns[i] = std::make_shared<ChunkedArray>(chunks);
  }

  *table = Table::Make(::arrow::schema(type->children()), columns, array->length());
  return Status::OK();
}

std::vector<std::string> Table::ColumnNames() const {
  std::vector<std::string> names(num_columns());
  for (int i = 0; i < num_columns(); ++i) {
    names[i] = field(i)->name();
  }
  return names;
}

Status Table::RenameColumns(const std::vector<std::string>& names,
                            std::shared_ptr<Table>* out) const {
  if (names.size() != static_cast<size_t>(num_columns())) {
    return Status::Invalid("tried to rename a table of ", num_columns(),
                           " columns but only ", names.size(), " names were provided");
  }
  std::vector<std::shared_ptr<ChunkedArray>> columns(num_columns());
  std::vector<std::shared_ptr<Field>> fields(num_columns());
  for (int i = 0; i < num_columns(); ++i) {
    columns[i] = column(i);
    fields[i] = field(i)->WithName(names[i]);
  }
  *out = Table::Make(::arrow::schema(std::move(fields)), std::move(columns), num_rows());
  return Status::OK();
}

Result<std::shared_ptr<Table>> ConcatenateTables(
    const std::vector<std::shared_ptr<Table>>& tables,
    const ConcatenateTablesOptions options, MemoryPool* memory_pool) {
  if (tables.size() == 0) {
    return Status::Invalid("Must pass at least one table");
  }

  std::vector<std::shared_ptr<Table>> promoted_tables;
  const std::vector<std::shared_ptr<Table>>* tables_to_concat = &tables;
  if (options.unify_schemas) {
    std::vector<std::shared_ptr<Schema>> schemas;
    schemas.reserve(tables.size());
    for (const auto& t : tables) {
      schemas.push_back(t->schema());
    }

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Schema> unified_schema,
                          UnifySchemas(schemas, options.field_merge_options));

    promoted_tables.reserve(tables.size());
    for (const auto& t : tables) {
      promoted_tables.emplace_back();
      ARROW_ASSIGN_OR_RAISE(promoted_tables.back(),
                            PromoteTableToSchema(t, unified_schema, memory_pool));
    }
    tables_to_concat = &promoted_tables;
  } else {
    auto first_schema = tables[0]->schema();
    for (size_t i = 1; i < tables.size(); ++i) {
      if (!tables[i]->schema()->Equals(*first_schema, false)) {
        return Status::Invalid("Schema at index ", i, " was different: \n",
                               first_schema->ToString(), "\nvs\n",
                               tables[i]->schema()->ToString());
      }
    }
  }

  std::shared_ptr<Schema> schema = tables_to_concat->front()->schema();

  const int ncolumns = schema->num_fields();

  std::vector<std::shared_ptr<ChunkedArray>> columns(ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    std::vector<std::shared_ptr<Array>> column_arrays;
    for (const auto& table : *tables_to_concat) {
      const std::vector<std::shared_ptr<Array>>& chunks = table->column(i)->chunks();
      for (const auto& chunk : chunks) {
        column_arrays.push_back(chunk);
      }
    }
    columns[i] = std::make_shared<ChunkedArray>(column_arrays, schema->field(i)->type());
  }
  return Table::Make(schema, columns);
}

Result<std::shared_ptr<Table>> PromoteTableToSchema(const std::shared_ptr<Table>& table,
                                                    const std::shared_ptr<Schema>& schema,
                                                    MemoryPool* pool) {
  const std::shared_ptr<Schema> current_schema = table->schema();
  if (current_schema->Equals(*schema, /*check_metadata=*/false)) {
    return table->ReplaceSchemaMetadata(schema->metadata());
  }

  // fields_seen[i] == true iff that field is also in `schema`.
  std::vector<bool> fields_seen(current_schema->num_fields(), false);

  std::vector<std::shared_ptr<ChunkedArray>> columns;
  columns.reserve(schema->num_fields());
  const int64_t num_rows = table->num_rows();
  auto AppendColumnOfNulls = [pool, &columns,
                              num_rows](const std::shared_ptr<DataType>& type) {
    std::shared_ptr<Array> array_of_nulls;
    // TODO(bkietz): share the zero-filled buffers as much as possible across
    // the null-filled arrays created here.
    RETURN_NOT_OK(MakeArrayOfNull(pool, type, num_rows, &array_of_nulls));
    columns.push_back(std::make_shared<ChunkedArray>(array_of_nulls));
    return Status::OK();
  };

  for (const auto& field : schema->fields()) {
    const std::vector<int> field_indices =
        current_schema->GetAllFieldIndices(field->name());
    if (field_indices.empty()) {
      RETURN_NOT_OK(AppendColumnOfNulls(field->type()));
      continue;
    }

    if (field_indices.size() > 1) {
      return Status::Invalid(
          "PromoteTableToSchema cannot handle schemas with duplicate fields: ",
          field->name());
    }

    const int field_index = field_indices[0];
    const auto& current_field = current_schema->field(field_index);
    if (!field->nullable() && current_field->nullable()) {
      return Status::Invalid("Unable to promote field ", current_field->name(),
                             ": it was nullable but the target schema was not.");
    }

    fields_seen[field_index] = true;
    if (current_field->type()->Equals(field->type())) {
      columns.push_back(table->column(field_index));
      continue;
    }

    if (current_field->type()->id() == Type::NA) {
      RETURN_NOT_OK(AppendColumnOfNulls(field->type()));
      continue;
    }

    return Status::Invalid("Unable to promote field ", field->name(),
                           ": incompatible types: ", field->type()->ToString(), " vs ",
                           current_field->type()->ToString());
  }

  auto unseen_field_iter = std::find(fields_seen.begin(), fields_seen.end(), false);
  if (unseen_field_iter != fields_seen.end()) {
    const size_t unseen_field_index = unseen_field_iter - fields_seen.begin();
    return Status::Invalid(
        "Incompatible schemas: field ",
        current_schema->field(static_cast<int>(unseen_field_index))->name(),
        " did not exist in the new schema.");
  }

  return Table::Make(schema, std::move(columns));
}

bool Table::Equals(const Table& other) const {
  if (this == &other) {
    return true;
  }
  if (!schema_->Equals(*other.schema())) {
    return false;
  }
  if (this->num_columns() != other.num_columns()) {
    return false;
  }

  for (int i = 0; i < this->num_columns(); i++) {
    if (!this->column(i)->Equals(other.column(i))) {
      return false;
    }
  }
  return true;
}

Status Table::CombineChunks(MemoryPool* pool, std::shared_ptr<Table>* out) const {
  const int ncolumns = num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> compacted_columns(ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    auto col = column(i);
    if (col->num_chunks() <= 1) {
      compacted_columns[i] = col;
    } else {
      std::shared_ptr<Array> compacted;
      RETURN_NOT_OK(Concatenate(col->chunks(), pool, &compacted));
      compacted_columns[i] = std::make_shared<ChunkedArray>(compacted);
    }
  }
  *out = Table::Make(schema(), compacted_columns);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Convert a table to a sequence of record batches

TableBatchReader::TableBatchReader(const Table& table)
    : table_(table),
      column_data_(table.num_columns()),
      chunk_numbers_(table.num_columns(), 0),
      chunk_offsets_(table.num_columns(), 0),
      absolute_row_position_(0),
      max_chunksize_(std::numeric_limits<int64_t>::max()) {
  for (int i = 0; i < table.num_columns(); ++i) {
    column_data_[i] = table.column(i).get();
  }
}

std::shared_ptr<Schema> TableBatchReader::schema() const { return table_.schema(); }

void TableBatchReader::set_chunksize(int64_t chunksize) { max_chunksize_ = chunksize; }

Status TableBatchReader::ReadNext(std::shared_ptr<RecordBatch>* out) {
  if (absolute_row_position_ == table_.num_rows()) {
    *out = nullptr;
    return Status::OK();
  }

  // Determine the minimum contiguous slice across all columns
  int64_t chunksize = std::min(table_.num_rows(), max_chunksize_);
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
  std::vector<std::shared_ptr<ArrayData>> batch_data(table_.num_columns());

  for (int i = 0; i < table_.num_columns(); ++i) {
    // Exhausted chunk
    const Array* chunk = chunks[i];
    const int64_t offset = chunk_offsets_[i];
    std::shared_ptr<ArrayData> slice_data;
    if ((chunk->length() - offset) == chunksize) {
      ++chunk_numbers_[i];
      chunk_offsets_[i] = 0;
      if (offset > 0) {
        // Need to slice
        slice_data = chunk->Slice(offset, chunksize)->data();
      } else {
        // No slice
        slice_data = chunk->data();
      }
    } else {
      chunk_offsets_[i] += chunksize;
      slice_data = chunk->Slice(offset, chunksize)->data();
    }
    batch_data[i] = std::move(slice_data);
  }

  absolute_row_position_ += chunksize;
  *out = RecordBatch::Make(table_.schema(), chunksize, std::move(batch_data));

  return Status::OK();
}

}  // namespace arrow
