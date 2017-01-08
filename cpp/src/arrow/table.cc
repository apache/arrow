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

#include <cstdlib>
#include <memory>
#include <sstream>

#include "arrow/array.h"
#include "arrow/column.h"
#include "arrow/schema.h"
#include "arrow/status.h"

namespace arrow {

RecordBatch::RecordBatch(const std::shared_ptr<Schema>& schema, int num_rows,
    const std::vector<std::shared_ptr<Array>>& columns)
    : schema_(schema), num_rows_(num_rows), columns_(columns) {}

const std::string& RecordBatch::column_name(int i) const {
  return schema_->field(i)->name;
}

bool RecordBatch::Equals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->Equals(other.column(i))) { return false; }
  }

  return true;
}

bool RecordBatch::ApproxEquals(const RecordBatch& other) const {
  if (num_columns() != other.num_columns() || num_rows_ != other.num_rows()) {
    return false;
  }

  for (int i = 0; i < num_columns(); ++i) {
    if (!column(i)->ApproxEquals(other.column(i))) { return false; }
  }

  return true;
}

// ----------------------------------------------------------------------
// Table methods

Table::Table(const std::string& name, const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Column>>& columns)
    : name_(name), schema_(schema), columns_(columns) {
  if (columns.size() == 0) {
    num_rows_ = 0;
  } else {
    num_rows_ = columns[0]->length();
  }
}

Table::Table(const std::string& name, const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Column>>& columns, int64_t num_rows)
    : name_(name), schema_(schema), columns_(columns), num_rows_(num_rows) {}

Status Table::FromRecordBatches(const std::string& name,
    const std::vector<std::shared_ptr<RecordBatch>>& batches,
    std::shared_ptr<Table>* table) {
  if (batches.size() == 0) {
    return Status::Invalid("Must pass at least one record batch");
  }

  std::shared_ptr<Schema> schema = batches[0]->schema();

  const int nbatches = static_cast<int>(batches.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 1; i < nbatches; ++i) {
    if (!batches[i]->schema()->Equals(schema)) {
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

  *table = std::make_shared<Table>(name, schema, columns);
  return Status::OK();
}

Status ConcatenateTables(const std::string& output_name,
    const std::vector<std::shared_ptr<Table>>& tables, std::shared_ptr<Table>* table) {
  if (tables.size() == 0) { return Status::Invalid("Must pass at least one table"); }

  std::shared_ptr<Schema> schema = tables[0]->schema();

  const int ntables = static_cast<int>(tables.size());
  const int ncolumns = static_cast<int>(schema->num_fields());

  for (int i = 1; i < ntables; ++i) {
    if (!tables[i]->schema()->Equals(schema)) {
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
  *table = std::make_shared<Table>(output_name, schema, columns);
  return Status::OK();
}

bool Table::Equals(const Table& other) const {
  if (name_ != other.name()) { return false; }
  if (!schema_->Equals(other.schema())) { return false; }
  if (static_cast<int64_t>(columns_.size()) != other.num_columns()) { return false; }

  for (size_t i = 0; i < columns_.size(); i++) {
    if (!columns_[i]->Equals(other.column(i))) { return false; }
  }
  return true;
}

bool Table::Equals(const std::shared_ptr<Table>& other) const {
  if (this == other.get()) { return true; }
  if (!other) { return false; }
  return Equals(*other.get());
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

}  // namespace arrow
