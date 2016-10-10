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
#include "arrow/util/status.h"

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
