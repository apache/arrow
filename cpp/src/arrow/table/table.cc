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

#include "arrow/table/table.h"

#include <memory>
#include <sstream>

#include "arrow/table/column.h"
#include "arrow/table/schema.h"
#include "arrow/type.h"
#include "arrow/util/status.h"

namespace arrow {

Table::Table(const std::string& name, const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Column> >& columns) :
    name_(name),
    schema_(schema),
    columns_(columns) {
  if (columns.size() == 0) {
    num_rows_ = 0;
  } else {
    num_rows_ = columns[0]->length();
  }
}

Table::Table(const std::string& name, const std::shared_ptr<Schema>& schema,
    const std::vector<std::shared_ptr<Column> >& columns, int64_t num_rows) :
    name_(name),
    schema_(schema),
    columns_(columns),
    num_rows_(num_rows) {}

Status Table::ValidateColumns() const {
  if (num_columns() != schema_->num_fields()) {
    return Status::Invalid("Number of columns did not match schema");
  }

  if (columns_.size() == 0) {
    return Status::OK();
  }

  // Make sure columns are all the same length
  for (size_t i = 0; i < columns_.size(); ++i) {
    const Column* col = columns_[i].get();
    if (col->length() != num_rows_) {
      std::stringstream ss;
      ss << "Column " << i << " expected length "
         << num_rows_
         << " but got length "
         << col->length();
      return Status::Invalid(ss.str());
    }
  }
  return Status::OK();
}

} // namespace arrow
