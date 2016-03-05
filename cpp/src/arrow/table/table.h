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

#ifndef ARROW_TABLE_TABLE_H
#define ARROW_TABLE_TABLE_H

#include <memory>
#include <string>
#include <vector>

namespace arrow {

class Column;
class Schema;
class Status;

// Immutable container of fixed-length columns conforming to a particular schema
class Table {
 public:
  // If columns is zero-length, the table's number of rows is zero
  Table(const std::string& name, const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<Column> >& columns);

  Table(const std::string& name, const std::shared_ptr<Schema>& schema,
      const std::vector<std::shared_ptr<Column> >& columns, int64_t num_rows);

  // @returns: the table's name, if any (may be length 0)
  const std::string& name() const {
    return name_;
  }

  // @returns: the table's schema
  const std::shared_ptr<Schema>& schema() const {
    return schema_;
  }

  // Note: Does not boundscheck
  // @returns: the i-th column
  const std::shared_ptr<Column>& column(int i) const {
    return columns_[i];
  }

  // @returns: the number of columns in the table
  int num_columns() const {
    return columns_.size();
  }

  // @returns: the number of rows (the corresponding length of each column)
  int64_t num_rows() const {
    return num_rows_;
  }

  // After construction, perform any checks to validate the input arguments
  Status ValidateColumns() const;

 private:
  // The table's name, optional
  std::string name_;

  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<Column> > columns_;

  int64_t num_rows_;
};

} // namespace arrow

#endif  // ARROW_TABLE_TABLE_H
