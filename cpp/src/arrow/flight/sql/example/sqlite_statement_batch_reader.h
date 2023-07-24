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

#include <sqlite3.h>

#include <memory>

#include "arrow/flight/sql/example/sqlite_statement.h"
#include "arrow/record_batch.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

class SqliteStatementBatchReader : public RecordBatchReader {
 public:
  /// \brief Creates a RecordBatchReader backed by a SQLite statement.
  /// \param[in] statement    SQLite statement to be read.
  /// \return                 A SqliteStatementBatchReader.
  static arrow::Result<std::shared_ptr<SqliteStatementBatchReader>> Create(
      const std::shared_ptr<SqliteStatement>& statement);

  /// \brief Creates a RecordBatchReader backed by a SQLite statement.
  /// \param[in] statement    SQLite statement to be read.
  /// \param[in] schema       Schema to be used on results.
  /// \return                 A SqliteStatementBatchReader..
  static arrow::Result<std::shared_ptr<SqliteStatementBatchReader>> Create(
      const std::shared_ptr<SqliteStatement>& statement,
      const std::shared_ptr<Schema>& schema);

  std::shared_ptr<Schema> schema() const override;

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override;

 private:
  std::shared_ptr<SqliteStatement> statement_;
  std::shared_ptr<Schema> schema_;
  int rc_;
  bool already_executed_;

  // State for parameter binding
  size_t batch_index_{0};
  int64_t row_index_{0};

  SqliteStatementBatchReader(std::shared_ptr<SqliteStatement> statement,
                             std::shared_ptr<Schema> schema);
};

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
