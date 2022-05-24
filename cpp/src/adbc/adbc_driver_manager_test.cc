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

// Test the driver, but using the driver manager's stubs instead of
// the function pointer table.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "adbc/adbc.h"
#include "adbc/adbc_driver_manager.h"
#include "adbc/test_sqlite_internal.h"
#include "adbc/test_util.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"

namespace adbc {

using arrow::PointeesEqual;

TEST(AdbcDriverManager, Basics) {
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};

  AdbcDriver driver;
  ASSERT_NO_FATAL_FAILURE(InitSqlite(&driver));

  {
    AdbcDatabaseOptions options;
    std::memset(&options, 0, sizeof(options));
    options.driver = &driver;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              AdbcDatabaseInit(&options, &database, &error));
    ASSERT_NE(database.private_data, nullptr);
  }

  {
    AdbcConnectionOptions options;
    std::memset(&options, 0, sizeof(options));
    options.database = &database;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              AdbcConnectionInit(&options, &connection, &error));
    ASSERT_NE(connection.private_data, nullptr);
  }

  {
    std::string query = "SELECT 1";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              AdbcStatementInit(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        AdbcConnectionSqlExecute(&connection, query.c_str(), &statement, &error));

    arrow::RecordBatchVector batches;
    ArrowArrayStream stream;

    ADBC_ASSERT_OK(AdbcStatementGetStream(&statement, &stream, &error));
    ASSERT_OK_AND_ASSIGN(auto reader, arrow::ImportRecordBatchReader(&stream));

    auto schema = reader->schema();
    while (true) {
      ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
      if (!batch) break;
      batches.push_back(std::move(batch));
    }
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error, AdbcStatementRelease(&statement, &error));

    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("1", arrow::int64())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), {
                                         arrow::RecordBatchFromJSON(schema, "[[1]]"),
                                     }));
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, AdbcConnectionRelease(&connection, &error));
  ASSERT_EQ(connection.private_data, nullptr);

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, AdbcDatabaseRelease(&database, &error));
  ASSERT_EQ(database.private_data, nullptr);
}

}  // namespace adbc
