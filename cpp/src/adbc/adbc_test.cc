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

TEST(Adbc, Basics) {
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};

  AdbcDriver driver;
  ASSERT_NO_FATAL_FAILURE(InitSqlite(&driver));

  {
    AdbcDatabaseOptions options;
    std::memset(&options, 0, sizeof(options));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.DatabaseInit(&options, &database, &error));
    ASSERT_NE(database.private_data, nullptr);
  }

  {
    AdbcConnectionOptions options;
    std::memset(&options, 0, sizeof(options));
    options.database = &database;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionInit(&options, &connection, &error));
    ASSERT_NE(connection.private_data, nullptr);
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.ConnectionRelease(&connection, &error));
  ASSERT_EQ(connection.private_data, nullptr);

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.DatabaseRelease(&database, &error));
  ASSERT_EQ(database.private_data, nullptr);
}

TEST(Adbc, Errors) {
  AdbcDriver driver;
  size_t initialized = 0;
  ASSERT_NE(ADBC_STATUS_OK, AdbcLoadDriver("Driver=libadbc_driver_fake.so",
                                           ADBC_VERSION_0_0_1, &driver, &initialized));
}

TEST(AdbcSqlite, SqlExecute) {
  // Execute a query with the SQLite example driver.
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};

  AdbcDriver driver;
  ASSERT_NO_FATAL_FAILURE(InitSqlite(&driver));

  {
    AdbcDatabaseOptions options;
    std::memset(&options, 0, sizeof(options));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.DatabaseInit(&options, &database, &error));
  }
  {
    AdbcConnectionOptions options;
    std::memset(&options, 0, sizeof(options));
    options.database = &database;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionInit(&options, &connection, &error));
  }

  {
    std::string query = "SELECT 1";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.ConnectionSqlExecute(&connection, query.c_str(), &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&driver, &statement, &schema, &batches);
    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("1", arrow::int64())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), {
                                         arrow::RecordBatchFromJSON(schema, "[[1]]"),
                                     }));
  }

  {
    std::string query = "INVALID";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection, &statement, &error));
    ASSERT_NE(driver.ConnectionSqlExecute(&connection, query.c_str(), &statement, &error),
              ADBC_STATUS_OK);
    ADBC_ASSERT_ERROR_THAT(
        &driver, error,
        ::testing::AllOf(::testing::HasSubstr("[SQLite3] sqlite3_prepare_v2:"),
                         ::testing::HasSubstr("syntax error")));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementRelease(&statement, &error));
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.ConnectionRelease(&connection, &error));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.DatabaseRelease(&database, &error));
}

TEST(AdbcSqlite, SqlPrepare) {
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};

  AdbcDriver driver;
  ASSERT_NO_FATAL_FAILURE(InitSqlite(&driver));

  {
    AdbcDatabaseOptions options;
    std::memset(&options, 0, sizeof(options));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.DatabaseInit(&options, &database, &error));
  }
  {
    AdbcConnectionOptions options;
    std::memset(&options, 0, sizeof(options));
    options.database = &database;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionInit(&options, &connection, &error));
  }

  {
    std::string query = "SELECT 1";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.ConnectionSqlPrepare(&connection, query.c_str(), &statement, &error));

    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&driver, &statement, &schema, &batches));
    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("1", arrow::int64())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), {
                                         arrow::RecordBatchFromJSON(schema, "[[1]]"),
                                     }));
  }

  {
    auto param_schema = arrow::schema(
        {arrow::field("1", arrow::int64()), arrow::field("2", arrow::utf8())});
    std::string query = "SELECT ?, ?";
    AdbcStatement statement;
    ArrowArray export_params;
    ArrowSchema export_schema;
    std::memset(&statement, 0, sizeof(statement));

    ASSERT_OK(ExportRecordBatch(
        *arrow::RecordBatchFromJSON(param_schema, R"([[1, "foo"], [2, "bar"]])"),
        &export_params));
    ASSERT_OK(ExportSchema(*param_schema, &export_schema));

    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.ConnectionSqlPrepare(&connection, query.c_str(), &statement, &error));

    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.StatementBind(&statement, &export_params, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&driver, &statement, &schema, &batches));
    arrow::AssertSchemaEqual(*schema, *arrow::schema({arrow::field("?", arrow::int64()),
                                                      arrow::field("?", arrow::utf8())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(),
                    {
                        arrow::RecordBatchFromJSON(schema, R"([[1, "foo"], [2, "bar"]])"),
                    }));
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.ConnectionRelease(&connection, &error));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.DatabaseRelease(&database, &error));
}

TEST(AdbcSqlite, MultipleConnections) {
  // Execute a query with the SQLite example driver.
  AdbcDatabase database;
  AdbcConnection connection1, connection2;
  AdbcError error = {};

  AdbcDriver driver;
  ASSERT_NO_FATAL_FAILURE(InitSqlite(&driver));

  {
    AdbcDatabaseOptions options;
    std::memset(&options, 0, sizeof(options));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.DatabaseInit(&options, &database, &error));
  }
  {
    AdbcConnectionOptions options;
    std::memset(&options, 0, sizeof(options));
    options.database = &database;
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionInit(&options, &connection1, &error));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionInit(&options, &connection2, &error));
  }

  {
    std::string query = "CREATE TABLE foo (bar INTEGER)";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection1, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.ConnectionSqlExecute(&connection1, query.c_str(), &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&driver, &statement, &schema, &batches));
    arrow::AssertSchemaEqual(*schema, *arrow::schema({}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), std::vector<std::shared_ptr<arrow::RecordBatch>>{}));
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.ConnectionRelease(&connection1, &error));

  {
    std::string query = "SELECT * FROM foo";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection2, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.ConnectionSqlExecute(&connection2, query.c_str(), &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&driver, &statement, &schema, &batches);
    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("bar", arrow::null())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), std::vector<std::shared_ptr<arrow::RecordBatch>>{}));
  }

  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.ConnectionRelease(&connection2, &error));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.DatabaseRelease(&database, &error));
}

}  // namespace adbc
