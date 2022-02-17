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

#include "adbc/c/types.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

#define ADBC_ASSERT_OK(expr)          \
  do {                                \
    auto code_ = (expr);              \
    ASSERT_EQ(code_, ADBC_STATUS_OK); \
  } while (false)

TEST(Adbc, Basics) {
  AdbcConnection connection;

  AdbcConnectionOptions options;
  std::string target = "Library=libadbc_driver_sqlite.so";
  options.target = target.c_str();
  ADBC_ASSERT_OK(AdbcConnectionInit(&options, &connection));
  ASSERT_NE(connection.private_data, nullptr);

  ASSERT_NE(connection.close, nullptr);
  ADBC_ASSERT_OK(connection.close(&connection));
  ASSERT_NE(connection.private_data, nullptr);

  ASSERT_NE(connection.release, nullptr);
  connection.release(&connection);
  ASSERT_EQ(connection.private_data, nullptr);
}

TEST(Adbc, Errors) {
  AdbcConnection connection;

  AdbcConnectionOptions options;
  std::string target = "Library=libadbc_driver_fake.so";
  options.target = target.c_str();
  auto status = AdbcConnectionInit(&options, &connection);
  ASSERT_EQ(status, ADBC_STATUS_UNKNOWN);
  ASSERT_NE(connection.private_data, nullptr);

  ASSERT_NE(connection.get_error, nullptr);
  int count = 0;
  while (true) {
    char* message = connection.get_error(&connection);
    if (!message) break;
    count++;
    ARROW_LOG(WARNING) << "Got error message: " << message;
    EXPECT_THAT(message,
                ::testing::HasSubstr(
                    "[ADBC] AdbcConnectionInit: could not load libadbc_driver_fake.so"));
    // TODO: prefer to make user delete message, or handle it in
    // release (which means we have unbounded memory?)
    delete[] message;
  }
  ASSERT_EQ(1, count);

  ASSERT_NE(connection.release, nullptr);
  connection.release(&connection);
  ASSERT_EQ(connection.private_data, nullptr);
}

TEST(AdbcSqlite, SqlExecute) {
  AdbcConnection connection;

  AdbcConnectionOptions options;
  std::string target = "Library=libadbc_driver_sqlite.so";
  options.target = target.c_str();
  ADBC_ASSERT_OK(AdbcConnectionInit(&options, &connection));

  {
    auto query = "SELECT 1";
    AdbcStatement statement;
    ADBC_ASSERT_OK(connection.sql_execute(&connection, query, &statement));

    ArrowArrayStream stream;
    ADBC_ASSERT_OK(statement.get_results(&statement, &stream));
    ASSERT_OK_AND_ASSIGN(auto reader, arrow::ImportRecordBatchReader(&stream));

    auto schema = arrow::schema({arrow::field("1", arrow::int64())});
    arrow::AssertSchemaEqual(*reader->schema(), *schema);

    ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
    arrow::AssertBatchesEqual(*arrow::RecordBatchFromJSON(schema, "[[1]]"), *batch);

    ASSERT_OK_AND_ASSIGN(batch, reader->Next());
    ASSERT_EQ(batch, nullptr);

    ADBC_ASSERT_OK(statement.close(&statement));
    statement.release(&statement);
  }

  {
    auto query = "INVALID";
    AdbcStatement statement;
    ASSERT_NE(connection.sql_execute(&connection, query, &statement), ADBC_STATUS_OK);

    char* message = connection.get_error(&connection);
    ASSERT_NE(message, nullptr);
    ARROW_LOG(WARNING) << "Got error message: " << message;
    EXPECT_THAT(message, ::testing::HasSubstr("[SQLite3] sqlite3_prepare_v2:"));
    EXPECT_THAT(message, ::testing::HasSubstr("syntax error"));
    delete[] message;
  }

  ADBC_ASSERT_OK(connection.close(&connection));
  connection.release(&connection);
}

// TODO: these should be split into separate compilation units

TEST(AdbcFlightSql, SqlExecute) {
  AdbcConnection connection;

  AdbcConnectionOptions options;
  std::string target =
      "Library=libadbc_driver_flight_sql.so;Location=grpc://localhost:31337";
  options.target = target.c_str();
  ADBC_ASSERT_OK(AdbcConnectionInit(&options, &connection));

  {
    auto query = "SELECT 1";
    AdbcStatement statement;
    ADBC_ASSERT_OK(connection.sql_execute(&connection, query, &statement));

    ArrowArrayStream stream;
    ADBC_ASSERT_OK(statement.get_results(&statement, &stream));
    ASSERT_OK_AND_ASSIGN(auto reader, arrow::ImportRecordBatchReader(&stream));

    auto schema = arrow::schema({arrow::field("1", arrow::int64())});
    arrow::AssertSchemaEqual(*reader->schema(), *schema);

    ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
    arrow::AssertBatchesEqual(*arrow::RecordBatchFromJSON(schema, "[[1]]"), *batch);

    ASSERT_OK_AND_ASSIGN(batch, reader->Next());
    ASSERT_EQ(batch, nullptr);

    ADBC_ASSERT_OK(statement.close(&statement));
    statement.release(&statement);
  }

  {
    auto query = "INVALID";
    AdbcStatement statement;
    ASSERT_NE(connection.sql_execute(&connection, query, &statement), ADBC_STATUS_OK);

    char* message = connection.get_error(&connection);
    ASSERT_NE(message, nullptr);
    ARROW_LOG(WARNING) << "Got error message: " << message;
    EXPECT_THAT(message, ::testing::HasSubstr("[Flight SQL] GetFlightInfo:"));
    EXPECT_THAT(message, ::testing::HasSubstr("syntax error"));
    delete[] message;
  }

  ADBC_ASSERT_OK(connection.close(&connection));
  connection.release(&connection);
}
