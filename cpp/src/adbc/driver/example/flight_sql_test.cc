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
#include "adbc/test_util.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/logging.h"

namespace adbc {

using arrow::PointeesEqual;

class AdbcFlightSqlTest : public ::testing::Test {
 public:
  void SetUp() override {
    {
      arrow::flight::Location location;
      ASSERT_OK(arrow::flight::Location::ForGrpcTcp("localhost", 0, &location));
      arrow::flight::FlightServerOptions options(location);
      ASSERT_OK_AND_ASSIGN(server,
                           arrow::flight::sql::example::SQLiteFlightSqlServer::Create());
      ASSERT_OK(server->Init(options));
      ASSERT_GT(server->port(), 0);
    }

    {
      size_t initialized = 0;
      ADBC_ASSERT_OK(
          AdbcLoadDriver("Driver=libadbc_driver_flight_sql.so;"
                         "Entrypoint=AdbcFlightSqlDriverInit",
                         ADBC_VERSION_0_0_1, &driver, &initialized));
    }

    {
      AdbcDatabaseOptions options;
      std::string target = "Location=grpc://localhost:" + std::to_string(server->port());
      options.target = target.c_str();
      ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                                driver.DatabaseInit(&options, &database, &error));
    }

    {
      AdbcConnectionOptions options;
      options.database = &database;
      ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                                driver.ConnectionInit(&options, &connection, &error));
    }
  }

  void TearDown() override {
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.ConnectionRelease(&connection, &error));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.DatabaseRelease(&database, &error));
    ASSERT_OK(server->Shutdown());
  }

 protected:
  std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server;
  AdbcDriver driver;
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};
};

TEST_F(AdbcFlightSqlTest, Metadata) {
  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                              driver.StatementInit(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error, driver.ConnectionGetTableTypes(&connection, &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&driver, &statement, &schema, &batches);
    arrow::AssertSchemaEqual(
        *schema,
        *arrow::schema({arrow::field("table_type", arrow::utf8(), /*nullable=*/false)}));
    EXPECT_THAT(batches, ::testing::UnorderedPointwise(
                             PointeesEqual(),
                             {
                                 arrow::RecordBatchFromJSON(schema, R"([["table"]])"),
                             }));
  }
}

TEST_F(AdbcFlightSqlTest, SqlExecute) {
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
  arrow::AssertSchemaEqual(*schema, *arrow::schema({arrow::field("1", arrow::int64())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       arrow::RecordBatchFromJSON(schema, "[[1]]"),
                                   }));
}

TEST_F(AdbcFlightSqlTest, Partitions) {
  // Serialize the query result handle into a partition so it can be
  // retrieved separately. (With multiple partitions we could
  // distribute them across multiple machines or fetch data in
  // parallel.)
  std::string query = "SELECT 42";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.StatementInit(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      &driver, error,
      driver.ConnectionSqlExecute(&connection, query.c_str(), &statement, &error));

  std::vector<std::vector<uint8_t>> descs;

  while (true) {
    size_t length = 0;
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.StatementGetPartitionDescSize(&statement, &length, &error));
    if (length == 0) break;
    descs.emplace_back(length);
    ADBC_ASSERT_OK_WITH_ERROR(
        &driver, error,
        driver.StatementGetPartitionDesc(&statement, descs.back().data(), &error));
  }

  ASSERT_EQ(descs.size(), 1);
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.StatementRelease(&statement, &error));

  // Reconstruct the partition
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.StatementInit(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      &driver, error,
      driver.ConnectionDeserializePartitionDesc(&connection, descs.back().data(),
                                                descs.back().size(), &statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&driver, &statement, &schema, &batches);
  arrow::AssertSchemaEqual(*schema, *arrow::schema({arrow::field("42", arrow::int64())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       arrow::RecordBatchFromJSON(schema, "[[42]]"),
                                   }));
}

TEST_F(AdbcFlightSqlTest, InvalidSql) {
  std::string query = "INVALID";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error,
                            driver.StatementInit(&connection, &statement, &error));
  ASSERT_NE(driver.ConnectionSqlExecute(&connection, query.c_str(), &statement, &error),
            ADBC_STATUS_OK);
  ADBC_ASSERT_ERROR_THAT(&driver, error, ::testing::HasSubstr("syntax error"));
  ADBC_ASSERT_OK_WITH_ERROR(&driver, error, driver.StatementRelease(&statement, &error));
}

}  // namespace adbc
