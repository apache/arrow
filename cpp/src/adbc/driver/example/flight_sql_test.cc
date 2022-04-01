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
#include "adbc/client/driver.h"
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
    ARROW_LOG(WARNING) << "Server running on port " << server->port();

    ASSERT_OK_AND_ASSIGN(driver, adbc::AdbcDriver::Load("libadbc_driver_flight_sql.so"));

    AdbcConnectionOptions options;
    std::string target = "Location=grpc://localhost:" + std::to_string(server->port());
    options.target = target.c_str();
    options.target_length = target.size();
    ASSERT_OK_AND_ASSIGN(connection, driver->ConnectRaw(options));
  }

  void TearDown() override {
    ADBC_ASSERT_OK(connection.release(&connection, &error));
    ASSERT_OK(server->Shutdown());
  }

 protected:
  std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server;
  std::unique_ptr<adbc::AdbcDriver> driver;
  AdbcConnection connection;
  AdbcError error = {};
};

TEST_F(AdbcFlightSqlTest, Metadata) {
  {
    AdbcStatement statement;
    ADBC_ASSERT_OK(connection.get_table_types(&connection, &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&statement, &schema, &batches);
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
  {
    std::string query = "SELECT 1";
    AdbcStatement statement;
    ADBC_ASSERT_OK(connection.sql_execute(&connection, query.c_str(), query.size(),
                                          &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&statement, &schema, &batches);
    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("1", arrow::int64())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), {
                                         arrow::RecordBatchFromJSON(schema, "[[1]]"),
                                     }));
  }

  // Serialize the query result handle into a partition so it can be
  // retrieved separately. (With multiple partitions we could
  // distribute them across multiple machines or fetch data in
  // parallel.)
  {
    std::string query = "SELECT 42";
    AdbcStatement statement;
    ADBC_ASSERT_OK(connection.sql_execute(&connection, query.c_str(), query.size(),
                                          &statement, &error));

    size_t num_partitions = 0;
    size_t desc_size = 0;
    ADBC_ASSERT_OK(statement.num_partitions(&statement, &num_partitions, &error));
    ASSERT_EQ(num_partitions, 1);
    ADBC_ASSERT_OK(statement.get_partition_desc_size(&statement, 0, &desc_size, &error));
    ASSERT_GT(desc_size, 0);
    std::vector<uint8_t> desc(desc_size, 0);
    ADBC_ASSERT_OK(statement.get_partition_desc(&statement, 0, desc.data(), &error));

    ADBC_ASSERT_OK(statement.release(&statement, &error));

    // Reconstruct the partition
    ADBC_ASSERT_OK(connection.deserialize_partition_desc(
        &connection, desc.data(), desc.size(), &statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&statement, &schema, &batches);
    arrow::AssertSchemaEqual(*schema,
                             *arrow::schema({arrow::field("42", arrow::int64())}));
    EXPECT_THAT(batches,
                ::testing::UnorderedPointwise(
                    PointeesEqual(), {
                                         arrow::RecordBatchFromJSON(schema, "[[42]]"),
                                     }));
  }

  {
    std::string query = "INVALID";
    AdbcStatement statement;
    ASSERT_NE(connection.sql_execute(&connection, query.c_str(), query.size(), &statement,
                                     &error),
              ADBC_STATUS_OK);

    ARROW_LOG(WARNING) << "Got error message: " << error.message;
    EXPECT_THAT(error.message, ::testing::HasSubstr("syntax error"));
    driver->ReleaseError(&error);
  }
}

}  // namespace adbc
