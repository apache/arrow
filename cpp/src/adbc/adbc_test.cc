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
#include "adbc/c/types.h"
#include "adbc/test_util.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/logging.h"

namespace adbc {

using arrow::PointeesEqual;

TEST(Adbc, Basics) {
  AdbcConnection connection;
  AdbcError error = {};

  AdbcConnectionOptions options;
  ASSERT_OK_AND_ASSIGN(connection, adbc::ConnectRaw("libadbc_driver_sqlite.so", options));
  ASSERT_NE(connection.private_data, nullptr);

  ASSERT_NE(connection.release, nullptr);
  ADBC_ASSERT_OK(connection.release(&connection, &error));
  ASSERT_EQ(connection.private_data, nullptr);
}

TEST(Adbc, Errors) {
  AdbcConnectionOptions options;
  ASSERT_RAISES(Invalid, adbc::ConnectRaw("libadbc_driver_fake.so", options));
}

TEST(AdbcSqlite, SqlExecute) {
  // Execute a query with the SQLite example driver.
  AdbcConnection connection;
  AdbcError error = {};

  AdbcConnectionOptions options;
  ASSERT_OK_AND_ASSIGN(connection, adbc::ConnectRaw("libadbc_driver_sqlite.so", options));

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

  {
    std::string query = "INVALID";
    AdbcStatement statement;
    ASSERT_NE(connection.sql_execute(&connection, query.c_str(), query.size(), &statement,
                                     &error),
              ADBC_STATUS_OK);

    ASSERT_NE(error.message, nullptr);
    ARROW_LOG(WARNING) << "Got error message: " << error.message;
    EXPECT_THAT(error.message, ::testing::HasSubstr("[SQLite3] sqlite3_prepare_v2:"));
    EXPECT_THAT(error.message, ::testing::HasSubstr("syntax error"));
    error.release(&error);
  }

  ADBC_ASSERT_OK(connection.release(&connection, &error));
}

// TODO: these should be split into separate compilation units

// TODO: this could benefit from a fixture
// TODO: we should spin up the server in-process
TEST(AdbcFlightSql, Metadata) {
  AdbcConnection connection;
  AdbcError error = {};

  AdbcConnectionOptions options;
  std::string target = "Location=grpc://localhost:31337";
  options.target = target.c_str();
  options.target_length = target.size();
  ASSERT_OK_AND_ASSIGN(connection,
                       adbc::ConnectRaw("libadbc_driver_flight_sql.so", options));

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

  ADBC_ASSERT_OK(connection.release(&connection, &error));
}

TEST(AdbcFlightSql, SqlExecute) {
  // Execute a query with the Flight SQL example driver.
  AdbcConnection connection;
  AdbcError error = {};

  AdbcConnectionOptions options;
  std::string target = "Location=grpc://localhost:31337";
  options.target = target.c_str();
  options.target_length = target.size();
  ASSERT_OK_AND_ASSIGN(connection,
                       adbc::ConnectRaw("libadbc_driver_flight_sql.so", options));

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
    error.release(&error);
  }

  ADBC_ASSERT_OK(connection.release(&connection, &error));
}

}  // namespace adbc
