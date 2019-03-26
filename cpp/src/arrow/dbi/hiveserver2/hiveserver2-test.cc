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

#include "arrow/dbi/hiveserver2/operation.h"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/dbi/hiveserver2/service.h"
#include "arrow/dbi/hiveserver2/session.h"
#include "arrow/dbi/hiveserver2/thrift-internal.h"

#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace hiveserver2 {

static std::string GetTestHost() {
  const char* host = std::getenv("ARROW_HIVESERVER2_TEST_HOST");
  return host == nullptr ? "localhost" : std::string(host);
}

// Convenience functions for finding a row of values given several columns.
template <typename VType, typename CType>
bool FindRow(VType value, CType* column) {
  for (int i = 0; i < column->length(); ++i) {
    if (column->data()[i] == value) {
      return true;
    }
  }
  return false;
}

template <typename V1Type, typename V2Type, typename C1Type, typename C2Type>
bool FindRow(V1Type value1, V2Type value2, C1Type* column1, C2Type* column2) {
  EXPECT_EQ(column1->length(), column2->length());
  for (int i = 0; i < column1->length(); ++i) {
    if (column1->data()[i] == value1 && column2->data()[i] == value2) {
      return true;
    }
  }
  return false;
}

template <typename V1Type, typename V2Type, typename V3Type, typename C1Type,
          typename C2Type, typename C3Type>
bool FindRow(V1Type value1, V2Type value2, V3Type value3, C1Type* column1,
             C2Type* column2, C3Type column3) {
  EXPECT_EQ(column1->length(), column2->length());
  EXPECT_EQ(column1->length(), column3->length());
  for (int i = 0; i < column1->length(); ++i) {
    if (column1->data()[i] == value1 && column2->data()[i] == value2 &&
        column3->data()[i] == value3) {
      return true;
    }
  }
  return false;
}

// Waits for this operation to reach the given state, sleeping for sleep microseconds
// between checks, and failing after max_retries checks.
Status Wait(const std::unique_ptr<Operation>& op,
            Operation::State state = Operation::State::FINISHED, int sleep_us = 10000,
            int max_retries = 100) {
  int retries = 0;
  Operation::State op_state;
  RETURN_NOT_OK(op->GetState(&op_state));
  while (op_state != state && retries < max_retries) {
    usleep(sleep_us);
    RETURN_NOT_OK(op->GetState(&op_state));
    ++retries;
  }

  if (op_state == state) {
    return Status::OK();
  } else {
    return Status::IOError("Failed to reach state '", OperationStateToString(state),
                           "' after ", retries, " retries");
  }
}

// Creates a service, session, and database for use in tests.
class HS2ClientTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    hostname_ = GetTestHost();

    int conn_timeout = 0;
    ProtocolVersion protocol_version = ProtocolVersion::PROTOCOL_V7;
    ASSERT_OK(
        Service::Connect(hostname_, port, conn_timeout, protocol_version, &service_));

    std::string user = "user";
    HS2ClientConfig config;
    ASSERT_OK(service_->OpenSession(user, config, &session_));

    std::unique_ptr<Operation> drop_db_op;
    ASSERT_OK(session_->ExecuteStatement(
        "drop database if exists " + TEST_DB + " cascade", &drop_db_op));
    ASSERT_OK(drop_db_op->Close());

    std::unique_ptr<Operation> create_db_op;
    ASSERT_OK(session_->ExecuteStatement("create database " + TEST_DB, &create_db_op));
    ASSERT_OK(create_db_op->Close());

    std::unique_ptr<Operation> use_db_op;
    ASSERT_OK(session_->ExecuteStatement("use " + TEST_DB, &use_db_op));
    ASSERT_OK(use_db_op->Close());
  }

  virtual void TearDown() {
    std::unique_ptr<Operation> use_db_op;
    if (session_) {
      // We were able to create a session and service
      ASSERT_OK(session_->ExecuteStatement("use default", &use_db_op));
      ASSERT_OK(use_db_op->Close());

      std::unique_ptr<Operation> drop_db_op;
      ASSERT_OK(session_->ExecuteStatement("drop database " + TEST_DB + " cascade",
                                           &drop_db_op));
      ASSERT_OK(drop_db_op->Close());

      ASSERT_OK(session_->Close());
      ASSERT_OK(service_->Close());
    }
  }

  void CreateTestTable() {
    std::unique_ptr<Operation> create_table_op;
    ASSERT_OK(session_->ExecuteStatement(
        "create table " + TEST_TBL + " (" + TEST_COL1 + " int, " + TEST_COL2 + " string)",
        &create_table_op));
    ASSERT_OK(create_table_op->Close());
  }

  void InsertIntoTestTable(std::vector<int> int_col_data,
                           std::vector<std::string> string_col_data) {
    ASSERT_EQ(int_col_data.size(), string_col_data.size());

    std::stringstream query;
    query << "insert into " << TEST_TBL << " VALUES ";
    for (size_t i = 0; i < int_col_data.size(); i++) {
      if (int_col_data[i] == NULL_INT_VALUE) {
        query << " (NULL, ";
      } else {
        query << " (" << int_col_data[i] << ", ";
      }

      if (string_col_data[i] == "NULL") {
        query << "NULL)";
      } else {
        query << "'" << string_col_data[i] << "')";
      }

      if (i != int_col_data.size() - 1) {
        query << ", ";
      }
    }

    std::unique_ptr<Operation> insert_op;
    ASSERT_OK(session_->ExecuteStatement(query.str(), &insert_op));
    ASSERT_OK(Wait(insert_op));
    Operation::State insert_op_state;
    ASSERT_OK(insert_op->GetState(&insert_op_state));
    ASSERT_EQ(insert_op_state, Operation::State::FINISHED);
    ASSERT_OK(insert_op->Close());
  }
  std::string hostname_;

  int port = 21050;

  const std::string TEST_DB = "hs2client_test_db";
  const std::string TEST_TBL = "hs2client_test_table";
  const std::string TEST_COL1 = "int_col";
  const std::string TEST_COL2 = "string_col";

  const int NULL_INT_VALUE = -1;

  std::unique_ptr<Service> service_;
  std::unique_ptr<Session> session_;
};

class OperationTest : public HS2ClientTest {};

TEST_F(OperationTest, TestFetch) {
  CreateTestTable();
  InsertIntoTestTable(std::vector<int>({1, 2, 3, 4}),
                      std::vector<string>({"a", "b", "c", "d"}));

  std::unique_ptr<Operation> select_op;
  ASSERT_OK(session_->ExecuteStatement("select * from " + TEST_TBL + " order by int_col",
                                       &select_op));

  std::unique_ptr<ColumnarRowSet> results;
  bool has_more_rows = false;
  // Impala only supports NEXT and FIRST.
  ASSERT_RAISES(IOError,
                select_op->Fetch(2, FetchOrientation::LAST, &results, &has_more_rows));

  // Fetch the results in two batches by passing max_rows to Fetch.
  ASSERT_OK(select_op->Fetch(2, FetchOrientation::NEXT, &results, &has_more_rows));
  ASSERT_OK(Wait(select_op));
  ASSERT_TRUE(select_op->HasResultSet());
  std::unique_ptr<Int32Column> int_col = results->GetInt32Col(0);
  std::unique_ptr<StringColumn> string_col = results->GetStringCol(1);
  ASSERT_EQ(int_col->data(), std::vector<int>({1, 2}));
  ASSERT_EQ(string_col->data(), std::vector<string>({"a", "b"}));
  ASSERT_TRUE(has_more_rows);

  ASSERT_OK(select_op->Fetch(2, FetchOrientation::NEXT, &results, &has_more_rows));
  int_col = results->GetInt32Col(0);
  string_col = results->GetStringCol(1);
  ASSERT_EQ(int_col->data(), std::vector<int>({3, 4}));
  ASSERT_EQ(string_col->data(), std::vector<string>({"c", "d"}));

  ASSERT_OK(select_op->Fetch(2, FetchOrientation::NEXT, &results, &has_more_rows));
  int_col = results->GetInt32Col(0);
  string_col = results->GetStringCol(1);
  ASSERT_EQ(int_col->length(), 0);
  ASSERT_EQ(string_col->length(), 0);
  ASSERT_FALSE(has_more_rows);

  ASSERT_OK(select_op->Fetch(2, FetchOrientation::NEXT, &results, &has_more_rows));
  int_col = results->GetInt32Col(0);
  string_col = results->GetStringCol(1);
  ASSERT_EQ(int_col->length(), 0);
  ASSERT_EQ(string_col->length(), 0);
  ASSERT_FALSE(has_more_rows);

  ASSERT_OK(select_op->Close());
}

TEST_F(OperationTest, TestIsNull) {
  CreateTestTable();
  // Insert some NULLs and ensure Column::IsNull() is correct.
  InsertIntoTestTable(std::vector<int>({1, 2, 3, 4, 5, NULL_INT_VALUE}),
                      std::vector<string>({"a", "b", "NULL", "d", "NULL", "f"}));

  std::unique_ptr<Operation> select_nulls_op;
  ASSERT_OK(session_->ExecuteStatement("select * from " + TEST_TBL + " order by int_col",
                                       &select_nulls_op));

  std::unique_ptr<ColumnarRowSet> nulls_results;
  bool has_more_rows = false;
  ASSERT_OK(select_nulls_op->Fetch(&nulls_results, &has_more_rows));
  std::unique_ptr<Int32Column> int_col = nulls_results->GetInt32Col(0);
  std::unique_ptr<StringColumn> string_col = nulls_results->GetStringCol(1);
  ASSERT_EQ(int_col->length(), 6);
  ASSERT_EQ(int_col->length(), string_col->length());

  bool int_nulls[] = {false, false, false, false, false, true};
  for (int i = 0; i < int_col->length(); i++) {
    ASSERT_EQ(int_col->IsNull(i), int_nulls[i]);
  }
  bool string_nulls[] = {false, false, true, false, true, false};
  for (int i = 0; i < string_col->length(); i++) {
    ASSERT_EQ(string_col->IsNull(i), string_nulls[i]);
  }

  ASSERT_OK(select_nulls_op->Close());
}

TEST_F(OperationTest, TestCancel) {
  CreateTestTable();
  InsertIntoTestTable(std::vector<int>({1, 2, 3, 4}),
                      std::vector<string>({"a", "b", "c", "d"}));

  std::unique_ptr<Operation> op;
  ASSERT_OK(session_->ExecuteStatement("select count(*) from " + TEST_TBL, &op));
  ASSERT_OK(op->Cancel());
  // Impala currently returns ERROR and not CANCELED for canceled queries
  // due to the use of beeswax states, which don't support a canceled state.
  ASSERT_OK(Wait(op, Operation::State::ERROR));

  std::string profile;
  ASSERT_OK(op->GetProfile(&profile));
  ASSERT_TRUE(profile.find("Cancelled") != std::string::npos);

  ASSERT_OK(op->Close());
}

TEST_F(OperationTest, TestGetLog) {
  CreateTestTable();

  std::unique_ptr<Operation> op;
  ASSERT_OK(session_->ExecuteStatement("select count(*) from " + TEST_TBL, &op));
  std::string log;
  ASSERT_OK(op->GetLog(&log));
  ASSERT_NE(log, "");

  ASSERT_OK(op->Close());
}

TEST_F(OperationTest, TestGetResultSetMetadata) {
  const std::string TEST_COL1 = "int_col";
  const std::string TEST_COL2 = "varchar_col";
  const int MAX_LENGTH = 10;
  const std::string TEST_COL3 = "decimal_cal";
  const int PRECISION = 5;
  const int SCALE = 3;
  std::stringstream create_query;
  create_query << "create table " << TEST_TBL << " (" << TEST_COL1 << " int, "
               << TEST_COL2 << " varchar(" << MAX_LENGTH << "), " << TEST_COL3
               << " decimal(" << PRECISION << ", " << SCALE << "))";
  std::unique_ptr<Operation> create_table_op;
  ASSERT_OK(session_->ExecuteStatement(create_query.str(), &create_table_op));
  ASSERT_OK(create_table_op->Close());

  // Perform a select, and check that we get the right metadata back.
  std::unique_ptr<Operation> select_op;
  ASSERT_OK(session_->ExecuteStatement("select * from " + TEST_TBL, &select_op));
  std::vector<ColumnDesc> column_descs;
  ASSERT_OK(select_op->GetResultSetMetadata(&column_descs));
  ASSERT_EQ(column_descs.size(), 3);

  ASSERT_EQ(column_descs[0].column_name(), TEST_COL1);
  ASSERT_EQ(column_descs[0].type()->ToString(), "INT");
  ASSERT_EQ(column_descs[0].type()->type_id(), ColumnType::TypeId::INT);
  ASSERT_EQ(column_descs[0].position(), 0);

  ASSERT_EQ(column_descs[1].column_name(), TEST_COL2);
  ASSERT_EQ(column_descs[1].type()->ToString(), "VARCHAR");
  ASSERT_EQ(column_descs[1].type()->type_id(), ColumnType::TypeId::VARCHAR);
  ASSERT_EQ(column_descs[1].position(), 1);
  ASSERT_EQ(column_descs[1].GetCharacterType()->max_length(), MAX_LENGTH);

  ASSERT_EQ(column_descs[2].column_name(), TEST_COL3);
  ASSERT_EQ(column_descs[2].type()->ToString(), "DECIMAL");
  ASSERT_EQ(column_descs[2].type()->type_id(), ColumnType::TypeId::DECIMAL);
  ASSERT_EQ(column_descs[2].position(), 2);
  ASSERT_EQ(column_descs[2].GetDecimalType()->precision(), PRECISION);
  ASSERT_EQ(column_descs[2].GetDecimalType()->scale(), SCALE);

  ASSERT_OK(select_op->Close());

  // Insert ops don't have result sets.
  std::stringstream insert_query;
  insert_query << "insert into " << TEST_TBL << " VALUES (1, cast('a' as varchar("
               << MAX_LENGTH << ")), cast(1 as decimal(" << PRECISION << ", " << SCALE
               << ")))";
  std::unique_ptr<Operation> insert_op;
  ASSERT_OK(session_->ExecuteStatement(insert_query.str(), &insert_op));
  std::vector<ColumnDesc> insert_column_descs;
  ASSERT_OK(insert_op->GetResultSetMetadata(&insert_column_descs));
  ASSERT_EQ(insert_column_descs.size(), 0);
  ASSERT_OK(insert_op->Close());
}

class SessionTest : public HS2ClientTest {};

TEST_F(SessionTest, TestSessionConfig) {
  // Create a table in TEST_DB.
  const std::string& TEST_TBL = "hs2client_test_table";
  std::unique_ptr<Operation> create_table_op;
  ASSERT_OK(session_->ExecuteStatement(
      "create table " + TEST_TBL + " (int_col int, string_col string)",
      &create_table_op));
  ASSERT_OK(create_table_op->Close());

  // Start a new session with the use:database session option.
  std::string user = "user";
  HS2ClientConfig config_use;
  config_use.SetOption("use:database", TEST_DB);
  std::unique_ptr<Session> session_ok;
  ASSERT_OK(service_->OpenSession(user, config_use, &session_ok));

  // Ensure the use:database worked and we can access the table.
  std::unique_ptr<Operation> select_op;
  ASSERT_OK(session_ok->ExecuteStatement("select * from " + TEST_TBL, &select_op));
  ASSERT_OK(select_op->Close());
  ASSERT_OK(session_ok->Close());

  // Start another session without use:database.
  HS2ClientConfig config_no_use;
  std::unique_ptr<Session> session_error;
  ASSERT_OK(service_->OpenSession(user, config_no_use, &session_error));

  // Ensure the we can't access the table.
  std::unique_ptr<Operation> select_op_error;
  ASSERT_RAISES(IOError, session_error->ExecuteStatement("select * from " + TEST_TBL,
                                                         &select_op_error));
  ASSERT_OK(session_error->Close());
}

TEST(ServiceTest, TestConnect) {
  // Open a connection.
  std::string host = GetTestHost();
  int port = 21050;
  int conn_timeout = 0;
  ProtocolVersion protocol_version = ProtocolVersion::PROTOCOL_V7;
  std::unique_ptr<Service> service;
  ASSERT_OK(Service::Connect(host, port, conn_timeout, protocol_version, &service));
  ASSERT_TRUE(service->IsConnected());

  // Check that we can start a session.
  std::string user = "user";
  HS2ClientConfig config;
  std::unique_ptr<Session> session1;
  ASSERT_OK(service->OpenSession(user, config, &session1));
  ASSERT_OK(session1->Close());

  // Close the service. We should not be able to open a session.
  ASSERT_OK(service->Close());
  ASSERT_FALSE(service->IsConnected());
  ASSERT_OK(service->Close());
  std::unique_ptr<Session> session3;
  ASSERT_RAISES(IOError, service->OpenSession(user, config, &session3));
  ASSERT_OK(session3->Close());

  // We should be able to call Close again without errors.
  ASSERT_OK(service->Close());
  ASSERT_FALSE(service->IsConnected());
}

TEST(ServiceTest, TestFailedConnect) {
  std::string host = GetTestHost();
  int port = 21050;

  // Set 100ms timeout so these return quickly
  int conn_timeout = 100;

  ProtocolVersion protocol_version = ProtocolVersion::PROTOCOL_V7;
  std::unique_ptr<Service> service;

  std::string invalid_host = "does_not_exist";
  ASSERT_RAISES(IOError, Service::Connect(invalid_host, port, conn_timeout,
                                          protocol_version, &service));

  int invalid_port = -1;
  ASSERT_RAISES(IOError, Service::Connect(host, invalid_port, conn_timeout,
                                          protocol_version, &service));

  ProtocolVersion invalid_protocol_version = ProtocolVersion::PROTOCOL_V2;
  ASSERT_RAISES(NotImplemented, Service::Connect(host, port, conn_timeout,
                                                 invalid_protocol_version, &service));
}

}  // namespace hiveserver2
}  // namespace arrow
