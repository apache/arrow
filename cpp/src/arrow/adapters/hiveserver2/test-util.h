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

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "arrow/adapters/hiveserver2/service.h"
#include "arrow/adapters/hiveserver2/session.h"
#include "arrow/adapters/hiveserver2/thrift-internal.h"

#include "arrow/status.h"

namespace arrow {
namespace hiveserver2 {

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
    std::stringstream ss;
    ss << "Failed to reach state '" << OperationStateToString(state) << "' after "
       << retries << " retries.";
    return Status::Error(ss.str());
  }
}

// Creates a service, session, and database for use in tests.
class HS2ClientTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    static std::string kTestDB = "hs2client_test_db";

    int conn_timeout = 0;
    ProtocolVersion protocol_version = ProtocolVersion::PROTOCOL_V7;
    EXPECT_OK(
        Service::Connect(hostname, port, conn_timeout, protocol_version, &service_));

    std::string user = "user";
    HS2ClientConfig config;
    EXPECT_OK(service_->OpenSession(user, config, &session_));

    std::unique_ptr<Operation> drop_db_op;
    EXPECT_OK(session_->ExecuteStatement(
        "drop database if exists " + kTestDB + " cascade", &drop_db_op));
    EXPECT_OK(drop_db_op->Close());

    std::unique_ptr<Operation> create_db_op;
    EXPECT_OK(session_->ExecuteStatement("create database " + kTestDB, &create_db_op));
    EXPECT_OK(create_db_op->Close());

    std::unique_ptr<Operation> use_db_op;
    EXPECT_OK(session_->ExecuteStatement("use " + kTestDB, &use_db_op));
    EXPECT_OK(use_db_op->Close());
  }

  virtual void TearDown() {
    std::unique_ptr<Operation> use_db_op;
    EXPECT_OK(session_->ExecuteStatement("use default", &use_db_op));
    EXPECT_OK(use_db_op->Close());

    std::unique_ptr<Operation> drop_db_op;
    EXPECT_OK(
        session_->ExecuteStatement("drop database " + kTestDB + " cascade", &drop_db_op));
    EXPECT_OK(drop_db_op->Close());

    EXPECT_OK(session_->Close());
    EXPECT_OK(service_->Close());
  }

  void CreateTestTable() {
    std::unique_ptr<Operation> create_table_op;
    EXPECT_OK(session_->ExecuteStatement(
        "create table " + TEST_TBL + " (" + TEST_COL1 + " int, " + TEST_COL2 + " string)",
        &create_table_op));
    EXPECT_OK(create_table_op->Close());
  }

  void InsertIntoTestTable(std::vector<int> int_col_data,
                           std::vector<std::string> string_col_data) {
    EXPECT_EQ(int_col_data.size(), string_col_data.size());

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
    EXPECT_OK(session_->ExecuteStatement(query.str(), &insert_op));
    EXPECT_OK(Wait(insert_op));
    Operation::State insert_op_state;
    EXPECT_OK(insert_op->GetState(&insert_op_state));
    EXPECT_EQ(insert_op_state, Operation::State::FINISHED);
    EXPECT_OK(insert_op->Close());
  }

  const std::string hostname = "localhost";
  int port = 21050;

  const std::string TEST_TBL = "hs2client_test_table";
  const std::string TEST_COL1 = "int_col";
  const std::string TEST_COL2 = "string_col";

  const int NULL_INT_VALUE = -1;

  std::unique_ptr<Service> service_;
  std::unique_ptr<Session> session_;
};

}  // namespace hiveserver2
}  // namespace arrow
