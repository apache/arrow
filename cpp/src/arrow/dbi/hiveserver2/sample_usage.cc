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

#include <cassert>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/dbi/hiveserver2/api.h"

namespace hs2 = arrow::hiveserver2;

using arrow::Status;

using hs2::Operation;
using hs2::Service;
using hs2::Session;

#define ABORT_NOT_OK(s)                  \
  do {                                   \
    ::arrow::Status _s = (s);            \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      std::cerr << s.ToString() << "\n"; \
      std::abort();                      \
    }                                    \
  } while (false);

int main(int argc, char** argv) {
  // Connect to the server.
  std::string host = "localhost";
  int port = 21050;
  int conn_timeout = 0;
  hs2::ProtocolVersion protocol = hs2::ProtocolVersion::PROTOCOL_V7;
  std::unique_ptr<Service> service;
  Status status = Service::Connect(host, port, conn_timeout, protocol, &service);
  if (!status.ok()) {
    std::cout << "Failed to connect to service: " << status.ToString();
    ABORT_NOT_OK(service->Close());
    return 1;
  }

  // Open a session.
  std::string user = "user";
  hs2::HS2ClientConfig config;
  std::unique_ptr<Session> session;
  status = service->OpenSession(user, config, &session);
  if (!status.ok()) {
    std::cout << "Failed to open session: " << status.ToString();
    ABORT_NOT_OK(session->Close());
    ABORT_NOT_OK(service->Close());
    return 1;
  }

  // Execute a statement.
  std::string statement = "SELECT int_col, string_col FROM test order by int_col";
  std::unique_ptr<hs2::Operation> execute_op;
  status = session->ExecuteStatement(statement, &execute_op);
  if (!status.ok()) {
    std::cout << "Failed to execute select: " << status.ToString();
    ABORT_NOT_OK(execute_op->Close());
    ABORT_NOT_OK(session->Close());
    ABORT_NOT_OK(service->Close());
    return 1;
  }

  std::unique_ptr<hs2::ColumnarRowSet> execute_results;
  bool has_more_rows = true;
  int64_t total_retrieved = 0;
  std::cout << "Contents of test:\n";
  while (has_more_rows) {
    status = execute_op->Fetch(&execute_results, &has_more_rows);
    if (!status.ok()) {
      std::cout << "Failed to fetch results: " << status.ToString();
      ABORT_NOT_OK(execute_op->Close());
      ABORT_NOT_OK(session->Close());
      ABORT_NOT_OK(service->Close());
      return 1;
    }

    std::unique_ptr<hs2::Int32Column> int_col = execute_results->GetInt32Col(0);
    std::unique_ptr<hs2::StringColumn> string_col = execute_results->GetStringCol(1);
    assert(int_col->length() == string_col->length());
    total_retrieved += int_col->length();
    for (int64_t i = 0; i < int_col->length(); ++i) {
      if (int_col->IsNull(i)) {
        std::cout << "NULL";
      } else {
        std::cout << int_col->GetData(i);
      }
      std::cout << ":";

      if (string_col->IsNull(i)) {
        std::cout << "NULL";
      } else {
        std::cout << "'" << string_col->GetData(i) << "'";
      }
      std::cout << "\n";
    }
  }
  std::cout << "retrieved " << total_retrieved << " rows\n";
  std::cout << "\n";
  ABORT_NOT_OK(execute_op->Close());

  std::unique_ptr<Operation> show_tables_op;
  status = session->ExecuteStatement("show tables", &show_tables_op);
  if (!status.ok()) {
    std::cout << "Failed to execute GetTables: " << status.ToString();
    ABORT_NOT_OK(show_tables_op->Close());
    ABORT_NOT_OK(session->Close());
    ABORT_NOT_OK(service->Close());
    return 1;
  }

  std::cout << "Show tables:\n";
  hs2::Util::PrintResults(show_tables_op.get(), std::cout);
  ABORT_NOT_OK(show_tables_op->Close());

  // Shut down.
  ABORT_NOT_OK(session->Close());
  ABORT_NOT_OK(service->Close());

  return 0;
}
