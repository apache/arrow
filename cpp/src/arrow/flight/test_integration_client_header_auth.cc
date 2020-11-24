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

// Client implementation for Flight integration testing. Loads
// RecordBatches from the given JSON file and uploads them to the
// Flight server, which stores the data and schema in memory. The
// client then requests the data from the server and compares it to
// the data originally uploaded.

#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/file.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/json_integration.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/test_integration.h"
#include "arrow/flight/test_util.h"

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(username, "flight1", "Username to use in basic auth");
DEFINE_string(password, "woohoo1", "Password to use in basic auth");
DEFINE_string(username_invalid, "foooo", "Username to use in basic auth");
DEFINE_string(password_invalid, "barrr", "Password to use in basic auth");

void TestValidCredentials() {
  std::cout << "Testing with valid auth credentials." << std::endl;
  auto get_uri = []() {
    return "grpc+tcp://" + FLAGS_host + ":" + std::to_string(FLAGS_port);
  };

  // Generate Location with URI.
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::Parse(get_uri(), &location));

  // Create client and connect to Location.
  std::unique_ptr<arrow::flight::FlightClient> client;
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  // Authenticate credentials and retreive token.
  std::pair<std::string, std::string> bearer_token = std::make_pair("", "");
  ABORT_NOT_OK(
      client->AuthenticateBasicToken({}, FLAGS_username, FLAGS_password, &bearer_token));

  // Validate token was received.
  if (bearer_token == std::make_pair(std::string(""), std::string(""))) {
    std::cout << "Testing valid credentials was unsuccessful: "
              << "Failed to get token from basic authentication." << std::endl;
    return;
  }

  // Try to list flights, this will force the bearer token to be send and authenticated.
  std::unique_ptr<arrow::flight::FlightListing> listing;
  arrow::flight::FlightCallOptions options;
  options.headers.push_back(bearer_token);
  ABORT_NOT_OK(client->ListFlights(options, {}, &listing));
  std::cout << "Test valid credentials was successful." << std::endl;
}

void TestInvalidCredentials() {
  auto get_uri = []() {
    return "grpc+tcp://" + FLAGS_host + ":" + std::to_string(FLAGS_port);
  };

  // Generate Location with URI.
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::Parse(get_uri(), &location));

  // Create client and connect to Location.
  std::unique_ptr<arrow::flight::FlightClient> client;
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  // Authenticate credentials and retreive token.
  std::pair<std::string, std::string> bearer_token = std::make_pair("", "");
  EXPECT_EQ(arrow::StatusCode::IOError,
            client
                ->AuthenticateBasicToken({}, FLAGS_username_invalid,
                                         FLAGS_password_invalid, &bearer_token)
                .code());

  // Validate token was received.
  if (bearer_token != std::make_pair(std::string(""), std::string(""))) {
    std::cout << "Testing invalid credentials was unsuccessful: "
              << "Obtained token from basic authentication when using "
              << "invalid credentials." << std::endl;
  }

  std::cout << "Testing invalid credentials was successful." << std::endl;
}

int main(int argc, char** argv) {
  std::cout << "Starting auth header based flight integration test." << std::endl;
  TestValidCredentials();
  TestInvalidCredentials();
}
