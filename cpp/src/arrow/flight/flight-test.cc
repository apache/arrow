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

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/ipc/test-common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/flight/api.h"

#ifdef GRPCPP_GRPCPP_H
#error "gRPC headers should not be in public API"
#endif

#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/test-util.h"

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {

TEST(TestFlight, StartStopTestServer) {
  TestServer server("flight-test-server", 30000);
  server.Start();
  ASSERT_TRUE(server.IsRunning());

  std::this_thread::sleep_for(std::chrono::duration<double>(0.2));

  ASSERT_TRUE(server.IsRunning());
  int exit_code = server.Stop();
  ASSERT_EQ(0, exit_code);
  ASSERT_FALSE(server.IsRunning());
}

// ----------------------------------------------------------------------
// Client tests

class TestFlightClient : public ::testing::Test {
 public:
  // Uncomment these when you want to run the server separately for
  // debugging/valgrind/gdb

  // void SetUp() {
  //   port_ = 92358;
  //   ASSERT_OK(ConnectClient());
  // }
  // void TearDown() {}

  void SetUp() {
    port_ = 30000;
    server_.reset(new TestServer("flight-test-server", port_));
    server_->Start();
    ASSERT_OK(ConnectClient());
  }

  void TearDown() { server_->Stop(); }

  Status ConnectClient() { return FlightClient::Connect("localhost", port_, &client_); }

 protected:
  int port_;
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<TestServer> server_;
};

// The server implementation is in test-server.cc; to make changes to the
// expected results, make edits there
void AssertEqual(const FlightDescriptor& expected, const FlightDescriptor& actual) {}

void AssertEqual(const Ticket& expected, const Ticket& actual) {
  ASSERT_EQ(expected.ticket, actual.ticket);
}

void AssertEqual(const Location& expected, const Location& actual) {
  ASSERT_EQ(expected.host, actual.host);
  ASSERT_EQ(expected.port, actual.port);
}

void AssertEqual(const std::vector<FlightEndpoint>& expected,
                 const std::vector<FlightEndpoint>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    AssertEqual(expected[i].ticket, actual[i].ticket);

    ASSERT_EQ(expected[i].locations.size(), actual[i].locations.size());
    for (size_t j = 0; j < expected[i].locations.size(); ++j) {
      AssertEqual(expected[i].locations[j], actual[i].locations[j]);
    }
  }
}

void AssertEqual(const FlightInfo& expected, const FlightInfo& actual) {
  std::shared_ptr<Schema> ex_schema, actual_schema;
  ASSERT_OK(expected.GetSchema(&ex_schema));
  ASSERT_OK(actual.GetSchema(&actual_schema));

  AssertSchemaEqual(*ex_schema, *actual_schema);
  ASSERT_EQ(expected.total_records(), actual.total_records());
  ASSERT_EQ(expected.total_bytes(), actual.total_bytes());

  AssertEqual(expected.descriptor(), actual.descriptor());
  AssertEqual(expected.endpoints(), actual.endpoints());
}

void AssertEqual(const ActionType& expected, const ActionType& actual) {
  ASSERT_EQ(expected.type, actual.type);
  ASSERT_EQ(expected.description, actual.description);
}

template <typename T>
void AssertEqual(const std::vector<T>& expected, const std::vector<T>& actual) {
  ASSERT_EQ(expected.size(), actual.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    AssertEqual(expected[i], actual[i]);
  }
}

TEST_F(TestFlightClient, ListFlights) {
  std::unique_ptr<FlightListing> listing;
  ASSERT_OK(client_->ListFlights(&listing));
  ASSERT_TRUE(listing != nullptr);

  std::vector<FlightInfo> flights = ExampleFlightInfo();
  std::unique_ptr<FlightInfo> info;

  for (const FlightInfo& flight : flights) {
    ASSERT_OK(listing->Next(&info));
    AssertEqual(flight, *info);
  }
  ASSERT_OK(listing->Next(&info));
  ASSERT_TRUE(info == nullptr);

  ASSERT_OK(listing->Next(&info));
}

TEST_F(TestFlightClient, GetFlightInfo) {
  FlightDescriptor descr{FlightDescriptor::PATH, "", {"foo", "bar"}};
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK(client_->GetFlightInfo(descr, &info));

  ASSERT_TRUE(info != nullptr);

  std::vector<FlightInfo> flights = ExampleFlightInfo();
  AssertEqual(flights[0], *info);
}

TEST(TestFlightProtocol, FlightDescriptor) {
  FlightDescriptor descr_test;
  pb::FlightDescriptor pb_descr;

  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"foo", "bar"}};
  ASSERT_OK(internal::ToProto(descr1, &pb_descr));
  ASSERT_OK(internal::FromProto(pb_descr, &descr_test));
  AssertEqual(descr1, descr_test);

  FlightDescriptor descr2{FlightDescriptor::CMD, "command", {}};
  ASSERT_OK(internal::ToProto(descr2, &pb_descr));
  ASSERT_OK(internal::FromProto(pb_descr, &descr_test));
  AssertEqual(descr2, descr_test);
}

TEST_F(TestFlightClient, DoGet) {
  FlightDescriptor descr{FlightDescriptor::PATH, "", {"foo", "bar"}};
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK(client_->GetFlightInfo(descr, &info));

  // Two endpoints in the example FlightInfo
  ASSERT_EQ(2, info->endpoints().size());

  Ticket ticket = info->endpoints()[0].ticket;
  AssertEqual(Ticket{"ticket-id-1"}, ticket);

  std::shared_ptr<Schema> schema;
  ASSERT_OK(info->GetSchema(&schema));

  auto expected_schema = ExampleSchema1();
  AssertSchemaEqual(*expected_schema, *schema);

  std::unique_ptr<RecordBatchReader> stream;
  ASSERT_OK(client_->DoGet(ticket, &stream));

  BatchVector expected_batches;
  const int num_batches = 5;
  ASSERT_OK(SimpleIntegerBatches(num_batches, &expected_batches));
  std::shared_ptr<RecordBatch> chunk;
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK(stream->ReadNext(&chunk));
    ASSERT_BATCHES_EQUAL(*expected_batches[i], *chunk);
  }

  // Stream exhausted
  ASSERT_OK(stream->ReadNext(&chunk));
  ASSERT_EQ(nullptr, chunk);
}

TEST_F(TestFlightClient, ListActions) {
  std::vector<ActionType> actions;
  ASSERT_OK(client_->ListActions(&actions));

  std::vector<ActionType> expected = ExampleActionTypes();
  AssertEqual(expected, actions);
}

TEST_F(TestFlightClient, DoAction) {
  Action action;
  std::unique_ptr<ResultStream> stream;
  std::unique_ptr<Result> result;

  // Run action1
  action.type = "action1";

  const std::string action1_value = "action1-content";
  ASSERT_OK(Buffer::FromString(action1_value, &action.body));
  ASSERT_OK(client_->DoAction(action, &stream));

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(stream->Next(&result));
    std::string expected = action1_value + "-part" + std::to_string(i);
    ASSERT_EQ(expected, result->body->ToString());
  }

  // stream consumed
  ASSERT_OK(stream->Next(&result));
  ASSERT_EQ(nullptr, result);

  // Run action2, no results
  action.type = "action2";
  ASSERT_OK(client_->DoAction(action, &stream));

  ASSERT_OK(stream->Next(&result));
  ASSERT_EQ(nullptr, result);
}

}  // namespace flight
}  // namespace arrow
