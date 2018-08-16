// Copyright 2016 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "hs2client/service.h"

#include <gtest/gtest.h>
#include <memory>

#include "hs2client/session.h"
#include "hs2client/test-util.h"

using namespace hs2client;
using namespace std;

TEST(ServiceTest, TestConnect) {
  // Open a connection.
  string host = "localhost";
  int port = 21050;
  int conn_timeout = 0;
  ProtocolVersion protocol_version = ProtocolVersion::HS2CLIENT_PROTOCOL_V7;
  unique_ptr<Service> service;
  EXPECT_OK(Service::Connect(host, port, conn_timeout, protocol_version,
      &service));
  EXPECT_TRUE(service->IsConnected());

  // Check that we can start a session.
  string user = "user";
  HS2ClientConfig config;
  unique_ptr<Session> session1;
  EXPECT_OK(service->OpenSession(user, config, &session1));
  EXPECT_OK(session1->Close());

  // Close the service. We should not be able to open a session.
  EXPECT_OK(service->Close());
  EXPECT_FALSE(service->IsConnected());
  EXPECT_OK(service->Close());
  unique_ptr<Session> session3;
  EXPECT_ERROR(service->OpenSession(user, config, &session3));
  EXPECT_OK(session3->Close());

  // We should be able to call Close again without errors.
  EXPECT_OK(service->Close());
  EXPECT_FALSE(service->IsConnected());
}

TEST(ServiceTest, TestFailedConnect) {
  string host = "localhost";
  int port = 21050;
  int conn_timeout = 0;
  ProtocolVersion protocol_version = ProtocolVersion::HS2CLIENT_PROTOCOL_V7;
  unique_ptr<Service> service;

  string invalid_host = "does_not_exist";
  EXPECT_ERROR(Service::Connect(invalid_host, port, conn_timeout, protocol_version,
      &service));

  int invalid_port = -1;
  EXPECT_ERROR(Service::Connect(host, invalid_port, conn_timeout, protocol_version,
      &service));

  ProtocolVersion invalid_protocol_version = ProtocolVersion::HS2CLIENT_PROTOCOL_V2;
  EXPECT_ERROR(Service::Connect(host, port, conn_timeout, invalid_protocol_version,
      &service));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
