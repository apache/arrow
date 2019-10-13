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

#include "arrow/flight/test_integration.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace arrow {
namespace flight {

/// \brief The server for the basic auth integration test.
class AuthBasicProtoServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // Respond with the authenticated username.
    auto buf = Buffer::FromString(context.peer_identity());
    *result = std::unique_ptr<ResultStream>(new SimpleResultStream({Result{buf}}));
    return Status::OK();
  }
};

/// Validate the result of a DoAction.
Status CheckActionResults(FlightClient* client, const Action& action,
                          std::vector<std::string> results) {
  std::unique_ptr<ResultStream> stream;
  RETURN_NOT_OK(client->DoAction(action, &stream));
  std::unique_ptr<Result> result;
  for (const std::string& expected : results) {
    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    const auto actual = result->body->ToString();
    if (expected != actual) {
      return Status::Invalid("Got wrong result; expected", expected, "but got", actual);
    }
  }
  RETURN_NOT_OK(stream->Next(&result));
  if (result) {
    return Status::Invalid("Action result stream had too many entries");
  }
  return Status::OK();
}

// The expected username for the basic auth integration test.
constexpr auto kAuthUsername = "arrow";
// The expected password for the basic auth integration test.
constexpr auto kAuthPassword = "flight";

/// \brief A scenario testing the basic auth protobuf.
class AuthBasicProtoScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new AuthBasicProtoServer());
    options->auth_handler =
        std::make_shared<TestServerBasicAuthHandler>(kAuthUsername, kAuthPassword);
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    Action action;
    std::unique_ptr<ResultStream> stream;
    std::shared_ptr<FlightStatusDetail> detail;
    const auto& status = client->DoAction(action, &stream);
    detail = FlightStatusDetail::UnwrapStatus(status);
    // This client is unauthenticated and should fail.
    if (detail == nullptr) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", status.ToString());
    }
    if (detail->code() != FlightStatusCode::Unauthenticated) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", detail->ToString());
    }

    auto client_handler = std::unique_ptr<ClientAuthHandler>(
        new TestClientBasicAuthHandler(kAuthUsername, kAuthPassword));
    RETURN_NOT_OK(client->Authenticate({}, std::move(client_handler)));
    return CheckActionResults(client.get(), action, {kAuthUsername});
  }
};

Status GetScenario(const std::string& scenario_name, std::shared_ptr<Scenario>* out) {
  if (scenario_name == "auth:basic_proto") {
    *out = std::make_shared<AuthBasicProtoScenario>();
    return Status::OK();
  }
  return Status::KeyError("Scenario not found: ", scenario_name);
}

}  // namespace flight
}  // namespace arrow
