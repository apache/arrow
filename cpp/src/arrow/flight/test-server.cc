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

// Example server implementation to use for unit testing and benchmarking
// purposes

#include <signal.h>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/test-common.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"

#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/test-util.h"

DEFINE_int32(port, 31337, "Server port to listen on");

namespace arrow {
namespace flight {

Status GetBatchForFlight(const Ticket& ticket, std::shared_ptr<RecordBatchReader>* out) {
  if (ticket.ticket == "ticket-ints-1") {
    BatchVector batches;
    RETURN_NOT_OK(ExampleIntBatches(&batches));
    *out = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
    return Status::OK();
  } else if (ticket.ticket == "ticket-dicts-1") {
    BatchVector batches;
    RETURN_NOT_OK(ExampleDictBatches(&batches));
    *out = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
    return Status::OK();
  } else {
    return Status::NotImplemented("no stream implemented for this ticket");
  }
}

class FlightTestServer : public FlightServerBase {
  Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                     std::unique_ptr<FlightListing>* listings) override {
    std::vector<FlightInfo> flights = ExampleFlightInfo();
    *listings = std::unique_ptr<FlightListing>(new SimpleFlightListing(flights));
    return Status::OK();
  }

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* out) override {
    std::vector<FlightInfo> flights = ExampleFlightInfo();

    for (const auto& info : flights) {
      if (info.descriptor().Equals(request)) {
        *out = std::unique_ptr<FlightInfo>(new FlightInfo(info));
        return Status::OK();
      }
    }
    return Status::Invalid("Flight not found: ", request.ToString());
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    // Test for ARROW-5095
    if (request.ticket == "ARROW-5095-fail") {
      return Status::UnknownError("Server-side error");
    }
    if (request.ticket == "ARROW-5095-success") {
      return Status::OK();
    }

    std::shared_ptr<RecordBatchReader> batch_reader;
    RETURN_NOT_OK(GetBatchForFlight(request, &batch_reader));

    *data_stream = std::unique_ptr<FlightDataStream>(new RecordBatchStream(batch_reader));
    return Status::OK();
  }

  Status RunAction1(const Action& action, std::unique_ptr<ResultStream>* out) {
    std::vector<Result> results;
    for (int i = 0; i < 3; ++i) {
      Result result;
      std::string value = action.body->ToString() + "-part" + std::to_string(i);
      RETURN_NOT_OK(Buffer::FromString(value, &result.body));
      results.push_back(result);
    }
    *out = std::unique_ptr<ResultStream>(new SimpleResultStream(std::move(results)));
    return Status::OK();
  }

  Status RunAction2(std::unique_ptr<ResultStream>* out) {
    // Empty
    *out = std::unique_ptr<ResultStream>(new SimpleResultStream({}));
    return Status::OK();
  }

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* out) override {
    if (action.type == "action1") {
      return RunAction1(action, out);
    } else if (action.type == "action2") {
      return RunAction2(out);
    } else {
      return Status::NotImplemented(action.type);
    }
  }

  Status ListActions(const ServerCallContext& context,
                     std::vector<ActionType>* out) override {
    std::vector<ActionType> actions = ExampleActionTypes();
    *out = std::move(actions);
    return Status::OK();
  }
};

}  // namespace flight
}  // namespace arrow

std::unique_ptr<arrow::flight::FlightTestServer> g_server;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  g_server.reset(new arrow::flight::FlightTestServer);
  ARROW_CHECK_OK(
      g_server->Init(std::unique_ptr<arrow::flight::NoOpAuthHandler>(), FLAGS_port));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(g_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Server listening on localhost:" << FLAGS_port << std::endl;
  ARROW_CHECK_OK(g_server->Serve());
  return 0;
}
