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

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/status.h"

#include "arrow/flight/client_auth.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/types.h"
#include "arrow/flight/visibility.h"

namespace boost {
namespace process {

class child;

}  // namespace process
}  // namespace boost

namespace arrow {
namespace flight {

// ----------------------------------------------------------------------
// Fixture to use for running test servers

// Get a TCP port number to listen on.  This is a different number every time,
// as reusing the same port accross tests can produce spurious "Stream removed"
// errors as Windows.
ARROW_FLIGHT_EXPORT
int GetListenPort();

class ARROW_FLIGHT_EXPORT TestServer {
 public:
  explicit TestServer(const std::string& executable_name)
      : executable_name_(executable_name), port_(GetListenPort()) {}
  explicit TestServer(const std::string& executable_name, int port)
      : executable_name_(executable_name), port_(port) {}

  void Start();

  int Stop();

  bool IsRunning();

  int port() const;

 private:
  std::string executable_name_;
  int port_;
  std::shared_ptr<::boost::process::child> server_process_;
};

class ARROW_FLIGHT_EXPORT InProcessTestServer {
 public:
  explicit InProcessTestServer(std::unique_ptr<FlightServerBase> server,
                               const Location& location)
      : server_(std::move(server)), location_(location), thread_() {}
  ~InProcessTestServer();
  Status Start();
  void Stop();
  const Location& location() const;

 private:
  std::unique_ptr<FlightServerBase> server_;
  Location location_;
  std::thread thread_;
};

// ----------------------------------------------------------------------
// A RecordBatchReader for serving a sequence of in-memory record batches

// Silence warning
// "non dll-interface class RecordBatchReader used as base for dll-interface class"
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4275)
#endif

class ARROW_FLIGHT_EXPORT BatchIterator : public RecordBatchReader {
 public:
  BatchIterator(const std::shared_ptr<Schema>& schema,
                const std::vector<std::shared_ptr<RecordBatch>>& batches)
      : schema_(schema), batches_(batches), position_(0) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    if (position_ >= batches_.size()) {
      *out = nullptr;
    } else {
      *out = batches_[position_++];
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  size_t position_;
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

// ----------------------------------------------------------------------
// Example data for test-server and unit tests

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

ARROW_FLIGHT_EXPORT
std::shared_ptr<Schema> ExampleIntSchema();

ARROW_FLIGHT_EXPORT
std::shared_ptr<Schema> ExampleStringSchema();

ARROW_FLIGHT_EXPORT
std::shared_ptr<Schema> ExampleDictSchema();

ARROW_FLIGHT_EXPORT
Status ExampleIntBatches(BatchVector* out);

ARROW_FLIGHT_EXPORT
Status ExampleDictBatches(BatchVector* out);

ARROW_FLIGHT_EXPORT
std::vector<FlightInfo> ExampleFlightInfo();

ARROW_FLIGHT_EXPORT
std::vector<ActionType> ExampleActionTypes();

ARROW_FLIGHT_EXPORT
Status MakeFlightInfo(const Schema& schema, const FlightDescriptor& descriptor,
                      const std::vector<FlightEndpoint>& endpoints, int64_t total_records,
                      int64_t total_bytes, FlightInfo::Data* out);

// ----------------------------------------------------------------------
// A pair of authentication handlers that check for a predefined password
// and set the peer identity to a predefined username.

class ARROW_FLIGHT_EXPORT TestServerAuthHandler : public ServerAuthHandler {
 public:
  explicit TestServerAuthHandler(const std::string& username,
                                 const std::string& password);
  ~TestServerAuthHandler() override;
  Status Authenticate(ServerAuthSender* outgoing, ServerAuthReader* incoming) override;
  Status IsValid(const std::string& token, std::string* peer_identity) override;

 private:
  std::string username_;
  std::string password_;
};

class ARROW_FLIGHT_EXPORT TestClientAuthHandler : public ClientAuthHandler {
 public:
  explicit TestClientAuthHandler(const std::string& username,
                                 const std::string& password);
  ~TestClientAuthHandler() override;
  Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override;
  Status GetToken(std::string* token) override;

 private:
  std::string username_;
  std::string password_;
};

}  // namespace flight
}  // namespace arrow
