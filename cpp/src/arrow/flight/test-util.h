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
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/process.hpp>

#include "arrow/ipc/test-common.h"
#include "arrow/status.h"
#include "arrow/test-util.h"

#include "arrow/flight/api.h"
#include "arrow/flight/internal.h"

namespace bp = boost::process;

namespace arrow {
namespace flight {

// ----------------------------------------------------------------------
// Fixture to use for running test servers

struct TestServer {
 public:
  explicit TestServer(const std::string& executable_name, int port)
      : executable_name_(executable_name), port_(port) {}

  void Start() {
    std::string str_port = std::to_string(port_);
    server_process_.reset(
        new bp::child(bp::search_path(executable_name_), "-port", str_port));
    std::cout << "Server running with pid " << server_process_->id() << std::endl;
  }

  int Stop() {
    kill(server_process_->id(), SIGTERM);
    server_process_->wait();
    return server_process_->exit_code();
  }

  bool IsRunning() { return server_process_->running(); }

  int port() const { return port_; }

 private:
  std::string executable_name_;
  int port_;
  std::unique_ptr<bp::child> server_process_;
};

// ----------------------------------------------------------------------
// A RecordBatchReader for serving a sequence of in-memory record batches

class BatchIterator : public RecordBatchReader {
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

// ----------------------------------------------------------------------
// Example data for test-server and unit tests

using BatchVector = std::vector<std::shared_ptr<RecordBatch>>;

std::shared_ptr<Schema> ExampleSchema1() {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", int32());
  return ::arrow::schema({f0, f1});
}

std::shared_ptr<Schema> ExampleSchema2() {
  auto f0 = field("f0", utf8());
  auto f1 = field("f1", binary());
  return ::arrow::schema({f0, f1});
}

Status MakeFlightInfo(const Schema& schema, const FlightDescriptor& descriptor,
                      const std::vector<FlightEndpoint>& endpoints,
                      uint64_t total_records, uint64_t total_bytes,
                      FlightInfo::Data* out) {
  out->descriptor = descriptor;
  out->endpoints = endpoints;
  out->total_records = total_records;
  out->total_bytes = total_bytes;
  return internal::SchemaToString(schema, &out->schema);
}

std::vector<FlightInfo> ExampleFlightInfo() {
  FlightEndpoint endpoint1({{"ticket-id-1"}, {{"foo1.bar.com", 92385}}});
  FlightEndpoint endpoint2({{"ticket-id-2"}, {{"foo2.bar.com", 92385}}});
  FlightEndpoint endpoint3({{"ticket-id-3"}, {{"foo3.bar.com", 92385}}});
  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"foo", "bar"}};
  FlightDescriptor descr2{FlightDescriptor::CMD, "my_command", {}};

  auto schema1 = ExampleSchema1();
  auto schema2 = ExampleSchema2();

  FlightInfo::Data flight1, flight2;
  EXPECT_OK(
      MakeFlightInfo(*schema1, descr1, {endpoint1, endpoint2}, 1000, 100000, &flight1));
  EXPECT_OK(MakeFlightInfo(*schema2, descr2, {endpoint3}, 1000, 100000, &flight2));
  return {FlightInfo(flight1), FlightInfo(flight2)};
}

Status SimpleIntegerBatches(const int num_batches, BatchVector* out) {
  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < num_batches; ++i) {
    // Make all different sizes, use different random seed
    RETURN_NOT_OK(ipc::MakeIntBatchSized(10 + i, &batch, i));
    out->push_back(batch);
  }
  return Status::OK();
}

std::vector<ActionType> ExampleActionTypes() {
  return {{"drop", "drop a dataset"}, {"cache", "cache a dataset"}};
}

}  // namespace flight
}  // namespace arrow
