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

// Common test definitions for Flight. Individual transport
// implementations can instantiate these tests.

#pragma once

#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/flight/server.h"
#include "arrow/flight/types.h"

namespace arrow {
namespace flight {

class FlightTest : public testing::TestWithParam<std::string> {
 protected:
  std::string transport() const { return GetParam(); }
};

/// Common tests of startup/shutdown
class ConnectivityTest : public FlightTest {};

/// Common tests of data plane methods
class DataTest : public FlightTest {
 public:
  ~DataTest();

  void SetUp();
  void TearDown();
  Status ConnectClient();

  void CheckDoGet(
      const FlightDescriptor& descr, const RecordBatchVector& expected_batches,
      std::function<void(const std::vector<FlightEndpoint>&)> check_endpoints);
  void CheckDoGet(const Ticket& ticket, const RecordBatchVector& expected_batches);

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class DoPutTest : public FlightTest {
 public:
  void SetUp();
  void TearDown();

  void CheckBatches(const FlightDescriptor& expected_descriptor,
                    const RecordBatchVector& expected_batches);

  void CheckDoPut(const FlightDescriptor& descr, const std::shared_ptr<Schema>& schema,
                  const RecordBatchVector& batches);

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class AppMetadataTestServer : public FlightServerBase {
 public:
  virtual ~AppMetadataTestServer() = default;

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override;

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override;
};

class AppMetadataTest : public FlightTest {
 public:
  void SetUp();

  void TearDown();

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class IpcOptionsTest : public FlightTest {
 public:
  void SetUp();

  void TearDown();

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

}  // namespace flight
}  // namespace arrow
