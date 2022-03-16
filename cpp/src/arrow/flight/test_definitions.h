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
//
// While Googletest's value-parameterized tests would be a more
// natural way to do this, they cause runtime issues on MinGW/MSVC
// (Googletest thinks the test suite has been defined twice).

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

class ARROW_FLIGHT_EXPORT FlightTest : public testing::Test {
 protected:
  virtual std::string transport() const = 0;
};

/// Common tests of startup/shutdown
class ARROW_FLIGHT_EXPORT ConnectivityTest : public FlightTest {
 public:
  // Test methods
  void TestGetPort();
  void TestBuilderHook();
  void TestShutdown();
  void TestShutdownWithDeadline();
};

/// Common tests of data plane methods
class ARROW_FLIGHT_EXPORT DataTest : public FlightTest {
 public:
  void SetUp();
  void TearDown();
  Status ConnectClient();

  // Test methods
  void TestDoGetInts();
  void TestDoGetFloats();
  void TestDoGetDicts();
  void TestDoGetLargeBatch();
  void TestOverflowServerBatch();
  void TestOverflowClientBatch();
  void TestDoExchange();
  void TestDoExchangeNoData();
  void TestDoExchangeWriteOnlySchema();
  void TestDoExchangeGet();
  void TestDoExchangePut();
  void TestDoExchangeEcho();
  void TestDoExchangeTotal();
  void TestDoExchangeError();
  void TestIssue5095();

 private:
  void CheckDoGet(
      const FlightDescriptor& descr, const RecordBatchVector& expected_batches,
      std::function<void(const std::vector<FlightEndpoint>&)> check_endpoints);
  void CheckDoGet(const Ticket& ticket, const RecordBatchVector& expected_batches);

  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

/// \brief Specific tests of DoPut.
class ARROW_FLIGHT_EXPORT DoPutTest : public FlightTest {
 public:
  void SetUp();
  void TearDown();
  void CheckBatches(const FlightDescriptor& expected_descriptor,
                    const RecordBatchVector& expected_batches);
  void CheckDoPut(const FlightDescriptor& descr, const std::shared_ptr<Schema>& schema,
                  const RecordBatchVector& batches);

  // Test methods
  void TestInts();
  void TestFloats();
  void TestEmptyBatch();
  void TestDicts();
  void TestLargeBatch();
  void TestSizeLimit();

 private:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class ARROW_FLIGHT_EXPORT AppMetadataTestServer : public FlightServerBase {
 public:
  virtual ~AppMetadataTestServer() = default;

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override;

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override;
};

/// \brief Tests of app_metadata in data plane methods.
class ARROW_FLIGHT_EXPORT AppMetadataTest : public FlightTest {
 public:
  void SetUp();
  void TearDown();

  // Test methods
  void TestDoGet();
  void TestDoGetDictionaries();
  void TestDoPut();
  void TestDoPutDictionaries();
  void TestDoPutReadMetadata();

 private:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

/// \brief Tests of IPC options in data plane methods.
class ARROW_FLIGHT_EXPORT IpcOptionsTest : public FlightTest {
 public:
  void SetUp();
  void TearDown();

  // Test methods
  void TestDoGetReadOptions();
  void TestDoPutWriteOptions();
  void TestDoExchangeClientWriteOptions();
  void TestDoExchangeClientWriteOptionsBegin();
  void TestDoExchangeServerWriteOptions();

 private:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

/// \brief Tests of data plane methods with CUDA memory.
///
/// If not built with ARROW_CUDA, tests are no-ops.
class ARROW_FLIGHT_EXPORT CudaDataTest : public FlightTest {
 public:
  void SetUp() override;
  void TearDown() override;

  // Test methods
  void TestDoGet();
  void TestDoPut();
  void TestDoExchange();

 private:
  class Impl;
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
  std::shared_ptr<Impl> impl_;
};

}  // namespace flight
}  // namespace arrow
