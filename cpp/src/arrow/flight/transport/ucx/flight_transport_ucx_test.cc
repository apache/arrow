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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/flight/test_definitions.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/transport/ucx/ucx.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/config.h"

#ifdef UCP_API_VERSION
#error "UCX headers should not be in public API"
#endif

#include "arrow/flight/transport/ucx/ucx_internal.h"

#ifdef ARROW_CUDA
#include "arrow/gpu/cuda_api.h"
#endif

namespace arrow {
namespace flight {

class UcxEnvironment : public ::testing::Environment {
 public:
  void SetUp() override { transport::ucx::InitializeFlightUcx(); }
};

testing::Environment* const kUcxEnvironment =
    testing::AddGlobalTestEnvironment(new UcxEnvironment());

//------------------------------------------------------------
// Common transport tests

class UcxConnectivityTest : public ConnectivityTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_CONNECTIVITY(UcxConnectivityTest);

class UcxDataTest : public DataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_DATA(UcxDataTest);

class UcxDoPutTest : public DoPutTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_DO_PUT(UcxDoPutTest);

class UcxAppMetadataTest : public AppMetadataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_APP_METADATA(UcxAppMetadataTest);

class UcxIpcOptionsTest : public IpcOptionsTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_IPC_OPTIONS(UcxIpcOptionsTest);

class UcxCudaDataTest : public CudaDataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_CUDA_DATA(UcxCudaDataTest);

class UcxErrorHandlingTest : public ErrorHandlingTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "ucx"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }

  void TestGetFlightInfoMetadata() { GTEST_SKIP() << "Middleware not implemented"; }
};
ARROW_FLIGHT_TEST_ERROR_HANDLING(UcxErrorHandlingTest);

//------------------------------------------------------------
// UCX internals tests

constexpr std::initializer_list<StatusCode> kStatusCodes = {
    StatusCode::OK,
    StatusCode::OutOfMemory,
    StatusCode::KeyError,
    StatusCode::TypeError,
    StatusCode::Invalid,
    StatusCode::IOError,
    StatusCode::CapacityError,
    StatusCode::IndexError,
    StatusCode::Cancelled,
    StatusCode::UnknownError,
    StatusCode::NotImplemented,
    StatusCode::SerializationError,
    StatusCode::RError,
    StatusCode::CodeGenError,
    StatusCode::ExpressionValidationError,
    StatusCode::ExecutionError,
    StatusCode::AlreadyExists,
};

constexpr std::initializer_list<FlightStatusCode> kFlightStatusCodes = {
    FlightStatusCode::Internal,     FlightStatusCode::TimedOut,
    FlightStatusCode::Cancelled,    FlightStatusCode::Unauthenticated,
    FlightStatusCode::Unauthorized, FlightStatusCode::Unavailable,
    FlightStatusCode::Failed,
};

class TestStatusDetail : public StatusDetail {
 public:
  const char* type_id() const override { return "test-status-detail"; }
  std::string ToString() const override { return "Custom status detail"; }
};

namespace transport {
namespace ucx {

static constexpr std::initializer_list<FrameType> kFrameTypes = {
    FrameType::kHeaders,     FrameType::kBuffer,     FrameType::kPayloadHeader,
    FrameType::kPayloadBody, FrameType::kDisconnect,
};

TEST(FrameHeader, Basics) {
  for (const auto frame_type : kFrameTypes) {
    FrameHeader header;
    ASSERT_OK(header.Set(frame_type, /*counter=*/42, /*body_size=*/65535));
    if (frame_type == FrameType::kDisconnect) {
      ASSERT_RAISES(Cancelled, Frame::ParseHeader(header.data(), header.size()));
    } else {
      ASSERT_OK_AND_ASSIGN(auto frame, Frame::ParseHeader(header.data(), header.size()));
      ASSERT_EQ(frame->type, frame_type);
      ASSERT_EQ(frame->counter, 42);
      ASSERT_EQ(frame->size, 65535);
    }
  }
}

TEST(FrameHeader, FrameType) {
  for (const auto frame_type : kFrameTypes) {
    ASSERT_LE(static_cast<int>(frame_type), static_cast<int>(FrameType::kMaxFrameType));
  }
}

TEST(HeadersFrame, Parse) {
  const char* data =
      ("\x00\x00\x00\x02\x00\x00\x00\x05\x00\x00\x00\x03x-foobar"
       "\x00\x00\x00\x05\x00\x00\x00\x01x-bin\x01");
  constexpr int64_t size = 34;

  {
    std::unique_ptr<Buffer> buffer(
        new Buffer(reinterpret_cast<const uint8_t*>(data), size));
    ASSERT_OK_AND_ASSIGN(auto headers, HeadersFrame::Parse(std::move(buffer)));
    ASSERT_OK_AND_ASSIGN(auto foo, headers.Get("x-foo"));
    ASSERT_EQ(foo, "bar");
    ASSERT_OK_AND_ASSIGN(auto bin, headers.Get("x-bin"));
    ASSERT_EQ(bin, "\x01");
  }
  {
    std::unique_ptr<Buffer> buffer(new Buffer(reinterpret_cast<const uint8_t*>(data), 3));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("expected number of headers"),
                                    HeadersFrame::Parse(std::move(buffer)));
  }
  {
    std::unique_ptr<Buffer> buffer(new Buffer(reinterpret_cast<const uint8_t*>(data), 7));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("expected length of key 1"),
                                    HeadersFrame::Parse(std::move(buffer)));
  }
  {
    std::unique_ptr<Buffer> buffer(
        new Buffer(reinterpret_cast<const uint8_t*>(data), 10));
    EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                    ::testing::HasSubstr("expected length of value 1"),
                                    HeadersFrame::Parse(std::move(buffer)));
  }
  {
    std::unique_ptr<Buffer> buffer(
        new Buffer(reinterpret_cast<const uint8_t*>(data), 12));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr("expected key 1 to have length 5, but only 0 bytes remain"),
        HeadersFrame::Parse(std::move(buffer)));
  }
  {
    std::unique_ptr<Buffer> buffer(
        new Buffer(reinterpret_cast<const uint8_t*>(data), 17));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid,
        ::testing::HasSubstr(
            "expected value 1 to have length 3, but only 0 bytes remain"),
        HeadersFrame::Parse(std::move(buffer)));
  }
}
}  // namespace ucx
}  // namespace transport

//------------------------------------------------------------
// Ad-hoc UCX-specific tests

class SimpleTestServer : public FlightServerBase {
 public:
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (request.path.size() > 0 && request.path[0] == "error") {
      return status_;
    }
    auto examples = ExampleFlightInfo();
    info->reset(new FlightInfo(examples[0]));
    return Status::OK();
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleIntBatches(&batches));
    auto batch_reader = std::make_shared<BatchIterator>(batches[0]->schema(), batches);
    *data_stream = std::make_unique<RecordBatchStream>(batch_reader);
    return Status::OK();
  }

  void set_error_status(Status st) { status_ = std::move(st); }

 private:
  Status status_;
};

class TestUcx : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme("ucx", "127.0.0.1", 0));
    ASSERT_OK(MakeServer<SimpleTestServer>(
        location, &server_, &client_,
        [](FlightServerOptions* options) { return Status::OK(); },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

TEST_F(TestUcx, GetFlightInfo) {
  auto descriptor = FlightDescriptor::Path({"foo", "bar"});
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK_AND_ASSIGN(info, client_->GetFlightInfo(descriptor));
  // Test that we can reuse the connection
  ASSERT_OK_AND_ASSIGN(info, client_->GetFlightInfo(descriptor));
}

TEST_F(TestUcx, SequentialClients) {
  ASSERT_OK_AND_ASSIGN(
      auto client2,
      FlightClient::Connect(server_->location(), FlightClientOptions::Defaults()));

  Ticket ticket{"a"};

  ASSERT_OK_AND_ASSIGN(auto stream1, client_->DoGet(ticket));
  ASSERT_OK_AND_ASSIGN(auto table1, stream1->ToTable());

  ASSERT_OK_AND_ASSIGN(auto stream2, client2->DoGet(ticket));
  ASSERT_OK_AND_ASSIGN(auto table2, stream2->ToTable());

  AssertTablesEqual(*table1, *table2);
}

TEST_F(TestUcx, ConcurrentClients) {
  ASSERT_OK_AND_ASSIGN(
      auto client2,
      FlightClient::Connect(server_->location(), FlightClientOptions::Defaults()));

  Ticket ticket{"a"};

  ASSERT_OK_AND_ASSIGN(auto stream1, client_->DoGet(ticket));
  ASSERT_OK_AND_ASSIGN(auto stream2, client2->DoGet(ticket));

  ASSERT_OK_AND_ASSIGN(auto table1, stream1->ToTable());
  ASSERT_OK_AND_ASSIGN(auto table2, stream2->ToTable());

  AssertTablesEqual(*table1, *table2);
}

TEST_F(TestUcx, Errors) {
  auto descriptor = FlightDescriptor::Path({"error", "bar"});
  auto* server = reinterpret_cast<SimpleTestServer*>(server_.get());
  for (const auto code : kStatusCodes) {
    if (code == StatusCode::OK) continue;

    Status expected(code, "Error message");
    server->set_error_status(expected);
    Status actual = client_->GetFlightInfo(descriptor).status();
    ASSERT_EQ(actual.code(), expected.code()) << actual.ToString();
    ASSERT_THAT(actual.message(), ::testing::HasSubstr("Error message"))
        << actual.ToString();

    // Attach a generic status detail
    {
      auto detail = std::make_shared<TestStatusDetail>();
      server->set_error_status(Status(code, "foo", detail));
      Status expected(code, "foo",
                      std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal,
                                                           detail->ToString()));
      Status actual = client_->GetFlightInfo(descriptor).status();
      ASSERT_EQ(actual.code(), expected.code()) << actual.ToString();
      ASSERT_THAT(actual.message(), ::testing::HasSubstr("foo")) << actual.ToString();
      ASSERT_THAT(actual.message(), ::testing::HasSubstr("Custom status detail"))
          << actual.ToString();
    }

    // Attach a Flight status detail
    for (const auto flight_code : kFlightStatusCodes) {
      Status expected(code, "Error message",
                      std::make_shared<FlightStatusDetail>(flight_code, "extra"));
      server->set_error_status(expected);
      Status actual = client_->GetFlightInfo(descriptor).status();
      ASSERT_EQ(actual.code(), expected.code()) << actual.ToString();
      ASSERT_THAT(actual.message(), ::testing::HasSubstr("Error message"))
          << actual.ToString();
    }
  }
}

TEST(TestUcxIpV6, DISABLED_IpV6Port) {
  // Also, disabled in CI as machines lack an IPv6 interface
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme("ucx", "[::1]", 0));

  std::unique_ptr<FlightServerBase> server(new SimpleTestServer());
  FlightServerOptions server_options(location);
  ASSERT_OK(server->Init(server_options));

  FlightClientOptions client_options = FlightClientOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto client,
                       FlightClient::Connect(server->location(), client_options));

  auto descriptor = FlightDescriptor::Path({"foo", "bar"});
  ASSERT_OK_AND_ASSIGN(auto info, client->GetFlightInfo(descriptor));
}

}  // namespace flight
}  // namespace arrow
