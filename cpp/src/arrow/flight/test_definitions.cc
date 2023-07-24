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

#include "arrow/flight/test_definitions.h"

#include <chrono>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/util.h"
#include "arrow/flight/api.h"
#include "arrow/flight/test_util.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"

#if defined(ARROW_CUDA)
#include "arrow/gpu/cuda_api.h"
#endif

namespace arrow {
namespace flight {

using arrow::internal::checked_cast;

//------------------------------------------------------------
// Tests of initialization/shutdown

void ConnectivityTest::TestGetPort() {
  std::unique_ptr<FlightServerBase> server = ExampleTestServer();

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  FlightServerOptions options(location);
  ASSERT_OK(server->Init(options));
  ASSERT_GT(server->port(), 0);
}
void ConnectivityTest::TestBuilderHook() {
  std::unique_ptr<FlightServerBase> server = ExampleTestServer();

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  FlightServerOptions options(location);
  bool builder_hook_run = false;
  options.builder_hook = [&builder_hook_run](void* builder) {
    ASSERT_NE(nullptr, builder);
    builder_hook_run = true;
  };
  ASSERT_OK(server->Init(options));
  ASSERT_TRUE(builder_hook_run);
  ASSERT_GT(server->port(), 0);
  ASSERT_OK(server->Shutdown());
}
void ConnectivityTest::TestShutdown() {
  // Regression test for ARROW-15181
  constexpr int kIterations = 10;
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  for (int i = 0; i < kIterations; i++) {
    std::unique_ptr<FlightServerBase> server = ExampleTestServer();

    FlightServerOptions options(location);
    ASSERT_OK(server->Init(options));
    ASSERT_GT(server->port(), 0);
    std::thread t([&]() { ASSERT_OK(server->Serve()); });
    ASSERT_OK(server->Shutdown());
    ASSERT_OK(server->Wait());
    t.join();
  }
}
void ConnectivityTest::TestShutdownWithDeadline() {
  std::unique_ptr<FlightServerBase> server = ExampleTestServer();

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  FlightServerOptions options(location);
  ASSERT_OK(server->Init(options));
  ASSERT_GT(server->port(), 0);

  auto deadline = std::chrono::system_clock::now() + std::chrono::microseconds(10);

  ASSERT_OK(server->Shutdown(&deadline));
  ASSERT_OK(server->Wait());
}
void ConnectivityTest::TestBrokenConnection() {
  std::unique_ptr<FlightServerBase> server = ExampleTestServer();
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  FlightServerOptions options(location);
  ASSERT_OK(server->Init(options));

  std::unique_ptr<FlightClient> client;
  ASSERT_OK_AND_ASSIGN(location,
                       Location::ForScheme(transport(), "127.0.0.1", server->port()));
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location));

  ASSERT_OK(server->Shutdown());
  ASSERT_OK(server->Wait());

  auto status = client->GetFlightInfo(FlightDescriptor::Command(""));
  ASSERT_NOT_OK(status);
  ASSERT_THAT(
      status.status().code(),
      ::testing::AnyOf(::arrow::StatusCode::IOError, ::arrow::StatusCode::UnknownError));
}

//------------------------------------------------------------
// Tests of data plane methods

void DataTest::SetUpTest() {
  server_ = ExampleTestServer();

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  FlightServerOptions options(location);
  ASSERT_OK(server_->Init(options));

  ASSERT_OK(ConnectClient());
}
void DataTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
}
Status DataTest::ConnectClient() {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        Location::ForScheme(transport(), "127.0.0.1", server_->port()));
  ARROW_ASSIGN_OR_RAISE(client_, FlightClient::Connect(location));
  return Status::OK();
}
void DataTest::CheckDoGet(
    const FlightDescriptor& descr, const RecordBatchVector& expected_batches,
    std::function<void(const std::vector<FlightEndpoint>&)> check_endpoints) {
  auto expected_schema = expected_batches[0]->schema();

  ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descr));
  check_endpoints(info->endpoints());

  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto schema, info->GetSchema(&dict_memo));
  AssertSchemaEqual(*expected_schema, *schema);

  // By convention, fetch the first endpoint
  Ticket ticket = info->endpoints()[0].ticket;
  CheckDoGet(ticket, expected_batches);
}
void DataTest::CheckDoGet(const Ticket& ticket,
                          const RecordBatchVector& expected_batches) {
  auto num_batches = static_cast<int>(expected_batches.size());
  ASSERT_GE(num_batches, 2);

  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(ticket));

  ASSERT_OK_AND_ASSIGN(auto stream2, client_->DoGet(ticket));
  ASSERT_OK_AND_ASSIGN(auto reader, MakeRecordBatchReader(std::move(stream2)));

  std::shared_ptr<RecordBatch> batch;
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
    ASSERT_OK(reader->ReadNext(&batch));
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_NE(nullptr, batch);
#if !defined(__MINGW32__)
    ASSERT_BATCHES_EQUAL(*expected_batches[i], *chunk.data);
    ASSERT_BATCHES_EQUAL(*expected_batches[i], *batch);
#else
    // In MINGW32, the following code does not have the reproducibility at the LSB
    // even when this is called twice with the same seed.
    // As a workaround, use approxEqual
    //   /* from GenerateTypedData in random.cc */
    //   std::default_random_engine rng(seed);  // seed = 282475250
    //   std::uniform_real_distribution<double> dist;
    //   std::generate(data, data + n,          // n = 10
    //                 [&dist, &rng] { return static_cast<ValueType>(dist(rng)); });
    //   /* data[1] = 0x40852cdfe23d3976 or 0x40852cdfe23d3975 */
    ASSERT_BATCHES_APPROX_EQUAL(*expected_batches[i], *chunk.data);
    ASSERT_BATCHES_APPROX_EQUAL(*expected_batches[i], *batch);
#endif
  }

  // Stream exhausted
  ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
  ASSERT_OK(reader->ReadNext(&batch));
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_EQ(nullptr, batch);
}

void DataTest::TestDoGetInts() {
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleIntBatches(&expected_batches));

  auto check_endpoints = [](const std::vector<FlightEndpoint>& endpoints) {
    // Two endpoints in the example FlightInfo
    ASSERT_EQ(2, endpoints.size());
    ASSERT_EQ(Ticket{"ticket-ints-1"}, endpoints[0].ticket);
  };

  CheckDoGet(descr, expected_batches, check_endpoints);
}
void DataTest::TestDoGetFloats() {
  auto descr = FlightDescriptor::Path({"examples", "floats"});
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleFloatBatches(&expected_batches));

  auto check_endpoints = [](const std::vector<FlightEndpoint>& endpoints) {
    // One endpoint in the example FlightInfo
    ASSERT_EQ(1, endpoints.size());
    ASSERT_EQ(Ticket{"ticket-floats-1"}, endpoints[0].ticket);
  };

  CheckDoGet(descr, expected_batches, check_endpoints);
}
void DataTest::TestDoGetDicts() {
  auto descr = FlightDescriptor::Path({"examples", "dicts"});
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleDictBatches(&expected_batches));

  auto check_endpoints = [](const std::vector<FlightEndpoint>& endpoints) {
    // One endpoint in the example FlightInfo
    ASSERT_EQ(1, endpoints.size());
    ASSERT_EQ(Ticket{"ticket-dicts-1"}, endpoints[0].ticket);
  };

  CheckDoGet(descr, expected_batches, check_endpoints);
}
// Ensure clients are configured to allow large messages by default
// Tests a 32 MiB batch
void DataTest::TestDoGetLargeBatch() {
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleLargeBatches(&expected_batches));
  Ticket ticket{"ticket-large-batch-1"};
  CheckDoGet(ticket, expected_batches);
}
// Ensure FlightDataStream/RecordBatchStream::Close errors are propagated
void DataTest::TestFlightDataStreamError() {
  Ticket ticket{"ticket-stream-error"};

  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(ticket));
  Status status;
  while (true) {
    FlightStreamChunk chunk;
    status = stream->Next().Value(&chunk);
    if (!chunk.data) break;
    if (!status.ok()) break;
  }
  EXPECT_RAISES_WITH_MESSAGE_THAT(IOError, ::testing::HasSubstr("Expected error"),
                                  status);
}
void DataTest::TestOverflowServerBatch() {
  // Regression test for ARROW-13253
  // N.B. this is rather a slow and memory-hungry test
  {
    // DoGet: check for overflow on large batch
    Ticket ticket{"ARROW-13253-DoGet-Batch"};
    std::unique_ptr<FlightStreamReader> stream;
    ASSERT_OK_AND_ASSIGN(stream, client_->DoGet(ticket));
    FlightStreamChunk chunk;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Cannot send record batches exceeding 2GiB yet"),
        stream->Next());
  }
  {
    // DoExchange: check for overflow on large batch from server
    auto descr = FlightDescriptor::Command("large_batch");
    ASSERT_OK_AND_ASSIGN(auto do_exchange_result, client_->DoExchange(descr));
    RecordBatchVector batches;
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Cannot send record batches exceeding 2GiB yet"),
        do_exchange_result.reader->ToRecordBatches().Value(&batches));
    ARROW_UNUSED(do_exchange_result.writer->Close());
  }
}
void DataTest::TestOverflowClientBatch() {
  ASSERT_OK_AND_ASSIGN(auto batch, VeryLargeBatch());
  {
    // DoPut: check for overflow on large batch
    auto descr = FlightDescriptor::Path({""});
    ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(descr, batch->schema()));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Cannot send record batches exceeding 2GiB yet"),
        do_put_result.writer->WriteRecordBatch(*batch));
    ASSERT_OK(do_put_result.writer->Close());
  }
  {
    // DoExchange: check for overflow on large batch from client
    auto descr = FlightDescriptor::Command("counter");
    ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
    auto writer = std::move(exchange.writer);
    ASSERT_OK(writer->Begin(batch->schema()));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Cannot send record batches exceeding 2GiB yet"),
        writer->WriteRecordBatch(*batch));
    ASSERT_OK(writer->Close());
  }
}
void DataTest::TestDoExchange() {
  auto descr = FlightDescriptor::Command("counter");
  RecordBatchVector batches;
  auto a1 = ArrayFromJSON(int32(), "[4, 5, 6, null]");
  auto schema = arrow::schema({field("f1", a1->type())});
  batches.push_back(RecordBatch::Make(schema, a1->length(), {a1}));
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  ASSERT_OK(writer->Begin(schema));
  for (const auto& batch : batches) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_NE(nullptr, chunk.app_metadata);
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_EQ("1", chunk.app_metadata->ToString());
  ASSERT_OK_AND_ASSIGN(auto server_schema, reader->GetSchema());
  AssertSchemaEqual(schema, server_schema);
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(chunk, reader->Next());
    ASSERT_BATCHES_EQUAL(*batch, *chunk.data);
  }
  ASSERT_OK(writer->Close());
}
// Test pure-metadata DoExchange to ensure nothing blocks waiting for
// schema messages
void DataTest::TestDoExchangeNoData() {
  auto descr = FlightDescriptor::Command("counter");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_NE(nullptr, chunk.app_metadata);
  ASSERT_EQ("0", chunk.app_metadata->ToString());
  ASSERT_OK(writer->Close());
}
// Test sending a schema without any data, as this hits an edge case
// in the client-side writer.
void DataTest::TestDoExchangeWriteOnlySchema() {
  auto descr = FlightDescriptor::Command("counter");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  auto schema = arrow::schema({field("f1", arrow::int32())});
  ASSERT_OK(writer->Begin(schema));
  ASSERT_OK(writer->WriteMetadata(Buffer::FromString("foo")));
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_NE(nullptr, chunk.app_metadata);
  ASSERT_EQ("0", chunk.app_metadata->ToString());
  ASSERT_OK(writer->Close());
}
// Emulate DoGet
void DataTest::TestDoExchangeGet() {
  auto descr = FlightDescriptor::Command("get");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  ASSERT_OK_AND_ASSIGN(auto server_schema, reader->GetSchema());
  AssertSchemaEqual(*ExampleIntSchema(), *server_schema);
  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    AssertBatchesEqual(*batch, *chunk.data);
  }
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_EQ(nullptr, chunk.app_metadata);
  ASSERT_OK(writer->Close());
}
// Emulate DoPut
void DataTest::TestDoExchangePut() {
  auto descr = FlightDescriptor::Command("put");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  ASSERT_OK(writer->Begin(ExampleIntSchema()));
  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));
  for (const auto& batch : batches) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_NE(nullptr, chunk.app_metadata);
  AssertBufferEqual(*chunk.app_metadata, "done");
  ASSERT_OK_AND_ASSIGN(chunk, reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_EQ(nullptr, chunk.app_metadata);
  ASSERT_OK(writer->Close());
}
// Test the echo server
void DataTest::TestDoExchangeEcho() {
  auto descr = FlightDescriptor::Command("echo");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);
  ASSERT_OK(writer->Begin(ExampleIntSchema()));
  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));
  for (const auto& batch : batches) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_EQ(nullptr, chunk.app_metadata);
    AssertBatchesEqual(*batch, *chunk.data);
  }
  for (int i = 0; i < 10; i++) {
    const auto buf = Buffer::FromString(std::to_string(i));
    ASSERT_OK(writer->WriteMetadata(buf));
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_EQ(nullptr, chunk.data);
    ASSERT_NE(nullptr, chunk.app_metadata);
    AssertBufferEqual(*buf, *chunk.app_metadata);
  }
  int index = 0;
  for (const auto& batch : batches) {
    const auto buf = Buffer::FromString(std::to_string(index));
    ASSERT_OK(writer->WriteWithMetadata(*batch, buf));
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_NE(nullptr, chunk.app_metadata);
    AssertBatchesEqual(*batch, *chunk.data);
    AssertBufferEqual(*buf, *chunk.app_metadata);
    index++;
  }
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_EQ(nullptr, chunk.app_metadata);
  ASSERT_OK(writer->Close());
}
// Test interleaved reading/writing
void DataTest::TestDoExchangeTotal() {
  auto descr = FlightDescriptor::Command("total");
  std::unique_ptr<FlightStreamReader> reader;
  std::unique_ptr<FlightStreamWriter> writer;
  {
    auto a1 = ArrayFromJSON(arrow::int32(), "[4, 5, 6, null]");
    auto schema = arrow::schema({field("f1", a1->type())});
    // XXX: as noted in flight/client.cc, Begin() is lazy and the
    // schema message won't be written until some data is also
    // written. There's also timing issues; hence we check each status
    // here.
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Field is not INT64: f1"), ([&]() {
          ARROW_ASSIGN_OR_RAISE(auto exchange, client_->DoExchange(descr));
          reader = std::move(exchange.reader);
          writer = std::move(exchange.writer);
          RETURN_NOT_OK(writer->Begin(schema));
          auto batch = RecordBatch::Make(schema, /* num_rows */ 4, {a1});
          RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
          return writer->Close();
        })());
  }
  {
    auto a1 = ArrayFromJSON(arrow::int64(), "[1, 2, null, 3]");
    auto a2 = ArrayFromJSON(arrow::int64(), "[null, 4, 5, 6]");
    auto schema = arrow::schema({field("f1", a1->type()), field("f2", a2->type())});
    ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
    reader = std::move(exchange.reader);
    writer = std::move(exchange.writer);
    ASSERT_OK(writer->Begin(schema));
    auto batch = RecordBatch::Make(schema, /* num_rows */ 4, {a1, a2});
    ASSERT_OK(writer->WriteRecordBatch(*batch));
    ASSERT_OK_AND_ASSIGN(auto server_schema, reader->GetSchema());
    AssertSchemaEqual(*schema, *server_schema);

    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    auto expected1 = RecordBatch::Make(
        schema, /* num_rows */ 1,
        {ArrayFromJSON(arrow::int64(), "[6]"), ArrayFromJSON(arrow::int64(), "[15]")});
    AssertBatchesEqual(*expected1, *chunk.data);

    ASSERT_OK(writer->WriteRecordBatch(*batch));
    ASSERT_OK_AND_ASSIGN(chunk, reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    auto expected2 = RecordBatch::Make(
        schema, /* num_rows */ 1,
        {ArrayFromJSON(arrow::int64(), "[12]"), ArrayFromJSON(arrow::int64(), "[30]")});
    AssertBatchesEqual(*expected2, *chunk.data);

    ASSERT_OK(writer->Close());
  }
}
// Ensure server errors get propagated no matter what we try
void DataTest::TestDoExchangeError() {
  auto descr = FlightDescriptor::Command("error");
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightStreamReader> reader;
  {
    ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
    writer = std::move(exchange.writer);
    reader = std::move(exchange.reader);
    auto status = writer->Close();
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Expected error"), writer->Close());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
    writer = std::move(exchange.writer);
    reader = std::move(exchange.reader);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Expected error"), reader->Next());
    ARROW_UNUSED(writer->Close());
  }
  {
    ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
    writer = std::move(exchange.writer);
    reader = std::move(exchange.reader);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Expected error"), reader->GetSchema());
    ARROW_UNUSED(writer->Close());
  }
  // writer->Begin isn't tested here because, as noted in client.cc,
  // OpenRecordBatchWriter lazily writes the initial message - hence
  // Begin() won't fail. Additionally, transports are allowed to
  // buffer writes - a write won't immediately fail even if the server
  // would immediately return an error.
}
void DataTest::TestDoExchangeConcurrency() {
  // Ensure that we can do reads/writes on separate threads
  auto descr = FlightDescriptor::Command("echo");
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);

  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));
  ASSERT_OK(writer->Begin(ExampleIntSchema()));

  std::thread reader_thread([&reader, &batches]() {
    for (size_t i = 0; i < batches.size(); i++) {
      ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
      ASSERT_NE(nullptr, chunk.data);
      ASSERT_EQ(nullptr, chunk.app_metadata);
      AssertBatchesEqual(*batches[i], *chunk.data);
    }
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    ASSERT_EQ(nullptr, chunk.data);
    ASSERT_EQ(nullptr, chunk.app_metadata);
  });

  for (const auto& batch : batches) {
    ASSERT_OK(writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->DoneWriting());
  reader_thread.join();
  ASSERT_OK(writer->Close());
}
void DataTest::TestDoExchangeUndrained() {
  // Ensure if the application doesn't drain all messages, that the
  // server/transport does

  auto descr = FlightDescriptor::Command("TestUndrained");
  auto schema = arrow::schema({arrow::field("ints", int64())});
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamReader> reader = std::move(exchange.reader);
  std::unique_ptr<FlightStreamWriter> writer = std::move(exchange.writer);

  auto batch = RecordBatchFromJSON(schema, "[[1], [2], [3], [4]]");
  ASSERT_OK(writer->Begin(schema));
  // These calls may or may not fail depending on how quickly the
  // transport reacts, whether it batches, writes, etc.
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ASSERT_OK(writer->Close());

  // We should be able to make another call
  TestDoExchangeGet();
}
void DataTest::TestIssue5095() {
  // Make sure the server-side error message is reflected to the
  // client
  Ticket ticket1{"ARROW-5095-fail"};
  Status status = client_->DoGet(ticket1).status();
  ASSERT_RAISES(UnknownError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Server-side error"));

  Ticket ticket2{"ARROW-5095-success"};
  status = client_->DoGet(ticket2).status();
  ASSERT_RAISES(KeyError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("No data"));
}

//------------------------------------------------------------
// Specific tests for DoPut

static constexpr char kExpectedMetadata[] = "foo bar";

class DoPutTestServer : public FlightServerBase {
 public:
  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    descriptor_ = reader->descriptor();

    if (descriptor_.type == FlightDescriptor::DescriptorType::CMD) {
      if (descriptor_.cmd == "TestUndrained") {
        // Don't read all the messages
        return Status::OK();
      }
    }

    int counter = 0;
    FlightStreamChunk chunk;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
      if (!chunk.data) break;
      if (counter % 2 == 1) {
        if (!chunk.app_metadata) {
          return Status::Invalid("Expected app_metadata");
        } else if (chunk.app_metadata->ToString() != std::to_string(counter)) {
          return Status::Invalid("Expected app_metadata to be ", counter, " but got ",
                                 chunk.app_metadata->ToString());
        }
      } else if (chunk.app_metadata) {
        return Status::Invalid("Expected no app_metadata");
      }
      batches_.push_back(std::move(chunk.data));
      auto buffer = Buffer::FromString(std::to_string(counter));
      RETURN_NOT_OK(writer->WriteMetadata(*buffer));
      counter++;
    }

    // Expect a metadata-only message
    if (!chunk.app_metadata) {
      return Status::Invalid("Expected app_metadata at end of stream (#1)");
    } else if (chunk.app_metadata->ToString() != kExpectedMetadata) {
      return Status::Invalid("Expected app_metadata to be ", kExpectedMetadata,
                             " but got ", chunk.app_metadata->ToString());
    }

    return Status::OK();
  }

 protected:
  FlightDescriptor descriptor_;
  RecordBatchVector batches_;

  friend class DoPutTest;
};

void DoPutTest::SetUpTest() {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  ASSERT_OK(MakeServer<DoPutTestServer>(
      location, &server_, &client_,
      [](FlightServerOptions* options) { return Status::OK(); },
      [](FlightClientOptions* options) { return Status::OK(); }));
}
void DoPutTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
  reinterpret_cast<DoPutTestServer*>(server_.get())->batches_.clear();
}
void DoPutTest::CheckBatches(const FlightDescriptor& expected_descriptor,
                             const RecordBatchVector& expected_batches) {
  auto* do_put_server = (DoPutTestServer*)server_.get();
  ASSERT_EQ(do_put_server->descriptor_, expected_descriptor);
  ASSERT_EQ(do_put_server->batches_.size(), expected_batches.size());
  for (size_t i = 0; i < expected_batches.size(); ++i) {
    ASSERT_BATCHES_EQUAL(*do_put_server->batches_[i], *expected_batches[i]);
  }
}
void DoPutTest::CheckDoPut(const FlightDescriptor& descr,
                           const std::shared_ptr<Schema>& schema,
                           const RecordBatchVector& batches) {
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(descr, schema));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
  std::unique_ptr<FlightMetadataReader> reader = std::move(do_put_result.reader);

  // Ensure that the reader can be used independently of the writer
  std::thread reader_thread([&reader, &batches]() {
    for (size_t i = 0; i < batches.size(); i++) {
      std::shared_ptr<Buffer> out;
      ASSERT_OK(reader->ReadMetadata(&out));
    }
  });

  int64_t counter = 0;
  for (const auto& batch : batches) {
    if (counter % 2 == 0) {
      ASSERT_OK(writer->WriteRecordBatch(*batch));
    } else {
      auto buffer = Buffer::FromString(std::to_string(counter));
      ASSERT_OK(writer->WriteWithMetadata(*batch, std::move(buffer)));
    }
    counter++;
  }
  // Write a metadata-only message
  ASSERT_OK(writer->WriteMetadata(Buffer::FromString(kExpectedMetadata)));
  ASSERT_OK(writer->DoneWriting());
  reader_thread.join();
  ASSERT_OK(writer->Close());

  CheckBatches(descr, batches);
}

void DoPutTest::TestInts() {
  auto descr = FlightDescriptor::Path({"ints"});
  RecordBatchVector batches;
  auto a0 = ArrayFromJSON(int8(), "[0, 1, 127, -128, null]");
  auto a1 = ArrayFromJSON(uint8(), "[0, 1, 127, 255, null]");
  auto a2 = ArrayFromJSON(int16(), "[0, 258, 32767, -32768, null]");
  auto a3 = ArrayFromJSON(uint16(), "[0, 258, 32767, 65535, null]");
  auto a4 = ArrayFromJSON(int32(), "[0, 65538, 2147483647, -2147483648, null]");
  auto a5 = ArrayFromJSON(uint32(), "[0, 65538, 2147483647, 4294967295, null]");
  auto a6 = ArrayFromJSON(
      int64(), "[0, 4294967298, 9223372036854775807, -9223372036854775808, null]");
  auto a7 = ArrayFromJSON(
      uint64(), "[0, 4294967298, 9223372036854775807, 18446744073709551615, null]");
  auto schema = arrow::schema({field("f0", a0->type()), field("f1", a1->type()),
                               field("f2", a2->type()), field("f3", a3->type()),
                               field("f4", a4->type()), field("f5", a5->type()),
                               field("f6", a6->type()), field("f7", a7->type())});
  batches.push_back(
      RecordBatch::Make(schema, a0->length(), {a0, a1, a2, a3, a4, a5, a6, a7}));

  CheckDoPut(descr, schema, batches);
}

void DoPutTest::TestFloats() {
  auto descr = FlightDescriptor::Path({"floats"});
  RecordBatchVector batches;
  auto a0 = ArrayFromJSON(float32(), "[0, 1.2, -3.4, 5.6, null]");
  auto a1 = ArrayFromJSON(float64(), "[0, 1.2, -3.4, 5.6, null]");
  auto schema = arrow::schema({field("f0", a0->type()), field("f1", a1->type())});
  batches.push_back(RecordBatch::Make(schema, a0->length(), {a0, a1}));

  CheckDoPut(descr, schema, batches);
}

void DoPutTest::TestEmptyBatch() {
  // Sending and receiving a 0-sized batch shouldn't fail
  auto descr = FlightDescriptor::Path({"ints"});
  RecordBatchVector batches;
  auto a1 = ArrayFromJSON(int32(), "[]");
  auto schema = arrow::schema({field("f1", a1->type())});
  batches.push_back(RecordBatch::Make(schema, a1->length(), {a1}));

  CheckDoPut(descr, schema, batches);
}

void DoPutTest::TestDicts() {
  auto descr = FlightDescriptor::Path({"dicts"});
  RecordBatchVector batches;
  auto dict_values = ArrayFromJSON(utf8(), "[\"foo\", \"bar\", \"quux\"]");
  auto ty = dictionary(int8(), dict_values->type());
  auto schema = arrow::schema({field("f1", ty)});
  // Make several batches
  for (const char* json : {"[1, 0, 1]", "[null]", "[null, 1]"}) {
    auto indices = ArrayFromJSON(int8(), json);
    auto dict_array = std::make_shared<DictionaryArray>(ty, indices, dict_values);
    batches.push_back(RecordBatch::Make(schema, dict_array->length(), {dict_array}));
  }

  CheckDoPut(descr, schema, batches);
}

// Ensure the server is configured to allow large messages by default
// Tests a 32 MiB batch
void DoPutTest::TestLargeBatch() {
  auto descr = FlightDescriptor::Path({"large-batches"});
  auto schema = ExampleLargeSchema();
  RecordBatchVector batches;
  ASSERT_OK(ExampleLargeBatches(&batches));
  CheckDoPut(descr, schema, batches);
}

void DoPutTest::TestSizeLimit() {
  const int64_t size_limit = 4096;
  ASSERT_OK_AND_ASSIGN(auto location,
                       Location::ForScheme(transport(), "127.0.0.1", server_->port()));
  auto client_options = FlightClientOptions::Defaults();
  client_options.write_size_limit_bytes = size_limit;
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location, client_options));

  auto descr = FlightDescriptor::Command("simple");
  // Batch is too large to fit in one message
  auto schema = arrow::schema({field("f1", arrow::int64())});
  auto batch = arrow::ConstantArrayGenerator::Zeroes(768, schema);
  auto batch1 = batch->Slice(0, 384);
  auto batch2 = batch->Slice(384);

  ASSERT_OK_AND_ASSIGN(auto do_put_result, client->DoPut(descr, schema));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
  std::unique_ptr<FlightMetadataReader> reader = std::move(do_put_result.reader);

  // Large batch will exceed the limit
  const auto status = writer->WriteRecordBatch(*batch);
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("exceeded soft limit"),
                                  status);
  auto detail = FlightWriteSizeStatusDetail::UnwrapStatus(status);
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(size_limit, detail->limit());
  ASSERT_GT(detail->actual(), size_limit);

  // But we can retry with smaller batches
  ASSERT_OK(writer->WriteRecordBatch(*batch1));
  ASSERT_OK(writer->WriteWithMetadata(*batch2, Buffer::FromString("1")));

  // Write a metadata-only message
  ASSERT_OK(writer->WriteMetadata(Buffer::FromString(kExpectedMetadata)));

  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK(writer->Close());
  CheckBatches(descr, {batch1, batch2});
}
void DoPutTest::TestUndrained() {
  // Ensure if the application doesn't drain all messages, that the
  // server/transport does

  auto descr = FlightDescriptor::Command("TestUndrained");
  auto schema = arrow::schema({arrow::field("ints", int64())});
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(descr, schema));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
  std::unique_ptr<FlightMetadataReader> reader = std::move(do_put_result.reader);
  auto batch = RecordBatchFromJSON(schema, "[[1], [2], [3], [4]]");
  // These calls may or may not fail depending on how quickly the
  // transport reacts, whether it batches, writes, etc.
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ARROW_UNUSED(writer->WriteRecordBatch(*batch));
  ASSERT_OK(writer->Close());

  // We should be able to make another call
  CheckDoPut(FlightDescriptor::Command("foo"), schema, {batch, batch});
}

//------------------------------------------------------------
// Test app_metadata in data plane methods

Status AppMetadataTestServer::DoGet(const ServerCallContext& context,
                                    const Ticket& request,
                                    std::unique_ptr<FlightDataStream>* data_stream) {
  RecordBatchVector batches;
  if (request.ticket == "dicts") {
    RETURN_NOT_OK(ExampleDictBatches(&batches));
  } else if (request.ticket == "floats") {
    RETURN_NOT_OK(ExampleFloatBatches(&batches));
  } else {
    RETURN_NOT_OK(ExampleIntBatches(&batches));
  }
  ARROW_ASSIGN_OR_RAISE(auto batch_reader, RecordBatchReader::Make(batches));
  *data_stream = std::make_unique<NumberingStream>(
      std::make_unique<RecordBatchStream>(batch_reader));
  return Status::OK();
}
Status AppMetadataTestServer::DoPut(const ServerCallContext& context,
                                    std::unique_ptr<FlightMessageReader> reader,
                                    std::unique_ptr<FlightMetadataWriter> writer) {
  FlightStreamChunk chunk;
  int counter = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
    if (chunk.data == nullptr) break;
    if (chunk.app_metadata == nullptr) {
      return Status::Invalid("Expected application metadata to be provided");
    }
    if (std::to_string(counter) != chunk.app_metadata->ToString()) {
      return Status::Invalid("Expected metadata value: " + std::to_string(counter) +
                             " but got: " + chunk.app_metadata->ToString());
    }
    auto metadata = Buffer::FromString(std::to_string(counter));
    RETURN_NOT_OK(writer->WriteMetadata(*metadata));
    counter++;
  }
  return Status::OK();
}

void AppMetadataTest::SetUpTest() {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  ASSERT_OK(MakeServer<AppMetadataTestServer>(
      location, &server_, &client_,
      [](FlightServerOptions* options) { return Status::OK(); },
      [](FlightClientOptions* options) { return Status::OK(); }));
}
void AppMetadataTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
}
void AppMetadataTest::TestDoGet() {
  Ticket ticket{""};
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FlightStreamReader> stream,
                       client_->DoGet(ticket));

  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleIntBatches(&expected_batches));

  auto num_batches = static_cast<int>(expected_batches.size());
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_NE(nullptr, chunk.app_metadata);
    ASSERT_BATCHES_EQUAL(*expected_batches[i], *chunk.data);
    ASSERT_EQ(std::to_string(i), chunk.app_metadata->ToString());
  }
  ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
  ASSERT_EQ(nullptr, chunk.data);
}
// Test dictionaries. This tests a corner case in the reader:
// dictionary batches come in between the schema and the first record
// batch, so the server must take care to read application metadata
// from the record batch, and not one of the dictionary batches.
void AppMetadataTest::TestDoGetDictionaries() {
  Ticket ticket{"dicts"};
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<FlightStreamReader> stream,
                       client_->DoGet(ticket));

  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleDictBatches(&expected_batches));

  auto num_batches = static_cast<int>(expected_batches.size());
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_NE(nullptr, chunk.app_metadata);
    ASSERT_BATCHES_EQUAL(*expected_batches[i], *chunk.data);
    ASSERT_EQ(std::to_string(i), chunk.app_metadata->ToString());
  }
  ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
  ASSERT_EQ(nullptr, chunk.data);
}
void AppMetadataTest::TestDoPut() {
  std::shared_ptr<Schema> schema = ExampleIntSchema();
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(FlightDescriptor{}, schema));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);

  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleIntBatches(&expected_batches));

  std::shared_ptr<RecordBatch> chunk;
  std::shared_ptr<Buffer> metadata;
  auto num_batches = static_cast<int>(expected_batches.size());
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK(writer->WriteWithMetadata(*expected_batches[i],
                                        Buffer::FromString(std::to_string(i))));
  }
  // Transports may behave unpredictably if streams are not
  // drained. So explicitly close to see if the transport misbehaves
  // (e.g. gRPC will hang if the Flight transport layer doesn't drain
  // messages)
  ASSERT_OK(writer->Close());
}
// Test DoPut() with dictionaries. This tests a corner case in the
// server-side reader; see DoGetDictionaries above.
void AppMetadataTest::TestDoPutDictionaries() {
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleDictBatches(&expected_batches));
  // ARROW-8749: don't get the schema via ExampleDictSchema because
  // DictionaryMemo uses field addresses to determine whether it's
  // seen a field before. Hence, if we use a schema that is different
  // (identity-wise) than the schema of the first batch we write,
  // we'll end up generating a duplicate set of dictionaries that
  // confuses the reader.
  ASSERT_OK_AND_ASSIGN(auto do_put_result,
                       client_->DoPut(FlightDescriptor{}, expected_batches[0]->schema()));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);

  std::shared_ptr<RecordBatch> chunk;
  std::shared_ptr<Buffer> metadata;
  auto num_batches = static_cast<int>(expected_batches.size());
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK(writer->WriteWithMetadata(*expected_batches[i],
                                        Buffer::FromString(std::to_string(i))));
  }
  ASSERT_OK(writer->Close());
}
void AppMetadataTest::TestDoPutReadMetadata() {
  std::shared_ptr<Schema> schema = ExampleIntSchema();
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(FlightDescriptor{}, schema));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
  std::unique_ptr<FlightMetadataReader> reader = std::move(do_put_result.reader);

  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleIntBatches(&expected_batches));

  std::shared_ptr<RecordBatch> chunk;
  std::shared_ptr<Buffer> metadata;
  auto num_batches = static_cast<int>(expected_batches.size());
  for (int i = 0; i < num_batches; ++i) {
    ASSERT_OK(writer->WriteWithMetadata(*expected_batches[i],
                                        Buffer::FromString(std::to_string(i))));
    ASSERT_OK(reader->ReadMetadata(&metadata));
    ASSERT_NE(nullptr, metadata);
    ASSERT_EQ(std::to_string(i), metadata->ToString());
  }
  // As opposed to DoPutDrainMetadata, now we've read the messages, so
  // make sure this still closes as expected.
  ASSERT_OK(writer->Close());
}

//------------------------------------------------------------
// Test IPC options in data plane methods

class IpcOptionsTestServer : public FlightServerBase {
  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    RecordBatchVector batches;
    RETURN_NOT_OK(ExampleNestedBatches(&batches));
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make(batches));
    *data_stream = std::make_unique<RecordBatchStream>(reader);
    return Status::OK();
  }

  // Just echo the number of batches written. The client will try to
  // call this method with different write options set.
  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    int counter = 0;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, reader->Next());
      if (chunk.data == nullptr) break;
      counter++;
    }
    auto metadata = Buffer::FromString(std::to_string(counter));
    return writer->WriteMetadata(*metadata);
  }

  // Echo client data, but with write options set to limit the nesting
  // level.
  Status DoExchange(const ServerCallContext& context,
                    std::unique_ptr<FlightMessageReader> reader,
                    std::unique_ptr<FlightMessageWriter> writer) override {
    auto options = ipc::IpcWriteOptions::Defaults();
    options.max_recursion_depth = 1;
    bool begun = false;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, reader->Next());
      if (!chunk.data && !chunk.app_metadata) {
        break;
      }
      if (!begun && chunk.data) {
        begun = true;
        RETURN_NOT_OK(writer->Begin(chunk.data->schema(), options));
      }
      if (chunk.data && chunk.app_metadata) {
        RETURN_NOT_OK(writer->WriteWithMetadata(*chunk.data, chunk.app_metadata));
      } else if (chunk.data) {
        RETURN_NOT_OK(writer->WriteRecordBatch(*chunk.data));
      } else if (chunk.app_metadata) {
        RETURN_NOT_OK(writer->WriteMetadata(chunk.app_metadata));
      }
    }
    return Status::OK();
  }
};

void IpcOptionsTest::SetUpTest() {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  ASSERT_OK(MakeServer<IpcOptionsTestServer>(
      location, &server_, &client_,
      [](FlightServerOptions* options) { return Status::OK(); },
      [](FlightClientOptions* options) { return Status::OK(); }));
}
void IpcOptionsTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
}
void IpcOptionsTest::TestDoGetReadOptions() {
  // Call DoGet, but with a very low read nesting depth set to fail the call.
  Ticket ticket{""};
  auto options = FlightCallOptions();
  options.read_options.max_recursion_depth = 1;
  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK_AND_ASSIGN(stream, client_->DoGet(options, ticket));
  ASSERT_RAISES(Invalid, stream->Next());
}
void IpcOptionsTest::TestDoPutWriteOptions() {
  // Call DoPut, but with a very low write nesting depth set to fail the call.
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  RecordBatchVector expected_batches;
  ASSERT_OK(ExampleNestedBatches(&expected_batches));

  auto options = FlightCallOptions();
  options.write_options.max_recursion_depth = 1;
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(options, FlightDescriptor{},
                                                          expected_batches[0]->schema()));
  for (const auto& batch : expected_batches) {
    ASSERT_RAISES(Invalid, do_put_result.writer->WriteRecordBatch(*batch));
  }
}
void IpcOptionsTest::TestDoExchangeClientWriteOptions() {
  // Call DoExchange and write nested data, but with a very low nesting depth set to
  // fail the call.
  auto options = FlightCallOptions();
  options.write_options.max_recursion_depth = 1;
  auto descr = FlightDescriptor::Command("");
  ASSERT_OK_AND_ASSIGN(auto do_exchange_result, client_->DoExchange(options, descr));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_exchange_result.writer);
  RecordBatchVector batches;
  ASSERT_OK(ExampleNestedBatches(&batches));
  ASSERT_OK(writer->Begin(batches[0]->schema()));
  for (const auto& batch : batches) {
    ASSERT_RAISES(Invalid, writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK(writer->Close());
}
void IpcOptionsTest::TestDoExchangeClientWriteOptionsBegin() {
  // Call DoExchange and write nested data, but with a very low nesting depth set to
  // fail the call. Here the options are set explicitly when we write data and not in the
  // call options.
  auto descr = FlightDescriptor::Command("");
  ASSERT_OK_AND_ASSIGN(auto do_exchange_result, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_exchange_result.writer);
  RecordBatchVector batches;
  ASSERT_OK(ExampleNestedBatches(&batches));
  auto options = ipc::IpcWriteOptions::Defaults();
  options.max_recursion_depth = 1;
  ASSERT_OK(writer->Begin(batches[0]->schema(), options));
  for (const auto& batch : batches) {
    ASSERT_RAISES(Invalid, writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(writer->DoneWriting());
  ASSERT_OK(writer->Close());
}
void IpcOptionsTest::TestDoExchangeServerWriteOptions() {
  // Call DoExchange and write nested data, but with a very low nesting depth set to fail
  // the call. (The low nesting depth is set on the server side.)
  auto descr = FlightDescriptor::Command("");
  ASSERT_OK_AND_ASSIGN(auto do_exchange_result, client_->DoExchange(descr));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_exchange_result.writer);
  RecordBatchVector batches;
  ASSERT_OK(ExampleNestedBatches(&batches));
  ASSERT_OK(writer->Begin(batches[0]->schema()));
  FlightStreamChunk chunk;
  ASSERT_OK(writer->WriteRecordBatch(*batches[0]));
  ASSERT_OK(writer->DoneWriting());
  ASSERT_RAISES(Invalid, writer->Close());
}

//------------------------------------------------------------
// Test CUDA memory in data plane methods

#if defined(ARROW_CUDA)

Status CheckBuffersOnDevice(const Array& array, const Device& device) {
  if (array.num_fields() != 0) {
    return Status::NotImplemented("Nested arrays");
  }
  for (const auto& buffer : array.data()->buffers) {
    if (!buffer) continue;
    if (!buffer->device()->Equals(device)) {
      return Status::Invalid("Expected buffer on device: ", device.ToString(),
                             ". Was allocated on device: ", buffer->device()->ToString());
    }
  }
  return Status::OK();
}

Status CheckBuffersOnDevice(const RecordBatch& batch, const Device& device) {
  for (const auto& column : batch.columns()) {
    RETURN_NOT_OK(CheckBuffersOnDevice(*column, device));
  }
  return Status::OK();
}

// Copy a record batch to host memory.
arrow::Result<std::shared_ptr<RecordBatch>> CopyBatchToHost(const RecordBatch& batch) {
  auto mm = CPUDevice::Instance()->default_memory_manager();
  ArrayVector arrays;
  for (const auto& column : batch.columns()) {
    std::shared_ptr<ArrayData> data = column->data()->Copy();
    if (data->child_data.size() != 0) {
      return Status::NotImplemented("Nested arrays");
    }

    for (size_t i = 0; i < data->buffers.size(); i++) {
      const auto& buffer = data->buffers[i];
      if (!buffer || buffer->is_cpu()) continue;
      ARROW_ASSIGN_OR_RAISE(data->buffers[i], Buffer::Copy(buffer, mm));
    }
    arrays.push_back(MakeArray(data));
  }
  return RecordBatch::Make(batch.schema(), batch.num_rows(), std::move(arrays));
}

class CudaTestServer : public FlightServerBase {
 public:
  explicit CudaTestServer(std::shared_ptr<Device> device,
                          std::shared_ptr<cuda::CudaContext> context)
      : device_(std::move(device)), context_(std::move(context)) {}

  Status DoGet(const ServerCallContext&, const Ticket&,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    RETURN_NOT_OK(ExampleIntBatches(&batches_));
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, RecordBatchReader::Make(batches_));
    *data_stream = std::make_unique<RecordBatchStream>(batch_reader);
    return Status::OK();
  }

  Status DoPut(const ServerCallContext&, std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    RETURN_NOT_OK(reader->ToRecordBatches().Value(&batches_));
    return Status::OK();
  }

  Status DoExchange(const ServerCallContext& context,
                    std::unique_ptr<FlightMessageReader> reader,
                    std::unique_ptr<FlightMessageWriter> writer) override {
    FlightStreamChunk chunk;
    bool begun = false;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
      if (!chunk.data) break;
      if (!begun) {
        begun = true;
        RETURN_NOT_OK(writer->Begin(chunk.data->schema()));
      }
      RETURN_NOT_OK(CheckBuffersOnDevice(*chunk.data, *device_));
      // XXX: do not assume transport will synchronize, we must
      // synchronize or else data will be "missing"
      RETURN_NOT_OK(context_->Synchronize());
      RETURN_NOT_OK(writer->WriteRecordBatch(*chunk.data));
    }
    return Status::OK();
  }

  const RecordBatchVector& batches() const { return batches_; }

 private:
  RecordBatchVector batches_;
  std::shared_ptr<Device> device_;
  std::shared_ptr<cuda::CudaContext> context_;
};

// Store CUDA objects without exposing them in the public header
class CudaDataTest::Impl {
 public:
  cuda::CudaDeviceManager* manager;
  std::shared_ptr<cuda::CudaDevice> device;
  std::shared_ptr<cuda::CudaContext> context;
};

void CudaDataTest::SetUpTest() {
  ASSERT_OK_AND_ASSIGN(auto manager, cuda::CudaDeviceManager::Instance());
  ASSERT_OK_AND_ASSIGN(auto device, manager->GetDevice(0));
  ASSERT_OK_AND_ASSIGN(auto context, device->GetContext());
  impl_.reset(new Impl());
  impl_->manager = manager;
  impl_->device = std::move(device);
  impl_->context = std::move(context);

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  ASSERT_OK(MakeServer<CudaTestServer>(
      location, &server_, &client_,
      [this](FlightServerOptions* options) {
        options->memory_manager = impl_->device->default_memory_manager();
        return Status::OK();
      },
      [](FlightClientOptions* options) { return Status::OK(); }, impl_->device,
      impl_->context));
}
void CudaDataTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
}
void CudaDataTest::TestDoGet() {
  // Check that we can allocate the results of DoGet with a custom
  // memory manager.
  FlightCallOptions options;
  options.memory_manager = impl_->device->default_memory_manager();

  const RecordBatchVector& batches =
      checked_cast<CudaTestServer*>(server_.get())->batches();

  Ticket ticket{""};
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(options, ticket));

  size_t idx = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, stream->Next());
    if (!chunk.data) break;

    ASSERT_OK(CheckBuffersOnDevice(*chunk.data, *impl_->device));
    if (idx >= batches.size()) {
      FAIL() << "Server returned more than " << batches.size() << " batches";
      return;
    }

    // Bounce record batch back to host memory
    ASSERT_OK_AND_ASSIGN(auto host_batch, CopyBatchToHost(*chunk.data));
    AssertBatchesEqual(*batches[idx], *host_batch);
    idx++;
  }
  ASSERT_EQ(idx, batches.size()) << "Server returned too few batches";
}
void CudaDataTest::TestDoPut() {
  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));

  auto descriptor = FlightDescriptor::Path({""});
  ASSERT_OK_AND_ASSIGN(auto do_put_result,
                       client_->DoPut(descriptor, batches[0]->schema()));

  ipc::DictionaryMemo memo;
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(auto buffer,
                         cuda::SerializeRecordBatch(*batch, impl_->context.get()));
    ASSERT_OK_AND_ASSIGN(auto cuda_batch,
                         cuda::ReadRecordBatch(batch->schema(), &memo, buffer));

    ASSERT_OK(CheckBuffersOnDevice(*cuda_batch, *impl_->device));
    ASSERT_OK(do_put_result.writer->WriteRecordBatch(*cuda_batch));
  }
  ASSERT_OK(do_put_result.writer->Close());
  ASSERT_OK(impl_->context->Synchronize());

  const RecordBatchVector& written =
      checked_cast<CudaTestServer*>(server_.get())->batches();
  ASSERT_EQ(written.size(), batches.size());

  size_t idx = 0;
  for (const auto& batch : written) {
    ASSERT_OK(CheckBuffersOnDevice(*batch, *impl_->device));
    // Bounce record batch back to host memory
    ASSERT_OK_AND_ASSIGN(auto host_batch, CopyBatchToHost(*batch));
    AssertBatchesEqual(*batches[idx], *host_batch);
    idx++;
  }
}
void CudaDataTest::TestDoExchange() {
  FlightCallOptions options;
  options.memory_manager = impl_->device->default_memory_manager();

  RecordBatchVector batches;
  ASSERT_OK(ExampleIntBatches(&batches));

  auto descriptor = FlightDescriptor::Path({""});
  ASSERT_OK_AND_ASSIGN(auto exchange, client_->DoExchange(options, descriptor));
  ASSERT_OK(exchange.writer->Begin(batches[0]->schema()));

  ipc::DictionaryMemo write_memo;
  ipc::DictionaryMemo read_memo;
  for (const auto& batch : batches) {
    ASSERT_OK_AND_ASSIGN(auto buffer,
                         cuda::SerializeRecordBatch(*batch, impl_->context.get()));
    ASSERT_OK_AND_ASSIGN(auto cuda_batch,
                         cuda::ReadRecordBatch(batch->schema(), &write_memo, buffer));

    ASSERT_OK(CheckBuffersOnDevice(*cuda_batch, *impl_->device));
    ASSERT_OK(exchange.writer->WriteRecordBatch(*cuda_batch));

    ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
    ASSERT_OK(CheckBuffersOnDevice(*chunk.data, *impl_->device));

    // Bounce record batch back to host memory
    ASSERT_OK_AND_ASSIGN(auto host_batch, CopyBatchToHost(*chunk.data));
    AssertBatchesEqual(*batch, *host_batch);
  }
  ASSERT_OK(exchange.writer->Close());
}

#else

void CudaDataTest::SetUpTest() {}
void CudaDataTest::TearDownTest() {}
void CudaDataTest::TestDoGet() { GTEST_SKIP() << "Arrow was built without ARROW_CUDA"; }
void CudaDataTest::TestDoPut() { GTEST_SKIP() << "Arrow was built without ARROW_CUDA"; }
void CudaDataTest::TestDoExchange() {
  GTEST_SKIP() << "Arrow was built without ARROW_CUDA";
}

#endif

//------------------------------------------------------------
// Test error handling

namespace {
static const std::vector<StatusCode> kStatusCodes = {
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

static const std::vector<FlightStatusCode> kFlightStatusCodes = {
    FlightStatusCode::Internal,     FlightStatusCode::TimedOut,
    FlightStatusCode::Cancelled,    FlightStatusCode::Unauthenticated,
    FlightStatusCode::Unauthorized, FlightStatusCode::Unavailable,
    FlightStatusCode::Failed,
};

arrow::Result<StatusCode> TryConvertStatusCode(int raw_code) {
  for (const auto status_code : kStatusCodes) {
    if (raw_code == static_cast<int>(status_code)) {
      return status_code;
    }
  }
  return Status::Invalid(raw_code);
}
arrow::Result<FlightStatusCode> TryConvertFlightStatusCode(int raw_code) {
  for (const auto status_code : kFlightStatusCodes) {
    if (raw_code == static_cast<int>(status_code)) {
      return status_code;
    }
  }
  return Status::Invalid(raw_code);
}

class TestStatusDetail : public StatusDetail {
 public:
  const char* type_id() const override { return "test-status-detail"; }
  std::string ToString() const override { return "Custom status detail"; }
};
class ErrorHandlingTestServer : public FlightServerBase {
 public:
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (request.path.size() >= 2) {
      const int raw_code = std::atoi(request.path[0].c_str());
      ARROW_ASSIGN_OR_RAISE(StatusCode code, TryConvertStatusCode(raw_code));

      if (request.path.size() == 2) {
        return Status(code, request.path[1]);
      } else if (request.path.size() == 3) {
        return Status(code, request.path[1], std::make_shared<TestStatusDetail>());
      } else {
        const int raw_code = std::atoi(request.path[2].c_str());
        ARROW_ASSIGN_OR_RAISE(FlightStatusCode flight_code,
                              TryConvertFlightStatusCode(raw_code));
        return Status(code, request.path[1],
                      std::make_shared<FlightStatusDetail>(flight_code, request.path[3]));
      }
    }
    return Status::NotImplemented("NYI");
  }

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    return MakeFlightError(FlightStatusCode::Unauthorized, "Unauthorized", "extra info");
  }

  Status DoExchange(const ServerCallContext& context,
                    std::unique_ptr<FlightMessageReader> reader,
                    std::unique_ptr<FlightMessageWriter> writer) override {
    return MakeFlightError(FlightStatusCode::Unauthorized, "Unauthorized", "extra info");
  }
};
}  // namespace

void ErrorHandlingTest::SetUpTest() {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForScheme(transport(), "127.0.0.1", 0));
  ASSERT_OK(MakeServer<ErrorHandlingTestServer>(
      location, &server_, &client_,
      [](FlightServerOptions* options) { return Status::OK(); },
      [](FlightClientOptions* options) { return Status::OK(); }));
}
void ErrorHandlingTest::TearDownTest() {
  ASSERT_OK(client_->Close());
  ASSERT_OK(server_->Shutdown());
}

void ErrorHandlingTest::TestGetFlightInfo() {
  std::unique_ptr<FlightInfo> info;
  for (const auto code : kStatusCodes) {
    ARROW_SCOPED_TRACE("C++ status code: ", static_cast<int>(code));
    auto descr = FlightDescriptor::Path(
        {std::to_string(static_cast<int>(code)), "Expected message"});
    auto status = client_->GetFlightInfo(descr).status();
    EXPECT_EQ(status.code(), code);
    EXPECT_THAT(status.message(), ::testing::HasSubstr("Expected message"));

    // Custom status detail
    descr = FlightDescriptor::Path(
        {std::to_string(static_cast<int>(code)), "Expected message", ""});
    status = client_->GetFlightInfo(descr).status();
    EXPECT_EQ(status.code(), code);
    EXPECT_THAT(status.message(), ::testing::HasSubstr("Expected message"));
    EXPECT_THAT(status.message(), ::testing::HasSubstr("Detail: Custom status detail"));

    // Flight status detail
    for (const auto flight_code : kFlightStatusCodes) {
      ARROW_SCOPED_TRACE("Flight status code: ", static_cast<int>(flight_code));
      descr = FlightDescriptor::Path(
          {std::to_string(static_cast<int>(code)), "Expected message",
           std::to_string(static_cast<int>(flight_code)), "Expected detail message"});
      status = client_->GetFlightInfo(descr).status();
      // Don't check status code, since Flight code may override it
      EXPECT_THAT(status.message(), ::testing::HasSubstr("Expected message"));
      auto detail = FlightStatusDetail::UnwrapStatus(status);
      ASSERT_NE(detail, nullptr);
      EXPECT_EQ(detail->code(), flight_code);
      EXPECT_THAT(detail->extra_info(), ::testing::HasSubstr("Expected detail message"));
    }
  }
}

void CheckErrorDetail(const Status& status) {
  auto detail = FlightStatusDetail::UnwrapStatus(status);
  ASSERT_NE(detail, nullptr) << status.ToString();
  ASSERT_EQ(detail->code(), FlightStatusCode::Unauthorized);
  ASSERT_EQ(detail->extra_info(), "extra info");
}

void ErrorHandlingTest::TestDoPut() {
  // ARROW-16592
  auto schema = arrow::schema({field("int64", int64())});
  auto descr = FlightDescriptor::Path({""});
  FlightClient::DoPutResult stream;
  auto status = client_->DoPut(descr, schema).Value(&stream);
  if (!status.ok()) {
    ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(status));
    return;
  }

  std::thread reader_thread([&]() {
    std::shared_ptr<Buffer> out;
    while (true) {
      if (!stream.reader->ReadMetadata(&out).ok()) {
        return;
      }
    }
  });

  auto batch = RecordBatchFromJSON(schema, "[[0]]");
  while (true) {
    status = stream.writer->WriteRecordBatch(*batch);
    if (!status.ok()) break;
  }

  ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(status));
  ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(stream.writer->Close()));
  reader_thread.join();
}

void ErrorHandlingTest::TestDoExchange() {
  // ARROW-16592
  FlightClient::DoExchangeResult stream;
  auto status = client_->DoExchange(FlightDescriptor::Path({""})).Value(&stream);
  if (!status.ok()) {
    ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(status));
    return;
  }

  std::thread reader_thread([&]() {
    while (true) {
      if (!stream.reader->Next().ok()) return;
    }
  });

  while (true) {
    status = stream.writer->WriteMetadata(Buffer::FromString("foo"));
    if (!status.ok()) break;
  }

  ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(status));
  ASSERT_NO_FATAL_FAILURE(CheckErrorDetail(stream.writer->Close()));
  reader_thread.join();
}

}  // namespace flight
}  // namespace arrow
