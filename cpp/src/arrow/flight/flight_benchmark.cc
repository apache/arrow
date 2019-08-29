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
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/stopwatch.h"
#include "arrow/util/thread_pool.h"

#include "arrow/flight/api.h"
#include "arrow/flight/perf.pb.h"
#include "arrow/flight/test_util.h"

DEFINE_string(server_host, "",
              "An existing performance server to benchmark against (leave blank to spawn "
              "one automatically)");
DEFINE_int32(server_port, 31337, "The port to connect to");
DEFINE_int32(num_servers, 1, "Number of performance servers to run");
DEFINE_int32(num_streams, 4, "Number of streams for each server");
DEFINE_int32(num_threads, 4, "Number of concurrent gets");
DEFINE_int32(records_per_stream, 10000000, "Total records per stream");
DEFINE_int32(records_per_batch, 4096, "Total records per batch within stream");
DEFINE_bool(test_put, false, "Test DoPut instead of DoGet");

namespace perf = arrow::flight::perf;

namespace arrow {

using internal::StopWatch;
using internal::ThreadPool;

namespace flight {

struct PerformanceResult {
  int64_t num_records;
  int64_t num_bytes;
};

struct PerformanceStats {
  PerformanceStats() : total_records(0), total_bytes(0) {}
  std::mutex mutex;
  int64_t total_records;
  int64_t total_bytes;

  void Update(const int64_t total_records, const int64_t total_bytes) {
    std::lock_guard<std::mutex> lock(this->mutex);
    this->total_records += total_records;
    this->total_bytes += total_bytes;
  }
};

Status WaitForReady(FlightClient* client) {
  Action action{"ping", nullptr};
  for (int attempt = 0; attempt < 10; attempt++) {
    std::unique_ptr<ResultStream> stream;
    if (client->DoAction(action, &stream).ok()) {
      return Status::OK();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  return Status::IOError("Server was not available after 10 attempts");
}

arrow::Result<PerformanceResult> RunDoGetTest(FlightClient* client,
                                              const perf::Token& token,
                                              const FlightEndpoint& endpoint) {
  std::unique_ptr<FlightStreamReader> reader;
  RETURN_NOT_OK(client->DoGet(endpoint.ticket, &reader));

  FlightStreamChunk batch;

  // This is hard-coded for right now, 4 columns each with int64
  const int bytes_per_record = 32;

  // This must also be set in perf_server.cc
  const bool verify = false;

  int64_t num_bytes = 0;
  int64_t num_records = 0;
  while (true) {
    RETURN_NOT_OK(reader->Next(&batch));
    if (!batch.data) {
      break;
    }

    if (verify) {
      auto values = batch.data->column_data(0)->GetValues<int64_t>(1);
      const int64_t start = token.start() + num_records;
      for (int64_t i = 0; i < batch.data->num_rows(); ++i) {
        if (values[i] != start + i) {
          return Status::Invalid("verification failure");
        }
      }
    }

    num_records += batch.data->num_rows();

    // Hard-coded
    num_bytes += batch.data->num_rows() * bytes_per_record;
  }
  return PerformanceResult{num_records, num_bytes};
}

arrow::Result<PerformanceResult> RunDoPutTest(FlightClient* client,
                                              const perf::Token& token,
                                              const FlightEndpoint& endpoint) {
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  std::shared_ptr<Schema> schema =
      arrow::schema({field("a", int64()), field("b", int64()), field("c", int64()),
                     field("d", int64())});
  RETURN_NOT_OK(client->DoPut(FlightDescriptor{}, schema, &writer, &reader));

  // This is hard-coded for right now, 4 columns each with int64
  const int bytes_per_record = 32;

  int64_t num_bytes = 0;
  int64_t num_records = 0;

  std::shared_ptr<ResizableBuffer> buffer;
  std::vector<std::shared_ptr<Array>> arrays;

  const int32_t length = token.definition().records_per_batch();
  const int32_t ncolumns = 4;
  for (int i = 0; i < ncolumns; ++i) {
    RETURN_NOT_OK(MakeRandomByteBuffer(length * sizeof(int64_t), default_memory_pool(),
                                       &buffer, static_cast<int32_t>(i) /* seed */));
    arrays.push_back(std::make_shared<Int64Array>(length, buffer));
    RETURN_NOT_OK(arrays.back()->Validate());
  }

  std::shared_ptr<RecordBatch> batch = RecordBatch::Make(schema, length, arrays);

  int records_sent = 0;
  const int total_records = token.definition().records_per_stream();
  while (records_sent < total_records) {
    if (records_sent + length > total_records) {
      const int last_length = total_records - records_sent;
      RETURN_NOT_OK(writer->WriteRecordBatch(*(batch->Slice(0, last_length))));
      num_records += last_length;
      // Hard-coded
      num_bytes += last_length * bytes_per_record;
      records_sent += last_length;
    } else {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      num_records += length;
      // Hard-coded
      num_bytes += length * bytes_per_record;
      records_sent += length;
    }
  }

  RETURN_NOT_OK(writer->Close());
  return PerformanceResult{num_records, num_bytes};
}

Status RunPerformanceTest(FlightClient* client, bool test_put) {
  // TODO(wesm): Multiple servers
  // std::vector<std::unique_ptr<TestServer>> servers;

  // schema not needed
  perf::Perf perf;
  perf.set_stream_count(FLAGS_num_streams);
  perf.set_records_per_stream(FLAGS_records_per_stream);
  perf.set_records_per_batch(FLAGS_records_per_batch);

  // Plan the query
  FlightDescriptor descriptor;
  descriptor.type = FlightDescriptor::CMD;
  perf.SerializeToString(&descriptor.cmd);

  std::unique_ptr<FlightInfo> plan;
  RETURN_NOT_OK(client->GetFlightInfo(descriptor, &plan));

  // Read the streams in parallel
  std::shared_ptr<Schema> schema;
  ipc::DictionaryMemo dict_memo;
  RETURN_NOT_OK(plan->GetSchema(&dict_memo, &schema));

  PerformanceStats stats;
  auto test_loop = test_put ? &RunDoPutTest : &RunDoGetTest;
  auto ConsumeStream = [&stats, &test_loop](const FlightEndpoint& endpoint) {
    // TODO(wesm): Use location from endpoint, same host/port for now
    std::unique_ptr<FlightClient> client;
    RETURN_NOT_OK(FlightClient::Connect(endpoint.locations.front(), &client));

    perf::Token token;
    token.ParseFromString(endpoint.ticket.ticket);

    const auto& result = test_loop(client.get(), token, endpoint);
    if (result.ok()) {
      const PerformanceResult& perf = result.ValueOrDie();
      stats.Update(perf.num_records, perf.num_bytes);
    }
    return result.status();
  };

  StopWatch timer;
  timer.Start();

  // XXX(wesm): Serial version for debugging
  // for (const auto& endpoint : plan->endpoints()) {
  //   RETURN_NOT_OK(ConsumeStream(endpoint));
  // }

  std::shared_ptr<ThreadPool> pool;
  RETURN_NOT_OK(ThreadPool::Make(FLAGS_num_threads, &pool));
  std::vector<std::future<Status>> tasks;
  for (const auto& endpoint : plan->endpoints()) {
    tasks.emplace_back(pool->Submit(ConsumeStream, endpoint));
  }

  // Wait for tasks to finish
  for (auto&& task : tasks) {
    RETURN_NOT_OK(task.get());
  }

  // Elapsed time in seconds
  uint64_t elapsed_nanos = timer.Stop();
  double time_elapsed =
      static_cast<double>(elapsed_nanos) / static_cast<double>(1000000000);

  constexpr double kMegabyte = static_cast<double>(1 << 20);

  // Check that number of rows read is as expected
  if (stats.total_records != static_cast<int64_t>(plan->total_records())) {
    return Status::Invalid("Did not consume expected number of records");
  }

  std::cout << "Bytes read: " << stats.total_bytes << std::endl;
  std::cout << "Nanos: " << elapsed_nanos << std::endl;
  std::cout << "Speed: "
            << (static_cast<double>(stats.total_bytes) / kMegabyte / time_elapsed)
            << " MB/s" << std::endl;
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<arrow::flight::TestServer> server;
  std::string hostname = "localhost";
  if (FLAGS_server_host == "") {
    std::cout << "Using remote server: false" << std::endl;
    server.reset(
        new arrow::flight::TestServer("arrow-flight-perf-server", FLAGS_server_port));
    server->Start();
  } else {
    std::cout << "Using remote server: true" << std::endl;
    hostname = FLAGS_server_host;
  }

  std::cout << "Testing method: ";
  if (FLAGS_test_put) {
    std::cout << "DoPut";
  } else {
    std::cout << "DoGet";
  }
  std::cout << std::endl;

  std::cout << "Server host: " << hostname << std::endl
            << "Server port: " << FLAGS_server_port << std::endl;

  std::unique_ptr<arrow::flight::FlightClient> client;
  arrow::flight::Location location;
  ABORT_NOT_OK(
      arrow::flight::Location::ForGrpcTcp(hostname, FLAGS_server_port, &location));
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));
  ABORT_NOT_OK(arrow::flight::WaitForReady(client.get()));

  arrow::Status s = arrow::flight::RunPerformanceTest(client.get(), FLAGS_test_put);

  if (server) {
    server->Stop();
  }

  if (!s.ok()) {
    std::cerr << "Failed with error: << " << s.ToString() << std::endl;
  }

  return 0;
}
