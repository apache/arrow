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
#include "arrow/util/thread-pool.h"

#include "arrow/flight/api.h"
#include "arrow/flight/perf.pb.h"
#include "arrow/flight/test-util.h"

DEFINE_string(server_host, "",
              "An existing performance server to benchmark against (leave blank to spawn "
              "one automatically)");
DEFINE_int32(server_port, 31337, "The port to connect to");
DEFINE_int32(num_servers, 1, "Number of performance servers to run");
DEFINE_int32(num_streams, 4, "Number of streams for each server");
DEFINE_int32(num_threads, 4, "Number of concurrent gets");
DEFINE_int32(records_per_stream, 10000000, "Total records per stream");
DEFINE_int32(records_per_batch, 4096, "Total records per batch within stream");

namespace perf = arrow::flight::perf;

namespace arrow {

using internal::StopWatch;
using internal::ThreadPool;

namespace flight {

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

Status RunPerformanceTest(const std::string& hostname, const int port) {
  // TODO(wesm): Multiple servers
  // std::vector<std::unique_ptr<TestServer>> servers;

  // schema not needed
  perf::Perf perf;
  perf.set_stream_count(FLAGS_num_streams);
  perf.set_records_per_stream(FLAGS_records_per_stream);
  perf.set_records_per_batch(FLAGS_records_per_batch);

  // Construct client and plan the query
  std::unique_ptr<FlightClient> client;
  Location location;
  RETURN_NOT_OK(Location::ForGrpcTcp("localhost", port, &location));
  RETURN_NOT_OK(FlightClient::Connect(location, &client));

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
  auto ConsumeStream = [&stats, &port](const FlightEndpoint& endpoint) {
    // TODO(wesm): Use location from endpoint, same host/port for now
    std::unique_ptr<FlightClient> client;
    Location location;
    RETURN_NOT_OK(Location::ForGrpcTcp("localhost", port, &location));
    RETURN_NOT_OK(FlightClient::Connect(location, &client));

    perf::Token token;
    token.ParseFromString(endpoint.ticket.ticket);

    std::unique_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(client->DoGet(endpoint.ticket, &reader));

    std::shared_ptr<RecordBatch> batch;

    // This is hard-coded for right now, 4 columns each with int64
    const int bytes_per_record = 32;

    // This must also be set in perf-server.c
    const bool verify = false;

    int64_t num_bytes = 0;
    int64_t num_records = 0;
    while (true) {
      RETURN_NOT_OK(reader->ReadNext(&batch));
      if (!batch) {
        break;
      }

      if (verify) {
        auto values =
            reinterpret_cast<const int64_t*>(batch->column_data(0)->buffers[1]->data());
        const int64_t start = token.start() + num_records;
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
          if (values[i] != start + i) {
            return Status::Invalid("verification failure");
          }
        }
      }

      num_records += batch->num_rows();

      // Hard-coded
      num_bytes += batch->num_rows() * bytes_per_record;
    }
    stats.Update(num_records, num_bytes);
    return Status::OK();
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

  std::cout << "Server host: " << hostname << std::endl
            << "Server port: " << FLAGS_server_port << std::endl;

  arrow::Status s = arrow::flight::RunPerformanceTest(hostname, FLAGS_server_port);

  if (server) {
    server->Stop();
  }

  if (!s.ok()) {
    std::cerr << "Failed with error: << " << s.ToString() << std::endl;
  }

  return 0;
}
