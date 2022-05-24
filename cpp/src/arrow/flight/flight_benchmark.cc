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

#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"
#include "arrow/util/config.h"
#include "arrow/util/stopwatch.h"
#include "arrow/util/tdigest.h"
#include "arrow/util/thread_pool.h"

#include "arrow/flight/api.h"
#include "arrow/flight/perf.pb.h"
#include "arrow/flight/test_util.h"

#ifdef ARROW_CUDA
#include <cuda.h>
#include "arrow/gpu/cuda_api.h"
#endif
#ifdef ARROW_WITH_UCX
#include "arrow/flight/transport/ucx/ucx.h"
#endif

DEFINE_bool(cuda, false, "Allocate results in CUDA memory");
DEFINE_string(transport, "grpc",
              "The network transport to use. Supported: \"grpc\" (default)"
#ifdef ARROW_WITH_UCX
              ", \"ucx\""
#endif  // ARROW_WITH_UCX
              ".");
DEFINE_string(server_host, "",
              "An existing performance server to benchmark against (leave blank to spawn "
              "one automatically)");
DEFINE_int32(server_port, 31337, "The port to connect to");
DEFINE_string(server_unix, "",
              "An existing performance server listening on Unix socket (leave blank to "
              "spawn one automatically)");
DEFINE_bool(test_unix, false, "Test Unix socket instead of TCP");
DEFINE_int32(num_perf_runs, 1,
             "Number of times to run the perf test to "
             "increase precision");
DEFINE_int32(num_servers, 1, "Number of performance servers to run");
DEFINE_int32(num_streams, 4, "Number of streams for each server");
DEFINE_int32(num_threads, 4, "Number of concurrent gets");
DEFINE_int64(records_per_stream, 10000000, "Total records per stream");
DEFINE_int32(records_per_batch, 4096, "Total records per batch within stream");
DEFINE_bool(test_put, false, "Test DoPut instead of DoGet");
DEFINE_string(compression, "",
              "Select compression method (\"zstd\", \"lz4\"). "
              "Leave blank to disable compression.\n"
              "E.g., \"zstd\":   zstd with default compression level.\n"
              "      \"zstd:7\": zstd with compression leve = 7.\n");
DEFINE_string(
    data_file, "",
    "Instead of random data, use data from the given IPC file. Only affects -test_put.");
DEFINE_string(cert_file, "", "Path to TLS certificate");
DEFINE_string(key_file, "", "Path to TLS private key (used when spawning a server)");

namespace perf = arrow::flight::perf;

namespace arrow {

using internal::StopWatch;
using internal::ThreadPool;

namespace flight {

struct PerformanceResult {
  int64_t num_batches;
  int64_t num_records;
  int64_t num_bytes;
};

struct PerformanceStats {
  std::mutex mutex;
  int64_t total_batches = 0;
  int64_t total_records = 0;
  int64_t total_bytes = 0;
  const std::array<double, 3> quantiles = {0.5, 0.95, 0.99};
  mutable arrow::internal::TDigest latencies;

  void Update(int64_t total_batches, int64_t total_records, int64_t total_bytes) {
    std::lock_guard<std::mutex> lock(this->mutex);
    this->total_batches += total_batches;
    this->total_records += total_records;
    this->total_bytes += total_bytes;
  }

  // Invoked per batch in the test loop. Holding a lock looks not scalable.
  // Tested with 1 ~ 8 threads, no noticeable overhead is observed.
  // A better approach may be calculate per-thread quantiles and merge.
  void AddLatency(uint64_t elapsed_nanos) {
    std::lock_guard<std::mutex> lock(this->mutex);
    latencies.Add(static_cast<double>(elapsed_nanos));
  }

  // ns -> us
  uint64_t max_latency() const { return latencies.Max() / 1000; }

  uint64_t mean_latency() const { return latencies.Mean() / 1000; }

  uint64_t quantile_latency(double q) const { return latencies.Quantile(q) / 1000; }
};

Status WaitForReady(FlightClient* client, const FlightCallOptions& call_options) {
  Action action{"ping", nullptr};
  for (int attempt = 0; attempt < 10; attempt++) {
    if (client->DoAction(call_options, action).ok()) {
      return Status::OK();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  return Status::IOError("Server was not available after 10 attempts");
}

arrow::Result<PerformanceResult> RunDoGetTest(FlightClient* client,
                                              const FlightCallOptions& call_options,
                                              const perf::Token& token,
                                              const FlightEndpoint& endpoint,
                                              PerformanceStats* stats) {
  std::unique_ptr<FlightStreamReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, client->DoGet(call_options, endpoint.ticket));

  FlightStreamChunk batch;

  // This is hard-coded for right now, 4 columns each with int64
  const int bytes_per_record = 32;

  // This must also be set in perf_server.cc
  const bool verify = false;

  int64_t num_bytes = 0;
  int64_t num_records = 0;
  int64_t num_batches = 0;
  StopWatch timer;
  while (true) {
    timer.Start();
    ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
    stats->AddLatency(timer.Stop());
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

    ++num_batches;
    num_records += batch.data->num_rows();

    // Hard-coded
    num_bytes += batch.data->num_rows() * bytes_per_record;
  }
  return PerformanceResult{num_batches, num_records, num_bytes};
}

struct SizedBatch {
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t bytes;
};

arrow::Result<std::vector<SizedBatch>> GetPutData(const perf::Token& token) {
  if (!FLAGS_data_file.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto file, arrow::io::ReadableFile::Open(FLAGS_data_file));
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::ipc::RecordBatchFileReader::Open(std::move(file)));
    std::vector<SizedBatch> batches(reader->num_record_batches());
    for (int i = 0; i < reader->num_record_batches(); i++) {
      ARROW_ASSIGN_OR_RAISE(batches[i].batch, reader->ReadRecordBatch(i));
      RETURN_NOT_OK(arrow::ipc::GetRecordBatchSize(*batches[i].batch, &batches[i].bytes));
    }
    return batches;
  }

  std::shared_ptr<Schema> schema =
      arrow::schema({field("a", int64()), field("b", int64()), field("c", int64()),
                     field("d", int64())});

  // This is hard-coded for right now, 4 columns each with int64
  const int bytes_per_record = 32;

  std::shared_ptr<ResizableBuffer> buffer;
  std::vector<std::shared_ptr<Array>> arrays;

  const int64_t total_records = token.definition().records_per_stream();
  const int32_t length = token.definition().records_per_batch();
  const int32_t ncolumns = 4;
  for (int i = 0; i < ncolumns; ++i) {
    RETURN_NOT_OK(MakeRandomByteBuffer(length * sizeof(int64_t), default_memory_pool(),
                                       &buffer, static_cast<int32_t>(i) /* seed */));
    arrays.push_back(std::make_shared<Int64Array>(length, buffer));
    RETURN_NOT_OK(arrays.back()->Validate());
  }

  std::shared_ptr<RecordBatch> batch = RecordBatch::Make(schema, length, arrays);
  std::vector<SizedBatch> batches;

  int64_t records_sent = 0;
  while (records_sent < total_records) {
    if (records_sent + length > total_records) {
      const int last_length = total_records - records_sent;
      // Hard-coded
      batches.push_back(SizedBatch{batch->Slice(0, last_length),
                                   /*bytes=*/last_length * bytes_per_record});
      records_sent += last_length;
    } else {
      // Hard-coded
      batches.push_back(SizedBatch{batch, /*bytes=*/length * bytes_per_record});
      records_sent += length;
    }
  }
  return batches;
}

arrow::Result<PerformanceResult> RunDoPutTest(FlightClient* client,
                                              const FlightCallOptions& call_options,
                                              const perf::Token& token,
                                              const FlightEndpoint& endpoint,
                                              PerformanceStats* stats) {
  ARROW_ASSIGN_OR_RAISE(const auto batches, GetPutData(token));
  StopWatch timer;
  int64_t num_records = 0;
  int64_t num_bytes = 0;
  ARROW_ASSIGN_OR_RAISE(
      auto do_put_result,
      client->DoPut(call_options, FlightDescriptor{}, batches[0].batch->schema()));
  std::unique_ptr<FlightStreamWriter> writer = std::move(do_put_result.writer);
  for (size_t i = 0; i < batches.size(); i++) {
    auto batch = batches[i];
    auto is_last = i == (batches.size() - 1);
    if (is_last) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch.batch));
      num_records += batch.batch->num_rows();
      num_bytes += batch.bytes;
    } else {
      timer.Start();
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch.batch));
      stats->AddLatency(timer.Stop());
      num_records += batch.batch->num_rows();
      num_bytes += batch.bytes;
    }
  }
  RETURN_NOT_OK(writer->Close());
  return PerformanceResult{static_cast<int64_t>(batches.size()), num_records, num_bytes};
}

Status DoSinglePerfRun(FlightClient* client, const FlightClientOptions client_options,
                       const FlightCallOptions& call_options, bool test_put,
                       PerformanceStats* stats) {
  // schema not needed
  perf::Perf perf;
  perf.set_stream_count(FLAGS_num_streams);
  perf.set_records_per_stream(FLAGS_records_per_stream);
  perf.set_records_per_batch(FLAGS_records_per_batch);

  // Plan the query
  FlightDescriptor descriptor;
  descriptor.type = FlightDescriptor::CMD;
  perf.SerializeToString(&descriptor.cmd);

  ARROW_ASSIGN_OR_RAISE(auto plan, client->GetFlightInfo(call_options, descriptor));

  // Read the streams in parallel
  ipc::DictionaryMemo dict_memo;
  ARROW_ASSIGN_OR_RAISE(auto schema, plan->GetSchema(&dict_memo));

  int64_t start_total_records = stats->total_records;

  auto test_loop = test_put ? &RunDoPutTest : &RunDoGetTest;
  auto ConsumeStream = [&client, &stats, &test_loop, &client_options,
                        &call_options](const FlightEndpoint& endpoint) {
    std::unique_ptr<FlightClient> local_client;
    FlightClient* data_client;
    if (endpoint.locations.empty()) {
      data_client = client;
    } else {
      ARROW_ASSIGN_OR_RAISE(
          local_client,
          FlightClient::Connect(endpoint.locations.front(), client_options));
      data_client = local_client.get();
    }

    perf::Token token;
    token.ParseFromString(endpoint.ticket.ticket);

    const auto& result = test_loop(data_client, call_options, token, endpoint, stats);
    if (result.ok()) {
      const PerformanceResult& perf = result.ValueOrDie();
      stats->Update(perf.num_batches, perf.num_records, perf.num_bytes);
    }
    return result.status();
  };

  // XXX(wesm): Serial version for debugging
  // for (const auto& endpoint : plan->endpoints()) {
  //   RETURN_NOT_OK(ConsumeStream(endpoint));
  // }

  ARROW_ASSIGN_OR_RAISE(auto pool, ThreadPool::Make(FLAGS_num_threads));
  std::vector<Future<>> tasks;
  for (const auto& endpoint : plan->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto task, pool->Submit(ConsumeStream, endpoint));
    tasks.push_back(std::move(task));
  }

  // Wait for tasks to finish
  for (auto&& task : tasks) {
    RETURN_NOT_OK(task.status());
  }

  if (FLAGS_data_file.empty()) {
    // Check that number of rows read / written is as expected
    int64_t records_for_run = stats->total_records - start_total_records;
    if (records_for_run != static_cast<int64_t>(plan->total_records())) {
      return Status::Invalid("Did not consume expected number of records, got: ",
                             records_for_run, " but expected: ", plan->total_records());
    }
  }
  return Status::OK();
}

Status RunPerformanceTest(FlightClient* client, const FlightClientOptions& client_options,
                          const FlightCallOptions& call_options, bool test_put) {
  StopWatch timer;
  timer.Start();

  PerformanceStats stats;
  for (int i = 0; i < FLAGS_num_perf_runs; ++i) {
    RETURN_NOT_OK(
        DoSinglePerfRun(client, client_options, call_options, test_put, &stats));
  }

  // Elapsed time in seconds
  uint64_t elapsed_nanos = timer.Stop();
  double time_elapsed =
      static_cast<double>(elapsed_nanos) / static_cast<double>(1000000000);

  constexpr double kMegabyte = static_cast<double>(1 << 20);

  std::cout << "Number of perf runs: " << FLAGS_num_perf_runs << std::endl;
  std::cout << "Number of concurrent gets/puts: " << FLAGS_num_threads << std::endl;
  std::cout << "Batch size: " << stats.total_bytes / stats.total_batches << std::endl;
  if (FLAGS_test_put) {
    std::cout << "Batches written: " << stats.total_batches << std::endl;
    std::cout << "Bytes written: " << stats.total_bytes << std::endl;
  } else {
    std::cout << "Batches read: " << stats.total_batches << std::endl;
    std::cout << "Bytes read: " << stats.total_bytes << std::endl;
  }

  std::cout << "Nanos: " << elapsed_nanos << std::endl;
  std::cout << "Speed: "
            << (static_cast<double>(stats.total_bytes) / kMegabyte / time_elapsed)
            << " MB/s" << std::endl;

  // Calculate throughput(IOPS) and latency vs batch size
  std::cout << "Throughput: " << (static_cast<double>(stats.total_batches) / time_elapsed)
            << " batches/s" << std::endl;
  std::cout << "Latency mean: " << stats.mean_latency() << " us" << std::endl;
  for (auto q : stats.quantiles) {
    std::cout << "Latency quantile=" << q << ": " << stats.quantile_latency(q) << " us"
              << std::endl;
  }
  std::cout << "Latency max: " << stats.max_latency() << " us" << std::endl;

  return Status::OK();
}

}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::cout << "Testing method: ";
  if (FLAGS_test_put) {
    std::cout << "DoPut";
  } else {
    std::cout << "DoGet";
  }
  std::cout << std::endl;

  arrow::flight::FlightCallOptions call_options;
  if (!FLAGS_compression.empty()) {
    if (!FLAGS_test_put) {
      std::cerr << "Compression is only useful for Put test now, "
                   "please append \"-test_put\" to command line"
                << std::endl;
      std::abort();
    }

    // "zstd"   -> name = "zstd", level = default
    // "zstd:7" -> name = "zstd", level = 7
    const size_t delim = FLAGS_compression.find(":");
    const std::string name = FLAGS_compression.substr(0, delim);
    const std::string level_str =
        delim == std::string::npos
            ? ""
            : FLAGS_compression.substr(delim + 1, FLAGS_compression.length() - delim - 1);
    const int level = level_str.empty() ? arrow::util::kUseDefaultCompressionLevel
                                        : std::stoi(level_str);
    const auto type = arrow::util::Codec::GetCompressionType(name).ValueOrDie();
    auto codec = arrow::util::Codec::Create(type, level).ValueOrDie();
    std::cout << "Compression method: " << name;
    if (!level_str.empty()) {
      std::cout << ", level " << level;
    }
    std::cout << std::endl;

    call_options.write_options.codec = std::move(codec);
  }
  if (!FLAGS_data_file.empty() && !FLAGS_test_put) {
    std::cerr << "A data file can only be specified with \"-test_put\"" << std::endl;
    return 1;
  }

  std::unique_ptr<arrow::flight::TestServer> server;
  std::vector<std::string> server_args;
  server_args.push_back("-transport");
  server_args.push_back(FLAGS_transport);
  arrow::flight::Location location;
  auto options = arrow::flight::FlightClientOptions::Defaults();
  if (FLAGS_transport == "grpc") {
    if (FLAGS_test_unix || !FLAGS_server_unix.empty()) {
      if (FLAGS_server_unix == "") {
        FLAGS_server_unix = "/tmp/flight-bench-spawn.sock";
        std::cout << "Using spawned Unix server" << std::endl;
        server.reset(
            new arrow::flight::TestServer("arrow-flight-perf-server", FLAGS_server_unix));
      } else {
        std::cout << "Using standalone Unix server" << std::endl;
      }
      std::cout << "Server unix socket: " << FLAGS_server_unix << std::endl;
      ABORT_NOT_OK(
          arrow::flight::Location::ForGrpcUnix(FLAGS_server_unix).Value(&location));
    } else {
      if (FLAGS_server_host == "") {
        FLAGS_server_host = "localhost";
        std::cout << "Using spawned TCP server" << std::endl;
        server.reset(
            new arrow::flight::TestServer("arrow-flight-perf-server", FLAGS_server_port));
        if (!FLAGS_cert_file.empty() || !FLAGS_key_file.empty()) {
          if (!FLAGS_cert_file.empty() && !FLAGS_key_file.empty()) {
            std::cout << "Enabling TLS for spawned server" << std::endl;
            server_args.push_back("-cert_file");
            server_args.push_back(FLAGS_cert_file);
            server_args.push_back("-key_file");
            server_args.push_back(FLAGS_key_file);
          } else {
            std::cerr << "If providing TLS cert/key, must provide both" << std::endl;
            return 1;
          }
        }
      } else {
        std::cout << "Using standalone TCP server" << std::endl;
      }
      if (server) {
        if (FLAGS_cuda && FLAGS_test_put) {
          server_args.push_back("-cuda");
        }
        server->Start(server_args);
      }
      std::cout << "Server host: " << FLAGS_server_host << std::endl
                << "Server port: " << FLAGS_server_port << std::endl;
      if (FLAGS_cert_file.empty()) {
        ABORT_NOT_OK(
            arrow::flight::Location::ForGrpcTcp(FLAGS_server_host, FLAGS_server_port)
                .Value(&location));
      } else {
        ABORT_NOT_OK(
            arrow::flight::Location::ForGrpcTls(FLAGS_server_host, FLAGS_server_port)
                .Value(&location));
        options.disable_server_verification = true;
      }
    }
  } else if (FLAGS_transport == "ucx") {
#ifdef ARROW_WITH_UCX
    arrow::flight::transport::ucx::InitializeFlightUcx();
    if (FLAGS_test_unix || !FLAGS_server_unix.empty()) {
      std::cerr << "Transport does not support domain sockets: " << FLAGS_transport
                << std::endl;
      return EXIT_FAILURE;
    }
    ARROW_CHECK_OK(arrow::flight::Location::Parse("ucx://" + FLAGS_server_host + ":" +
                                                  std::to_string(FLAGS_server_port))
                       .Value(&location));
#else
    std::cerr << "Not built with transport: " << FLAGS_transport << std::endl;
    return EXIT_FAILURE;
#endif
  } else {
    std::cerr << "Unknown transport: " << FLAGS_transport << std::endl;
    return EXIT_FAILURE;
  }

  if (FLAGS_cuda) {
#ifdef ARROW_CUDA
    if (FLAGS_test_put && !server) {
      std::cerr << "Warning: -cuda has no effect with -test_put" << std::endl;
      std::cerr << "Warning: (enable it on the server instead)" << std::endl;
    }
    arrow::cuda::CudaDeviceManager* manager = nullptr;
    std::shared_ptr<arrow::cuda::CudaDevice> device;

    ABORT_NOT_OK(arrow::cuda::CudaDeviceManager::Instance().Value(&manager));
    ABORT_NOT_OK(manager->GetDevice(0).Value(&device));
    call_options.memory_manager = device->default_memory_manager();

    // Needed to prevent UCX warning
    // cuda_md.c:162  UCX  ERROR cuMemGetAddressRange(0x7f2ab5dc0000) error: invalid
    // device context
    std::shared_ptr<arrow::cuda::CudaContext> context;
    ABORT_NOT_OK(device->GetContext().Value(&context));
    auto cuda_status = cuCtxPushCurrent(reinterpret_cast<CUcontext>(context->handle()));
    if (cuda_status != CUDA_SUCCESS) {
      ARROW_LOG(WARNING) << "CUDA error " << cuda_status;
    }
#else
    std::cerr << "-cuda requires that Arrow is built with ARROW_CUDA" << std::endl;
    return 1;
#endif
  }

  auto client = arrow::flight::FlightClient::Connect(location, options).ValueOrDie();
  ABORT_NOT_OK(arrow::flight::WaitForReady(client.get(), call_options));

  arrow::Status s = arrow::flight::RunPerformanceTest(client.get(), options, call_options,
                                                      FLAGS_test_put);

  if (server) {
    server->Stop();
  }

  if (!s.ok()) {
    std::cerr << "Failed with error: << " << s.ToString() << std::endl;
  }

  return 0;
}
