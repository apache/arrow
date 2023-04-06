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

// Performance server for benchmarking purposes

#include <signal.h>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/array.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/perf.pb.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/test_util.h"

#ifdef ARROW_CUDA
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
DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(port, 31337, "Server port to listen on");
DEFINE_string(server_unix, "", "Unix socket path where the server is running on");
DEFINE_string(cert_file, "", "Path to TLS certificate");
DEFINE_string(key_file, "", "Path to TLS private key");

namespace perf = arrow::flight::perf;
namespace proto = arrow::flight::protocol;

namespace arrow {
namespace flight {

#define CHECK_PARSE(EXPR)                              \
  do {                                                 \
    if (!EXPR) {                                       \
      return Status::Invalid("cannot parse protobuf"); \
    }                                                  \
  } while (0)

// Create record batches with a unique "a" column so we can verify on the
// client side that the results are correct
class PerfDataStream : public FlightDataStream {
 public:
  PerfDataStream(bool verify, const int64_t start, const int64_t total_records,
                 const std::shared_ptr<Schema>& schema, const ArrayVector& arrays)
      : start_(start),
        verify_(verify),
        batch_length_(arrays[0]->length()),
        total_records_(total_records),
        records_sent_(0),
        schema_(schema),
        mapper_(*schema),
        arrays_(arrays) {
    batch_ = RecordBatch::Make(schema, batch_length_, arrays_);
  }

  std::shared_ptr<Schema> schema() override { return schema_; }

  arrow::Result<FlightPayload> GetSchemaPayload() override {
    FlightPayload payload;
    RETURN_NOT_OK(
        ipc::GetSchemaPayload(*schema_, ipc_options_, mapper_, &payload.ipc_message));
    return payload;
  }

  arrow::Result<FlightPayload> Next() override {
    FlightPayload payload;
    if (records_sent_ >= total_records_) {
      // Signal that iteration is over
      payload.ipc_message.metadata = nullptr;
      return payload;
    }

    if (verify_) {
      // mutate first array
      auto data =
          reinterpret_cast<int64_t*>(arrays_[0]->data()->buffers[1]->mutable_data());
      for (int64_t i = 0; i < batch_length_; ++i) {
        data[i] = start_ + records_sent_ + i;
      }
    }

    auto batch = batch_;

    // Last partial batch
    if (records_sent_ + batch_length_ > total_records_) {
      batch = batch_->Slice(0, total_records_ - records_sent_);
      records_sent_ += total_records_ - records_sent_;
    } else {
      records_sent_ += batch_length_;
    }
    RETURN_NOT_OK(ipc::GetRecordBatchPayload(*batch, ipc_options_, &payload.ipc_message));
    return payload;
  }

 private:
  const int64_t start_;
  bool verify_;
  const int64_t batch_length_;
  const int64_t total_records_;
  int64_t records_sent_;
  std::shared_ptr<Schema> schema_;
  ipc::DictionaryFieldMapper mapper_;
  ipc::IpcWriteOptions ipc_options_;
  std::shared_ptr<RecordBatch> batch_;
  ArrayVector arrays_;
};

Status GetPerfBatches(const perf::Token& token, const std::shared_ptr<Schema>& schema,
                      bool use_verifier, std::unique_ptr<FlightDataStream>* data_stream) {
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

  *data_stream = std::unique_ptr<FlightDataStream>(
      new PerfDataStream(use_verifier, token.start(),
                         token.definition().records_per_stream(), schema, arrays));
  return Status::OK();
}

class FlightPerfServer : public FlightServerBase {
 public:
  FlightPerfServer() : location_() {
    perf_schema_ = schema({field("a", int64()), field("b", int64()), field("c", int64()),
                           field("d", int64())});
  }

  void SetLocation(Location location) { location_ = location; }

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    perf::Perf perf_request;
    CHECK_PARSE(perf_request.ParseFromString(request.cmd));

    perf::Token token;
    token.mutable_definition()->CopyFrom(perf_request);

    std::vector<FlightEndpoint> endpoints;
    Ticket tmp_ticket;
    for (int64_t i = 0; i < perf_request.stream_count(); ++i) {
      token.set_start(i * perf_request.records_per_stream());
      token.set_end((i + 1) * perf_request.records_per_stream());

      (void)token.SerializeToString(&tmp_ticket.ticket);

      // All endpoints same location for now
      endpoints.push_back(FlightEndpoint{tmp_ticket, {location_}});
    }

    uint64_t total_records =
        perf_request.stream_count() * perf_request.records_per_stream();

    *info = std::make_unique<FlightInfo>(
        MakeFlightInfo(*perf_schema_, request, endpoints, total_records, -1));
    return Status::OK();
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    perf::Token token;
    CHECK_PARSE(token.ParseFromString(request.ticket));
    // This must also be set in flight_benchmark.cc
    return GetPerfBatches(token, perf_schema_, /*verify=*/false, data_stream);
  }

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    FlightStreamChunk chunk;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
      if (!chunk.data) break;
      if (chunk.app_metadata) {
        RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
      }
    }
    return Status::OK();
  }

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    if (action.type == "ping") {
      std::shared_ptr<Buffer> buf = Buffer::FromString("ok");
      *result = std::make_unique<SimpleResultStream>(std::vector<Result>{Result{buf}});
      return Status::OK();
    }
    return Status::NotImplemented(action.type);
  }

 private:
  Location location_;
  std::shared_ptr<Schema> perf_schema_;
};

}  // namespace flight
}  // namespace arrow

std::unique_ptr<arrow::flight::FlightPerfServer> g_server;

void Shutdown(int signal) {
  if (g_server != nullptr) {
    ARROW_CHECK_OK(g_server->Shutdown());
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  g_server.reset(new arrow::flight::FlightPerfServer);

  arrow::flight::Location bind_location;
  arrow::flight::Location connect_location;
  if (FLAGS_transport == "grpc") {
    if (FLAGS_server_unix.empty()) {
      if (!FLAGS_cert_file.empty() || !FLAGS_key_file.empty()) {
        if (!FLAGS_cert_file.empty() && !FLAGS_key_file.empty()) {
          ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTls("0.0.0.0", FLAGS_port)
                             .Value(&bind_location));
          ARROW_CHECK_OK(
              arrow::flight::Location::ForGrpcTls(FLAGS_server_host, FLAGS_port)
                  .Value(&connect_location));
        } else {
          std::cerr << "If providing TLS cert/key, must provide both" << std::endl;
          return EXIT_FAILURE;
        }
      } else {
        ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port)
                           .Value(&bind_location));
        ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp(FLAGS_server_host, FLAGS_port)
                           .Value(&connect_location));
      }
    } else {
      ARROW_CHECK_OK(
          arrow::flight::Location::ForGrpcUnix(FLAGS_server_unix).Value(&bind_location));
      ARROW_CHECK_OK(arrow::flight::Location::ForGrpcUnix(FLAGS_server_unix)
                         .Value(&connect_location));
    }
  } else if (FLAGS_transport == "ucx") {
#ifdef ARROW_WITH_UCX
    arrow::flight::transport::ucx::InitializeFlightUcx();
    if (FLAGS_server_unix.empty()) {
      if (!FLAGS_cert_file.empty() || !FLAGS_key_file.empty()) {
        std::cerr << "Transport does not support TLS: " << FLAGS_transport << std::endl;
        return EXIT_FAILURE;
      }
      ARROW_CHECK_OK(arrow::flight::Location::Parse("ucx://" + FLAGS_server_host + ":" +
                                                    std::to_string(FLAGS_port))
                         .Value(&bind_location));
      ARROW_CHECK_OK(arrow::flight::Location::Parse("ucx://" + FLAGS_server_host + ":" +
                                                    std::to_string(FLAGS_port))
                         .Value(&connect_location));
    } else {
      std::cerr << "Transport does not support domain sockets: " << FLAGS_transport
                << std::endl;
      return EXIT_FAILURE;
    }
#else
    std::cerr << "Not built with transport: " << FLAGS_transport << std::endl;
    return EXIT_FAILURE;
#endif
  } else {
    std::cerr << "Unknown transport: " << FLAGS_transport << std::endl;
    return EXIT_FAILURE;
  }
  arrow::flight::FlightServerOptions options(bind_location);
  if (!FLAGS_cert_file.empty() && !FLAGS_key_file.empty()) {
    std::cout << "Enabling TLS" << std::endl;
    std::ifstream cert_file(FLAGS_cert_file);
    std::string cert((std::istreambuf_iterator<char>(cert_file)),
                     (std::istreambuf_iterator<char>()));
    std::ifstream key_file(FLAGS_key_file);
    std::string key((std::istreambuf_iterator<char>(key_file)),
                    (std::istreambuf_iterator<char>()));
    options.tls_certificates.push_back(arrow::flight::CertKeyPair{cert, key});
  }

  if (FLAGS_cuda) {
#ifdef ARROW_CUDA
    arrow::cuda::CudaDeviceManager* manager = nullptr;
    std::shared_ptr<arrow::cuda::CudaDevice> device;

    ARROW_CHECK_OK(arrow::cuda::CudaDeviceManager::Instance().Value(&manager));
    ARROW_CHECK_OK(manager->GetDevice(0).Value(&device));
    options.memory_manager = device->default_memory_manager();
#else
    std::cerr << "-cuda requires that Arrow is built with ARROW_CUDA" << std::endl;
    return EXIT_FAILURE;
#endif
  }

  ARROW_CHECK_OK(g_server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(g_server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Server transport: " << FLAGS_transport << std::endl;
  std::cout << "Server location: " << connect_location.ToString() << std::endl;
  if (FLAGS_server_unix.empty()) {
    std::cout << "Server host: " << FLAGS_server_host << std::endl;
    std::cout << "Server port: " << FLAGS_port << std::endl;
  } else {
    std::cout << "Server unix socket: " << FLAGS_server_unix << std::endl;
  }
  g_server->SetLocation(connect_location);
  ARROW_CHECK_OK(g_server->Serve());
  return EXIT_SUCCESS;
}
