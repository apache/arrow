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
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/array.h"
#include "arrow/io/test-common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"

#include "arrow/flight/perf.pb.h"
#include "arrow/flight/server.h"
#include "arrow/flight/test-util.h"

DEFINE_int32(port, 31337, "Server port to listen on");

namespace perf = arrow::flight::perf;
namespace proto = arrow::flight::protocol;

using IpcPayload = arrow::ipc::internal::IpcPayload;

namespace arrow {
namespace flight {

#define CHECK_PARSE(EXPR)                              \
  do {                                                 \
    if (!EXPR) {                                       \
      return Status::Invalid("cannot parse protobuf"); \
    }                                                  \
  } while (0)

using ArrayVector = std::vector<std::shared_ptr<Array>>;

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
        arrays_(arrays) {
    batch_ = RecordBatch::Make(schema, batch_length_, arrays_);
  }

  Status Next(IpcPayload* payload) override {
    if (records_sent_ >= total_records_) {
      // Signal that iteration is over
      payload->metadata = nullptr;
      return Status::OK();
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
    return ipc::internal::GetRecordBatchPayload(*batch, default_memory_pool(), payload);
  }

 private:
  const int64_t start_;
  bool verify_;
  const int64_t batch_length_;
  const int64_t total_records_;
  int64_t records_sent_;
  std::shared_ptr<Schema> schema_;
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
    RETURN_NOT_OK(MakeRandomBuffer<int64_t>(length, default_memory_pool(), &buffer));
    arrays.push_back(std::make_shared<Int64Array>(length, buffer));
  }

  *data_stream = std::unique_ptr<FlightDataStream>(
      new PerfDataStream(use_verifier, token.start(),
                         token.definition().records_per_stream(), schema, arrays));
  return Status::OK();
}

class FlightPerfServer : public FlightServerBase {
 public:
  FlightPerfServer() : location_(Location{"localhost", FLAGS_port}) {
    perf_schema_ = schema({field("a", int64()), field("b", int64()), field("c", int64()),
                           field("d", int64())});
  }

  Status GetFlightInfo(const FlightDescriptor& request,
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

    FlightInfo::Data data;
    RETURN_NOT_OK(
        MakeFlightInfo(*perf_schema_, request, endpoints, total_records, -1, &data));
    *info = std::unique_ptr<FlightInfo>(new FlightInfo(data));
    return Status::OK();
  }

  Status DoGet(const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    perf::Token token;
    CHECK_PARSE(token.ParseFromString(request.ticket));
    return GetPerfBatches(token, perf_schema_, false, data_stream);
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
    g_server->Shutdown();
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // SIGTERM shuts down the server
  signal(SIGTERM, Shutdown);

  g_server.reset(new arrow::flight::FlightPerfServer);

  // TODO(wesm): How can we tell if the server failed to start for some reason?
  g_server->Run(FLAGS_port);
  return 0;
}
