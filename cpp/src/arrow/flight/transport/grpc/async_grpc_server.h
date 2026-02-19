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

// Async gRPC-based. This is a PoC.

#pragma once

#include <grpcpp/generic/async_generic_service.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport/grpc/customize_grpc.h"
#include "arrow/flight/transport/grpc/serialization_internal.h"
#include "arrow/record_batch.h"

namespace arrow::flight::transport::grpc {

namespace pb = arrow::flight::protocol;

// DoGet using gRPC's generic callback API with ServerGenericBidiReactor.
class DoGetReactor : public ::grpc::ServerGenericBidiReactor {
 public:
  DoGetReactor() { StartRead(&request_buf_); }

  void OnReadDone(bool ok) override {
    // Request has been read.
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Failed to read request"));
      return;
    }

    // DoGet request must contain the Ticket.
    // TODO Parse ticket, we do not care about it in this PoC.
    WriteNextPayload();
  }

  void OnWriteDone(bool ok) override {
    // We have finished writing. We can write the next payload or finish the stream.
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Write failed"));
      return;
    }
    WriteNextPayload();
  }

  void OnCancel() override {
    // Client cancelled the RPC. We must implement this out of the PoC.
  }

  void OnDone() override { delete this; }

 private:
  void WriteNextPayload() {
    FlightPayload payload;
    if (data_stream_ == nullptr) {
      auto schema = arrow::schema(
          {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
      arrow::Int64Builder builder_a, builder_b;
      (void)builder_a.AppendValues({1, 2, 3, 4, 5});
      (void)builder_b.AppendValues({10, 20, 30, 40, 50});
      auto arr_a = *builder_a.Finish();
      auto arr_b = *builder_b.Finish();
      auto batch = arrow::RecordBatch::Make(schema, 5, {arr_a, arr_b});
      auto reader =
          RecordBatchReader::Make({batch, batch, batch, batch, batch}).ValueOrDie();
      data_stream_ = std::make_unique<RecordBatchStream>(std::move(reader));
      payload = data_stream_->GetSchemaPayload().ValueOrDie();
    } else {
      payload = data_stream_->Next().ValueOrDie();
    }

    if (payload.ipc_message.metadata == nullptr) {
      Finish(::grpc::Status::OK);
      return;
    }

    auto buffers = payload.SerializeToBuffers().ValueOrDie();
    std::vector<::grpc::Slice> slices;
    slices.reserve(buffers.size());
    for (const auto& buf : buffers) {
      slices.push_back(SliceFromBuffer(buf).ValueOrDie());
    }
    write_buf_ = ::grpc::ByteBuffer(slices.data(), slices.size());

    StartWrite(&write_buf_);
  }

  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
  std::unique_ptr<FlightDataStream> data_stream_;
};

class DoPutReactor : public ::grpc::ServerGenericBidiReactor {
 public:
  DoPutReactor() { StartRead(&request_buf_); }

  void OnReadDone(bool ok) override {
    // Request has been read.
    if (!ok) {
      // End of stream, ack completion.
      pb::PutResult pb_result;
      ::grpc::Slice slice(pb_result.SerializeAsString());
      // Not use a local variable for the ByteBuffer as
      // StartWrite requires the buffer to remain valid until OnWriteDone.
      write_buf_ = ::grpc::ByteBuffer(&slice, 1);
      StartWrite(&write_buf_);
      return;
    }
    // DoPut requests are a stream of FlightData messages.
    // Deserialize directly from the grpc::ByteBuffer. This is the key
    // we are trying to test in this PoC.
    arrow::flight::internal::FlightData flight_data;
    auto status = FlightDataDeserialize(&request_buf_, &flight_data);
    if (!status.ok()) {
      Finish(status);
      return;
    }
    // Read next FlightData
    StartRead(&request_buf_);
  }

  void OnWriteDone(bool ok) override {
    // We don't really write on DoPut.
    if (!ok) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL, "Write failed"));
      return;
    }
    Finish(::grpc::Status::OK);
  }

  void OnCancel() override {
    // Client cancelled the RPC. We must implement this out of the PoC.
  }

  void OnDone() override { delete this; }

 private:
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
};

class FlightCallbackService : public ::grpc::CallbackGenericService {
 public:
  ::grpc::ServerGenericBidiReactor* CreateReactor(
      ::grpc::GenericCallbackServerContext* ctx) override {
    const std::string& method = ctx->method();
    if (method == "/arrow.flight.protocol.FlightService/DoGet") {
      return new DoGetReactor();
    }
    if (method == "/arrow.flight.protocol.FlightService/DoPut") {
      return new DoPutReactor();
    }
    // Reject unknown methods
    class Unimplemented : public ::grpc::ServerGenericBidiReactor {
     public:
      Unimplemented() { Finish(::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "")); }
      void OnDone() override { delete this; }
    };
    return new Unimplemented();
  }
};

}  // namespace arrow::flight::transport::grpc
