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
#include "arrow/flight/flight_data_decoder.h"
#include "arrow/flight/server.h"
#include "arrow/flight/transport/grpc/customize_grpc.h"
// Currently used for `SliceFromBuffer` and `WrapGrpcBuffer`.
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
      // Should we move this out of the internal files and expose as
      // utility to the users?
      slices.push_back(SliceFromBuffer(buf).ValueOrDie());
    }
    write_buf_ = ::grpc::ByteBuffer(slices.data(), slices.size());

    StartWrite(&write_buf_);
  }

  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
  std::unique_ptr<FlightDataStream> data_stream_;
};

// A listener that counts received batches, only for PoC purposes.
class DoPutListener : public arrow::flight::FlightDataListener {
 public:
  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema) override {
    schema_ = std::move(schema);
    return arrow::Status::OK();
  }

  arrow::Status OnNext(arrow::flight::FlightStreamChunk chunk) override {
    if (chunk.data) {
      ++batches_received_;
    }
    return arrow::Status::OK();
  }

  int batches_received() const { return batches_received_; }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  int batches_received_ = 0;
};

class DoPutReactor : public ::grpc::ServerGenericBidiReactor {
 public:
  DoPutReactor() : decoder_(std::make_shared<DoPutListener>()) {
    StartRead(&request_buf_);
  }

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

    // Extract Arrow buffers from the gRPC ByteBuffer, then feed it to
    // the FlightMessageDecoder which fires Listener callbacks
    // (OnSchemaDecoded / OnNext).
    std::shared_ptr<arrow::Buffer> arrow_buf;
    // TODO: What do we do with the WrapGrpcBuffer? This is internal and
    // we don't want to expose it but it's quite complex to leave it to the
    // user to implement. Similar to SliceFromBuffer, should we move this out
    // of the serialization_internal file and expose as a utility to the user?
    auto wrap_status = WrapGrpcBuffer(&request_buf_, &arrow_buf);
    if (!wrap_status.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL,
                            "Failed to wrap gRPC buffer: " + wrap_status.message()));
      return;
    }

    // Push the buffer into the decoder, which will fire the appropriate calls to the
    // listener.
    auto decode_status = decoder_.Consume(std::move(arrow_buf));
    if (!decode_status.ok()) {
      Finish(::grpc::Status(::grpc::StatusCode::INTERNAL,
                            "Failed to decode Arrow buffer: " + decode_status.message()));
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
  arrow::flight::FlightMessageDecoder decoder_;
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
