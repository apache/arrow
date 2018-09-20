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

#include "arrow/flight/client.h"

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/wire_format_lite.h"
#include "grpc/byte_buffer_reader.h"
#include "grpcpp/grpcpp.h"

#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#include "arrow/flight/Flight.grpc.pb.h"
#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/internal.h"

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {

/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
struct FlightData {
  /// Used only for puts, may be null
  std::unique_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Message body
  std::shared_ptr<Buffer> body;
};

}  // namespace flight
}  // namespace arrow

namespace grpc {

// Customizations to gRPC for more efficient deserialization of FlightData

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::CodedInputStream;

using arrow::flight::FlightData;

bool ReadBytesZeroCopy(const std::shared_ptr<arrow::Buffer>& source_data,
                       CodedInputStream* input, std::shared_ptr<arrow::Buffer>* out) {
  uint32_t length;
  if (!input->ReadVarint32(&length)) {
    return false;
  }
  *out = arrow::SliceBuffer(source_data, input->CurrentPosition(),
                            static_cast<int64_t>(length));
  return input->Skip(static_cast<int>(length));
}

// Internal wrapper for gRPC ByteBuffer so its memory can be exposed to Arrow
// consumers with zero-copy
class GrpcBuffer : public arrow::MutableBuffer {
 public:
  GrpcBuffer(grpc_slice slice, bool incref)
      : MutableBuffer(GRPC_SLICE_START_PTR(slice),
                      static_cast<int64_t>(GRPC_SLICE_LENGTH(slice))),
        slice_(incref ? grpc_slice_ref(slice) : slice) {}

  ~GrpcBuffer() override {
    // Decref slice
    grpc_slice_unref(slice_);
  }

  static arrow::Status Wrap(ByteBuffer* cpp_buf, std::shared_ptr<arrow::Buffer>* out) {
    // These types are guaranteed by static assertions in gRPC to have the same
    // in-memory representation

    auto buffer = *reinterpret_cast<grpc_byte_buffer**>(cpp_buf);

    // This part below is based on the Flatbuffers gRPC SerializationTraits in
    // flatbuffers/grpc.h

    // Check if this is a single uncompressed slice.
    if ((buffer->type == GRPC_BB_RAW) &&
        (buffer->data.raw.compression == GRPC_COMPRESS_NONE) &&
        (buffer->data.raw.slice_buffer.count == 1)) {
      // If it is, then we can reference the `grpc_slice` directly.
      grpc_slice slice = buffer->data.raw.slice_buffer.slices[0];

      // Increment reference count so this memory remains valid
      *out = std::make_shared<GrpcBuffer>(slice, true);
    } else {
      // Otherwise, we need to use `grpc_byte_buffer_reader_readall` to read
      // `buffer` into a single contiguous `grpc_slice`. The gRPC reader gives
      // us back a new slice with the refcount already incremented.
      grpc_byte_buffer_reader reader;
      if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return arrow::Status::IOError("Internal gRPC error reading from ByteBuffer");
      }
      grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
      grpc_byte_buffer_reader_destroy(&reader);

      // Steal the slice reference
      *out = std::make_shared<GrpcBuffer>(slice, false);
    }

    return arrow::Status::OK();
  }

 private:
  grpc_slice slice_;
};

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
template <>
class SerializationTraits<FlightData> {
 public:
  static Status Serialize(const FlightData& msg, ByteBuffer** buffer, bool* own_buffer) {
    return Status(StatusCode::UNIMPLEMENTED,
                  "internal::FlightData serialization not implemented");
  }

  static Status Deserialize(ByteBuffer* buffer, FlightData* out) {
    if (!buffer) {
      return Status(StatusCode::INTERNAL, "No payload");
    }

    std::shared_ptr<arrow::Buffer> wrapped_buffer;
    GRPC_RETURN_NOT_OK(GrpcBuffer::Wrap(buffer, &wrapped_buffer));

    auto buffer_length = static_cast<int>(wrapped_buffer->size());
    CodedInputStream pb_stream(wrapped_buffer->data(), buffer_length);

    // TODO(wesm): The 2-parameter version of this function is deprecated
    pb_stream.SetTotalBytesLimit(buffer_length, -1 /* no threshold */);

    // This is the bytes remaining when using CodedInputStream like this
    while (pb_stream.BytesUntilTotalBytesLimit()) {
      const uint32_t tag = pb_stream.ReadTag();
      const int field_number = WireFormatLite::GetTagFieldNumber(tag);
      switch (field_number) {
        case pb::FlightData::kFlightDescriptorFieldNumber: {
          pb::FlightDescriptor pb_descriptor;
          if (!pb_descriptor.ParseFromCodedStream(&pb_stream)) {
            return Status(StatusCode::INTERNAL, "Unable to parse FlightDescriptor");
          }
        } break;
        case pb::FlightData::kDataHeaderFieldNumber: {
          if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->metadata)) {
            return Status(StatusCode::INTERNAL, "Unable to read FlightData metadata");
          }
        } break;
        case pb::FlightData::kDataBodyFieldNumber: {
          if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->body)) {
            return Status(StatusCode::INTERNAL, "Unable to read FlightData body");
          }
        } break;
        default:
          DCHECK(false) << "cannot happen";
      }
    }
    buffer->Clear();

    // TODO(wesm): Where and when should we verify that the FlightData is not
    // malformed or missing components?

    return Status::OK;
  }
};

}  // namespace grpc

namespace arrow {
namespace flight {

struct ClientRpc {
  grpc::ClientContext context;

  ClientRpc() {
    /// XXX workaround until we have a handshake in Connect
    context.set_wait_for_ready(true);
  }
};

class FlightStreamReader : public RecordBatchReader {
 public:
  FlightStreamReader(std::unique_ptr<ClientRpc> rpc,
                     const std::shared_ptr<Schema>& schema,
                     std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream)
      : rpc_(std::move(rpc)),
        stream_finished_(false),
        schema_(schema),
        stream_(std::move(stream)) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    FlightData data;

    if (stream_finished_) {
      *out = nullptr;
      return Status::OK();
    }

    // For customizing read path for better memory/serialization efficiency
    auto custom_reader = reinterpret_cast<grpc::ClientReader<FlightData>*>(stream_.get());

    if (custom_reader->Read(&data)) {
      std::unique_ptr<ipc::Message> message;

      // Validate IPC message
      RETURN_NOT_OK(ipc::Message::Open(data.metadata, data.body, &message));
      return ipc::ReadRecordBatch(*message, schema_, out);
    } else {
      // Stream is completed
      stream_finished_ = true;
      *out = nullptr;
      return internal::FromGrpcStatus(stream_->Finish());
    }
  }

 private:
  // The RPC context lifetime must be coupled to the ClientReader
  std::unique_ptr<ClientRpc> rpc_;

  bool stream_finished_;
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream_;
};

class FlightClient::FlightClientImpl {
 public:
  Status Connect(const std::string& host, int port) {
    // TODO(wesm): Support other kinds of GRPC ChannelCredentials
    std::stringstream ss;
    ss << host << ":" << port;
    std::string uri = ss.str();

    stub_ = pb::FlightService::NewStub(
        grpc::CreateChannel(ss.str(), grpc::InsecureChannelCredentials()));
    return Status::OK();
  }

  Status ListFlights(const Criteria& criteria, std::unique_ptr<FlightListing>* listing) {
    // TODO(wesm): populate criteria
    pb::Criteria pb_criteria;

    ClientRpc rpc;
    std::unique_ptr<grpc::ClientReader<pb::FlightGetInfo>> stream(
        stub_->ListFlights(&rpc.context, pb_criteria));

    std::vector<FlightInfo> flights;

    pb::FlightGetInfo pb_info;
    FlightInfo::Data info_data;
    while (stream->Read(&pb_info)) {
      RETURN_NOT_OK(internal::FromProto(pb_info, &info_data));
      flights.emplace_back(FlightInfo(std::move(info_data)));
    }

    listing->reset(new SimpleFlightListing(flights));
    return internal::FromGrpcStatus(stream->Finish());
  }

  Status DoAction(const Action& action, std::unique_ptr<ResultStream>* results) {
    pb::Action pb_action;
    RETURN_NOT_OK(internal::ToProto(action, &pb_action));

    ClientRpc rpc;
    std::unique_ptr<grpc::ClientReader<pb::Result>> stream(
        stub_->DoAction(&rpc.context, pb_action));

    pb::Result pb_result;

    std::vector<Result> materialized_results;
    while (stream->Read(&pb_result)) {
      Result result;
      RETURN_NOT_OK(internal::FromProto(pb_result, &result));
      materialized_results.emplace_back(std::move(result));
    }

    *results = std::unique_ptr<ResultStream>(
        new SimpleResultStream(std::move(materialized_results)));
    return internal::FromGrpcStatus(stream->Finish());
  }

  Status ListActions(std::vector<ActionType>* types) {
    pb::Empty empty;

    ClientRpc rpc;
    std::unique_ptr<grpc::ClientReader<pb::ActionType>> stream(
        stub_->ListActions(&rpc.context, empty));

    pb::ActionType pb_type;
    ActionType type;
    while (stream->Read(&pb_type)) {
      RETURN_NOT_OK(internal::FromProto(pb_type, &type));
      types->emplace_back(std::move(type));
    }
    return internal::FromGrpcStatus(stream->Finish());
  }

  Status GetFlightInfo(const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) {
    pb::FlightDescriptor pb_descriptor;
    pb::FlightGetInfo pb_response;

    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descriptor));

    ClientRpc rpc;
    Status s = internal::FromGrpcStatus(
        stub_->GetFlightInfo(&rpc.context, pb_descriptor, &pb_response));
    RETURN_NOT_OK(s);

    FlightInfo::Data info_data;
    RETURN_NOT_OK(internal::FromProto(pb_response, &info_data));
    info->reset(new FlightInfo(std::move(info_data)));
    return Status::OK();
  }

  Status DoGet(const Ticket& ticket, const std::shared_ptr<Schema>& schema,
               std::unique_ptr<RecordBatchReader>* out) {
    pb::Ticket pb_ticket;
    internal::ToProto(ticket, &pb_ticket);

    // ClientRpc rpc;
    std::unique_ptr<ClientRpc> rpc(new ClientRpc);
    std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream(
        stub_->DoGet(&rpc->context, pb_ticket));

    *out = std::unique_ptr<RecordBatchReader>(
        new FlightStreamReader(std::move(rpc), schema, std::move(stream)));
    return Status::OK();
  }

  Status DoPut(std::unique_ptr<FlightPutWriter>* stream) {
    return Status::NotImplemented("DoPut");
  }

 private:
  std::unique_ptr<pb::FlightService::Stub> stub_;
};

FlightClient::FlightClient() { impl_.reset(new FlightClientImpl); }

FlightClient::~FlightClient() {}

Status FlightClient::Connect(const std::string& host, int port,
                             std::unique_ptr<FlightClient>* client) {
  client->reset(new FlightClient);
  return (*client)->impl_->Connect(host, port);
}

Status FlightClient::DoAction(const Action& action,
                              std::unique_ptr<ResultStream>* results) {
  return impl_->DoAction(action, results);
}

Status FlightClient::ListActions(std::vector<ActionType>* actions) {
  return impl_->ListActions(actions);
}

Status FlightClient::GetFlightInfo(const FlightDescriptor& descriptor,
                                   std::unique_ptr<FlightInfo>* info) {
  return impl_->GetFlightInfo(descriptor, info);
}

Status FlightClient::ListFlights(std::unique_ptr<FlightListing>* listing) {
  return ListFlights({}, listing);
}

Status FlightClient::ListFlights(const Criteria& criteria,
                                 std::unique_ptr<FlightListing>* listing) {
  return impl_->ListFlights(criteria, listing);
}

Status FlightClient::DoGet(const Ticket& ticket, const std::shared_ptr<Schema>& schema,
                           std::unique_ptr<RecordBatchReader>* stream) {
  return impl_->DoGet(ticket, schema, stream);
}

Status FlightClient::DoPut(const Schema& schema,
                           std::unique_ptr<FlightPutWriter>* stream) {
  return Status::NotImplemented("DoPut");
}

}  // namespace flight
}  // namespace arrow
