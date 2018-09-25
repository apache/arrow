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

#include "arrow/flight/server.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/wire_format_lite.h"
#include "grpcpp/grpcpp.h"

#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/flight/Flight.grpc.pb.h"
#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"

using FlightService = arrow::flight::protocol::FlightService;
using ServerContext = grpc::ServerContext;

using arrow::ipc::internal::IpcPayload;

template <typename T>
using ServerWriter = grpc::ServerWriter<T>;

namespace pb = arrow::flight::protocol;

constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();

namespace grpc {

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::CodedOutputStream;

// More efficient writing of FlightData to gRPC output buffer
// Implementation of ZeroCopyOutputStream that writes to a fixed-size buffer
class FixedSizeProtoWriter : public ::google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit FixedSizeProtoWriter(grpc_slice slice)
      : slice_(slice),
        bytes_written_(0),
        total_size_(static_cast<int>(GRPC_SLICE_LENGTH(slice))) {}

  bool Next(void** data, int* size) override {
    // Consume the whole slice
    *data = GRPC_SLICE_START_PTR(slice_) + bytes_written_;
    *size = total_size_ - bytes_written_;
    bytes_written_ = total_size_;
    return true;
  }

  void BackUp(int count) override { bytes_written_ -= count; }

  int64_t ByteCount() const override { return bytes_written_; }

 private:
  grpc_slice slice_;
  int bytes_written_;
  int total_size_;
};

// Write FlightData to a grpc::ByteBuffer without extra copying
template <>
class SerializationTraits<IpcPayload> {
 public:
  static grpc::Status Deserialize(ByteBuffer* buffer, IpcPayload* out) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "IpcPayload deserialization not implemented");
  }

  static grpc::Status Serialize(const IpcPayload& msg, ByteBuffer* out,
                                bool* own_buffer) {
    size_t total_size = 0;

    DCHECK_LT(msg.metadata->size(), kInt32Max);
    const int32_t metadata_size = static_cast<int32_t>(msg.metadata->size());

    // 1 byte for metadata tag
    total_size += 1 + WireFormatLite::LengthDelimitedSize(metadata_size);

    int64_t body_size = 0;
    for (const auto& buffer : msg.body_buffers) {
      body_size += buffer->size();

      const int64_t remainder = buffer->size() % 8;
      if (remainder) {
        body_size += 8 - remainder;
      }
    }

    // 2 bytes for body tag
    total_size += 2 + WireFormatLite::LengthDelimitedSize(static_cast<size_t>(body_size));

    // TODO(wesm): messages over 2GB unlikely to be yet supported
    if (total_size > kInt32Max) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          "Cannot send record batches exceeding 2GB yet");
    }

    // Allocate slice, assign to output buffer
    grpc::Slice slice(total_size);

    // XXX(wesm): for debugging
    // std::cout << "Writing record batch with total size " << total_size << std::endl;

    FixedSizeProtoWriter writer(*reinterpret_cast<grpc_slice*>(&slice));
    CodedOutputStream pb_stream(&writer);

    // Write header
    WireFormatLite::WriteTag(pb::FlightData::kDataHeaderFieldNumber,
                             WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
    pb_stream.WriteVarint32(metadata_size);
    pb_stream.WriteRawMaybeAliased(msg.metadata->data(),
                                   static_cast<int>(msg.metadata->size()));

    // Write body
    WireFormatLite::WriteTag(pb::FlightData::kDataBodyFieldNumber,
                             WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
    pb_stream.WriteVarint32(static_cast<uint32_t>(body_size));

    constexpr uint8_t kPaddingBytes[8] = {0};

    for (const auto& buffer : msg.body_buffers) {
      pb_stream.WriteRawMaybeAliased(buffer->data(), static_cast<int>(buffer->size()));

      // Write padding if not multiple of 8
      const int remainder = static_cast<int>(buffer->size() % 8);
      if (remainder) {
        pb_stream.WriteRawMaybeAliased(kPaddingBytes, 8 - remainder);
      }
    }

    DCHECK_EQ(static_cast<int>(total_size), pb_stream.ByteCount());

    // Hand off the slice to the returned ByteBuffer
    grpc::ByteBuffer tmp(&slice, 1);
    out->Swap(&tmp);
    *own_buffer = true;
    return grpc::Status::OK;
  }
};

}  // namespace grpc

namespace arrow {
namespace flight {

#define CHECK_ARG_NOT_NULL(VAL, MESSAGE)                              \
  if (VAL == nullptr) {                                               \
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, MESSAGE); \
  }

// This class glues an implementation of FlightServerBase together with the
// gRPC service definition, so the latter is not exposed in the public API
class FlightServiceImpl : public FlightService::Service {
 public:
  explicit FlightServiceImpl(FlightServerBase* server) : server_(server) {}

  template <typename UserType, typename Iterator, typename ProtoType>
  grpc::Status WriteStream(Iterator* iterator, ServerWriter<ProtoType>* writer) {
    // Write flight info to stream until listing is exhausted
    ProtoType pb_value;
    std::unique_ptr<UserType> value;
    while (true) {
      GRPC_RETURN_NOT_OK(iterator->Next(&value));
      if (!value) {
        break;
      }
      GRPC_RETURN_NOT_OK(internal::ToProto(*value, &pb_value));

      // Blocking write
      if (!writer->Write(pb_value)) {
        // Write returns false if the stream is closed
        break;
      }
    }
    return grpc::Status::OK;
  }

  template <typename UserType, typename ProtoType>
  grpc::Status WriteStream(const std::vector<UserType>& values,
                           ServerWriter<ProtoType>* writer) {
    // Write flight info to stream until listing is exhausted
    ProtoType pb_value;
    for (const UserType& value : values) {
      GRPC_RETURN_NOT_OK(internal::ToProto(value, &pb_value));
      // Blocking write
      if (!writer->Write(pb_value)) {
        // Write returns false if the stream is closed
        break;
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
                           ServerWriter<pb::FlightGetInfo>* writer) {
    // Retrieve the listing from the implementation
    std::unique_ptr<FlightListing> listing;

    Criteria criteria;
    if (request) {
      GRPC_RETURN_NOT_OK(internal::FromProto(*request, &criteria));
    }
    GRPC_RETURN_NOT_OK(server_->ListFlights(&criteria, &listing));
    return WriteStream<FlightInfo>(listing.get(), writer);
  }

  grpc::Status GetFlightInfo(ServerContext* context, const pb::FlightDescriptor* request,
                             pb::FlightGetInfo* response) {
    CHECK_ARG_NOT_NULL(request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    GRPC_RETURN_NOT_OK(server_->GetFlightInfo(descr, &info));

    GRPC_RETURN_NOT_OK(internal::ToProto(*info, response));
    return grpc::Status::OK;
  }

  grpc::Status DoGet(ServerContext* context, const pb::Ticket* request,
                     ServerWriter<pb::FlightData>* writer) {
    CHECK_ARG_NOT_NULL(request, "ticket cannot be null");

    Ticket ticket;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &ticket));

    std::unique_ptr<FlightDataStream> data_stream;
    GRPC_RETURN_NOT_OK(server_->DoGet(ticket, &data_stream));

    // Requires ServerWriter customization in grpc_customizations.h
    auto custom_writer = reinterpret_cast<ServerWriter<IpcPayload>*>(writer);

    while (true) {
      IpcPayload payload;
      GRPC_RETURN_NOT_OK(data_stream->Next(&payload));
      if (payload.metadata == nullptr ||
          !custom_writer->Write(payload, grpc::WriteOptions())) {
        // No more messages to write, or connection terminated for some other
        // reason
        break;
      }
    }
    return grpc::Status::OK;
  }

  grpc::Status DoPut(ServerContext* context, grpc::ServerReader<pb::FlightData>* reader,
                     pb::PutResult* response) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "");
  }

  grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                           ServerWriter<pb::ActionType>* writer) {
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    GRPC_RETURN_NOT_OK(server_->ListActions(&types));
    return WriteStream<ActionType>(types, writer);
  }

  grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                        ServerWriter<pb::Result>* writer) {
    CHECK_ARG_NOT_NULL(request, "Action cannot be null");
    Action action;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &action));

    std::unique_ptr<ResultStream> results;
    GRPC_RETURN_NOT_OK(server_->DoAction(action, &results));

    std::unique_ptr<Result> result;
    pb::Result pb_result;
    while (true) {
      GRPC_RETURN_NOT_OK(results->Next(&result));
      if (!result) {
        // No more results
        break;
      }
      GRPC_RETURN_NOT_OK(internal::ToProto(*result, &pb_result));
      if (!writer->Write(pb_result)) {
        // Stream may be closed
        break;
      }
    }
    return grpc::Status::OK;
  }

 private:
  FlightServerBase* server_;
};

struct FlightServerBase::FlightServerBaseImpl {
  std::unique_ptr<grpc::Server> server;
};

FlightServerBase::FlightServerBase() { impl_.reset(new FlightServerBaseImpl); }

FlightServerBase::~FlightServerBase() {}

void FlightServerBase::Run(int port) {
  std::string address = "localhost:" + std::to_string(port);

  FlightServiceImpl service(this);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  impl_->server = builder.BuildAndStart();
  std::cout << "Server listening on " << address << std::endl;
  impl_->server->Wait();
}

void FlightServerBase::Shutdown() {
  DCHECK(impl_->server);
  impl_->server->Shutdown();
}

Status FlightServerBase::ListFlights(const Criteria* criteria,
                                     std::unique_ptr<FlightListing>* listings) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::GetFlightInfo(const FlightDescriptor& request,
                                       std::unique_ptr<FlightInfo>* info) {
  std::cout << "GetFlightInfo" << std::endl;
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoGet(const Ticket& request,
                               std::unique_ptr<FlightDataStream>* data_stream) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoAction(const Action& action,
                                  std::unique_ptr<ResultStream>* result) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::ListActions(std::vector<ActionType>* actions) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader)
    : pool_(default_memory_pool()), reader_(reader) {}

Status RecordBatchStream::Next(IpcPayload* payload) {
  std::shared_ptr<RecordBatch> batch;
  RETURN_NOT_OK(reader_->ReadNext(&batch));

  if (!batch) {
    // Signal that iteration is over
    payload->metadata = nullptr;
    return Status::OK();
  } else {
    return ipc::internal::GetRecordBatchPayload(*batch, pool_, payload);
  }
}

}  // namespace flight
}  // namespace arrow
