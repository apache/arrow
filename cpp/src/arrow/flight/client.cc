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
#include "arrow/flight/protocol-internal.h"  // IWYU pragma: keep

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>

#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/metadata-internal.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/serialization-internal.h"
#include "arrow/flight/types.h"

namespace pb = arrow::flight::protocol;

namespace arrow {

class MemoryPool;

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
    internal::FlightData data;

    if (stream_finished_) {
      *out = nullptr;
      return Status::OK();
    }

    // Pretend to be pb::FlightData and intercept in SerializationTraits
    if (stream_->Read(reinterpret_cast<pb::FlightData*>(&data))) {
      std::unique_ptr<ipc::Message> message;

      // Validate IPC message
      RETURN_NOT_OK(ipc::Message::Open(data.metadata, data.body, &message));
      // The first message is a schema; read it and then try to read a
      // record batch.
      if (message->type() == ipc::Message::Type::SCHEMA) {
        RETURN_NOT_OK(ipc::ReadSchema(*message, &schema_));
        return ReadNext(out);
      } else if (message->type() == ipc::Message::Type::RECORD_BATCH) {
        return ipc::ReadRecordBatch(*message, schema_, out);
      } else {
        return Status(StatusCode::Invalid, "Unrecognized message in Flight stream");
      }
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

/// \brief A RecordBatchWriter implementation that writes to a Flight
/// DoPut stream.
class FlightPutWriter::FlightPutWriterImpl : public ipc::RecordBatchWriter {
 public:
  explicit FlightPutWriterImpl(std::unique_ptr<ClientRpc> rpc,
                               const FlightDescriptor& descriptor,
                               const std::shared_ptr<Schema>& schema,
                               MemoryPool* pool = default_memory_pool())
      : rpc_(std::move(rpc)), descriptor_(descriptor), schema_(schema), pool_(pool) {}

  Status WriteRecordBatch(const RecordBatch& batch, bool allow_64bit = false) override {
    FlightPayload payload;
    RETURN_NOT_OK(
        ipc::internal::GetRecordBatchPayload(batch, pool_, &payload.ipc_message));

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
    if (!writer_->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                        grpc::WriteOptions())) {
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
      std::stringstream ss;
      ss << "Could not write record batch to stream: "
         << rpc_->context.debug_error_string();
      return Status::IOError(ss.str());
    }
    return Status::OK();
  }

  Status Close() override {
    bool finished_writes = writer_->WritesDone();
    RETURN_NOT_OK(internal::FromGrpcStatus(writer_->Finish()));
    if (!finished_writes) {
      return Status::UnknownError(
          "Could not finish writing record batches before closing");
    }
    return Status::OK();
  }

  void set_memory_pool(MemoryPool* pool) override { pool_ = pool; }

 private:
  /// \brief Set the gRPC writer backing this Flight stream.
  /// \param [in] writer the gRPC writer
  void set_stream(std::unique_ptr<grpc::ClientWriter<pb::FlightData>> writer) {
    writer_ = std::move(writer);
  }

  // TODO: there isn't a way to access this as a user.
  protocol::PutResult response;
  std::unique_ptr<ClientRpc> rpc_;
  FlightDescriptor descriptor_;
  std::shared_ptr<Schema> schema_;
  std::unique_ptr<grpc::ClientWriter<pb::FlightData>> writer_;
  MemoryPool* pool_;

  // We need to reference some fields
  friend class FlightClient;
};

FlightPutWriter::~FlightPutWriter() {}

FlightPutWriter::FlightPutWriter(std::unique_ptr<FlightPutWriterImpl> impl) {
  impl_ = std::move(impl);
}

Status FlightPutWriter::WriteRecordBatch(const RecordBatch& batch, bool allow_64bit) {
  return impl_->WriteRecordBatch(batch, allow_64bit);
}

Status FlightPutWriter::Close() { return impl_->Close(); }

void FlightPutWriter::set_memory_pool(MemoryPool* pool) {
  return impl_->set_memory_pool(pool);
}

class FlightClient::FlightClientImpl {
 public:
  Status Connect(const std::string& host, int port) {
    // TODO(wesm): Support other kinds of GRPC ChannelCredentials
    std::stringstream ss;
    ss << host << ":" << port;
    std::string uri = ss.str();

    grpc::ChannelArguments args;
    // Try to reconnect quickly at first, in case the server is still starting up
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    stub_ = pb::FlightService::NewStub(
        grpc::CreateCustomChannel(ss.str(), grpc::InsecureChannelCredentials(), args));
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

  Status DoPut(const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema,
               std::unique_ptr<ipc::RecordBatchWriter>* stream) {
    std::unique_ptr<ClientRpc> rpc(new ClientRpc);
    std::unique_ptr<FlightPutWriter::FlightPutWriterImpl> out(
        new FlightPutWriter::FlightPutWriterImpl(std::move(rpc), descriptor, schema));
    std::unique_ptr<grpc::ClientWriter<pb::FlightData>> write_stream(
        stub_->DoPut(&out->rpc_->context, &out->response));

    // First write the descriptor and schema to the stream.
    FlightPayload payload;
    ipc::DictionaryMemo dictionary_memo;
    RETURN_NOT_OK(ipc::internal::GetSchemaPayload(*schema, out->pool_, &dictionary_memo,
                                                  &payload.ipc_message));
    pb::FlightDescriptor pb_descr;
    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descr));
    std::string str_descr;
    pb_descr.SerializeToString(&str_descr);
    RETURN_NOT_OK(Buffer::FromString(str_descr, &payload.descriptor));

#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif
    if (!write_stream->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                             grpc::WriteOptions())) {
#ifndef _WIN32
#pragma GCC diagnostic pop
#endif
      std::stringstream ss;
      ss << "Could not write descriptor and schema to stream: "
         << rpc->context.debug_error_string();
      return Status::IOError(ss.str());
    }

    out->set_stream(std::move(write_stream));
    *stream =
        std::unique_ptr<ipc::RecordBatchWriter>(new FlightPutWriter(std::move(out)));
    return Status::OK();
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

Status FlightClient::DoPut(const FlightDescriptor& descriptor,
                           const std::shared_ptr<Schema>& schema,
                           std::unique_ptr<ipc::RecordBatchWriter>* stream) {
  return impl_->DoPut(descriptor, schema, stream);
}

}  // namespace flight
}  // namespace arrow
