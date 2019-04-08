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

#include "arrow/util/config.h"
#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/buffer.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
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

  Status IOError(const std::string& error_message) {
    std::stringstream ss;
    ss << error_message << context.debug_error_string();
    return Status::IOError(ss.str());
  }
};

class FlightIpcMessageReader : public ipc::MessageReader {
 public:
  FlightIpcMessageReader(std::unique_ptr<ClientRpc> rpc,
                         std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream)
      : rpc_(std::move(rpc)), stream_(std::move(stream)), stream_finished_(false) {}

  Status ReadNextMessage(std::unique_ptr<ipc::Message>* out) override {
    if (stream_finished_) {
      *out = nullptr;
      return Status::OK();
    }
    internal::FlightData data;
    if (!internal::ReadPayload(stream_.get(), &data)) {
      // Stream is completed
      stream_finished_ = true;
      *out = nullptr;
      return OverrideWithServerError(Status::OK());
    }
    // Validate IPC message
    auto st = data.OpenMessage(out);
    if (!st.ok()) {
      return OverrideWithServerError(std::move(st));
    }
    return Status::OK();
  }

 protected:
  Status OverrideWithServerError(Status&& st) {
    // Get the gRPC status if not OK, to propagate any server error message
    RETURN_NOT_OK(internal::FromGrpcStatus(stream_->Finish()));
    return st;
  }

  // The RPC context lifetime must be coupled to the ClientReader
  std::unique_ptr<ClientRpc> rpc_;
  std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream_;
  bool stream_finished_;
};

/// A IpcPayloadWriter implementation that writes to a DoPut stream
class DoPutPayloadWriter : public ipc::internal::IpcPayloadWriter {
 public:
  DoPutPayloadWriter(const FlightDescriptor& descriptor, std::unique_ptr<ClientRpc> rpc,
                     std::unique_ptr<protocol::PutResult> response,
                     std::unique_ptr<grpc::ClientWriter<pb::FlightData>> writer)
      : descriptor_(descriptor),
        rpc_(std::move(rpc)),
        response_(std::move(response)),
        writer_(std::move(writer)),
        first_payload_(true) {}

  ~DoPutPayloadWriter() override = default;

  Status Start() override { return Status::OK(); }

  Status WritePayload(const ipc::internal::IpcPayload& ipc_payload) override {
    FlightPayload payload;
    payload.ipc_message = ipc_payload;

    if (first_payload_) {
      // First Flight message needs to encore the Flight descriptor
      DCHECK_EQ(ipc_payload.type, ipc::Message::SCHEMA);
      std::string str_descr;
      {
        pb::FlightDescriptor pb_descr;
        RETURN_NOT_OK(internal::ToProto(descriptor_, &pb_descr));
        if (!pb_descr.SerializeToString(&str_descr)) {
          return Status::UnknownError("Failed to serialized Flight descriptor");
        }
      }
      RETURN_NOT_OK(Buffer::FromString(str_descr, &payload.descriptor));
      first_payload_ = false;
    }

    if (!internal::WritePayload(payload, writer_.get())) {
      return rpc_->IOError("Could not write record batch to stream: ");
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

 protected:
  // TODO: there isn't a way to access this as a user.
  const FlightDescriptor descriptor_;
  std::unique_ptr<ClientRpc> rpc_;
  std::unique_ptr<protocol::PutResult> response_;
  std::unique_ptr<grpc::ClientWriter<pb::FlightData>> writer_;
  bool first_payload_;
};

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
    // Receive messages of any size
    args.SetMaxReceiveMessageSize(-1);
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
    while (stream->Read(&pb_info)) {
      FlightInfo::Data info_data;
      RETURN_NOT_OK(internal::FromProto(pb_info, &info_data));
      flights.emplace_back(std::move(info_data));
    }

    listing->reset(new SimpleFlightListing(std::move(flights)));
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

  Status DoGet(const Ticket& ticket, std::unique_ptr<RecordBatchReader>* out) {
    pb::Ticket pb_ticket;
    internal::ToProto(ticket, &pb_ticket);

    std::unique_ptr<ClientRpc> rpc(new ClientRpc);
    std::unique_ptr<grpc::ClientReader<pb::FlightData>> stream(
        stub_->DoGet(&rpc->context, pb_ticket));

    std::unique_ptr<ipc::MessageReader> message_reader(
        new FlightIpcMessageReader(std::move(rpc), std::move(stream)));
    return ipc::RecordBatchStreamReader::Open(std::move(message_reader), out);
  }

  Status DoPut(const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema,
               std::unique_ptr<ipc::RecordBatchWriter>* out) {
    std::unique_ptr<ClientRpc> rpc(new ClientRpc);
    std::unique_ptr<protocol::PutResult> response(new protocol::PutResult);
    std::unique_ptr<grpc::ClientWriter<pb::FlightData>> writer(
        stub_->DoPut(&rpc->context, response.get()));

    std::unique_ptr<ipc::internal::IpcPayloadWriter> payload_writer(
        new DoPutPayloadWriter(descriptor, std::move(rpc), std::move(response),
                               std::move(writer)));

    return ipc::internal::OpenRecordBatchWriter(std::move(payload_writer), schema, out);
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

Status FlightClient::DoGet(const Ticket& ticket,
                           std::unique_ptr<RecordBatchReader>* stream) {
  return impl_->DoGet(ticket, stream);
}

Status FlightClient::DoPut(const FlightDescriptor& descriptor,
                           const std::shared_ptr<Schema>& schema,
                           std::unique_ptr<ipc::RecordBatchWriter>* stream) {
  return impl_->DoPut(descriptor, schema, stream);
}

}  // namespace flight
}  // namespace arrow
