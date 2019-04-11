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

#include <signal.h>
#include <atomic>
#include <cstdint>
#include <memory>
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
#include "arrow/util/logging.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/serialization-internal.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/types.h"

using FlightService = arrow::flight::protocol::FlightService;
using ServerContext = grpc::ServerContext;

template <typename T>
using ServerWriter = grpc::ServerWriter<T>;

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {

#define CHECK_ARG_NOT_NULL(VAL, MESSAGE)                              \
  if (VAL == nullptr) {                                               \
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, MESSAGE); \
  }

class FlightMessageReaderImpl : public FlightMessageReader {
 public:
  FlightMessageReaderImpl(const FlightDescriptor& descriptor,
                          std::shared_ptr<Schema> schema,
                          grpc::ServerReader<pb::FlightData>* reader)
      : descriptor_(descriptor),
        schema_(schema),
        reader_(reader),
        stream_finished_(false) {}

  const FlightDescriptor& descriptor() const override { return descriptor_; }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    if (stream_finished_) {
      *out = nullptr;
      return Status::OK();
    }

    internal::FlightData data;
    if (internal::ReadPayload(reader_, &data)) {
      std::unique_ptr<ipc::Message> message;

      // Validate IPC message
      RETURN_NOT_OK(data.OpenMessage(&message));
      if (message->type() == ipc::Message::Type::RECORD_BATCH) {
        return ipc::ReadRecordBatch(*message, schema_, out);
      } else {
        return Status(StatusCode::Invalid, "Unrecognized message in Flight stream");
      }
    } else {
      // Stream is completed
      stream_finished_ = true;
      *out = nullptr;
      return Status::OK();
    }
  }

 private:
  FlightDescriptor descriptor_;
  std::shared_ptr<Schema> schema_;
  grpc::ServerReader<pb::FlightData>* reader_;
  bool stream_finished_;
};

class GrpcServerAuthReader : public ServerAuthReader {
 public:
  explicit GrpcServerAuthReader(
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
      : stream_(stream) {}

  Status Read(std::string* token) override {
    pb::HandshakeRequest request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.release_payload());
      return Status::OK();
    }
    return Status::UnknownError("Could not read client handshake request.");
  }

 private:
  grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

class GrpcServerAuthSender : public ServerAuthSender {
 public:
  explicit GrpcServerAuthSender(
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
      : stream_(stream) {}

  Status Write(const std::string& token) override {
    pb::HandshakeResponse response;
    response.set_payload(token);
    if (stream_->Write(response)) {
      return Status::OK();
    }
    return Status::IOError("Stream was closed.");
  }

 private:
  grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

class FlightServiceImpl;
class GrpcServerCallContext : public ServerCallContext {
  const std::string& peer_identity() const override { return peer_identity_; }

 private:
  friend class FlightServiceImpl;
  ServerContext* context_;
  std::string peer_identity_;
};

// This class glues an implementation of FlightServerBase together with the
// gRPC service definition, so the latter is not exposed in the public API
class FlightServiceImpl : public FlightService::Service {
 public:
  explicit FlightServiceImpl(std::shared_ptr<ServerAuthHandler> auth_handler,
                             FlightServerBase* server)
      : auth_handler_(auth_handler), server_(server) {}

  template <typename UserType, typename Iterator, typename ProtoType>
  grpc::Status WriteStream(Iterator* iterator, ServerWriter<ProtoType>* writer) {
    if (!iterator) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "No items to iterate");
    }
    // Write flight info to stream until listing is exhausted
    while (true) {
      ProtoType pb_value;
      std::unique_ptr<UserType> value;
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
    for (const UserType& value : values) {
      ProtoType pb_value;
      GRPC_RETURN_NOT_OK(internal::ToProto(value, &pb_value));
      // Blocking write
      if (!writer->Write(pb_value)) {
        // Write returns false if the stream is closed
        break;
      }
    }
    return grpc::Status::OK;
  }

  // Authenticate the client (if applicable) and construct the call context
  grpc::Status CheckAuth(ServerContext* context, GrpcServerCallContext& flight_context) {
    flight_context.context_ = context;
    if (!auth_handler_) {
      flight_context.peer_identity_ = "";
      return grpc::Status::OK;
    }

    const auto client_metadata = context->client_metadata();
    const auto auth_header = client_metadata.find(internal::AUTH_HEADER);
    std::string token;
    if (auth_header == client_metadata.end()) {
      token = "";
    } else {
      token = std::string(auth_header->second.data(), auth_header->second.length());
    }
    GRPC_RETURN_NOT_OK(auth_handler_->IsValid(token, &flight_context.peer_identity_));
    return grpc::Status::OK;
  }

  grpc::Status Handshake(
      ServerContext* context,
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream) {
    if (!auth_handler_) {
      return grpc::Status(
          grpc::StatusCode::UNIMPLEMENTED,
          "This service does not have an authentication mechanism enabled.");
    }
    GrpcServerAuthSender outgoing{stream};
    GrpcServerAuthReader incoming{stream};
    GRPC_RETURN_NOT_OK(auth_handler_->Authenticate(&outgoing, &incoming));
    return grpc::Status::OK;
  }

  grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
                           ServerWriter<pb::FlightInfo>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    // Retrieve the listing from the implementation
    std::unique_ptr<FlightListing> listing;

    Criteria criteria;
    if (request) {
      GRPC_RETURN_NOT_OK(internal::FromProto(*request, &criteria));
    }
    GRPC_RETURN_NOT_OK(server_->ListFlights(flight_context, &criteria, &listing));
    if (!listing) {
      // Treat null listing as no flights available
      return grpc::Status::OK;
    }
    return WriteStream<FlightInfo>(listing.get(), writer);
  }

  grpc::Status GetFlightInfo(ServerContext* context, const pb::FlightDescriptor* request,
                             pb::FlightInfo* response) {
    CHECK_ARG_NOT_NULL(request, "FlightDescriptor cannot be null");
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    FlightDescriptor descr;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    GRPC_RETURN_NOT_OK(server_->GetFlightInfo(flight_context, descr, &info));

    if (!info) {
      // Treat null listing as no flights available
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "Flight not found");
    }

    GRPC_RETURN_NOT_OK(internal::ToProto(*info, response));
    return grpc::Status::OK;
  }

  grpc::Status DoGet(ServerContext* context, const pb::Ticket* request,
                     ServerWriter<pb::FlightData>* writer) {
    CHECK_ARG_NOT_NULL(request, "ticket cannot be null");
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));

    Ticket ticket;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &ticket));

    std::unique_ptr<FlightDataStream> data_stream;
    GRPC_RETURN_NOT_OK(server_->DoGet(flight_context, ticket, &data_stream));

    if (!data_stream) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "No data in this flight");
    }

    // Write the schema as the first message(s) in the stream
    // (several messages may be required if there are dictionaries)
    MemoryPool* pool = default_memory_pool();
    std::vector<ipc::internal::IpcPayload> ipc_payloads;
    GRPC_RETURN_NOT_OK(
        ipc::internal::GetSchemaPayloads(*data_stream->schema(), pool, &ipc_payloads));

    for (auto& ipc_payload : ipc_payloads) {
      // For DoGet, descriptor doesn't need to be written out
      FlightPayload schema_payload;
      schema_payload.ipc_message = std::move(ipc_payload);

      if (!internal::WritePayload(schema_payload, writer)) {
        // Connection terminated?  XXX return error code?
        return grpc::Status::OK;
      }
    }

    // Write incoming data as individual messages
    while (true) {
      FlightPayload payload;
      GRPC_RETURN_NOT_OK(data_stream->Next(&payload));
      if (payload.ipc_message.metadata == nullptr ||
          !internal::WritePayload(payload, writer))
        // No more messages to write, or connection terminated for some other
        // reason
        break;
    }
    return grpc::Status::OK;
  }

  grpc::Status DoPut(ServerContext* context, grpc::ServerReader<pb::FlightData>* reader,
                     pb::PutResult* response) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));
    // Get metadata
    internal::FlightData data;
    if (internal::ReadPayload(reader, &data)) {
      // Message only lives as long as data
      std::unique_ptr<ipc::Message> message;
      GRPC_RETURN_NOT_OK(data.OpenMessage(&message));

      if (!message || message->type() != ipc::Message::Type::SCHEMA) {
        return internal::ToGrpcStatus(
            Status(StatusCode::Invalid, "DoPut must start with schema/descriptor"));
      } else if (!data.descriptor) {
        return internal::ToGrpcStatus(
            Status(StatusCode::Invalid, "DoPut must start with non-null descriptor"));
      } else {
        std::shared_ptr<Schema> schema;
        GRPC_RETURN_NOT_OK(ipc::ReadSchema(*message, &schema));

        auto message_reader = std::unique_ptr<FlightMessageReader>(
            new FlightMessageReaderImpl(*data.descriptor.get(), schema, reader));
        return internal::ToGrpcStatus(
            server_->DoPut(flight_context, std::move(message_reader)));
      }
    } else {
      return internal::ToGrpcStatus(
          Status(StatusCode::Invalid,
                 "Client provided malformed message or did not provide message"));
    }
  }

  grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                           ServerWriter<pb::ActionType>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    GRPC_RETURN_NOT_OK(server_->ListActions(flight_context, &types));
    return WriteStream<ActionType>(types, writer);
  }

  grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                        ServerWriter<pb::Result>* writer) {
    GrpcServerCallContext flight_context;
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(context, flight_context));
    CHECK_ARG_NOT_NULL(request, "Action cannot be null");
    Action action;
    GRPC_RETURN_NOT_OK(internal::FromProto(*request, &action));

    std::unique_ptr<ResultStream> results;
    GRPC_RETURN_NOT_OK(server_->DoAction(flight_context, action, &results));

    if (!results) {
      return grpc::Status::CANCELLED;
    }

    while (true) {
      std::unique_ptr<Result> result;
      GRPC_RETURN_NOT_OK(results->Next(&result));
      if (!result) {
        // No more results
        break;
      }
      pb::Result pb_result;
      GRPC_RETURN_NOT_OK(internal::ToProto(*result, &pb_result));
      if (!writer->Write(pb_result)) {
        // Stream may be closed
        break;
      }
    }
    return grpc::Status::OK;
  }

 private:
  std::shared_ptr<ServerAuthHandler> auth_handler_;
  FlightServerBase* server_;
};

#if (ATOMIC_INT_LOCK_FREE != 2 || ATOMIC_POINTER_LOCK_FREE != 2)
#error "atomic ints and atomic pointers not always lock-free!"
#endif

struct FlightServerBase::Impl {
  std::string address_;
  std::unique_ptr<FlightServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;

  // Signal handling
  std::vector<int> signals_;
  std::vector<struct sigaction> old_signal_handlers_;
  std::atomic<int> got_signal_;
  static thread_local std::atomic<Impl*> running_instance_;

  static void HandleSignal(int signum);

  void DoHandleSignal(int signum) {
    got_signal_ = signum;
    server_->Shutdown();
  }
};

thread_local std::atomic<FlightServerBase::Impl*>
    FlightServerBase::Impl::running_instance_;

void FlightServerBase::Impl::HandleSignal(int signum) {
  auto instance = running_instance_.load();
  if (instance != nullptr) {
    instance->DoHandleSignal(signum);
  }
}

FlightServerBase::FlightServerBase() { impl_.reset(new Impl); }

FlightServerBase::~FlightServerBase() {}

Status FlightServerBase::Init(std::unique_ptr<ServerAuthHandler> auth_handler, int port) {
  impl_->address_ = "localhost:" + std::to_string(port);
  std::shared_ptr<ServerAuthHandler> handler = std::move(auth_handler);
  impl_->service_.reset(new FlightServiceImpl(handler, this));

  grpc::ServerBuilder builder;
  // Allow uploading messages of any length
  builder.SetMaxReceiveMessageSize(-1);
  builder.AddListeningPort(impl_->address_, grpc::InsecureServerCredentials());
  builder.RegisterService(impl_->service_.get());

  impl_->server_ = builder.BuildAndStart();
  if (!impl_->server_) {
    return Status::UnknownError("Server did not start properly");
  }
  return Status::OK();
}

Status FlightServerBase::SetShutdownOnSignals(const std::vector<int> sigs) {
  impl_->signals_ = sigs;
  impl_->old_signal_handlers_.clear();
  return Status::OK();
}

Status FlightServerBase::Serve() {
  if (!impl_->server_) {
    return Status::UnknownError("Server did not start properly");
  }

  impl_->got_signal_ = 0;
  impl_->running_instance_ = impl_.get();

  // Setup signal handlers
  impl_->old_signal_handlers_.clear();
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    int signum = impl_->signals_[i];
    // Override with our own handler so as to stop the server.
    struct sigaction sa, old_handler;
    sa.sa_handler = &Impl::HandleSignal;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    int ret = sigaction(signum, &sa, &old_handler);
    if (ret != 0) {
      return Status::IOError("sigaction call failed");
    }
    impl_->old_signal_handlers_.push_back(old_handler);
  }

  impl_->server_->Wait();
  impl_->running_instance_ = nullptr;

  // Restore signal handlers
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    int ret = sigaction(impl_->signals_[i], &impl_->old_signal_handlers_[i], nullptr);
    if (ret != 0) {
      return Status::IOError("sigaction call failed");
    }
  }

  return Status::OK();
}

int FlightServerBase::GotSignal() const { return impl_->got_signal_; }

void FlightServerBase::Shutdown() {
  DCHECK(impl_->server_);
  impl_->server_->Shutdown();
}

Status FlightServerBase::ListFlights(const ServerCallContext& context,
                                     const Criteria* criteria,
                                     std::unique_ptr<FlightListing>* listings) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::GetFlightInfo(const ServerCallContext& context,
                                       const FlightDescriptor& request,
                                       std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                               std::unique_ptr<FlightDataStream>* data_stream) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoPut(const ServerCallContext& context,
                               std::unique_ptr<FlightMessageReader> reader) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoAction(const ServerCallContext& context, const Action& action,
                                  std::unique_ptr<ResultStream>* result) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::ListActions(const ServerCallContext& context,
                                     std::vector<ActionType>* actions) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader)
    : pool_(default_memory_pool()), reader_(reader) {}

std::shared_ptr<Schema> RecordBatchStream::schema() { return reader_->schema(); }

Status RecordBatchStream::Next(FlightPayload* payload) {
  std::shared_ptr<RecordBatch> batch;
  RETURN_NOT_OK(reader_->ReadNext(&batch));

  if (!batch) {
    // Signal that iteration is over
    payload->ipc_message.metadata = nullptr;
    return Status::OK();
  } else {
    return ipc::internal::GetRecordBatchPayload(*batch, pool_, &payload->ipc_message);
  }
}

}  // namespace flight
}  // namespace arrow
