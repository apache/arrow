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

// gRPC transport implementation for Arrow Flight

#include "arrow/flight/transport/grpc/grpc_server.h"

#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "arrow/flight/transport/grpc/customize_grpc.h"

#include <grpcpp/grpcpp.h>

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/grpc_server_internal.h"
#include "arrow/flight/transport/grpc/serialization_internal.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/flight/transport_server.h"
#include "arrow/flight/types.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace grpc {

namespace pb = arrow::flight::protocol;
using FlightService = pb::FlightService;
using ServerContext = ::grpc::ServerContext;
template <typename T>
using ServerWriter = ::grpc::ServerWriter<T>;

// Macro that runs interceptors before returning the given status
#define RETURN_WITH_MIDDLEWARE(CONTEXT, STATUS) \
  do {                                          \
    const auto& __s = (STATUS);                 \
    return CONTEXT.FinishRequest(__s);          \
  } while (false)
#define CHECK_ARG_NOT_NULL(CONTEXT, VAL, MESSAGE)                                \
  if (VAL == nullptr) {                                                          \
    RETURN_WITH_MIDDLEWARE(                                                      \
        CONTEXT, ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, MESSAGE)); \
  }
// Same as RETURN_NOT_OK, but accepts either Arrow or gRPC status, and
// will run interceptors
#define SERVICE_RETURN_NOT_OK(CONTEXT, expr) \
  do {                                       \
    const auto& _s = (expr);                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {     \
      return CONTEXT.FinishRequest(_s);      \
    }                                        \
  } while (false)

namespace {
class GrpcServerAuthReader : public ServerAuthReader {
 public:
  explicit GrpcServerAuthReader(
      ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
      : stream_(stream) {}

  Status Read(std::string* token) override {
    pb::HandshakeRequest request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.mutable_payload());
      return Status::OK();
    }
    return Status::IOError("Stream is closed.");
  }

 private:
  ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

class GrpcServerAuthSender : public ServerAuthSender {
 public:
  explicit GrpcServerAuthSender(
      ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
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
  ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream_;
};

using GrpcServerCallContext = transport::grpc::GrpcServerCallContext<ServerContext>;
using GrpcContextHelper = transport::grpc::GrpcServerCallContextHelper<ServerContext>;

// A ServerDataStream for streaming data to the client.
class GetDataStream : public internal::ServerDataStream {
 public:
  explicit GetDataStream(ServerWriter<pb::FlightData>* writer) : writer_(writer) {}

  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    return WritePayload(payload, writer_);
  }

 private:
  ServerWriter<pb::FlightData>* writer_;
};

// A ServerDataStream for reading data from the client.
class PutDataStream final : public internal::ServerDataStream {
 public:
  explicit PutDataStream(
      ::grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* stream)
      : stream_(stream) {}

  bool ReadData(internal::FlightData* data) override {
    return ReadPayload(&*stream_, data);
  }
  Status WritePutMetadata(const Buffer& metadata) override {
    pb::PutResult message{};
    message.set_app_metadata(metadata.data(), metadata.size());
    if (stream_->Write(message)) {
      return Status::OK();
    }
    return Status::IOError("Unknown error writing metadata.");
  }

 private:
  ::grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* stream_;
};

// A ServerDataStream for a bidirectional data exchange.
class ExchangeDataStream final : public internal::ServerDataStream {
 public:
  explicit ExchangeDataStream(
      ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream)
      : stream_(stream) {}

  bool ReadData(internal::FlightData* data) override {
    return ReadPayload(&*stream_, data);
  }
  arrow::Result<bool> WriteData(const FlightPayload& payload) override {
    return WritePayload(payload, stream_);
  }

 private:
  ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream_;
};

// The gRPC service implementation, which forwards calls to the Flight
// service and bridges between the Flight transport API and gRPC.
class GrpcServiceHandler final : public FlightService::Service {
 public:
  GrpcServiceHandler(
      std::shared_ptr<ServerAuthHandler> auth_handler,
      std::vector<std::pair<std::string, std::shared_ptr<ServerMiddlewareFactory>>>
          middleware,
      internal::ServerTransport* impl)
      : helper_(std::move(auth_handler), std::move(middleware)), impl_(impl) {}

  template <typename UserType, typename Iterator, typename ProtoType>
  ::grpc::Status WriteStream(Iterator* iterator, ServerWriter<ProtoType>* writer) {
    if (!iterator) {
      return ::grpc::Status(::grpc::StatusCode::INTERNAL, "No items to iterate");
    }
    // Write flight info to stream until listing is exhausted
    while (true) {
      ProtoType pb_value;
      std::unique_ptr<UserType> value;
      GRPC_RETURN_NOT_OK(iterator->Next().Value(&value));
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
    return ::grpc::Status::OK;
  }

  template <typename UserType, typename ProtoType>
  ::grpc::Status WriteStream(const std::vector<UserType>& values,
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
    return ::grpc::Status::OK;
  }

  ::grpc::Status Handshake(
      ServerContext* context,
      ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.MakeCallContext(FlightMethod::Handshake, context, &flight_context));
    if (!helper_.auth_handler()) {
      RETURN_WITH_MIDDLEWARE(
          flight_context,
          ::grpc::Status(
              ::grpc::StatusCode::UNIMPLEMENTED,
              "This service does not have an authentication mechanism enabled."));
    }
    GrpcServerAuthSender outgoing{stream};
    GrpcServerAuthReader incoming{stream};
    RETURN_WITH_MIDDLEWARE(flight_context, helper_.auth_handler()->Authenticate(
                                               flight_context, &outgoing, &incoming));
  }

  ::grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
                             ServerWriter<pb::FlightInfo>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::ListFlights, context, &flight_context));
    // Retrieve the listing from the implementation
    std::unique_ptr<FlightListing> listing;

    Criteria criteria;
    if (request) {
      SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &criteria));
    }
    SERVICE_RETURN_NOT_OK(
        flight_context, impl_->base()->ListFlights(flight_context, &criteria, &listing));
    if (!listing) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::OK);
    }
    RETURN_WITH_MIDDLEWARE(flight_context,
                           WriteStream<FlightInfo>(listing.get(), writer));
  }

  ::grpc::Status GetFlightInfo(ServerContext* context,
                               const pb::FlightDescriptor* request,
                               pb::FlightInfo* response) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::GetFlightInfo, context, &flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    auto res = impl_->base()->GetFlightInfo(flight_context, descr, &info);
    SERVICE_RETURN_NOT_OK(flight_context, res);

    if (!info) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                                                            "Flight not found"));
    }

    SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*info, response));
    RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::OK);
  }

  ::grpc::Status PollFlightInfo(ServerContext* context,
                                const pb::FlightDescriptor* request,
                                pb::PollInfo* response) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::PollFlightInfo, context, &flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<PollInfo> info;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->PollFlightInfo(flight_context, descr, &info));

    if (!info) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                                                            "Flight not found"));
    }

    SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*info, response));
    RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::OK);
  }

  ::grpc::Status GetSchema(ServerContext* context, const pb::FlightDescriptor* request,
                           pb::SchemaResult* response) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::GetSchema, context, &flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<SchemaResult> result;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->GetSchema(flight_context, descr, &result));

    if (!result) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status(::grpc::StatusCode::NOT_FOUND,
                                                            "Flight not found"));
    }

    SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*result, response));
    RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::OK);
  }

  ::grpc::Status DoGet(ServerContext* context, const pb::Ticket* request,
                       ServerWriter<pb::FlightData>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::DoGet, context, &flight_context));
    CHECK_ARG_NOT_NULL(flight_context, request, "ticket cannot be null");

    Ticket ticket;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &ticket));

    GetDataStream stream(writer);
    RETURN_WITH_MIDDLEWARE(flight_context,
                           impl_->DoGet(flight_context, std::move(ticket), &stream));
  }

  ::grpc::Status DoPut(
      ServerContext* context,
      ::grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::DoPut, context, &flight_context));
    PutDataStream stream(reader);
    RETURN_WITH_MIDDLEWARE(flight_context, impl_->DoPut(flight_context, &stream));
  }

  ::grpc::Status DoExchange(
      ServerContext* context,
      ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::DoExchange, context, &flight_context));
    ExchangeDataStream data_stream(stream);
    RETURN_WITH_MIDDLEWARE(flight_context,
                           impl_->DoExchange(flight_context, &data_stream));
  }

  ::grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                             ServerWriter<pb::ActionType>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::ListActions, context, &flight_context));
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->ListActions(flight_context, &types));
    RETURN_WITH_MIDDLEWARE(flight_context, WriteStream<ActionType>(types, writer));
  }

  ::grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                          ServerWriter<pb::Result>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        helper_.CheckAuth(FlightMethod::DoAction, context, &flight_context));
    CHECK_ARG_NOT_NULL(flight_context, request, "Action cannot be null");
    Action action;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &action));

    std::unique_ptr<ResultStream> results;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->DoAction(flight_context, action, &results));

    if (!results) {
      RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::CANCELLED);
    }

    while (true) {
      std::unique_ptr<Result> result;
      SERVICE_RETURN_NOT_OK(flight_context, results->Next().Value(&result));
      if (!result) {
        // No more results
        break;
      }
      pb::Result pb_result;
      SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*result, &pb_result));
      if (!writer->Write(pb_result)) {
        // Stream may be closed
        break;
      }
    }
    RETURN_WITH_MIDDLEWARE(flight_context, ::grpc::Status::OK);
  }

 private:
  GrpcContextHelper helper_;
  internal::ServerTransport* impl_;
};

// The ServerTransport implementation for gRPC. Manages the gRPC server itself.
class GrpcServerTransport : public internal::ServerTransport {
 public:
  using internal::ServerTransport::ServerTransport;

  static arrow::Result<std::unique_ptr<internal::ServerTransport>> Make(
      FlightServerBase* base, std::shared_ptr<MemoryManager> memory_manager) {
    return std::unique_ptr<internal::ServerTransport>(
        new GrpcServerTransport(base, std::move(memory_manager)));
  }

  Status Init(const FlightServerOptions& options, const arrow::util::Uri& uri) override {
    grpc_service_.reset(
        new GrpcServiceHandler(options.auth_handler, options.middleware, this));

    ::grpc::ServerBuilder builder;
    int port = 0;
    RETURN_NOT_OK(AddServerListeningPort(options, uri, &builder, &location_, &port));

    builder.RegisterService(grpc_service_.get());
    ConfigureServerBuilderOptions(options, &builder);

    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
      return Status::UnknownError("Server did not start properly");
    }
    return SetServerLocationFromUri(uri, port, &location_);
  }
  Status Shutdown() override {
    grpc_server_->Shutdown();
    return Status::OK();
  }
  Status Shutdown(const std::chrono::system_clock::time_point& deadline) override {
    grpc_server_->Shutdown(deadline);
    return Status::OK();
  }
  Status Wait() override {
    grpc_server_->Wait();
    return Status::OK();
  }
  Location location() const override { return location_; }

 private:
  std::unique_ptr<GrpcServiceHandler> grpc_service_;
  std::unique_ptr<::grpc::Server> grpc_server_;
  Location location_;
};

std::once_flag kGrpcServerTransportInitialized;
}  // namespace

void InitializeFlightGrpcServer() {
  std::call_once(kGrpcServerTransportInitialized, []() {
    auto* registry = flight::internal::GetDefaultTransportRegistry();
    for (const auto& transport : {"grpc", "grpc+tls", "grpc+tcp", "grpc+unix"}) {
      ARROW_CHECK_OK(registry->RegisterServer(transport, GrpcServerTransport::Make));
    }
  });
}

#undef CHECK_ARG_NOT_NULL
#undef RETURN_WITH_MIDDLEWARE
#undef SERVICE_RETURN_NOT_OK

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
