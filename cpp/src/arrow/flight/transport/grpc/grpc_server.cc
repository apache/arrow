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

#include "arrow/util/config.h"
#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/transport.h"
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

class GrpcServerCallContext : public ServerCallContext {
  explicit GrpcServerCallContext(::grpc::ServerContext* context)
      : context_(context), peer_(context_->peer()) {
    for (const auto& entry : context->client_metadata()) {
      incoming_headers_.insert(
          {std::string_view(entry.first.data(), entry.first.length()),
           std::string_view(entry.second.data(), entry.second.length())});
    }
  }

  const std::string& peer_identity() const override { return peer_identity_; }
  const std::string& peer() const override { return peer_; }
  bool is_cancelled() const override { return context_->IsCancelled(); }
  const CallHeaders& incoming_headers() const override { return incoming_headers_; }

  // Helper method that runs interceptors given the result of an RPC,
  // then returns the final gRPC status to send to the client
  ::grpc::Status FinishRequest(const ::grpc::Status& status) {
    // Don't double-convert status - return the original one here
    FinishRequest(FromGrpcStatus(status));
    return status;
  }

  ::grpc::Status FinishRequest(const arrow::Status& status) {
    for (const auto& instance : middleware_) {
      instance->CallCompleted(status);
    }

    // Set custom headers to map the exact Arrow status for clients
    // who want it.
    return ToGrpcStatus(status, context_);
  }

  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    const auto& instance = middleware_map_.find(key);
    if (instance == middleware_map_.end()) {
      return nullptr;
    }
    return instance->second.get();
  }

 private:
  friend class GrpcServiceHandler;
  ServerContext* context_;
  std::string peer_;
  std::string peer_identity_;
  std::vector<std::shared_ptr<ServerMiddleware>> middleware_;
  std::unordered_map<std::string, std::shared_ptr<ServerMiddleware>> middleware_map_;
  CallHeaders incoming_headers_;
};

class GrpcAddServerHeaders : public AddCallHeaders {
 public:
  explicit GrpcAddServerHeaders(::grpc::ServerContext* context) : context_(context) {}
  ~GrpcAddServerHeaders() override = default;

  void AddHeader(const std::string& key, const std::string& value) override {
    context_->AddInitialMetadata(key, value);
  }

 private:
  ::grpc::ServerContext* context_;
};

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
      : auth_handler_(auth_handler), middleware_(middleware), impl_(impl) {}

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

  // Authenticate the client (if applicable) and construct the call context
  ::grpc::Status CheckAuth(const FlightMethod& method, ServerContext* context,
                           GrpcServerCallContext& flight_context) {
    if (!auth_handler_) {
      const auto auth_context = context->auth_context();
      if (auth_context && auth_context->IsPeerAuthenticated()) {
        auto peer_identity = auth_context->GetPeerIdentity();
        flight_context.peer_identity_ =
            peer_identity.empty()
                ? ""
                : std::string(peer_identity.front().begin(), peer_identity.front().end());
      } else {
        flight_context.peer_identity_ = "";
      }
    } else {
      const auto client_metadata = context->client_metadata();
      const auto auth_header = client_metadata.find(kGrpcAuthHeader);
      std::string token;
      if (auth_header == client_metadata.end()) {
        token = "";
      } else {
        token = std::string(auth_header->second.data(), auth_header->second.length());
      }
      GRPC_RETURN_NOT_OK(
          auth_handler_->IsValid(flight_context, token, &flight_context.peer_identity_));
    }

    return MakeCallContext(method, context, flight_context);
  }

  // Authenticate the client (if applicable) and construct the call context
  ::grpc::Status MakeCallContext(const FlightMethod& method, ServerContext* context,
                                 GrpcServerCallContext& flight_context) {
    // Run server middleware
    const CallInfo info{method};

    GrpcAddServerHeaders outgoing_headers(context);
    for (const auto& factory : middleware_) {
      std::shared_ptr<ServerMiddleware> instance;
      Status result = factory.second->StartCall(info, flight_context, &instance);
      if (!result.ok()) {
        // Interceptor rejected call, end the request on all existing
        // interceptors
        return flight_context.FinishRequest(result);
      }
      if (instance != nullptr) {
        flight_context.middleware_.push_back(instance);
        flight_context.middleware_map_.insert({factory.first, instance});
        instance->SendingHeaders(&outgoing_headers);
      }
    }

    return ::grpc::Status::OK;
  }

  ::grpc::Status Handshake(
      ServerContext* context,
      ::grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        MakeCallContext(FlightMethod::Handshake, context, flight_context));

    if (!auth_handler_) {
      RETURN_WITH_MIDDLEWARE(
          flight_context,
          ::grpc::Status(
              ::grpc::StatusCode::UNIMPLEMENTED,
              "This service does not have an authentication mechanism enabled."));
    }
    GrpcServerAuthSender outgoing{stream};
    GrpcServerAuthReader incoming{stream};
    RETURN_WITH_MIDDLEWARE(flight_context, auth_handler_->Authenticate(
                                               flight_context, &outgoing, &incoming));
  }

  ::grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
                             ServerWriter<pb::FlightInfo>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        CheckAuth(FlightMethod::ListFlights, context, flight_context));

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
        CheckAuth(FlightMethod::GetFlightInfo, context, flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->GetFlightInfo(flight_context, descr, &info));

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
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::GetSchema, context, flight_context));

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
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoGet, context, flight_context));

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
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoPut, context, flight_context));

    PutDataStream stream(reader);
    RETURN_WITH_MIDDLEWARE(flight_context, impl_->DoPut(flight_context, &stream));
  }

  ::grpc::Status DoExchange(
      ServerContext* context,
      ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoExchange, context, flight_context));

    ExchangeDataStream data_stream(stream);
    RETURN_WITH_MIDDLEWARE(flight_context,
                           impl_->DoExchange(flight_context, &data_stream));
  }

  ::grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                             ServerWriter<pb::ActionType>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        CheckAuth(FlightMethod::ListActions, context, flight_context));
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    SERVICE_RETURN_NOT_OK(flight_context,
                          impl_->base()->ListActions(flight_context, &types));
    RETURN_WITH_MIDDLEWARE(flight_context, WriteStream<ActionType>(types, writer));
  }

  ::grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                          ServerWriter<pb::Result>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoAction, context, flight_context));
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
  std::shared_ptr<ServerAuthHandler> auth_handler_;
  std::vector<std::pair<std::string, std::shared_ptr<ServerMiddlewareFactory>>>
      middleware_;
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

  Status Init(const FlightServerOptions& options,
              const arrow::internal::Uri& uri) override {
    grpc_service_.reset(
        new GrpcServiceHandler(options.auth_handler, options.middleware, this));

    ::grpc::ServerBuilder builder;
    // Allow uploading messages of any length
    builder.SetMaxReceiveMessageSize(-1);

    const std::string scheme = uri.scheme();
    int port = 0;
    if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
      std::stringstream address;
      address << arrow::internal::UriEncodeHost(uri.host()) << ':' << uri.port_text();

      std::shared_ptr<::grpc::ServerCredentials> creds;
      if (scheme == kSchemeGrpcTls) {
        ::grpc::SslServerCredentialsOptions ssl_options;
        for (const auto& pair : options.tls_certificates) {
          ssl_options.pem_key_cert_pairs.push_back({pair.pem_key, pair.pem_cert});
        }
        if (options.verify_client) {
          ssl_options.client_certificate_request =
              GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
        }
        if (!options.root_certificates.empty()) {
          ssl_options.pem_root_certs = options.root_certificates;
        }
        creds = ::grpc::SslServerCredentials(ssl_options);
      } else {
        creds = ::grpc::InsecureServerCredentials();
      }

      builder.AddListeningPort(address.str(), creds, &port);
    } else if (scheme == kSchemeGrpcUnix) {
      std::stringstream address;
      address << "unix:" << uri.path();
      builder.AddListeningPort(address.str(), ::grpc::InsecureServerCredentials());
      location_ = options.location;
    } else {
      return Status::NotImplemented("Scheme is not supported: " + scheme);
    }

    builder.RegisterService(grpc_service_.get());

    // Disable SO_REUSEPORT - it makes debugging/testing a pain as
    // leftover processes can handle requests on accident
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

    if (options.builder_hook) {
      options.builder_hook(&builder);
    }

    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_) {
      return Status::UnknownError("Server did not start properly");
    }

    if (scheme == kSchemeGrpcTls) {
      ARROW_ASSIGN_OR_RAISE(location_, Location::ForGrpcTls(uri.host(), port));
    } else if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp) {
      ARROW_ASSIGN_OR_RAISE(location_, Location::ForGrpcTcp(uri.host(), port));
    }
    return Status::OK();
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
