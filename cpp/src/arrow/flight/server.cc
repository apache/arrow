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

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/server.h"

#ifdef _WIN32
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/buffer.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/middleware.h"
#include "arrow/flight/middleware_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/transport_impl.h"
#include "arrow/flight/types.h"

using FlightService = arrow::flight::protocol::FlightService;
using ServerContext = grpc::ServerContext;

template <typename T>
using ServerWriter = grpc::ServerWriter<T>;

namespace arrow {
namespace flight {

namespace pb = arrow::flight::protocol;

// Macro that runs interceptors before returning the given status
#define RETURN_WITH_MIDDLEWARE(CONTEXT, STATUS) \
  do {                                          \
    const auto& __s = (STATUS);                 \
    return CONTEXT.FinishRequest(__s);          \
  } while (false)

#define CHECK_ARG_NOT_NULL(CONTEXT, VAL, MESSAGE)                                      \
  if (VAL == nullptr) {                                                                \
    RETURN_WITH_MIDDLEWARE(CONTEXT,                                                    \
                           grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, MESSAGE)); \
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

FlightMetadataWriter::~FlightMetadataWriter() = default;

namespace {
class GrpcServerAuthReader : public ServerAuthReader {
 public:
  explicit GrpcServerAuthReader(
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream)
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

class FlightGrpcServiceImpl;
class GrpcServerCallContext : public ServerCallContext {
  explicit GrpcServerCallContext(grpc::ServerContext* context)
      : context_(context), peer_(context_->peer()) {}

  const std::string& peer_identity() const override { return peer_identity_; }
  const std::string& peer() const override { return peer_; }
  bool is_cancelled() const override { return context_->IsCancelled(); }

  // Helper method that runs interceptors given the result of an RPC,
  // then returns the final gRPC status to send to the client
  grpc::Status FinishRequest(const grpc::Status& status) {
    // Don't double-convert status - return the original one here
    FinishRequest(internal::FromGrpcStatus(status));
    return status;
  }

  grpc::Status FinishRequest(const arrow::Status& status) {
    for (const auto& instance : middleware_) {
      instance->CallCompleted(status);
    }

    // Set custom headers to map the exact Arrow status for clients
    // who want it.
    return internal::ToGrpcStatus(status, context_);
  }

  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    const auto& instance = middleware_map_.find(key);
    if (instance == middleware_map_.end()) {
      return nullptr;
    }
    return instance->second.get();
  }

 private:
  friend class FlightGrpcServiceImpl;
  ServerContext* context_;
  std::string peer_;
  std::string peer_identity_;
  std::vector<std::shared_ptr<ServerMiddleware>> middleware_;
  std::unordered_map<std::string, std::shared_ptr<ServerMiddleware>> middleware_map_;
};

class GrpcAddCallHeaders : public AddCallHeaders {
 public:
  explicit GrpcAddCallHeaders(grpc::ServerContext* context) : context_(context) {}
  ~GrpcAddCallHeaders() override = default;

  void AddHeader(const std::string& key, const std::string& value) override {
    context_->AddInitialMetadata(key, value);
  }

 private:
  grpc::ServerContext* context_;
};

class GetDataStream : public internal::TransportDataStream {
 public:
  explicit GetDataStream(ServerWriter<pb::FlightData>* writer) : writer_(writer) {}

  Status WriteData(const FlightPayload& payload) override {
    return internal::WritePayload(payload, writer_);
  }

 private:
  ServerWriter<pb::FlightData>* writer_;
};

class PutDataStream final : public internal::TransportDataStream {
 public:
  explicit PutDataStream(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* stream)
      : stream_(stream) {}

  bool ReadData(internal::FlightData* data) override {
    return internal::ReadPayload(&*stream_, data);
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
  grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* stream_;
};

class ExchangeDataStream final : public internal::TransportDataStream {
 public:
  explicit ExchangeDataStream(
      grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream)
      : stream_(stream) {}

  bool ReadData(internal::FlightData* data) override {
    return internal::ReadPayload(&*stream_, data);
  }
  Status WriteData(const FlightPayload& payload) override {
    return internal::WritePayload(payload, stream_);
  }

 private:
  grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream_;
};

// This class glues an implementation of FlightServerBase together with the
// gRPC service definition, so the latter is not exposed in the public API
class FlightGrpcServiceImpl : public FlightService::Service {
 public:
  explicit FlightGrpcServiceImpl(
      std::shared_ptr<ServerAuthHandler> auth_handler,
      std::vector<std::pair<std::string, std::shared_ptr<ServerMiddlewareFactory>>>
          middleware,
      internal::FlightServiceImpl* service)
      : auth_handler_(auth_handler),
        middleware_(middleware),
        service_(service),
        server_(service_->base()) {}

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
  grpc::Status CheckAuth(const FlightMethod& method, ServerContext* context,
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
      const auto auth_header = client_metadata.find(internal::kGrpcAuthHeader);
      std::string token;
      if (auth_header == client_metadata.end()) {
        token = "";
      } else {
        token = std::string(auth_header->second.data(), auth_header->second.length());
      }
      GRPC_RETURN_NOT_OK(auth_handler_->IsValid(token, &flight_context.peer_identity_));
    }

    return MakeCallContext(method, context, flight_context);
  }

  // Authenticate the client (if applicable) and construct the call context
  grpc::Status MakeCallContext(const FlightMethod& method, ServerContext* context,
                               GrpcServerCallContext& flight_context) {
    // Run server middleware
    const CallInfo info{method};
    CallHeaders incoming_headers;
    for (const auto& entry : context->client_metadata()) {
      incoming_headers.insert(
          {util::string_view(entry.first.data(), entry.first.length()),
           util::string_view(entry.second.data(), entry.second.length())});
    }

    GrpcAddCallHeaders outgoing_headers(context);
    for (const auto& factory : middleware_) {
      std::shared_ptr<ServerMiddleware> instance;
      Status result = factory.second->StartCall(info, incoming_headers, &instance);
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

    return grpc::Status::OK;
  }

  grpc::Status Handshake(
      ServerContext* context,
      grpc::ServerReaderWriter<pb::HandshakeResponse, pb::HandshakeRequest>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        MakeCallContext(FlightMethod::Handshake, context, flight_context));

    if (!auth_handler_) {
      RETURN_WITH_MIDDLEWARE(
          flight_context,
          grpc::Status(
              grpc::StatusCode::UNIMPLEMENTED,
              "This service does not have an authentication mechanism enabled."));
    }
    GrpcServerAuthSender outgoing{stream};
    GrpcServerAuthReader incoming{stream};
    RETURN_WITH_MIDDLEWARE(flight_context,
                           auth_handler_->Authenticate(&outgoing, &incoming));
  }

  grpc::Status ListFlights(ServerContext* context, const pb::Criteria* request,
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
    SERVICE_RETURN_NOT_OK(flight_context,
                          server_->ListFlights(flight_context, &criteria, &listing));
    if (!listing) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(flight_context, grpc::Status::OK);
    }
    RETURN_WITH_MIDDLEWARE(flight_context,
                           WriteStream<FlightInfo>(listing.get(), writer));
  }

  grpc::Status GetFlightInfo(ServerContext* context, const pb::FlightDescriptor* request,
                             pb::FlightInfo* response) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        CheckAuth(FlightMethod::GetFlightInfo, context, flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<FlightInfo> info;
    SERVICE_RETURN_NOT_OK(flight_context,
                          server_->GetFlightInfo(flight_context, descr, &info));

    if (!info) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(
          flight_context, grpc::Status(grpc::StatusCode::NOT_FOUND, "Flight not found"));
    }

    SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*info, response));
    RETURN_WITH_MIDDLEWARE(flight_context, grpc::Status::OK);
  }

  grpc::Status GetSchema(ServerContext* context, const pb::FlightDescriptor* request,
                         pb::SchemaResult* response) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::GetSchema, context, flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "FlightDescriptor cannot be null");

    FlightDescriptor descr;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &descr));

    std::unique_ptr<SchemaResult> result;
    SERVICE_RETURN_NOT_OK(flight_context,
                          server_->GetSchema(flight_context, descr, &result));

    if (!result) {
      // Treat null listing as no flights available
      RETURN_WITH_MIDDLEWARE(
          flight_context, grpc::Status(grpc::StatusCode::NOT_FOUND, "Flight not found"));
    }

    SERVICE_RETURN_NOT_OK(flight_context, internal::ToProto(*result, response));
    RETURN_WITH_MIDDLEWARE(flight_context, grpc::Status::OK);
  }

  grpc::Status DoGet(ServerContext* context, const pb::Ticket* request,
                     ServerWriter<pb::FlightData>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoGet, context, flight_context));

    CHECK_ARG_NOT_NULL(flight_context, request, "ticket cannot be null");

    Ticket ticket;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &ticket));

    GetDataStream stream(writer);
    RETURN_WITH_MIDDLEWARE(flight_context,
                           service_->DoGet(flight_context, std::move(ticket), &stream));
  }

  grpc::Status DoPut(ServerContext* context,
                     grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader) {
    GrpcServerCallContext flight_context(context);
    // TODO: auth needs to be reified
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoPut, context, flight_context));

    PutDataStream stream(reader);
    RETURN_WITH_MIDDLEWARE(flight_context, service_->DoPut(flight_context, &stream));
  }

  grpc::Status DoExchange(
      ServerContext* context,
      grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* stream) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoExchange, context, flight_context));

    ExchangeDataStream data_stream(stream);
    RETURN_WITH_MIDDLEWARE(flight_context,
                           service_->DoExchange(flight_context, &data_stream));
  }

  grpc::Status ListActions(ServerContext* context, const pb::Empty* request,
                           ServerWriter<pb::ActionType>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(
        CheckAuth(FlightMethod::ListActions, context, flight_context));
    // Retrieve the listing from the implementation
    std::vector<ActionType> types;
    SERVICE_RETURN_NOT_OK(flight_context, server_->ListActions(flight_context, &types));
    RETURN_WITH_MIDDLEWARE(flight_context, WriteStream<ActionType>(types, writer));
  }

  grpc::Status DoAction(ServerContext* context, const pb::Action* request,
                        ServerWriter<pb::Result>* writer) {
    GrpcServerCallContext flight_context(context);
    GRPC_RETURN_NOT_GRPC_OK(CheckAuth(FlightMethod::DoAction, context, flight_context));
    CHECK_ARG_NOT_NULL(flight_context, request, "Action cannot be null");
    Action action;
    SERVICE_RETURN_NOT_OK(flight_context, internal::FromProto(*request, &action));

    std::unique_ptr<ResultStream> results;
    SERVICE_RETURN_NOT_OK(flight_context,
                          server_->DoAction(flight_context, action, &results));

    if (!results) {
      RETURN_WITH_MIDDLEWARE(flight_context, grpc::Status::CANCELLED);
    }

    while (true) {
      std::unique_ptr<Result> result;
      SERVICE_RETURN_NOT_OK(flight_context, results->Next(&result));
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
    RETURN_WITH_MIDDLEWARE(flight_context, grpc::Status::OK);
  }

 private:
  std::shared_ptr<ServerAuthHandler> auth_handler_;
  std::vector<std::pair<std::string, std::shared_ptr<ServerMiddlewareFactory>>>
      middleware_;
  internal::FlightServiceImpl* service_;
  FlightServerBase* server_;
};

//
// gRPC server lifecycle
//

#if (ATOMIC_INT_LOCK_FREE != 2 || ATOMIC_POINTER_LOCK_FREE != 2)
#error "atomic ints and atomic pointers not always lock-free!"
#endif

using ::arrow::internal::SetSignalHandler;
using ::arrow::internal::SignalHandler;

#ifdef WIN32
#define PIPE_WRITE _write
#define PIPE_READ _read
#else
#define PIPE_WRITE write
#define PIPE_READ read
#endif

/// RAII guard that manages a self-pipe and a thread that listens on
/// the self-pipe, shutting down the gRPC server when a signal handler
/// writes to the pipe.
class ServerSignalHandler {
 public:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ServerSignalHandler);
  ServerSignalHandler() = default;

  /// Create the pipe and handler thread.
  ///
  /// \return the fd of the write side of the pipe.
  template <typename Fn>
  arrow::Result<int> Init(Fn handler) {
    ARROW_ASSIGN_OR_RAISE(auto pipe, arrow::internal::CreatePipe());
#ifndef WIN32
    // Make write end nonblocking
    int flags = fcntl(pipe.wfd, F_GETFL);
    if (flags == -1) {
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.rfd));
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.wfd));
      return arrow::internal::IOErrorFromErrno(
          errno, "Could not initialize self-pipe to wait for signals");
    }
    flags |= O_NONBLOCK;
    if (fcntl(pipe.wfd, F_SETFL, flags) == -1) {
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.rfd));
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.wfd));
      return arrow::internal::IOErrorFromErrno(
          errno, "Could not initialize self-pipe to wait for signals");
    }
#endif
    self_pipe_ = pipe;
    handle_signals_ = std::thread(handler, self_pipe_.rfd);
    return self_pipe_.wfd;
  }

  Status Shutdown() {
    if (self_pipe_.rfd == 0) {
      // Already closed
      return Status::OK();
    }
    if (PIPE_WRITE(self_pipe_.wfd, "0", 1) < 0 && errno != EAGAIN &&
        errno != EWOULDBLOCK && errno != EINTR) {
      return arrow::internal::IOErrorFromErrno(errno, "Could not unblock signal thread");
    }
    handle_signals_.join();
    RETURN_NOT_OK(arrow::internal::FileClose(self_pipe_.rfd));
    RETURN_NOT_OK(arrow::internal::FileClose(self_pipe_.wfd));
    self_pipe_.rfd = 0;
    self_pipe_.wfd = 0;
    return Status::OK();
  }

  ~ServerSignalHandler() { ARROW_CHECK_OK(Shutdown()); }

 private:
  arrow::internal::Pipe self_pipe_;
  std::thread handle_signals_;
};

class GrpcServerImpl : public internal::ServerTransportImpl {
 public:
  Status Init(const FlightServerOptions& options, const arrow::internal::Uri& uri,
              internal::FlightServiceImpl* server) override {
    service_.reset(
        new FlightGrpcServiceImpl(options.auth_handler, options.middleware, server));

    grpc::ServerBuilder builder;
    // Allow uploading messages of any length
    builder.SetMaxReceiveMessageSize(-1);

    const std::string scheme = uri.scheme();
    int port = 0;
    if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
      std::stringstream address;
      address << arrow::internal::UriEncodeHost(uri.host()) << ':' << uri.port_text();

      std::shared_ptr<grpc::ServerCredentials> creds;
      if (scheme == kSchemeGrpcTls) {
        grpc::SslServerCredentialsOptions ssl_options;
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
        creds = grpc::SslServerCredentials(ssl_options);
      } else {
        creds = grpc::InsecureServerCredentials();
      }

      builder.AddListeningPort(address.str(), creds, &port);
    } else if (scheme == kSchemeGrpcUnix) {
      std::stringstream address;
      address << "unix:" << uri.path();
      builder.AddListeningPort(address.str(), grpc::InsecureServerCredentials());
      location_ = options.location;
    } else {
      return Status::NotImplemented("Scheme is not supported: " + scheme);
    }

    builder.RegisterService(service_.get());

    // Disable SO_REUSEPORT - it makes debugging/testing a pain as
    // leftover processes can handle requests on accident
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);

    if (options.builder_hook) {
      options.builder_hook(&builder);
    }

    server_ = builder.BuildAndStart();
    if (!server_) {
      return Status::UnknownError("Server did not start properly");
    }

    if (scheme == kSchemeGrpcTls) {
      RETURN_NOT_OK(Location::ForGrpcTls(uri.host(), port, &location_));
    } else if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp) {
      RETURN_NOT_OK(Location::ForGrpcTcp(uri.host(), port, &location_));
    }
    return Status::OK();
  }
  Status Shutdown() override {
    server_->Shutdown();
    return Status::OK();
  }
  Status Shutdown(const std::chrono::system_clock::time_point& deadline) override {
    server_->Shutdown(deadline);
    return Status::OK();
  }
  Status Wait() override {
    server_->Wait();
    return Status::OK();
  }
  Location location() const override { return location_; }

 private:
  std::unique_ptr<FlightGrpcServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  Location location_;
};

}  // namespace

struct FlightServerBase::Impl {
  std::unique_ptr<internal::ServerTransportImpl> server_;
  std::unique_ptr<internal::FlightServiceImpl> service_;

  // Signal handlers (on Windows) and the shutdown handler (other platforms)
  // are executed in a separate thread, so getting the current thread instance
  // wouldn't make sense.  This means only a single instance can receive signals.
  static std::atomic<Impl*> running_instance_;
  // We'll use the self-pipe trick to notify a thread from the signal
  // handler. The thread will then shut down the gRPC server.
  int self_pipe_wfd_;

  // Signal handling
  std::vector<int> signals_;
  std::vector<SignalHandler> old_signal_handlers_;
  std::atomic<int> got_signal_;

  static void HandleSignal(int signum) {
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      instance->DoHandleSignal(signum);
    }
  }

  void DoHandleSignal(int signum) {
    got_signal_ = signum;
    int saved_errno = errno;
    // Ignore errors - pipe is nonblocking
    PIPE_WRITE(self_pipe_wfd_, "0", 1);
    errno = saved_errno;
  }

  static void WaitForSignals(int fd) {
    // Wait for a signal handler to write to the pipe
    int8_t buf[1];
    while (PIPE_READ(fd, /*buf=*/buf, /*count=*/1) == -1) {
      if (errno == EINTR) {
        continue;
      }
      ARROW_CHECK_OK(arrow::internal::IOErrorFromErrno(
          errno, "Error while waiting for shutdown signal"));
    }
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      auto status = instance->server_->Shutdown();
      if (!status.ok()) {
        ARROW_LOG(WARNING) << "Error shutting down server: " << status.ToString();
      }
    }
  }
};

std::atomic<FlightServerBase::Impl*> FlightServerBase::Impl::running_instance_;

FlightServerOptions::FlightServerOptions(const Location& location_)
    : location(location_),
      auth_handler(nullptr),
      tls_certificates(),
      verify_client(false),
      root_certificates(),
      middleware(),
      memory_manager(CPUDevice::Instance()->default_memory_manager()),
      builder_hook(nullptr) {}

FlightServerOptions::~FlightServerOptions() = default;

FlightServerBase::FlightServerBase() { impl_.reset(new Impl); }

FlightServerBase::~FlightServerBase() {}

Status FlightServerBase::Init(const FlightServerOptions& options) {
  const auto scheme = options.location.scheme();
  if (util::string_view(scheme).starts_with("grpc")) {
    impl_->server_.reset(new GrpcServerImpl());
  } else {
    ARROW_ASSIGN_OR_RAISE(
        impl_->server_,
        internal::GetDefaultTransportImplRegistry()->MakeServerImpl(scheme));
  }
  impl_->service_.reset(new internal::FlightServiceImpl(this, options.memory_manager));
  return impl_->server_->Init(options, *options.location.uri_, impl_->service_.get());
}

int FlightServerBase::port() const { return location().uri_->port(); }

Location FlightServerBase::location() const { return impl_->server_->location(); }

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
  impl_->old_signal_handlers_.clear();
  impl_->running_instance_ = impl_.get();

  ServerSignalHandler signal_handler;
  ARROW_ASSIGN_OR_RAISE(impl_->self_pipe_wfd_,
                        signal_handler.Init(&Impl::WaitForSignals));
  // Override existing signal handlers with our own handler so as to stop the server.
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    int signum = impl_->signals_[i];
    SignalHandler new_handler(&Impl::HandleSignal), old_handler;
    ARROW_ASSIGN_OR_RAISE(old_handler, SetSignalHandler(signum, new_handler));
    impl_->old_signal_handlers_.push_back(std::move(old_handler));
  }

  RETURN_NOT_OK(impl_->server_->Wait());
  impl_->running_instance_ = nullptr;

  // Restore signal handlers
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    RETURN_NOT_OK(
        SetSignalHandler(impl_->signals_[i], impl_->old_signal_handlers_[i]).status());
  }
  return Status::OK();
}

int FlightServerBase::GotSignal() const { return impl_->got_signal_; }

Status FlightServerBase::Shutdown(const std::chrono::system_clock::time_point* deadline) {
  auto server = impl_->server_.get();
  if (!server) {
    return Status::Invalid("Shutdown() on uninitialized FlightServerBase");
  }
  impl_->running_instance_ = nullptr;
  if (deadline) {
    return impl_->server_->Shutdown(*deadline);
  }
  return impl_->server_->Shutdown();
}

Status FlightServerBase::Wait() {
  RETURN_NOT_OK(impl_->server_->Wait());
  impl_->running_instance_ = nullptr;
  return Status::OK();
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
                               std::unique_ptr<FlightMessageReader> reader,
                               std::unique_ptr<FlightMetadataWriter> writer) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoExchange(const ServerCallContext& context,
                                    std::unique_ptr<FlightMessageReader> reader,
                                    std::unique_ptr<FlightMessageWriter> writer) {
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

Status FlightServerBase::GetSchema(const ServerCallContext& context,
                                   const FlightDescriptor& request,
                                   std::unique_ptr<SchemaResult>* schema) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

class RecordBatchStream::RecordBatchStreamImpl {
 public:
  // Stages of the stream when producing payloads
  enum class Stage {
    NEW,          // The stream has been created, but Next has not been called yet
    DICTIONARY,   // Dictionaries have been collected, and are being sent
    RECORD_BATCH  // Initial have been sent
  };

  RecordBatchStreamImpl(const std::shared_ptr<RecordBatchReader>& reader,
                        const ipc::IpcWriteOptions& options)
      : reader_(reader), mapper_(*reader_->schema()), ipc_options_(options) {}

  std::shared_ptr<Schema> schema() { return reader_->schema(); }

  Status GetSchemaPayload(FlightPayload* payload) {
    return ipc::GetSchemaPayload(*reader_->schema(), ipc_options_, mapper_,
                                 &payload->ipc_message);
  }

  Status Next(FlightPayload* payload) {
    if (stage_ == Stage::NEW) {
      RETURN_NOT_OK(reader_->ReadNext(&current_batch_));
      if (!current_batch_) {
        // Signal that iteration is over
        payload->ipc_message.metadata = nullptr;
        return Status::OK();
      }
      ARROW_ASSIGN_OR_RAISE(dictionaries_,
                            ipc::CollectDictionaries(*current_batch_, mapper_));
      stage_ = Stage::DICTIONARY;
    }

    if (stage_ == Stage::DICTIONARY) {
      if (dictionary_index_ == static_cast<int>(dictionaries_.size())) {
        stage_ = Stage::RECORD_BATCH;
        return ipc::GetRecordBatchPayload(*current_batch_, ipc_options_,
                                          &payload->ipc_message);
      } else {
        return GetNextDictionary(payload);
      }
    }

    RETURN_NOT_OK(reader_->ReadNext(&current_batch_));

    // TODO(wesm): Delta dictionaries
    if (!current_batch_) {
      // Signal that iteration is over
      payload->ipc_message.metadata = nullptr;
      return Status::OK();
    } else {
      return ipc::GetRecordBatchPayload(*current_batch_, ipc_options_,
                                        &payload->ipc_message);
    }
  }

 private:
  Status GetNextDictionary(FlightPayload* payload) {
    const auto& it = dictionaries_[dictionary_index_++];
    return ipc::GetDictionaryPayload(it.first, it.second, ipc_options_,
                                     &payload->ipc_message);
  }

  Stage stage_ = Stage::NEW;
  std::shared_ptr<RecordBatchReader> reader_;
  ipc::DictionaryFieldMapper mapper_;
  ipc::IpcWriteOptions ipc_options_;
  std::shared_ptr<RecordBatch> current_batch_;
  std::vector<std::pair<int64_t, std::shared_ptr<Array>>> dictionaries_;

  // Index of next dictionary to send
  int dictionary_index_ = 0;
};

FlightDataStream::~FlightDataStream() {}

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                                     const ipc::IpcWriteOptions& options) {
  impl_.reset(new RecordBatchStreamImpl(reader, options));
}

RecordBatchStream::~RecordBatchStream() {}

std::shared_ptr<Schema> RecordBatchStream::schema() { return impl_->schema(); }

Status RecordBatchStream::GetSchemaPayload(FlightPayload* payload) {
  return impl_->GetSchemaPayload(payload);
}

Status RecordBatchStream::Next(FlightPayload* payload) { return impl_->Next(payload); }

}  // namespace flight
}  // namespace arrow
