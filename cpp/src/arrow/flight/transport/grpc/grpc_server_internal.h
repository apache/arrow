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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/util/uri.h"

namespace arrow::flight::transport::grpc {

using MiddlewareFactoryList =
    std::vector<std::pair<std::string, std::shared_ptr<ServerMiddlewareFactory>>>;

ARROW_FLIGHT_EXPORT
::grpc::Status FinishGrpcServerStatus(const Status& arrow_status,
                                      ::grpc::ServerContext* context);

ARROW_FLIGHT_EXPORT
::grpc::Status FinishGrpcServerStatus(const Status& arrow_status,
                                      ::grpc::CallbackServerContext* context);

template <typename GrpcContext>
class GrpcAddServerHeaders : public AddCallHeaders {
 public:
  explicit GrpcAddServerHeaders(GrpcContext* context) : context_(context) {}
  ~GrpcAddServerHeaders() override = default;

  void AddHeader(const std::string& key, const std::string& value) override {
    context_->AddInitialMetadata(key, value);
  }

 private:
  GrpcContext* context_;
};

template <typename GrpcContext>
class GrpcServerCallContext : public ServerCallContext {
 public:
  explicit GrpcServerCallContext(GrpcContext* context)
      : context_(context), peer_(context->peer()) {
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

  ::grpc::Status FinishRequest(const Status& status) {
    for (const auto& instance : middleware_) {
      instance->CallCompleted(status);
    }
    return FinishGrpcServerStatus(status, context_);
  }

  void AddHeader(const std::string& key, const std::string& value) const override {
    context_->AddInitialMetadata(key, value);
  }

  void AddTrailer(const std::string& key, const std::string& value) const override {
    context_->AddTrailingMetadata(key, value);
  }

  ServerMiddleware* GetMiddleware(const std::string& key) const override {
    const auto& instance = middleware_map_.find(key);
    if (instance == middleware_map_.end()) {
      return nullptr;
    }
    return instance->second.get();
  }

 private:
  template <typename>
  friend class GrpcServerCallContextHelper;

  GrpcContext* context_;
  std::string peer_;
  std::string peer_identity_;
  std::vector<std::shared_ptr<ServerMiddleware>> middleware_;
  std::unordered_map<std::string, std::shared_ptr<ServerMiddleware>> middleware_map_;
  CallHeaders incoming_headers_;
};

template <typename GrpcContext>
class GrpcServerCallContextHelper {
 public:
  GrpcServerCallContextHelper(std::shared_ptr<ServerAuthHandler> auth_handler,
                              MiddlewareFactoryList middleware)
      : auth_handler_(std::move(auth_handler)), middleware_(std::move(middleware)) {}

  // Authenticate the client (if applicable) and construct the call context
  ::grpc::Status MakeCallContext(FlightMethod method, GrpcContext* context,
                                 GrpcServerCallContext<GrpcContext>* flight_context,
                                 bool skip_headers = false) const {
    // Run server middleware
    const CallInfo info{method};
    for (const auto& factory : middleware_) {
      std::shared_ptr<ServerMiddleware> instance;
      Status result = factory.second->StartCall(info, *flight_context, &instance);
      if (!result.ok()) {
        // Interceptor rejected call, end the request on all existing
        // interceptors
        return flight_context->FinishRequest(result);
      }
      if (instance != nullptr) {
        flight_context->middleware_.push_back(instance);
        flight_context->middleware_map_.insert({factory.first, instance});
      }
    }
    // TODO factor this out after fixing all streaming and non-streaming handlers
    if (!skip_headers) {
      AddMiddlewareHeaders(context, flight_context);
    }

    return ::grpc::Status::OK;
  }

  void AddMiddlewareHeaders(GrpcContext* context,
                            GrpcServerCallContext<GrpcContext>* flight_context) const {
    GrpcAddServerHeaders<GrpcContext> outgoing_headers(context);
    for (const auto& instance : flight_context->middleware_) {
      instance->SendingHeaders(&outgoing_headers);
    }
  }

  // Authenticate the client (if applicable) and construct the call context
  ::grpc::Status CheckAuth(FlightMethod method, GrpcContext* context,
                           GrpcServerCallContext<GrpcContext>* flight_context) const {
    if (!auth_handler_) {
      const auto auth_context = context->auth_context();
      if (auth_context && auth_context->IsPeerAuthenticated()) {
        auto peer_identity = auth_context->GetPeerIdentity();
        flight_context->peer_identity_ =
            peer_identity.empty()
                ? ""
                : std::string(peer_identity.front().begin(), peer_identity.front().end());
      } else {
        flight_context->peer_identity_ = "";
      }
    } else {
      const auto client_metadata = context->client_metadata();
      const auto [auth_header, auth_header_end] =
          client_metadata.equal_range(kGrpcAuthHeader);
      std::string token;
      if (auth_header != auth_header_end) {
        token = std::string(auth_header->second.data(), auth_header->second.length());
      }
      auto auth_status =
          auth_handler_->IsValid(*flight_context, token, &flight_context->peer_identity_);
      if (!auth_status.ok()) {
        return flight_context->FinishRequest(auth_status);
      }
    }
    return MakeCallContext(method, context, flight_context);
  }

  const std::shared_ptr<ServerAuthHandler>& auth_handler() const { return auth_handler_; }

 private:
  std::shared_ptr<ServerAuthHandler> auth_handler_;
  MiddlewareFactoryList middleware_;
};

ARROW_FLIGHT_EXPORT
Status AddServerListeningPort(const FlightServerOptions& options,
                              const arrow::util::Uri& uri, ::grpc::ServerBuilder* builder,
                              Location* location, int* port);

ARROW_FLIGHT_EXPORT
void ConfigureServerBuilderOptions(const FlightServerOptions& options,
                                   ::grpc::ServerBuilder* builder);

ARROW_FLIGHT_EXPORT
Status SetServerLocationFromUri(const arrow::util::Uri& uri, int port,
                                Location* location);

}  // namespace arrow::flight::transport::grpc
