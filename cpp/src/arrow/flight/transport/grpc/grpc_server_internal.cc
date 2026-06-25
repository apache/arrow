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

#include "arrow/flight/transport/grpc/grpc_server_internal.h"

#include <sstream>

#include "arrow/flight/transport/grpc/customize_grpc.h"

namespace arrow::flight::transport::grpc {

::grpc::Status FinishGrpcServerStatus(const Status& arrow_status,
                                      ::grpc::ServerContext* context) {
  return ToGrpcStatus(arrow_status, context);
}

::grpc::Status FinishGrpcServerStatus(const Status& arrow_status,
                                      ::grpc::CallbackServerContext* context) {
  return ToGrpcStatus(arrow_status, context);
}

Status AddServerListeningPort(const FlightServerOptions& options,
                              const arrow::util::Uri& uri, ::grpc::ServerBuilder* builder,
                              Location* location, int* port) {
  const std::string scheme = uri.scheme();
  if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
    std::stringstream address;
    address << arrow::util::UriEncodeHost(uri.host()) << ':' << uri.port_text();

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
    builder->AddListeningPort(address.str(), creds, port);
    return Status::OK();
  }
  if (scheme == kSchemeGrpcUnix) {
    std::stringstream address;
    address << "unix:" << uri.path();
    builder->AddListeningPort(address.str(), ::grpc::InsecureServerCredentials());
    *location = options.location;
    return Status::OK();
  }
  return Status::NotImplemented("Scheme is not supported: " + scheme);
}

void ConfigureServerBuilderOptions(const FlightServerOptions& options,
                                   ::grpc::ServerBuilder* builder) {
  // Allow uploading messages of any length
  builder->SetMaxReceiveMessageSize(-1);
  // Disable SO_REUSEPORT - it makes debugging/testing a pain as
  // leftover processes can handle requests on accident
  builder->AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  if (options.builder_hook) {
    options.builder_hook(builder);
  }
}

Status SetServerLocationFromUri(const arrow::util::Uri& uri, int port, Location* location) {
  const std::string scheme = uri.scheme();
  if (scheme == kSchemeGrpcTls) {
    ARROW_ASSIGN_OR_RAISE(*location,
                          Location::ForGrpcTls(arrow::util::UriEncodeHost(uri.host()), port));
  } else if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp) {
    ARROW_ASSIGN_OR_RAISE(*location,
                          Location::ForGrpcTcp(arrow::util::UriEncodeHost(uri.host()), port));
  }
  return Status::OK();
}

}  // namespace arrow::flight::transport::grpc
