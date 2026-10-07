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

#include <grpcpp/support/server_callback.h>

namespace arrow::flight::transport::grpc {

arrow::Result<GrpcServerEndpoint> ParseServerEndpoint(const FlightServerOptions& options,
                                                       const arrow::util::Uri& uri) {
  GrpcServerEndpoint endpoint;
  endpoint.location = options.location;
  const std::string scheme = uri.scheme();
  if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
    std::stringstream address;
    address << arrow::util::UriEncodeHost(uri.host()) << ':' << uri.port_text();
    endpoint.address = address.str();

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
      endpoint.credentials = ::grpc::SslServerCredentials(ssl_options);
    } else {
      endpoint.credentials = ::grpc::InsecureServerCredentials();
    }
    return endpoint;
  }
  if (scheme == kSchemeGrpcUnix) {
    std::stringstream address;
    address << "unix:" << uri.path();
    endpoint.address = address.str();
    endpoint.credentials = ::grpc::InsecureServerCredentials();
    return endpoint;
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

}  // namespace arrow::flight::transport::grpc
