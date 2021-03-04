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

// Dummy file for checking if TlsCredentialsOptions exists in
// the grpc::experimental namespace. gRPC starting from 1.34
// put it here. This is for supporting disabling server
// validation when using TLS.

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/tls_credentials_options.h>

// Dummy file for checking if TlsCredentialsOptions exists in
// the grpc::experimental namespace. gRPC starting from 1.34
// puts it here. This is for supporting disabling server
// validation when using TLS.

static void check() {
  // In 1.34, there's no parameterless constructor; in 1.36, there's
  // only a parameterless constructor
  auto options =
      std::make_shared<grpc::experimental::TlsChannelCredentialsOptions>(nullptr);
  options->set_server_verification_option(
      grpc_tls_server_verification_option::GRPC_TLS_SERVER_VERIFICATION);
}

int main(int argc, const char** argv) {
  check();
  return 0;
}
