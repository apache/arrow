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

// Dummy file for checking if TlsCredentialsOptions supports
// set_verify_server_certs. gRPC starting from 1.43 added this boolean
// flag as opposed to prior versions which used an enum. This is for
// supporting disabling server validation when using TLS.

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/security/tls_credentials_options.h>

static void check() {
  // 1.36 uses an enum; 1.43 uses booleans
  auto options = std::make_shared<grpc::experimental::TlsChannelCredentialsOptions>();
  options->set_check_call_host(false);
  options->set_verify_server_certs(false);
}

int main(int argc, const char** argv) {
  check();
  return 0;
}
