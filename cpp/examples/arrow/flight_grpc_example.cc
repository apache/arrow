// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <signal.h>
#include <cstdlib>
#include <iostream>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <gflags/gflags.h>
#include <grpc++/grpc++.h>

#include "examples/arrow/helloworld.grpc.pb.h"
#include "examples/arrow/helloworld.pb.h"

// Demonstrate registering a gRPC service alongside a Flight service
//
// The gRPC service can be accessed with a gRPC client, on the same
// port as the Flight service. Additionally, the CMake config for this
// example links against the gRPC reflection library, enabling tools
// like grpc_cli and grpcurl to list and call RPCs on the server
// without needing local copies of the Protobuf definitions.
// For example, with grpcurl (https://github.com/fullstorydev/grpcurl):
//
// grpcurl -d '{"name": "Rakka"}' -plaintext localhost:31337 HelloWorldService/SayHello
//
// Note that for applications that wish to follow the example here,
// care must be taken to ensure that Protobuf and gRPC are not
// multiply linked, else the resulting program may crash or silently
// corrupt data. In particular:
//
// * If dynamically linking Arrow Flight, then your application and
//   Arrow Flight must also dynamically link Protobuf and gRPC. (The
//   same goes for static linking.)
// * The Flight packages on some platforms may make this difficult,
//   because the Flight dynamic library will itself have statically
//   linked Protobuf and gRPC since the platform does not ship a
//   recent enough version of those dependencies.
// * The versions of Protobuf and gRPC must be the same between Flight
//   and your application.
//
// See "Using Arrow C++ in your own project" in the documentation.

DEFINE_int32(port, -1, "Server port to listen on");

namespace flight = ::arrow::flight;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

// Flight service
class SimpleFlightServer : public flight::FlightServerBase {};

// gRPC service
class HelloWorldServiceImpl : public HelloWorldService::Service {
  grpc::Status SayHello(grpc::ServerContext* ctx, const HelloRequest* request,
                        HelloResponse* reply) override {
    const std::string& name = request->name();
    if (name.empty()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Must provide a name!");
    }
    reply->set_reply("Hello, " + name);
    return grpc::Status::OK;
  }
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_port < 0) {
    // For CI
    std::cout << "Must specify a port with -port" << std::endl;
    return EXIT_SUCCESS;
  }

  std::unique_ptr<flight::FlightServerBase> server;
  server.reset(new SimpleFlightServer());

  flight::Location bind_location;
  ABORT_ON_FAILURE(
      flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port).Value(&bind_location));
  flight::FlightServerOptions options(bind_location);

  HelloWorldServiceImpl grpc_service;
  int extra_port = 0;

  options.builder_hook = [&](void* raw_builder) {
    auto* builder = reinterpret_cast<grpc::ServerBuilder*>(raw_builder);
    builder->AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(),
                              &extra_port);
    builder->RegisterService(&grpc_service);
  };
  ABORT_ON_FAILURE(server->Init(options));
  std::cout << "Listening on ports " << FLAGS_port << " and " << extra_port << std::endl;
  ABORT_ON_FAILURE(server->SetShutdownOnSignals({SIGTERM}));
  ABORT_ON_FAILURE(server->Serve());
  return EXIT_SUCCESS;
}
