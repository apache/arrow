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

#include "arrow/flight/data_plane/types.h"
#include "arrow/flight/data_plane/internal.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

namespace arrow {
namespace flight {
namespace internal {

namespace {

// data plane registry (name -> data plane maker)
struct Registry {
  std::map<const std::string, DataPlaneMaker> makers;

  // register all data planes on creation of registry singleton
  Registry() {
#ifdef FLIGHT_DP_SHM
    Register("shm", GetShmDataPlaneMaker());
#endif
  }

  static const Registry& instance() {
    static const Registry registry;
    return registry;
  }

  void Register(const std::string& name, const DataPlaneMaker& maker) {
    DCHECK_EQ(makers.find(name), makers.end());
    makers[name] = maker;
  }

  arrow::Result<DataPlaneMaker> GetDataPlaneMaker(const std::string& uri) const {
    const std::string name = uri.substr(0, uri.find(':'));
    auto it = makers.find(name);
    if (it == makers.end()) {
      return Status::Invalid("unknown data plane: ", name);
    }
    return it->second;
  }
};

std::string GetGrpcMetadata(const grpc::ServerContext& context, const std::string& key) {
  const auto client_metadata = context.client_metadata();
  const auto found = client_metadata.find(key);
  std::string token;
  if (found == client_metadata.end()) {
    DCHECK(false);
    token = "";
  } else {
    token = std::string(found->second.data(), found->second.length());
  }
  return token;
}

// TODO(yibo): getting data plane uri from env var is bad, shall we extend
// location to support two uri (control, data)? or any better approach to
// negotiate data plane uri?
std::string DataUriFromLocation(const Location& /*location*/) {
  auto result = arrow::internal::GetEnvVar("FLIGHT_DATAPLANE");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  return "";  // empty uri -> default grpc data plane
}

}  // namespace

arrow::Result<std::unique_ptr<ClientDataPlane>> ClientDataPlane::Make(
    const Location& location) {
  const std::string uri = DataUriFromLocation(location);
  if (uri.empty()) return arrow::internal::make_unique<ClientDataPlane>();
  ARROW_ASSIGN_OR_RAISE(auto maker, Registry::instance().GetDataPlaneMaker(uri));
  return maker.make_client(uri);
}

arrow::Result<std::unique_ptr<ServerDataPlane>> ServerDataPlane::Make(
    const Location& location) {
  const std::string uri = DataUriFromLocation(location);
  if (uri.empty()) return arrow::internal::make_unique<ServerDataPlane>();
  ARROW_ASSIGN_OR_RAISE(auto maker, Registry::instance().GetDataPlaneMaker(uri));
  return maker.make_server(uri);
}

void ClientDataPlane::AppendStreamMap(
    grpc::ClientContext* context, const std::map<std::string, std::string>& stream_map) {
  for (const auto& kv : stream_map) {
    context->AddMetadata(kv.first, kv.second);
  }
}

std::map<std::string, std::string> ServerDataPlane::ExtractStreamMap(
    const grpc::ServerContext& context) {
  std::map<std::string, std::string> stream_map;
  for (const auto& key : stream_keys()) {
    stream_map[key] = GetGrpcMetadata(context, key);
  }
  return stream_map;
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
