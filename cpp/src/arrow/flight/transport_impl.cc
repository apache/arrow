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

#include "arrow/flight/transport_impl.h"

#include <unordered_map>

#include "arrow/flight/client_auth.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace flight {
namespace internal {

bool TransportDataStream::ReadData(internal::FlightData*) { return false; }
Status TransportDataStream::WriteData(const FlightPayload&) {
  return Status::NotImplemented("Writing data for this stream");
}
Status TransportDataStream::WritesDone() { return Status::OK(); }
Status ServerDataStream::WritePutMetadata(const Buffer&) {
  return Status::NotImplemented("Writing put metadata for this stream");
}
bool ClientDataStream::ReadPutMetadata(std::shared_ptr<Buffer>*) { return false; }
Status ClientDataStream::Finish(Status st) {
  auto server_status = Finish();
  if (server_status.ok()) return st;

  return Status::FromDetailAndArgs(server_status.code(), server_status.detail(),
                                   server_status.message(),
                                   ". Client context: ", st.ToString());
}

Status ClientTransportImpl::Authenticate(
    const FlightCallOptions& options, std::unique_ptr<ClientAuthHandler> auth_handler) {
  return Status::NotImplemented("Authenticate for this transport");
}
arrow::Result<std::pair<std::string, std::string>>
ClientTransportImpl::AuthenticateBasicToken(const FlightCallOptions& options,
                                            const std::string& username,
                                            const std::string& password) {
  return Status::NotImplemented("AuthenticateBasicToken for this transport");
}
Status ClientTransportImpl::DoAction(const FlightCallOptions& options,
                                     const Action& action,
                                     std::unique_ptr<ResultStream>* results) {
  return Status::NotImplemented("DoAction for this transport");
}
Status ClientTransportImpl::ListActions(const FlightCallOptions& options,
                                        std::vector<ActionType>* actions) {
  return Status::NotImplemented("ListActions for this transport");
}
Status ClientTransportImpl::GetFlightInfo(const FlightCallOptions& options,
                                          const FlightDescriptor& descriptor,
                                          std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfo for this transport");
}
Status ClientTransportImpl::GetSchema(const FlightCallOptions& options,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<SchemaResult>* schema_result) {
  return Status::NotImplemented("GetSchema for this transport");
}
Status ClientTransportImpl::ListFlights(const FlightCallOptions& options,
                                        const Criteria& criteria,
                                        std::unique_ptr<FlightListing>* listing) {
  return Status::NotImplemented("ListFlights for this transport");
}
Status ClientTransportImpl::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                                  std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoGet for this transport");
}
Status ClientTransportImpl::DoPut(const FlightCallOptions& options,
                                  std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoPut for this transport");
}
Status ClientTransportImpl::DoExchange(const FlightCallOptions& options,
                                       std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoExchange for this transport");
}

class TransportImplRegistry::Impl {
 public:
  arrow::Result<std::unique_ptr<ClientTransportImpl>> MakeClientImpl(
      const std::string& scheme) {
    auto it = client_factories_.find(scheme);
    if (it == client_factories_.end()) {
      return Status::KeyError("No client transport implementation for ", scheme);
    }
    return it->second();
  }
  arrow::Result<std::unique_ptr<ServerTransportImpl>> MakeServerImpl(
      const std::string& scheme) {
    auto it = server_factories_.find(scheme);
    if (it == server_factories_.end()) {
      return Status::KeyError("No server transport implementation for ", scheme);
    }
    return it->second();
  }
  Status RegisterClient(const std::string& scheme, ClientFactory factory) {
    auto it = client_factories_.insert({scheme, std::move(factory)});
    if (!it.second) {
      return Status::Invalid("Client transport already registered for ", scheme);
    }
    return Status::OK();
  }
  Status RegisterServer(const std::string& scheme, ServerFactory factory) {
    auto it = server_factories_.insert({scheme, std::move(factory)});
    if (!it.second) {
      return Status::Invalid("Server transport already registered for ", scheme);
    }
    return Status::OK();
  }

 private:
  std::unordered_map<std::string, TransportImplRegistry::ClientFactory> client_factories_;
  std::unordered_map<std::string, TransportImplRegistry::ServerFactory> server_factories_;
};

TransportImplRegistry::TransportImplRegistry() {
  impl_ = arrow::internal::make_unique<Impl>();
}
TransportImplRegistry::~TransportImplRegistry() = default;
arrow::Result<std::unique_ptr<ClientTransportImpl>> TransportImplRegistry::MakeClientImpl(
    const std::string& scheme) {
  return impl_->MakeClientImpl(scheme);
}
arrow::Result<std::unique_ptr<ServerTransportImpl>> TransportImplRegistry::MakeServerImpl(
    const std::string& scheme) {
  return impl_->MakeServerImpl(scheme);
}
Status TransportImplRegistry::RegisterClient(const std::string& scheme,
                                             ClientFactory factory) {
  return impl_->RegisterClient(scheme, std::move(factory));
}
Status TransportImplRegistry::RegisterServer(const std::string& scheme,
                                             ServerFactory factory) {
  return impl_->RegisterServer(scheme, std::move(factory));
}

TransportImplRegistry* GetDefaultTransportImplRegistry() {
  static TransportImplRegistry kRegistry;
  return &kRegistry;
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
