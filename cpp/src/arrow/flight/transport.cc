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

#include "arrow/flight/transport.h"

#include <memory>
#include <sstream>
#include <unordered_map>

#include "arrow/flight/client_auth.h"
#include "arrow/flight/transport_server.h"
#include "arrow/flight/types.h"
#include "arrow/flight/types_async.h"
#include "arrow/ipc/message.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace internal {

::arrow::Result<std::unique_ptr<ipc::Message>> FlightData::OpenMessage() {
  return ipc::Message::Open(metadata, body);
}

bool TransportDataStream::ReadData(internal::FlightData*) { return false; }
arrow::Result<bool> TransportDataStream::WriteData(const FlightPayload&) {
  return Status::NotImplemented("Writing data for this stream");
}
Status TransportDataStream::WritesDone() { return Status::OK(); }
bool ClientDataStream::ReadPutMetadata(std::shared_ptr<Buffer>*) { return false; }
Status ClientDataStream::Finish(Status st) {
  auto server_status = DoFinish();
  if (server_status.ok()) return st;

  return Status::FromDetailAndArgs(server_status.code(), server_status.detail(),
                                   server_status.message(),
                                   ". Client context: ", st.ToString());
}

Status ClientTransport::Authenticate(const FlightCallOptions& options,
                                     std::unique_ptr<ClientAuthHandler> auth_handler) {
  return Status::NotImplemented("Authenticate for this transport");
}
arrow::Result<std::pair<std::string, std::string>>
ClientTransport::AuthenticateBasicToken(const FlightCallOptions& options,
                                        const std::string& username,
                                        const std::string& password) {
  return Status::NotImplemented("AuthenticateBasicToken for this transport");
}
Status ClientTransport::DoAction(const FlightCallOptions& options, const Action& action,
                                 std::unique_ptr<ResultStream>* results) {
  return Status::NotImplemented("DoAction for this transport");
}
void ClientTransport::DoAction(const FlightCallOptions& options, const Action& action,
                               AsyncListener<Result>* listener) {
  listener->OnFinish(TransportStatus{TransportStatusCode::kUnimplemented,
                                     "Async DoAction for this transport"});
}
Status ClientTransport::ListActions(const FlightCallOptions& options,
                                    std::vector<ActionType>* actions) {
  return Status::NotImplemented("ListActions for this transport");
}
Status ClientTransport::GetFlightInfo(const FlightCallOptions& options,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfo for this transport");
}
void ClientTransport::GetFlightInfo(const FlightCallOptions& options,
                                    const FlightDescriptor& descriptor,
                                    AsyncListener<FlightInfo>* listener) {
  listener->OnFinish(TransportStatus{TransportStatusCode::kUnimplemented,
                                     "Async GetFlightInfo for this transport"});
}
arrow::Result<std::unique_ptr<SchemaResult>> ClientTransport::GetSchema(
    const FlightCallOptions& options, const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetSchema for this transport");
}
Status ClientTransport::ListFlights(const FlightCallOptions& options,
                                    const Criteria& criteria,
                                    std::unique_ptr<FlightListing>* listing) {
  return Status::NotImplemented("ListFlights for this transport");
}
Status ClientTransport::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                              std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoGet for this transport");
}
void ClientTransport::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                            IpcListener* listener) {
  listener->OnFinish(TransportStatus{TransportStatusCode::kUnimplemented,
                                     "Async DoGet for this transport"});
}
Status ClientTransport::DoPut(const FlightCallOptions& options,
                              std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoPut for this transport");
}
void ClientTransport::DoPut(const FlightCallOptions& options, IpcPutter* listener) {
  listener->OnFinish(TransportStatus{TransportStatusCode::kUnimplemented,
                                     "Async DoPut for this transport"});
}
Status ClientTransport::DoExchange(const FlightCallOptions& options,
                                   std::unique_ptr<ClientDataStream>* stream) {
  return Status::NotImplemented("DoExchange for this transport");
}
void ClientTransport::SetAsyncRpc(AsyncListenerBase* listener,
                                  std::unique_ptr<AsyncRpc>&& rpc) {
  listener->rpc_state_ = std::move(rpc);
}
AsyncRpc* ClientTransport::GetAsyncRpc(AsyncListenerBase* listener) {
  return listener->rpc_state_.get();
}

class TransportRegistry::Impl final {
 public:
  arrow::Result<std::unique_ptr<ClientTransport>> MakeClient(
      const std::string& scheme) const {
    auto it = client_factories_.find(scheme);
    if (it == client_factories_.end()) {
      return Status::KeyError("No client transport implementation for ", scheme);
    }
    return it->second();
  }
  arrow::Result<std::unique_ptr<ServerTransport>> MakeServer(
      const std::string& scheme, FlightServerBase* base,
      std::shared_ptr<MemoryManager> memory_manager) const {
    auto it = server_factories_.find(scheme);
    if (it == server_factories_.end()) {
      return Status::KeyError("No server transport implementation for ", scheme);
    }
    return it->second(base, std::move(memory_manager));
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
  std::unordered_map<std::string, TransportRegistry::ClientFactory> client_factories_;
  std::unordered_map<std::string, TransportRegistry::ServerFactory> server_factories_;
};

TransportRegistry::TransportRegistry() { impl_ = std::make_unique<Impl>(); }
TransportRegistry::~TransportRegistry() = default;
arrow::Result<std::unique_ptr<ClientTransport>> TransportRegistry::MakeClient(
    const std::string& scheme) const {
  return impl_->MakeClient(scheme);
}
arrow::Result<std::unique_ptr<ServerTransport>> TransportRegistry::MakeServer(
    const std::string& scheme, FlightServerBase* base,
    std::shared_ptr<MemoryManager> memory_manager) const {
  return impl_->MakeServer(scheme, base, std::move(memory_manager));
}
Status TransportRegistry::RegisterClient(const std::string& scheme,
                                         ClientFactory factory) {
  return impl_->RegisterClient(scheme, std::move(factory));
}
Status TransportRegistry::RegisterServer(const std::string& scheme,
                                         ServerFactory factory) {
  return impl_->RegisterServer(scheme, std::move(factory));
}

TransportRegistry* GetDefaultTransportRegistry() {
  static TransportRegistry kRegistry;
  return &kRegistry;
}

Status ReconstructStatus(const std::string& code_str, const Status& current_status,
                         std::optional<std::string> message,
                         std::optional<std::string> detail_message,
                         std::optional<std::string> detail_bin,
                         std::shared_ptr<FlightStatusDetail> detail) {
  // Bounce through std::string to get a proper null-terminated C string
  StatusCode status_code = current_status.code();
  std::stringstream status_message;
  try {
    const auto code_int = std::stoi(code_str);
    switch (code_int) {
      case static_cast<int>(StatusCode::OutOfMemory):
      case static_cast<int>(StatusCode::KeyError):
      case static_cast<int>(StatusCode::TypeError):
      case static_cast<int>(StatusCode::Invalid):
      case static_cast<int>(StatusCode::IOError):
      case static_cast<int>(StatusCode::CapacityError):
      case static_cast<int>(StatusCode::IndexError):
      case static_cast<int>(StatusCode::Cancelled):
      case static_cast<int>(StatusCode::UnknownError):
      case static_cast<int>(StatusCode::NotImplemented):
      case static_cast<int>(StatusCode::SerializationError):
      case static_cast<int>(StatusCode::RError):
      case static_cast<int>(StatusCode::CodeGenError):
      case static_cast<int>(StatusCode::ExpressionValidationError):
      case static_cast<int>(StatusCode::ExecutionError):
      case static_cast<int>(StatusCode::AlreadyExists): {
        status_code = static_cast<StatusCode>(code_int);
        break;
      }
      default: {
        status_message << ". Also, server sent unknown or invalid Arrow status code "
                       << code_str;
        break;
      }
    }
  } catch (...) {
    status_message << ". Also, server sent unknown or invalid Arrow status code "
                   << code_str;
  }

  status_message << (message.has_value() ? *message : current_status.message());
  if (detail_message.has_value()) {
    status_message << ". Detail: " << *detail_message;
  }
  if (detail_bin.has_value()) {
    if (!detail) {
      detail = std::make_shared<FlightStatusDetail>(FlightStatusCode::Internal);
    }
    detail->set_extra_info(std::move(*detail_bin));
  }
  return Status(status_code, status_message.str(), std::move(detail));
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
