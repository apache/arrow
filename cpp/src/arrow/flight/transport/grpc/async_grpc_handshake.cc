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

#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"

#include <string>
#include <utility>

namespace arrow::flight::transport::grpc::detail {

namespace {

/// Serve the Handshake RPC over the generic callback API. The handler is
/// synchronous, so every path finishes the RPC from the callback that started
/// it.
class HandshakeReactor final : public AsyncReactorBase {
 public:
  HandshakeReactor(AsyncCallContext flight_context, AsyncGenericFlightServerBase* base)
      : AsyncReactorBase(std::move(flight_context)), base_(base) {
    StartRead(&request_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to read request"));
      return;
    }
    auto request =
        ParseProtoRequest<pb::HandshakeRequest>(request_buf_, "HandshakeRequest");
    if (!request.ok()) {
      FinishOnce(std::move(request).status());
      return;
    }
    std::string response;
    const auto status = base_->Handshake(flight_context(), request->payload(), &response);
    if (!status.ok()) {
      FinishOnce(status);
      return;
    }
    pb::HandshakeResponse pb_response;
    pb_response.set_payload(std::move(response));
    write_buf_ = MakeWriteBuffer(pb_response);
    WriteIfLive([this] { StartWrite(&write_buf_); });
  }

  void OnWriteDone(bool ok) override {
    FinishOnce(
        ok ? arrow::Status::OK()
           : MakeFlightError(FlightStatusCode::Internal, "Failed to write response"));
  }

 private:
  AsyncGenericFlightServerBase* base_;
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeHandshakeReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return new HandshakeReactor(std::move(flight_context), base);
}

}  // namespace arrow::flight::transport::grpc::detail
