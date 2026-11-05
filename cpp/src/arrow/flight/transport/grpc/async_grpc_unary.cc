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

#include <functional>
#include <memory>
#include <utility>

namespace arrow::flight::transport::grpc::detail {

namespace {

/**
 * A generic unary reactor for handling async gRPC calls in the Flight service.
 * T is the type of the response object, and PbT is the corresponding protobuf type.
 */
template <typename T, typename PbT>
class UnaryReactor final : public AsyncReactorBase {
 public:
  using HandlerFn = std::function<arrow::Future<std::shared_ptr<T>>(
      const ServerCallContext&, const FlightDescriptor&)>;

  UnaryReactor(AsyncCallContext flight_context, HandlerFn handler)
      : AsyncReactorBase(std::move(flight_context)), handler_(std::move(handler)) {
    StartRead(&request_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to read request"));
      return;
    }
    const auto descriptor = ParseProtoRequest<pb::FlightDescriptor, FlightDescriptor>(
        request_buf_, "FlightDescriptor");
    if (!descriptor.ok()) {
      FinishOnce(descriptor.status());
      return;
    }
    arrow::Future<std::shared_ptr<T>> future = handler_(flight_context(), *descriptor);
    future.AddCallback(
        [this, token = hold()](const arrow::Result<std::shared_ptr<T>>& result) mutable {
          if (!finished()) {
            if (!result.ok()) {
              FinishOnce(result.status());
            } else {
              PbT response;
              const auto status = SerializeOrNotFound(*result, &response);
              if (!status.ok()) {
                FinishOnce(status);
              } else {
                response_buf_ = MakeWriteBuffer(response);
                WriteIfLive([this] { StartWrite(&response_buf_); });
              }
            }
          }
        });
  }

  void OnWriteDone(bool ok) override {
    if (finished()) {
      return;
    }
    FinishOnce(
        ok ? arrow::Status::OK()
           : MakeFlightError(FlightStatusCode::Internal, "Failed to write response"));
  }

 private:
  HandlerFn handler_;
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer response_buf_;
};

template <typename T, typename PbT, typename HandlerFn>
::grpc::ServerGenericBidiReactor* MakeUnaryReactor(AsyncCallContext flight_context,
                                                   HandlerFn handler) {
  return new UnaryReactor<T, PbT>(std::move(flight_context), std::move(handler));
}

}  // namespace

::grpc::ServerGenericBidiReactor* MakeGetFlightInfoReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return MakeUnaryReactor<FlightInfo, pb::FlightInfo>(
      std::move(flight_context),
      [base](const ServerCallContext& context, const FlightDescriptor& descriptor) {
        return base->GetFlightInfoAsync(context, descriptor);
      });
}

::grpc::ServerGenericBidiReactor* MakeGetSchemaReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return MakeUnaryReactor<SchemaResult, pb::SchemaResult>(
      std::move(flight_context),
      [base](const ServerCallContext& context, const FlightDescriptor& descriptor) {
        return base->GetSchemaAsync(context, descriptor);
      });
}

::grpc::ServerGenericBidiReactor* MakePollFlightInfoReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return MakeUnaryReactor<PollInfo, pb::PollInfo>(
      std::move(flight_context),
      [base](const ServerCallContext& context, const FlightDescriptor& descriptor) {
        return base->PollFlightInfoAsync(context, descriptor);
      });
}

}  // namespace arrow::flight::transport::grpc::detail
