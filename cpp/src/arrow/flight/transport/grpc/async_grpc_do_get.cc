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

#include <atomic>
#include <memory>
#include <optional>
#include <utility>

namespace arrow::flight::transport::grpc::detail {

namespace {

/// Serve one DoGet RPC over the generic callback API.
class DoGetReactor : public AsyncReactorBase {
 public:
  DoGetReactor(AsyncCallContext flight_context, AsyncGenericFlightServerBase* base)
      : AsyncReactorBase(std::move(flight_context)), base_(base) {
    StartRead(&request_buf_);
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to read request"));
      return;
    }

    const auto ticket = ParseTicket();
    if (!ticket.ok()) {
      FinishOnce(ticket.status());
      return;
    }

    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> future =
        base_->DoGetAsync(flight_context(), *ticket);
    future.AddCallback(
        [this, token = hold()](
            const arrow::Result<std::shared_ptr<AsyncFlightDataStream>>& result) {
          if (!finished()) {
            if (!result.ok()) {
              FinishOnce(result.status());
            } else {
              async_data_stream_ = result.ValueOrDie();
              if (async_data_stream_ == nullptr) {
                FinishOnce(arrow::Status::KeyError("No data in this flight"));
              } else {
                WriteNextPayload();
              }
            }
          }
        });
  }

  void OnWriteDone(bool ok) override {
    if (finished()) {
      return;
    }
    if (!ok) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Write failed"));
      ARROW_WARN_NOT_OK(CloseStreamOnce(),
                        "DoGet: closing the data stream after a failed write failed");
      return;
    }
    WriteNextPayload();
  }

  void OnCancel() override {
    FinishOnce(arrow::Status::Cancelled());
    ARROW_WARN_NOT_OK(CloseStreamOnce(),
                      "DoGet: closing the stream of a cancelled call failed");
  }

 private:
  arrow::Result<Ticket> ParseTicket() {
    return ParseProtoRequest<pb::Ticket, Ticket>(request_buf_, "Ticket");
  }

  Status CloseStreamOnce() {
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
      return Status::OK();
    }
    if (async_data_stream_ == nullptr) {
      return Status::OK();
    }
    return async_data_stream_->Close();
  }

  void WriteNextPayload() {
    if (finished()) {
      return;
    }
    HoldToken token = hold();

    arrow::Future<std::optional<FlightPayload>> next;
    if (!wrote_schema_) {
      next = async_data_stream_->GetSchemaPayloadAsync().Then(
          [](FlightPayload payload) -> std::optional<FlightPayload> {
            return {std::move(payload)};
          });
    } else {
      next = async_data_stream_->NextAsync();
    }
    next.AddCallback([this, token = token.release()](
                         arrow::Result<std::optional<FlightPayload>> result) {
      if (!finished()) {
        if (!result.ok()) {
          FinishOnce(result.status());
          ARROW_WARN_NOT_OK(
              CloseStreamOnce(),
              "DoGet: closing the data stream after a failed payload failed");
        } else {
          wrote_schema_ = true;
          std::optional<FlightPayload> payload = std::move(*result);
          if (!payload.has_value()) {
            FinishOnce(CloseStreamOnce());
          } else {
            bool own_buffer = false;
            const ::grpc::Status grpc_status =
                FlightDataSerialize(*payload, &write_buf_, &own_buffer);
            if (!grpc_status.ok()) {
              FinishOnce(MakeFlightError(FlightStatusCode::Internal,
                                         grpc_status.error_message()));
              ARROW_WARN_NOT_OK(
                  CloseStreamOnce(),
                  "DoGet: closing the data stream after a failed serialization failed");
            } else {
              StartWrite(&write_buf_);
            }
          }
        }
      }
    });
  }

  AsyncGenericFlightServerBase* base_;
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
  std::shared_ptr<AsyncFlightDataStream> async_data_stream_;
  bool wrote_schema_ = false;
  std::atomic<bool> closed_{false};
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeDoGetReactor(AsyncCallContext flight_context,
                                                   AsyncGenericFlightServerBase* base) {
  return new DoGetReactor(std::move(flight_context), base);
}

}  // namespace arrow::flight::transport::grpc::detail