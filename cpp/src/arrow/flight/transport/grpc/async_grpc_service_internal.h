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

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include "arrow/flight/flight_data_decoder.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server_async.h"
#include "arrow/flight/transport/grpc/async_grpc_compat.h"
#include "arrow/flight/transport/grpc/async_grpc_service.h"
#include "arrow/flight/transport/grpc/serialization_internal.h"

namespace arrow::flight::transport::grpc::detail {

namespace pb = arrow::flight::protocol;

#if ARROW_FLIGHT_HAS_ASYNC_SERVER

using AsyncCallContext = GrpcServerCallContext<::grpc::CallbackServerContext>;

template <typename ProtoT>
::grpc::ByteBuffer MakeWriteBuffer(const ProtoT& message) {
  const std::string bytes = message.SerializeAsString();
  ::grpc::Slice slice(bytes);
  return {&slice, 1};
}

template <typename PbT>
arrow::Result<PbT> ParseProtoRequest(const ::grpc::ByteBuffer& buf,
                                     std::string_view what) {
  ARROW_ASSIGN_OR_RAISE(std::string bytes,
                        BytesFromBuffer(buf, "Failed to read request"));
  PbT pb;
  if (!pb.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
    return arrow::Status::Invalid("Failed to parse ", what);
  }
  return pb;
}

template <typename PbT, typename T>
arrow::Result<T> ParseProtoRequest(const ::grpc::ByteBuffer& buf, std::string_view what) {
  ARROW_ASSIGN_OR_RAISE(auto pb, ParseProtoRequest<PbT>(buf, what));
  T out;
  ARROW_RETURN_NOT_OK(arrow::flight::internal::FromProto(pb, &out));
  return out;
}

template <typename T, typename PbT>
arrow::Status SerializeOrNotFound(const std::shared_ptr<T>& value, PbT* out) {
  if (value == nullptr) {
    return arrow::Status::KeyError("Flight not found");
  }
  return arrow::flight::internal::ToProto(*value, out);
}

/// \brief Lifetime and termination handling shared by the callback reactors.
///
/// A reactor's callbacks run on arbitrary gRPC threads, so it cannot be
/// destroyed while one is outstanding: the RPC's own lifetime holds the initial
/// reference (released in OnDone(), after which gRPC never touches the reactor),
/// and every callback that may outlive its StartRead()/StartWrite() takes
/// another one. The last ReleaseHold() deletes the reactor.
class AsyncReactorBase : public ::grpc::ServerGenericBidiReactor {
 public:
  explicit AsyncReactorBase(AsyncCallContext flight_context)
      : flight_context_(std::move(flight_context)) {
    live_reactors_.fetch_add(1, std::memory_order_relaxed);
  }

  ~AsyncReactorBase() override { live_reactors_.fetch_sub(1, std::memory_order_relaxed); }

  /// \brief How many reactors are alive right now.
  ///
  /// A reactor is deleted by the last ReleaseHold(), so this returns to its
  /// starting value once every RPC (including the ones whose handler abandoned
  /// its future) has been torn down.
  static int64_t live_reactors() {
    return live_reactors_.load(std::memory_order_relaxed);
  }

  /// The default cancellation: report the RPC as cancelled and finish it.
  /// Reactors with their own teardown override this and call it first.
  void OnCancel() override { FinishOnce(arrow::Status::Cancelled()); }

  void OnDone() override { ReleaseHold(); }

 protected:
  /// \brief Finish the RPC with this status, later calls are no-ops.
  ///
  /// The status is the one the client sees, so the first one wins: the other
  /// paths (cancel after error, error after cancel) must not overwrite it.
  void FinishOnce(arrow::Status status) {
    bool expected = false;
    if (finished_.compare_exchange_strong(expected, true)) {
      Finish(flight_context_.FinishRequest(status));
    }
  }

  /// Whether FinishOnce() has already run (the RPC's status is settled).
  bool finished() const { return finished_.load(); }

  /// The call context: handlers and the teardown paths need it.
  AsyncCallContext& flight_context() { return flight_context_; }

  /// Account for work that may outlive the callback that started it.
  void Hold() { refs_.fetch_add(1, std::memory_order_relaxed); }

  /// Drop a reference, deleting the reactor once the last one is gone.
  void ReleaseHold() {
    if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  /// A reference the reactor owns, to be handed to a callback.
  class HoldToken {
   public:
    HoldToken() = default;
    explicit HoldToken(AsyncReactorBase* reactor) : reactor_(reactor) {
      reactor_->Hold();
    }
    HoldToken(const HoldToken&) = delete;
    HoldToken& operator=(const HoldToken&) = delete;
    HoldToken(HoldToken&& other) noexcept
        : reactor_(std::exchange(other.reactor_, nullptr)) {}
    HoldToken& operator=(HoldToken&& other) noexcept {
      if (this != &other) {
        Release();
        reactor_ = std::exchange(other.reactor_, nullptr);
      }
      return *this;
    }
    ~HoldToken() { Release(); }

    /// \brief The reference, to be captured by the callback and moved out of.
    HoldToken&& release() { return std::move(*this); }

   private:
    void Release() {
      if (reactor_ != nullptr) {
        reactor_->ReleaseHold();
        reactor_ = nullptr;
      }
    }
    AsyncReactorBase* reactor_ = nullptr;
  };

  /// Take a reference and return the token that holds it.
  HoldToken hold() { return HoldToken(this); }

 private:
  AsyncCallContext flight_context_;
  std::atomic<bool> finished_{false};
  std::atomic<int> refs_{1};
  inline static std::atomic<int64_t> live_reactors_{0};
};

::grpc::ServerGenericBidiReactor* MakeHandshakeReactor(AsyncCallContext flight_context,
                                                       HandshakeFn handshake_handler);
::grpc::ServerGenericBidiReactor* MakeGetFlightInfoReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeGetSchemaReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakePollFlightInfoReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeDoGetReactor(AsyncCallContext flight_context,
                                                   AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeDoPutReactor(
    AsyncCallContext flight_context, std::shared_ptr<AsyncFlightDataListener> listener);
::grpc::ServerGenericBidiReactor* MakeListActionsReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeDoActionReactor(AsyncCallContext flight_context,
                                                      AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeListFlightsReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);
::grpc::ServerGenericBidiReactor* MakeExchangeReactor(AsyncCallContext flight_context,
                                                      AsyncGenericFlightServerBase* base);

#endif  // ARROW_FLIGHT_HAS_ASYNC_SERVER

}  // namespace arrow::flight::transport::grpc::detail