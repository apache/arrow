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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

/// \brief The context of one async call: GrpcServerCallContext plus the
/// callbacks AsyncGenericFlightServerBase::AddCancelCallback() registers, which
/// AsyncReactorBase::OnCancel() runs.
class AsyncCallContext : public GrpcServerCallContext<::grpc::CallbackServerContext> {
 public:
  explicit AsyncCallContext(::grpc::CallbackServerContext* context)
      : GrpcServerCallContext<::grpc::CallbackServerContext>(context) {}

  arrow::Status AddCancelCallback(std::function<void()> callback) const {
    {
      std::lock_guard<std::mutex> lock(cancel_->mutex);
      if (!cancel_->cancelled) {
        cancel_->callbacks.push_back(std::move(callback));
        return arrow::Status::OK();
      }
    }
    // Already cancelled: run it now, on the caller's thread, unlocked.
    callback();
    return arrow::Status::OK();
  }

  /// \brief Run the registered callbacks once, unlocked, so a callback may
  /// register another (which then runs inline). Called by
  /// AsyncReactorBase::OnCancel() only.
  void RunCancelCallbacks() {
    std::vector<std::function<void()>> callbacks;
    {
      std::lock_guard<std::mutex> lock(cancel_->mutex);
      cancel_->cancelled = true;
      callbacks.swap(cancel_->callbacks);
    }
    for (auto& callback : callbacks) {
      callback();
    }
  }

 private:
  struct CancelCallbacks {
    std::mutex mutex;
    bool cancelled = false;
    std::vector<std::function<void()>> callbacks;
  };
  /// Behind a pointer so the context stays movable.
  std::unique_ptr<CancelCallbacks> cancel_ = std::make_unique<CancelCallbacks>();
};

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

  /// \brief gRPC's cancellation (client cancel, deadline, network failure).
  ///
  /// Final, so every reactor does both halves in this order: the transport's
  /// teardown settles the RPC's status, then the callbacks handlers registered
  /// with AsyncGenericFlightServerBase::AddCancelCallback() run.
  void OnCancel() final {
    TearDownOnCancel();
    flight_context_.RunCancelCallbacks();
  }

  void OnDone() override { ReleaseHold(); }

  /// \brief HoldToken's deleter: drops the reference the token owns.
  struct ReleaseHoldDeleter {
    void operator()(AsyncReactorBase* reactor) const { reactor->ReleaseHold(); }
  };

  /// \brief A counted reference to a reactor, dropped when the token is destroyed.
  ///
  /// Capture one in every callback that may run after OnDone(), and hand it on
  /// with std::move. Never call release() on it: unique_ptr::release() gives the
  /// pointer up without dropping the reference, so the reactor is never deleted.
  using HoldToken = std::unique_ptr<AsyncReactorBase, ReleaseHoldDeleter>;

  /// \brief Take a reference: the reactor stays alive until the token is destroyed.
  ///
  /// Public so the DoExchange reader and writer can pin the reactor they reach
  /// through a back-pointer; the raw Hold()/ReleaseHold() stay protected.
  [[nodiscard]] HoldToken hold() {
    Hold();
    return HoldToken(this);
  }

 protected:
  /// \brief Finish the RPC with this status, later calls are no-ops.
  ///
  /// The status is the one the client sees, so the first one wins: the other
  /// paths (cancel after error, error after cancel) must not overwrite it.
  void FinishOnce(arrow::Status status) {
    bool expected = false;
    if (finished_.compare_exchange_strong(expected, true)) {
      // FinishRequest() runs the middleware's CallCompleted(), application code
      // that may call back into this RPC, so it runs before taking write_mutex_:
      // finished_ is already set, so WriteIfLive() starts nothing meanwhile.
      const ::grpc::Status grpc_status = flight_context_.FinishRequest(status);
      std::lock_guard<std::mutex> lock(write_mutex_);
      Finish(grpc_status);
    }
  }

  /// Whether FinishOnce() has already run (the RPC's status is settled).
  bool finished() const { return finished_.load(); }

  /// The transport's teardown on cancellation: finish the RPC as cancelled.
  /// Reactors with more to unwind override it and call FinishOnce() first.
  virtual void TearDownOnCancel() { FinishOnce(arrow::Status::Cancelled()); }

  /// The call context: handlers and the teardown paths need it.
  AsyncCallContext& flight_context() { return flight_context_; }

  /// \brief Run `start_write` unless the RPC has already been finished.
  ///
  /// A handler continuation runs on the application's thread and may pass a
  /// bare finished() check just before another path (a cancel, a deadline)
  /// calls Finish() and gRPC tears the call down. Running the write under
  /// write_mutex_ closes that window: FinishOnce() takes the same mutex, so
  /// either the write wins and Finish() waits for it to be issued, or
  /// finished() is already set and no write is started. A HoldToken keeps the
  /// reactor alive but not the underlying gRPC call, which is why the check
  /// alone is not enough.
  /// \return true if `start_write` ran.
  template <typename StartWriteFn>
  bool WriteIfLive(StartWriteFn&& start_write) {
    std::lock_guard<std::mutex> lock(write_mutex_);
    if (finished()) {
      return false;
    }
    start_write();
    return true;
  }

  /// \brief The mutex guarding write initiation, see WriteIfLive().
  std::mutex& write_mutex() { return write_mutex_; }

  /// Account for work that may outlive the callback that started it.
  void Hold() { refs_.fetch_add(1, std::memory_order_relaxed); }

  /// Drop a reference, deleting the reactor once the last one is gone.
  void ReleaseHold() {
    if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      delete this;
    }
  }

  /// The request message StartRead() fills and the message StartWrite() sends.
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;

 private:
  AsyncCallContext flight_context_;
  std::mutex write_mutex_;
  std::atomic<bool> finished_{false};
  std::atomic<int> refs_{1};
  inline static std::atomic<int64_t> live_reactors_{0};
};

/// \brief A unary RPC: read one FlightDescriptor, write the handler's response.
///
/// T is the type of the response object, and PbT is the corresponding protobuf type.
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
          if (!result.ok()) {
            FinishOnce(result.status());
            return;
          }
          PbT response;
          const auto status = SerializeOrNotFound(*result, &response);
          if (!status.ok()) {
            FinishOnce(status);
            return;
          }
          write_buf_ = MakeWriteBuffer(response);
          WriteIfLive([this] { StartWrite(&write_buf_); });
        });
  }

  void OnWriteDone(bool ok) override {
    FinishOnce(
        ok ? arrow::Status::OK()
           : MakeFlightError(FlightStatusCode::Internal, "Failed to write response"));
  }

 private:
  HandlerFn handler_;
};

::grpc::ServerGenericBidiReactor* MakeHandshakeReactor(AsyncCallContext flight_context,
                                                       HandshakeFn handshake_handler);
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
