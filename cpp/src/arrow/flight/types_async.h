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

#include <memory>

#include "arrow/flight/type_fwd.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/options.h"
#include "arrow/type_fwd.h"

namespace arrow::flight {

class IpcPutter;

/// \defgroup flight-async Async Flight Types
/// Common types used for asynchronous Flight APIs.
/// @{

/// \brief Non-templated state for an async RPC.
class AsyncListenerBase {
 public:
  AsyncListenerBase();
  virtual ~AsyncListenerBase();

  /// \brief Request cancellation of the RPC.
  ///
  /// The RPC is not cancelled until AsyncListener::OnFinish is called.
  void TryCancel();

 private:
  friend class arrow::flight::internal::ClientTransport;
  friend class arrow::flight::IpcPutter;

  /// Transport-specific state for this RPC.  Transport
  /// implementations may store and retrieve state here via
  /// ClientTransport::SetAsyncRpc and ClientTransport::GetAsyncRpc.
  std::unique_ptr<internal::AsyncRpc> rpc_state_;
};

/// \brief Callbacks for results from async RPCs.
///
/// A single listener may not be used for multiple concurrent RPC
/// calls.  The application MUST hold the listener alive until
/// OnFinish() is called and has finished.
template <typename T>
class ARROW_FLIGHT_EXPORT AsyncListener : public AsyncListenerBase {
 public:
  /// \brief Get the next server result.
  /// This will never be called concurrently with itself or OnFinish.
  virtual void OnNext(T message) = 0;
  /// \brief Get the final status.
  /// This will never be called concurrently with itself or OnNext.
  virtual void OnFinish(TransportStatus status) = 0;
};

/// \brief Callbacks for results from async RPCs that read Arrow data.
class ARROW_FLIGHT_EXPORT IpcListener : public AsyncListener<FlightStreamChunk> {
 public:
  /// \brief Get the IPC schema.
  /// This will never be called concurrently with itself, OnNext, or OnFinish.
  virtual void OnSchema(std::shared_ptr<Schema> schema) = 0;
};

/// \brief Callbacks for DoPut.
class ARROW_FLIGHT_EXPORT IpcPutter : public AsyncListener<std::unique_ptr<Buffer>> {
 public:
  /// \brief Begin writing an IPC stream.  May only be called once.
  ///   Must be called before writing any record batches.
  void Begin(const FlightDescriptor& descriptor, std::shared_ptr<Schema> schema);

  /// \brief Write a record batch with application metadata.
  ///
  /// This may be called multiple times (but not concurrently with
  /// Begin or Write).  Batches will be queued up and written in the
  /// order they are provided.  OnWritten will be called for each
  /// batch (in order) when it has been written.
  void Write(FlightStreamChunk chunk);
  /// \brief Asynchronously write a record batch.
  void Write(std::shared_ptr<RecordBatch> batch) {
    Write(FlightStreamChunk{std::move(batch), NULLPTR});
  }
  /// \brief Asynchronously write application metadata.  May be called
  ///   before Begin.
  void Write(std::shared_ptr<Buffer> app_metadata) {
    Write(FlightStreamChunk{NULLPTR, std::move(app_metadata)});
  }
  /// \brief Indicate that the client is done writing.  Must be
  ///   called, or OnFinish will never be called.
  ///
  /// May be called even before receiving callbacks for all batches,
  /// but Write may not be called after calling this.
  void DoneWriting();

  /// \brief The number of messages in the queue.  Intended for
  ///   applications that have more sophisticated backpressure
  ///   strategies.
  size_t QueueDepth() const;

  /// \brief A Begin or Write finished.
  ///
  /// This may be called multiple times for a single Write because of
  /// dictionary batches.
  virtual void OnWritten() = 0;
};

/// \brief Callbacks for DoExchange.
class IpcExchanger : public AsyncListener<FlightStreamChunk> {};

/// @}

}  // namespace arrow::flight
