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

#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"
#include "arrow/ipc/writer.h"

namespace arrow::flight::transport::grpc::detail {

// DoExchange: the bidirectional RPC.
//
// Its handler is asynchronous user code: it receives an
// AsyncFlightMessageReader/AsyncFlightMessageWriter pair and returns a future
// the reactor finishes the RPC with. The three classes below are mutually
// coupled on purpose -- the reader asks the reactor for the next read, the
// writer asks it to write a payload, and the reactor drives both from the
// gRPC callbacks -- so they are declared together here and implemented one
// per translation unit (async_grpc_exchange_reader.cc,
// async_grpc_exchange_writer.cc, async_grpc_exchange_reactor.cc).

class ExchangeReactor;

/// \brief The AsyncFlightMessageReader of one DoExchange RPC.
///
/// Decoding reuses the push decoder DoPut uses, so the wire shapes behave
/// identically: the schema and dictionary messages are read through (they are
/// not chunks), a metadata-only message is a chunk with a null data member,
/// and the end of the exchange is the chunk whose members are both null.
/// Messages are pulled one demand at a time: NextAsync() arms the next read,
/// and the chunk (or the end, or the failure) resolves the demand.
///
/// The reactor drives the I/O (it owns the read buffer and calls OnMessage /
/// OnReadClosed / OnReadFailed), this class owns the decode state machine.
class ExchangeReader final : public AsyncFlightMessageReader,
                             public std::enable_shared_from_this<ExchangeReader> {
 public:
  ExchangeReader();

  const FlightDescriptor& descriptor() const override;

  arrow::Future<FlightStreamChunk> NextAsync() override;

  /// \brief The reactor wires itself before the handler starts.
  void SetReactor(ExchangeReactor* reactor);
  /// \brief Called by the reactor once the exchange is over: from then on the
  /// handler's reader must refuse to start reads instead of calling into a
  /// freed reactor.
  void DetachReactor();
  /// \brief The reactor sets the descriptor from the first message, before the
  /// handler starts.
  void SetDescriptor(const FlightDescriptor& descriptor);
  /// \brief The reactor hands over the already-read first message, the reader
  /// consumes it on its first NextAsync().
  void OfferFirstRead(internal::FlightData data);

  /// \brief The reactor's OnReadDone: a message was read.
  void OnMessage(internal::FlightData data);
  /// \brief The reactor's OnReadDone with ok == false: the read side is over.
  void OnReadClosed();
  /// \brief The reactor read a message that could not be deserialized.
  void OnReadFailed(Status status);
  /// \brief The RPC was cancelled: fail the demand so the handler unwinds.
  void OnCancelled();

 private:
  /// The decoder's listener: chunks are handed to the reader.
  class ChunkListener;

  void ResolvePending(FlightStreamChunk chunk);
  void ResolvePending(const Status& status);

  /// \brief Decode one message and resolve the demand it satisfies, or keep
  /// reading when the message was read through (a schema or dictionary
  /// message).
  void Consume(internal::FlightData data);

  /// \brief Arm the next read through the reactor, defined in
  /// async_grpc_exchange_reader.cc, which has the reactor's definition.
  void RequestRead();

  /// Guarded by mutex_.
  ExchangeReactor* reactor_ = nullptr;
  std::shared_ptr<ChunkListener> listener_;
  AsyncFlightMessageDecoder decoder_;
  FlightDescriptor descriptor_;
  std::optional<internal::FlightData> first_read_;
  std::optional<FlightStreamChunk> buffered_;
  arrow::Future<FlightStreamChunk> pending_;
  Status read_error_;
  bool read_in_flight_ = false;
  bool read_closed_ = false;
  std::mutex mutex_;
};

/// \brief The AsyncFlightMessageWriter of one DoExchange RPC.
///
/// Record batches are serialized through the same IPC writer stack the
/// synchronous transport uses (dictionary messages included) into an
/// IpcPayloadWriter sink that queues FlightPayloads, the reactor then writes
/// them one at a time. Each method's future resolves when its messages have
/// been handed to the reactor, or with the first failure.
class ExchangeWriter final : public AsyncFlightMessageWriter {
 public:
  explicit ExchangeWriter(ExchangeReactor* reactor);

  /// \brief Called by the reactor once the exchange is over: from then on the
  /// handler's writer must not call into a freed reactor.
  void DetachReactor();

  arrow::Future<> BeginAsync(std::shared_ptr<Schema> schema) override;
  arrow::Future<> WriteRecordBatchAsync(const RecordBatch& batch) override;
  arrow::Future<> WriteWithMetadataAsync(const RecordBatch& batch,
                                         std::shared_ptr<Buffer> app_metadata) override;
  arrow::Future<> WriteMetadataAsync(std::shared_ptr<Buffer> app_metadata) override;
  arrow::Future<> CloseAsync() override;

  /// \brief The reactor's OnWriteDone: continue flushing the queue.
  void OnWriteDone(bool ok);
  /// \brief The RPC was cancelled: fail the pending write so the handler
  /// unwinds.
  void OnCancelled();

 private:
  /// The IpcPayloadWriter that queues the IPC payloads the writer stack
  /// produces, the schema, dictionary and record-batch messages all travel
  /// this way, as on the synchronous path.
  class PayloadSink;

  arrow::Future<> WriteBatch(const RecordBatch& batch);

  /// \brief Write the queued payloads one at a time, the returned future
  /// resolves when the queue is empty, or with the first failure.
  arrow::Future<> Flush();

  /// \brief Write queued payloads through the reactor, defined in
  /// async_grpc_exchange_writer.cc, which has the reactor's definition.
  void Pump();

  void FailFlush(Status status);

  /// Set by the constructor, cleared by DetachReactor(); read under mutex_.
  ExchangeReactor* reactor_;
  std::unique_ptr<ipc::RecordBatchWriter> batch_writer_;
  ipc::IpcWriteOptions options_ = ipc::IpcWriteOptions::Defaults();
  std::shared_ptr<Buffer> pending_app_metadata_;
  std::deque<FlightPayload> queue_;
  arrow::Future<> flush_;
  bool write_in_flight_ = false;
  std::mutex mutex_;
};

/// \brief Serve one DoExchange RPC.
///
/// The handler receives an AsyncFlightMessageReader/Writer pair and returns a
/// future, the RPC then runs entirely on continuations. The reader pulls one
/// message per NextAsync() demand (the reactor arms the read), the writer
/// serializes through the IPC writer stack and the reactor writes one message
/// per StartWrite, and the handler's future completing finishes the RPC with
/// its status. No callback thread is ever held by the handler.
class ExchangeReactor final : public AsyncReactorBase {
 public:
  ExchangeReactor(AsyncCallContext flight_context, AsyncGenericFlightServerBase* base);

  void OnReadDone(bool ok) override;
  void OnWriteDone(bool ok) override;
  void TearDownOnCancel() override;
  void OnDone() override;

  /// \brief The reader's demand: arm the next read. The demand guarantees at
  /// most one read is outstanding (ExchangeReader::RequestRead is the only
  /// caller).
  void StartReadNext();

  /// \brief Serialize one payload into the write buffer and start the write.
  /// Called by ExchangeWriter, OnWriteDone() resumes it.
  arrow::Status WritePayload(FlightPayload payload);

 private:
  AsyncGenericFlightServerBase* base_;
  std::shared_ptr<ExchangeReader> reader_;
  std::shared_ptr<ExchangeWriter> writer_;
  internal::FlightData read_data_;
  bool started_ = false;
};

}  // namespace arrow::flight::transport::grpc::detail
