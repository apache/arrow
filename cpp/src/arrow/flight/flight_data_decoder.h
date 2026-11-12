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

#include <functional>
#include <memory>
#include <mutex>

#include "arrow/flight/types.h"
#include "arrow/flight/visibility.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/status.h"

namespace arrow::flight {

namespace internal {
struct FlightData;
}  // namespace internal

class AsyncFlightDataListener;

namespace internal {

/// \brief The per-RPC handle a transport gives an AsyncFlightDataListener so the
/// application can act on the RPC while it is in flight.
///
/// The transport owns this, the listener only holds a reference for as long as
/// the RPC is live, and asks for it under a lock the transport takes to clear
/// it (see FlightDataListenerTransport::Clear)
class ARROW_FLIGHT_EXPORT FlightDataListenerTransport {
 public:
  virtual ~FlightDataListenerTransport();

  /// \brief Finish the upload's RPC with `status`, from any thread.
  /// The caller keeps the transport alive across the call (see Hold()).
  /// \param status The status to finish the upload with.
  virtual void CancelUpload(Status status) = 0;

  /// \brief Start sending one application metadata message to the client, from
  /// any thread.
  /// \param app_metadata The metadata to attach to the next PutResult message,
  ///   copied before the call returns.
  /// \return A future completed once the message has been written, or already
  ///   failed when a message is still in flight or the upload is ending or over.
  virtual Future<> WriteMetadataAsync(const Buffer& app_metadata) = 0;

  /// \brief Pin this transport's lifetime, from any thread.
  ///
  /// Install()/Clear() publish and retire the transport through the listener's
  /// mutex, but a caller that reads the pointer and then releases the mutex
  /// before calling in (as Cancel() must, so OnFinish() can re-enter) still
  /// races with Clear() and the reactor's deletion. Hold under the mutex and
  /// pair with ReleaseHold() around the call so the object stays alive.
  virtual void Hold() = 0;

  /// \brief Drop a reference taken with Hold(). Deletes the transport at zero.
  virtual void ReleaseHold() = 0;

  /// \brief Install `transport` on `listener` for the life of the RPC.
  /// \param listener The listener to install the transport on.
  /// \param transport The transport to install.
  static void Install(const std::shared_ptr<AsyncFlightDataListener>& listener,
                      FlightDataListenerTransport* transport);

  /// \brief Clear the installed transport.
  /// \param listener The listener to clear the transport from.
  static void Clear(const std::shared_ptr<AsyncFlightDataListener>& listener);

  /// \brief Report the upload's terminal status to the listener, once.
  /// \param listener The listener to report the status to.
  /// \param status The terminal status of the upload.
  /// \return the listener's OnFinish() future. The caller must not block on it
  ///   (this runs on a gRPC callback thread) but should chain the RPC's
  ///   completion on it, so an asynchronous failure to commit is reported to the
  ///   client instead of a success. Returns a finished future when the status
  ///   was already reported.
  static Future<> ReportFinish(const std::shared_ptr<AsyncFlightDataListener>& listener,
                               Status status);
};

}  // namespace internal

/// \brief Sends application-specific metadata back to the client during an upload.
///
/// The async counterpart of FlightMetadataWriter: the transport hands one to the
/// AsyncFlightDataListener at the start of an upload (AsyncFlightDataListener::writer)
/// and it stays valid for as long as the upload's RPC runs.  Every call sends one
/// PutResult message, the client reads them with ReadMetadata() on its DoPutResult.
/// One message is in flight at a time: await each returned future before writing
/// the next. Returning a future chained on it from OnNext() holds the next read
/// until the acknowledgement is written, which is how a listener applies
/// backpressure.
class ARROW_FLIGHT_EXPORT AsyncFlightDataWriter {
 public:
  virtual ~AsyncFlightDataWriter();

  /// \brief Send a metadata message to the client.
  ///
  /// Safe to call from any thread while the upload is in flight: the write is
  /// started on the calling thread, and `app_metadata` is copied before the call
  /// returns.
  /// \param app_metadata The application metadata to send.
  /// \return A future completed, on the gRPC thread that sees the write finish,
  ///   once the message has been written (IOError if the write failed or the
  ///   upload's RPC ended first). It is already failed with Invalid when the
  ///   previous message is still in flight, or the upload is ending or over.
  Future<> WriteMetadataAsync(const Buffer& app_metadata);

 private:
  friend class internal::FlightDataListenerTransport;

  /// The transport's RPC state, set and cleared through
  /// FlightDataListenerTransport::Install/Clear, like
  /// AsyncFlightDataListener::transport_.
  internal::FlightDataListenerTransport* transport_ = nullptr;
  /// Guards transport_, since WriteMetadataAsync may be called from any thread while
  /// the transport installs or clears the pointer.
  mutable std::mutex writer_mutex_;
};

/// \brief A general listener class to receive events from FlightMessageDecoder
///
/// User must implement callback methods for interested events.
///
/// When it serves a DoPut, the transport continues on the thread that
/// completes the futures this listener returns: see the threading notes on
/// AsyncGenericFlightServerBase.
class ARROW_FLIGHT_EXPORT AsyncFlightDataListener : public ipc::Listener {
 public:
  AsyncFlightDataListener();
  ~AsyncFlightDataListener() override;

  /// \brief Called for each decoded FlightStreamChunk.
  /// \param chunk The decoded FlightStreamChunk.
  /// \return A future that completes when the chunk has been processed.
  virtual Future<> OnNext(FlightStreamChunk chunk) = 0;

  /// \brief Called when the descriptor of an upload is decoded.
  ///
  /// Fired before any schema or data of that upload, so the listener knows
  /// which upload it is being handed. A non-OK status rejects the upload.
  /// \param descriptor The decoded FlightDescriptor.
  /// \return A future that completes when the descriptor has been processed.
  virtual Future<> OnDescriptor(const FlightDescriptor& descriptor) {
    return Future<>::MakeFinished();
  }

  /// \brief Called once, when the upload ends, whichever way it ends.
  ///
  /// Runs on a transport thread, so it must not block.
  /// `status` is OK for an upload if the client ended normally, and the failure otherwise
  /// (the client went away, the transport failed, or the upload was rejected).
  ///
  /// \param status The terminal status of the upload.
  /// \return A future that completes when the finish has been processed.
  virtual Future<> OnFinish(Status status) { return Future<>::MakeFinished(); }

  /// \brief The writer for application metadata sent back to the client.
  ///
  /// Valid for as long as the upload's RPC is live (until OnFinish is reported).
  /// Never null, calls after the upload ended report the ending instead of writing.
  const std::shared_ptr<AsyncFlightDataWriter>& writer() const { return writer_; }

  /// \brief Cancel the upload with `status`, from any thread.
  ///
  /// Finishes the RPC with that status without waiting for the client to end
  /// the upload: the client's pending write or Close() reports it.
  /// Use for a server-side rejection discovered mid-upload (quota, bad batch, upstream
  /// error).
  /// Safe to call from any thread at any time: once the upload is no
  /// longer in flight this reports Invalid instead of reaching a finished RPC.
  /// \param status The status to cancel the upload with.
  ///
  /// \return OK when the cancel was handed to the transport, or Invalid when
  /// there is no upload in flight.
  Future<> Cancel(Status status);

 private:
  /// The transport installs and clears the state through
  /// FlightDataListenerTransport::Install/Clear, both under this lock.
  friend class internal::FlightDataListenerTransport;

  /// Whether OnFinish was already reported.
  std::atomic<bool> finished_{false};

  /// Never null: the listener may use it without waiting for OnDescriptor.
  std::shared_ptr<AsyncFlightDataWriter> writer_ =
      std::make_shared<AsyncFlightDataWriter>();

  mutable std::mutex transport_mutex_;
  /// Not owned: the transport's own RPC state, valid until Clear().
  internal::FlightDataListenerTransport* transport_ = nullptr;
};

/// \brief Push style stream decoder that turns raw arrow Buffers into
/// FlightStreamChunks.
///
/// This class decodes Apache Arrow Flight data format from arrow::Buffer
/// and fires events on the provided AsyncFlightDataListener.
class ARROW_FLIGHT_EXPORT AsyncFlightMessageDecoder {
 public:
  /// \brief Construct an AsyncFlightMessageDecoder with the given listener and IPC read
  /// options.
  ///
  /// \param listener The listener that will receive decoded Flight messages.
  /// \param options The IPC read options to use for decoding.
  explicit AsyncFlightMessageDecoder(
      std::shared_ptr<AsyncFlightDataListener> listener,
      ipc::IpcReadOptions options = ipc::IpcReadOptions::Defaults());
  ~AsyncFlightMessageDecoder();

  /// \brief Decode one FlightData message directly from a buffer.
  ///
  /// Fires listener->OnSchemaDecoded() on the first message containing
  /// a schema, listener->OnNext() for each subsequent record batch,
  /// metadata-only message or dictionary batch.
  ///
  /// \param[in] buffer a raw buffer directly from the transport. Example
  /// the arrow::Buffer extracted from the grpc::ByteBuffer from the gRPC transport.
  /// \return Status
  Future<> Consume(std::shared_ptr<Buffer> buffer);

  /// \brief Decode one FlightData message already deserialized by the transport.
  ///
  /// Same as Consume(Buffer) but avoids a serialization round-trip for transports
  /// that hand out an internal::FlightData directly (the gRPC client reads FlightData
  /// straight into internal::FlightData).
  /// \internal This method is intended for internal use within the Arrow library.
  Future<> Consume(internal::FlightData data);

  /// \brief The decoded schema.
  /// \return The decoded schema, or nullptr if no schema has been decoded yet.
  std::shared_ptr<Schema> schema() const;

 private:
  class AsyncFlightMessageDecoderImpl;
  std::unique_ptr<AsyncFlightMessageDecoderImpl> impl_;
};

}  // namespace arrow::flight
