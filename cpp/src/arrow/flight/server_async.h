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

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/flight/flight_data_decoder.h"
#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/util/future.h"
#include "arrow/util/macros.h"

namespace arrow::flight {

/// \brief Interface that produces a sequence of IPC payloads to be sent in
/// FlightData protobuf messages, where a payload may arrive later.
class ARROW_FLIGHT_EXPORT AsyncFlightDataStream {
 public:
  virtual ~AsyncFlightDataStream();

  /// \brief The schema of the payloads this stream produces.
  ///
  /// The transport never calls this: the schema the client sees is the one
  /// GetSchemaPayloadAsync() returns.
  /// \return The schema of the payloads this stream produces.
  virtual std::shared_ptr<Schema> schema() = 0;

  /// \brief Like GetSchemaPayload(), but the payload arrives later.
  ///
  /// Called once, before any NextAsync() payload is requested.
  /// \return A future completed with the schema payload. A non-OK status fails the RPC.
  virtual arrow::Future<FlightPayload> GetSchemaPayloadAsync() = 0;

  /// \brief Like Next(), but the payload arrives later.
  ///
  /// The transport keeps one call in flight at a time and only asks again once
  /// the previous payload has been written.
  ///
  /// \return A future completed with the next payload, or with a nullopt once the stream
  /// is exhausted. A non-OK status fails the RPC.
  virtual arrow::Future<std::optional<FlightPayload>> NextAsync() = 0;

  /// \brief Stop the stream: the RPC it was producing for is over.
  ///
  /// Called once, possibly from any thread, whichever way the RPC ends, including for a
  /// stream DoGetAsync() resolves after the RPC was cancelled. A pending
  /// NextAsync()/GetSchemaPayloadAsync() future must be resolved here (or abandoned
  /// knowingly): the transport does not wait for it.
  /// Its return status becomes the RPC's final status only on the normal end-of-stream
  /// path (after NextAsync() reports end of stream).
  /// On every other path (write failure, cancellation) the RPC already has a status and
  /// the returned status is discarded.
  /// \return The status of closing the stream.
  virtual Status Close() { return Status::OK(); }
};

/// \brief An asynchronous iterator of FlightInfo instances, returned by
/// ListFlightsAsync.
///
/// The transport pulls it one NextAsync() at a time and writes each
/// FlightInfo to the client as it becomes available.
class ARROW_FLIGHT_EXPORT AsyncFlightListing {
 public:
  virtual ~AsyncFlightListing();

  /// \brief The next FlightInfo in the listing.
  /// \return A future completed with the next FlightInfo, or with a nullptr
  /// once the listing is exhausted. A non-OK status fails the RPC.
  virtual arrow::Future<std::shared_ptr<FlightInfo>> NextAsync() = 0;
};

/// \brief An asynchronous iterator of Result instances, returned by
/// DoActionAsync.
class ARROW_FLIGHT_EXPORT AsyncResultStream {
 public:
  virtual ~AsyncResultStream();

  /// \brief The next Result in the stream.
  /// \return A future completed with the next Result, or with a nullptr once
  /// the stream is exhausted. A non-OK status fails the RPC.
  virtual arrow::Future<std::shared_ptr<Result>> NextAsync() = 0;
};

/// \brief The reader of one DoExchange: the messages the client sends, pulled
/// one NextAsync() at a time.
///
/// Runs on the thread that calls it, at most one NextAsync() may be
/// outstanding.
/// Schema and dictionary messages are read through (they are not chunks), a metadata-only
/// message is a chunk with a null data member, and the chunk whose members are both null
/// ends the exchange.
/// The reader must not be used after the future returned by DoExchangeAsync completes.
class ARROW_FLIGHT_EXPORT AsyncFlightMessageReader {
 public:
  virtual ~AsyncFlightMessageReader();

  /// \brief The descriptor of this exchange.
  virtual const FlightDescriptor& descriptor() const = 0;

  /// \brief The next message of the exchange, decoded.
  /// \return A future completed with the next chunk, both of its members are
  /// null at the end of the exchange. A non-OK status fails the RPC.
  virtual arrow::Future<FlightStreamChunk> NextAsync() = 0;
};

/// \brief The writer of one DoExchange: the messages the server sends back.
///
/// Runs on the thread that calls it, await each returned future before
/// writing again. The writer must not be used after the future returned by
/// DoExchangeAsync completes.
class ARROW_FLIGHT_EXPORT AsyncFlightMessageWriter {
 public:
  virtual ~AsyncFlightMessageWriter();

  /// \brief Start the writer with the schema of the messages it will write.
  /// Must be called before any record batch is written.
  /// \param schema The schema of the messages to be written.
  /// \return A future that completes once the writer is ready to accept messages.
  virtual arrow::Future<> BeginAsync(std::shared_ptr<Schema> schema) = 0;

  /// \brief Write one record batch.
  /// \param batch The record batch to write.
  /// \return A future that completes once the batch has been written.
  virtual arrow::Future<> WriteRecordBatchAsync(const RecordBatch& batch) = 0;

  /// \brief Write one application-metadata-only message.
  /// \param app_metadata The application metadata to write.
  /// \return A future that completes once the metadata has been written.
  virtual arrow::Future<> WriteMetadataAsync(std::shared_ptr<Buffer> app_metadata) = 0;

  /// \brief Write one record batch with application metadata attached.
  /// \param batch The record batch to write.
  /// \param app_metadata The application metadata to attach.
  /// \return A future that completes once the batch and metadata have been written.
  virtual arrow::Future<> WriteWithMetadataAsync(
      const RecordBatch& batch, std::shared_ptr<Buffer> app_metadata) = 0;

  /// \brief Finish the write side: end the IPC stream.
  ///
  /// Call once, after the last message, then await it before completing
  /// DoExchangeAsync's future.
  /// \return A future that completes once the write side has been closed.
  virtual arrow::Future<> CloseAsync() = 0;
};

/// \brief a Flight server served by the async generic gRPC
/// callback service.
///
/// Derive from this class alone and implement the handlers you serve:
///
/// \code
/// class MyServer : public arrow::flight::AsyncGenericFlightServerBase {
///   arrow::Future<std::shared_ptr<arrow::flight::FlightInfo>> GetFlightInfoAsync(
///       const arrow::flight::ServerCallContext&,
///       const arrow::flight::FlightDescriptor&) override;
///
///   arrow::Future<std::shared_ptr<arrow::flight::AsyncFlightDataStream>> DoGetAsync(
///       const arrow::flight::ServerCallContext&,
///       const arrow::flight::Ticket&) override;
/// };
/// \endcode
///
/// Every handler returns a future: complete it from your own thread or pool
/// when the answer is ready, and no gRPC callback thread is ever held waiting
/// for one. Handlers that do run inline should still return promptly: blocking
/// a callback thread starves new connections.
///
/// All handlers must be non-blocking and return promptly.
/// Non overriding handlers will return `Status::NotImplemented` by default.
///
/// Threading: the transport has no thread or executor of its own. It chains
/// its continuation on every future you return (a handler's result,
/// AsyncFlightDataStream::GetSchemaPayloadAsync()/NextAsync(),
/// AsyncFlightListing::NextAsync(), AsyncResultStream::NextAsync(), and an
/// AsyncFlightDataListener's OnDescriptor()/OnNext()/OnFinish()) with the
/// default CallbackOptions, so the continuation runs on whichever thread
/// completes your future, or inline on the gRPC callback thread that called
/// you if the future is already complete when you return it. That
/// continuation starts the RPC's next operation: the next StartWrite() (a
/// payload, a listing entry, an action result, a unary response), the next
/// StartRead(), or Finish(). Complete your futures on the executor you want
/// the RPC's I/O issued from.
///
/// Every request argument a handler receives (`Ticket`, `FlightDescriptor`,
/// `Criteria`, `Action`) is only valid for the duration of that handler call:
/// copy what you need before completing the returned future from another
/// thread. The `ServerCallContext` handed to a handler is likewise the call's
/// own context and must not outlive it.
///
/// Authentication goes through Handshake()/ValidateToken(): a blocking
/// ServerAuthHandler in FlightServerOptions is not supported, and Init()
/// rejects it.
class ARROW_FLIGHT_EXPORT AsyncGenericFlightServerBase {
 public:
  AsyncGenericFlightServerBase();
  virtual ~AsyncGenericFlightServerBase();

  /// \brief Initialize the server listening at the given location.
  /// Must be called before any other method.
  /// \param options The server options to use for initialization.
  /// \return Status indicating success or failure of the initialization.
  Status Init(const FlightServerOptions& options);

  /// \brief Get the port that the Flight server is listening on.
  ///
  /// This method must only be called after Init(). Will return a non-positive
  /// value if no port exists (e.g. when listening on a domain socket).
  /// \return The port number the server is listening on, or a non-positive value.
  int port() const;

  /// \return The address the server is listening on.
  /// \warning Only valid after Init() has been called.
  Location location() const;

  /// \brief Set the server to stop on any of the given signal numbers.
  /// \warning Must be called before Serve().
  /// \param signals The signal numbers on which the server should shut down.
  /// \return Status indicating success or failure of setting the shutdown signals.
  Status SetShutdownOnSignals(const std::vector<int> signals);

  /// \brief Start serving. Blocks until the server shuts down, which happens
  /// when Shutdown() is called or a registered signal arrives.
  /// \return Status indicating success or failure of starting the server.
  Status Serve();

  /// \return The signal number that interrupted Serve(), or 0.
  /// \note Only valid after Serve() has returned.
  int GotSignal() const;

  /// \brief Shut down the server, blocking until in-flight RPCs finish.
  /// May be called from another thread or a signal handler while Serve()
  /// blocks, with an optional deadline.
  ///
  /// An RPC whose handler never completes its future is not finished, so this
  /// blocks until that future resolves: a handler that abandons its future
  /// (see the per-handler notes on the transport not waiting for it) needs a
  /// deadline here, or the future must be given up on before shutdown.
  /// \param[in] deadline is an optional time point by which the server should complete
  /// shutdown.
  /// \return Status indicating success or failure of the shutdown operation.
  /// \warning Call at most once.
  Status Shutdown(const std::chrono::system_clock::time_point* deadline = NULLPTR);

  /// \brief Block until the server shuts down. Does not respond to signals.
  ///
  /// Like Shutdown(), this does not return while a handler future on a live RPC
  /// is still unresolved.
  Status Wait();

  /// \brief Run `callback` once if the RPC behind `context` is cancelled: the
  /// push counterpart of ServerCallContext::is_cancelled(), to stop work a
  /// handler started instead of polling.
  ///
  /// The callback runs when the RPC is cancelled (by the client, a deadline, a
  /// network error, etc.), on the transport thread that observes it and after
  /// the transport has settled the RPC's status; or before this call returns,
  /// on the calling thread, if the RPC is already cancelled. It never runs if
  /// the RPC ends without being cancelled. It must not block.
  ///
  /// Call this while the RPC is live: during a handler call, or while a future
  /// the transport awaits from that handler is still pending.
  /// \param[in] context The context an AsyncGenericFlightServerBase handed to the
  ///   handler.
  /// \param[in] callback The function to run on cancellation.
  /// \return OK once the callback is registered (or has run); Invalid when
  ///   `context` comes from anywhere else.
  static Status AddCancelCallback(const ServerCallContext& context,
                                  std::function<void()> callback);

  /// \brief Asynchronously retrieve a list of available fields given an
  /// optional opaque criteria.
  /// \param[in] context is the server call context.
  /// \param[in] criteria is the optional opaque criteria for listing flights.
  /// \see FlightServerBase::ListFlights
  ///
  /// \return A future completed with the listing, or with a non-OK status to
  /// fail the RPC.
  /// A future completed with a nullptr listing serves an empty listing.
  /// The transport pulls the listing one NextAsync() at a time, the criteria pointer is
  /// never null and is only valid for the duration of the call.
  virtual arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
      const ServerCallContext& context, const Criteria* criteria);

  /// \brief Asynchronously retrieve the schema and an access plan for the
  /// indicated descriptor.
  /// \see FlightServerBase::GetFlightInfo
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the info.
  ///
  /// \return A future completed with the info, or with a non-OK status to
  /// fail the RPC. A `nullptr` info answers `KeyError("Flight not found")`, as
  /// a synchronous server that returns OK without setting the response does.
  virtual arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Asynchronously retrieve the current status of the target query.
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the status.
  /// \see FlightServerBase::PollFlightInfo.
  /// \return A future completed with the info, or with a non-OK status to
  /// fail the RPC. A nullptr info answers `KeyError("Flight not found")`.
  virtual arrow::Future<std::shared_ptr<PollInfo>> PollFlightInfoAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Asynchronously retrieve the schema for the indicated descriptor.
  /// \param[in] context is the server call context.
  /// \param[in] request is the flight descriptor for which to retrieve the schema.
  /// \see FlightServerBase::GetSchema.
  /// \return A future completed with the schema, or with a non-OK status to
  /// fail the RPC.
  /// A `nullptr` schema answers `KeyError("Flight not found")`.
  virtual arrow::Future<std::shared_ptr<SchemaResult>> GetSchemaAsync(
      const ServerCallContext& context, const FlightDescriptor& request);

  /// \brief Asynchronously execute an action, returning a stream of zero or
  /// more results.
  /// \param[in] context is the server call context.
  /// \param[in] action is the action to execute.
  /// \see FlightServerBase::DoAction.
  /// \return A future completed with the result stream, or with a non-OK
  /// status to fail the RPC. A future completed with a `nullptr` stream
  /// answers CANCELLED, as a synchronous server that returns OK without a
  /// stream does.
  /// The transport pulls the stream one NextAsync() at a time.
  virtual arrow::Future<std::shared_ptr<AsyncResultStream>> DoActionAsync(
      const ServerCallContext& context, const Action& action);

  /// \brief Asynchronously retrieve the list of available actions.
  /// \param[in] context is the server call context.
  /// \see FlightServerBase::ListActions.
  virtual arrow::Future<std::vector<ActionType>> ListActionsAsync(
      const ServerCallContext& context);

  /// \brief Asynchronously process a bidirectional stream of IPC payloads.
  /// \see FlightServerBase::DoExchange.
  /// \param[in] context is the server call context.
  /// \param[in] reader is the asynchronous reader for the client's messages.
  /// \param[in] writer is the asynchronous writer for the server's messages.
  /// \return A future completed when the exchange is over, its status is the
  /// RPC's status.
  /// While it runs, consume the client's messages through the reader and send the
  /// server's through the writer. Both must not be used after the future completes.
  virtual arrow::Future<> DoExchangeAsync(
      const ServerCallContext& context, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer);

  /// \brief The stream serving one DoGet RPC.
  ///
  /// Called after the ticket is parsed, the request is only valid for the
  /// duration of the call.
  /// \param[in] context is the server call context.
  /// \param[in] request is an opaque ticket.
  /// \return A future completed with the stream to serve, or with a non-OK
  /// status to fail the RPC.
  /// A future completed with a `nullptr` stream answers `KeyError("No data in this
  /// flight")`. Complete the future from your ownthread or pool: the transport writes
  /// whatever stream it resolves to, and must not be blocked waiting for it.
  ///
  /// If the RPC is cancelled while the returned future is pending, the
  /// transport finishes the RPC without waiting for it. The stream the future
  /// eventually resolves to is never read from, but still receives Close():
  /// on the thread that completes the future, or on the transport's thread when
  /// that completion races the cancellation. To stop the work behind a pending
  /// future, register a callback with AddCancelCallback().
  virtual arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext& context, const Ticket& request);

  /// \brief Create the AsyncFlightDataListener serving one DoPut RPC.
  /// \see AsyncFlightDataListener
  /// \param[in] context is the server call context.
  /// \return A shared pointer to the AsyncFlightDataListener handling the upload, or
  /// `nullptr` to refuse the upload.
  virtual std::shared_ptr<AsyncFlightDataListener> CreateDoPutListener(
      const ServerCallContext& context);

  /// \brief Handle the handshake protocol with the client.
  /// \param[in] context is the server call context.
  /// \param[in] request is the client's handshake message.
  /// \param[out] response is where the server writes its handshake response.
  /// \return Status indicating success or failure of the handshake.
  virtual Status Handshake(const ServerCallContext& context, const std::string& request,
                           std::string* response);

  /// \brief Validate the token sent in the `auth-token-bin` header of RPCs
  /// issued after a successful Handshake().
  /// \param[in] context is the server call context.
  /// \param[in] token is the authentication token sent by the client.
  /// \param[out] peer_identity is set to the authenticated identity on success.
  /// \return Status indicating success or failure of the token validation.
  virtual Status ValidateToken(const ServerCallContext& context, const std::string& token,
                               std::string* peer_identity);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace arrow::flight
