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

#include "arrow/flight/client.h"

// Platform-specific defines
#include "arrow/flight/platform.h"

#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
#include <grpcpp/security/tls_credentials_options.h>
#endif
#else
#include <grpc++/grpc++.h>
#endif

#include <grpc/grpc_security_constants.h>

#include "arrow/buffer.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

#include "arrow/flight/client_auth.h"
#include "arrow/flight/client_header_internal.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/middleware.h"
#include "arrow/flight/middleware_internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/types.h"

namespace arrow {

namespace flight {

namespace pb = arrow::flight::protocol;

const char* kWriteSizeDetailTypeId = "flight::FlightWriteSizeStatusDetail";

FlightCallOptions::FlightCallOptions()
    : timeout(-1),
      read_options(ipc::IpcReadOptions::Defaults()),
      write_options(ipc::IpcWriteOptions::Defaults()) {}

const char* FlightWriteSizeStatusDetail::type_id() const {
  return kWriteSizeDetailTypeId;
}

std::string FlightWriteSizeStatusDetail::ToString() const {
  std::stringstream ss;
  ss << "IPC payload size (" << actual_ << " bytes) exceeded soft limit (" << limit_
     << " bytes)";
  return ss.str();
}

std::shared_ptr<FlightWriteSizeStatusDetail> FlightWriteSizeStatusDetail::UnwrapStatus(
    const arrow::Status& status) {
  if (!status.detail() || status.detail()->type_id() != kWriteSizeDetailTypeId) {
    return nullptr;
  }
  return std::dynamic_pointer_cast<FlightWriteSizeStatusDetail>(status.detail());
}

FlightClientOptions FlightClientOptions::Defaults() { return FlightClientOptions(); }

Status FlightStreamReader::ReadAll(std::shared_ptr<Table>* table,
                                   const StopToken& stop_token) {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  RETURN_NOT_OK(ReadAll(&batches, stop_token));
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches)).Value(table);
}

struct ClientRpc {
  grpc::ClientContext context;

  explicit ClientRpc(const FlightCallOptions& options) {
    if (options.timeout.count() >= 0) {
      std::chrono::system_clock::time_point deadline =
          std::chrono::time_point_cast<std::chrono::system_clock::time_point::duration>(
              std::chrono::system_clock::now() + options.timeout);
      context.set_deadline(deadline);
    }
    for (auto header : options.headers) {
      context.AddMetadata(header.first, header.second);
    }
  }

  /// \brief Add an auth token via an auth handler
  Status SetToken(ClientAuthHandler* auth_handler) {
    if (auth_handler) {
      std::string token;
      RETURN_NOT_OK(auth_handler->GetToken(&token));
      context.AddMetadata(internal::kGrpcAuthHeader, token);
    }
    return Status::OK();
  }
};

/// Helper that manages Finish() of a gRPC stream.
///
/// When we encounter an error (e.g. could not decode an IPC message),
/// we want to provide both the client-side error context and any
/// available server-side context. This helper helps wrap up that
/// logic.
///
/// This class protects the stream with a flag (so that Finish is
/// idempotent), and drains the read side (so that Finish won't hang).
///
/// The template lets us abstract between DoGet/DoExchange and DoPut,
/// which respectively read internal::FlightData and pb::PutResult.
template <typename Stream, typename ReadT>
class FinishableStream {
 public:
  FinishableStream(std::shared_ptr<ClientRpc> rpc, std::shared_ptr<Stream> stream)
      : rpc_(rpc), stream_(stream), finished_(false), server_status_() {}
  virtual ~FinishableStream() = default;

  /// \brief Get the underlying stream.
  std::shared_ptr<Stream> stream() const { return stream_; }

  /// \brief Finish the call, adding server context to the given status.
  virtual Status Finish(Status st) {
    if (finished_) {
      return MergeStatus(std::move(st));
    }

    // Drain the read side, as otherwise gRPC Finish() will hang. We
    // only call Finish() when the client closes the writer or the
    // reader finishes, so it's OK to assume the client no longer
    // wants to read and drain the read side. (If the client wants to
    // indicate that it is done writing, but not done reading, it
    // should use DoneWriting.
    ReadT message;
    while (internal::ReadPayload(stream_.get(), &message)) {
      // Drain the read side to avoid gRPC hanging in Finish()
    }

    server_status_ = internal::FromGrpcStatus(stream_->Finish(), &rpc_->context);
    finished_ = true;

    return MergeStatus(std::move(st));
  }

 private:
  Status MergeStatus(Status&& st) {
    if (server_status_.ok()) {
      return std::move(st);
    }
    return Status::FromDetailAndArgs(
        server_status_.code(), server_status_.detail(), server_status_.message(),
        ". Client context: ", st.ToString(),
        ". gRPC client debug context: ", rpc_->context.debug_error_string());
  }

  std::shared_ptr<ClientRpc> rpc_;
  std::shared_ptr<Stream> stream_;
  bool finished_;
  Status server_status_;
};

/// Helper that manages \a Finish() of a read-write gRPC stream.
///
/// This also calls \a WritesDone() and protects itself with a mutex
/// to enable sharing between the reader and writer.
template <typename Stream, typename ReadT>
class FinishableWritableStream : public FinishableStream<Stream, ReadT> {
 public:
  FinishableWritableStream(std::shared_ptr<ClientRpc> rpc,
                           std::shared_ptr<std::mutex> read_mutex,
                           std::shared_ptr<Stream> stream)
      : FinishableStream<Stream, ReadT>(rpc, stream),
        finish_mutex_(),
        read_mutex_(read_mutex),
        done_writing_(false) {}
  virtual ~FinishableWritableStream() = default;

  /// \brief Indicate to gRPC that the write half of the stream is done.
  Status DoneWriting() {
    // This is only used by the writer side of a stream, so it need
    // not be protected with a lock.
    if (done_writing_) {
      return Status::OK();
    }
    done_writing_ = true;
    if (!this->stream()->WritesDone()) {
      // Error happened, try to close the stream to get more detailed info
      return Finish(MakeFlightError(FlightStatusCode::Internal,
                                    "Could not flush pending record batches"));
    }
    return Status::OK();
  }

  Status Finish(Status st) override {
    // This may be used concurrently by reader/writer side of a
    // stream, so it needs to be protected.
    std::lock_guard<std::mutex> guard(finish_mutex_);

    // Now that we're shared between a reader and writer, we need to
    // protect ourselves from being called while there's an
    // outstanding read.
    std::unique_lock<std::mutex> read_guard(*read_mutex_, std::try_to_lock);
    if (!read_guard.owns_lock()) {
      return MakeFlightError(
          FlightStatusCode::Internal,
          "Cannot close stream with pending read operation. Client context: " +
              st.ToString());
    }

    // Try to flush pending writes. Don't use our WritesDone() to
    // avoid recursion.
    bool finished_writes = done_writing_ || this->stream()->WritesDone();
    done_writing_ = true;

    st = FinishableStream<Stream, ReadT>::Finish(std::move(st));

    if (!finished_writes) {
      return Status::FromDetailAndArgs(
          st.code(), st.detail(), st.message(),
          ". Additionally, could not finish writing record batches before closing");
    }
    return st;
  }

 private:
  std::mutex finish_mutex_;
  std::shared_ptr<std::mutex> read_mutex_;
  bool done_writing_;
};

class GrpcAddCallHeaders : public AddCallHeaders {
 public:
  explicit GrpcAddCallHeaders(std::multimap<grpc::string, grpc::string>* metadata)
      : metadata_(metadata) {}
  ~GrpcAddCallHeaders() override = default;

  void AddHeader(const std::string& key, const std::string& value) override {
    metadata_->insert(std::make_pair(key, value));
  }

 private:
  std::multimap<grpc::string, grpc::string>* metadata_;
};

class GrpcClientInterceptorAdapter : public grpc::experimental::Interceptor {
 public:
  explicit GrpcClientInterceptorAdapter(
      std::vector<std::unique_ptr<ClientMiddleware>> middleware)
      : middleware_(std::move(middleware)), received_headers_(false) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods* methods) {
    using InterceptionHookPoints = grpc::experimental::InterceptionHookPoints;
    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
      GrpcAddCallHeaders add_headers(methods->GetSendInitialMetadata());
      for (const auto& middleware : middleware_) {
        middleware->SendingHeaders(&add_headers);
      }
    }

    if (methods->QueryInterceptionHookPoint(
            InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
      if (!methods->GetRecvInitialMetadata()->empty()) {
        ReceivedHeaders(*methods->GetRecvInitialMetadata());
      }
    }

    if (methods->QueryInterceptionHookPoint(InterceptionHookPoints::POST_RECV_STATUS)) {
      DCHECK_NE(nullptr, methods->GetRecvStatus());
      DCHECK_NE(nullptr, methods->GetRecvTrailingMetadata());
      ReceivedHeaders(*methods->GetRecvTrailingMetadata());
      const Status status = internal::FromGrpcStatus(*methods->GetRecvStatus());
      for (const auto& middleware : middleware_) {
        middleware->CallCompleted(status);
      }
    }

    methods->Proceed();
  }

 private:
  void ReceivedHeaders(
      const std::multimap<grpc::string_ref, grpc::string_ref>& metadata) {
    if (received_headers_) {
      return;
    }
    received_headers_ = true;
    CallHeaders headers;
    for (const auto& entry : metadata) {
      headers.insert({util::string_view(entry.first.data(), entry.first.length()),
                      util::string_view(entry.second.data(), entry.second.length())});
    }
    for (const auto& middleware : middleware_) {
      middleware->ReceivedHeaders(headers);
    }
  }

  std::vector<std::unique_ptr<ClientMiddleware>> middleware_;
  // When communicating with a gRPC-Java server, the server may not
  // send back headers if the call fails right away. Instead, the
  // headers will be consolidated into the trailers. We don't want to
  // call the client middleware callback twice, so instead track
  // whether we saw headers - if not, then we need to check trailers.
  bool received_headers_;
};

class GrpcClientInterceptorAdapterFactory
    : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  GrpcClientInterceptorAdapterFactory(
      std::vector<std::shared_ptr<ClientMiddlewareFactory>> middleware)
      : middleware_(middleware) {}

  grpc::experimental::Interceptor* CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo* info) override {
    std::vector<std::unique_ptr<ClientMiddleware>> middleware;

    FlightMethod flight_method = FlightMethod::Invalid;
    util::string_view method(info->method());
    if (method.ends_with("/Handshake")) {
      flight_method = FlightMethod::Handshake;
    } else if (method.ends_with("/ListFlights")) {
      flight_method = FlightMethod::ListFlights;
    } else if (method.ends_with("/GetFlightInfo")) {
      flight_method = FlightMethod::GetFlightInfo;
    } else if (method.ends_with("/GetSchema")) {
      flight_method = FlightMethod::GetSchema;
    } else if (method.ends_with("/DoGet")) {
      flight_method = FlightMethod::DoGet;
    } else if (method.ends_with("/DoPut")) {
      flight_method = FlightMethod::DoPut;
    } else if (method.ends_with("/DoExchange")) {
      flight_method = FlightMethod::DoExchange;
    } else if (method.ends_with("/DoAction")) {
      flight_method = FlightMethod::DoAction;
    } else if (method.ends_with("/ListActions")) {
      flight_method = FlightMethod::ListActions;
    } else {
      DCHECK(false) << "Unknown Flight method: " << info->method();
    }

    const CallInfo flight_info{flight_method};
    for (auto& factory : middleware_) {
      std::unique_ptr<ClientMiddleware> instance;
      factory->StartCall(flight_info, &instance);
      if (instance) {
        middleware.push_back(std::move(instance));
      }
    }
    return new GrpcClientInterceptorAdapter(std::move(middleware));
  }

 private:
  std::vector<std::shared_ptr<ClientMiddlewareFactory>> middleware_;
};

class GrpcClientAuthSender : public ClientAuthSender {
 public:
  explicit GrpcClientAuthSender(
      std::shared_ptr<
          grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
          stream)
      : stream_(stream) {}

  Status Write(const std::string& token) override {
    pb::HandshakeRequest response;
    response.set_payload(token);
    if (stream_->Write(response)) {
      return Status::OK();
    }
    return internal::FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
      stream_;
};

class GrpcClientAuthReader : public ClientAuthReader {
 public:
  explicit GrpcClientAuthReader(
      std::shared_ptr<
          grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
          stream)
      : stream_(stream) {}

  Status Read(std::string* token) override {
    pb::HandshakeResponse request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.mutable_payload());
      return Status::OK();
    }
    return internal::FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
      stream_;
};

// An ipc::MessageReader that adapts any readable gRPC stream
// returning FlightData.
template <typename Reader>
class GrpcIpcMessageReader : public ipc::MessageReader {
 public:
  GrpcIpcMessageReader(
      std::shared_ptr<ClientRpc> rpc, std::shared_ptr<MemoryManager> memory_manager,
      std::shared_ptr<std::mutex> read_mutex,
      std::shared_ptr<FinishableStream<Reader, internal::FlightData>> stream,
      std::shared_ptr<internal::PeekableFlightDataReader<std::shared_ptr<Reader>>>
          peekable_reader,
      std::shared_ptr<Buffer>* app_metadata)
      : rpc_(rpc),
        memory_manager_(std::move(memory_manager)),
        read_mutex_(read_mutex),
        stream_(std::move(stream)),
        peekable_reader_(peekable_reader),
        app_metadata_(app_metadata),
        stream_finished_(false) {}

  ::arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (stream_finished_) {
      return nullptr;
    }
    internal::FlightData* data;
    {
      auto guard = read_mutex_ ? std::unique_lock<std::mutex>(*read_mutex_)
                               : std::unique_lock<std::mutex>();
      peekable_reader_->Next(&data);
    }
    if (!data) {
      stream_finished_ = true;
      return stream_->Finish(Status::OK());
    }

    if (ARROW_PREDICT_FALSE(!memory_manager_->is_cpu() && data->body)) {
      ARROW_ASSIGN_OR_RAISE(data->body, Buffer::ViewOrCopy(data->body, memory_manager_));
    }

    // Validate IPC message
    auto result = data->OpenMessage();
    if (!result.ok()) {
      return stream_->Finish(std::move(result).status());
    }
    *app_metadata_ = std::move(data->app_metadata);
    return result;
  }

 private:
  // The RPC context lifetime must be coupled to the ClientReader
  std::shared_ptr<ClientRpc> rpc_;
  std::shared_ptr<MemoryManager> memory_manager_;
  // Guard reads with a mutex to prevent concurrent reads if the write
  // side calls Finish(). Nullable as DoGet doesn't need this.
  std::shared_ptr<std::mutex> read_mutex_;
  std::shared_ptr<FinishableStream<Reader, internal::FlightData>> stream_;
  std::shared_ptr<internal::PeekableFlightDataReader<std::shared_ptr<Reader>>>
      peekable_reader_;
  // A reference to GrpcStreamReader.app_metadata_. That class
  // can't access the app metadata because when it Peek()s the stream,
  // it may be looking at a dictionary batch, not the record
  // batch. Updating it here ensures the reader is always updated with
  // the last metadata message read.
  std::shared_ptr<Buffer>* app_metadata_;
  bool stream_finished_;
};

/// The implementation of the public-facing API for reading from a
/// FlightData stream
template <typename Reader>
class GrpcStreamReader : public FlightStreamReader {
 public:
  GrpcStreamReader(std::shared_ptr<ClientRpc> rpc,
                   std::shared_ptr<MemoryManager> memory_manager,
                   std::shared_ptr<std::mutex> read_mutex,
                   const ipc::IpcReadOptions& options, StopToken stop_token,
                   std::shared_ptr<FinishableStream<Reader, internal::FlightData>> stream)
      : rpc_(rpc),
        memory_manager_(memory_manager ? std::move(memory_manager)
                                       : CPUDevice::Instance()->default_memory_manager()),
        read_mutex_(read_mutex),
        options_(options),
        stop_token_(std::move(stop_token)),
        stream_(stream),
        peekable_reader_(new internal::PeekableFlightDataReader<std::shared_ptr<Reader>>(
            stream->stream())),
        app_metadata_(nullptr) {}

  Status EnsureDataStarted() {
    if (!batch_reader_) {
      bool skipped_to_data = false;
      {
        auto guard = TakeGuard();
        skipped_to_data = peekable_reader_->SkipToData();
      }
      // peek() until we find the first data message; discard metadata
      if (!skipped_to_data) {
        return OverrideWithServerError(MakeFlightError(
            FlightStatusCode::Internal, "Server never sent a data message"));
      }

      auto message_reader = std::unique_ptr<ipc::MessageReader>(
          new GrpcIpcMessageReader<Reader>(rpc_, memory_manager_, read_mutex_, stream_,
                                           peekable_reader_, &app_metadata_));
      auto result =
          ipc::RecordBatchStreamReader::Open(std::move(message_reader), options_);
      RETURN_NOT_OK(OverrideWithServerError(std::move(result).Value(&batch_reader_)));
    }
    return Status::OK();
  }
  arrow::Result<std::shared_ptr<Schema>> GetSchema() override {
    RETURN_NOT_OK(EnsureDataStarted());
    return batch_reader_->schema();
  }
  Status Next(FlightStreamChunk* out) override {
    internal::FlightData* data;
    {
      auto guard = TakeGuard();
      peekable_reader_->Peek(&data);
    }
    if (!data) {
      out->app_metadata = nullptr;
      out->data = nullptr;
      return stream_->Finish(Status::OK());
    }

    if (!data->metadata) {
      // Metadata-only (data->metadata is the IPC header)
      out->app_metadata = data->app_metadata;
      out->data = nullptr;
      {
        auto guard = TakeGuard();
        peekable_reader_->Next(&data);
      }
      return Status::OK();
    }

    if (!batch_reader_) {
      RETURN_NOT_OK(EnsureDataStarted());
      // Re-peek here since EnsureDataStarted() advances the stream
      return Next(out);
    }
    RETURN_NOT_OK(batch_reader_->ReadNext(&out->data));
    out->app_metadata = std::move(app_metadata_);
    return Status::OK();
  }
  Status ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches) override {
    return ReadAll(batches, stop_token_);
  }
  Status ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches,
                 const StopToken& stop_token) override {
    FlightStreamChunk chunk;

    while (true) {
      if (stop_token.IsStopRequested()) {
        Cancel();
        return stop_token.Poll();
      }
      RETURN_NOT_OK(Next(&chunk));
      if (!chunk.data) break;
      batches->emplace_back(std::move(chunk.data));
    }
    return Status::OK();
  }
  Status ReadAll(std::shared_ptr<Table>* table) override {
    return ReadAll(table, stop_token_);
  }
  using FlightStreamReader::ReadAll;
  void Cancel() override { rpc_->context.TryCancel(); }

 private:
  std::unique_lock<std::mutex> TakeGuard() {
    return read_mutex_ ? std::unique_lock<std::mutex>(*read_mutex_)
                       : std::unique_lock<std::mutex>();
  }

  Status OverrideWithServerError(Status&& st) {
    if (st.ok()) {
      return std::move(st);
    }
    return stream_->Finish(std::move(st));
  }

  friend class GrpcIpcMessageReader<Reader>;
  std::shared_ptr<ClientRpc> rpc_;
  std::shared_ptr<MemoryManager> memory_manager_;
  // Guard reads with a lock to prevent Finish()/Close() from being
  // called on the writer while the reader has a pending
  // read. Nullable, as DoGet() doesn't need this.
  std::shared_ptr<std::mutex> read_mutex_;
  ipc::IpcReadOptions options_;
  StopToken stop_token_;
  std::shared_ptr<FinishableStream<Reader, internal::FlightData>> stream_;
  std::shared_ptr<internal::PeekableFlightDataReader<std::shared_ptr<Reader>>>
      peekable_reader_;
  std::shared_ptr<ipc::RecordBatchReader> batch_reader_;
  std::shared_ptr<Buffer> app_metadata_;
};

// The next two classes implement writing to a FlightData stream.
// Similarly to the read side, we want to reuse the implementation of
// RecordBatchWriter. As a result, these two classes are intertwined
// in order to pass application metadata "through" RecordBatchWriter.
// In order to get application-specific metadata to the
// IpcPayloadWriter, DoPutPayloadWriter takes a pointer to
// GrpcStreamWriter. GrpcStreamWriter updates a metadata field on
// write; DoPutPayloadWriter reads that metadata field to determine
// what to write.

template <typename ProtoReadT, typename FlightReadT>
class DoPutPayloadWriter;

template <typename ProtoReadT, typename FlightReadT>
class GrpcStreamWriter : public FlightStreamWriter {
 public:
  ~GrpcStreamWriter() override = default;

  using GrpcStream = grpc::ClientReaderWriter<pb::FlightData, ProtoReadT>;

  explicit GrpcStreamWriter(
      const FlightDescriptor& descriptor, std::shared_ptr<ClientRpc> rpc,
      int64_t write_size_limit_bytes, const ipc::IpcWriteOptions& options,
      std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer)
      : app_metadata_(nullptr),
        batch_writer_(nullptr),
        writer_(std::move(writer)),
        rpc_(std::move(rpc)),
        write_size_limit_bytes_(write_size_limit_bytes),
        options_(options),
        descriptor_(descriptor),
        writer_closed_(false) {}

  static Status Open(
      const FlightDescriptor& descriptor, std::shared_ptr<Schema> schema,
      const ipc::IpcWriteOptions& options, std::shared_ptr<ClientRpc> rpc,
      int64_t write_size_limit_bytes,
      std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer,
      std::unique_ptr<FlightStreamWriter>* out);

  Status CheckStarted() {
    if (!batch_writer_) {
      return Status::Invalid("Writer not initialized. Call Begin() with a schema.");
    }
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    if (batch_writer_) {
      return Status::Invalid("This writer has already been started.");
    }
    std::unique_ptr<ipc::internal::IpcPayloadWriter> payload_writer(
        new DoPutPayloadWriter<ProtoReadT, FlightReadT>(
            descriptor_, std::move(rpc_), write_size_limit_bytes_, writer_, this));
    // XXX: this does not actually write the message to the stream.
    // See Close().
    ARROW_ASSIGN_OR_RAISE(batch_writer_, ipc::internal::OpenRecordBatchWriter(
                                             std::move(payload_writer), schema, options));
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema) override {
    return Begin(schema, options_);
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    RETURN_NOT_OK(CheckStarted());
    return WriteWithMetadata(batch, nullptr);
  }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    FlightPayload payload{};
    payload.app_metadata = app_metadata;
    auto status = internal::WritePayload(payload, writer_->stream().get());
    if (status.IsIOError()) {
      return writer_->Finish(MakeFlightError(FlightStatusCode::Internal,
                                             "Could not write metadata to stream"));
    }
    return status;
  }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    RETURN_NOT_OK(CheckStarted());
    app_metadata_ = app_metadata;
    return batch_writer_->WriteRecordBatch(batch);
  }

  Status DoneWriting() override {
    // Do not CheckStarted - DoneWriting applies to data and metadata
    if (batch_writer_) {
      // Close the writer if we have one; this will force it to flush any
      // remaining data, before we close the write side of the stream.
      writer_closed_ = true;
      Status st = batch_writer_->Close();
      if (!st.ok()) {
        return writer_->Finish(std::move(st));
      }
    }
    return writer_->DoneWriting();
  }

  Status Close() override {
    // Do not CheckStarted - Close applies to data and metadata
    if (batch_writer_ && !writer_closed_) {
      // This is important! Close() calls
      // IpcPayloadWriter::CheckStarted() which will force the initial
      // schema message to be written to the stream. This is required
      // to unstick the server, else the client and the server end up
      // waiting for each other. This happens if the client never
      // wrote anything before calling Close().
      writer_closed_ = true;
      return writer_->Finish(batch_writer_->Close());
    }
    return writer_->Finish(Status::OK());
  }

  ipc::WriteStats stats() const override {
    ARROW_CHECK_NE(batch_writer_, nullptr);
    return batch_writer_->stats();
  }

 private:
  friend class DoPutPayloadWriter<ProtoReadT, FlightReadT>;
  std::shared_ptr<Buffer> app_metadata_;
  std::unique_ptr<ipc::RecordBatchWriter> batch_writer_;
  std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer_;

  // Fields used to lazy-initialize the IpcPayloadWriter. They're
  // invalid once Begin() is called.
  std::shared_ptr<ClientRpc> rpc_;
  int64_t write_size_limit_bytes_;
  ipc::IpcWriteOptions options_;
  FlightDescriptor descriptor_;
  bool writer_closed_;
};

/// A IpcPayloadWriter implementation that writes to a gRPC stream of
/// FlightData messages.
template <typename ProtoReadT, typename FlightReadT>
class DoPutPayloadWriter : public ipc::internal::IpcPayloadWriter {
 public:
  using GrpcStream = grpc::ClientReaderWriter<pb::FlightData, ProtoReadT>;

  DoPutPayloadWriter(
      const FlightDescriptor& descriptor, std::shared_ptr<ClientRpc> rpc,
      int64_t write_size_limit_bytes,
      std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer,
      GrpcStreamWriter<ProtoReadT, FlightReadT>* stream_writer)
      : descriptor_(descriptor),
        rpc_(rpc),
        write_size_limit_bytes_(write_size_limit_bytes),
        writer_(std::move(writer)),
        first_payload_(true),
        stream_writer_(stream_writer) {}

  ~DoPutPayloadWriter() override = default;

  Status Start() override { return Status::OK(); }

  Status WritePayload(const ipc::IpcPayload& ipc_payload) override {
    FlightPayload payload;
    payload.ipc_message = ipc_payload;

    if (first_payload_) {
      // First Flight message needs to encore the Flight descriptor
      if (ipc_payload.type != ipc::MessageType::SCHEMA) {
        return Status::Invalid("First IPC message should be schema");
      }
      // Write the descriptor to begin with
      RETURN_NOT_OK(internal::ToPayload(descriptor_, &payload.descriptor));
      first_payload_ = false;
    } else if (ipc_payload.type == ipc::MessageType::RECORD_BATCH &&
               stream_writer_->app_metadata_) {
      payload.app_metadata = std::move(stream_writer_->app_metadata_);
    }

    if (write_size_limit_bytes_ > 0) {
      // Check if the total size is greater than the user-configured
      // soft-limit.
      int64_t size = ipc_payload.body_length + ipc_payload.metadata->size();
      if (payload.descriptor) {
        size += payload.descriptor->size();
      }
      if (payload.app_metadata) {
        size += payload.app_metadata->size();
      }
      if (size > write_size_limit_bytes_) {
        return arrow::Status(
            arrow::StatusCode::Invalid, "IPC payload size exceeded soft limit",
            std::make_shared<FlightWriteSizeStatusDetail>(write_size_limit_bytes_, size));
      }
    }

    auto status = internal::WritePayload(payload, writer_->stream().get());
    if (status.IsIOError()) {
      return writer_->Finish(MakeFlightError(FlightStatusCode::Internal,
                                             "Could not write record batch to stream"));
    }
    return status;
  }

  Status Close() override {
    // Closing is handled one layer up in GrpcStreamWriter::Close
    return Status::OK();
  }

 protected:
  const FlightDescriptor descriptor_;
  std::shared_ptr<ClientRpc> rpc_;
  int64_t write_size_limit_bytes_;
  std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer_;
  bool first_payload_;
  GrpcStreamWriter<ProtoReadT, FlightReadT>* stream_writer_;
};

template <typename ProtoReadT, typename FlightReadT>
Status GrpcStreamWriter<ProtoReadT, FlightReadT>::Open(
    const FlightDescriptor& descriptor,
    std::shared_ptr<Schema> schema,  // this schema is nullable
    const ipc::IpcWriteOptions& options, std::shared_ptr<ClientRpc> rpc,
    int64_t write_size_limit_bytes,
    std::shared_ptr<FinishableWritableStream<GrpcStream, FlightReadT>> writer,
    std::unique_ptr<FlightStreamWriter>* out) {
  std::unique_ptr<GrpcStreamWriter<ProtoReadT, FlightReadT>> instance(
      new GrpcStreamWriter<ProtoReadT, FlightReadT>(
          descriptor, std::move(rpc), write_size_limit_bytes, options, writer));
  if (schema) {
    // The schema was provided (DoPut). Eagerly write the schema and
    // descriptor together as the first message.
    RETURN_NOT_OK(instance->Begin(schema, options));
  } else {
    // The schema was not provided (DoExchange). Eagerly write just
    // the descriptor as the first message. Note that if the client
    // calls Begin() to send data, we'll send a redundant descriptor.
    FlightPayload payload{};
    RETURN_NOT_OK(internal::ToPayload(descriptor, &payload.descriptor));
    auto status = internal::WritePayload(payload, instance->writer_->stream().get());
    if (status.IsIOError()) {
      return writer->Finish(MakeFlightError(FlightStatusCode::Internal,
                                            "Could not write descriptor to stream"));
    }
    RETURN_NOT_OK(status);
  }
  *out = std::move(instance);
  return Status::OK();
}

FlightMetadataReader::~FlightMetadataReader() = default;

class GrpcMetadataReader : public FlightMetadataReader {
 public:
  explicit GrpcMetadataReader(
      std::shared_ptr<grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>> reader,
      std::shared_ptr<std::mutex> read_mutex)
      : reader_(reader), read_mutex_(read_mutex) {}

  Status ReadMetadata(std::shared_ptr<Buffer>* out) override {
    std::lock_guard<std::mutex> guard(*read_mutex_);
    pb::PutResult message;
    if (reader_->Read(&message)) {
      *out = Buffer::FromString(std::move(*message.mutable_app_metadata()));
    } else {
      // Stream finished
      *out = nullptr;
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>> reader_;
  std::shared_ptr<std::mutex> read_mutex_;
};

namespace {
// Dummy self-signed certificate to be used because TlsCredentials
// requires root CA certs, even if you are skipping server
// verification.
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
constexpr char kDummyRootCert[] =
    "-----BEGIN CERTIFICATE-----\n"
    "MIICwzCCAaugAwIBAgIJAM12DOkcaqrhMA0GCSqGSIb3DQEBBQUAMBQxEjAQBgNV\n"
    "BAMTCWxvY2FsaG9zdDAeFw0yMDEwMDcwODIyNDFaFw0zMDEwMDUwODIyNDFaMBQx\n"
    "EjAQBgNVBAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
    "ggEBALjJ8KPEpF0P4GjMPrJhjIBHUL0AX9E4oWdgJRCSFkPKKEWzQabTQBikMOhI\n"
    "W4VvBMaHEBuECE5OEyrzDRiAO354I4F4JbBfxMOY8NIW0uWD6THWm2KkCzoZRIPW\n"
    "yZL6dN+mK6cEH+YvbNuy5ZQGNjGG43tyiXdOCAc4AI9POeTtjdMpbbpR2VY4Ad/E\n"
    "oTEiS3gNnN7WIAdgMhCJxjzvPwKszV3f7pwuTHzFMsuHLKr6JeaVUYfbi4DxxC8Z\n"
    "k6PF6dLlLf3ngTSLBJyaXP1BhKMvz0TaMK3F0y2OGwHM9J8np2zWjTlNVEzffQZx\n"
    "SWMOQManlJGs60xYx9KCPJMZZsMCAwEAAaMYMBYwFAYDVR0RBA0wC4IJbG9jYWxo\n"
    "b3N0MA0GCSqGSIb3DQEBBQUAA4IBAQC0LrmbcNKgO+D50d/wOc+vhi9K04EZh8bg\n"
    "WYAK1kLOT4eShbzqWGV/1EggY4muQ6ypSELCLuSsg88kVtFQIeRilA6bHFqQSj6t\n"
    "sqgh2cWsMwyllCtmX6Maf3CLb2ZdoJlqUwdiBdrbIbuyeAZj3QweCtLKGSQzGDyI\n"
    "KH7G8nC5d0IoRPiCMB6RnMMKsrhviuCdWbAFHop7Ff36JaOJ8iRa2sSf2OXE8j/5\n"
    "obCXCUvYHf4Zw27JcM2AnnQI9VJLnYxis83TysC5s2Z7t0OYNS9kFmtXQbUNlmpS\n"
    "doQ/Eu47vWX7S0TXeGziGtbAOKxbHE0BGGPDOAB/jGW/JVbeTiXY\n"
    "-----END CERTIFICATE-----\n";
#endif
}  // namespace
class FlightClient::FlightClientImpl {
 public:
  Status Connect(const Location& location, const FlightClientOptions& options) {
    const std::string& scheme = location.scheme();

    std::stringstream grpc_uri;
    std::shared_ptr<grpc::ChannelCredentials> creds;
    if (scheme == kSchemeGrpc || scheme == kSchemeGrpcTcp || scheme == kSchemeGrpcTls) {
      grpc_uri << arrow::internal::UriEncodeHost(location.uri_->host()) << ':'
               << location.uri_->port_text();

      if (scheme == kSchemeGrpcTls) {
        if (options.disable_server_verification) {
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          namespace ge = GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS;

#if defined(GRPC_USE_CERTIFICATE_VERIFIER)
          // gRPC >= 1.43
          class NoOpCertificateVerifier : public ge::ExternalCertificateVerifier {
           public:
            bool Verify(ge::TlsCustomVerificationCheckRequest*,
                        std::function<void(grpc::Status)>,
                        grpc::Status* sync_status) override {
              *sync_status = grpc::Status::OK;
              return true;  // Check done synchronously
            }
            void Cancel(ge::TlsCustomVerificationCheckRequest*) override {}
          };
          auto cert_verifier =
              ge::ExternalCertificateVerifier::Create<NoOpCertificateVerifier>();

#else   // defined(GRPC_USE_CERTIFICATE_VERIFIER)
        // gRPC < 1.43
        // A callback to supply to TlsCredentialsOptions that accepts any server
        // arguments.
          struct NoOpTlsAuthorizationCheck
              : public ge::TlsServerAuthorizationCheckInterface {
            int Schedule(ge::TlsServerAuthorizationCheckArg* arg) override {
              arg->set_success(1);
              arg->set_status(GRPC_STATUS_OK);
              return 0;
            }
          };
          auto server_authorization_check = std::make_shared<NoOpTlsAuthorizationCheck>();
          noop_auth_check_ = std::make_shared<ge::TlsServerAuthorizationCheckConfig>(
              server_authorization_check);
#endif  // defined(GRPC_USE_CERTIFICATE_VERIFIER)

#if defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          auto certificate_provider =
              std::make_shared<grpc::experimental::StaticDataCertificateProvider>(
                  kDummyRootCert);
#if defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
          grpc::experimental::TlsChannelCredentialsOptions tls_options(
              certificate_provider);
#else   // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
        // While gRPC >= 1.36 does not require a root cert (it has a default)
        // in practice the path it hardcodes is broken. See grpc/grpc#21655.
          grpc::experimental::TlsChannelCredentialsOptions tls_options;
          tls_options.set_certificate_provider(certificate_provider);
#endif  // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS_ROOT_CERTS)
          tls_options.watch_root_certs();
          tls_options.set_root_cert_name("dummy");
#if defined(GRPC_USE_CERTIFICATE_VERIFIER)
          tls_options.set_certificate_verifier(std::move(cert_verifier));
          tls_options.set_check_call_host(false);
          tls_options.set_verify_server_certs(false);
#else   // defined(GRPC_USE_CERTIFICATE_VERIFIER)
          tls_options.set_server_verification_option(
              grpc_tls_server_verification_option::GRPC_TLS_SKIP_ALL_SERVER_VERIFICATION);
          tls_options.set_server_authorization_check_config(noop_auth_check_);
#endif  // defined(GRPC_USE_CERTIFICATE_VERIFIER)
#elif defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          // continues defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          auto materials_config = std::make_shared<ge::TlsKeyMaterialsConfig>();
          materials_config->set_pem_root_certs(kDummyRootCert);
          ge::TlsCredentialsOptions tls_options(
              GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE,
              GRPC_TLS_SKIP_ALL_SERVER_VERIFICATION, materials_config,
              std::shared_ptr<ge::TlsCredentialReloadConfig>(), noop_auth_check_);
#endif  // defined(GRPC_USE_TLS_CHANNEL_CREDENTIALS_OPTIONS)
          creds = ge::TlsCredentials(tls_options);
#else   // defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
          return Status::NotImplemented(
              "Using encryption with server verification disabled is unsupported. "
              "Please use a release of Arrow Flight built with gRPC 1.27 or higher.");
#endif  // defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
        } else {
          grpc::SslCredentialsOptions ssl_options;
          if (!options.tls_root_certs.empty()) {
            ssl_options.pem_root_certs = options.tls_root_certs;
          }
          if (!options.cert_chain.empty()) {
            ssl_options.pem_cert_chain = options.cert_chain;
          }
          if (!options.private_key.empty()) {
            ssl_options.pem_private_key = options.private_key;
          }
          creds = grpc::SslCredentials(ssl_options);
        }
      } else {
        creds = grpc::InsecureChannelCredentials();
      }
    } else if (scheme == kSchemeGrpcUnix) {
      grpc_uri << "unix://" << location.uri_->path();
      creds = grpc::InsecureChannelCredentials();
    } else {
      return Status::NotImplemented("Flight scheme " + scheme + " is not supported.");
    }

    grpc::ChannelArguments args;
    // We can't set the same config value twice, so for values where
    // we want to set defaults, keep them in a map and update them;
    // then update them all at once
    std::unordered_map<std::string, int> default_args;
    // Try to reconnect quickly at first, in case the server is still starting up
    default_args[GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS] = 100;
    // Receive messages of any size
    default_args[GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH] = -1;
    // Setting this arg enables each client to open it's own TCP connection to server,
    // not sharing one single connection, which becomes bottleneck under high load.
    default_args[GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL] = 1;

    if (options.override_hostname != "") {
      args.SetSslTargetNameOverride(options.override_hostname);
    }

    // Allow setting generic gRPC options.
    for (const auto& arg : options.generic_options) {
      if (util::holds_alternative<int>(arg.second)) {
        default_args[arg.first] = util::get<int>(arg.second);
      } else if (util::holds_alternative<std::string>(arg.second)) {
        args.SetString(arg.first, util::get<std::string>(arg.second));
      }
      // Otherwise unimplemented
    }
    for (const auto& pair : default_args) {
      args.SetInt(pair.first, pair.second);
    }

    std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
        interceptors;
    interceptors.emplace_back(
        new GrpcClientInterceptorAdapterFactory(std::move(options.middleware)));

    stub_ = pb::FlightService::NewStub(
        grpc::experimental::CreateCustomChannelWithInterceptors(
            grpc_uri.str(), creds, args, std::move(interceptors)));

    write_size_limit_bytes_ = options.write_size_limit_bytes;
    return Status::OK();
  }

  Status Authenticate(const FlightCallOptions& options,
                      std::unique_ptr<ClientAuthHandler> auth_handler) {
    auth_handler_ = std::move(auth_handler);
    ClientRpc rpc(options);
    std::shared_ptr<grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
        stream = stub_->Handshake(&rpc.context);
    GrpcClientAuthSender outgoing{stream};
    GrpcClientAuthReader incoming{stream};
    RETURN_NOT_OK(auth_handler_->Authenticate(&outgoing, &incoming));
    // Explicitly close our side of the connection
    bool finished_writes = stream->WritesDone();
    RETURN_NOT_OK(internal::FromGrpcStatus(stream->Finish(), &rpc.context));
    if (!finished_writes) {
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not finish writing before closing");
    }
    return Status::OK();
  }

  arrow::Result<std::pair<std::string, std::string>> AuthenticateBasicToken(
      const FlightCallOptions& options, const std::string& username,
      const std::string& password) {
    // Add basic auth headers to outgoing headers.
    ClientRpc rpc(options);
    internal::AddBasicAuthHeaders(&rpc.context, username, password);

    std::shared_ptr<grpc::ClientReaderWriter<pb::HandshakeRequest, pb::HandshakeResponse>>
        stream = stub_->Handshake(&rpc.context);
    GrpcClientAuthSender outgoing{stream};
    GrpcClientAuthReader incoming{stream};

    // Explicitly close our side of the connection.
    bool finished_writes = stream->WritesDone();
    RETURN_NOT_OK(internal::FromGrpcStatus(stream->Finish(), &rpc.context));
    if (!finished_writes) {
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not finish writing before closing");
    }

    // Grab bearer token from incoming headers.
    return internal::GetBearerTokenHeader(rpc.context);
  }

  Status ListFlights(const FlightCallOptions& options, const Criteria& criteria,
                     std::unique_ptr<FlightListing>* listing) {
    pb::Criteria pb_criteria;
    RETURN_NOT_OK(internal::ToProto(criteria, &pb_criteria));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    std::unique_ptr<grpc::ClientReader<pb::FlightInfo>> stream(
        stub_->ListFlights(&rpc.context, pb_criteria));

    std::vector<FlightInfo> flights;

    pb::FlightInfo pb_info;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
      FlightInfo::Data info_data;
      RETURN_NOT_OK(internal::FromProto(pb_info, &info_data));
      flights.emplace_back(std::move(info_data));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
    RETURN_NOT_OK(options.stop_token.Poll());
    listing->reset(new SimpleFlightListing(std::move(flights)));
    return internal::FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) {
    pb::Action pb_action;
    RETURN_NOT_OK(internal::ToProto(action, &pb_action));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    std::unique_ptr<grpc::ClientReader<pb::Result>> stream(
        stub_->DoAction(&rpc.context, pb_action));

    pb::Result pb_result;

    std::vector<Result> materialized_results;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_result)) {
      Result result;
      RETURN_NOT_OK(internal::FromProto(pb_result, &result));
      materialized_results.emplace_back(std::move(result));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
    RETURN_NOT_OK(options.stop_token.Poll());

    *results = std::unique_ptr<ResultStream>(
        new SimpleResultStream(std::move(materialized_results)));
    return internal::FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  Status ListActions(const FlightCallOptions& options, std::vector<ActionType>* types) {
    pb::Empty empty;

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    std::unique_ptr<grpc::ClientReader<pb::ActionType>> stream(
        stub_->ListActions(&rpc.context, empty));

    pb::ActionType pb_type;
    ActionType type;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_type)) {
      RETURN_NOT_OK(internal::FromProto(pb_type, &type));
      types->emplace_back(std::move(type));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();
    RETURN_NOT_OK(options.stop_token.Poll());
    return internal::FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) {
    pb::FlightDescriptor pb_descriptor;
    pb::FlightInfo pb_response;

    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descriptor));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    Status s = internal::FromGrpcStatus(
        stub_->GetFlightInfo(&rpc.context, pb_descriptor, &pb_response), &rpc.context);
    RETURN_NOT_OK(s);

    FlightInfo::Data info_data;
    RETURN_NOT_OK(internal::FromProto(pb_response, &info_data));
    info->reset(new FlightInfo(std::move(info_data)));
    return Status::OK();
  }

  Status GetSchema(const FlightCallOptions& options, const FlightDescriptor& descriptor,
                   std::unique_ptr<SchemaResult>* schema_result) {
    pb::FlightDescriptor pb_descriptor;
    pb::SchemaResult pb_response;

    RETURN_NOT_OK(internal::ToProto(descriptor, &pb_descriptor));

    ClientRpc rpc(options);
    RETURN_NOT_OK(rpc.SetToken(auth_handler_.get()));
    Status s = internal::FromGrpcStatus(
        stub_->GetSchema(&rpc.context, pb_descriptor, &pb_response), &rpc.context);
    RETURN_NOT_OK(s);

    std::string str;
    RETURN_NOT_OK(internal::FromProto(pb_response, &str));
    schema_result->reset(new SchemaResult(str));
    return Status::OK();
  }

  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* out) {
    using StreamReader = GrpcStreamReader<grpc::ClientReader<pb::FlightData>>;
    pb::Ticket pb_ticket;
    internal::ToProto(ticket, &pb_ticket);

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<grpc::ClientReader<pb::FlightData>> stream =
        stub_->DoGet(&rpc->context, pb_ticket);
    auto finishable_stream = std::make_shared<
        FinishableStream<grpc::ClientReader<pb::FlightData>, internal::FlightData>>(
        rpc, stream);
    *out = std::unique_ptr<StreamReader>(
        new StreamReader(rpc, options.memory_manager, nullptr, options.read_options,
                         options.stop_token, finishable_stream));
    // Eagerly read the schema
    return static_cast<StreamReader*>(out->get())->EnsureDataStarted();
  }

  Status DoPut(const FlightCallOptions& options, const FlightDescriptor& descriptor,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>* out,
               std::unique_ptr<FlightMetadataReader>* reader) {
    using GrpcStream = grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>;
    using StreamWriter = GrpcStreamWriter<pb::PutResult, pb::PutResult>;

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<GrpcStream> stream = stub_->DoPut(&rpc->context);
    // The writer drains the reader on close to avoid hanging inside
    // gRPC. Concurrent reads are unsafe, so a mutex protects this operation.
    std::shared_ptr<std::mutex> read_mutex = std::make_shared<std::mutex>();
    auto finishable_stream =
        std::make_shared<FinishableWritableStream<GrpcStream, pb::PutResult>>(
            rpc, read_mutex, stream);
    *reader =
        std::unique_ptr<FlightMetadataReader>(new GrpcMetadataReader(stream, read_mutex));
    return StreamWriter::Open(descriptor, schema, options.write_options, rpc,
                              write_size_limit_bytes_, finishable_stream, out);
  }

  Status DoExchange(const FlightCallOptions& options, const FlightDescriptor& descriptor,
                    std::unique_ptr<FlightStreamWriter>* writer,
                    std::unique_ptr<FlightStreamReader>* reader) {
    using GrpcStream = grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>;
    using StreamReader = GrpcStreamReader<GrpcStream>;
    using StreamWriter = GrpcStreamWriter<pb::FlightData, internal::FlightData>;

    auto rpc = std::make_shared<ClientRpc>(options);
    RETURN_NOT_OK(rpc->SetToken(auth_handler_.get()));
    std::shared_ptr<grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>> stream =
        stub_->DoExchange(&rpc->context);
    // The writer drains the reader on close to avoid hanging inside
    // gRPC. Concurrent reads are unsafe, so a mutex protects this operation.
    std::shared_ptr<std::mutex> read_mutex = std::make_shared<std::mutex>();
    auto finishable_stream =
        std::make_shared<FinishableWritableStream<GrpcStream, internal::FlightData>>(
            rpc, read_mutex, stream);
    *reader = std::unique_ptr<StreamReader>(
        new StreamReader(rpc, options.memory_manager, read_mutex, options.read_options,
                         options.stop_token, finishable_stream));
    // Do not eagerly read the schema. There may be metadata messages
    // before any data is sent, or data may not be sent at all.
    return StreamWriter::Open(descriptor, nullptr, options.write_options, rpc,
                              write_size_limit_bytes_, finishable_stream, writer);
  }

 private:
  std::unique_ptr<pb::FlightService::Stub> stub_;
  std::shared_ptr<ClientAuthHandler> auth_handler_;
#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS) && \
    !defined(GRPC_USE_CERTIFICATE_VERIFIER)
  // Scope the TlsServerAuthorizationCheckConfig to be at the class instance level, since
  // it gets created during Connect() and needs to persist to DoAction() calls. gRPC does
  // not correctly increase the reference count of this object:
  // https://github.com/grpc/grpc/issues/22287
  std::shared_ptr<
      GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS::TlsServerAuthorizationCheckConfig>
      noop_auth_check_;
#endif
  int64_t write_size_limit_bytes_;
};

FlightClient::FlightClient() { impl_.reset(new FlightClientImpl); }

FlightClient::~FlightClient() {
  auto st = Close();
  if (!st.ok()) {
    ARROW_LOG(WARNING) << "FlightClient::~FlightClient(): Close() failed: "
                       << st.ToString();
  }
}

Status FlightClient::Connect(const Location& location,
                             std::unique_ptr<FlightClient>* client) {
  return Connect(location, FlightClientOptions::Defaults(), client);
}

Status FlightClient::Connect(const Location& location, const FlightClientOptions& options,
                             std::unique_ptr<FlightClient>* client) {
  client->reset(new FlightClient);
  return (*client)->impl_->Connect(location, options);
}

Status FlightClient::Authenticate(const FlightCallOptions& options,
                                  std::unique_ptr<ClientAuthHandler> auth_handler) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->Authenticate(options, std::move(auth_handler));
}

arrow::Result<std::pair<std::string, std::string>> FlightClient::AuthenticateBasicToken(
    const FlightCallOptions& options, const std::string& username,
    const std::string& password) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->AuthenticateBasicToken(options, username, password);
}

Status FlightClient::DoAction(const FlightCallOptions& options, const Action& action,
                              std::unique_ptr<ResultStream>* results) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->DoAction(options, action, results);
}

Status FlightClient::ListActions(const FlightCallOptions& options,
                                 std::vector<ActionType>* actions) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->ListActions(options, actions);
}

Status FlightClient::GetFlightInfo(const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor,
                                   std::unique_ptr<FlightInfo>* info) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->GetFlightInfo(options, descriptor, info);
}

Status FlightClient::GetSchema(const FlightCallOptions& options,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<SchemaResult>* schema_result) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->GetSchema(options, descriptor, schema_result);
}

Status FlightClient::ListFlights(std::unique_ptr<FlightListing>* listing) {
  RETURN_NOT_OK(CheckOpen());
  return ListFlights({}, {}, listing);
}

Status FlightClient::ListFlights(const FlightCallOptions& options,
                                 const Criteria& criteria,
                                 std::unique_ptr<FlightListing>* listing) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->ListFlights(options, criteria, listing);
}

Status FlightClient::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                           std::unique_ptr<FlightStreamReader>* stream) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->DoGet(options, ticket, stream);
}

Status FlightClient::DoPut(const FlightCallOptions& options,
                           const FlightDescriptor& descriptor,
                           const std::shared_ptr<Schema>& schema,
                           std::unique_ptr<FlightStreamWriter>* stream,
                           std::unique_ptr<FlightMetadataReader>* reader) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->DoPut(options, descriptor, schema, stream, reader);
}

Status FlightClient::DoExchange(const FlightCallOptions& options,
                                const FlightDescriptor& descriptor,
                                std::unique_ptr<FlightStreamWriter>* writer,
                                std::unique_ptr<FlightStreamReader>* reader) {
  RETURN_NOT_OK(CheckOpen());
  return impl_->DoExchange(options, descriptor, writer, reader);
}

Status FlightClient::Close() {
  // gRPC doesn't offer an explicit shutdown
  impl_.reset(nullptr);
  // TODO(ARROW-15473): if we track ongoing RPCs, we can cancel them first
  return Status::OK();
}

Status FlightClient::CheckOpen() const {
  if (!impl_) {
    return Status::Invalid("FlightClient is closed");
  }
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow
