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

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

#include "arrow/flight/client_auth.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/grpc_client.h"
#include "arrow/flight/types.h"

namespace arrow {

namespace flight {

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

arrow::Result<std::shared_ptr<Table>> FlightStreamReader::ToTable(
    const StopToken& stop_token) {
  ARROW_ASSIGN_OR_RAISE(auto batches, ToRecordBatches(stop_token));
  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  return Table::FromRecordBatches(schema, std::move(batches));
}

Status FlightStreamReader::ReadAll(std::vector<std::shared_ptr<RecordBatch>>* batches,
                                   const StopToken& stop_token) {
  return ToRecordBatches(stop_token).Value(batches);
}

Status FlightStreamReader::ReadAll(std::shared_ptr<Table>* table,
                                   const StopToken& stop_token) {
  return ToTable(stop_token).Value(table);
}

/// \brief An ipc::MessageReader adapting the Flight ClientDataStream interface.
///
/// In order to support app_metadata and reuse the existing IPC
/// infrastructure, this takes a pointer to a buffer (provided by the
/// FlightStreamReader implementation) and upon reading a message,
/// updates that buffer with the one read from the server.
class IpcMessageReader : public ipc::MessageReader {
 public:
  IpcMessageReader(std::shared_ptr<internal::ClientDataStream> stream,
                   std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader,
                   std::shared_ptr<MemoryManager> memory_manager,
                   std::shared_ptr<Buffer>* app_metadata)
      : stream_(std::move(stream)),
        peekable_reader_(peekable_reader),
        memory_manager_(memory_manager ? std::move(memory_manager)
                                       : CPUDevice::Instance()->default_memory_manager()),
        app_metadata_(app_metadata),
        stream_finished_(false) {}

  ::arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (stream_finished_) {
      return nullptr;
    }
    internal::FlightData* data;
    peekable_reader_->Next(&data);
    if (!data) {
      stream_finished_ = true;
      ARROW_RETURN_NOT_OK(stream_->Finish(Status::OK()));
      return nullptr;
    }
    if (data->body) {
      ARROW_ASSIGN_OR_RAISE(data->body, Buffer::ViewOrCopy(data->body, memory_manager_));
    }
    // Validate IPC message
    auto result = data->OpenMessage();
    if (!result.ok()) {
      stream_finished_ = true;
      ARROW_RETURN_NOT_OK(stream_->Finish(std::move(result).status()));
      return nullptr;
    }
    *app_metadata_ = std::move(data->app_metadata);
    return result;
  }

 private:
  std::shared_ptr<internal::ClientDataStream> stream_;
  std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader_;
  std::shared_ptr<MemoryManager> memory_manager_;
  // A reference to ClientStreamReader.app_metadata_. That class
  // can't access the app metadata because when it Peek()s the stream,
  // it may be looking at a dictionary batch, not the record
  // batch. Updating it here ensures the reader is always updated with
  // the last metadata message read.
  std::shared_ptr<Buffer>* app_metadata_;
  bool stream_finished_;
};

/// \brief A reader for any ClientDataStream.
class ClientStreamReader : public FlightStreamReader {
 public:
  ClientStreamReader(std::shared_ptr<internal::ClientDataStream> stream,
                     const ipc::IpcReadOptions& options, StopToken stop_token,
                     std::shared_ptr<MemoryManager> memory_manager)
      : stream_(std::move(stream)),
        options_(options),
        stop_token_(std::move(stop_token)),
        memory_manager_(std::move(memory_manager)),
        peekable_reader_(new internal::PeekableFlightDataReader(stream_.get())),
        app_metadata_(nullptr) {}

  Status EnsureDataStarted() {
    if (!batch_reader_) {
      bool skipped_to_data = false;
      skipped_to_data = peekable_reader_->SkipToData();
      // peek() until we find the first data message; discard metadata
      if (!skipped_to_data) {
        return OverrideWithServerError(MakeFlightError(
            FlightStatusCode::Internal, "Server never sent a data message"));
      }

      auto message_reader = std::unique_ptr<ipc::MessageReader>(new IpcMessageReader(
          stream_, peekable_reader_, memory_manager_, &app_metadata_));
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
  arrow::Result<FlightStreamChunk> Next() override {
    FlightStreamChunk out;
    internal::FlightData* data;
    peekable_reader_->Peek(&data);
    if (!data) {
      out.app_metadata = nullptr;
      out.data = nullptr;
      RETURN_NOT_OK(stream_->Finish(Status::OK()));
      return out;
    }

    if (!data->metadata) {
      // Metadata-only (data->metadata is the IPC header)
      out.app_metadata = data->app_metadata;
      out.data = nullptr;
      peekable_reader_->Next(&data);
      return out;
    }

    if (!batch_reader_) {
      RETURN_NOT_OK(EnsureDataStarted());
      // Re-peek here since EnsureDataStarted() advances the stream
      return Next();
    }
    auto status = batch_reader_->ReadNext(&out.data);
    if (ARROW_PREDICT_FALSE(!status.ok())) {
      return stream_->Finish(std::move(status));
    }
    out.app_metadata = std::move(app_metadata_);
    return out;
  }
  arrow::Result<std::vector<std::shared_ptr<RecordBatch>>> ToRecordBatches() override {
    return ToRecordBatches(stop_token_);
  }
  arrow::Result<std::vector<std::shared_ptr<RecordBatch>>> ToRecordBatches(
      const StopToken& stop_token) override {
    std::vector<std::shared_ptr<RecordBatch>> batches;
    FlightStreamChunk chunk;

    while (true) {
      if (stop_token.IsStopRequested()) {
        Cancel();
        return stop_token.Poll();
      }
      ARROW_ASSIGN_OR_RAISE(chunk, Next());
      if (!chunk.data) break;
      batches.emplace_back(std::move(chunk.data));
    }
    return batches;
  }
  arrow::Result<std::shared_ptr<Table>> ToTable() override {
    return ToTable(stop_token_);
  }
  using FlightStreamReader::ToTable;
  void Cancel() override { stream_->TryCancel(); }

 private:
  Status OverrideWithServerError(Status&& st) {
    if (st.ok()) {
      return std::move(st);
    }
    return stream_->Finish(std::move(st));
  }

  std::shared_ptr<internal::ClientDataStream> stream_;
  ipc::IpcReadOptions options_;
  StopToken stop_token_;
  std::shared_ptr<MemoryManager> memory_manager_;
  std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader_;
  std::shared_ptr<ipc::RecordBatchReader> batch_reader_;
  std::shared_ptr<Buffer> app_metadata_;
};

FlightMetadataReader::~FlightMetadataReader() = default;

class ClientMetadataReader : public FlightMetadataReader {
 public:
  explicit ClientMetadataReader(std::shared_ptr<internal::ClientDataStream> stream)
      : stream_(std::move(stream)) {}

  Status ReadMetadata(std::shared_ptr<Buffer>* out) override {
    if (!stream_->ReadPutMetadata(out)) {
      return stream_->Finish(Status::OK());
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<internal::ClientDataStream> stream_;
};

/// This status detail indicates the write failed in the transport
/// (due to the server) and that we should finish the call at a higher
/// level (to get the server error); otherwise the client should pass
/// through the status (it may be recoverable) instead of finishing
/// the call (which may inadvertently make the server think the client
/// intended to end the call successfully) or canceling the call
/// (which may generate an unexpected error message on the client
/// side).
const char* kTagDetailTypeId = "flight::ServerErrorTagStatusDetail";
class ServerErrorTagStatusDetail : public arrow::StatusDetail {
 public:
  const char* type_id() const override { return kTagDetailTypeId; }
  std::string ToString() const override { return type_id(); };

  static bool UnwrapStatus(const arrow::Status& status) {
    return status.detail() && status.detail()->type_id() == kTagDetailTypeId;
  }
};

/// \brief An IpcPayloadWriter for any ClientDataStream.
///
/// To support app_metadata and reuse the existing IPC infrastructure,
/// this takes a pointer to a buffer to be combined with the IPC
/// payload when writing a Flight payload.
class ClientPutPayloadWriter : public ipc::internal::IpcPayloadWriter {
 public:
  ClientPutPayloadWriter(std::shared_ptr<internal::ClientDataStream> stream,
                         FlightDescriptor descriptor, int64_t write_size_limit_bytes,
                         std::shared_ptr<Buffer>* app_metadata)
      : descriptor_(std::move(descriptor)),
        write_size_limit_bytes_(write_size_limit_bytes),
        stream_(std::move(stream)),
        app_metadata_(app_metadata),
        first_payload_(true) {}

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
    } else if (ipc_payload.type == ipc::MessageType::RECORD_BATCH && *app_metadata_) {
      payload.app_metadata = std::move(*app_metadata_);
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
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      return Status::FromDetailAndArgs(
          StatusCode::IOError, std::make_shared<ServerErrorTagStatusDetail>(),
          "Could not write record batch to stream (server disconnect?)");
    }
    return Status::OK();
  }
  Status Close() override {
    // Closing is handled one layer up in ClientStreamWriter::Close
    return Status::OK();
  }

 private:
  const FlightDescriptor descriptor_;
  const int64_t write_size_limit_bytes_;
  std::shared_ptr<internal::ClientDataStream> stream_;
  std::shared_ptr<Buffer>* app_metadata_;
  bool first_payload_;
};

class ClientStreamWriter : public FlightStreamWriter {
 public:
  explicit ClientStreamWriter(std::shared_ptr<internal::ClientDataStream> stream,
                              const ipc::IpcWriteOptions& options,
                              int64_t write_size_limit_bytes, FlightDescriptor descriptor)
      : stream_(std::move(stream)),
        batch_writer_(nullptr),
        app_metadata_(nullptr),
        writer_closed_(false),
        closed_(false),
        write_options_(options),
        write_size_limit_bytes_(write_size_limit_bytes),
        descriptor_(std::move(descriptor)) {}

  ~ClientStreamWriter() {
    if (closed_) return;
    // Implicitly Close() on destruction, though it's best if the
    // application closes explicitly
    ARROW_WARN_NOT_OK(Close(), "Close() failed");
  }

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    if (batch_writer_) {
      return Status::Invalid("This writer has already been started.");
    }
    std::unique_ptr<ipc::internal::IpcPayloadWriter> payload_writer(
        new ClientPutPayloadWriter(stream_, std::move(descriptor_),
                                   write_size_limit_bytes_, &app_metadata_));
    // XXX: this does not actually write the message to the stream.
    // See Close().

    // On failure, we should close the stream to make sure we get any gRPC-side error
    auto status =
        ipc::internal::OpenRecordBatchWriter(std::move(payload_writer), schema, options)
            .Value(&batch_writer_);
    if (!status.ok()) {
      closed_ = true;
      final_status_ = stream_->Finish(std::move(status));
      return final_status_;
    }
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema) override {
    return Begin(schema, write_options_);
  }

  // Overload used by FlightClient::DoExchange
  Status Begin() {
    FlightPayload payload;
    RETURN_NOT_OK(internal::ToPayload(descriptor_, &payload.descriptor));
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      return Close();
    }
    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    RETURN_NOT_OK(CheckStarted());
    return WriteWithMetadata(batch, nullptr);
  }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    FlightPayload payload;
    payload.app_metadata = app_metadata;
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      return Close();
    }
    return Status::OK();
  }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    RETURN_NOT_OK(CheckStarted());
    app_metadata_ = app_metadata;
    auto status = batch_writer_->WriteRecordBatch(batch);
    if (!status.ok() &&
        // Only want to Close() if server error, not for client error
        ServerErrorTagStatusDetail::UnwrapStatus(status)) {
      return Close();
    }
    return status;
  }

  Status DoneWriting() override {
    // Do not CheckStarted - DoneWriting applies to data and metadata
    if (batch_writer_) {
      // Close the writer if we have one; this will force it to flush any
      // remaining data, before we close the write side of the stream.
      writer_closed_ = true;
      Status st = batch_writer_->Close();
      if (!st.ok()) {
        return stream_->Finish(std::move(st));
      }
    }
    return stream_->WritesDone();
  }

  Status Close() override {
    // Do not CheckStarted - Close applies to data and metadata
    if (!closed_) {
      closed_ = true;
      if (batch_writer_ && !writer_closed_) {
        // This is important! Close() calls
        // IpcPayloadWriter::CheckStarted() which will force the initial
        // schema message to be written to the stream. This is required
        // to unstick the server, else the client and the server end up
        // waiting for each other. This happens if the client never
        // wrote anything before calling Close().
        writer_closed_ = true;
        final_status_ = stream_->Finish(batch_writer_->Close());
      } else {
        final_status_ = stream_->Finish(Status::OK());
      }
    }
    return final_status_;
  }

  ipc::WriteStats stats() const override {
    ARROW_CHECK_NE(batch_writer_, nullptr);
    return batch_writer_->stats();
  }

 private:
  Status CheckStarted() {
    if (!batch_writer_) {
      return Status::Invalid("Writer not initialized. Call Begin() with a schema.");
    }
    return Status::OK();
  }

  std::shared_ptr<internal::ClientDataStream> stream_;
  std::unique_ptr<ipc::RecordBatchWriter> batch_writer_;
  std::shared_ptr<Buffer> app_metadata_;
  bool writer_closed_;
  bool closed_;
  // Close() is expected to be idempotent
  Status final_status_;

  // Temporary state to construct the IPC payload writer
  ipc::IpcWriteOptions write_options_;
  int64_t write_size_limit_bytes_;
  FlightDescriptor descriptor_;
};

FlightClient::FlightClient() : closed_(false), write_size_limit_bytes_(0) {}

FlightClient::~FlightClient() {
  ARROW_WARN_NOT_OK(Close(), "FlightClient::~FlightClient(): Close() failed");
}

arrow::Result<std::unique_ptr<FlightClient>> FlightClient::Connect(
    const Location& location) {
  return Connect(location, FlightClientOptions::Defaults());
}

Status FlightClient::Connect(const Location& location,
                             std::unique_ptr<FlightClient>* client) {
  return Connect(location, FlightClientOptions::Defaults()).Value(client);
}

arrow::Result<std::unique_ptr<FlightClient>> FlightClient::Connect(
    const Location& location, const FlightClientOptions& options) {
  flight::transport::grpc::InitializeFlightGrpcClient();

  std::unique_ptr<FlightClient> client(new FlightClient());
  client->write_size_limit_bytes_ = options.write_size_limit_bytes;
  const auto scheme = location.scheme();
  ARROW_ASSIGN_OR_RAISE(client->transport_,
                        internal::GetDefaultTransportRegistry()->MakeClient(scheme));
  RETURN_NOT_OK(client->transport_->Init(options, location, *location.uri_));
  return client;
}

Status FlightClient::Connect(const Location& location, const FlightClientOptions& options,
                             std::unique_ptr<FlightClient>* client) {
  return Connect(location, options).Value(client);
}

Status FlightClient::Authenticate(const FlightCallOptions& options,
                                  std::unique_ptr<ClientAuthHandler> auth_handler) {
  RETURN_NOT_OK(CheckOpen());
  return transport_->Authenticate(options, std::move(auth_handler));
}

arrow::Result<std::pair<std::string, std::string>> FlightClient::AuthenticateBasicToken(
    const FlightCallOptions& options, const std::string& username,
    const std::string& password) {
  RETURN_NOT_OK(CheckOpen());
  return transport_->AuthenticateBasicToken(options, username, password);
}

arrow::Result<std::unique_ptr<ResultStream>> FlightClient::DoAction(
    const FlightCallOptions& options, const Action& action) {
  std::unique_ptr<ResultStream> results;
  RETURN_NOT_OK(CheckOpen());
  RETURN_NOT_OK(transport_->DoAction(options, action, &results));
  return results;
}

Status FlightClient::DoAction(const FlightCallOptions& options, const Action& action,
                              std::unique_ptr<ResultStream>* results) {
  return DoAction(options, action).Value(results);
}

arrow::Result<std::vector<ActionType>> FlightClient::ListActions(
    const FlightCallOptions& options) {
  std::vector<ActionType> actions;
  RETURN_NOT_OK(CheckOpen());
  RETURN_NOT_OK(transport_->ListActions(options, &actions));
  return actions;
}

Status FlightClient::ListActions(const FlightCallOptions& options,
                                 std::vector<ActionType>* actions) {
  return ListActions(options).Value(actions);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightClient::GetFlightInfo(
    const FlightCallOptions& options, const FlightDescriptor& descriptor) {
  std::unique_ptr<FlightInfo> info;
  RETURN_NOT_OK(CheckOpen());
  RETURN_NOT_OK(transport_->GetFlightInfo(options, descriptor, &info));
  return info;
}

Status FlightClient::GetFlightInfo(const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor,
                                   std::unique_ptr<FlightInfo>* info) {
  return GetFlightInfo(options, descriptor).Value(info);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightClient::GetSchema(
    const FlightCallOptions& options, const FlightDescriptor& descriptor) {
  RETURN_NOT_OK(CheckOpen());
  return transport_->GetSchema(options, descriptor);
}

Status FlightClient::GetSchema(const FlightCallOptions& options,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<SchemaResult>* schema_result) {
  return GetSchema(options, descriptor).Value(schema_result);
}

arrow::Result<std::unique_ptr<FlightListing>> FlightClient::ListFlights() {
  return ListFlights({}, {});
}

Status FlightClient::ListFlights(std::unique_ptr<FlightListing>* listing) {
  return ListFlights({}, {}).Value(listing);
}

arrow::Result<std::unique_ptr<FlightListing>> FlightClient::ListFlights(
    const FlightCallOptions& options, const Criteria& criteria) {
  std::unique_ptr<FlightListing> listing;
  RETURN_NOT_OK(CheckOpen());
  RETURN_NOT_OK(transport_->ListFlights(options, criteria, &listing));
  return listing;
}

Status FlightClient::ListFlights(const FlightCallOptions& options,
                                 const Criteria& criteria,
                                 std::unique_ptr<FlightListing>* listing) {
  return ListFlights(options, criteria).Value(listing);
}

arrow::Result<std::unique_ptr<FlightStreamReader>> FlightClient::DoGet(
    const FlightCallOptions& options, const Ticket& ticket) {
  RETURN_NOT_OK(CheckOpen());
  std::unique_ptr<internal::ClientDataStream> remote_stream;
  RETURN_NOT_OK(transport_->DoGet(options, ticket, &remote_stream));
  std::unique_ptr<FlightStreamReader> stream_reader =
      std::make_unique<ClientStreamReader>(std::move(remote_stream), options.read_options,
                                           options.stop_token, options.memory_manager);
  // Eagerly read the schema
  RETURN_NOT_OK(
      static_cast<ClientStreamReader*>(stream_reader.get())->EnsureDataStarted());
  return stream_reader;
}

Status FlightClient::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                           std::unique_ptr<FlightStreamReader>* stream) {
  return DoGet(options, ticket).Value(stream);
}

arrow::Result<FlightClient::DoPutResult> FlightClient::DoPut(
    const FlightCallOptions& options, const FlightDescriptor& descriptor,
    const std::shared_ptr<Schema>& schema) {
  RETURN_NOT_OK(CheckOpen());
  std::unique_ptr<internal::ClientDataStream> remote_stream;
  RETURN_NOT_OK(transport_->DoPut(options, &remote_stream));
  std::shared_ptr<internal::ClientDataStream> shared_stream = std::move(remote_stream);
  DoPutResult result;
  result.reader = std::make_unique<ClientMetadataReader>(shared_stream);
  result.writer = std::make_unique<ClientStreamWriter>(
      std::move(shared_stream), options.write_options, write_size_limit_bytes_,
      descriptor);
  RETURN_NOT_OK(result.writer->Begin(schema, options.write_options));
  return result;
}

Status FlightClient::DoPut(const FlightCallOptions& options,
                           const FlightDescriptor& descriptor,
                           const std::shared_ptr<Schema>& schema,
                           std::unique_ptr<FlightStreamWriter>* writer,
                           std::unique_ptr<FlightMetadataReader>* reader) {
  ARROW_ASSIGN_OR_RAISE(auto result, DoPut(options, descriptor, schema));
  *writer = std::move(result.writer);
  *reader = std::move(result.reader);
  return Status::OK();
}

arrow::Result<FlightClient::DoExchangeResult> FlightClient::DoExchange(
    const FlightCallOptions& options, const FlightDescriptor& descriptor) {
  RETURN_NOT_OK(CheckOpen());
  std::unique_ptr<internal::ClientDataStream> remote_stream;
  RETURN_NOT_OK(transport_->DoExchange(options, &remote_stream));
  std::shared_ptr<internal::ClientDataStream> shared_stream = std::move(remote_stream);
  DoExchangeResult result;
  result.reader = std::make_unique<ClientStreamReader>(
      shared_stream, options.read_options, options.stop_token, options.memory_manager);
  auto stream_writer = std::make_unique<ClientStreamWriter>(
      std::move(shared_stream), options.write_options, write_size_limit_bytes_,
      descriptor);
  RETURN_NOT_OK(stream_writer->Begin());
  result.writer = std::move(stream_writer);
  return result;
}

Status FlightClient::DoExchange(const FlightCallOptions& options,
                                const FlightDescriptor& descriptor,
                                std::unique_ptr<FlightStreamWriter>* writer,
                                std::unique_ptr<FlightStreamReader>* reader) {
  ARROW_ASSIGN_OR_RAISE(auto result, DoExchange(options, descriptor));
  *writer = std::move(result.writer);
  *reader = std::move(result.reader);
  return Status::OK();
}

Status FlightClient::Close() {
  if (!closed_) {
    closed_ = true;
    if (transport_) RETURN_NOT_OK(transport_->Close());
    transport_.reset(nullptr);
  }
  return Status::OK();
}

Status FlightClient::CheckOpen() const {
  if (closed_) {
    return Status::Invalid("FlightClient is closed");
  }
  return Status::OK();
}

}  // namespace flight
}  // namespace arrow
