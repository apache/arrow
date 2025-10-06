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

#include "arrow/flight/transport_server.h"

#include <unordered_map>

#include "arrow/buffer.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/reader.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace flight {
namespace internal {

Status ServerDataStream::WritePutMetadata(const Buffer&) {
  return Status::NotImplemented("Writing put metadata for this stream");
}

namespace {
class TransportIpcMessageReader : public ipc::MessageReader {
 public:
  TransportIpcMessageReader(
      std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader,
      std::shared_ptr<MemoryManager> memory_manager,
      std::shared_ptr<Buffer>* app_metadata)
      : peekable_reader_(peekable_reader),
        memory_manager_(std::move(memory_manager)),
        app_metadata_(app_metadata) {}

  ::arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (stream_finished_) return nullptr;
    internal::FlightData* data;
    peekable_reader_->Next(&data);
    if (!data) {
      stream_finished_ = true;
      return nullptr;
    }
    if (data->body) {
      ARROW_ASSIGN_OR_RAISE(data->body, Buffer::ViewOrCopy(data->body, memory_manager_));
    }
    *app_metadata_ = std::move(data->app_metadata);
    return data->OpenMessage();
  }

 protected:
  std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader_;
  std::shared_ptr<MemoryManager> memory_manager_;
  // A reference to TransportDataStream.app_metadata_. That class
  // can't access the app metadata because when it Peek()s the stream,
  // it may be looking at a dictionary batch, not the record
  // batch. Updating it here ensures the reader is always updated with
  // the last metadata message read.
  std::shared_ptr<Buffer>* app_metadata_;
  bool stream_finished_ = false;
};

/// \brief Adapt TransportDataStream to the FlightMessageReader
///   interface for DoPut.
class TransportMessageReader final : public FlightMessageReader {
 public:
  TransportMessageReader(ServerDataStream* stream,
                         std::shared_ptr<MemoryManager> memory_manager)
      : peekable_reader_(new internal::PeekableFlightDataReader(stream)),
        memory_manager_(std::move(memory_manager)) {}

  Status Init() {
    // Peek the first message to get the descriptor.
    internal::FlightData* data;
    peekable_reader_->Peek(&data);
    if (!data) {
      return Status::IOError("Stream finished before first message sent");
    }
    if (!data->descriptor) {
      return Status::IOError("Descriptor missing on first message");
    }
    descriptor_ = *data->descriptor;
    // If there's a schema (=DoPut), also Open().
    if (data->metadata) {
      return EnsureDataStarted();
    }
    peekable_reader_->Next(&data);
    return Status::OK();
  }

  const FlightDescriptor& descriptor() const override { return descriptor_; }

  arrow::Result<std::shared_ptr<Schema>> GetSchema() override {
    RETURN_NOT_OK(EnsureDataStarted());
    return batch_reader_->schema();
  }

  arrow::Result<FlightStreamChunk> Next() override {
    FlightStreamChunk out;
    internal::FlightData* data = nullptr;
    peekable_reader_->Peek(&data);
    if (!data) {
      out.app_metadata = nullptr;
      out.data = nullptr;
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
      // re-peek here since EnsureDataStarted() advances the stream
      return Next();
    }
    RETURN_NOT_OK(batch_reader_->ReadNext(&out.data));
    out.app_metadata = std::move(app_metadata_);
    return out;
  }

  ipc::ReadStats stats() const override {
    if (batch_reader_ == nullptr) {
      return ipc::ReadStats{};
    }
    return batch_reader_->stats();
  }

 private:
  /// Ensure we are set up to read data.
  Status EnsureDataStarted() {
    if (!batch_reader_) {
      // peek() until we find the first data message; discard metadata
      if (!peekable_reader_->SkipToData()) {
        return Status::IOError("Client never sent a data message");
      }
      auto message_reader =
          std::unique_ptr<ipc::MessageReader>(new TransportIpcMessageReader(
              peekable_reader_, memory_manager_, &app_metadata_));
      ARROW_ASSIGN_OR_RAISE(
          batch_reader_, ipc::RecordBatchStreamReader::Open(std::move(message_reader)));
    }
    return Status::OK();
  }

  FlightDescriptor descriptor_;
  std::shared_ptr<internal::PeekableFlightDataReader> peekable_reader_;
  std::shared_ptr<MemoryManager> memory_manager_;
  std::shared_ptr<ipc::RecordBatchStreamReader> batch_reader_;
  std::shared_ptr<Buffer> app_metadata_;
};

/// \brief An IpcPayloadWriter for ServerDataStream.
///
/// To support app_metadata and reuse the existing IPC infrastructure,
/// this takes a pointer to a buffer to be combined with the IPC
/// payload when writing a Flight payload.
class TransportMessagePayloadWriter : public ipc::internal::IpcPayloadWriter {
 public:
  TransportMessagePayloadWriter(ServerDataStream* stream,
                                std::shared_ptr<Buffer>* app_metadata)
      : stream_(stream), app_metadata_(app_metadata) {}

  Status Start() override { return Status::OK(); }
  Status WritePayload(const ipc::IpcPayload& ipc_payload) override {
    FlightPayload payload;
    payload.ipc_message = ipc_payload;

    if (ipc_payload.type == ipc::MessageType::RECORD_BATCH && *app_metadata_) {
      payload.app_metadata = std::move(*app_metadata_);
    }
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      return MakeFlightError(
          FlightStatusCode::Internal,
          "Could not write record batch to stream (client disconnect?)");
    }
    return arrow::Status::OK();
  }
  Status Close() override {
    // Closing is handled one layer up in TransportMessageWriter::Close
    return Status::OK();
  }

 private:
  ServerDataStream* stream_;
  std::shared_ptr<Buffer>* app_metadata_;
};

class TransportMessageWriter final : public FlightMessageWriter {
 public:
  explicit TransportMessageWriter(ServerDataStream* stream)
      : stream_(stream),
        app_metadata_(nullptr),
        ipc_options_(::arrow::ipc::IpcWriteOptions::Defaults()) {}

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    if (batch_writer_) {
      return Status::Invalid("This writer has already been started.");
    }
    ipc_options_ = options;
    std::unique_ptr<ipc::internal::IpcPayloadWriter> payload_writer(
        new TransportMessagePayloadWriter(stream_, &app_metadata_));

    ARROW_ASSIGN_OR_RAISE(batch_writer_,
                          ipc::internal::OpenRecordBatchWriter(std::move(payload_writer),
                                                               schema, ipc_options_));
    return Status::OK();
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    return WriteWithMetadata(batch, nullptr);
  }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    FlightPayload payload{};
    payload.app_metadata = app_metadata;
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      ARROW_RETURN_NOT_OK(Close());
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not write metadata to stream (client disconnect?)");
    }
    return Status::OK();
  }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    RETURN_NOT_OK(CheckStarted());
    app_metadata_ = app_metadata;
    auto status = batch_writer_->WriteRecordBatch(batch);
    if (!status.ok()) {
      ARROW_RETURN_NOT_OK(Close());
    }
    return status;
  }

  Status Close() override {
    if (batch_writer_) {
      RETURN_NOT_OK(batch_writer_->Close());
    }
    return Status::OK();
  }

  ipc::WriteStats stats() const override {
    ARROW_CHECK_NE(batch_writer_, nullptr);
    return batch_writer_->stats();
  }

 private:
  Status CheckStarted() {
    if (!batch_writer_) {
      return Status::Invalid("This writer is not started. Call Begin() with a schema");
    }
    return Status::OK();
  }

  ServerDataStream* stream_;
  std::unique_ptr<ipc::RecordBatchWriter> batch_writer_;
  std::shared_ptr<Buffer> app_metadata_;
  ::arrow::ipc::IpcWriteOptions ipc_options_;
};

/// \brief Adapt TransportDataStream to the FlightMetadataWriter
///   interface for DoPut.
class TransportMetadataWriter final : public FlightMetadataWriter {
 public:
  explicit TransportMetadataWriter(ServerDataStream* stream) : stream_(stream) {}

  Status WriteMetadata(const Buffer& buffer) override {
    return stream_->WritePutMetadata(buffer);
  }

 private:
  ServerDataStream* stream_;
};
}  // namespace

Status ServerTransport::DoGet(const ServerCallContext& context, const Ticket& ticket,
                              ServerDataStream* stream) {
  std::unique_ptr<FlightDataStream> data_stream;
  RETURN_NOT_OK(base_->DoGet(context, ticket, &data_stream));

  if (!data_stream) return Status::KeyError("No data in this flight");

  // Write the schema as the first message in the stream
  ARROW_ASSIGN_OR_RAISE(auto schema_payload, data_stream->GetSchemaPayload());
  ARROW_ASSIGN_OR_RAISE(auto success, stream->WriteData(schema_payload));
  // Connection terminated
  if (!success) return Status::OK();

  // Consume data stream and write out payloads
  while (true) {
    ARROW_ASSIGN_OR_RAISE(FlightPayload payload, data_stream->Next());
    // End of stream
    if (payload.ipc_message.metadata == nullptr) break;
    ARROW_ASSIGN_OR_RAISE(auto success, stream->WriteData(payload));
    // Connection terminated
    if (!success) return Status::OK();
  }
  RETURN_NOT_OK(stream->WritesDone());
  return data_stream->Close();
}

Status ServerTransport::DoPut(const ServerCallContext& context,
                              ServerDataStream* stream) {
  std::unique_ptr<TransportMessageReader> reader(
      new TransportMessageReader(stream, memory_manager_));
  std::unique_ptr<FlightMetadataWriter> writer(new TransportMetadataWriter(stream));
  RETURN_NOT_OK(reader->Init());
  RETURN_NOT_OK(base_->DoPut(context, std::move(reader), std::move(writer)));
  RETURN_NOT_OK(stream->WritesDone());
  return Status::OK();
}

Status ServerTransport::DoExchange(const ServerCallContext& context,
                                   ServerDataStream* stream) {
  std::unique_ptr<TransportMessageReader> reader(
      new TransportMessageReader(stream, memory_manager_));
  std::unique_ptr<FlightMessageWriter> writer(new TransportMessageWriter(stream));
  RETURN_NOT_OK(reader->Init());
  RETURN_NOT_OK(base_->DoExchange(context, std::move(reader), std::move(writer)));
  RETURN_NOT_OK(stream->WritesDone());
  return Status::OK();
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
