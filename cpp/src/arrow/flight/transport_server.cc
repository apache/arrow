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
    if (data->body &&
        ARROW_PREDICT_FALSE(!data->body->device()->Equals(*memory_manager_->device()))) {
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
    internal::FlightData* data;
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
  std::shared_ptr<RecordBatchReader> batch_reader_;
  std::shared_ptr<Buffer> app_metadata_;
};

// TODO(ARROW-10787): this should use the same writer/ipc trick as client
class TransportMessageWriter final : public FlightMessageWriter {
 public:
  explicit TransportMessageWriter(ServerDataStream* stream)
      : stream_(stream), ipc_options_(::arrow::ipc::IpcWriteOptions::Defaults()) {}

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    if (started_) {
      return Status::Invalid("This writer has already been started.");
    }
    started_ = true;
    ipc_options_ = options;

    RETURN_NOT_OK(mapper_.AddSchemaFields(*schema));
    FlightPayload schema_payload;
    RETURN_NOT_OK(ipc::GetSchemaPayload(*schema, ipc_options_, mapper_,
                                        &schema_payload.ipc_message));
    return WritePayload(schema_payload);
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    return WriteWithMetadata(batch, nullptr);
  }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    FlightPayload payload{};
    payload.app_metadata = app_metadata;
    return WritePayload(payload);
  }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    RETURN_NOT_OK(CheckStarted());
    RETURN_NOT_OK(EnsureDictionariesWritten(batch));
    FlightPayload payload{};
    if (app_metadata) {
      payload.app_metadata = app_metadata;
    }
    RETURN_NOT_OK(ipc::GetRecordBatchPayload(batch, ipc_options_, &payload.ipc_message));
    RETURN_NOT_OK(WritePayload(payload));
    ++stats_.num_record_batches;
    return Status::OK();
  }

  Status Close() override {
    // It's fine to Close() without writing data
    return Status::OK();
  }

  ipc::WriteStats stats() const override { return stats_; }

 private:
  Status WritePayload(const FlightPayload& payload) {
    ARROW_ASSIGN_OR_RAISE(auto success, stream_->WriteData(payload));
    if (!success) {
      return MakeFlightError(FlightStatusCode::Internal,
                             "Could not write metadata to stream (client disconnect?)");
    }
    ++stats_.num_messages;
    return Status::OK();
  }

  Status CheckStarted() {
    if (!started_) {
      return Status::Invalid("This writer is not started. Call Begin() with a schema");
    }
    return Status::OK();
  }

  Status EnsureDictionariesWritten(const RecordBatch& batch) {
    if (dictionaries_written_) {
      return Status::OK();
    }
    dictionaries_written_ = true;
    ARROW_ASSIGN_OR_RAISE(const auto dictionaries,
                          ipc::CollectDictionaries(batch, mapper_));
    for (const auto& pair : dictionaries) {
      FlightPayload payload{};
      RETURN_NOT_OK(ipc::GetDictionaryPayload(pair.first, pair.second, ipc_options_,
                                              &payload.ipc_message));
      RETURN_NOT_OK(WritePayload(payload));
      ++stats_.num_dictionary_batches;
    }
    return Status::OK();
  }

  ServerDataStream* stream_;
  ::arrow::ipc::IpcWriteOptions ipc_options_;
  ipc::DictionaryFieldMapper mapper_;
  ipc::WriteStats stats_;
  bool started_ = false;
  bool dictionaries_written_ = false;
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
  return Status::OK();
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
