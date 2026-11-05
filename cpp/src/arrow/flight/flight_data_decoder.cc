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

#include "arrow/flight/flight_data_decoder.h"

#include <algorithm>
#include <deque>

#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow::flight {

namespace {

// FlightDataMessageReader is an ipc::MessageReader that accepts one at a time
// FlightData messages. Analogous to MessageReader::Open(InputStream*) but for
// individual FlightData messages directly read from the received buffers.
class FlightDataMessageReader : public ipc::MessageReader {
 public:
  /// \brief Queue one message, with the app metadata it arrived with.
  /// \param message The message to queue.
  /// \param app_metadata The application metadata associated with the message.
  void Push(std::unique_ptr<ipc::Message> message, std::shared_ptr<Buffer> app_metadata) {
    queue_.emplace_back(std::move(message), std::move(app_metadata));
  }

  /// \brief Check if there is a record batch message queued.
  /// \return True if a record batch message is queued, false otherwise.
  bool HasRecordBatch() const {
    return std::ranges::any_of(queue_, [](const QueuedMessage& queued) {
      return queued.message->type() == ipc::MessageType::RECORD_BATCH;
    });
  }

  /// \brief Take the app metadata of the last queued message, leaving that message
  /// with none: its app metadata has been delivered already.
  /// \return The app metadata of the last queued message, or nullptr if no messages are
  /// queued.
  std::shared_ptr<Buffer> TakeLastAppMetadata() {
    if (queue_.empty()) {
      return nullptr;
    }
    return std::move(queue_.back().app_metadata);
  }

  /// Take the next queued message, if any, and return it. The app metadata of
  /// the message taken is stored internally and can be retrieved with
  /// ReadAppMetadata().
  /// \return The next queued message, or nullptr if no messages are queued.
  ::arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (queue_.empty()) {
      return nullptr;
    }
    QueuedMessage queued = std::move(queue_.front());
    queue_.pop_front();
    app_metadata_ = std::move(queued.app_metadata);
    return std::move(queued.message);
  }

  /// \brief Read the application metadata of the last message taken by ReadNextMessage().
  /// \return The application metadata of the last message taken, or nullptr if no
  /// messages have been taken.
  std::shared_ptr<Buffer> ReadAppMetadata() { return app_metadata_; }

 private:
  /// \brief Queue of messages that have been pushed but not yet read.
  struct QueuedMessage {
    QueuedMessage() = default;
    QueuedMessage(std::unique_ptr<ipc::Message> message,
                  std::shared_ptr<Buffer> app_metadata)
        : message(std::move(message)), app_metadata(std::move(app_metadata)) {}
    std::unique_ptr<ipc::Message> message;
    std::shared_ptr<Buffer> app_metadata;
  };

  std::deque<QueuedMessage> queue_;
  std::shared_ptr<Buffer> app_metadata_;
};

// FlightDataMessageReader is owned by the decoder, not by the
// RecordBatchStreamReader that reads through it: Open() takes a unique_ptr and
// adopts it, so handing over the decoder's reader would free it on any failure
// path (a malformed first message, a first message that is not a schema),
// leaving the decoder with a dangling pointer.  This forwards to the decoder's
// reader and stays owned by it.
class FlightDataMessageReaderRef final : public ipc::MessageReader {
 public:
  explicit FlightDataMessageReaderRef(FlightDataMessageReader* reader)
      : reader_(reader) {}

  arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    return reader_->ReadNextMessage();
  }

 private:
  FlightDataMessageReader* reader_;
};

}  // namespace

class AsyncFlightMessageDecoder::AsyncFlightMessageDecoderImpl {
 public:
  AsyncFlightMessageDecoderImpl(std::shared_ptr<AsyncFlightDataListener> listener,
                                ipc::IpcReadOptions options)
      : listener_(std::move(listener)),
        options_(std::move(options)),
        message_reader_(std::make_unique<FlightDataMessageReader>()) {}

  /// \brief Consume a chunk of Flight data.
  /// \param data The Flight data to consume.
  /// \return A future carrying the listener's status for this message: a
  /// non-OK result rejects the upload.
  /// The descriptor of an upload is eported before the rest of its first message is
  /// decoded.
  Future<> ConsumeData(internal::FlightData data) {
    if (!data.descriptor) {
      return ConsumeDataAfterDescriptor(std::move(data));
    } else {
      // The descriptor of an upload arrives with the first message of the
      // upload, which also carries the schema: report the descriptor first, and
      // decode the rest of that message once the listener's future resolves.
      return listener_->OnDescriptor(*data.descriptor)
          .Then([this, data = std::move(data)]() mutable {
            return ConsumeDataAfterDescriptor(std::move(data));
          });
    }
  }

  std::shared_ptr<Schema> schema() const {
    return batch_reader_ ? batch_reader_->schema() : nullptr;
  }

 private:
  /// \brief Decode a FlightData message whose descriptor (if any) was already
  /// reported to the listener.
  /// \param data The Flight data to consume after the descriptor has been reported.
  /// \return A future carrying the listener's status for this message: a non-OK result
  /// rejects the upload.
  /// \note The descriptor of an upload is assumed to have been reported
  /// before calling this method.
  ///
  Future<> ConsumeDataAfterDescriptor(internal::FlightData data) {
    if (!data.metadata) {
      // Metadata-only message: no IPC content, just Flight app_metadata.
      if (data.app_metadata && data.app_metadata->size() > 0) {
        FlightStreamChunk chunk;
        chunk.app_metadata = std::move(data.app_metadata);
        return listener_->OnNext(std::move(chunk));
      }
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(auto message, data.OpenMessage());

    if (!batch_reader_) {
      // Initialize RecordBatchStreamReader and read the first IPC message.
      // It must be a schema.
      message_reader_->Push(std::move(message), std::move(data.app_metadata));
      ARROW_ASSIGN_OR_RAISE(
          batch_reader_,
          ipc::RecordBatchStreamReader::Open(
              std::make_unique<FlightDataMessageReaderRef>(message_reader_.get()),
              options_));
      return listener_->OnSchemaDecoded(batch_reader_->schema());
    }

    message_reader_->Push(std::move(message), std::move(data.app_metadata));

    if (!message_reader_->HasRecordBatch()) {
      auto app_metadata = message_reader_->TakeLastAppMetadata();
      if (app_metadata && app_metadata->size() > 0) {
        FlightStreamChunk chunk;
        chunk.app_metadata = std::move(app_metadata);
        return listener_->OnNext(std::move(chunk));
      }
      return arrow::Future<>::MakeFinished(Status::OK());
    }

    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(batch_reader_->ReadNext(&batch));
    auto app_metadata = message_reader_->ReadAppMetadata();

    if (batch) {
      FlightStreamChunk chunk{std::move(batch), std::move(app_metadata)};
      return listener_->OnNext(std::move(chunk));
    }

    // This has to be a Dictionary batch.
    if (app_metadata && app_metadata->size() > 0) {
      FlightStreamChunk chunk{nullptr, std::move(app_metadata)};
      return listener_->OnNext(std::move(chunk));
    }
    return arrow::Future<>::MakeFinished(Status::OK());
  }

  std::shared_ptr<AsyncFlightDataListener> listener_;
  ipc::IpcReadOptions options_;
  // Kept here, not transferred to the RecordBatchStreamReader: the decoder
  // reads the app metadata back out of it, and a failed Open() must leave it
  // usable.
  std::unique_ptr<FlightDataMessageReader> message_reader_;
  std::shared_ptr<ipc::RecordBatchStreamReader> batch_reader_;
};

AsyncFlightMessageDecoder::AsyncFlightMessageDecoder(
    std::shared_ptr<AsyncFlightDataListener> listener, ipc::IpcReadOptions options)
    : impl_(std::make_unique<AsyncFlightMessageDecoderImpl>(std::move(listener),
                                                            std::move(options))) {}

AsyncFlightMessageDecoder::~AsyncFlightMessageDecoder() = default;

Future<> AsyncFlightMessageDecoder::Consume(std::shared_ptr<Buffer> buffer) {
  ARROW_ASSIGN_OR_RAISE(auto data, internal::DeserializeFlightData(buffer));
  return impl_->ConsumeData(std::move(data));
}

Future<> AsyncFlightMessageDecoder::Consume(internal::FlightData data) {
  return impl_->ConsumeData(std::move(data));
}

std::shared_ptr<Schema> AsyncFlightMessageDecoder::schema() const {
  return impl_->schema();
}

// --- AsyncFlightDataListener: transport state and the terminal callbacks ---

AsyncFlightDataListener::AsyncFlightDataListener() = default;
AsyncFlightDataListener::~AsyncFlightDataListener() = default;
AsyncFlightDataWriter::~AsyncFlightDataWriter() = default;

Status AsyncFlightDataWriter::WriteMetadata(const Buffer& app_metadata) {
  if (transport_ == nullptr) {
    return Status::Invalid(
        "no upload in flight to write metadata to: the writer is only usable "
        "while the upload's RPC is running");
  }
  return transport_->WriteMetadata(app_metadata);
}

namespace internal {

FlightDataListenerTransport::~FlightDataListenerTransport() = default;

void FlightDataListenerTransport::Install(
    const std::shared_ptr<AsyncFlightDataListener>& listener,
    FlightDataListenerTransport* transport) {
  if (listener == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(listener->transport_mutex_);
  listener->transport_ = transport;
  if (listener->writer_ != nullptr) {
    std::lock_guard<std::mutex> writer_lock(listener->writer_->writer_mutex_);
    listener->writer_->transport_ = transport;
  }
}

void FlightDataListenerTransport::Clear(
    const std::shared_ptr<AsyncFlightDataListener>& listener) {
  if (listener == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(listener->transport_mutex_);
  listener->transport_ = nullptr;
  if (listener->writer_ != nullptr) {
    std::lock_guard<std::mutex> writer_lock(listener->writer_->writer_mutex_);
    listener->writer_->transport_ = nullptr;
  }
}

Status FlightDataListenerTransport::ReportFinish(
    const std::shared_ptr<AsyncFlightDataListener>& listener, Status status) {
  if (listener == nullptr) {
    return Status::OK();
  }
  // Cancel() holds the listener's transport lock across its call into the transport, and
  // the ending it triggers reports from inside that call.
  bool expected = false;
  if (!listener->finished_.compare_exchange_strong(expected, true)) {
    return Status::OK();
  }
  // OnFinish is application code, it may call back into the listener (as
  // Cancel() does), which is why nothing is held here.
  return listener->OnFinish(std::move(status)).status();
}

}  // namespace internal

Future<> AsyncFlightDataListener::Cancel(Status status) {
  std::lock_guard<std::mutex> lock(transport_mutex_);
  if (transport_ == nullptr) {
    return arrow::Future<>::MakeFinished(Status::Invalid(
        "no upload in flight to cancel: Cancel() must be called while the "
        "upload's RPC is running"));
  }
  transport_->CancelUpload(std::move(status));
  return arrow::Future<>::MakeFinished(Status::OK());
}

}  // namespace arrow::flight
