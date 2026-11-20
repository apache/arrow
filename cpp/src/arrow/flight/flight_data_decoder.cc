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

#include <atomic>
#include <deque>
#include <memory>

#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow::flight {

namespace {

// The IPC messages handed to the decoder and not read yet, as an
// ipc::MessageReader: an empty queue reads as the end of the stream.
//
// Owned by the decoder, not by the RecordBatchStreamReader reading through it:
// Open() adopts the unique_ptr it is given and frees it on failure (a malformed
// first message, a first message that is not a schema), which would leave the
// decoder with a dangling pointer. The reader is handed a forwarding Ref instead.
class FlightDataMessageQueue final : public ipc::MessageReader {
 public:
  void Push(std::unique_ptr<ipc::Message> message) {
    queue_.push_back(std::move(message));
  }
  bool empty() const { return queue_.empty(); }

  arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (queue_.empty()) {
      return nullptr;
    }
    auto message = std::move(queue_.front());
    queue_.pop_front();
    return message;
  }

  class Ref final : public ipc::MessageReader {
   public:
    explicit Ref(FlightDataMessageQueue* queue) : queue_(queue) {}
    arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
      return queue_->ReadNextMessage();
    }

   private:
    FlightDataMessageQueue* queue_;
  };

 private:
  std::deque<std::unique_ptr<ipc::Message>> queue_;
};

}  // namespace

class AsyncFlightMessageDecoder::AsyncFlightMessageDecoderImpl {
 public:
  AsyncFlightMessageDecoderImpl(std::shared_ptr<AsyncFlightDataListener> listener,
                                ipc::IpcReadOptions options)
      : listener_(std::move(listener)), options_(std::move(options)) {}

  /// Stop delivering to the listener: the upload it decodes for has ended.
  void Stop() { stopped_ = true; }

  /// \brief Check that the IPC stream ended where it could end.
  ///
  /// Dictionaries still queued were never read: reading them now with nothing
  /// after them lets the IPC reader report an incomplete initial dictionary set,
  /// or a bad dictionary, exactly as it would on a synchronous stream.
  Status Finish() {
    if (stopped_ || messages_.empty()) {
      return Status::OK();
    }
    std::shared_ptr<RecordBatch> batch;
    return batch_reader_->ReadNext(&batch);
  }

  /// \brief Consume a chunk of Flight data.
  /// \param data The Flight data to consume.
  /// \return A future carrying the listener's status for this message: a
  /// non-OK result rejects the upload.
  /// The descriptor of an upload is reported before the rest of its first message
  /// is decoded.
  Future<> ConsumeData(internal::FlightData data) {
    if (stopped_) {
      // The upload ended while this message was in flight: it is dropped rather
      // than delivered, so no listener callback runs after OnFinish().
      return Status::OK();
    }
    if (data.descriptor && !descriptor_reported_) {
      descriptor_reported_ = true;
      return listener_->OnDescriptor(*data.descriptor)
          .Then([this, data = std::move(data)]() mutable {
            if (stopped_) {
              return arrow::Future<>{Status::OK()};
            }
            return ConsumeDataAfterDescriptor(std::move(data));
          });
    }
    return ConsumeDataAfterDescriptor(std::move(data));
  }

 private:
  /// \brief Hand a message's app metadata to the listener as a metadata-only
  /// chunk, unless it is empty.
  Future<> DeliverMetadata(std::shared_ptr<Buffer> app_metadata) {
    if (app_metadata == nullptr || app_metadata->size() == 0) {
      return Status::OK();
    }
    FlightStreamChunk chunk;
    chunk.app_metadata = std::move(app_metadata);
    return listener_->OnNext(std::move(chunk));
  }

  /// \brief Decode a FlightData message whose descriptor (if any) was already
  /// reported to the listener.
  Future<> ConsumeDataAfterDescriptor(internal::FlightData data) {
    if (!data.metadata) {
      // Metadata-only message: no IPC content, just Flight app_metadata.
      return DeliverMetadata(std::move(data.app_metadata));
    }
    ARROW_ASSIGN_OR_RAISE(auto message, data.OpenMessage());

    if (!batch_reader_) {
      // The first IPC message opens the stream, and must be its schema.
      messages_.Push(std::move(message));
      ARROW_ASSIGN_OR_RAISE(
          batch_reader_,
          ipc::RecordBatchStreamReader::Open(
              std::make_unique<FlightDataMessageQueue::Ref>(&messages_), options_));
      RETURN_NOT_OK(listener_->OnSchemaDecoded(batch_reader_->schema()));
      if (stopped_) {
        return Status::OK();
      }
      // OnSchemaDecoded() has no place for the schema message's app_metadata.
      return DeliverMetadata(std::move(data.app_metadata));
    }

    const bool is_dictionary = message->type() == ipc::MessageType::DICTIONARY_BATCH;
    messages_.Push(std::move(message));
    if (is_dictionary) {
      // Read with the batch that needs it. Reading a dictionary on its own makes
      // the pull reader ask for the next message from an empty queue, which it
      // takes for the end of the stream while initial dictionaries are still due.
      return DeliverMetadata(std::move(data.app_metadata));
    }
    // Reads the queued dictionaries, then this batch.
    FlightStreamChunk chunk;
    RETURN_NOT_OK(batch_reader_->ReadNext(&chunk.data));
    chunk.app_metadata = std::move(data.app_metadata);
    return listener_->OnNext(std::move(chunk));
  }

  /// Whether the upload ended: no further message reaches the listener.
  std::atomic<bool> stopped_{false};

  std::shared_ptr<AsyncFlightDataListener> listener_;
  ipc::IpcReadOptions options_;
  FlightDataMessageQueue messages_;
  std::shared_ptr<ipc::RecordBatchStreamReader> batch_reader_;
  /// Whether OnDescriptor() was already reported: only the first message of an
  /// upload carries one, later ones are ignored like the sync transport does.
  bool descriptor_reported_ = false;
};

AsyncFlightMessageDecoder::AsyncFlightMessageDecoder(
    std::shared_ptr<AsyncFlightDataListener> listener, ipc::IpcReadOptions options)
    : impl_(std::make_unique<AsyncFlightMessageDecoderImpl>(std::move(listener),
                                                            std::move(options))) {}

AsyncFlightMessageDecoder::~AsyncFlightMessageDecoder() = default;

Future<> AsyncFlightMessageDecoder::Consume(internal::FlightData data) {
  return impl_->ConsumeData(std::move(data));
}

void AsyncFlightMessageDecoder::Stop() { impl_->Stop(); }

Status AsyncFlightMessageDecoder::Finish() { return impl_->Finish(); }

// --- AsyncFlightDataListener: transport state and the terminal callbacks ---

AsyncFlightDataListener::AsyncFlightDataListener() = default;
// The deleting-destructor variants of these never run: the listener and the
// transport are abstract, and writers are only owned through shared_ptr. Their
// base variants are what the tests execute.
AsyncFlightDataListener::~AsyncFlightDataListener() = default;  // LCOV_EXCL_LINE
AsyncFlightDataWriter::~AsyncFlightDataWriter() = default;      // LCOV_EXCL_LINE

Future<> AsyncFlightDataWriter::WriteMetadataAsync(const Buffer& app_metadata) {
  // SetTransport() swaps transport_ under writer_mutex_, so both the read and
  // the call must happen under it: otherwise this can read the old pointer just
  // before DoPutReactor::OnDone() clears it, and then call through a
  // destructed reactor. The transport's own WriteMetadataAsync only takes its
  // private write_mutex_, so there is no lock-order cycle.
  std::lock_guard<std::mutex> lock(writer_mutex_);
  if (transport_ == nullptr) {
    return Status::Invalid(
        "no upload in flight to write metadata to: the writer is only usable "
        "while the upload's RPC is running");
  }
  return transport_->WriteMetadataAsync(app_metadata);
}

namespace internal {

FlightDataListenerTransport::~FlightDataListenerTransport() = default;  // LCOV_EXCL_LINE

void FlightDataListenerTransport::SetTransport(
    const std::shared_ptr<AsyncFlightDataListener>& listener,
    FlightDataListenerTransport* transport) {
  std::lock_guard<std::mutex> lock(listener->writer_->writer_mutex_);
  listener->writer_->transport_ = transport;
}

}  // namespace internal

Status AsyncFlightDataListener::Cancel(Status status) {
  internal::FlightDataListenerTransport* transport;
  {
    std::lock_guard<std::mutex> lock(writer_->writer_mutex_);
    transport = writer_->transport_;
    if (transport == nullptr) {
      return Status::Invalid(
          "no upload in flight to cancel: Cancel() must be called while the "
          "upload's RPC is running");
    }
    transport->Hold();
  }
  // Unlocked, so OnFinish() may re-enter Cancel() without deadlocking.
  transport->CancelUpload(std::move(status));
  transport->ReleaseHold();
  return Status::OK();
}

}  // namespace arrow::flight
