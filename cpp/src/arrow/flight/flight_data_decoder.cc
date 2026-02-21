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

#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {

namespace {

// FlightDataMessageReader is an ipc::MessageReader that accepts one at a time
// FlightData messages. Analogous to MessageReader::Open(InputStream*) but for
// individual FlightData messages directly read from the received buffers.
class FlightDataMessageReader : public ipc::MessageReader {
 public:
  void Push(internal::FlightData data) { data_ = std::move(data); }

  ::arrow::Result<std::unique_ptr<ipc::Message>> ReadNextMessage() override {
    if (!data_.metadata) return nullptr;
    return data_.OpenMessage();
  }

  std::shared_ptr<Buffer> ReadAppMetadata() { return data_.app_metadata; }

 private:
  internal::FlightData data_;
};

}  // namespace

class FlightMessageDecoder::FlightMessageDecoderImpl {
 public:
  FlightMessageDecoderImpl(std::shared_ptr<FlightDataListener> listener,
                           ipc::IpcReadOptions options)
      : listener_(std::move(listener)),
        options_(std::move(options)),
        message_reader_(new FlightDataMessageReader()) {}

  Status Consume(std::shared_ptr<Buffer> buffer) {
    ARROW_ASSIGN_OR_RAISE(auto data, internal::DeserializeFlightData(buffer));

    if (!data.metadata) {
      // Metadata-only message: no IPC content, just Flight app_metadata.
      if (data.app_metadata && data.app_metadata->size() > 0) {
        FlightStreamChunk chunk;
        chunk.app_metadata = std::move(data.app_metadata);
        RETURN_NOT_OK(listener_->OnNext(std::move(chunk)));
      }
      return Status::OK();
    }

    message_reader_->Push(std::move(data));

    if (!batch_reader_) {
      // Initialize RecordBatchStreamReader and read the first IPC message.
      // It must be a schema.
      // RecordBatchStreamReader requiring unique_ptr is slightly awkward
      // since we want to keep a reference to the message reader.
      ARROW_ASSIGN_OR_RAISE(
          batch_reader_,
          ipc::RecordBatchStreamReader::Open(
              std::unique_ptr<ipc::MessageReader>(message_reader_), options_));
      return listener_->OnSchemaDecoded(batch_reader_->schema());
    }

    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(batch_reader_->ReadNext(&batch));
    auto app_metadata = message_reader_->ReadAppMetadata();

    if (batch) {
      FlightStreamChunk chunk;
      chunk.data = std::move(batch);
      chunk.app_metadata = std::move(app_metadata);
      return listener_->OnNext(std::move(chunk));
    }
    // This has to be a Dictionary batch.
    // TODO: Add unit test validating assumption.
    if (app_metadata && app_metadata->size() > 0) {
      FlightStreamChunk chunk;
      chunk.app_metadata = std::move(app_metadata);
      return listener_->OnNext(std::move(chunk));
    }
    return Status::OK();
  }

  std::shared_ptr<Schema> schema() const {
    return batch_reader_ ? batch_reader_->schema() : nullptr;
  }

 private:
  std::shared_ptr<FlightDataListener> listener_;
  ipc::IpcReadOptions options_;
  // This is owned by the RecordBatchStreamReader once it's passed to it.
  // We want to keep a reference to it so we can extract the app_metadata.
  FlightDataMessageReader* message_reader_;
  std::shared_ptr<ipc::RecordBatchStreamReader> batch_reader_;
};

FlightMessageDecoder::FlightMessageDecoder(std::shared_ptr<FlightDataListener> listener,
                                           ipc::IpcReadOptions options)
    : impl_(std::make_unique<FlightMessageDecoderImpl>(std::move(listener),
                                                       std::move(options))) {}

FlightMessageDecoder::~FlightMessageDecoder() = default;

Status FlightMessageDecoder::Consume(std::shared_ptr<Buffer> buffer) {
  return impl_->Consume(std::move(buffer));
}

std::shared_ptr<Schema> FlightMessageDecoder::schema() const { return impl_->schema(); }

}  // namespace flight
}  // namespace arrow
