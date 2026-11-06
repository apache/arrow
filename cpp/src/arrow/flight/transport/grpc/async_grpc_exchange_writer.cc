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

#include "arrow/flight/transport/grpc/async_grpc_exchange_internal.h"

namespace arrow::flight::transport::grpc::detail {

/// The IpcPayloadWriter that queues the IPC payloads the writer stack
/// produces, the schema, dictionary and record-batch messages all travel this
/// way, as on the synchronous path.
class ExchangeWriter::PayloadSink final : public ipc::internal::IpcPayloadWriter {
 public:
  explicit PayloadSink(ExchangeWriter* writer) : writer_(writer) {}

  arrow::Status Start() override { return arrow::Status::OK(); }

  arrow::Status WritePayload(const ipc::IpcPayload& ipc_payload) override {
    FlightPayload payload;
    payload.ipc_message = ipc_payload;
    if (ipc_payload.type == ipc::MessageType::RECORD_BATCH &&
        writer_->pending_app_metadata_ != nullptr) {
      payload.app_metadata = std::move(writer_->pending_app_metadata_);
    }
    writer_->queue_.push_back(std::move(payload));
    return arrow::Status::OK();
  }

  arrow::Status Close() override { return arrow::Status::OK(); }

 private:
  ExchangeWriter* writer_;
};

ExchangeWriter::ExchangeWriter(ExchangeReactor* reactor) : reactor_(reactor) {}

void ExchangeWriter::DetachReactor() { reactor_.store(nullptr); }

arrow::Future<> ExchangeWriter::BeginAsync(std::shared_ptr<Schema> schema) {
  if (reactor_.load() == nullptr) {
    return arrow::Future<>::MakeFinished(arrow::Status::Cancelled(
        "the exchange is over: the client disconnected or cancelled"));
  }
  if (batch_writer_ != nullptr) {
    return arrow::Future<>::MakeFinished(
        arrow::Status::Invalid("This writer has already been started."));
  }
  auto sink = std::make_unique<PayloadSink>(this);
  ARROW_ASSIGN_OR_RAISE(batch_writer_, ipc::internal::OpenRecordBatchWriter(
                                           std::move(sink), schema, options_));
  return Flush();
}

arrow::Future<> ExchangeWriter::WriteRecordBatchAsync(const RecordBatch& batch) {
  pending_app_metadata_ = nullptr;
  return WriteBatch(batch);
}

arrow::Future<> ExchangeWriter::WriteWithMetadataAsync(
    const RecordBatch& batch, std::shared_ptr<Buffer> app_metadata) {
  pending_app_metadata_ = std::move(app_metadata);
  return WriteBatch(batch);
}

arrow::Future<> ExchangeWriter::WriteMetadataAsync(std::shared_ptr<Buffer> app_metadata) {
  FlightPayload payload;
  payload.app_metadata = std::move(app_metadata);
  queue_.push_back(std::move(payload));
  return Flush();
}

arrow::Future<> ExchangeWriter::CloseAsync() {
  if (batch_writer_ != nullptr) {
    auto status = batch_writer_->Close();
    batch_writer_ = nullptr;
    if (!status.ok()) {
      return arrow::Future<>::MakeFinished(std::move(status));
    }
  }
  return arrow::Future<>::MakeFinished();
}

void ExchangeWriter::OnWriteDone(bool ok) {
  write_in_flight_ = false;
  if (!ok) {
    FailFlush(MakeFlightError(FlightStatusCode::Internal, "Failed to write response"));
    return;
  }
  Pump();
}

void ExchangeWriter::OnCancelled() {
  if (write_in_flight_) {
    write_in_flight_ = false;
    FailFlush(arrow::Status::Cancelled("the client cancelled the exchange"));
  }
}

arrow::Future<> ExchangeWriter::WriteBatch(const RecordBatch& batch) {
  if (batch_writer_ == nullptr) {
    return arrow::Future<>::MakeFinished(arrow::Status::Invalid(
        "This writer is not started. Call BeginAsync() with a schema"));
  }
  auto status = batch_writer_->WriteRecordBatch(batch);
  if (!status.ok()) {
    return arrow::Future<>::MakeFinished(std::move(status));
  }
  return Flush();
}

arrow::Future<> ExchangeWriter::Flush() {
  if (reactor_.load() == nullptr) {
    // The exchange is over: report it rather than pumping into a dead reactor.
    return arrow::Future<>::MakeFinished(arrow::Status::Cancelled(
        "the exchange is over: the client disconnected or cancelled"));
  }
  if (!flush_.is_valid() || flush_.is_finished()) {
    flush_ = arrow::Future<>::Make();
  }
  Pump();
  return flush_;
}

void ExchangeWriter::Pump() {
  if (write_in_flight_) {
    return;
  }
  if (queue_.empty()) {
    if (flush_.is_valid() && !flush_.is_finished()) {
      flush_.MarkFinished();
    }
    return;
  }
  ExchangeReactor* reactor = reactor_.load();
  if (reactor == nullptr) {
    write_in_flight_ = false;
    FailFlush(arrow::Status::Cancelled(
        "the exchange is over: the client disconnected or cancelled"));
    return;
  }
  FlightPayload payload = std::move(queue_.front());
  queue_.pop_front();
  write_in_flight_ = true;
  const auto status = reactor->WritePayload(std::move(payload));
  if (!status.ok()) {
    write_in_flight_ = false;
    FailFlush(std::move(status));
  }
}

void ExchangeWriter::FailFlush(Status status) {
  if (flush_.is_valid() && !flush_.is_finished()) {
    flush_.MarkFinished(std::move(status));
  }
}

}  // namespace arrow::flight::transport::grpc::detail
