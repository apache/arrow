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

/// The decoder's listener: chunks are handed to the reader, the descriptor and
/// the schema are consumed (the reactor sets the descriptor before the handler
/// starts).
class ExchangeReader::ChunkListener final : public AsyncFlightDataListener {
 public:
  explicit ChunkListener(ExchangeReader* reader) : reader_(reader) {}

  arrow::Future<> OnNext(FlightStreamChunk chunk) override {
    bool has_demand;
    {
      std::lock_guard<std::mutex> lock(reader_->mutex_);
      has_demand = reader_->pending_.is_valid() && !reader_->pending_.is_finished();
      if (!has_demand) {
        reader_->buffered_ = std::move(chunk);
      }
    }
    if (has_demand) {
      reader_->ResolvePending(std::move(chunk));
    }
    return Status::OK();
  }

 private:
  ExchangeReader* reader_;
};

ExchangeReader::ExchangeReader(ExchangeReactor* reactor)
    : reactor_(reactor),
      listener_(std::make_shared<ChunkListener>(this)),
      decoder_(listener_) {}

const FlightDescriptor& ExchangeReader::descriptor() const { return descriptor_; }

arrow::Future<FlightStreamChunk> ExchangeReader::NextAsync() {
  arrow::Future<FlightStreamChunk> returned;
  std::optional<internal::FlightData> first_read;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_.is_valid() && !pending_.is_finished()) {
      return Status::Invalid("one NextAsync at a time");
    }
    if (buffered_.has_value()) {
      FlightStreamChunk out = std::move(*buffered_);
      buffered_.reset();
      return out;
    }

    if (!read_error_.ok()) {
      return read_error_;
    }

    if (read_closed_ || reactor_ == nullptr) {
      // End of exchange: the reactor either saw the read side close or the RPC
      // is over (and the reactor is gone), so no further read can be started.
      return FlightStreamChunk{};
    }

    pending_ = arrow::Future<FlightStreamChunk>::Make();
    returned = pending_;
    first_read = std::exchange(first_read_, std::nullopt);
  }
  // Consume()/RequestRead() take the lock themselves and may resolve pending_
  // from a callback, so they must run unlocked.
  if (first_read.has_value()) {
    Consume(std::move(*first_read));
  } else {
    RequestRead();
  }
  return returned;
}

void ExchangeReader::DetachReactor() {
  std::lock_guard<std::mutex> lock(mutex_);
  reactor_ = nullptr;
}

void ExchangeReader::SetDescriptor(const FlightDescriptor& descriptor) {
  descriptor_ = descriptor;
}

void ExchangeReader::OfferFirstRead(internal::FlightData data) {
  // Set during reactor setup, but NextAsync() reads it under mutex_.
  std::lock_guard<std::mutex> lock(mutex_);
  first_read_ = std::move(data);
}

void ExchangeReader::OnMessage(internal::FlightData data) { Consume(std::move(data)); }

void ExchangeReader::OnReadClosed() {
  // A clean end of input still has to be a stream that could end: validate the
  // decoder before settling the reader, but never in place of an error the read
  // already reported.
  const Status eof = decoder_.Finish();
  Status error;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    read_in_flight_ = false;
    read_closed_ = true;
    if (!read_error_.ok()) {
      return;  // the saved error wins, and already reached the handler
    }
    if (!eof.ok()) {
      read_error_ = eof;
      error = eof;
    }
  }
  ResolvePending(error.ok() ? arrow::Result<FlightStreamChunk>(FlightStreamChunk{})
                            : arrow::Result<FlightStreamChunk>(error));
}

void ExchangeReader::OnReadFailed(Status status) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    read_in_flight_ = false;
    read_error_ = status;
  }
  ResolvePending(status);
}

void ExchangeReader::OnCancelled() {
  const auto status = arrow::Status::Cancelled("the client cancelled the exchange");
  {
    std::lock_guard<std::mutex> lock(mutex_);
    read_closed_ = true;
    if (read_error_.ok()) {
      // Report the cancellation to every later NextAsync(): a clean end of
      // exchange would tell the handler its input was complete.
      read_error_ = status;
    }
  }
  ResolvePending(status);
}

void ExchangeReader::ResolvePending(arrow::Result<FlightStreamChunk> result) {
  arrow::Future<FlightStreamChunk> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!pending_.is_valid() || pending_.is_finished()) {
      return;
    }
    pending = std::move(pending_);
    pending_ = arrow::Future<FlightStreamChunk>();
  }
  pending.MarkFinished(std::move(result));
}

void ExchangeReader::Consume(internal::FlightData data) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (read_closed_) {
      read_in_flight_ = false;
      return;
    }
  }
  auto self = shared_from_this();
  decoder_.Consume(std::move(data))
      .AddCallback([this, self](const arrow::Status& status) {
        bool has_demand = false;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          read_in_flight_ = false;
          if (!status.ok()) {
            if (read_error_.ok()) {
              read_error_ = status;
            }
          } else {
            has_demand = pending_.is_valid() && !pending_.is_finished();
          }
        }
        if (!status.ok()) {
          ResolvePending(status);
          return;
        }
        if (has_demand) {
          RequestRead();
        }
      });
}

void ExchangeReader::RequestRead() {
  ExchangeReactor* reactor = nullptr;
  AsyncReactorBase::HoldToken token;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (read_in_flight_ || read_closed_ || reactor_ == nullptr) {
      return;
    }
    reactor = reactor_;
    token = reactor->hold();
    read_in_flight_ = true;
  }
  reactor->StartReadNext();
}

}  // namespace arrow::flight::transport::grpc::detail
