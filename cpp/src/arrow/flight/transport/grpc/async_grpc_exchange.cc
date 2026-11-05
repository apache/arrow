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

ExchangeReactor::ExchangeReactor(AsyncCallContext flight_context,
                                 AsyncGenericFlightServerBase* base)
    : AsyncReactorBase(std::move(flight_context)),
      base_(base),
      reader_(std::make_shared<ExchangeReader>()),
      writer_(std::make_shared<ExchangeWriter>(this)) {
  reader_->SetReactor(this);
  // The first message carries the request descriptor.
  StartRead(&read_buf_);
}

void ExchangeReactor::OnReadDone(bool ok) {
  if (finished()) {
    return;
  }
  if (!ok) {
    if (!started_) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, "Failed to read request"));
      return;
    }
    reader_->OnReadClosed();
    return;
  }
  const ::grpc::Status deserialized = FlightDataDeserialize(&read_buf_, &read_data_);
  if (!deserialized.ok()) {
    if (!started_) {
      FinishOnce(MakeFlightError(FlightStatusCode::Internal, deserialized.error_message()));
      return;
    }
    reader_->OnReadFailed(
        MakeFlightError(FlightStatusCode::Internal, deserialized.error_message()));
    return;
  }
  if (!started_) {
    started_ = true;
    if (read_data_.descriptor == nullptr) {
      FinishOnce(
          MakeFlightError(FlightStatusCode::Internal, "Descriptor missing on first message"));
      return;
    }
    reader_->SetDescriptor(*read_data_.descriptor);
    // The first message may carry data,the reader consumes it on its first
    // NextAsync, so hand it over before the handler can ask.
    reader_->OfferFirstRead(std::move(read_data_));
    arrow::Future<> exchange =
        base_->DoExchangeAsync(flight_context(), reader_, writer_);
    exchange.AddCallback([this, token = hold()](const arrow::Status& status) {
      if (!finished()) {
        FinishOnce(status);
      }
    });
    return;
  }
  // A message read because the reader asked for one.
  reader_->OnMessage(std::move(read_data_));
}

void ExchangeReactor::OnWriteDone(bool ok) {
  if (finished()) {
    return;
  }
  writer_->OnWriteDone(ok);
}

void ExchangeReactor::OnCancel() {
  // Finish first (the handler's future may never resolve), then unwind the
  // reader and writer so a handler parked on their futures wakes up.
  FinishOnce(arrow::Status::Cancelled());
  reader_->OnCancelled();
  writer_->OnCancelled();
}

void ExchangeReactor::OnDone() {
  // The handler may still hold the reader/writer: cut their back-pointers
  // before this reactor is deleted so a late call fails cleanly instead of
  // dereferencing freed memory.
  reader_->DetachReactor();
  writer_->DetachReactor();
  AsyncReactorBase::OnDone();
}

void ExchangeReactor::StartReadNext() {
  if (!finished()) {
    StartRead(&read_buf_);
  }
}

arrow::Status ExchangeReactor::WritePayload(FlightPayload payload) {
  if (finished()) {
    return arrow::Status::Cancelled("the exchange is over");
  }
  bool own_buffer = false;
  const ::grpc::Status serialize =
      FlightDataSerialize(payload, &write_buf_, &own_buffer);
  (void)own_buffer;
  if (!serialize.ok()) {
    return MakeFlightError(FlightStatusCode::Internal, serialize.error_message());
  }
  StartWrite(&write_buf_);
  return arrow::Status::OK();
}

::grpc::ServerGenericBidiReactor* MakeExchangeReactor(
    AsyncCallContext flight_context, AsyncGenericFlightServerBase* base) {
  return new ExchangeReactor(std::move(flight_context), base);
}

}  // namespace arrow::flight::transport::grpc::detail
