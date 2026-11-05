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

#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"

#include "arrow/flight/transport/grpc/customize_grpc.h"

#include <memory>
#include <mutex>
#include <utility>

namespace arrow::flight::transport::grpc::detail {

namespace {

/// Serve one DoPut RPC over the generic callback API.
class DoPutReactor : public AsyncReactorBase,
                     public arrow::flight::internal::FlightDataListenerTransport {
 public:
  DoPutReactor(AsyncCallContext flight_context,
               std::shared_ptr<AsyncFlightDataListener> listener)
      : AsyncReactorBase(std::move(flight_context)),
        listener_(std::move(listener)),
        decoder_(listener_) {
    arrow::flight::internal::FlightDataListenerTransport::Install(listener_, this);
    StartRead(&request_buf_);
  }

  void Hold() override { AsyncReactorBase::Hold(); }

  void ReleaseHold() override { AsyncReactorBase::ReleaseHold(); }

  void CancelUpload(Status status) override {
    if (status.ok()) {
      status = Status::Invalid("Cancel() needs a non-OK status");
    }
    AsyncReactorBase::Hold();
    FinishUpload(std::move(status));
    AsyncReactorBase::ReleaseHold();
  }

  Status WriteMetadata(const Buffer& app_metadata) override {
    std::lock_guard<std::mutex> lock(write_mutex());
    if (finished()) {
      return Status::Invalid(
          "the upload is over: WriteMetadata() is only usable while the "
          "upload's RPC is running");
    }
    if (metadata_in_flight_ || ending_) {
      // One message in flight at a time: OnWriteDone serializes the PutResults.
      return Status::Invalid(
          "a PutResult message is already in flight: await it before writing "
          "another metadata message");
    }
    metadata_in_flight_ = true;
    pb::PutResult pb_result;
    if (app_metadata.size() > 0) {
      pb_result.set_app_metadata(app_metadata.data(), app_metadata.size());
    }
    write_buf_ = MakeWriteBuffer(pb_result);
    StartWrite(&write_buf_);
    return Status::OK();
  }

  void OnReadDone(bool ok) override {
    if (!ok) {
      // Decide under the lock, act outside it: FinishUpload() runs the
      // listener's OnFinish(), which is application code and may itself call
      // WriteMetadata(). That takes writer_mutex_ and then write_mutex_, the
      // reverse of a caller holding write_mutex_ and waiting for writer_mutex_,
      // so invoking it here would deadlock the two threads.
      {
        std::lock_guard<std::mutex> lock(write_mutex());
        if (finished()) {
          return;
        }
        if (read_descriptor_) {
          // The client half-closed: this ack ends the upload.  It takes the
          // same write slot as a mid-upload WriteMetadata, which may be on the
          // wire right now: starting a second write would clobber write_buf_
          // and the RPC would never see its OnWriteDone.
          ending_ = true;
          if (metadata_in_flight_) {
            return;  // OnWriteDone sends the ending ack once the slot frees up
          }
          StartEndingAck();
          return;
        }
      }
      FinishUpload(
          MakeFlightError(FlightStatusCode::Failed,
                          "Client sent an empty upload: the first message must carry the "
                          "descriptor"));
      return;
    }
    std::shared_ptr<arrow::Buffer> arrow_buf;
    const Status wrap_status = WrapGrpcBuffer(&request_buf_, &arrow_buf);
    if (!wrap_status.ok()) {
      FinishUpload(
          MakeFlightError(FlightStatusCode::Internal,
                          "Failed to wrap gRPC buffer: " + wrap_status.message()));
      return;
    }

    if (!read_descriptor_) {
      // The first message must carry the descriptor of the upload, exactly as
      // the sync transport requires.
      // Without this the listener never hears OnDescriptor and the upload is
      // acknowledged as if it were well formed.
      read_descriptor_ = true;
      internal::FlightData data;
      const ::grpc::Status deserialized = FlightDataDeserialize(&request_buf_, &data);
      if (!deserialized.ok()) {
        FinishUpload(
            MakeFlightError(FlightStatusCode::Internal, deserialized.error_message()));
        return;
      }
      if (!data.descriptor) {
        FinishUpload(Status::IOError("Descriptor missing on first message"));
        return;
      }
    }

    Future<> decode_status = decoder_.Consume(std::move(arrow_buf));
    decode_status.AddCallback([this, token = hold()](arrow::Status status) {
      if (finished()) {
        // The RPC is over (cancelled while the listener was working): the
        // message is abandoned.
        return;
      }
      if (!status.ok()) {
        FinishUpload(std::move(status));
        return;
      }
      // Arm the next read through WriteIfLive(): this continuation runs on the
      // decoder's thread, so a cancellation can finish the RPC right after the
      // finished() check above. WriteIfLive re-checks under write_mutex_, which
      // FinishOnce also holds, so the read is never started on a dead call.
      WriteIfLive([this] { StartRead(&request_buf_); });
    });
  }

  void OnWriteDone(bool ok) override {
    // Decide under the lock, act outside it: FinishUpload() runs the listener's
    // OnFinish(), which is application code and may call back into WriteMetadata().
    Status finish_status;
    bool ended = false;
    {
      std::lock_guard<std::mutex> lock(write_mutex());
      if (finished()) {
        return;
      }
      if (!ok) {
        finish_status = MakeFlightError(FlightStatusCode::Internal, "Write failed");
        ended = true;
      } else if (ending_ack_in_flight_) {
        // The ending ack reached the client: that is the clean end of the upload.
        ended = true;
      } else {
        metadata_in_flight_ = false;
        if (ending_) {
          // The client half-closed while a metadata message was on the wire:
          // with the slot free, the ending ack is the next write.
          StartEndingAck();
        }
      }
    }
    if (ended) {
      FinishUpload(std::move(finish_status));
    }
  }

  void OnCancel() override {
    // The RPC is cancelled: report it to the listener but do not let a failing
    // OnFinish replace the cancellation the client is seeing. The report is
    // fire-and-forget here; keeping a reference alive for it is unnecessary as
    // FinishOnce() below releases the reactor's hold and the callback only
    // touches immutable state.
    const auto status = Status::Cancelled("the client cancelled the upload");
    static_cast<void>(arrow::flight::internal::FlightDataListenerTransport::ReportFinish(
        listener_, status));
    FinishOnce(status);
  }

  void OnDone() override {
    arrow::flight::internal::FlightDataListenerTransport::Clear(listener_);
    ReleaseHold();
  }

 private:
  /// Send the empty ack that ends the upload.  Caller holds write_mutex_.
  void StartEndingAck() {
    ending_ack_in_flight_ = true;
    pb::PutResult pb_result;
    write_buf_ = MakeWriteBuffer(pb_result);
    StartWrite(&write_buf_);
  }

  void FinishUpload(Status status) {
    // Chain the RPC's completion on the listener's OnFinish() future instead of
    // waiting for it: FinishOnce() must not run on a gRPC callback thread while
    // the application is still committing, and a listener that fails
    // asynchronously must not make the client see success. Keep the reactor
    // alive across the wait.
    Status reported = status;
    arrow::flight::internal::FlightDataListenerTransport::ReportFinish(listener_,
                                                                       std::move(status))
        .AddCallback([this, token = hold(),
                      reported = std::move(reported)](const Status& finish_status) {
          // A failing OnFinish overrides a successful upload, but must not mask
          // a status the upload already failed with.
          FinishOnce(reported.ok() ? finish_status : reported);
        });
  }

  std::shared_ptr<AsyncFlightDataListener> listener_;
  AsyncFlightMessageDecoder decoder_;
  /// Whether the descriptor of the first message has been checked.
  bool read_descriptor_ = false;
  // The write side is serialized by AsyncReactorBase::write_mutex_, which
  // FinishOnce() also takes: WriteMetadata() runs on the listener's thread
  // while OnReadDone()/OnWriteDone() run on gRPC callback threads, and this
  // must be the same mutex the base uses or finishing can race a write.
  /// Whether a mid-upload metadata message is on the wire.
  bool metadata_in_flight_ = false;
  /// Whether the client half-closed, so the ending ack is owed.
  bool ending_ = false;
  /// Whether that ending ack is the write in flight.
  bool ending_ack_in_flight_ = false;
  ::grpc::ByteBuffer request_buf_;
  ::grpc::ByteBuffer write_buf_;
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeDoPutReactor(
    AsyncCallContext flight_context, std::shared_ptr<AsyncFlightDataListener> listener) {
  return new DoPutReactor(std::move(flight_context), std::move(listener));
}

}  // namespace arrow::flight::transport::grpc::detail
