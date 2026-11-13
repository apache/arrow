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

#include <atomic>
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
    arrow::flight::internal::FlightDataListenerTransport::SetTransport(listener_, this);
    StartRead(&request_buf_);
  }

  void Hold() override { AsyncReactorBase::Hold(); }

  void ReleaseHold() override { AsyncReactorBase::ReleaseHold(); }

  void CancelUpload(Status status) override {
    if (status.ok()) {
      status = Status::Invalid("Cancel() needs a non-OK status");
    }
    FinishUpload(std::move(status));
  }

  Future<> WriteMetadataAsync(const Buffer& app_metadata) override {
    std::lock_guard<std::mutex> lock(write_mutex());
    if (finished() || upload_ending_) {
      return Future<>::MakeFinished(Status::Invalid(
          "the upload is over: WriteMetadataAsync() is only usable while the "
          "upload's RPC is running"));
    }
    if (metadata_in_flight_ || ending_) {
      // One message in flight at a time: OnWriteDone serializes the PutResults.
      return Future<>::MakeFinished(Status::Invalid(
          "a PutResult message is already in flight: await the previous "
          "WriteMetadataAsync() future before writing another metadata message"));
    }
    metadata_in_flight_ = true;
    metadata_written_ = Future<>::Make();
    pb::PutResult pb_result;
    if (app_metadata.size() > 0) {
      pb_result.set_app_metadata(app_metadata.data(), app_metadata.size());
    }
    write_buf_ = MakeWriteBuffer(pb_result);
    StartWrite(&write_buf_);
    return metadata_written_;
  }

  void OnReadDone(bool ok) override {
    if (upload_ending_) {
      // The upload is ending (its OnFinish() was reported or is being
      // reported): nothing read from now on reaches the listener, and no
      // further read is armed.
      return;
    }
    if (!ok) {
      // Decide under the lock, act outside it: FinishUpload() runs the
      // listener's OnFinish(), which is application code and may itself call
      // WriteMetadataAsync(). That takes writer_mutex_ and then write_mutex_, the
      // reverse of a caller holding write_mutex_ and waiting for writer_mutex_,
      // so invoking it here would deadlock the two threads.
      {
        std::lock_guard<std::mutex> lock(write_mutex());
        if (finished()) {
          return;
        }
        if (read_descriptor_) {
          // The client half-closed: this ack ends the upload.  It takes the
          // same write slot as a mid-upload WriteMetadataAsync, which may be on the
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
    // Deserialize once: the descriptor check and the decoder share the result.
    auto data = arrow::flight::internal::DeserializeFlightData(arrow_buf);
    if (!data.ok()) {
      FinishUpload(data.status());
      return;
    }
    if (!read_descriptor_) {
      // The first message must carry the descriptor of the upload, exactly as
      // the sync transport requires.
      // Without this the listener never hears OnDescriptor and the upload is
      // acknowledged as if it were well formed.
      read_descriptor_ = true;
      if (!data->descriptor) {
        FinishUpload(Status::IOError("Descriptor missing on first message"));
        return;
      }
    }

    Future<> decode_status = decoder_.Consume(std::move(*data));
    decode_status.AddCallback([this, token = hold()](arrow::Status status) {
      if (upload_ending_) {
        // The upload is ending (cancelled, or ended while the listener was
        // working): the message is abandoned and no further read is armed.
        return;
      }
      if (!status.ok()) {
        FinishUpload(std::move(status));
        return;
      }
      // Arm the next read through WriteIfLive(): this continuation runs on the
      // decoder's thread, so the RPC can be finished right after the check
      // above. WriteIfLive re-checks under write_mutex_, which FinishOnce also
      // holds, so the read is never started on a dead call (and OnReadDone
      // drops what a read armed as the upload starts ending brings in).
      WriteIfLive([this] { StartRead(&request_buf_); });
    });
  }

  void OnWriteDone(bool ok) override {
    // Decide under the lock, act outside it: completing a WriteMetadataAsync()
    // future runs its continuation and FinishUpload() runs the listener's
    // OnFinish(); both are application code that may call WriteMetadataAsync().
    Status finish_status;
    bool ended = false;
    // The mid-upload message this write carried, unless it was the ending ack.
    Future<> written;
    {
      std::lock_guard<std::mutex> lock(write_mutex());
      if (!ending_ack_in_flight_) {
        written = std::move(metadata_written_);
        metadata_written_ = Future<>();
        metadata_in_flight_ = false;
      }
      if (!finished()) {
        if (!ok) {
          finish_status = MakeFlightError(FlightStatusCode::Internal, "Write failed");
          ended = true;
          // The call is broken: a continuation of the failed future must not
          // start another write before FinishOnce() runs.
          ending_ = true;
        } else if (ending_ack_in_flight_) {
          // The ending ack reached the client: that is the clean end of the upload.
          ended = true;
        } else if (ending_) {
          // The client half-closed while a metadata message was on the wire:
          // with the slot free, the ending ack is the next write.
          StartEndingAck();
        }
      }
    }
    if (written.is_valid()) {
      written.MarkFinished(
          ok ? Status::OK()
             : Status::IOError("the PutResult message was not written: "
                               "the upload's RPC failed or was cancelled"));
    }
    if (ended) {
      FinishUpload(std::move(finish_status));
    }
  }

  void TearDownOnCancel() override {
    // The RPC is cancelled: settle the status the client sees first, then tell
    // the listener unless another ending already did. OnFinish()'s future is
    // ignored: a failing OnFinish must not replace the cancellation.
    upload_ending_ = true;
    const auto status = Status::Cancelled("the client cancelled the upload");
    FinishOnce(status);
    static_cast<void>(arrow::flight::internal::FlightDataListenerTransport::ReportFinish(
        listener_, status));
  }

  void OnDone() override {
    arrow::flight::internal::FlightDataListenerTransport::SetTransport(listener_,
                                                                       nullptr);
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
    // Only the first ending reports to the listener and decides the RPC's
    // status: a later one (the client's half-close racing a server-side
    // Cancel()) must not finish the RPC with its own status while the first
    // one's OnFinish() is still running.
    if (upload_ending_.exchange(true)) {
      return;
    }
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
  /// Set by the first ending (FinishUpload() or the cancellation), before the
  /// listener hears OnFinish(): from then on nothing more is read, handed to
  /// the listener or written.
  std::atomic<bool> upload_ending_{false};
  // The write side is serialized by AsyncReactorBase::write_mutex_, which
  // FinishOnce() also takes: WriteMetadataAsync() runs on the listener's thread
  // while OnReadDone()/OnWriteDone() run on gRPC callback threads, and this
  // must be the same mutex the base uses or finishing can race a write.
  /// Whether a mid-upload metadata message is on the wire.
  bool metadata_in_flight_ = false;
  /// Completed by OnWriteDone() once that mid-upload message is written.
  Future<> metadata_written_;
  /// Whether the client half-closed, so the ending ack is owed.
  bool ending_ = false;
  /// Whether that ending ack is the write in flight.
  bool ending_ack_in_flight_ = false;
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeDoPutReactor(
    AsyncCallContext flight_context, std::shared_ptr<AsyncFlightDataListener> listener) {
  return new DoPutReactor(std::move(flight_context), std::move(listener));
}

}  // namespace arrow::flight::transport::grpc::detail
