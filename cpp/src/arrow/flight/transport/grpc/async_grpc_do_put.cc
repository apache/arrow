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
    if (finished()) {
      return Status::Invalid(
          "the upload is over: WriteMetadataAsync() is only usable while the "
          "upload's RPC is running");
    }
    if (metadata_written_.is_valid()) {
      // One message in flight at a time: OnWriteDone serializes the PutResults.
      return Status::Invalid(
          "a PutResult message is already in flight: await the previous "
          "WriteMetadataAsync() future before writing another metadata message");
    }
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
      // The read side is gone: the client half-closed, or the call was
      // cancelled. Only a half-close is a clean end. Nothing is written back for
      // it: the listener answers with its own PutResults, from OnFinish() too.
      if (flight_context().is_cancelled()) {
        FinishUpload(Status::Cancelled("the call was cancelled"));
      } else if (!read_descriptor_) {
        FinishUpload(
            MakeFlightError(FlightStatusCode::Failed,
                            "Client sent an empty upload: the first message must carry "
                            "the descriptor"));
      } else {
        // A clean half-close still has to have ended where an IPC stream can
        // end: a dictionary stream that stopped mid-way must not be reported to
        // the listener as a complete upload.
        FinishUpload(decoder_.Finish());
      }
      return;
    }
    internal::FlightData data;
    const ::grpc::Status deserialized = FlightDataDeserialize(&request_buf_, &data);
    if (!deserialized.ok()) {
      FinishUpload(FromGrpcStatus(deserialized));
      return;
    }
    if (!read_descriptor_) {
      // The first message must carry the descriptor of the upload, exactly as
      // the sync transport requires.
      // Without this the listener never hears OnDescriptor and the upload is
      // acknowledged as if it were well formed.
      read_descriptor_ = true;
      if (!data.descriptor) {
        FinishUpload(Status::IOError("Descriptor missing on first message"));
        return;
      }
    }

    Future<> decode_status = decoder_.Consume(std::move(data));
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
    Future<> written;
    bool finish_pending = false;
    Status finish_status;
    {
      std::lock_guard<std::mutex> lock(write_mutex());
      written = std::exchange(metadata_written_, Future<>());
      finish_pending = std::exchange(finish_pending_, false);
      finish_status = std::exchange(pending_finish_status_, Status());
    }
    // Finish before completing the future: its continuation may write again.
    if (finish_pending) {
      // OnFinish() already reported; the RPC waited for its last message. A write
      // that failed still decides the RPC's status, but only when the upload was
      // otherwise clean: an earlier rejection or cancellation keeps its own.
      if (!ok && finish_status.ok()) {
        finish_status = MakeFlightError(FlightStatusCode::Internal, "Write failed");
      }
      FinishOnce(std::move(finish_status));
    } else if (!ok) {
      FinishUpload(MakeFlightError(FlightStatusCode::Internal, "Write failed"));
    }
    if (written.is_valid()) {
      written.MarkFinished(
          ok ? Status::OK()
             : Status::IOError("the PutResult message was not written: "
                               "the upload's RPC failed or was cancelled"));
    }
  }

  void TearDownOnCancel() override {
    // The RPC is cancelled: settle the status the client sees first, then tell
    // the listener unless another ending already did (upload_ending_ makes
    // OnFinish() run exactly once). OnFinish()'s future is ignored: a failing
    // OnFinish must not replace the cancellation, but the callback keeps the
    // listener alive while the application's work runs.
    const bool already_ending = upload_ending_.exchange(true);
    decoder_.Stop();
    const auto status = Status::Cancelled("the client cancelled the upload");
    FinishOnce(status);
    if (!already_ending) {
      listener_->OnFinish(status).AddCallback([listener = listener_](const Status&) {});
    }
  }

  void OnDone() override {
    arrow::flight::internal::FlightDataListenerTransport::SetTransport(listener_,
                                                                       nullptr);
    ReleaseHold();
  }

 private:
  void FinishUpload(Status status) {
    // Only the first ending reports to the listener and decides the RPC's
    // status: a later one (the client's half-close racing a server-side
    // Cancel()) must not finish the RPC with its own status while the first
    // one's OnFinish() is still running.
    if (upload_ending_.exchange(true)) {
      return;
    }
    // Nothing more reaches the listener, not even from a message whose
    // OnDescriptor() or OnNext() future is still pending.
    decoder_.Stop();
    // Chain the RPC's completion on the listener's OnFinish() future instead of
    // waiting for it: FinishOnce() must not run on a gRPC callback thread while
    // the application is still committing, and a listener that fails
    // asynchronously must not make the client see success. Keep the reactor
    // alive across the wait.
    Status reported = status;
    listener_->OnFinish(std::move(status))
        .AddCallback([this, token = hold(),
                      reported = std::move(reported)](const Status& finish_status) {
          // A failing OnFinish overrides a successful upload, but must not mask
          // a status the upload already failed with.
          Status final_status = reported.ok() ? finish_status : reported;
          {
            // A message OnFinish() wrote must reach the client before the
            // RPC's status: if it is still on the wire, OnWriteDone finishes.
            std::lock_guard<std::mutex> lock(write_mutex());
            if (metadata_written_.is_valid()) {
              finish_pending_ = true;
              pending_finish_status_ = std::move(final_status);
              return;
            }
          }
          FinishOnce(std::move(final_status));
        });
  }

  std::shared_ptr<AsyncFlightDataListener> listener_;
  AsyncFlightMessageDecoder decoder_;
  /// Whether the descriptor of the first message has been checked.
  bool read_descriptor_ = false;
  /// Set by the first ending (FinishUpload() or the cancellation), before the
  /// listener hears OnFinish(): from then on nothing more is read or handed to
  /// the listener.
  std::atomic<bool> upload_ending_{false};
  // The write side is serialized by AsyncReactorBase::write_mutex_, which
  // FinishOnce() also takes: WriteMetadataAsync() runs on the listener's thread
  // while OnReadDone()/OnWriteDone() run on gRPC callback threads, and this
  // must be the same mutex the base uses or finishing can race a write.
  /// The message on the wire, completed by OnWriteDone(); invalid when none is.
  Future<> metadata_written_;
  /// Whether the RPC waits for that message before finishing with
  /// pending_finish_status_ (OnFinish() returned while its write was in flight).
  bool finish_pending_ = false;
  Status pending_finish_status_;
};

}  // namespace

::grpc::ServerGenericBidiReactor* MakeDoPutReactor(
    AsyncCallContext flight_context, std::shared_ptr<AsyncFlightDataListener> listener) {
  return new DoPutReactor(std::move(flight_context), std::move(listener));
}

}  // namespace arrow::flight::transport::grpc::detail
