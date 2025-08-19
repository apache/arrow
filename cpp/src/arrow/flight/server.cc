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

// Flight server lifecycle implementation on top of the transport
// interface

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/server.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include <thread>
#include <utility>

#include "arrow/device.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/grpc_server.h"
#include "arrow/flight/transport_server.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {

namespace {
#if (ATOMIC_INT_LOCK_FREE != 2 || ATOMIC_POINTER_LOCK_FREE != 2)
#  error "atomic ints and atomic pointers not always lock-free!"
#endif

using ::arrow::internal::SelfPipe;
using ::arrow::internal::SetSignalHandler;
using ::arrow::internal::SignalHandler;

/// RAII guard that manages a self-pipe and a thread that listens on
/// the self-pipe, shutting down the server when a signal handler
/// writes to the pipe.
class ServerSignalHandler {
 public:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ServerSignalHandler);
  ServerSignalHandler() = default;

  /// Create the pipe and handler thread.
  ///
  /// \return the fd of the write side of the pipe.
  template <typename Fn>
  arrow::Result<std::shared_ptr<SelfPipe>> Init(Fn handler) {
    ARROW_ASSIGN_OR_RAISE(self_pipe_, SelfPipe::Make(/*signal_safe=*/true));
    handle_signals_ = std::thread(handler, self_pipe_);
    return self_pipe_;
  }

  Status Shutdown() {
    RETURN_NOT_OK(self_pipe_->Shutdown());
    handle_signals_.join();
    return Status::OK();
  }

  ~ServerSignalHandler() { ARROW_CHECK_OK(Shutdown()); }

 private:
  std::shared_ptr<SelfPipe> self_pipe_;
  std::thread handle_signals_;
};
}  // namespace

/// Server implementation. Manages the lifecycle of the "real" server
/// (ServerTransport) and contains
struct FlightServerBase::Impl {
  std::unique_ptr<internal::ServerTransport> transport_;

  // Signal handlers (on Windows) and the shutdown handler (other platforms)
  // are executed in a separate thread, so getting the current thread instance
  // wouldn't make sense.  This means only a single instance can receive signals.
  static std::atomic<Impl*> running_instance_;
  // We'll use the self-pipe trick to notify a thread from the signal
  // handler. The thread will then shut down the server.
  std::shared_ptr<SelfPipe> self_pipe_;

  // Signal handling
  std::vector<int> signals_;
  std::vector<SignalHandler> old_signal_handlers_;
  std::atomic<int> got_signal_;

  static void HandleSignal(int signum) {
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      instance->DoHandleSignal(signum);
    }
  }

  void DoHandleSignal(int signum) {
    got_signal_ = signum;

    // Send dummy payload over self-pipe
    self_pipe_->Send(/*payload=*/0);
  }

  static void WaitForSignals(std::shared_ptr<SelfPipe> self_pipe) {
    // Wait for a signal handler to wake up the pipe
    auto st = self_pipe->Wait().status();
    // Status::Invalid means the pipe was shutdown without any wakeup
    if (!st.ok() && !st.IsInvalid()) {
      ARROW_LOG(FATAL) << "Failed to wait on self-pipe: " << st.ToString();
    }
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      ARROW_WARN_NOT_OK(instance->transport_->Shutdown(), "Error shutting down server");
    }
  }
};

std::atomic<FlightServerBase::Impl*> FlightServerBase::Impl::running_instance_;

FlightServerOptions::FlightServerOptions(const Location& location_)
    : location(location_),
      auth_handler(nullptr),
      tls_certificates(),
      verify_client(false),
      root_certificates(),
      middleware(),
      memory_manager(CPUDevice::Instance()->default_memory_manager()),
      builder_hook(nullptr) {}

FlightServerOptions::~FlightServerOptions() = default;

FlightServerBase::FlightServerBase() { impl_.reset(new Impl); }

FlightServerBase::~FlightServerBase() {}

Status FlightServerBase::Init(const FlightServerOptions& options) {
  flight::transport::grpc::InitializeFlightGrpcServer();

  const auto scheme = options.location.scheme();
  ARROW_ASSIGN_OR_RAISE(impl_->transport_,
                        internal::GetDefaultTransportRegistry()->MakeServer(
                            scheme, this, options.memory_manager));
  return impl_->transport_->Init(options, *options.location.uri_);
}

int FlightServerBase::port() const { return location().uri_->port(); }

Location FlightServerBase::location() const { return impl_->transport_->location(); }

Status FlightServerBase::SetShutdownOnSignals(const std::vector<int> sigs) {
  impl_->signals_ = sigs;
  impl_->old_signal_handlers_.clear();
  return Status::OK();
}

Status FlightServerBase::Serve() {
  if (!impl_->transport_) {
    return Status::UnknownError("Server did not start properly");
  }
  impl_->got_signal_ = 0;
  impl_->old_signal_handlers_.clear();
  impl_->running_instance_ = impl_.get();

  ServerSignalHandler signal_handler;
  ARROW_ASSIGN_OR_RAISE(impl_->self_pipe_, signal_handler.Init(&Impl::WaitForSignals));
  // Override existing signal handlers with our own handler so as to stop the server.
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    int signum = impl_->signals_[i];
    SignalHandler new_handler(&Impl::HandleSignal), old_handler;
    ARROW_ASSIGN_OR_RAISE(old_handler, SetSignalHandler(signum, new_handler));
    impl_->old_signal_handlers_.push_back(std::move(old_handler));
  }

  RETURN_NOT_OK(impl_->transport_->Wait());
  impl_->running_instance_ = nullptr;

  // Restore signal handlers
  for (size_t i = 0; i < impl_->signals_.size(); ++i) {
    RETURN_NOT_OK(
        SetSignalHandler(impl_->signals_[i], impl_->old_signal_handlers_[i]).status());
  }
  return Status::OK();
}

int FlightServerBase::GotSignal() const { return impl_->got_signal_; }

Status FlightServerBase::Shutdown(const std::chrono::system_clock::time_point* deadline) {
  auto server = impl_->transport_.get();
  if (!server) {
    return Status::Invalid("Shutdown() on uninitialized FlightServerBase");
  }
  impl_->running_instance_ = nullptr;
  if (deadline) {
    return impl_->transport_->Shutdown(*deadline);
  }
  return impl_->transport_->Shutdown();
}

Status FlightServerBase::Wait() {
  RETURN_NOT_OK(impl_->transport_->Wait());
  impl_->running_instance_ = nullptr;
  return Status::OK();
}

Status FlightServerBase::ListFlights(const ServerCallContext& context,
                                     const Criteria* criteria,
                                     std::unique_ptr<FlightListing>* listings) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::GetFlightInfo(const ServerCallContext& context,
                                       const FlightDescriptor& request,
                                       std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::PollFlightInfo(const ServerCallContext& context,
                                        const FlightDescriptor& request,
                                        std::unique_ptr<PollInfo>* info) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                               std::unique_ptr<FlightDataStream>* data_stream) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoPut(const ServerCallContext& context,
                               std::unique_ptr<FlightMessageReader> reader,
                               std::unique_ptr<FlightMetadataWriter> writer) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoExchange(const ServerCallContext& context,
                                    std::unique_ptr<FlightMessageReader> reader,
                                    std::unique_ptr<FlightMessageWriter> writer) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::DoAction(const ServerCallContext& context, const Action& action,
                                  std::unique_ptr<ResultStream>* result) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::ListActions(const ServerCallContext& context,
                                     std::vector<ActionType>* actions) {
  return Status::NotImplemented("NYI");
}

Status FlightServerBase::GetSchema(const ServerCallContext& context,
                                   const FlightDescriptor& request,
                                   std::unique_ptr<SchemaResult>* schema) {
  return Status::NotImplemented("NYI");
}

// ----------------------------------------------------------------------
// Implement RecordBatchStream

class RecordBatchStream::RecordBatchStreamImpl {
 public:
  RecordBatchStreamImpl(const std::shared_ptr<RecordBatchReader>& reader,
                        const ipc::IpcWriteOptions& options)
      : reader_(reader), options_(options) {}

  std::shared_ptr<Schema> schema() { return reader_->schema(); }

  Status GetSchemaPayload(FlightPayload* payload) {
    if (!writer_) {
      // Create the IPC writer on first call
      auto payload_writer =
          std::make_unique<ServerRecordBatchPayloadWriter>(&payload_deque_);
      ARROW_ASSIGN_OR_RAISE(
          writer_, ipc::internal::OpenRecordBatchWriter(std::move(payload_writer),
                                                        reader_->schema(), options_));
    }

    // Return the expected schema payload.
    if (payload_deque_.empty()) {
      return Status::UnknownError("No schema payload generated");
    }
    *payload = std::move(payload_deque_.front());
    payload_deque_.pop_front();
    return Status::OK();
  }

  Status Next(FlightPayload* payload) {
    // If we have previous payloads (dictionary messages or previous record batches)
    // we will return them before reading the next record batch.
    if (payload_deque_.empty()) {
      std::shared_ptr<RecordBatch> batch;
      RETURN_NOT_OK(reader_->ReadNext(&batch));
      if (!batch) {
        // End of stream
        if (writer_) {
          RETURN_NOT_OK(writer_->Close());
        }
        payload->ipc_message.metadata = nullptr;
        return Status::OK();
      }
      if (!writer_) {
        return Status::UnknownError(
            "Writer should be initialized before reading Next batches");
      }
      // One WriteRecordBatch call might generate multiple payloads, so we
      // need to collect them in a deque.
      RETURN_NOT_OK(writer_->WriteRecordBatch(*batch));
    }

    // There must be at least one payload generated after WriteRecordBatch or
    // from previous calls to WriteRecordBatch.
    if (payload_deque_.empty()) {
      return Status::UnknownError("IPC writer didn't produce any payloads");
    }

    *payload = std::move(payload_deque_.front());
    payload_deque_.pop_front();
    return Status::OK();
  }

  Status Close() {
    if (writer_) {
      RETURN_NOT_OK(writer_->Close());
    }
    return reader_->Close();
  }

 private:
  // Simple payload writer that uses a deque to store generated payloads.
  class ServerRecordBatchPayloadWriter : public ipc::internal::IpcPayloadWriter {
   public:
    explicit ServerRecordBatchPayloadWriter(std::deque<FlightPayload>* payload_deque)
        : payload_deque_(payload_deque) {}

    Status Start() override { return Status::OK(); }

    Status WritePayload(const ipc::IpcPayload& ipc_payload) override {
      FlightPayload payload;
      payload.ipc_message = ipc_payload;

      payload_deque_->push_back(std::move(payload));
      return Status::OK();
    }

    Status Close() override { return Status::OK(); }

   private:
    std::deque<FlightPayload>* payload_deque_;
  };

  std::shared_ptr<RecordBatchReader> reader_;
  ipc::IpcWriteOptions options_;
  std::unique_ptr<ipc::RecordBatchWriter> writer_;
  std::deque<FlightPayload> payload_deque_;
};

FlightMetadataWriter::~FlightMetadataWriter() = default;

FlightDataStream::~FlightDataStream() {}
Status FlightDataStream::Close() { return Status::OK(); }

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                                     const ipc::IpcWriteOptions& options) {
  impl_.reset(new RecordBatchStreamImpl(reader, options));
}

RecordBatchStream::~RecordBatchStream() {
  ARROW_WARN_NOT_OK(impl_->Close(), "Failed to close FlightDataStream");
}

Status RecordBatchStream::Close() { return impl_->Close(); }

std::shared_ptr<Schema> RecordBatchStream::schema() { return impl_->schema(); }

arrow::Result<FlightPayload> RecordBatchStream::GetSchemaPayload() {
  FlightPayload payload;
  RETURN_NOT_OK(impl_->GetSchemaPayload(&payload));
  return payload;
}

arrow::Result<FlightPayload> RecordBatchStream::Next() {
  FlightPayload payload;
  RETURN_NOT_OK(impl_->Next(&payload));
  return payload;
}

}  // namespace flight
}  // namespace arrow
