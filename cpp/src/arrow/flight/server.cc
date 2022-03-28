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

#ifdef _WIN32
#include "arrow/util/windows_compatibility.h"

#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <utility>

#include "arrow/device.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/grpc_server.h"
#include "arrow/flight/transport_server.h"
#include "arrow/flight/types.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace {
#if (ATOMIC_INT_LOCK_FREE != 2 || ATOMIC_POINTER_LOCK_FREE != 2)
#error "atomic ints and atomic pointers not always lock-free!"
#endif

using ::arrow::internal::SetSignalHandler;
using ::arrow::internal::SignalHandler;

#ifdef WIN32
#define PIPE_WRITE _write
#define PIPE_READ _read
#else
#define PIPE_WRITE write
#define PIPE_READ read
#endif

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
  arrow::Result<int> Init(Fn handler) {
    ARROW_ASSIGN_OR_RAISE(auto pipe, arrow::internal::CreatePipe());
#ifndef WIN32
    // Make write end nonblocking
    int flags = fcntl(pipe.wfd, F_GETFL);
    if (flags == -1) {
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.rfd));
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.wfd));
      return arrow::internal::IOErrorFromErrno(
          errno, "Could not initialize self-pipe to wait for signals");
    }
    flags |= O_NONBLOCK;
    if (fcntl(pipe.wfd, F_SETFL, flags) == -1) {
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.rfd));
      RETURN_NOT_OK(arrow::internal::FileClose(pipe.wfd));
      return arrow::internal::IOErrorFromErrno(
          errno, "Could not initialize self-pipe to wait for signals");
    }
#endif
    self_pipe_ = pipe;
    handle_signals_ = std::thread(handler, self_pipe_.rfd);
    return self_pipe_.wfd;
  }

  Status Shutdown() {
    if (self_pipe_.rfd == 0) {
      // Already closed
      return Status::OK();
    }
    if (PIPE_WRITE(self_pipe_.wfd, "0", 1) < 0 && errno != EAGAIN &&
        errno != EWOULDBLOCK && errno != EINTR) {
      return arrow::internal::IOErrorFromErrno(errno, "Could not unblock signal thread");
    }
    handle_signals_.join();
    RETURN_NOT_OK(arrow::internal::FileClose(self_pipe_.rfd));
    RETURN_NOT_OK(arrow::internal::FileClose(self_pipe_.wfd));
    self_pipe_.rfd = 0;
    self_pipe_.wfd = 0;
    return Status::OK();
  }

  ~ServerSignalHandler() { ARROW_CHECK_OK(Shutdown()); }

 private:
  arrow::internal::Pipe self_pipe_;
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
  int self_pipe_wfd_;

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
    int saved_errno = errno;
    // Ignore errors - pipe is nonblocking
    PIPE_WRITE(self_pipe_wfd_, "0", 1);
    errno = saved_errno;
  }

  static void WaitForSignals(int fd) {
    // Wait for a signal handler to write to the pipe
    int8_t buf[1];
    while (PIPE_READ(fd, /*buf=*/buf, /*count=*/1) == -1) {
      if (errno == EINTR) {
        continue;
      }
      ARROW_CHECK_OK(arrow::internal::IOErrorFromErrno(
          errno, "Error while waiting for shutdown signal"));
    }
    auto instance = running_instance_.load();
    if (instance != nullptr) {
      auto status = instance->transport_->Shutdown();
      if (!status.ok()) {
        ARROW_LOG(WARNING) << "Error shutting down server: " << status.ToString();
      }
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
  ARROW_ASSIGN_OR_RAISE(impl_->self_pipe_wfd_,
                        signal_handler.Init(&Impl::WaitForSignals));
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
  // Stages of the stream when producing payloads
  enum class Stage {
    NEW,          // The stream has been created, but Next has not been called yet
    DICTIONARY,   // Dictionaries have been collected, and are being sent
    RECORD_BATCH  // Initial have been sent
  };

  RecordBatchStreamImpl(const std::shared_ptr<RecordBatchReader>& reader,
                        const ipc::IpcWriteOptions& options)
      : reader_(reader), mapper_(*reader_->schema()), ipc_options_(options) {}

  std::shared_ptr<Schema> schema() { return reader_->schema(); }

  Status GetSchemaPayload(FlightPayload* payload) {
    return ipc::GetSchemaPayload(*reader_->schema(), ipc_options_, mapper_,
                                 &payload->ipc_message);
  }

  Status Next(FlightPayload* payload) {
    if (stage_ == Stage::NEW) {
      RETURN_NOT_OK(reader_->ReadNext(&current_batch_));
      if (!current_batch_) {
        // Signal that iteration is over
        payload->ipc_message.metadata = nullptr;
        return Status::OK();
      }
      ARROW_ASSIGN_OR_RAISE(dictionaries_,
                            ipc::CollectDictionaries(*current_batch_, mapper_));
      stage_ = Stage::DICTIONARY;
    }

    if (stage_ == Stage::DICTIONARY) {
      if (dictionary_index_ == static_cast<int>(dictionaries_.size())) {
        stage_ = Stage::RECORD_BATCH;
        return ipc::GetRecordBatchPayload(*current_batch_, ipc_options_,
                                          &payload->ipc_message);
      } else {
        return GetNextDictionary(payload);
      }
    }

    RETURN_NOT_OK(reader_->ReadNext(&current_batch_));

    // TODO(ARROW-10787): Delta dictionaries
    if (!current_batch_) {
      // Signal that iteration is over
      payload->ipc_message.metadata = nullptr;
      return Status::OK();
    } else {
      return ipc::GetRecordBatchPayload(*current_batch_, ipc_options_,
                                        &payload->ipc_message);
    }
  }

 private:
  Status GetNextDictionary(FlightPayload* payload) {
    const auto& it = dictionaries_[dictionary_index_++];
    return ipc::GetDictionaryPayload(it.first, it.second, ipc_options_,
                                     &payload->ipc_message);
  }

  Stage stage_ = Stage::NEW;
  std::shared_ptr<RecordBatchReader> reader_;
  ipc::DictionaryFieldMapper mapper_;
  ipc::IpcWriteOptions ipc_options_;
  std::shared_ptr<RecordBatch> current_batch_;
  std::vector<std::pair<int64_t, std::shared_ptr<Array>>> dictionaries_;

  // Index of next dictionary to send
  int dictionary_index_ = 0;
};

FlightMetadataWriter::~FlightMetadataWriter() = default;

FlightDataStream::~FlightDataStream() {}

RecordBatchStream::RecordBatchStream(const std::shared_ptr<RecordBatchReader>& reader,
                                     const ipc::IpcWriteOptions& options) {
  impl_.reset(new RecordBatchStreamImpl(reader, options));
}

RecordBatchStream::~RecordBatchStream() {}

std::shared_ptr<Schema> RecordBatchStream::schema() { return impl_->schema(); }

Status RecordBatchStream::GetSchemaPayload(FlightPayload* payload) {
  return impl_->GetSchemaPayload(payload);
}

arrow::Result<FlightPayload> RecordBatchStream::Next() {
  FlightPayload payload;
  RETURN_NOT_OK(impl_->Next(&payload));
  return payload;
}

Status RecordBatchStream::Next(FlightPayload* payload) { return Next().Value(payload); }

}  // namespace flight
}  // namespace arrow
