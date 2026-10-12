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

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/flight/platform.h"
#include "arrow/flight/server.h"
#include "arrow/flight/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/uri.h"

namespace arrow::flight::internal {

class ServerDataStream;
class PeekableFlightDataReader;

/// \brief Adapt TransportDataStream to the FlightMessageReader
///   interface for DoPut.
class TransportMessageReader final : public FlightMessageReader {
 public:
  TransportMessageReader(ServerDataStream* stream,
                         std::shared_ptr<MemoryManager> memory_manager);

  Status Init();

  const FlightDescriptor& descriptor() const override;

  arrow::Result<std::shared_ptr<Schema>> GetSchema() override;

  arrow::Result<FlightStreamChunk> Next() override;

  ipc::ReadStats stats() const override;

 private:
  /// Ensure we are set up to read data.
  Status EnsureDataStarted();

  FlightDescriptor descriptor_;
  std::shared_ptr<PeekableFlightDataReader> peekable_reader_;
  std::shared_ptr<MemoryManager> memory_manager_;
  std::shared_ptr<ipc::RecordBatchStreamReader> batch_reader_;
  std::shared_ptr<Buffer> app_metadata_;
};

/// \brief A writer for IPC payloads to a ServerDataStream.
class TransportMessageWriter final : public FlightMessageWriter {
 public:
  explicit TransportMessageWriter(ServerDataStream* stream);

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override;

  Status WriteRecordBatch(const RecordBatch& batch) override;

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override;

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override;

  Status Close() override;

  ipc::WriteStats stats() const override;

 private:
  Status CheckStarted();

  ServerDataStream* stream_;
  std::unique_ptr<ipc::RecordBatchWriter> batch_writer_;
  std::shared_ptr<Buffer> app_metadata_;
  ::arrow::ipc::IpcWriteOptions ipc_options_;
};

/// \brief A writer for application-defined metadata to a ServerDataStream.
class TransportMetadataWriter final : public FlightMetadataWriter {
 public:
  explicit TransportMetadataWriter(ServerDataStream* stream);

  Status WriteMetadata(const Buffer& buffer) override;

 private:
  ServerDataStream* stream_;
};

/// \brief Write a FlightDataStream to a transport-level stream.
///
/// Used by DoGet and DoExchange to stream results back to the client.
/// The schema is written first, followed by record batches until the
/// stream is exhausted.
///
/// \param[in] data_stream The Arrow data stream to read from.
/// \param[in] stream The transport-specific data stream to write to.
Status WriteDataStream(std::unique_ptr<FlightDataStream> data_stream,
                       ServerDataStream* stream);

/// \brief Manages a background thread that waits for OS signals.
///
/// Creates a signal-safe SelfPipe and spawns a thread that blocks on it.
/// When a signal is received, the signal handler writes to the SelfPipe
/// to wake the thread. The destructor ensures the thread is joined.
class ServerSignalHandler {
 public:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ServerSignalHandler);
  ServerSignalHandler() = default;

  /// \brief Initialize the signal handler and start the background thread.
  ///
  /// \param[in] self_pipe The pipe the handler thread should wait on It is
  /// stored before the thread is started, so the signal handler can always
  /// write to it.
  /// \param[in] handler A callable that will be invoked on the background
  ///   thread, receiving the SelfPipe as an argument.
  template <typename Fn>
  Status Init(const std::shared_ptr<::arrow::internal::SelfPipe>& self_pipe,
              Fn&& handler) {
    self_pipe_ = self_pipe;
    handle_signals_ = std::thread(std::forward<Fn>(handler), self_pipe_);
    return Status::OK();
  }

  /// \brief Shut down the signal handler and join the background thread.
  ///
  /// The thread is woken with a plain payload rather than SelfPipe::Shutdown():
  /// a signal handler may have been entered and preempted just before this
  /// point, and writing to a pipe whose read end was closed raises SIGPIPE in
  /// that handler, killing the process.
  Status Shutdown() {
    self_pipe_->Send(/*payload=*/0);
    handle_signals_.join();
    return Status::OK();
  }

  ~ServerSignalHandler() { ARROW_CHECK_OK(Shutdown()); }

 private:
  std::shared_ptr<::arrow::internal::SelfPipe> self_pipe_;
  std::thread handle_signals_;
};

/// \brief Manages signal-driven graceful shutdown for server transports.
///
/// Installs signal handlers for the given set of signals. When a signal is
/// received, the registered shutdown callback is invoked. Uses a SelfPipe
/// to safely bridge the signal handler (which runs in a restricted context)
/// to a background thread that performs the actual shutdown.
class ARROW_FLIGHT_EXPORT ServerSignalState {
 public:
  ServerSignalState() = default;

  /// \brief Configure which signals should trigger shutdown.
  ///
  /// \param[in] sigs List of signal numbers (e.g. SIGINT, SIGTERM).
  Status SetShutdownOnSignals(const std::vector<int>& sigs);
  /// \brief Return the last signal that was received, or 0 if none.
  int GotSignal() const;

  /// \brief Run the server with signal-aware lifecycle management.
  ///
  /// Installs signal handlers, calls \p wait (which should block until the
  /// server stops), and restores original signal handlers on return.
  /// If a signal is received during \p wait, \p shutdown is called first.
  ///
  /// \param[in] wait A callable that blocks until the server stops.
  /// \param[in] shutdown A callable invoked on signal receipt to stop the server.
  /// \param[in] not_started_message Logged if shutdown is called before the server
  /// starts. \param[in] shutdown_warning Logged if shutdown itself fails.
  template <typename WaitFn, typename ShutdownFn>
  Status Serve(WaitFn&& wait, ShutdownFn&& shutdown, const char* not_started_message,
               const char* shutdown_warning) {
    got_signal_ = 0;
    old_signal_handlers_.clear();
    // Everything the signal handler touches must be initialized before the
    // instance is published: a signal arriving earlier would otherwise find an
    // empty self-pipe (crash) or an empty shutdown callback, in which case
    // WaitForSignals() consumes the wakeup and wait() blocks forever.
    ARROW_ASSIGN_OR_RAISE(self_pipe_,
                          ::arrow::internal::SelfPipe::Make(/*signal_safe=*/true));
    shutdown_ = [shutdown = std::forward<ShutdownFn>(shutdown)]() mutable {
      return shutdown(nullptr);
    };
    shutdown_warning_ = shutdown_warning;
    running_instance_ = this;

    Status status;
    {
      ServerSignalHandler signal_handler;
      status = signal_handler.Init(self_pipe_, &ServerSignalState::WaitForSignals);
      if (status.ok()) {
        for (size_t i = 0; i < signals_.size(); ++i) {
          int signum = signals_[i];
          ::arrow::internal::SignalHandler new_handler(&ServerSignalState::HandleSignal),
              old_handler;
          auto old = ::arrow::internal::SetSignalHandler(signum, new_handler);
          if (!old.ok()) {
            status = old.status();
            break;
          }
          old_signal_handlers_.push_back(std::move(old).ValueOrDie());
        }
      }
      if (status.ok()) {
        status = wait();
      }
      // Stop publishing this instance before the signal thread is joined: the
      // destructor shuts the self-pipe down, and a signal handler writing to a
      // closed pipe would kill the process with SIGPIPE.
      running_instance_ = nullptr;
      // The signal thread is joined by the destructor here, before the callback
      // is cleared below: it may be executing it right now.
    }

    shutdown_ = nullptr;
    shutdown_warning_ = nullptr;

    // Restore the original handlers, whatever the outcome of wait() was.
    Status restore_status;
    for (size_t i = 0; i < old_signal_handlers_.size(); ++i) {
      auto restored =
          ::arrow::internal::SetSignalHandler(signals_[i], old_signal_handlers_[i]);
      if (!restored.ok() && restore_status.ok()) {
        restore_status = restored.status();
      }
    }
    if (!status.ok()) {
      return status;
    }
    return restore_status;
  }

  /// \brief Shut down the server through the given callback.
  ///
  /// Clears the running instance pointer, then invokes \p shutdown.
  ///
  /// \param[in] shutdown A callable that performs the transport-specific shutdown.
  /// \param[in] deadline Optional deadline for the shutdown.
  template <typename ShutdownFn>
  Status Shutdown(ShutdownFn&& shutdown,
                  const std::chrono::system_clock::time_point* deadline) {
    running_instance_ = nullptr;
    return shutdown(deadline);
  }

  /// \brief Wait for the server to stop without initiating shutdown.
  ///
  /// \param[in] wait A callable that blocks until the server stops.
  template <typename WaitFn>
  Status Wait(WaitFn&& wait) {
    RETURN_NOT_OK(wait());
    running_instance_ = nullptr;
    return Status::OK();
  }

 private:
  /// \brief Static entry point for the OS signal handler.
  static void HandleSignal(int signum);

  /// \brief Record the received signal and wake the background thread.
  void DoHandleSignal(int signum);

  /// \brief Background thread that blocks on the SelfPipe, then invokes shutdown.
  static void WaitForSignals(std::shared_ptr<::arrow::internal::SelfPipe> self_pipe);

  // Signal handlers (on Windows) and the shutdown handler (other platforms)
  // are executed in a separate thread, so getting the current thread instance
  // wouldn't make sense.  This means only a single instance can receive signals.
  static std::atomic<ServerSignalState*> running_instance_;

  // We'll use the self-pipe trick to notify a thread from the signal
  // handler. The thread will then shut down the server.
  std::shared_ptr<::arrow::internal::SelfPipe> self_pipe_;

  // Signal handling
  std::vector<int> signals_;
  std::vector<::arrow::internal::SignalHandler> old_signal_handlers_;
  std::atomic<int> got_signal_{0};
  std::function<Status()> shutdown_;
  const char* shutdown_warning_ = nullptr;
};

}  // namespace arrow::flight::internal
