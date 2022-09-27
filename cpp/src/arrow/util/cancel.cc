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

#include "arrow/util/cancel.h"

#include <atomic>
#include <mutex>
#include <sstream>
#include <thread>
#include <utility>

#ifndef _WIN32
// For getpid()
#include <sys/types.h>
#include <unistd.h>
#endif

#include "arrow/result.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/mutex.h"
#include "arrow/util/visibility.h"

namespace arrow {

#if ATOMIC_INT_LOCK_FREE != 2
#error Lock-free atomic int required for signal safety
#endif

using internal::ReinstateSignalHandler;
using internal::SelfPipe;
using internal::SetSignalHandler;
using internal::SignalHandler;

// NOTE: We care mainly about the making the common case (not cancelled) fast.

struct StopSourceImpl {
  std::atomic<int> requested_{0};  // will be -1 or signal number if requested
  std::mutex mutex_;
  Status cancel_error_;
};

StopSource::StopSource() : impl_(new StopSourceImpl) {}

StopSource::~StopSource() = default;

void StopSource::RequestStop() { RequestStop(Status::Cancelled("Operation cancelled")); }

void StopSource::RequestStop(Status st) {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  DCHECK(!st.ok());
  if (!impl_->requested_) {
    impl_->requested_ = -1;
    impl_->cancel_error_ = std::move(st);
  }
}

void StopSource::RequestStopFromSignal(int signum) {
  // Only async-signal-safe code allowed here
  impl_->requested_.store(signum);
}

void StopSource::Reset() {
  std::lock_guard<std::mutex> lock(impl_->mutex_);
  impl_->cancel_error_ = Status::OK();
  impl_->requested_.store(0);
}

StopToken StopSource::token() { return StopToken(impl_); }

bool StopToken::IsStopRequested() const {
  if (!impl_) {
    return false;
  }
  return impl_->requested_.load() != 0;
}

Status StopToken::Poll() const {
  if (!impl_) {
    return Status::OK();
  }
  if (!impl_->requested_.load()) {
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(impl_->mutex_);
  if (impl_->cancel_error_.ok()) {
    auto signum = impl_->requested_.load();
    DCHECK_GT(signum, 0);
    impl_->cancel_error_ = internal::CancelledFromSignal(signum, "Operation cancelled");
  }
  return impl_->cancel_error_;
}

namespace {

struct SignalStopState {
  struct SavedSignalHandler {
    int signum;
    SignalHandler handler;
  };

  Status RegisterHandlers(const std::vector<int>& signals) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!saved_handlers_.empty()) {
      return Status::Invalid("Signal handlers already registered");
    }
    if (!self_pipe_) {
      // Make sure the self-pipe is initialized
      // (NOTE: avoid std::atomic_is_lock_free() which may require libatomic)
#if ATOMIC_POINTER_LOCK_FREE != 2
      return Status::NotImplemented(
          "Cannot setup signal StopSource because atomic pointers are not "
          "lock-free on this platform");
#else
      ARROW_ASSIGN_OR_RAISE(self_pipe_, SelfPipe::Make(/*signal_safe=*/true));
      // Spawn thread for receiving signals
      DCHECK(!signal_receiving_thread_);
      SpawnSignalReceivingThread();
#endif
    }
    self_pipe_ptr_.store(self_pipe_.get());
    for (int signum : signals) {
      ARROW_ASSIGN_OR_RAISE(auto handler,
                            SetSignalHandler(signum, SignalHandler{&HandleSignal}));
      saved_handlers_.push_back({signum, handler});
    }
    return Status::OK();
  }

  void UnregisterHandlers() {
    std::lock_guard<std::mutex> lock(mutex_);
    self_pipe_ptr_.store(nullptr);
    auto handlers = std::move(saved_handlers_);
    for (const auto& h : handlers) {
      ARROW_CHECK_OK(SetSignalHandler(h.signum, h.handler).status());
    }
  }

  ~SignalStopState() {
    UnregisterHandlers();
    Disable();
    if (signal_receiving_thread_) {
      if (InForkedChild()) {
        // std::thread is basically unusuable in a forked child, even the
        // destructor can crash.
        signal_receiving_thread_.release();
      } else {
        // Tell the receiving thread to stop
        auto st = self_pipe_->Shutdown();
        ARROW_WARN_NOT_OK(st, "Failed to shutdown self-pipe");
        if (st.ok()) {
          signal_receiving_thread_->join();
        } else {
          signal_receiving_thread_->detach();
        }
      }
    }
  }

  StopSource* stop_source() {
    std::lock_guard<std::mutex> lock(mutex_);
    return stop_source_.get();
  }

  bool enabled() {
    std::lock_guard<std::mutex> lock(mutex_);
    return stop_source_ != nullptr;
  }

  void Enable() {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_source_ = std::make_shared<StopSource>();
  }

  void Disable() {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_source_ = NullSource();
  }

  static const std::shared_ptr<SignalStopState>& instance() {
    static auto instance = std::make_shared<SignalStopState>();
#ifndef _WIN32
    if (instance->InForkedChild()) {
      // In child process
      auto lock = arrow::util::GlobalForkSafeMutex()->Lock();
      if (instance->pid_.load() != getpid()) {
        bool thread_was_launched{instance->signal_receiving_thread_};
        // Reinitialize internal structures after fork
        instance = std::make_shared<SignalStopState>();
        if (thread_was_launched) {
          instance->SpawnSignalReceivingThread();
        }
      }
    }
#endif
    return instance;
  }

 private:
  // For readability
  std::shared_ptr<StopSource> NullSource() { return nullptr; }

  void SpawnSignalReceivingThread() {
    signal_receiving_thread_ = std::make_unique<std::thread>(ReceiveSignals, self_pipe_);
  }

  static void HandleSignal(int signum) { instance()->DoHandleSignal(signum); }

  void DoHandleSignal(int signum) {
    // async-signal-safe code only
    SelfPipe* self_pipe = self_pipe_ptr_.load();
    if (self_pipe) {
      self_pipe->Send(/*payload=*/signum);
    }
    ReinstateSignalHandler(signum, &HandleSignal);
  }

  static void ReceiveSignals(std::shared_ptr<SelfPipe> self_pipe) {
    // Wait for signals on the self-pipe and propagate them to the current StopSource
    DCHECK(self_pipe);
    while (true) {
      auto maybe_payload = self_pipe->Wait();
      if (maybe_payload.status().IsInvalid()) {
        // Pipe shut down
        return;
      }
      if (!maybe_payload.ok()) {
        maybe_payload.status().Warn();
        return;
      }
      const int signum = static_cast<int>(maybe_payload.ValueUnsafe());
      instance()->ReceiveSignal(signum);
    }
  }

  void ReceiveSignal(int signum) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_source_) {
      stop_source_->RequestStopFromSignal(signum);
    }
  }

  bool InForkedChild() {
#ifndef _WIN32
    return pid_.load() != getpid();
#else
    return false;
#endif
  }

  std::mutex mutex_;
  std::vector<SavedSignalHandler> saved_handlers_;
  std::shared_ptr<StopSource> stop_source_;
  std::unique_ptr<std::thread> signal_receiving_thread_;

  // For signal handler interaction
  std::shared_ptr<SelfPipe> self_pipe_;
  // Raw atomic pointer, as atomic load/store of a shared_ptr may not be lock-free
  // (it is not on libstdc++).
  std::atomic<SelfPipe*> self_pipe_ptr_;

  // For fork safety
#ifndef _WIN32
  std::atomic<pid_t> pid_{getpid()};
#endif
};

}  // namespace

Result<StopSource*> SetSignalStopSource() {
  auto stop_state = SignalStopState::instance();
  if (stop_state->enabled()) {
    return Status::Invalid("Signal stop source already set up");
  }
  stop_state->Enable();
  return stop_state->stop_source();
}

void ResetSignalStopSource() {
  auto stop_state = SignalStopState::instance();
  DCHECK(stop_state->enabled());
  stop_state->Disable();
}

Status RegisterCancellingSignalHandler(const std::vector<int>& signals) {
  auto stop_state = SignalStopState::instance();
  if (!stop_state->enabled()) {
    return Status::Invalid("Signal stop source was not set up");
  }
  return stop_state->RegisterHandlers(signals);
}

void UnregisterCancellingSignalHandler() {
  auto stop_state = SignalStopState::instance();
  DCHECK(stop_state->enabled());
  stop_state->UnregisterHandlers();
}

}  // namespace arrow
