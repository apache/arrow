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
#include <utility>

#include "arrow/result.h"
#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"

namespace arrow {

#if ATOMIC_INT_LOCK_FREE != 2
#error Lock-free atomic int required for signal safety
#endif

using internal::ReinstateSignalHandler;
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
    if (!saved_handlers_.empty()) {
      return Status::Invalid("Signal handlers already registered");
    }
    for (int signum : signals) {
      ARROW_ASSIGN_OR_RAISE(auto handler,
                            SetSignalHandler(signum, SignalHandler{&HandleSignal}));
      saved_handlers_.push_back({signum, handler});
    }
    return Status::OK();
  }

  void UnregisterHandlers() {
    auto handlers = std::move(saved_handlers_);
    for (const auto& h : handlers) {
      ARROW_CHECK_OK(SetSignalHandler(h.signum, h.handler).status());
    }
  }

  ~SignalStopState() {
    UnregisterHandlers();
    Disable();
  }

  StopSource* stop_source() { return stop_source_.get(); }

  bool enabled() { return stop_source_ != nullptr; }

  void Enable() {
    // Before creating a new StopSource, delete any lingering reference to
    // the previous one in the trash can.  See DoHandleSignal() for details.
    EmptyTrashCan();
    internal::atomic_store(&stop_source_, std::make_shared<StopSource>());
  }

  void Disable() { internal::atomic_store(&stop_source_, NullSource()); }

  static SignalStopState* instance() { return &instance_; }

 private:
  // For readability
  std::shared_ptr<StopSource> NullSource() { return nullptr; }

  void EmptyTrashCan() { internal::atomic_store(&trash_can_, NullSource()); }

  static void HandleSignal(int signum) { instance_.DoHandleSignal(signum); }

  void DoHandleSignal(int signum) {
    // async-signal-safe code only
    auto source = internal::atomic_load(&stop_source_);
    if (source) {
      source->RequestStopFromSignal(signum);
      // Disable() may have been called in the meantime, but we can't
      // deallocate a shared_ptr here, so instead move it to a "trash can".
      // This minimizes the possibility of running a deallocator here,
      // however it doesn't entirely preclude it.
      //
      // Possible case:
      // - a signal handler (A) starts running, fetches the current source
      // - Disable() then Enable() are called, emptying the trash can and
      //   replacing the current source
      // - a signal handler (B) starts running, fetches the current source
      // - signal handler A resumes, moves its source (the old source) into
      //   the trash can (the only remaining reference)
      // - signal handler B resumes, moves its source (the current source)
      //   into the trash can.  This triggers deallocation of the old source,
      //   since the trash can had the only remaining reference to it.
      //
      // This case should be sufficiently unlikely, but we cannot entirely
      // rule it out.  The problem might be solved properly with a lock-free
      // linked list of StopSources.
      internal::atomic_store(&trash_can_, std::move(source));
    }
    ReinstateSignalHandler(signum, &HandleSignal);
  }

  std::shared_ptr<StopSource> stop_source_;
  std::shared_ptr<StopSource> trash_can_;

  std::vector<SavedSignalHandler> saved_handlers_;

  static SignalStopState instance_;
};

SignalStopState SignalStopState::instance_{};

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
