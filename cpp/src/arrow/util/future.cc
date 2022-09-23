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

#include "arrow/util/future.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <numeric>

#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {

using internal::checked_cast;

class ConcreteFutureImpl : public FutureImpl {
 public:
  void DoMarkFinished() { DoMarkFinishedOrFailed(FutureState::SUCCESS); }

  void DoMarkFailed() { DoMarkFinishedOrFailed(FutureState::FAILURE); }

  void CheckOptions(const CallbackOptions& opts) {
    if (opts.should_schedule != ShouldSchedule::Never) {
      DCHECK_NE(opts.executor, nullptr)
          << "An executor must be specified when adding a callback that might schedule";
    }
  }

  void AddCallback(Callback callback, CallbackOptions opts) {
    CheckOptions(opts);
    std::unique_lock<std::mutex> lock(mutex_);
#ifdef ARROW_WITH_OPENTELEMETRY
    callback = [func = std::move(callback),
                active_span = ::arrow::internal::tracing::GetTracer()->GetCurrentSpan()](
                   const FutureImpl& impl) mutable {
      auto scope = ::arrow::internal::tracing::GetTracer()->WithActiveSpan(active_span);
      std::move(func)(impl);
    };
#endif
    CallbackRecord callback_record{std::move(callback), opts};
    if (IsFutureFinished(state_)) {
      lock.unlock();
      RunOrScheduleCallback(shared_from_this(), std::move(callback_record),
                            /*in_add_callback=*/true);
    } else {
      callbacks_.push_back(std::move(callback_record));
    }
  }

  bool TryAddCallback(const std::function<Callback()>& callback_factory,
                      CallbackOptions opts) {
    CheckOptions(opts);
    std::unique_lock<std::mutex> lock(mutex_);
    if (IsFutureFinished(state_)) {
      return false;
    } else {
      callbacks_.push_back({callback_factory(), opts});
      return true;
    }
  }

  static bool ShouldScheduleCallback(const CallbackRecord& callback_record,
                                     bool in_add_callback) {
    switch (callback_record.options.should_schedule) {
      case ShouldSchedule::Never:
        return false;
      case ShouldSchedule::Always:
        return true;
      case ShouldSchedule::IfUnfinished:
        return !in_add_callback;
      case ShouldSchedule::IfDifferentExecutor:
        return !callback_record.options.executor->OwnsThisThread();
      default:
        DCHECK(false) << "Unrecognized ShouldSchedule option";
        return false;
    }
  }

  static void RunOrScheduleCallback(const std::shared_ptr<FutureImpl>& self,
                                    CallbackRecord&& callback_record,
                                    bool in_add_callback) {
    if (ShouldScheduleCallback(callback_record, in_add_callback)) {
      // Need to keep `this` alive until the callback has a chance to be scheduled.
      auto task = [self, callback = std::move(callback_record.callback)]() mutable {
        return std::move(callback)(*self);
      };
      DCHECK_OK(callback_record.options.executor->Spawn(std::move(task)));
    } else {
      std::move(callback_record.callback)(*self);
    }
  }

  void DoMarkFinishedOrFailed(FutureState state) {
    std::vector<CallbackRecord> callbacks;
    std::shared_ptr<FutureImpl> self;
    {
      std::unique_lock<std::mutex> lock(mutex_);
#ifdef ARROW_WITH_OPENTELEMETRY
      if (this->span_) {
        util::tracing::Span& span = *span_;
        END_SPAN(span);
      }
#endif

      DCHECK(!IsFutureFinished(state_)) << "Future already marked finished";
      if (!callbacks_.empty()) {
        callbacks = std::move(callbacks_);
        auto self_inner = shared_from_this();
        self = std::move(self_inner);
      }

      state_ = state;
      // We need to notify while holding the lock.  This notify often triggers
      // waiters to delete the future and it is not safe to delete a cv_ while
      // it is performing a notify_all
      cv_.notify_all();
    }
    if (callbacks.empty()) return;

    // run callbacks, lock not needed since the future is finished by this
    // point so nothing else can modify the callbacks list and it is safe
    // to iterate.
    //
    // In fact, it is important not to hold the locks because the callback
    // may be slow or do its own locking on other resources
    for (auto& callback_record : callbacks) {
      RunOrScheduleCallback(self, std::move(callback_record), /*in_add_callback=*/false);
    }
  }

  void DoWait() {
    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait(lock, [this] { return IsFutureFinished(state_); });
  }

  bool DoWait(double seconds) {
    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait_for(lock, std::chrono::duration<double>(seconds),
                 [this] { return IsFutureFinished(state_); });
    return IsFutureFinished(state_);
  }

  std::mutex mutex_;
  std::condition_variable cv_;
};

namespace {

ConcreteFutureImpl* GetConcreteFuture(FutureImpl* future) {
  return checked_cast<ConcreteFutureImpl*>(future);
}

}  // namespace

std::unique_ptr<FutureImpl> FutureImpl::Make() {
  return std::unique_ptr<FutureImpl>(new ConcreteFutureImpl());
}

std::unique_ptr<FutureImpl> FutureImpl::MakeFinished(FutureState state) {
  std::unique_ptr<ConcreteFutureImpl> ptr(new ConcreteFutureImpl());
  ptr->state_ = state;
  return std::move(ptr);
}

FutureImpl::FutureImpl() : state_(FutureState::PENDING) {}

void FutureImpl::Wait() { GetConcreteFuture(this)->DoWait(); }

bool FutureImpl::Wait(double seconds) { return GetConcreteFuture(this)->DoWait(seconds); }

void FutureImpl::MarkFinished() { GetConcreteFuture(this)->DoMarkFinished(); }

void FutureImpl::MarkFailed() { GetConcreteFuture(this)->DoMarkFailed(); }

void FutureImpl::AddCallback(Callback callback, CallbackOptions opts) {
  GetConcreteFuture(this)->AddCallback(std::move(callback), opts);
}

bool FutureImpl::TryAddCallback(const std::function<Callback()>& callback_factory,
                                CallbackOptions opts) {
  return GetConcreteFuture(this)->TryAddCallback(callback_factory, opts);
}

Future<> AllComplete(const std::vector<Future<>>& futures) {
  struct State {
    explicit State(int64_t n_futures) : mutex(), n_remaining(n_futures) {}

    std::mutex mutex;
    std::atomic<size_t> n_remaining;
  };

  if (futures.empty()) {
    return Future<>::MakeFinished();
  }

  auto state = std::make_shared<State>(futures.size());
  auto out = Future<>::Make();
  for (const auto& future : futures) {
    future.AddCallback([state, out](const Status& status) mutable {
      if (!status.ok()) {
        std::unique_lock<std::mutex> lock(state->mutex);
        if (!out.is_finished()) {
          out.MarkFinished(status);
        }
        return;
      }
      if (state->n_remaining.fetch_sub(1) != 1) return;
      out.MarkFinished();
    });
  }
  return out;
}

Future<> AllFinished(const std::vector<Future<>>& futures) {
  return All(futures).Then([](const std::vector<Result<internal::Empty>>& results) {
    for (const auto& res : results) {
      if (!res.ok()) {
        return res.status();
      }
    }
    return Status::OK();
  });
}

}  // namespace arrow
