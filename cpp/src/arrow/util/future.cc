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

namespace arrow {

using internal::checked_cast;

// Shared mutex for all FutureWaiter instances.
// This simplifies lock management compared to a per-waiter mutex.
// The locking order is: global waiter mutex, then per-future mutex.
//
// It is unlikely that many waiter instances are alive at once, so this
// should ideally not limit scalability.
static std::mutex global_waiter_mutex;

const double FutureWaiter::kInfinity = HUGE_VAL;

class FutureWaiterImpl : public FutureWaiter {
 public:
  FutureWaiterImpl(Kind kind, std::vector<FutureImpl*> futures)
      : signalled_(false),
        kind_(kind),
        futures_(std::move(futures)),
        one_failed_(-1),
        fetch_pos_(0) {
    finished_futures_.reserve(futures_.size());

    // Observe the current state of futures and add waiters to receive future
    // state changes, atomically per future.
    // We need to lock ourselves, because as soon as SetWaiter() is called,
    // a FutureImpl may call MarkFutureFinished() from another thread
    // before this constructor finishes.
    std::unique_lock<std::mutex> lock(global_waiter_mutex);

    for (int i = 0; i < static_cast<int>(futures_.size()); ++i) {
      const auto state = futures_[i]->SetWaiter(this, i);
      if (IsFutureFinished(state)) {
        finished_futures_.push_back(i);
      }
      if (state != FutureState::SUCCESS) {
        one_failed_ = i;
      }
    }

    // Maybe signal the waiter, if the ending condition is already satisfied
    if (ShouldSignal()) {
      // No need to notify non-existent Wait() calls
      signalled_ = true;
    }
  }

  ~FutureWaiterImpl() override {
    for (auto future : futures_) {
      future->RemoveWaiter(this);
    }
  }

  // Is the ending condition satisfied?
  bool ShouldSignal() {
    bool do_signal = false;
    switch (kind_) {
      case ANY:
        do_signal = (finished_futures_.size() > 0);
        break;
      case ALL:
        do_signal = (finished_futures_.size() == futures_.size());
        break;
      case ALL_OR_FIRST_FAILED:
        do_signal = (finished_futures_.size() == futures_.size()) || one_failed_ >= 0;
        break;
      case ITERATE:
        do_signal = (finished_futures_.size() > static_cast<size_t>(fetch_pos_));
        break;
    }
    return do_signal;
  }

  void Signal() {
    signalled_ = true;
    cv_.notify_one();
  }

  void DoWaitUnlocked(std::unique_lock<std::mutex>* lock) {
    cv_.wait(*lock, [this] { return signalled_.load(); });
  }

  bool DoWait() {
    if (signalled_) {
      return true;
    }
    std::unique_lock<std::mutex> lock(global_waiter_mutex);
    DoWaitUnlocked(&lock);
    return true;
  }

  template <class Rep, class Period>
  bool DoWait(const std::chrono::duration<Rep, Period>& duration) {
    if (signalled_) {
      return true;
    }
    std::unique_lock<std::mutex> lock(global_waiter_mutex);
    cv_.wait_for(lock, duration, [this] { return signalled_.load(); });
    return signalled_.load();
  }

  void DoMarkFutureFinishedUnlocked(int future_num, FutureState state) {
    finished_futures_.push_back(future_num);
    if (state != FutureState::SUCCESS) {
      one_failed_ = future_num;
    }
    if (!signalled_ && ShouldSignal()) {
      Signal();
    }
  }

  int DoWaitAndFetchOne() {
    std::unique_lock<std::mutex> lock(global_waiter_mutex);

    DCHECK_EQ(kind_, ITERATE);
    DoWaitUnlocked(&lock);
    DCHECK_LT(static_cast<size_t>(fetch_pos_), finished_futures_.size());
    if (static_cast<size_t>(fetch_pos_) == finished_futures_.size() - 1) {
      signalled_ = false;
    }
    return finished_futures_[fetch_pos_++];
  }

  std::vector<int> DoMoveFinishedFutures() {
    std::unique_lock<std::mutex> lock(global_waiter_mutex);

    return std::move(finished_futures_);
  }

 protected:
  std::condition_variable cv_;
  std::atomic<bool> signalled_;

  Kind kind_;
  std::vector<FutureImpl*> futures_;
  std::vector<int> finished_futures_;
  int one_failed_;
  int fetch_pos_;
};

namespace {

FutureWaiterImpl* GetConcreteWaiter(FutureWaiter* waiter) {
  return checked_cast<FutureWaiterImpl*>(waiter);
}

}  // namespace

FutureWaiter::FutureWaiter() = default;

FutureWaiter::~FutureWaiter() = default;

std::unique_ptr<FutureWaiter> FutureWaiter::Make(Kind kind,
                                                 std::vector<FutureImpl*> futures) {
  return std::unique_ptr<FutureWaiter>(new FutureWaiterImpl(kind, std::move(futures)));
}

void FutureWaiter::MarkFutureFinishedUnlocked(int future_num, FutureState state) {
  // Called by FutureImpl on state changes
  GetConcreteWaiter(this)->DoMarkFutureFinishedUnlocked(future_num, state);
}

bool FutureWaiter::Wait(double seconds) {
  if (seconds == kInfinity) {
    return GetConcreteWaiter(this)->DoWait();
  } else {
    return GetConcreteWaiter(this)->DoWait(std::chrono::duration<double>(seconds));
  }
}

int FutureWaiter::WaitAndFetchOne() {
  return GetConcreteWaiter(this)->DoWaitAndFetchOne();
}

std::vector<int> FutureWaiter::MoveFinishedFutures() {
  return GetConcreteWaiter(this)->DoMoveFinishedFutures();
}

class ConcreteFutureImpl : public FutureImpl {
 public:
  FutureState DoSetWaiter(FutureWaiter* w, int future_num) {
    std::unique_lock<std::mutex> lock(mutex_);

    // Atomically load state at the time of adding the waiter, to avoid
    // missed or duplicate events in the caller
    ARROW_CHECK_EQ(waiter_, nullptr)
        << "Only one Waiter allowed per Future at any given time";
    waiter_ = w;
    waiter_arg_ = future_num;
    return state_.load();
  }

  void DoRemoveWaiter(FutureWaiter* w) {
    std::unique_lock<std::mutex> lock(mutex_);

    ARROW_CHECK_EQ(waiter_, w);
    waiter_ = nullptr;
  }

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
    CallbackRecord callback_record{std::move(callback), opts};
    if (IsFutureFinished(state_)) {
      lock.unlock();
      RunOrScheduleCallback(std::move(callback_record), /*in_add_callback=*/true);
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

  bool ShouldScheduleCallback(const CallbackRecord& callback_record,
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

  void RunOrScheduleCallback(CallbackRecord&& callback_record, bool in_add_callback) {
    if (ShouldScheduleCallback(callback_record, in_add_callback)) {
      struct CallbackTask {
        void operator()() { std::move(callback)(*self); }

        Callback callback;
        std::shared_ptr<FutureImpl> self;
      };
      // Need to keep `this` alive until the callback has a chance to be scheduled.
      CallbackTask task{std::move(callback_record.callback), shared_from_this()};
      DCHECK_OK(callback_record.options.executor->Spawn(std::move(task)));
    } else {
      std::move(callback_record.callback)(*this);
    }
  }

  void DoMarkFinishedOrFailed(FutureState state) {
    {
      // Lock the hypothetical waiter first, and the future after.
      // This matches the locking order done in FutureWaiter constructor.
      std::unique_lock<std::mutex> waiter_lock(global_waiter_mutex);
      std::unique_lock<std::mutex> lock(mutex_);

      DCHECK(!IsFutureFinished(state_)) << "Future already marked finished";
      state_ = state;
      if (waiter_ != nullptr) {
        waiter_->MarkFutureFinishedUnlocked(waiter_arg_, state);
      }
    }
    cv_.notify_all();

    // run callbacks, lock not needed since the future is finished by this
    // point so nothing else can modify the callbacks list and it is safe
    // to iterate.
    //
    // In fact, it is important not to hold the locks because the callback
    // may be slow or do its own locking on other resources
    for (auto& callback_record : callbacks_) {
      RunOrScheduleCallback(std::move(callback_record), /*in_add_callback=*/false);
    }
    callbacks_.clear();
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
  FutureWaiter* waiter_ = nullptr;
  int waiter_arg_ = -1;
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

FutureState FutureImpl::SetWaiter(FutureWaiter* w, int future_num) {
  return GetConcreteFuture(this)->DoSetWaiter(w, future_num);
}

void FutureImpl::RemoveWaiter(FutureWaiter* w) {
  GetConcreteFuture(this)->DoRemoveWaiter(w);
}

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

}  // namespace arrow
