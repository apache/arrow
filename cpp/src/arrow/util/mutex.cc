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

#include "arrow/util/mutex.h"

#include <mutex>

#ifndef _WIN32
#include <pthread.h>
#include <atomic>
#endif

#include "arrow/util/config.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {

struct Mutex::Impl {
  std::mutex mutex_;
};

Mutex::Guard::Guard(Mutex* locked)
    : locked_(locked, [](Mutex* locked) {
#ifdef ARROW_ENABLE_THREADING
        DCHECK(!locked->impl_->mutex_.try_lock());
#endif
        locked->impl_->mutex_.unlock();
      }) {
}

Mutex::Guard Mutex::TryLock() {
  DCHECK_NE(impl_, nullptr);
  if (impl_->mutex_.try_lock()) {
    return Guard{this};
  }
  return Guard{};
}

Mutex::Guard Mutex::Lock() {
  DCHECK_NE(impl_, nullptr);
  impl_->mutex_.lock();
  return Guard{this};
}

Mutex::Mutex() : impl_(new Impl, [](Impl* impl) { delete impl; }) {}

#ifndef _WIN32
namespace {

struct AfterForkState {
  // A global instance that will also register the atfork handler when
  // constructed.
  static AfterForkState instance;

  // The mutex may be used at shutdown, so make it eternal.
  // The leak (only in child processes) is a small price to pay for robustness.
  Mutex* mutex = nullptr;

 private:
  AfterForkState() {
    pthread_atfork(/*prepare=*/nullptr, /*parent=*/nullptr, /*child=*/&AfterFork);
  }

  static void AfterFork() { instance.mutex = new Mutex; }
};

AfterForkState AfterForkState::instance;
}  // namespace

Mutex* GlobalForkSafeMutex() { return AfterForkState::instance.mutex; }
#endif  // _WIN32

}  // namespace util
}  // namespace arrow
