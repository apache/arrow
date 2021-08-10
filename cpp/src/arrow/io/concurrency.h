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

#include <memory>

#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {
namespace internal {

template <class LockType>
class SharedLockGuard {
 public:
  explicit SharedLockGuard(LockType* lock) : lock_(lock) { lock_->LockShared(); }

  ~SharedLockGuard() { lock_->UnlockShared(); }

 protected:
  LockType* lock_;
};

template <class LockType>
class ExclusiveLockGuard {
 public:
  explicit ExclusiveLockGuard(LockType* lock) : lock_(lock) { lock_->LockExclusive(); }

  ~ExclusiveLockGuard() { lock_->UnlockExclusive(); }

 protected:
  LockType* lock_;
};

// Debug concurrency checker that marks "shared" and "exclusive" code sections,
// aborting if the concurrency rules get violated.  Does nothing in release mode.
// Note that we intentionally use the same class declaration in debug and
// release builds in order to avoid runtime failures when e.g. loading a
// release-built DLL with a debug-built application, or the reverse.

class ARROW_EXPORT SharedExclusiveChecker {
 public:
  SharedExclusiveChecker();
  void LockShared();
  void UnlockShared();
  void LockExclusive();
  void UnlockExclusive();

  using SharedGuard = SharedLockGuard<SharedExclusiveChecker>;
  using ExclusiveGuard = ExclusiveLockGuard<SharedExclusiveChecker>;

  // These guards can be constructed by IO classes to check the correctness of
  // concurrent calls to various methods.  It is not necessary to include this
  // in all IO classes, only a few core classes that get used in tests.

  // methods [Close, Abort, Tell, Read, Peek, Seek]
  //   require that no other methods be called concurrently
  ExclusiveGuard CloseGuard() { return ExclusiveGuard(this); }
  ExclusiveGuard AbortGuard() { return ExclusiveGuard(this); }
  ExclusiveGuard TellGuard() { return ExclusiveGuard(this); }
  ExclusiveGuard ReadGuard() { return ExclusiveGuard(this); }
  ExclusiveGuard PeekGuard() { return ExclusiveGuard(this); }
  ExclusiveGuard SeekGuard() { return ExclusiveGuard(this); }

  // methods [GetSize, ReadAt]
  //   can be called concurrently with one another
  SharedGuard GetSizeGuard() { return SharedGuard(this); }
  SharedGuard ReadAtGuard() { return SharedGuard(this); }

 protected:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace io
}  // namespace arrow
