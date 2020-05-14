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

#include <functional>
#include <memory>
#include <string>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class StopToken;

// A RAII wrapper that automatically registers and unregisters
// a callback to a StopToken.
class ARROW_MUST_USE_TYPE ARROW_EXPORT StopCallback {
 public:
  using Callable = std::function<void(const Status&)>;
  StopCallback(StopToken* token, Callable cb);
  ~StopCallback();

  ARROW_DISALLOW_COPY_AND_ASSIGN(StopCallback);
  StopCallback(StopCallback&&);
  StopCallback& operator=(StopCallback&&);

  void Call(const Status&);

 protected:
  StopToken* token_;
  Callable cb_;
};

class ARROW_EXPORT StopToken {
 public:
  StopToken();
  ~StopToken();
  ARROW_DISALLOW_COPY_AND_ASSIGN(StopToken);

  // NOTE: all APIs here are non-blocking.  For consumers, waiting is done
  // at a higher level using e.g. Future.  Producers don't have to wait
  // on a StopToken.

  // Consumer API (the side that stops)
  void RequestStop();
  void RequestStop(Status error);

  // Producer API (the side that gets asked to stopped)
  Status Poll();
  bool IsStopRequested();

  // Register a callback that will be called whenever cancellation happens.
  // Note the callback may be called immediately, if cancellation was already
  // requested.  The callback will be unregistered when the returned object
  // is destroyed.
  StopCallback SetCallback(StopCallback::Callable cb);

 protected:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  void SetCallback(StopCallback* cb);
  void RemoveCallback(StopCallback* cb);

  friend class StopCallback;
};

}  // namespace arrow
