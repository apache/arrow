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

#include <any>
#include <functional>
#include <memory>
#include <utility>

#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

struct ARROW_EXPORT AtForkHandler {
  using CallbackBefore = std::function<std::any()>;
  using CallbackAfter = std::function<void(std::any)>;

  // The before-fork callback can return an arbitrary token (wrapped in std::any)
  // that will passed as-is to after-fork callbacks.  This can ensure that any
  // resource necessary for after-fork handling is kept alive.
  CallbackBefore before;
  CallbackAfter parent_after;
  CallbackAfter child_after;

  AtForkHandler() = default;

  explicit AtForkHandler(CallbackAfter child_after)
      : child_after(std::move(child_after)) {}

  AtForkHandler(CallbackBefore before, CallbackAfter parent_after,
                CallbackAfter child_after)
      : before(std::move(before)),
        parent_after(std::move(parent_after)),
        child_after(std::move(child_after)) {}
};

// Register the given at-fork handlers. Their intended lifetime should be tracked by
// calling code using an owning shared_ptr.
ARROW_EXPORT
void RegisterAtFork(std::weak_ptr<AtForkHandler>);

}  // namespace internal
}  // namespace arrow
