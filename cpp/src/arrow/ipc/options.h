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

#include <cstdint>

#include "arrow/util/visibility.h"

namespace arrow {
namespace ipc {

// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
// deeply nested schemas, it is expected the user will indicate explicitly the
// maximum allowed recursion depth
constexpr int kMaxNestingDepth = 64;

struct ARROW_EXPORT IpcOptions {
  // If true, allow field lengths that don't fit in a signed 32-bit int.
  // Some implementations may not be able to parse such streams.
  bool allow_64bit = false;
  // The maximum permitted schema nesting depth.
  int max_recursion_depth = kMaxNestingDepth;

  static IpcOptions Defaults();
};

}  // namespace ipc
}  // namespace arrow
