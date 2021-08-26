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

#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

#define ARROW_INITIALIZER_NAME(counter) ARROW_CONCAT(arrow_initializer_, counter)

/// Declares a block of code to be executed on load of the library.
/// The init block can only fail by aborting, so use this with caution.
///
///     ARROW_INITIALIZER({
///       DCHECK_OK(registry->Add(Thing::Make()));
///     });
#define ARROW_INITIALIZER(...) \
  ARROW_INITIALIZER_IMPL(ARROW_INITIALIZER_NAME(__COUNTER__), __VA_ARGS__)

#if !defined(_MSC_VER)

// __attribute__((constructor)) is supported by GCC and Clang, and
// declares that a function must be executed as part of library initialization.
#define ARROW_INITIALIZER_IMPL(NAME, ...) \
  __attribute__((constructor)) void NAME() __VA_ARGS__

#else

// MSVC has no equivalent of __attribute__((constructor)), so instead
// specify an object whose constructor executes the required code.
#define ARROW_INITIALIZER_IMPL(NAME, ...) \
  __declspec(dllexport) struct NAME { NAME() __VA_ARGS__ } NAME

#endif

}  // namespace internal
}  // namespace arrow
