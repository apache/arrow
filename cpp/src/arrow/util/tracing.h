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

#include "arrow/util/logging.h"

namespace arrow {

namespace internal {
namespace tracing {

// Forward declaration SpanImpl.
class SpanImpl;

}  // namespace tracing
}  // namespace internal

namespace util {
namespace tracing {

class ARROW_EXPORT Span {
 public:
  using Impl = arrow::internal::tracing::SpanImpl;

  Span() = default;  // Default constructor. The inner_impl is a nullptr.
  ~Span();  // Destructor. Default destructor defined in tracing.cc where impl is a
            // complete type.

  Impl& Set(const Impl&);
  Impl& Set(Impl&&);

  const Impl& Get() const {
    ARROW_CHECK(inner_impl)
        << "Attempted to dereference a null pointer. Use Span::Set before "
           "dereferencing.";
    return *inner_impl;
  }

  Impl& Get() {
    ARROW_CHECK(inner_impl)
        << "Attempted to dereference a null pointer. Use Span::Set before "
           "dereferencing.";
    return *inner_impl;
  }

 private:
  std::unique_ptr<Impl> inner_impl;
};

}  // namespace tracing
}  // namespace util
}  // namespace arrow
