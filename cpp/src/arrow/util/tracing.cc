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

#include "arrow/util/tracing.h"

#include "arrow/util/config.h"
#include "arrow/util/tracing_internal.h"

#include <memory>

namespace arrow {

namespace util {
namespace tracing {

#ifdef ARROW_WITH_OPENTELEMETRY

Span::Span() noexcept {
  details = std::make_unique<::arrow::internal::tracing::SpanImpl>();
}

bool Span::valid() const {
  return static_cast<::arrow::internal::tracing::SpanImpl*>(details.get())->valid();
}

void Span::reset() { details.reset(); }

#else

Span::Span() noexcept { /* details is left a nullptr */
}

bool Span::valid() const { return false; }
void Span::reset() {}

#endif

}  // namespace tracing
}  // namespace util
}  // namespace arrow
