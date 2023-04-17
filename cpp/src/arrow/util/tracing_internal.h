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

// Pick up ARROW_WITH_OPENTELEMETRY first
#include "arrow/util/config.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4522)
#endif
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/scope.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif
#endif

#include "arrow/memory_pool.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/tracing.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {
namespace tracing {

#ifdef ARROW_WITH_OPENTELEMETRY
ARROW_EXPORT
opentelemetry::trace::Tracer* GetTracer();

inline void MarkSpan(const Status& s, opentelemetry::trace::Span* span) {
  if (!s.ok()) {
    span->SetStatus(opentelemetry::trace::StatusCode::kError, s.ToString());
  } else {
    span->SetStatus(opentelemetry::trace::StatusCode::kOk);
  }
}

template <typename T>
inline Result<T> MarkSpan(Result<T> result, opentelemetry::trace::Span* span) {
  MarkSpan(result.status(), span);
  return result;
}

/// \brief Tie the current span to a generator, ending it when the generator finishes.
/// Optionally start a child span for each invocation.
template <typename T>
AsyncGenerator<T> WrapAsyncGenerator(AsyncGenerator<T> wrapped,
                                     const std::string& span_name = "",
                                     bool create_childspan = false) {
  auto active_span = GetTracer()->GetCurrentSpan();
  return [=]() mutable -> Future<T> {
    auto span = active_span;
    auto scope = GetTracer()->WithActiveSpan(active_span);
    auto fut = wrapped();
    if (create_childspan) {
      span = GetTracer()->StartSpan(span_name);
    }
    fut.AddCallback([span](const Result<T>& result) {
      MarkSpan(result.status(), span.get());
      span->End();
    });
    return fut;
  };
}

/// \brief Propagate the given span to each invocation of an async generator.
template <typename T>
AsyncGenerator<T> PropagateSpanThroughAsyncGenerator(
    AsyncGenerator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span) {
  return [=]() mutable -> Future<T> {
    auto scope = GetTracer()->WithActiveSpan(span);
    return wrapped();
  };
}

/// \brief Propagate the currently active span to each invocation of an async generator.
///
/// This prevents spans, created when running generator instances asynchronously,
/// ending up in a separate, disconnected trace.
template <typename T>
AsyncGenerator<T> PropagateSpanThroughAsyncGenerator(AsyncGenerator<T> wrapped) {
  auto span = GetTracer()->GetCurrentSpan();
  if (!span->GetContext().IsValid()) return wrapped;
  return PropagateSpanThroughAsyncGenerator(std::move(wrapped), std::move(span));
}

class SpanImpl : public ::arrow::util::tracing::SpanDetails {
 public:
  ~SpanImpl() override = default;
  bool valid() const { return ot_span != nullptr; }
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> ot_span;
};

struct Scope {
  Scope()
      : scope_impl(opentelemetry::trace::Scope(
            opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(nullptr))) {}
  explicit Scope(opentelemetry::trace::Scope ot_scope)
      : scope_impl(std::move(ot_scope)) {}
  opentelemetry::trace::Scope scope_impl;
};

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>& UnwrapSpan(
    ::arrow::util::tracing::SpanDetails* span);

const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>& UnwrapSpan(
    const ::arrow::util::tracing::SpanDetails* span);

opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>& RewrapSpan(
    ::arrow::util::tracing::SpanDetails* span,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> ot_span);

opentelemetry::trace::StartSpanOptions SpanOptionsWithParent(
    const util::tracing::Span& parent_span);

#define START_SPAN(target_span, ...)                           \
  auto opentelemetry_scope##__LINE__ =                         \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan( \
          ::arrow::internal::tracing::RewrapSpan(              \
              target_span.details.get(),                       \
              ::arrow::internal::tracing::GetTracer()->StartSpan(__VA_ARGS__)))

#define START_SCOPED_SPAN(target_span, ...)                    \
  ::arrow::internal::tracing::Scope(                           \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan( \
          ::arrow::internal::tracing::RewrapSpan(              \
              target_span.details.get(),                       \
              ::arrow::internal::tracing::GetTracer()->StartSpan(__VA_ARGS__))))

#define START_SCOPED_SPAN_SV(target_span, name, ...)                             \
  ::arrow::internal::tracing::Scope(                                             \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan(                   \
          ::arrow::internal::tracing::RewrapSpan(                                \
              target_span.details.get(),                                         \
              ::arrow::internal::tracing::GetTracer()->StartSpan(                \
                  ::opentelemetry::nostd::string_view(name.data(), name.size()), \
                  ##__VA_ARGS__))))

#define START_SCOPED_SPAN_WITH_PARENT_SV(target_span, parent_span, name, ...)    \
  ::arrow::internal::tracing::Scope(                                             \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan(                   \
          ::arrow::internal::tracing::RewrapSpan(                                \
              target_span.details.get(),                                         \
                                                                                 \
              ::arrow::internal::tracing::GetTracer()->StartSpan(                \
                  ::opentelemetry::nostd::string_view(name.data(), name.size()), \
                  __VA_ARGS__,                                                   \
                  ::arrow::internal::tracing::SpanOptionsWithParent(parent_span)))))

#define START_COMPUTE_SPAN(target_span, ...)                        \
  START_SPAN(target_span, __VA_ARGS__);                             \
  ::arrow::internal::tracing::UnwrapSpan(target_span.details.get()) \
      ->SetAttribute("arrow.memory_pool_bytes",                     \
                     ::arrow::default_memory_pool()->bytes_allocated())

#define EVENT_ON_CURRENT_SPAN(...) \
  ::arrow::internal::tracing::GetTracer()->GetCurrentSpan()->AddEvent(__VA_ARGS__)

#define EVENT(target_span, ...) \
  ::arrow::internal::tracing::UnwrapSpan(target_span.details.get())->AddEvent(__VA_ARGS__)

#define ACTIVATE_SPAN(target_span)                             \
  ::arrow::internal::tracing::Scope(                           \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan( \
          ::arrow::internal::tracing::UnwrapSpan(target_span.details.get())))

#define MARK_SPAN(target_span, status)  \
  ::arrow::internal::tracing::MarkSpan( \
      status, ::arrow::internal::tracing::UnwrapSpan(target_span.details.get()).get())

#define END_SPAN(target_span) \
  ::arrow::internal::tracing::UnwrapSpan(target_span.details.get())->End()

#define END_SPAN_ON_FUTURE_COMPLETION(target_span, target_future) \
  target_future.SetSpan(&target_span)

#define PROPAGATE_SPAN_TO_GENERATOR(generator)                                \
  generator = ::arrow::internal::tracing::PropagateSpanThroughAsyncGenerator( \
      std::move(generator))

#define WRAP_ASYNC_GENERATOR(generator) \
  generator = ::arrow::internal::tracing::WrapAsyncGenerator(std::move(generator))

#define WRAP_ASYNC_GENERATOR_WITH_CHILD_SPAN(generator, name) \
  generator =                                                 \
      ::arrow::internal::tracing::WrapAsyncGenerator(std::move(generator), name, true)

/*
 * Calls to the helper macros above are removed by the preprocessor when
 * building without opentelemetry, because of the empty definitions
 * below. Without them, every call to a helper function would need to be
 * surrounded with #ifdef ARROW_WITH_OPENTELEMETRY
 * ...
 * #endif
 */

#else  // !ARROW_WITH_OPENTELEMETRY

class SpanImpl {};
struct Scope {
  [[maybe_unused]] ~Scope() {}
};

#define START_SPAN(target_span, ...)
#define START_SCOPED_SPAN(target_span, ...) ::arrow::internal::tracing::Scope()
#define START_SCOPED_SPAN_SV(target_span, name, ...) ::arrow::internal::tracing::Scope()
#define START_COMPUTE_SPAN(target_span, ...)
#define ACTIVATE_SPAN(target_span) ::arrow::internal::tracing::Scope()
#define MARK_SPAN(target_span, status)
#define EVENT(target_span, ...)
#define EVENT_ON_CURRENT_SPAN(...)
#define END_SPAN(target_span)
#define END_SPAN_ON_FUTURE_COMPLETION(target_span, target_future)
#define PROPAGATE_SPAN_TO_GENERATOR(generator)
#define WRAP_ASYNC_GENERATOR(generator)
#define WRAP_ASYNC_GENERATOR_WITH_CHILD_SPAN(generator, name)

#endif

}  // namespace tracing
}  // namespace internal
}  // namespace arrow
