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

#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/make_unique.h"
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

template <typename T>
Iterator<T> WrapIterator(
    Iterator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> parent_span,
    const std::string& span_name) {
  struct {
    Result<T> operator()() {
      opentelemetry::trace::StartSpanOptions options;
      options.parent = parent_span->GetContext();
      auto span = GetTracer()->StartSpan(span_name, options);
      auto scope = GetTracer()->WithActiveSpan(span);
      return wrapped.Next();
    }

    Iterator<T> wrapped;
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> parent_span;
    std::string span_name;
  } Wrapper;
  Wrapper.wrapped = std::move(wrapped);
  Wrapper.parent_span = std::move(parent_span);
  Wrapper.span_name = span_name;
  return MakeFunctionIterator(std::move(Wrapper));
}

template <typename T>
AsyncGenerator<T> WrapAsyncGenerator(AsyncGenerator<T> wrapped,
                                     opentelemetry::trace::StartSpanOptions options,
                                     const std::string& span_name) {
  return [=]() mutable -> Future<T> {
    auto span = GetTracer()->StartSpan(span_name, {}, options);
    auto scope = GetTracer()->WithActiveSpan(span);
    auto fut = wrapped();
    fut.AddCallback([span](const Result<T>& result) {
      MarkSpan(result.status(), span.get());
      span->End();
    });
    return fut;
  };
}

/// \brief Start a new span for each invocation of a generator.
///
/// The parent span of the new span will be the currently active span
/// (if any) as of when WrapAsyncGenerator was itself called.
template <typename T>
AsyncGenerator<T> WrapAsyncGenerator(AsyncGenerator<T> wrapped,
                                     const std::string& span_name) {
  opentelemetry::trace::StartSpanOptions options;
  options.parent = GetTracer()->GetCurrentSpan()->GetContext();
  return WrapAsyncGenerator(std::move(wrapped), std::move(options), span_name);
}

/// \brief End the given span when the given async generator ends.
///
/// The span will be made the active span each time the generator is called.
template <typename T>
AsyncGenerator<T> TieSpanToAsyncGenerator(
    AsyncGenerator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span) {
  return [=]() mutable -> Future<T> {
    auto scope = GetTracer()->WithActiveSpan(span);
    return wrapped().Then(
        [span](const T& result) -> Result<T> {
          span->SetStatus(opentelemetry::trace::StatusCode::kOk);
          span->End();
          return result;
        },
        [span](const Status& status) -> Result<T> {
          MarkSpan(status, span.get());
          span->End();
          return status;
        });
  };
}

/// \brief End the given span when the given async generator ends.
///
/// The span will be made the active span each time the generator is called.
template <typename T>
AsyncGenerator<T> TieSpanToAsyncGenerator(AsyncGenerator<T> wrapped) {
  auto span = GetTracer()->GetCurrentSpan();
  return TieSpanToAsyncGenerator(wrapped, span);
}

/// \brief Activate the given span on each invocation of an async generator.
template <typename T>
AsyncGenerator<T> PropagateSpanThroughAsyncGenerator(
    AsyncGenerator<T> wrapped,
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span) {
  return [=]() mutable -> Future<T> {
    auto scope = GetTracer()->WithActiveSpan(span);
    return wrapped();
  };
}

/// \brief Activate the given span on each invocation of an async generator.
template <typename T>
AsyncGenerator<T> PropagateSpanThroughAsyncGenerator(AsyncGenerator<T> wrapped) {
  auto span = GetTracer()->GetCurrentSpan();
  if (!span->GetContext().IsValid()) return wrapped;
  return PropagateSpanThroughAsyncGenerator(std::move(wrapped), std::move(span));
}

class SpanImpl {
 public:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span;
};

opentelemetry::trace::StartSpanOptions SpanOptionsWithParent(
    const util::tracing::Span& parent_span);

#define START_SPAN(target_span, ...)                                                \
  auto opentelemetry_scope##__LINE__ =                                              \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan(                      \
          target_span                                                               \
              .Set(::arrow::util::tracing::Span::Impl{                              \
                  ::arrow::internal::tracing::GetTracer()->StartSpan(__VA_ARGS__)}) \
              .span)

#define START_SPAN_WITH_PARENT(target_span, parent_span, ...)                           \
  auto opentelemetry_scope##__LINE__ =                                                  \
      ::arrow::internal::tracing::GetTracer()->WithActiveSpan(                          \
          target_span                                                                   \
              .Set(::arrow::util::tracing::Span::Impl{                                  \
                  ::arrow::internal::tracing::GetTracer()->StartSpan(                   \
                      __VA_ARGS__,                                                      \
                      ::arrow::internal::tracing::SpanOptionsWithParent(parent_span))}) \
              .span)

#define EVENT(target_span, ...) target_span.Get().span->AddEvent(__VA_ARGS__)

#define MARK_SPAN(target_span, status) \
  ::arrow::internal::tracing::MarkSpan(status, target_span.Get().span.get())

#define END_SPAN(target_span) target_span.Get().span->End()

#define END_SPAN_ON_FUTURE_COMPLETION(target_span, target_future, target_capture) \
  target_future = target_future.Then(                                             \
      [target_capture]() {                                                        \
        MARK_SPAN(target_span, Status::OK());                                     \
        END_SPAN(target_span);                                                    \
      },                                                                          \
      [target_capture](const Status& st) {                                        \
        MARK_SPAN(target_span, st);                                               \
        END_SPAN(target_span);                                                    \
        return st;                                                                \
      })

#define GET_CURRENT_SPAN(lhs) \
  lhs = ::arrow::internal::tracing::GetTracer()->GetCurrentSpan()

#define SET_SPAN_SCOPE(lhs, span) \
  lhs = ::arrow::internal::tracing::GetTracer()->WithActiveSpan(span)

#define TIE_SPAN_TO_GENERATOR(generator) \
  generator = ::arrow::internal::tracing::TieSpanToAsyncGenerator(std::move(generator))

#define PROPAGATE_SPAN_TO_GENERATOR(generator)                                \
  generator = ::arrow::internal::tracing::PropagateSpanThroughAsyncGenerator( \
      std::move(generator))

#define WRAP_ASYNC_GENERATOR(generator, name) \
  generator = ::arrow::internal::tracing::WrapAsyncGenerator(std::move(generator), name)

#else

class SpanImpl {};

#define START_SPAN(target_span, ...)
#define START_SPAN_WITH_PARENT(target_span, parent_span, ...)
#define MARK_SPAN(target_span, status)
#define EVENT(target_span, ...)
#define END_SPAN(target_span)
#define END_SPAN_ON_FUTURE_COMPLETION(target_span, target_future, target_capture)
#define GET_CURRENT_SPAN(lhs)
#define SET_SPAN_SCOPE(lhs, span)
#define TIE_SPAN_TO_GENERATOR(generator)
#define PROPAGATE_SPAN_TO_GENERATOR(generator)
#define WRAP_ASYNC_GENERATOR(generator, name)

#endif

}  // namespace tracing
}  // namespace internal
}  // namespace arrow
