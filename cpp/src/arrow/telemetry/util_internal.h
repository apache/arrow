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

#include "arrow/telemetry/util.h"

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/common/key_value_iterable.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/nostd/string_view.h>

namespace arrow {
namespace telemetry {

using util::span;

namespace otel = ::opentelemetry;

template <typename T>
using otel_shared_ptr = otel::nostd::shared_ptr<T>;
template <typename T>
using otel_span = otel::nostd::span<T>;
using otel_string_view = otel::nostd::string_view;

inline otel_string_view ToOtel(std::string_view in) {
  return otel_string_view(in.data(), in.length());
}

/// \brief Converts AttributeValues to their equivalent OTel types (compatible with
/// std::visit)
///
/// NOTE: This class is stateful and allocates/owns memory when converting string spans,
/// so one should ensure that the output spans are copied before the converter is reused
/// or freed.
struct AttributeConverter {
  using OtelValue = otel::common::AttributeValue;

  OtelValue operator()(bool v) { return OtelValue(v); }
  OtelValue operator()(int32_t v) { return OtelValue(v); }
  OtelValue operator()(uint32_t v) { return OtelValue(v); }
  OtelValue operator()(int64_t v) { return OtelValue(v); }
  OtelValue operator()(double v) { return OtelValue(v); }
  OtelValue operator()(const char* v) { return OtelValue(otel_string_view(v)); }
  OtelValue operator()(std::string_view v) { return OtelValue(ToOtel(v)); }
  OtelValue operator()(span<const uint8_t> v) { return ToOtelSpan<uint8_t>(v); }
  OtelValue operator()(span<const int32_t> v) { return ToOtelSpan<int32_t>(v); }
  OtelValue operator()(span<const uint32_t> v) { return ToOtelSpan<uint32_t>(v); }
  OtelValue operator()(span<const int64_t> v) { return ToOtelSpan<int64_t>(v); }
  OtelValue operator()(span<const uint64_t> v) { return ToOtelSpan<uint64_t>(v); }
  OtelValue operator()(span<const double> v) { return ToOtelSpan<double>(v); }
  OtelValue operator()(span<const char* const> v) {
    return ToOtelStringSpan<const char*>(v);
  }
  OtelValue operator()(span<const std::string> v) {
    return ToOtelStringSpan<std::string>(v);
  }
  OtelValue operator()(span<const std::string_view> v) {
    return ToOtelStringSpan<std::string_view>(v);
  }

 private:
  template <typename T, typename U = T>
  OtelValue ToOtelSpan(span<const U> vals) const {
    return otel_span<const T>(vals.begin(), vals.end());
  }

  template <typename T>
  OtelValue ToOtelStringSpan(span<const T> vals) {
    const size_t length = vals.size();
    output_views_.resize(length);
    for (size_t i = 0; i < length; ++i) {
      output_views_[i] = ToOtel(std::string_view(vals[i]));
    }
    return otel_span<const otel_string_view>(output_views_.data(), length);
  }

  std::vector<otel_string_view> output_views_;
};

class OtelAttributeHolder : public otel::common::KeyValueIterable {
 public:
  explicit OtelAttributeHolder(const AttributeHolder& holder) : holder_(&holder) {}

  bool ForEachKeyValue(otel::nostd::function_ref<bool(otel::nostd::string_view,
                                                      otel::common::AttributeValue)>
                           callback) const noexcept override {
    auto wrapped_callback = [&](std::string_view k, const AttributeValue& v) {
      AttributeConverter converter{};
      return callback(ToOtel(k), std::visit(converter, v));
    };
    return holder_->ForEach(std::move(wrapped_callback));
  }

  size_t size() const noexcept override { return holder_->num_attributes(); }

 private:
  const AttributeHolder* holder_;
};

}  // namespace telemetry
}  // namespace arrow
