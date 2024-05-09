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

#include <opentelemetry/common/attribute_value.h>
#include <opentelemetry/common/key_value_iterable.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/nostd/span.h>
#include <opentelemetry/nostd/string_view.h>

namespace arrow {
namespace telemetry {

namespace otel = ::opentelemetry;

template <typename T>
using otel_shared_ptr = otel::nostd::shared_ptr<T>;
template <typename T>
using otel_span = otel::nostd::span<T>;
using otel_string_view = otel::nostd::string_view;

inline otel_string_view ToOtel(std::string_view in) {
  return otel_string_view(in.data(), in.length());
}

}  // namespace telemetry
}  // namespace arrow
