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
#include <initializer_list>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>
#include <vector>

#include "arrow/util/span.h"

namespace arrow {
namespace telemetry {

using AttributeValue =
    std::variant<bool, int32_t, int64_t, uint32_t, double, const char*, std::string_view,
                 util::span<const int32_t>, util::span<const int64_t>,
                 util::span<const uint32_t>, util::span<const double>,
                 util::span<const char* const>, util::span<const std::string_view>,
                 util::span<const std::string>>;

using Attribute = std::pair<std::string_view, AttributeValue>;

class AttributeHolder {
 public:
  virtual ~AttributeHolder() = default;

  virtual bool ForEach(
      std::function<bool(std::string_view, const AttributeValue&)>) const noexcept = 0;

  virtual size_t num_attributes() const noexcept = 0;
};

class AttributeList final : public AttributeHolder {
 public:
  AttributeList() = default;
  explicit AttributeList(std::initializer_list<Attribute> attributes)
      : attributes_(std::move(attributes)) {}

  bool ForEach(std::function<bool(std::string_view, const AttributeValue&)> callback)
      const noexcept override {
    for (const auto& [k, v] : attributes_) {
      callback(k, v);
    }
    return true;
  }

  size_t num_attributes() const noexcept override { return attributes_.size(); }

  AttributeList& Add(Attribute attribute) {
    attributes_.push_back(std::move(attribute));
    return *this;
  }
  AttributeList& Add(std::string_view key, AttributeValue value) {
    return Add(Attribute{key, std::move(value)});
  }

 private:
  std::vector<Attribute> attributes_;
};

class EmptyAttributeHolder : public AttributeHolder {
 public:
  bool ForEach(std::function<bool(std::string_view, const AttributeValue&)>)
      const noexcept override {
    return true;
  }
  size_t num_attributes() const noexcept override { return 0; }
};

}  // namespace telemetry
}  // namespace arrow
