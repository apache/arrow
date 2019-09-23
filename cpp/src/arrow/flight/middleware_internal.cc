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

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/middleware_internal.h"

#include <map>

#include "arrow/util/stl.h"

namespace arrow {
namespace flight {

namespace internal {

class MultimapIterator : public HeaderIterator::Impl {
 public:
  explicit MultimapIterator(GrpcMetadataMap::const_iterator it) : it_(it) {}

  void Next() override { it_++; }
  virtual const HeaderIterator::value_type& Dereference() const override {
    current_header_ = util::string_view(it_->first.data(), it_->first.length());
    current_value_ = util::string_view(it_->second.data(), it_->second.length());
    current_ = std::make_pair(current_header_, current_value_);
    return current_;
  }
  virtual bool Equals(const void* other) const override {
    return it_ == static_cast<const MultimapIterator*>(other)->it_;
  }
  virtual std::unique_ptr<HeaderIterator::Impl> Clone() const override {
    return arrow::internal::make_unique<MultimapIterator>(it_);
  }
  ~MultimapIterator() = default;

 private:
  GrpcMetadataMap::const_iterator it_;
  mutable util::string_view current_header_;
  mutable util::string_view current_value_;
  mutable std::pair<util::string_view, util::string_view> current_;
};

std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>
GrpcCallHeaders::GetHeaders(const std::string& key) const {
  const auto& result = metadata_->equal_range(key);
  return std::make_pair<const_iterator, const_iterator>(
      HeaderIterator(arrow::internal::make_unique<MultimapIterator>(result.first)),
      HeaderIterator(arrow::internal::make_unique<MultimapIterator>(result.second)));
}

std::size_t GrpcCallHeaders::Count(const std::string& key) const {
  return metadata_->count(key);
}

CallHeaders::const_iterator GrpcCallHeaders::cbegin() const noexcept {
  return HeaderIterator(
      arrow::internal::make_unique<MultimapIterator>(metadata_->cbegin()));
}

CallHeaders::const_iterator GrpcCallHeaders::cend() const noexcept {
  return HeaderIterator(
      arrow::internal::make_unique<MultimapIterator>(metadata_->cend()));
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
