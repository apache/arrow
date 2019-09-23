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

#include "arrow/flight/middleware.h"

#include <map>

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

#include "arrow/util/logging.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace flight {

HeaderIterator::HeaderIterator() : impl_(nullptr) {}

HeaderIterator::HeaderIterator(std::unique_ptr<Impl> impl) : impl_(impl.release()) {}

HeaderIterator::HeaderIterator(const HeaderIterator& copy)
    : HeaderIterator(copy.impl_->Clone()) {}

const HeaderIterator::value_type& HeaderIterator::operator*() const {
  ARROW_CHECK_NE(impl_, nullptr);
  return impl_->Dereference();
}

HeaderIterator& HeaderIterator::operator=(const HeaderIterator& other) {
  impl_ = other.impl_->Clone();
  return *this;
}

HeaderIterator& HeaderIterator::operator++() {
  ARROW_CHECK_NE(impl_, nullptr);
  impl_->Next();
  return *this;
}

HeaderIterator HeaderIterator::operator++(int) {
  HeaderIterator ret = *this;
  this->operator++();
  return ret;
}

bool HeaderIterator::operator==(const HeaderIterator& r) const {
  ARROW_CHECK_NE(impl_, nullptr);
  return impl_->Equals(r.impl_.get());
}

bool HeaderIterator::operator!=(const HeaderIterator& r) const {
  ARROW_CHECK_NE(impl_, nullptr);
  return !(*this == r);
}

}  // namespace flight
}  // namespace arrow
