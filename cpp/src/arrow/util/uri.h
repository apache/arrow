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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

class ARROW_EXPORT Uri {
 public:
  Uri();
  ~Uri();

  // XXX Should we use util::string_view instead?  These functions are
  // not performance-critical.

  std::string scheme() const;
  bool has_host() const;
  std::string host() const;
  std::string port_text() const;
  int32_t port() const;
  std::string path() const;

  Status Parse(const std::string& uri_string);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace internal
}  // namespace arrow
