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

#include <arpa/inet.h>
#include <ucp/api/ucp.h>
#include <string>

#include "arrow/flight/visibility.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

static inline void UInt32ToBytesBe(const uint32_t in, uint8_t* out) {
  out[0] = static_cast<uint8_t>((in >> 24) & 0xFF);
  out[1] = static_cast<uint8_t>((in >> 16) & 0xFF);
  out[2] = static_cast<uint8_t>((in >> 8) & 0xFF);
  out[3] = static_cast<uint8_t>(in & 0xFF);
}

static inline uint32_t BytesToUInt32Be(const uint8_t* in) {
  return static_cast<uint32_t>(in[3]) | (static_cast<uint32_t>(in[2]) << 8) |
         (static_cast<uint32_t>(in[1]) << 16) | (static_cast<uint32_t>(in[0]) << 24);
}

ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

/// \brief Helper to convert a Uri to a struct sockaddr (used in
///   ucp_listener_params_t)
///
/// \return The length of the sockaddr
ARROW_FLIGHT_EXPORT
arrow::Result<size_t> UriToSockaddr(const arrow::internal::Uri& uri,
                                    struct sockaddr_storage* addr);

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
