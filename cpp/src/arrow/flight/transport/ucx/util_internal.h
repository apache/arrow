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
#include "arrow/status.h"
#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/uri.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

static inline void UInt32ToBytesBe(const uint32_t in, uint8_t* out) {
  util::SafeStore(out, bit_util::ToBigEndian(in));
}

static inline uint32_t BytesToUInt32Be(const uint8_t* in) {
  return bit_util::FromBigEndian(util::SafeLoadAs<uint32_t>(in));
}

class ARROW_FLIGHT_EXPORT FlightUcxStatusDetail : public StatusDetail {
 public:
  explicit FlightUcxStatusDetail(ucs_status_t status) : status_(status) {}
  static constexpr char const kTypeId[] = "flight::transport::ucx::FlightUcxStatusDetail";

  const char* type_id() const override { return kTypeId; }
  std::string ToString() const override;
  static ucs_status_t Unwrap(const Status& status);

 private:
  ucs_status_t status_;
};

/// \brief Convert a UCS status to an Arrow Status.
ARROW_FLIGHT_EXPORT
Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status);

/// \brief Check if a UCS error code can be ignored in the context of
///   a disconnect.
static inline bool IsIgnorableDisconnectError(ucs_status_t ucs_status) {
  // Not connected, connection reset: we're already disconnected
  // Timeout: most likely disconnected, but we can't tell from our end
  return ucs_status == UCS_OK || ucs_status == UCS_ERR_ENDPOINT_TIMEOUT ||
         ucs_status == UCS_ERR_NOT_CONNECTED || ucs_status == UCS_ERR_CONNECTION_RESET;
}

/// \brief Helper to convert a Uri to a struct sockaddr (used in
///   ucp_listener_params_t)
///
/// \return The length of the sockaddr
ARROW_FLIGHT_EXPORT
arrow::Result<size_t> UriToSockaddr(const arrow::internal::Uri& uri,
                                    struct sockaddr_storage* addr);

ARROW_FLIGHT_EXPORT
arrow::Result<std::string> SockaddrToString(const struct sockaddr_storage& address);

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
