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

#include "arrow/flight/transport/ucx/util_internal.h"

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <cerrno>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "arrow/buffer.h"
#include "arrow/flight/types.h"
#include "arrow/util/base64.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/uri.h"

namespace arrow {

using internal::ToChars;

namespace flight {
namespace transport {
namespace ucx {

constexpr char FlightUcxStatusDetail::kTypeId[];
std::string FlightUcxStatusDetail::ToString() const { return ucs_status_string(status_); }
ucs_status_t FlightUcxStatusDetail::Unwrap(const Status& status) {
  if (!status.detail() || status.detail()->type_id() != kTypeId) return UCS_OK;
  return dynamic_cast<const FlightUcxStatusDetail*>(status.detail().get())->status_;
}

arrow::Result<size_t> UriToSockaddr(const arrow::internal::Uri& uri,
                                    struct sockaddr_storage* addr) {
  std::string host = uri.host();
  if (host.empty()) {
    return Status::Invalid("Must provide a host");
  } else if (uri.port() < 0) {
    return Status::Invalid("Must provide a port");
  }

  std::memset(addr, 0, sizeof(*addr));

  struct addrinfo* info = nullptr;
  int err = getaddrinfo(host.c_str(), /*service=*/nullptr, /*hints=*/nullptr, &info);
  if (err != 0) {
    if (err == EAI_SYSTEM) {
      return arrow::internal::IOErrorFromErrno(errno, "[getaddrinfo] Failure resolving ",
                                               host);
    } else {
      return Status::IOError("[getaddrinfo] Failure resolving ", host, ": ",
                             gai_strerror(err));
    }
  }

  struct addrinfo* cur_info = info;
  while (cur_info) {
    if (cur_info->ai_family != AF_INET && cur_info->ai_family != AF_INET6) {
      cur_info = cur_info->ai_next;
      continue;
    }

    std::memcpy(addr, cur_info->ai_addr, cur_info->ai_addrlen);
    if (cur_info->ai_family == AF_INET) {
      reinterpret_cast<sockaddr_in*>(addr)->sin_port = htons(uri.port());
    } else if (cur_info->ai_family == AF_INET6) {
      reinterpret_cast<sockaddr_in6*>(addr)->sin6_port = htons(uri.port());
    }
    size_t addrlen = cur_info->ai_addrlen;
    freeaddrinfo(info);
    return addrlen;
  }

  if (info) freeaddrinfo(info);
  return Status::IOError("[getaddrinfo] Failure resolving ", host,
                         ": no results of a supported family returned");
}

arrow::Result<std::string> SockaddrToString(const struct sockaddr_storage& address) {
  std::string result = "";
  if (address.ss_family != AF_INET && address.ss_family != AF_INET6) {
    return Status::NotImplemented("Unknown address family");
  }

  uint16_t port = 0;
  if (address.ss_family == AF_INET) {
    result.resize(INET_ADDRSTRLEN + 1);
    const auto* in_addr = reinterpret_cast<const struct sockaddr_in*>(&address);
    if (!inet_ntop(address.ss_family, &in_addr->sin_addr, &result[0], INET_ADDRSTRLEN)) {
      return arrow::internal::IOErrorFromErrno(errno,
                                               "Could not convert address to string");
    }
    port = ntohs(in_addr->sin_port);
  } else {
    result.resize(INET6_ADDRSTRLEN + 1);
    const auto* in6_addr = reinterpret_cast<const struct sockaddr_in6*>(&address);
    if (!inet_ntop(address.ss_family, &in6_addr->sin6_addr, &result[0],
                   INET6_ADDRSTRLEN)) {
      return arrow::internal::IOErrorFromErrno(errno,
                                               "Could not convert address to string");
    }
    port = ntohs(in6_addr->sin6_port);
  }

  const size_t pos = result.find('\0');
  DCHECK_NE(pos, std::string::npos);
  result[pos] = ':';
  result.resize(pos + 1);
  result += ToChars(port);
  return result;
}

Status FromUcsStatus(const std::string& context, ucs_status_t ucs_status) {
  switch (ucs_status) {
    case UCS_OK:
      return Status::OK();
    case UCS_INPROGRESS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_INPROGRESS ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_MESSAGE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_MESSAGE ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_RESOURCE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_RESOURCE ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_IO_ERROR:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_IO_ERROR ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_MEMORY:
      return Status::OutOfMemory(context, ": UCX error ",
                                 static_cast<int32_t>(ucs_status), ": ",
                                 "UCS_ERR_NO_MEMORY ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_INVALID_PARAM:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_PARAM ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_UNREACHABLE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_UNREACHABLE ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_INVALID_ADDR:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_INVALID_ADDR ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NOT_IMPLEMENTED:
      return Status::NotImplemented(
                 context, ": UCX error ", static_cast<int32_t>(ucs_status), ": ",
                 "UCS_ERR_NOT_IMPLEMENTED ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_MESSAGE_TRUNCATED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_MESSAGE_TRUNCATED ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_PROGRESS:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_PROGRESS ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_BUFFER_TOO_SMALL:
      return Status::Invalid(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_BUFFER_TOO_SMALL ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_ELEM:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_ELEM ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_SOME_CONNECTS_FAILED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_SOME_CONNECTS_FAILED ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NO_DEVICE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NO_DEVICE ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_BUSY:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_BUSY ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_CANCELED:
      return Status::Cancelled(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                               ": ", "UCS_ERR_CANCELED ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_SHMEM_SEGMENT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_SHMEM_SEGMENT ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_ALREADY_EXISTS:
      return Status::AlreadyExists(
                 context, ": UCX error ", static_cast<int32_t>(ucs_status), ": ",
                 "UCS_ERR_ALREADY_EXISTS ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_OUT_OF_RANGE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_OUT_OF_RANGE ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_TIMED_OUT:
      return Status::Cancelled(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                               ": ", "UCS_ERR_TIMED_OUT ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_EXCEEDS_LIMIT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_EXCEEDS_LIMIT ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_UNSUPPORTED:
      return Status::NotImplemented(context, ": UCX error ",
                                    static_cast<int32_t>(ucs_status), ": ",
                                    "UCS_ERR_UNSUPPORTED ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_REJECTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_REJECTED ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_NOT_CONNECTED:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_NOT_CONNECTED ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_CONNECTION_RESET:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_CONNECTION_RESET ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_FIRST_LINK_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_FIRST_LINK_FAILURE ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_LAST_LINK_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST_LINK_FAILURE ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_FIRST_ENDPOINT_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_FIRST_ENDPOINT_FAILURE ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_LAST_ENDPOINT_FAILURE:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST_ENDPOINT_FAILURE ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_ENDPOINT_TIMEOUT:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_ENDPOINT_TIMEOUT ",
                             ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    case UCS_ERR_LAST:
      return Status::IOError(context, ": UCX error ", static_cast<int32_t>(ucs_status),
                             ": ", "UCS_ERR_LAST ", ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
    default:
      return Status::UnknownError(
                 context, ": Unknown UCX error: ", static_cast<int32_t>(ucs_status), " ",
                 ucs_status_string(ucs_status))
          .WithDetail(std::make_shared<FlightUcxStatusDetail>(ucs_status));
  }
}

}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
