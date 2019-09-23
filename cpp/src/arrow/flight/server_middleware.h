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

// Interfaces for defining middleware for Flight servers. Currently
// experimental.

#pragma once

#include "arrow/flight/middleware.h"
#include "arrow/flight/visibility.h"  // IWYU pragma: keep
#include "arrow/status.h"

namespace arrow {

namespace flight {

class ARROW_FLIGHT_EXPORT ServerMiddleware {
 public:
  virtual ~ServerMiddleware() = default;

  virtual Status SendingHeaders(AddCallHeaders& outgoing_headers) = 0;
  virtual Status CallCompleted(const Status& status) = 0;
};

class ARROW_FLIGHT_EXPORT ServerMiddlewareFactory {
 public:
  virtual ~ServerMiddlewareFactory() = default;

  /// \brief A callback for the start of a new call.
  ///
  /// Return a non-OK status to reject the call with the given status.
  ///
  /// \param info Information about the call.
  /// \param incoming_headers Headers sent by the client for this call.
  ///     Do not retain a reference to this object.
  /// \param[out] middleware The middleware instance for this call. If
  ///     unset, will not add middleware to this call instance from
  ///     this factory.
  virtual Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                           std::shared_ptr<ServerMiddleware>* middleware) = 0;
};

}  // namespace flight

}  // namespace arrow
