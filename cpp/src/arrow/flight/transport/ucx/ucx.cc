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

#include "arrow/flight/transport/ucx/ucx.h"

#include <mutex>

#include "arrow/flight/transport.h"
#include "arrow/flight/transport/ucx/ucx_internal.h"
#include "arrow/flight/transport_server.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace transport {
namespace ucx {

namespace {
std::once_flag kInitializeOnce;
}
void InitializeFlightUcx() {
  std::call_once(kInitializeOnce, []() {
    auto* registry = flight::internal::GetDefaultTransportRegistry();
    DCHECK_OK(registry->RegisterClient("ucx", MakeUcxClientImpl));
    DCHECK_OK(registry->RegisterServer("ucx", MakeUcxServerImpl));
  });
}
}  // namespace ucx
}  // namespace transport
}  // namespace flight
}  // namespace arrow
