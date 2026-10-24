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

#include <string>

#include "gandiva/visibility.h"

namespace gandiva {

/// @brief Get a human-readable error string from OpenSSL's error queue.
/// @details Retrieves all errors from the OpenSSL error queue and concatenates them
///          with "; " as a separator. This ensures complete error information is
///          captured.
/// @return A string describing all OpenSSL errors in the queue, or "Unknown OpenSSL
/// error"
///         if no error is available.
GANDIVA_EXPORT
std::string get_openssl_error_string();

}  // namespace gandiva
