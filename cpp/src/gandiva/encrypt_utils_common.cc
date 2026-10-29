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

#include "gandiva/encrypt_utils_common.h"
#include <openssl/err.h>
#include <cstring>
#include <string>

namespace gandiva {

std::string get_openssl_error_string() {
  std::string error_string;
  char error_buffer[256];

  // Loop through all errors in the queue
  while (auto error_code = ERR_get_error()) {
    if (!error_string.empty()) {
      error_string += "; ";
    }
    ERR_error_string(error_code, error_buffer);
    error_string += std::string(error_buffer);
  }

  if (error_string.empty()) {
    return "Unknown OpenSSL error";
  }
  return error_string;
}

}  // namespace gandiva
