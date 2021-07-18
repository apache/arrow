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

// Operations that can handle CRC algorithms.
// To avoid references to boost (lib containing the CRC implementations) from
// the precompiled-to-ir code (this causes issues with symbol resolution at runtime), we
// use this wrapper exported from the CPP code.

#include "gandiva/crc32.h"

#include <boost/crc.hpp>

namespace gandiva {
namespace internal {
// Get CRC32 for a given input chars
int64_t GetCrc32(const char* input, int32_t input_len) {
  boost::crc_32_type result;
  result.process_bytes(input, input_len);
  return result.checksum();
}
} // namespace internal
} // namespace gandiva

extern "C" {
int64_t crc32(const char* input, int32_t input_len) {
  return gandiva::internal::GetCrc32(input, input_len);
}

}  // extern "C"