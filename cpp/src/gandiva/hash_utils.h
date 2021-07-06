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

#ifndef ARROW_SRC_HASH_UTILS_H_
#define ARROW_SRC_HASH_UTILS_H_

#include <cstdint>
#include <cstdlib>
#include "gandiva/visibility.h"
#include "openssl/evp.h"

namespace gandiva {
GANDIVA_EXPORT
const char* gdv_sha256_hash(int64_t context, const void* message, size_t message_length,
                            int32_t* out_length);

GANDIVA_EXPORT
const char* gdv_sha1_hash(int64_t context, const void* message, size_t message_length,
                          int32_t* out_length);

GANDIVA_EXPORT
const char* gdv_hash_using_openssl(int64_t context, const void* message,
                                   size_t message_length, const EVP_MD* hash_type,
                                   uint32_t result_buf_size, int32_t* out_length);

GANDIVA_EXPORT
const char* gdv_md5_hash(int64_t context, const void* message, size_t message_length,
                         int32_t* out_length);

GANDIVA_EXPORT
uint64_t gdv_double_to_long(double value);
}  // namespace gandiva

#endif  // ARROW_SRC_HASH_UTILS_H_
