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

#include <cstdlib>
#include <cstdint>
#include "gandiva/visibility.h"
#include "openssl/evp.h"

namespace gandiva {
class GANDIVA_EXPORT HashUtils {
 public:
  static const char *HashUsingSha256(int64_t context,
                                     const void *message,
                                     size_t message_length,
                                     int32_t *out_length);

  static const char *HashUsingSha1(int64_t context,
                                   const void *message,
                                   size_t message_length,
                                   int32_t *out_length);

  static uint64_t DoubleToLong(double value);
 private:
  static inline void CleanCharArray(char *buffer);

  static const char *GetHash(int64_t context,
                             const void *message,
                             size_t message_length,
                             const EVP_MD *hash_type,
                             uint32_t result_buf_size,
                             int32_t *out_length);

  static void ErrorMessage(int64_t context_ptr, char const *err_msg);
};
}  // namespace gandiva

#endif //ARROW_SRC_HASH_UTILS_H_
