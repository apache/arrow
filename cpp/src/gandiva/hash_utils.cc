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

#include <cstring>
#include "openssl/evp.h"
#include "gandiva/hash_utils.h"
#include "gandiva/execution_context.h"
#include "gandiva/gdv_function_stubs.h"

namespace gandiva {
  const char* HashUtils::HashUsingSha256(int64_t context,
                                            const void* message,
                                            size_t message_length,
                                            u_int32_t* out_length) {
    // The buffer size is the hash size + null character
    int sha256_result_length = 65;
    return HashUtils::GetHash(context, message, message_length, EVP_sha256(),
							  sha256_result_length, out_length);
  }
  const char* HashUtils::HashUsingSha128(int64_t context,
                                            const void* message,
                                            size_t message_length,
                                            u_int32_t* out_length) {
    // The buffer size is the hash size + null character
    int sha128_result_length = 41;
    return HashUtils::GetHash(context, message, message_length, EVP_sha1(),
							  sha128_result_length, out_length);
  }

  const char* HashUtils::GetHash(int64_t context,
                                 const void* message,
                                 size_t message_length,
                                 const EVP_MD *hash_type,
                                 int result_buf_size,
                                 u_int32_t* out_length) {
    EVP_MD_CTX *md_ctx = EVP_MD_CTX_new();

    if (md_ctx == nullptr) {
      HashUtils::ErrorMessage(context, "Could not allocate memory "
									   "for SHA processing.");
      return "";
    }

    int evp_success_status = 1;

    if (EVP_DigestInit_ex(md_ctx, hash_type, nullptr) != evp_success_status) {
      HashUtils::ErrorMessage(context, "Could not obtain the hash "
									   "for the defined value.");
      EVP_MD_CTX_free(md_ctx);
      return "";
    }

    if (EVP_DigestUpdate(md_ctx, message, message_length) != evp_success_status) {
      HashUtils::ErrorMessage(context, "Could not obtain the hash for "
									   "the defined value.");
      EVP_MD_CTX_free(md_ctx);
      return "";
    }

    int hash_size = EVP_MD_size(hash_type);
    auto* result = static_cast<unsigned char*>(OPENSSL_malloc(hash_size));

    if (result == nullptr) {
      HashUtils::ErrorMessage(context, "Could not allocate memory "
									   "for SHA processing.");
      EVP_MD_CTX_free(md_ctx);
      return "";
    }

    unsigned int result_length;
    EVP_DigestFinal_ex(md_ctx, result, &result_length);

    int tmp_buf_len = 4;

    auto hex_buffer =
        reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, tmp_buf_len));

    auto result_buffer =
        reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, result_buf_size));

    CleanCharArray(result_buffer);
    CleanCharArray(hex_buffer);

    if (hex_buffer == nullptr || result_buffer == nullptr) {
      gdv_fn_context_set_error_msg(context, "Could not allocate memory "
                                       "for the result buffers.");
      EVP_MD_CTX_free(md_ctx);
      return "";
    }

    for (unsigned int j = 0; j < result_length; j++) {
      unsigned char hex_number = result[j];
      snprintf(hex_buffer, tmp_buf_len, "%02x", hex_number);
      strncat(result_buffer, hex_buffer, tmp_buf_len);
    }

    // Add the NULL character to shows the end of the string
    result_buffer[result_buf_size - 1] = '\0';

    // free the resources to avoid memory leaks
    EVP_MD_CTX_free(md_ctx);
    free(result);

    *out_length = strlen(result_buffer);

    return result_buffer;
  }

  uint64_t HashUtils::DoubleToLong(double value) {
    uint64_t result;
    memcpy(&result, &value, sizeof(result));
    return result;
  }

  void HashUtils::CleanCharArray(char *buffer) {
    buffer[0] = '\0';
  }

  void HashUtils::ErrorMessage(int64_t context_ptr, char const *err_msg) {
    auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
    context->set_error_msg(err_msg);
  }
}  // namespace gandiva
