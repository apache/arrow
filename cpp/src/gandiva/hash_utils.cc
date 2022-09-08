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

#include "gandiva/hash_utils.h"
#include <cstring>
#include "arrow/util/logging.h"
#include "gandiva/gdv_function_stubs.h"
#include "openssl/evp.h"

namespace gandiva {

/// Hashes a generic message using the SHA512 algorithm
GANDIVA_EXPORT
const char* gdv_sha512_hash(int64_t context, const void* message, size_t message_length,
                            int32_t* out_length) {
  constexpr int sha512_result_length = 128;
  return gdv_hash_using_openssl(context, message, message_length, EVP_sha512(),
                                sha512_result_length, out_length);
}

/// Hashes a generic message using the SHA256 algorithm
GANDIVA_EXPORT
const char* gdv_sha256_hash(int64_t context, const void* message, size_t message_length,
                            int32_t* out_length) {
  constexpr int sha256_result_length = 64;
  return gdv_hash_using_openssl(context, message, message_length, EVP_sha256(),
                                sha256_result_length, out_length);
}

/// Hashes a generic message using the SHA1 algorithm
GANDIVA_EXPORT
const char* gdv_sha1_hash(int64_t context, const void* message, size_t message_length,
                          int32_t* out_length) {
  constexpr int sha1_result_length = 40;
  return gdv_hash_using_openssl(context, message, message_length, EVP_sha1(),
                                sha1_result_length, out_length);
}

GANDIVA_EXPORT
const char* gdv_md5_hash(int64_t context, const void* message, size_t message_length,
                         int32_t* out_length) {
  constexpr int md5_result_length = 32;
  return gdv_hash_using_openssl(context, message, message_length, EVP_md5(),
                                md5_result_length, out_length);
}

/// \brief Hashes a generic message using SHA algorithm.
///
/// It uses the EVP API in the OpenSSL library to generate
/// the hash. The type of the hash is defined by the
/// \b hash_type \b parameter.
GANDIVA_EXPORT
const char* gdv_hash_using_openssl(int64_t context, const void* message,
                                   size_t message_length, const EVP_MD* hash_type,
                                   uint32_t result_buf_size, int32_t* out_length) {
  EVP_MD_CTX* md_ctx = EVP_MD_CTX_new();

  if (md_ctx == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not create the context for SHA processing.");
    *out_length = 0;
    return "";
  }

  int evp_success_status = 1;

  if (EVP_DigestInit_ex(md_ctx, hash_type, nullptr) != evp_success_status ||
      EVP_DigestUpdate(md_ctx, message, message_length) != evp_success_status) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not obtain the hash for the defined value.");
    EVP_MD_CTX_free(md_ctx);

    *out_length = 0;
    return "";
  }

  // Create the temporary buffer used by the EVP to generate the hash
  unsigned int hash_digest_size = EVP_MD_size(hash_type);
  auto* result = static_cast<unsigned char*>(OPENSSL_malloc(hash_digest_size));

  if (result == nullptr) {
    gdv_fn_context_set_error_msg(context, "Could not allocate memory for SHA processing");
    EVP_MD_CTX_free(md_ctx);
    *out_length = 0;
    return "";
  }

  unsigned int result_length;
  EVP_DigestFinal_ex(md_ctx, result, &result_length);

  if (result_length != hash_digest_size && result_buf_size != (2 * hash_digest_size)) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not obtain the hash for the defined value");
    EVP_MD_CTX_free(md_ctx);
    OPENSSL_free(result);

    *out_length = 0;
    return "";
  }

  auto result_buffer =
      reinterpret_cast<char*>(gdv_fn_context_arena_malloc(context, result_buf_size));

  if (result_buffer == nullptr) {
    gdv_fn_context_set_error_msg(context,
                                 "Could not allocate memory for the result buffer");
    // Free the resources used by the EVP
    EVP_MD_CTX_free(md_ctx);
    OPENSSL_free(result);

    *out_length = 0;
    return "";
  }

  unsigned int result_buff_index = 0;
  for (unsigned int j = 0; j < result_length; j++) {
    DCHECK(result_buff_index >= 0 && result_buff_index < result_buf_size);

    unsigned char hex_number = result[j];
    result_buff_index +=
        snprintf(result_buffer + result_buff_index, result_buf_size, "%02x", hex_number);
  }

  // Free the resources used by the EVP to avoid memory leaks
  EVP_MD_CTX_free(md_ctx);
  OPENSSL_free(result);

  *out_length = result_buf_size;
  return result_buffer;
}

GANDIVA_EXPORT
uint64_t gdv_double_to_long(double value) {
  uint64_t result;
  memcpy(&result, &value, sizeof(result));
  return result;
}
}  // namespace gandiva
