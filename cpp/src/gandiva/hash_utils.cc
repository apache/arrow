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
#include "gandiva/hash_utils.h"
#include "openssl/evp.h"
#include "execution_context.h"

namespace gandiva {
  const char* HashUtils::HashUsingSha256(int64_t context,
                                            const void* message,
                                            size_t message_length,
                                            u_int32_t* out_length) {
    return HashUtils::GetHash(context, message, message_length, EVP_sha256(), out_length);
  }
  const char* HashUtils::HashUsingSha128(int64_t context,
                                            const void* message,
                                            size_t message_length,
                                            u_int32_t* out_length) {
    return HashUtils::GetHash(context, message, message_length, EVP_sha1(), out_length);
  }

  const char* HashUtils::GetHash(int64_t context,
                                 const void* message,
                                 size_t message_length,
                                 const EVP_MD *hash_type,
                                 u_int32_t* out_length) {
    if(message == nullptr){
      HashUtils::ErrorMessage(context, "A null value was given to be hashed.");
      return "";
    }

    EVP_MD_CTX *md_ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md_ctx, hash_type, nullptr);
    EVP_DigestUpdate(md_ctx, message, message_length);

    int hash_size = EVP_MD_size(hash_type);

    auto* result = static_cast<unsigned char*>(OPENSSL_malloc(hash_size));

    unsigned int result_length;

    EVP_DigestFinal_ex(md_ctx, result, &result_length);

    char* hex_buffer = new char[4];
    char* result_buffer = new char[65];

    CleanCharArray(hex_buffer);
    CleanCharArray(result_buffer);

    for (unsigned int j = 0; j < result_length; j++) {
      unsigned char hex_number = result[j];
      sprintf(hex_buffer, "%02x", hex_number);
      strcat(result_buffer, hex_buffer);
    }

    result_buffer[64] = '\0';

    // free the resources to avoid memory leaks
    EVP_MD_CTX_free(md_ctx);
    delete[] hex_buffer;
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

  void HashUtils::ErrorMessage(int64_t context_ptr, char const *err_msg){
    auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
    context->set_error_msg(err_msg);
  }
}  // namespace gandiva