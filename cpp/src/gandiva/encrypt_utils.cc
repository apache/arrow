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

#include "encrypt_utils.h"

#include "gandiva/gdv_function_stubs.h"

namespace gandiva {

int32_t aes_init(int64_t context, unsigned char* key_data, int key_data_len, unsigned char *salt,
                 EVP_CIPHER_CTX* e_ctx, EVP_CIPHER_CTX* d_ctx) {
  int i, nrounds = 5;
  unsigned char key[32], iv[32];
  std::string err_msg;

  i = EVP_BytesToKey(EVP_aes_256_cbc(), EVP_sha1(), salt, key_data, key_data_len, nrounds,
                     key, iv);
  if (i != 32) {
    err_msg = "Key size is " + std::to_string(i) + " bits - should be 256 bits";
    gdv_fn_context_set_error_msg(context, err_msg.c_str());
    return -1;
  }

  EVP_CIPHER_CTX_init(e_ctx);
  EVP_EncryptInit_ex(e_ctx, EVP_aes_256_cbc(), nullptr, key, iv);
  EVP_CIPHER_CTX_init(d_ctx);
  EVP_DecryptInit_ex(d_ctx, EVP_aes_256_cbc(), nullptr, key, iv);

  return 0;
}


unsigned char* aes_encrypt(int64_t context, EVP_CIPHER_CTX* e, unsigned char* plaintext,
                           int* len) {
  int c_len = *len + AES_BLOCK_SIZE, f_len = 0;
  auto* ciphertext = static_cast<unsigned char*>(malloc(c_len));

  EVP_EncryptInit_ex(e, nullptr, nullptr, nullptr, nullptr);

  EVP_EncryptUpdate(e, ciphertext, &c_len, plaintext, *len);

  EVP_EncryptFinal_ex(e, ciphertext + c_len, &f_len);

  *len = c_len + f_len;
  return ciphertext;
}


unsigned char* aes_decrypt(int64_t context, EVP_CIPHER_CTX* e, unsigned char* ciphertext,
                           int* len) {
  /* plaintext will always be equal to or lesser than length of ciphertext*/
  int p_len = *len, f_len = 0;
  auto* plaintext = static_cast<unsigned char*>(malloc(p_len));

  EVP_DecryptInit_ex(e, nullptr, nullptr, nullptr, nullptr);
  EVP_DecryptUpdate(e, plaintext, &p_len, ciphertext, *len);
  EVP_DecryptFinal_ex(e, plaintext + p_len, &f_len);

  *len = p_len + f_len;
  return plaintext;
}
}  // namespace gandiva
