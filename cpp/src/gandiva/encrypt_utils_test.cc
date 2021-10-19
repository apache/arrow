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

#include "gandiva/encrypt_utils.h"

#include <gtest/gtest.h>

#include <unordered_set>

#include "gandiva/execution_context.h"

TEST(TestShaEncryptUtils, TestAesEncryptDecrypt) {
  gandiva::ExecutionContext ctx;
  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  EVP_CIPHER_CTX* en = EVP_CIPHER_CTX_new();
  EVP_CIPHER_CTX* de = EVP_CIPHER_CTX_new();

  unsigned int salt[] = {12345, 54321};

  const unsigned char* key_data;
  int key_data_len;
  const char* input = "a";

  const char* key_vec = {"my key"};
  key_data = reinterpret_cast<const unsigned char*>(key_vec);
  key_data_len = static_cast<int>(strlen(key_vec));

  gandiva::aes_init(ctx_ptr, const_cast<unsigned char*>(key_data), key_data_len, (unsigned char *)&salt, en, de);

  char* plaintext;
  unsigned char* ciphertext;
  int olen, len;

  olen = len = static_cast<int>(strlen(input) + 1);

  ciphertext = gandiva::aes_encrypt(ctx_ptr, en, (unsigned char*)input, &len);
  plaintext =
      reinterpret_cast<char*>(gandiva::aes_decrypt(ctx_ptr, de, ciphertext, &len));

  EXPECT_EQ(strncmp(plaintext, input, olen), 0);

  free(ciphertext);
  free(plaintext);

  const char* input_two = "abcd";
  char* plaintext_two;
  unsigned char* ciphertext_two;
  int olen_two, len_two;

  olen_two = len_two = static_cast<int>(strlen(input_two) + 1);

  ciphertext_two = gandiva::aes_encrypt(ctx_ptr, en, (unsigned char*)input_two, &len_two);
  plaintext_two = reinterpret_cast<char*>(
      gandiva::aes_decrypt(ctx_ptr, de, ciphertext_two, &len_two));

  EXPECT_EQ(strncmp(plaintext_two, input_two, olen_two), 0);

  free(ciphertext_two);
  free(plaintext_two);

  const char* input_three = "some bigger \n test here";
  char* plaintext_three;
  unsigned char* ciphertext_three;
  int olen_three, len_three;

  olen_three = len_three = static_cast<int>(strlen(input_three) + 1);

  ciphertext_three = gandiva::aes_encrypt(ctx_ptr, en, (unsigned char*)input_three, &len_three);
  plaintext_three = reinterpret_cast<char*>(
      gandiva::aes_decrypt(ctx_ptr, de, ciphertext_three, &len_three));

  EXPECT_EQ(strncmp(plaintext_three, input_three, olen_three), 0);

  free(ciphertext_three);
  free(plaintext_three);

  EVP_CIPHER_CTX_cleanup(en);
  EVP_CIPHER_CTX_cleanup(de);
}
