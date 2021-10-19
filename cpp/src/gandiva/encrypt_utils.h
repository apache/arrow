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

#include <openssl/aes.h>
#include <openssl/evp.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "gandiva/visibility.h"

namespace gandiva {

/**
 * Initialize aes encryption
 *
 * This code is based on the implementation of Saju Pillai found here:
 * https://github.com/saju/misc/blob/master/misc/openssl_aes.c
 **/
int32_t aes_init(int64_t context, unsigned char* key_data, int key_data_len, unsigned char *salt, EVP_CIPHER_CTX* e_ctx, EVP_CIPHER_CTX* d_ctx);


/**
 * Encrypt data using aes algorithm
 *
 * This code is based on the implementation of Saju Pillai found here:
 * https://github.com/saju/misc/blob/master/misc/openssl_aes.c
 **/
unsigned char* aes_encrypt(int64_t context, EVP_CIPHER_CTX* e, unsigned char* plaintext,
                           int* len);

/**
 * Decrypt data using aes algorithm
 *
 * This code is based on the implementation of Saju Pillai found here:
 * https://github.com/saju/misc/blob/master/misc/openssl_aes.c
 **/
unsigned char* aes_decrypt(int64_t context, EVP_CIPHER_CTX* e, unsigned char* ciphertext,
                           int* len);

}  // namespace gandiva
