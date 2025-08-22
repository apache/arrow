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

#include "parquet/platform.h"

namespace parquet::encryption {

class PARQUET_EXPORT DecryptorInterface {
 public:
  virtual ~DecryptorInterface() = default;

  /// Calculate the size of the plaintext for a given ciphertext length.
  [[nodiscard]] virtual int32_t PlaintextLength(int32_t ciphertext_len) const = 0;

  /// Calculate the size of the ciphertext for a given plaintext length.
  [[nodiscard]] virtual int32_t CiphertextLength(int32_t plaintext_len) const = 0;

  /// Decrypt the ciphertext and leave the results in the plaintext buffer.
  /// Most implementations will require the key and aad to be provided, but it is up to
  /// each decryptor whether to use them or not.
  virtual int32_t Decrypt(::arrow::util::span<const uint8_t> ciphertext,
                          ::arrow::util::span<const uint8_t> key,
                          ::arrow::util::span<const uint8_t> aad,
                          ::arrow::util::span<uint8_t> plaintext) = 0;
};

}  // namespace parquet::encryption
