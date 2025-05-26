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

#include "parquet/encryption/secure_string.h"

#include <openssl/crypto.h>
#include <openssl/opensslv.h>
#include <utility>
#if defined(_WIN32)
#  include <windows.h>
#endif

#include "arrow/util/span.h"
#include "parquet/encryption/encryption.h"

namespace parquet::encryption {
SecureString::SecureString(SecureString&& secret) noexcept
    : secret_(std::move(secret.secret_)) {
  secret.Dispose();
}
SecureString::SecureString(std::string&& secret) noexcept : secret_(std::move(secret)) {
  SecureClear(&secret);
}
SecureString::SecureString(size_t n, char c) noexcept : secret_(n, c) {}

SecureString& SecureString::operator=(SecureString&& secret) noexcept {
  if (this == &secret) {
    // self-assignment
    return *this;
  }
  Dispose();
  secret_ = std::move(secret.secret_);
  secret.Dispose();
  return *this;
}
SecureString& SecureString::operator=(const SecureString& secret) {
  if (this == &secret) {
    // self-assignment
    return *this;
  }
  Dispose();
  secret_ = secret.secret_;
  return *this;
}
SecureString& SecureString::operator=(std::string&& secret) noexcept {
  Dispose();
  secret_ = std::move(secret);
  SecureClear(&secret);
  return *this;
}

bool SecureString::operator==(const SecureString& other) const {
  return secret_ == other.secret_;
}

bool SecureString::operator!=(const SecureString& other) const {
  return secret_ != other.secret_;
}

bool SecureString::empty() const { return secret_.empty(); }
std::size_t SecureString::size() const { return secret_.size(); }
std::size_t SecureString::length() const { return secret_.length(); }

::arrow::util::span<uint8_t> SecureString::as_span() {
  return {reinterpret_cast<uint8_t*>(secret_.data()), secret_.size()};
}
::arrow::util::span<const uint8_t> SecureString::as_span() const {
  return {reinterpret_cast<const uint8_t*>(secret_.data()), secret_.size()};
}
std::string_view SecureString::as_view() const {
  return {secret_.data(), secret_.size()};
}

void SecureString::Dispose() { SecureClear(&secret_); }
void SecureString::SecureClear(std::string* secret) {
  secret->clear();
  SecureClear(reinterpret_cast<uint8_t*>(secret->data()), secret->capacity());
}
inline void SecureString::SecureClear(uint8_t* data, size_t size) {
  // Heavily borrowed from libb2's `secure_zero_memory` at
  // https://github.com/BLAKE2/libb2/blob/master/src/blake2-impl.h
#if defined(_WIN32)
  SecureZeroMemory(data, size);
#elif defined(__STDC_LIB_EXT1__)
  // memset_s is meant to not be optimized away
  memset_s(data, size, 0, size);
#elif defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x30000000
  OPENSSL_cleanse(data, size);
#elif defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 25))
  // glibc 2.25+ has explicit_bzero
  explicit_bzero(data, size);
#else
  // Try to ensure that a true library call to memset() will be generated
  // by the compiler.
  static const volatile auto memset_v = &memset;
  memset_v(data, 0, size);
  __asm__ __volatile__("" ::"r"(data) : "memory");
#endif
}

}  // namespace parquet::encryption
