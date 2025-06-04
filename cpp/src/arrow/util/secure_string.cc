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

// __STDC_WANT_LIB_EXT1__ and string.h are required by memset_s:
// https://en.cppreference.com/w/c/string/byte/memset
#define __STDC_WANT_LIB_EXT1__ 1
#include <string.h>
#include <utility>

#if defined(ARROW_USE_OPENSSL)
#  include <openssl/crypto.h>
#  include <openssl/opensslv.h>
#endif

#include "arrow/util/windows_compatibility.h"
#if defined(_WIN32)
#  include <windows.h>
#endif

#include "arrow/util/logging.h"
#include "arrow/util/secure_string.h"
#include "arrow/util/span.h"

namespace arrow::util {

/// Note:
/// A string std::string is securely moved into a SecureString in two steps:
/// 1. the std::string is moved via std::move(string)
/// 2. the std::string is securely cleared
///
/// The std::move has two different effects, depending on the size of the string.
/// A very short string (called local string) stores the string in a local buffer,
/// a long string stores a pointer to allocated memory that stores the string.
///
/// If the string is a small string, std::move copies the local buffer.
/// If the string is a long string, std::move moves the pointer and then resets the
/// string size to 0 (which turns the string into a local string).
///
/// In both cases, after a std::move(string), the string uses the local buffer.
///
/// Thus, after a std::move(string), calling SecureClear(std::string*) only
/// securely clears the **local buffer** of the string. Therefore, std::move(string)
/// must move the pointer of long string into SecureString (which later clears the
/// string). Otherwise, the content of the string cannot be securely cleared.
///
/// This condition is checked by secure_move.

void secure_move(std::string& string, std::string& dst) {
  auto ptr = string.data();
  dst = std::move(string);

  // We require the buffer address string.data() to remain (not be freed),
  // or reused by dst. Otherwise, we cannot securely clear string after this move
  ARROW_CHECK(string.data() == ptr || dst.data() == ptr);
}

SecureString::SecureString(SecureString&& other) noexcept {
  secure_move(other.secret_, secret_);
  other.Dispose();
}

SecureString::SecureString(std::string&& secret) noexcept {
  secure_move(secret, secret_);
  SecureClear(&secret);
}

SecureString::SecureString(size_t n, char c) noexcept : secret_(n, c) {}

SecureString& SecureString::operator=(SecureString&& other) noexcept {
  if (this == &other) {
    // self-assignment
    return *this;
  }
  Dispose();
  secure_move(other.secret_, secret_);
  other.Dispose();
  return *this;
}

SecureString& SecureString::operator=(const SecureString& other) {
  if (this == &other) {
    // self-assignment
    return *this;
  }
  Dispose();
  secret_ = other.secret_;
  return *this;
}

SecureString& SecureString::operator=(std::string&& secret) noexcept {
  Dispose();
  secure_move(secret, secret_);
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

std::size_t SecureString::capacity() const { return secret_.capacity(); }

span<uint8_t> SecureString::as_span() {
  return {reinterpret_cast<uint8_t*>(secret_.data()), secret_.size()};
}

span<const uint8_t> SecureString::as_span() const {
  return {reinterpret_cast<const uint8_t*>(secret_.data()), secret_.size()};
}

std::string_view SecureString::as_view() const {
  return {secret_.data(), secret_.size()};
}

void SecureString::Dispose() { SecureClear(&secret_); }

void SecureString::SecureClear(std::string* secret) {
  // in case of non-local strings (long strings), this order is vital
  // first clear the string buffer
  SecureClear(reinterpret_cast<uint8_t*>(secret->data()), secret->capacity());
  // then reset the string size (moves from the non-local to the local string buffer)
  secret->clear();
}

inline void SecureString::SecureClear(uint8_t* data, size_t size) {
  // There is various prior art for this:
  // https://www.cryptologie.net/article/419/zeroing-memory-compiler-optimizations-and-memset_s/
  // - libb2's `secure_zero_memory` at
  // https://github.com/BLAKE2/libb2/blob/30d45a17c59dc7dbf853da3085b71d466275bd0a/src/blake2-impl.h#L140-L160
  // - libsodium's `sodium_memzero` at
  // https://github.com/jedisct1/libsodium/blob/be58b2e6664389d9c7993b55291402934b43b3ca/src/libsodium/sodium/utils.c#L78:L101
  // Note:
  // https://www.daemonology.net/blog/2014-09-06-zeroing-buffers-is-insufficient.html
#if defined(_WIN32)
  // SecureZeroMemory is meant to not be optimized away
  SecureZeroMemory(data, size);
#elif defined(__STDC_LIB_EXT1__)
  // memset_s is meant to not be optimized away
  memset_s(data, size, 0, size);
#elif defined(OPENSSL_VERSION_NUMBER) && OPENSSL_VERSION_NUMBER >= 0x30000000
  // rely on some implementation in OpenSSL cryptographic library
  OPENSSL_cleanse(data, size);
#elif defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 25))
  // explicit_bzero is meant to not be optimized away
  explicit_bzero(data, size);
#else
  // Volatile pointer to memset function is an attempt to avoid
  // that the compiler optimizes away the memset function call.
  // pretty much what OPENSSL_cleanse above does
  // https://github.com/openssl/openssl/blob/3423c30db3aa044f46e1f0270e2ecd899415bf5f/crypto/mem_clr.c#L22
  static const volatile auto memset_v = &memset;
  memset_v(data, 0, size);

#  if defined(__GNUC__) || defined(__clang__)
  // __asm__ only supported by GCC and Clang
  // not supported by MSVC on the ARM and x64 processors
  // https://en.cppreference.com/w/c/language/asm.html
  // https://en.cppreference.com/w/cpp/language/asm.html

  // Additional attempt on top of volatile memset_v above
  // to avoid that the compiler optimizes away the memset function call.
  // Assembler code that tells the compiler 'data' has side effects.
  // https://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html:
  // - "volatile": the asm produces side effects
  // - "memory": effectively forms a read/write memory barrier for the compiler
  __asm__ __volatile__(""          /* no actual code */
                       :           /* no output */
                       : "r"(data) /* input */
                       : "memory" /* memory side effects beyond input and output */);
#  endif
#endif
}

}  // namespace arrow::util
