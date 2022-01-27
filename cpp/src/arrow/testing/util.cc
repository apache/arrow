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

#include "arrow/testing/util.h"

#include <chrono>
#include <cstring>
#include <random>

#ifdef _WIN32
// clang-format off
// (prevent include reordering)
#include "arrow/util/windows_compatibility.h"
#include <winsock2.h>
// clang-format on
#else
#include <arpa/inet.h>   // IWYU pragma: keep
#include <netinet/in.h>  // IWYU pragma: keep
#include <sys/socket.h>  // IWYU pragma: keep
#include <sys/stat.h>    // IWYU pragma: keep
#include <sys/types.h>   // IWYU pragma: keep
#include <sys/wait.h>    // IWYU pragma: keep
#include <unistd.h>      // IWYU pragma: keep
#endif

#include "arrow/table.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/pcg_random.h"

namespace arrow {

using random::pcg32_fast;

uint64_t random_seed() {
  return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

void random_null_bytes(int64_t n, double pct_null, uint8_t* null_bytes) {
  const int random_seed = 0;
  pcg32_fast gen(random_seed);
  ::arrow::random::uniform_real_distribution<double> d(0.0, 1.0);
  std::generate(null_bytes, null_bytes + n,
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

void random_is_valid(int64_t n, double pct_null, std::vector<bool>* is_valid,
                     int random_seed) {
  pcg32_fast gen(random_seed);
  ::arrow::random::uniform_real_distribution<double> d(0.0, 1.0);
  is_valid->resize(n, false);
  std::generate(is_valid->begin(), is_valid->end(),
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

void random_bytes(int64_t n, uint32_t seed, uint8_t* out) {
  pcg32_fast gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
}

std::string random_string(int64_t n, uint32_t seed) {
  std::string s;
  s.resize(static_cast<size_t>(n));
  random_bytes(n, seed, reinterpret_cast<uint8_t*>(&s[0]));
  return s;
}

void random_ascii(int64_t n, uint32_t seed, uint8_t* out) {
  rand_uniform_int(n, seed, static_cast<int32_t>('A'), static_cast<int32_t>('z'), out);
}

int64_t CountNulls(const std::vector<uint8_t>& valid_bytes) {
  return static_cast<int64_t>(std::count(valid_bytes.cbegin(), valid_bytes.cend(), '\0'));
}

Status MakeRandomByteBuffer(int64_t length, MemoryPool* pool,
                            std::shared_ptr<ResizableBuffer>* out, uint32_t seed) {
  ARROW_ASSIGN_OR_RAISE(auto result, AllocateResizableBuffer(length, pool));
  random_bytes(length, seed, result->mutable_data());
  *out = std::move(result);
  return Status::OK();
}

Status GetTestResourceRoot(std::string* out) {
  const char* c_root = std::getenv("ARROW_TEST_DATA");
  if (!c_root) {
    return Status::IOError(
        "Test resources not found, set ARROW_TEST_DATA to <repo root>/testing/data");
  }
  *out = std::string(c_root);
  return Status::OK();
}

int GetListenPort() {
  // Get a new available port number by binding a socket to an ephemeral port
  // and then closing it.  Since ephemeral port allocation tends to avoid
  // reusing port numbers, this should give a different port number
  // every time, even across processes.
  struct sockaddr_in sin;
#ifdef _WIN32
  SOCKET sock_fd;
  auto sin_len = static_cast<int>(sizeof(sin));
  auto errno_message = []() -> std::string {
    return internal::WinErrorMessage(WSAGetLastError());
  };
#else
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
  int sock_fd;
  auto sin_len = static_cast<socklen_t>(sizeof(sin));
  auto errno_message = []() -> std::string { return internal::ErrnoMessage(errno); };
#endif

#ifdef _WIN32
  WSADATA wsa_data;
  if (WSAStartup(0x0202, &wsa_data) != 0) {
    ARROW_LOG(FATAL) << "Failed to initialize Windows Sockets";
  }
#endif

  sock_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock_fd == INVALID_SOCKET) {
    Status::IOError("Failed to create TCP socket: ", errno_message()).Abort();
  }
  // First bind to ('0.0.0.0', 0)
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  if (bind(sock_fd, reinterpret_cast<struct sockaddr*>(&sin), sin_len) == SOCKET_ERROR) {
    Status::IOError("bind() failed: ", errno_message()).Abort();
  }
  // Then get actual bound port number
  if (getsockname(sock_fd, reinterpret_cast<struct sockaddr*>(&sin), &sin_len) ==
      SOCKET_ERROR) {
    Status::IOError("getsockname() failed: ", errno_message()).Abort();
  }
  int port = ntohs(sin.sin_port);
#ifdef _WIN32
  closesocket(sock_fd);
#else
  close(sock_fd);
#endif

  return port;
}

const std::vector<std::shared_ptr<DataType>>& all_dictionary_index_types() {
  static std::vector<std::shared_ptr<DataType>> types = {
      int8(), uint8(), int16(), uint16(), int32(), uint32(), int64(), uint64()};
  return types;
}

}  // namespace arrow
