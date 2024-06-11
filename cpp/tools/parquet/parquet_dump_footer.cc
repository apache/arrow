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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <random>
#include <utility>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "parquet_types.h"

using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::transport::TMemoryBuffer;
using apache::thrift::transport::TTransport;

namespace {
int PrintHelp() {
  std::cerr << R"(
Usage: parquet-dump-footer
  -h|--help    Print help and exit
  --no-scrub   Do not scrub potentially PII metadata
  --json       Output JSON instead of binary
  --in         Input file: required
  --out        Output file: defaults to stdout

  Dumps the footer of a Parquet file to stdout or a file, optionally with
  potentially PII metadata scrubbed.
)";
  return 1;
}

uint32_t ReadLE32(const void* p) {
  auto* b = reinterpret_cast<const uint8_t*>(p);
  return uint32_t{b[3]} << 24 | uint32_t{b[2]} << 16 | uint32_t{b[1]} << 8 |
         uint32_t{b[0]};
}

void AppendLE32(uint32_t v, std::string* out) {
  out->push_back(v & 0xff);
  out->push_back((v >> 8) & 0xff);
  out->push_back((v >> 16) & 0xff);
  out->push_back((v >> 24) & 0xff);
}

std::pair<char*, size_t> MmapFile(const std::string& fname) {
  int fd = open(fname.c_str(), O_RDONLY);
  if (fd < 0) return {nullptr, 0};
  struct stat st;
  if (fstat(fd, &st)) return {nullptr, 0};
  size_t sz = st.st_size;
  void* map = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
  return {map == MAP_FAILED ? nullptr : static_cast<char*>(map), sz};
}

template <typename T>
bool Deserialize(const char* data, size_t len, T* obj) {
  TMemoryBuffer buf(reinterpret_cast<uint8_t*>(const_cast<char*>(data)), len);
  TCompactProtocol proto(std::shared_ptr<TTransport>(&buf, [](auto*) {}));
  try {
    obj->read(&proto);
    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to deserialize: " << e.what() << "\n";
    return false;
  }
}

template <typename T>
bool Serialize(const T& obj, std::string* out) {
  TMemoryBuffer buf;
  TCompactProtocol proto(std::shared_ptr<TTransport>(&buf, [](auto*) {}));
  try {
    obj.write(&proto);
    uint8_t* data;
    uint32_t len;
    buf.getBuffer(&data, &len);
    out->assign(reinterpret_cast<char*>(data), len);
    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to serialize: " << e.what() << "\n";
    return false;
  }
}

void Scrub(std::string* s) {
  static char pool[4096];
  static std::mt19937 rng(std::random_device{}());
  static const bool kPoolInit = [] {
    std::uniform_int_distribution<> caps(65, 90);
    for (size_t i = 0; i < sizeof(pool); i++) pool[i] = caps(rng);
    return true;
  }();
  (void)kPoolInit;

  const size_t n = s->size();
  s->clear();
  while (s->size() < n) {
    size_t m = std::min(n, sizeof(pool) / 2);
    std::uniform_int_distribution<> start(0, sizeof(pool) / 2);
    s->append(&pool[start(rng)], m);
  }
}

void Scrub(parquet::format::FileMetaData* md) {
  for (auto& s : md->schema) {
    Scrub(&s.name);
  }
  for (auto& r : md->row_groups) {
    for (auto& c : r.columns) {
      Scrub(&c.file_path);
      if (c.__isset.meta_data) {
        auto& m = c.meta_data;
        for (auto& p : m.path_in_schema) Scrub(&p);
        for (auto& kv : m.key_value_metadata) {
          Scrub(&kv.key);
          Scrub(&kv.value);
        }
        Scrub(&m.statistics.max_value);
        Scrub(&m.statistics.min_value);
        Scrub(&m.statistics.min);
        Scrub(&m.statistics.max);
      }

      if (c.crypto_metadata.__isset.ENCRYPTION_WITH_COLUMN_KEY) {
        auto& m = c.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY;
        for (auto& p : m.path_in_schema) Scrub(&p);
        Scrub(&m.key_metadata);
      }
      Scrub(&c.encrypted_column_metadata);
    }
  }
  for (auto& kv : md->key_value_metadata) {
    Scrub(&kv.key);
    Scrub(&kv.value);
  }
  Scrub(&md->footer_signing_key_metadata);
}
}  // namespace

int main(int argc, char** argv) {
  bool help = false;
  bool scrub = true;
  bool json = false;
  std::string in;
  std::string out;
  for (int i = 1; i < argc; i++) {
    char* arg = argv[i];
    help |= !std::strcmp(arg, "-h") || !std::strcmp(arg, "--help");
    scrub &= std::strcmp(arg, "--no-scrub");
    json |= !std::strcmp(arg, "--json");
    if (!std::strcmp(arg, "--in")) {
      if (i + 1 >= argc) return PrintHelp();
      in = argv[++i];
    }
    if (!std::strcmp(arg, "--out")) {
      if (i + 1 >= argc) return PrintHelp();
      out = argv[++i];
    }
  }
  if (help || in.empty()) return PrintHelp();
  auto [data, len] = MmapFile(in);

  if (len == 0) {
    std::cerr << "Failed to read file: " << in << "\n";
    return 3;
  }
  if (len < 8 || ReadLE32(data + len - 4) != ReadLE32("PAR1")) {
    std::cerr << "Not a Parquet file: " << in << "\n";
    return 4;
  }
  size_t footer_len = ReadLE32(data + len - 8);
  if (footer_len > len - 8) {
    std::cerr << "Invalid footer length: " << footer_len << "\n";
    return 5;
  }
  char* footer = data + len - 8 - footer_len;
  parquet::format::FileMetaData md;
  if (!Deserialize(footer, footer_len, &md)) return 5;
  if (scrub) Scrub(&md);

  std::string ser;
  if (json) {
    if (out.empty()) {
      md.printTo(std::cout);
    } else {
      std::fstream fout(out, std::ios::out);
      md.printTo(fout);
    }
  } else {
    int out_fd = out.empty() ? 1 : open(out.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
      std::cerr << "Failed to open output file: " << out << "\n";
      return 2;
    }
    if (!Serialize(md, &ser)) return 6;
    AppendLE32(ser.size(), &ser);
    ser.append("PAR1", 4);
    if (write(out_fd, ser.data(), ser.size()) != static_cast<ssize_t>(ser.size())) {
      std::cerr << "Failed to write to output file: " << out << "\n";
      return 7;
    }
    close(out_fd);
  }

  return 0;
}
