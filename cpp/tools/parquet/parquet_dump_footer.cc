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

#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <random>

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "arrow/filesystem/filesystem.h"
#include "parquet/thrift_internal.h"
#include "generated/parquet_types.h"

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

template <typename T>
bool Deserialize(const char* data, uint32_t len, T* obj) {
  parquet::ThriftDeserializer des(10 << 20, 10 << 20);
  try {
    des.DeserializeMessage(reinterpret_cast<const uint8_t*>(data), &len, obj);
    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to deserialize: " << e.what() << "\n";
    return false;
  }
}

template <typename T>
bool Serialize(const T& obj, std::string* out) {
  parquet::ThriftSerializer ser(10 << 20);
  try {
    ser.SerializeToString(&obj, out);
    return true;
  } catch (const std::exception& e) {
    std::cerr << "Failed to serialize: " << e.what() << "\n";
    return false;
  }
}

void Scrub(std::string* s) {
  static char pool[4096];
  static std::mt19937 rng(std::random_device {}());
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

// Returns:
// - 0 on success
// - -1 on error
// - the size of the footer if tail is too small
int64_t ParseFooter(const std::string& tail, parquet::format::FileMetaData* md) {
  if (tail.size() > std::numeric_limits<int32_t>::max()) return -1;

  const char* p = tail.data();
  const int32_t n = static_cast<int32_t>(tail.size());
  int32_t len = ReadLE32(p + n - 8);
  if (len > n - 8) return len;

  if (!Deserialize(tail.data() + n - 8 - len, len, md)) return -1;
  return 0;
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
    scrub &= !!std::strcmp(arg, "--no-scrub");
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
  std::string path;
  auto fs = arrow::fs::FileSystemFromUriOrPath(in, &path).ValueOrDie();
  auto file = fs->OpenInputFile(path).ValueOrDie();
  int64_t file_len = file->GetSize().ValueOrDie();
  if (file_len < 8) {
    std::cerr << "File too short: " << in << "\n";
    return 3;
  }
  int64_t tail_len = std::min(file_len, int64_t{1} << 20);
  std::string tail;
  tail.resize(tail_len);
  char* data = tail.data();
  file->ReadAt(file_len - tail_len, tail_len, data).ValueOrDie();
  if (ReadLE32(data + tail_len - 4) != ReadLE32("PAR1")) {
    std::cerr << "Not a Parquet file: " << in << "\n";
    return 4;
  }
  parquet::format::FileMetaData md;
  int64_t res = ParseFooter(tail, &md);
  if (res < 0) {
    std::cerr << "Failed to parse footer: " << in << "\n";
    return 5;
  } else if (res > 0) {
    if (res > file_len) {
      std::cerr << "File too short: " << in << "\n";
      return 6;
    }
    tail_len = res + 8;
    tail.resize(tail_len);
    data = tail.data();
    file->ReadAt(file_len - tail_len, tail_len, data).ValueOrDie();
  }
  if (ParseFooter(tail, &md) != 0) {
    std::cerr << "Failed to parse footer: " << in << "\n";
    return 7;
  }

  if (scrub) Scrub(&md);

  std::optional<std::fstream> fout;
  if (json) {
    if (!out.empty()) fout.emplace(out, std::ios::out);
    std::ostream& os = fout ? *fout : std::cout;
    md.printTo(os);
  } else {
    if (!out.empty()) fout.emplace(out, std::ios::out | std::ios::binary);
    std::ostream& os = fout ? *fout : std::cout;
    if (!os) {
      std::cerr << "Failed to open output file: " << out << "\n";
      return 8;
    }
    std::string ser;
    if (!Serialize(md, &ser)) return 6;
    AppendLE32(static_cast<uint32_t>(ser.size()), &ser);
    ser.append("PAR1", 4);
    if (!os.write(ser.data(), ser.size())) {
      std::cerr << "Failed to write to output file: " << out << "\n";
      return 9;
    }
  }

  return 0;
}
