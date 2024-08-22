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
#include <fstream>
#include <iostream>
#include <optional>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"
#include "parquet/metadata.h"

namespace parquet {
namespace {
uint32_t ReadLE32(const void* p) {
  uint32_t x = ::arrow::util::SafeLoadAs<uint32_t>(static_cast<const uint8_t*>(p));
  return ::arrow::bit_util::FromLittleEndian(x);
}

void AppendLE32(uint32_t v, std::string* out) {
  v = ::arrow::bit_util::ToLittleEndian(v);
  out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

int DoIt(std::string in, bool scrub, bool debug, std::string out) {
  std::string path;
  auto fs = ::arrow::fs::FileSystemFromUriOrPath(in, &path).ValueOrDie();
  auto file = fs->OpenInputFile(path).ValueOrDie();
  int64_t file_len = file->GetSize().ValueOrDie();
  if (file_len < 8) {
    std::cerr << "File too short: " << in << "\n";
    return 3;
  }
  // First do an opportunistic read of up to 1 MiB to try and get the entire footer.
  int64_t tail_len = std::min(file_len, int64_t{1} << 20);
  std::string tail;
  tail.resize(tail_len);
  char* data = tail.data();
  file->ReadAt(file_len - tail_len, tail_len, data).ValueOrDie();
  if (auto magic = ReadLE32(data + tail_len - 4); magic != ReadLE32("PAR1")) {
    std::cerr << "Not a Parquet file: " << in << "\n";
    return 4;
  }
  uint32_t metadata_len = ReadLE32(data + tail_len - 8);
  if (tail_len >= metadata_len + 8) {
    // The footer is entirely in the initial read. Trim to size.
    tail = tail.substr(tail_len - (metadata_len + 8));
  } else {
    // The footer is larger than the initial read, read again the exact size.
    if (metadata_len > file_len) {
      std::cerr << "File too short: " << in << "\n";
      return 5;
    }
    tail_len = metadata_len + 8;
    tail.resize(tail_len);
    data = tail.data();
    file->ReadAt(file_len - tail_len, tail_len, data).ValueOrDie();
  }
  auto md = FileMetaData::Make(tail.data(), &metadata_len);
  std::string ser = md->SerializeUnencrypted(scrub, debug);
  if (!debug) {
    AppendLE32(static_cast<uint32_t>(ser.size()), &ser);
    ser.append("PAR1", 4);
  }
  std::optional<std::fstream> fout;
  if (!out.empty()) fout.emplace(out, std::ios::out);
  std::ostream& os = fout ? *fout : std::cout;
  if (!os.write(ser.data(), ser.size())) {
    std::cerr << "Failed to write to output file: " << out << "\n";
    return 6;
  }

  return 0;
}
}  // namespace
}  // namespace parquet

static int PrintHelp() {
  std::cerr << R"(Usage: parquet-dump-footer
  -h|--help    Print help and exit
  --no-scrub   Do not scrub potentially confidential metadata
  --debug      Output text represenation of footer for inspection
  --in <uri>   Input file (required): must be an URI or an absolute local path
  --out <path> Output file (optional, default stdout)

  Dump the footer of a Parquet file to stdout or to a file, optionally with
  potentially confidential metadata scrubbed.
)";
  return 1;
}

int main(int argc, char** argv) {
  bool scrub = true;
  bool debug = false;
  std::string in;
  std::string out;
  for (int i = 1; i < argc; i++) {
    char* arg = argv[i];
    if (!std::strcmp(arg, "-h") || !std::strcmp(arg, "--help")) {
      return PrintHelp();
    } else if (!std::strcmp(arg, "--no-scrub")) {
      scrub = false;
    } else if (!std::strcmp(arg, "--debug")) {
      debug = true;
    } else if (!std::strcmp(arg, "--in")) {
      if (i + 1 >= argc) return PrintHelp();
      in = argv[++i];
    } else if (!std::strcmp(arg, "--out")) {
      if (i + 1 >= argc) return PrintHelp();
      out = argv[++i];
    } else {
      // Unknown option.
      return PrintHelp();
    }
  }
  if (in.empty()) return PrintHelp();

  return parquet::DoIt(in, scrub, debug, out);
}
