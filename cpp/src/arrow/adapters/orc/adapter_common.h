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

#include "arrow/util/visibility.h"

namespace arrow {

namespace adapters {

namespace orc {

enum WriterId {
  ORC_JAVA_WRITER = 0,
  ORC_CPP_WRITER = 1,
  PRESTO_WRITER = 2,
  SCRITCHLEY_GO = 3,
  TRINO_WRITER = 4,
  UNKNOWN_WRITER = INT32_MAX
};

enum WriterVersion {
  WriterVersion_ORIGINAL = 0,
  WriterVersion_HIVE_8732 = 1,
  WriterVersion_HIVE_4243 = 2,
  WriterVersion_HIVE_12055 = 3,
  WriterVersion_HIVE_13083 = 4,
  WriterVersion_ORC_101 = 5,
  WriterVersion_ORC_135 = 6,
  WriterVersion_ORC_517 = 7,
  WriterVersion_ORC_203 = 8,
  WriterVersion_ORC_14 = 9,
  WriterVersion_MAX = INT32_MAX
};

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_LZ4 = 4,
  CompressionKind_ZSTD = 5,
  CompressionKind_MAX = INT32_MAX
};

enum CompressionStrategy {
  CompressionStrategy_SPEED = 0,
  CompressionStrategy_COMPRESSION
};

enum RleVersion { RleVersion_1 = 0, RleVersion_2 = 1 };

enum BloomFilterVersion {
  // Include both the BLOOM_FILTER and BLOOM_FILTER_UTF8 streams to support
  // both old and new readers.
  ORIGINAL = 0,
  // Only include the BLOOM_FILTER_UTF8 streams that consistently use UTF8.
  // See ORC-101
  UTF8 = 1,
  FUTURE = INT32_MAX
};

class ARROW_EXPORT FileVersion {
 private:
  uint32_t major_version;
  uint32_t minor_version;

 public:
  static const FileVersion& v_0_11();
  static const FileVersion& v_0_12();

  FileVersion(uint32_t major, uint32_t minor)
      : major_version(major), minor_version(minor) {}

  /**
   * Get major version
   */
  uint32_t major() const { return this->major_version; }

  /**
   * Get minor version
   */
  uint32_t minor() const { return this->minor_version; }

  bool operator==(const FileVersion& right) const {
    return this->major_version == right.major() && this->minor_version == right.minor();
  }

  bool operator!=(const FileVersion& right) const { return !(*this == right); }

  std::string ToString() const;
};

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
