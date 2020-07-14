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

#include <cstdint>
#include <vector>

#include "arrow/ipc/type_fwd.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/compression.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

namespace ipc {

// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
// deeply nested schemas, it is expected the user will indicate explicitly the
// maximum allowed recursion depth
constexpr int kMaxNestingDepth = 64;

/// \brief Options for writing Arrow IPC messages
struct ARROW_EXPORT IpcWriteOptions {
  // If true, allow field lengths that don't fit in a signed 32-bit int.
  // Some implementations may not be able to parse such streams.
  bool allow_64bit = false;
  // The maximum permitted schema nesting depth.
  int max_recursion_depth = kMaxNestingDepth;

  // Write padding after memory buffers to this multiple of
  // bytes. Generally 8 or 64
  int32_t alignment = 8;

  /// \brief Write the pre-0.15.0 encapsulated IPC message format
  /// consisting of a 4-byte prefix instead of 8 byte
  bool write_legacy_ipc_format = false;

  /// \brief The memory pool to use for allocations made during IPC writing
  MemoryPool* memory_pool = default_memory_pool();

  /// \brief Compression codec to use for record batch body buffers
  ///
  /// May only be UNCOMPRESSED, LZ4_FRAME and ZSTD.
  Compression::type compression = Compression::UNCOMPRESSED;
  int compression_level = Compression::kUseDefaultCompressionLevel;

  /// \brief Use global CPU thread pool to parallelize any computational tasks
  /// like compression
  bool use_threads = true;

  /// \brief Format version to use for IPC messages and their metadata.
  ///
  /// Presently using V5 version (readable by 1.0.0 and later).
  /// V4 is also available (readable by 0.8.0 and later).
  MetadataVersion metadata_version = MetadataVersion::V5;

  static IpcWriteOptions Defaults();
};

#ifndef ARROW_NO_DEPRECATED_API
using IpcOptions = IpcWriteOptions;
#endif

struct ARROW_EXPORT IpcReadOptions {
  // The maximum permitted schema nesting depth.
  int max_recursion_depth = kMaxNestingDepth;

  /// \brief The memory pool to use for allocations made during IPC writing
  MemoryPool* memory_pool = default_memory_pool();

  /// \brief EXPERIMENTAL: Top-level schema fields to include when
  /// deserializing RecordBatch. If empty, return all deserialized fields
  std::vector<int> included_fields;

  /// \brief Use global CPU thread pool to parallelize any computational tasks
  /// like decompression
  bool use_threads = true;

  static IpcReadOptions Defaults();
};

namespace internal {

Status CheckCompressionSupported(Compression::type codec);

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
