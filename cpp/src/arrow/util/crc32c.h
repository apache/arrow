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

#include <cstddef>
#include <cstdint>

#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

/// \brief Compute the CRC32C checksum of the given data and return the masked value.
///
/// The CRC32C checksum uses the Castagnoli polynomial and is masked following
/// the Snappy framing specification.  The masking is reversible and is intended
/// to avoid undesirable interactions between the checksum and the data it
/// protects.
///
/// This helper is intended for Snappy framed streams, where each uncompressed
/// chunk is protected by a masked CRC32C of the uncompressed data.
ARROW_EXPORT
uint32_t crc32c_masked(const void* data, std::size_t length);

}  // namespace util
}  // namespace arrow
