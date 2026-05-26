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

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet::fuzzing::internal {

//
// Helper APIs for Parquet encoding roundtrip fuzzing
//

/// Custom header to workaround the lack of parametric fuzzing on OSS-Fuzz
///
/// (see https://github.com/google/oss-fuzz/issues/14437)
///
/// The idea is to prefix the fuzzer payload with a fixed-size header encoding
/// the fuzzer parameters. We also ensure that the seed corpus encompasses all
/// parameter variations.
///
/// In the fuzzer payload, the fixed-size header is followed by the actual fuzz
/// data. That fuzz data is supposed to represent `num_values` of type `type`
/// encoded with the `source_encoding` (of course this may be wrong if the fuzzer
/// mutated the payload).
struct FuzzEncodingHeader {
  /// The encoding the fuzz payload body is encoded with
  Encoding::type source_encoding;
  /// The encoding to roundtrip the decoded values with
  Encoding::type roundtrip_encoding;
  /// The type and type length (byte width) - the latter only for FIXED_LEN_BYTE_ARRAY
  Type::type type;
  int32_t type_length;
  /// The number of encoded values
  int32_t num_values;

  FuzzEncodingHeader(Encoding::type source_encoding, Encoding::type roundtrip_encoding,
                     Type::type type, int type_length, int num_values);
  FuzzEncodingHeader(Encoding::type source_encoding, Encoding::type roundtrip_encoding,
                     const ColumnDescriptor* descr, int num_values);

  /// Serialize as a fixed size header for fuzz corpus generation
  std::string Serialize() const;

  using ParseResult = std::pair<FuzzEncodingHeader, std::span<const uint8_t>>;

  /// Parse header from a fuzzer payload.
  /// Returns a pair of (decoded header, fuzzer body).
  static ::arrow::Result<ParseResult> Parse(std::span<const uint8_t> payload);
};

/// Fuzz a payload encoded as explained in FuzzEncodingHeader
PARQUET_EXPORT ::arrow::Status FuzzEncoding(const uint8_t* data, int64_t size);

PARQUET_EXPORT ColumnDescriptor MakeColumnDescriptor(Type::type type,
                                                     int type_length = -1);

}  // namespace parquet::fuzzing::internal
