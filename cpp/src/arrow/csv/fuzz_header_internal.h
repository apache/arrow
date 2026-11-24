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
#include <cstring>
#include <span>
#include <string>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow::csv::internal {

// The CSV fuzz target prepends a packed header to the fuzz payload to explore
// the CSV reader's option space (see
// https://github.com/google/oss-fuzz/issues/14437 for why fuzz target
// parameters have to be encoded inside the payload itself).
//
// The header starts with a magic number so that the fuzz target can detect
// whether an input was generated with this scheme. If the magic number
// matches, the header is parsed and the rest of the payload is read with the
// corresponding options. Otherwise (e.g. for legacy corpus entries), the
// whole input is read with the fuzzer's historical hardcoded options.

constexpr auto kFuzzCsvHeaderSize = 16;

inline constexpr std::array<char, 8> kFuzzCsvMagic = {'A', 'R', 'R', 'O',
                                                      'W', 'C', 'S', 'V'};

// Candidate values for the options encoded in FuzzCsvHeader below. The
// corresponding option fields store an index in these arrays (modulo the
// array length, so that all header values lead to a well-defined option set).
inline constexpr std::array<std::string_view, 6> kFuzzCsvDelimiters = {
    ",", "\t", ";", "|", " ", "@@"};
inline constexpr std::array<char, 2> kFuzzCsvQuoteChars = {'"', '\''};
inline constexpr std::array<char, 2> kFuzzCsvEscapeChars = {'\\', '"'};
// Small cardinality thresholds exercise dictionary truncation and fallback
// more often than large ones.
inline constexpr std::array<int32_t, 3> kFuzzCsvDictMaxCardinalities = {5, 20, 100};

// NOTE: The option bit-fields are grouped into dedicated packed structures
// of one byte each (verified by the static_asserts below), so that the
// layout is unambiguous.
ARROW_PACKED_START(struct, FuzzCsvParseGroup1) {
  // Index (modulo length) in kFuzzCsvDelimiters
  uint8_t delimiter_id : 3;
  uint8_t quoting : 1;
  // Index in kFuzzCsvQuoteChars
  uint8_t quote_char_id : 1;
  uint8_t double_quote : 1;
  uint8_t escaping : 1;
  // Index in kFuzzCsvEscapeChars
  uint8_t escape_char_id : 1;
};
ARROW_PACKED_END

ARROW_PACKED_START(struct, FuzzCsvParseGroup2) {
  uint8_t newlines_in_values : 1;
  uint8_t ignore_empty_lines : 1;
  uint8_t pad_short_rows : 1;
  uint8_t ignore_extra_columns : 1;
  uint8_t reserved_1 : 4;
};
ARROW_PACKED_END

ARROW_PACKED_START(struct, FuzzCsvReadGroup) {
  // 0 or 1
  uint8_t skip_rows : 1;
  uint8_t autogenerate_column_names : 1;
  uint8_t reserved_2 : 6;
};
ARROW_PACKED_END

ARROW_PACKED_START(struct, FuzzCsvConvertGroup1) {
  uint8_t strings_can_be_null : 1;
  uint8_t quoted_strings_can_be_null : 1;
  uint8_t auto_dict_encode : 1;
  // Index (modulo length) in kFuzzCsvDictMaxCardinalities
  uint8_t auto_dict_max_cardinality_id : 2;
  // 0: '.', 1: ','
  uint8_t decimal_point_is_comma : 1;
  uint8_t check_utf8 : 1;
  // Add a strptime timestamp parser with the "%Y-%m-%d" format
  uint8_t add_strptime_ymd_parser : 1;
};
ARROW_PACKED_END

ARROW_PACKED_START(struct, FuzzCsvConvertGroup2) {
  // Add a strptime timestamp parser with the "%m/%d/%Y" format
  uint8_t add_strptime_mdy_parser : 1;
  uint8_t reserved_3 : 7;
};
ARROW_PACKED_END

static_assert(sizeof(FuzzCsvParseGroup1) == 1, "FuzzCsvParseGroup1 not packed");
static_assert(sizeof(FuzzCsvParseGroup2) == 1, "FuzzCsvParseGroup2 not packed");
static_assert(sizeof(FuzzCsvReadGroup) == 1, "FuzzCsvReadGroup not packed");
static_assert(sizeof(FuzzCsvConvertGroup1) == 1, "FuzzCsvConvertGroup1 not packed");
static_assert(sizeof(FuzzCsvConvertGroup2) == 1, "FuzzCsvConvertGroup2 not packed");

ARROW_PACKED_START(struct, FuzzCsvHeader) {
  char magic[kFuzzCsvMagic.size()];
  FuzzCsvParseGroup1 parse_1;
  FuzzCsvParseGroup2 parse_2;
  FuzzCsvReadGroup read;
  FuzzCsvConvertGroup1 convert_1;
  FuzzCsvConvertGroup2 convert_2;
  // Unused bytes, should be zero
  std::array<uint8_t, kFuzzCsvHeaderSize - sizeof(magic) - 5> reserved;
};
ARROW_PACKED_END

static_assert(sizeof(FuzzCsvHeader) == kFuzzCsvHeaderSize,
              "FuzzCsvHeader not packed to the expected size");

// Fill in the magic number in `header` (all serialized headers should start
// with it).
inline void SetFuzzCsvMagic(FuzzCsvHeader& header) {
  std::memcpy(header.magic, kFuzzCsvMagic.data(), kFuzzCsvMagic.size());
}

// Serialize the header, for prepending to a fuzz payload (e.g. when
// generating seed corpus files).
inline std::string SerializeFuzzCsvHeader(const FuzzCsvHeader& header) {
  return std::string(reinterpret_cast<const char*>(&header), kFuzzCsvHeaderSize);
}

// Parse and validate the header at the beginning of `payload`.
// Status::Invalid is returned if the payload is too small, or if it doesn't
// start with the expected magic and padding (e.g. for a legacy input).
inline Result<FuzzCsvHeader> ParseFuzzCsvHeader(std::span<const uint8_t> payload) {
  auto invalid_payload = [] {
    return Status::Invalid("Invalid CSV fuzz payload header");
  };

  if (payload.size() < kFuzzCsvHeaderSize) {
    return invalid_payload();
  }
  FuzzCsvHeader header;
  std::memcpy(&header, payload.data(), kFuzzCsvHeaderSize);
  // We are strict in what we accept because we don't want the fuzzer to
  // explore pointless variations.
  if (std::memcmp(header.magic, kFuzzCsvMagic.data(), kFuzzCsvMagic.size()) != 0 ||
      header.parse_2.reserved_1 != 0 || header.read.reserved_2 != 0 ||
      header.convert_2.reserved_3 != 0 ||
      header.reserved != decltype(header.reserved){}) {
    return invalid_payload();
  }
  return header;
}

}  // namespace arrow::csv::internal
