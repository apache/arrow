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

#include "arrow/util/utf8.h"

#include <cstdint>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "arrow/result.h"
#include "arrow/util/logging.h"
#include "arrow/util/utf8_internal.h"
#include "arrow/vendored/utfcpp/checked.h"

// Can be defined by utfcpp
#ifdef NOEXCEPT
#  undef NOEXCEPT
#endif

namespace arrow {
namespace util {
namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

// clang-format off
const uint8_t utf8_small_table[] = { // NOLINT
  // The first part of the table maps bytes to character classes that
  // to reduce the size of the transition table and create bitmasks.
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,  9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,  // NOLINT
   7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  // NOLINT
   8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  // NOLINT
  10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8,  // NOLINT

  // The second part is a transition table that maps a combination
  // of a state of the automaton and a character class to a state.
  // Character classes are between 0 and 11, states are multiples of 12.
   0,12,24,36,60,96,84,12,12,12,48,72, 12,12,12,12,12,12,12,12,12,12,12,12,  // NOLINT
  12, 0,12,12,12,12,12, 0,12, 0,12,12, 12,24,12,12,12,12,12,24,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,24,12,12,12,12, 12,24,12,12,12,12,12,12,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,36,12,36,12,12, 12,36,12,12,12,12,12,36,12,36,12,12,  // NOLINT
  12,36,12,12,12,12,12,12,12,12,12,12,  // NOLINT
};
// clang-format on

uint16_t utf8_large_table[9 * 256] = {0xffff};

const uint8_t utf8_byte_size_table[16] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 4};

static void InitializeLargeTable() {
  for (uint32_t state = 0; state < 9; ++state) {
    for (uint32_t byte = 0; byte < 256; ++byte) {
      uint32_t byte_class = utf8_small_table[byte];
      uint8_t next_state = utf8_small_table[256 + state * 12 + byte_class] / 12;
      DCHECK_LT(next_state, 9);
      utf8_large_table[state * 256 + byte] = static_cast<uint16_t>(next_state * 256);
    }
  }
}

ARROW_EXPORT void CheckUTF8Initialized() {
  DCHECK_EQ(utf8_large_table[0], 0)
      << "InitializeUTF8() must be called before calling UTF8 routines";
}

}  // namespace internal

static std::once_flag utf8_initialized;

void InitializeUTF8() {
  std::call_once(utf8_initialized, internal::InitializeLargeTable);
}

bool ValidateUTF8(const uint8_t* data, int64_t size) {
  return ValidateUTF8Inline(data, size);
}

bool ValidateUTF8(std::string_view str) { return ValidateUTF8Inline(str); }

static const uint8_t kBOM[] = {0xEF, 0xBB, 0xBF};

Result<const uint8_t*> SkipUTF8BOM(const uint8_t* data, int64_t size) {
  int64_t i;
  for (i = 0; i < static_cast<int64_t>(sizeof(kBOM)); ++i) {
    if (size == 0) {
      if (i == 0) {
        // Empty string
        return data;
      } else {
        return Status::Invalid("UTF8 string too short (truncated byte order mark?)");
      }
    }
    if (data[i] != kBOM[i]) {
      // BOM not found
      return data;
    }
    --size;
  }
  // BOM found
  return data + i;
}

namespace {

// Some platforms (such as old MinGWs) don't have the <codecvt> header,
// so call into a vendored utf8 implementation instead.

std::wstring UTF8ToWideStringInternal(std::string_view source) {
  std::wstring ws;
#if WCHAR_MAX > 0xFFFF
  ::utf8::utf8to32(source.begin(), source.end(), std::back_inserter(ws));
#else
  ::utf8::utf8to16(source.begin(), source.end(), std::back_inserter(ws));
#endif
  return ws;
}

std::string WideStringToUTF8Internal(const std::wstring& source) {
  std::string s;
#if WCHAR_MAX > 0xFFFF
  ::utf8::utf32to8(source.begin(), source.end(), std::back_inserter(s));
#else
  ::utf8::utf16to8(source.begin(), source.end(), std::back_inserter(s));
#endif
  return s;
}

std::string UTF16StringToUTF8Internal(std::u16string_view source) {
  std::string s;
  ::utf8::utf16to8(source.begin(), source.end(), std::back_inserter(s));
  return s;
}

std::u16string UTF8StringToUTF16Internal(std::string_view source) {
  std::u16string s;
  ::utf8::utf8to16(source.begin(), source.end(), std::back_inserter(s));
  return s;
}

}  // namespace

Result<std::wstring> UTF8ToWideString(std::string_view source) {
  try {
    return UTF8ToWideStringInternal(source);
  } catch (std::exception& e) {
    return Status::Invalid(e.what());
  }
}

ARROW_EXPORT Result<std::string> WideStringToUTF8(const std::wstring& source) {
  try {
    return WideStringToUTF8Internal(source);
  } catch (std::exception& e) {
    return Status::Invalid(e.what());
  }
}

Result<std::string> UTF16StringToUTF8(std::u16string_view source) {
  try {
    return UTF16StringToUTF8Internal(source);
  } catch (std::exception& e) {
    return Status::Invalid(e.what());
  }
}

Result<std::u16string> UTF8StringToUTF16(std::string_view source) {
  try {
    return UTF8StringToUTF16Internal(source);
  } catch (std::exception& e) {
    return Status::Invalid(e.what());
  }
}

}  // namespace util
}  // namespace arrow
