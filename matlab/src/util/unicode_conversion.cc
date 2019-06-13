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

#include <locale> /* for std::wstring_convert */
#include <codecvt> /* for std::codecvt_utf8_utf16 */

#include "unicode_conversion.h"

namespace arrow {
namespace matlab {
namespace util {

mxArray* ConvertUTF8StringToUTF16CharMatrix(const std::string& utf8_string) {
  // Get pointers to the start and end of the std::string data.
  const char* string_start = utf8_string.c_str();
  const char* string_end = string_start + utf8_string.length();

  // Due to this issue on MSVC: https://stackoverflow.com/q/32055357 we cannot 
  // directly use a destination type of char16_t.
#if _MSC_VER >= 1900
  using CharType = int16_t;
#else
  using CharType = char16_t;
#endif
  using ConverterType = std::codecvt_utf8_utf16<CharType>;
  std::wstring_convert<ConverterType, CharType> code_converter{};

  std::basic_string<CharType> utf16_string;
  try {
    utf16_string = code_converter.from_bytes(string_start, string_end);
  } catch (...) {
    // In the case that any error occurs, just try returning a string in the 
    // user's current locale instead.
    return mxCreateString(string_start);
  }

  // Store the converter UTF-16 string in a mxCharMatrix and return it.
  const mwSize dimensions[2] = {1, utf16_string.size()};
  mxArray* character_matrix = mxCreateCharArray(2, dimensions);
  mxChar* character_matrix_pointer = mxGetChars(character_matrix);
  std::copy(utf16_string.data(), utf16_string.data() + utf16_string.size(), 
      character_matrix_pointer);

  return character_matrix;
}

}  // namespace util
}  // namespace matlab
}  // namespace arrow
