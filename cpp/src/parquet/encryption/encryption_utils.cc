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

#include "parquet/encryption/encryption_utils.h"
#include <openssl/err.h>
#include <openssl/rand.h>
#include "parquet/encryption/openssl_internal.h"
#include "parquet/exception.h"

namespace parquet::encryption {

static std::string ShortToBytesLe(int16_t input) {
  int8_t output[2];
  memset(output, 0, 2);
  output[1] = static_cast<int8_t>(0xff & (input >> 8));
  output[0] = static_cast<int8_t>(0xff & (input));

  return std::string(reinterpret_cast<const char*>(output), 2);
}

static void CheckPageOrdinal(int32_t page_ordinal) {
  if (ARROW_PREDICT_FALSE(page_ordinal > std::numeric_limits<int16_t>::max())) {
    throw ParquetException("Encrypted Parquet files can't have more than " +
                           std::to_string(std::numeric_limits<int16_t>::max()) +
                           " pages per chunk: got " + std::to_string(page_ordinal));
  }
}

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int32_t page_ordinal) {
  CheckPageOrdinal(page_ordinal);
  const int16_t page_ordinal_short = static_cast<int16_t>(page_ordinal);
  int8_t type_ordinal_bytes[1];
  type_ordinal_bytes[0] = module_type;
  std::string type_ordinal_bytes_str(reinterpret_cast<const char*>(type_ordinal_bytes),
                                     1);
  if (kFooter == module_type) {
    std::string result = file_aad + type_ordinal_bytes_str;
    return result;
  }
  std::string row_group_ordinal_bytes = ShortToBytesLe(row_group_ordinal);
  std::string column_ordinal_bytes = ShortToBytesLe(column_ordinal);
  if (kDataPage != module_type && kDataPageHeader != module_type) {
    std::ostringstream out;
    out << file_aad << type_ordinal_bytes_str << row_group_ordinal_bytes
        << column_ordinal_bytes;
    return out.str();
  }
  std::string page_ordinal_bytes = ShortToBytesLe(page_ordinal_short);
  std::ostringstream out;
  out << file_aad << type_ordinal_bytes_str << row_group_ordinal_bytes
      << column_ordinal_bytes << page_ordinal_bytes;
  return out.str();
}

std::string CreateFooterAad(const std::string& aad_prefix_bytes) {
  return CreateModuleAad(aad_prefix_bytes, kFooter, static_cast<int16_t>(-1),
                         static_cast<int16_t>(-1), static_cast<int16_t>(-1));
}

// Update last two bytes with new page ordinal (instead of creating new page AAD
// from scratch)
void QuickUpdatePageAad(int32_t new_page_ordinal, std::string* AAD) {
  CheckPageOrdinal(new_page_ordinal);
  const std::string page_ordinal_bytes =
      ShortToBytesLe(static_cast<int16_t>(new_page_ordinal));
  std::memcpy(AAD->data() + AAD->length() - 2, page_ordinal_bytes.data(), 2);
}

void RandBytes(unsigned char* buf, size_t num) {
  if (num > static_cast<size_t>(std::numeric_limits<int>::max())) {
    std::stringstream ss;
    ss << "Length " << num << " for RandBytes overflows int";
    throw ParquetException(ss.str());
  }
  openssl::EnsureInitialized();
  int status = RAND_bytes(buf, static_cast<int>(num));
  if (status != 1) {
    const auto error_code = ERR_get_error();
    char buffer[256];
    ERR_error_string_n(error_code, buffer, sizeof(buffer));
    std::stringstream ss;
    ss << "Failed to generate random bytes: " << buffer;
    throw ParquetException(ss.str());
  }
}

void EnsureBackendInitialized() { openssl::EnsureInitialized(); }

}  // namespace parquet::encryption
