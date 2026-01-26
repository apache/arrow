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
#include <string>

namespace parquet::encryption {

constexpr int32_t kGcmTagLength = 16;
constexpr int32_t kNonceLength = 12;

// Module types
constexpr int8_t kFooter = 0;
constexpr int8_t kColumnMetaData = 1;
constexpr int8_t kDataPage = 2;
constexpr int8_t kDictionaryPage = 3;
constexpr int8_t kDataPageHeader = 4;
constexpr int8_t kDictionaryPageHeader = 5;
constexpr int8_t kColumnIndex = 6;
constexpr int8_t kOffsetIndex = 7;
constexpr int8_t kBloomFilterHeader = 8;
constexpr int8_t kBloomFilterBitset = 9;

std::string CreateModuleAad(const std::string& file_aad, int8_t module_type,
                            int16_t row_group_ordinal, int16_t column_ordinal,
                            int32_t page_ordinal);

std::string CreateFooterAad(const std::string& aad_prefix_bytes);

// Update last two bytes of page (or page header) module AAD
void QuickUpdatePageAad(int32_t new_page_ordinal, std::string* AAD);

// Wraps OpenSSL RAND_bytes function
void RandBytes(unsigned char* buf, size_t num);

// Ensure OpenSSL is initialized.
//
// This is only necessary in specific situations since OpenSSL otherwise
// initializes itself automatically. For example, under Valgrind, a memory
// leak will be reported if OpenSSL is initialized for the first time from
// a worker thread; calling this function from the main thread prevents this.
void EnsureBackendInitialized();

}  // namespace parquet::encryption
