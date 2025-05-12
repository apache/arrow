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

#include "parquet/variant.h"

#include <cstdint>
#include <string_view>

#include "arrow/util/endian.h"
#include "parquet/exception.h"

namespace parquet::variant {

VariantMetadata::VariantMetadata(std::string_view metadata) : metadata_(metadata) {
  if (metadata.size() < 2) {
    throw ParquetException("Invalid Variant metadata: too short: " +
                           std::to_string(metadata.size()));
  }
}

int8_t VariantMetadata::version() const {
  return static_cast<int8_t>(metadata_[0]) & 0x0F;
}

bool VariantMetadata::sortedStrings() const { return (metadata_[0] & 0b10000) != 0; }

uint8_t VariantMetadata::offsetSize() const { return ((metadata_[0] >> 6) & 0x3) + 1; }

uint32_t VariantMetadata::dictionarySize() const {
  uint8_t length = offsetSize();
  if (length > 4) {
    throw ParquetException("Invalid offset size: " + std::to_string(length));
  }
  if (length + 1 > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: too short for dictionary size");
  }
  uint32_t dict_size = 0;
  memcpy(&dict_size, metadata_.data() + 1, length);
  dict_size = arrow::bit_util::FromLittleEndian(dict_size);
  return dict_size;
}

std::string_view VariantMetadata::getMetadataKey(int32_t variantId) const {
  uint32_t offset_size = offsetSize();
  uint32_t dict_size = dictionarySize();

  if (variantId < 0 || variantId >= static_cast<int32_t>(dict_size)) {
    throw ParquetException("Invalid Variant metadata: variantId out of range");
  }

  if ((dict_size + 1) * offset_size > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: offset out of range");
  }

  size_t offset_start_pos = 1 + offset_size + (variantId * offset_size);

  uint32_t variant_offset = 0;
  uint32_t variant_next_offset = 0;
  memcpy(&variant_offset, metadata_.data() + offset_start_pos, offset_size);
  variant_offset = arrow::bit_util::FromLittleEndian(variant_offset);
  memcpy(&variant_next_offset, metadata_.data() + offset_start_pos + offset_size,
         offset_size);
  variant_next_offset = arrow::bit_util::FromLittleEndian(variant_next_offset);

  uint32_t key_size = variant_next_offset - variant_offset;

  size_t string_start = 1 + offset_size * (dict_size + 2) + variant_offset;
  if (string_start + key_size > metadata_.size()) {
    throw ParquetException("Invalid Variant metadata: string data out of range");
  }
  return std::string_view(metadata_.data() + string_start, key_size);
}
}  // namespace parquet::variant
