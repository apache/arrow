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

#include <stdexcept>
#include <unordered_map>

#include <dbpa_interface.h>

#include "arrow/type_fwd.h"  // For arrow::Compression
#include "parquet/types.h"

namespace parquet::encryption::external {

/**
 * Utility class for translating between Parquet/Arrow enums and dbps::external enums.
 *
 * This class provides methods to convert between:
 * - parquet::Type and dbps::external::Type
 * - arrow::Compression and dbps::external::CompressionCodec
 */
class DBPAEnumUtils {
 public:
  // Static maps for type conversions
  static const std::unordered_map<parquet::Type::type, dbps::external::Type::type>
      parquet_to_external_type_map;
  static const std::unordered_map<::arrow::Compression::type,
                                  dbps::external::CompressionCodec::type>
      arrow_to_external_compression_map;

  /**
   * Convert parquet::Type to dbps::external::Type
   *
   * @param parquet_type The parquet type to convert
   * @return The corresponding dbps::external::Type
   */
  static dbps::external::Type::type ParquetTypeToDBPA(parquet::Type::type parquet_type);

  /**
   * Convert arrow::Compression to dbps::external::CompressionCodec
   *
   * @param arrow_compression The Arrow compression type to convert
   * @return The corresponding dbps::external::CompressionCodec
   * @throws std::invalid_argument if the Arrow compression type cannot be mapped
   */
  static dbps::external::CompressionCodec::type ArrowCompressionToDBPA(
      ::arrow::Compression::type arrow_compression);
};

}  // namespace parquet::encryption::external
