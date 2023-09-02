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

#include "arrow/io/type_fwd.h"
#include "parquet/types.h"

namespace parquet {

class BloomFilter;
class SchemaDescriptor;
struct BloomFilterOptions;
struct BloomFilterLocation;

/// \brief Interface for collecting bloom filter of a parquet file.
class PARQUET_EXPORT BloomFilterBuilder {
 public:
  /// \brief API convenience to create a BloomFilterBuilder.
  static std::unique_ptr<BloomFilterBuilder> Make(const SchemaDescriptor* schema,
                                                  const WriterProperties& properties);

  /// Append a new row group to host all incoming bloom filters.
  virtual void AppendRowGroup() = 0;

  /// \brief Get the BloomFilter from column ordinal.
  ///
  /// \param column_ordinal Column ordinal in schema, which is only for leaf columns.
  /// \param bloom_filter_options The options(like num distinct values and false positive
  /// rate) to create a BloomFilter.
  ///
  /// \return BloomFilter for the column and its memory ownership belongs to the
  /// BloomFilterBuilder.
  virtual BloomFilter* GetOrCreateBloomFilter(
      int32_t column_ordinal, const BloomFilterOptions& bloom_filter_options) = 0;

  /// \brief Write the bloom filter to sink.
  ///
  /// The bloom filter must have been finished first.
  ///
  /// \param[out] sink The output stream to write the bloom filter.
  /// \param[out] location The location of all bloom filter to the start of sink.
  virtual void WriteTo(::arrow::io::OutputStream* sink,
                       BloomFilterLocation* location) = 0;

  /// \brief Complete the bloom filter builder and no more write is allowed.
  ///
  /// This method must be called before WriteTo.
  virtual void Finish() = 0;

  virtual ~BloomFilterBuilder() = default;
};

}  // namespace parquet
