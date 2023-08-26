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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#pragma once

#include "arrow/io/interfaces.h"
#include "parquet/types.h"

namespace parquet {

class BloomFilter;
class SchemaDescriptor;
class InternalFileEncryptor;
struct BloomFilterOptions;
struct BloomFilterLocation;

namespace schema {
class ColumnPath;
}

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
  /// \return ColumnIndexBuilder for the column and its memory ownership belongs to
  /// the PageIndexBuilder.
  virtual BloomFilter* GetOrCreateBloomFilter(
      int32_t column_ordinal, const BloomFilterOptions& bloom_filter_options) = 0;

  /// \brief Write the bloom filter to sink.
  ///
  /// \param[out] sink The output stream to write the bloom filter.
  /// \param[out] location The location of all page index to the start of sink.
  virtual void WriteTo(::arrow::io::OutputStream* sink,
                       BloomFilterLocation* location) = 0;

  /// \brief Complete the bloom filter builder and no more write is allowed.
  virtual void Finish() = 0;

  virtual ~BloomFilterBuilder() = default;
};

}  // namespace parquet
