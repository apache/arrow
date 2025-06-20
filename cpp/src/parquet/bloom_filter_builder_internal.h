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

/// @brief Interface for building bloom filters of a parquet file.
class PARQUET_EXPORT BloomFilterBuilder {
 public:
  /// @brief API to create a BloomFilterBuilder.
  ///
  /// @param schema The schema of the file and it must outlive the created
  /// BloomFilterBuilder.
  /// @param properties The properties of the file with a set of `BloomFilterOption`s
  /// for columns enabling bloom filters. It must outlive the created BloomFilterBuilder.
  static std::unique_ptr<BloomFilterBuilder> Make(const SchemaDescriptor* schema,
                                                  const WriterProperties* properties);

  /// @brief Start a new row group to host all bloom filters belong to it.
  ///
  /// This method must be called before `GetOrCreateBloomFilter` for columns of a new row
  /// group.
  ///
  /// @throws ParquetException if WriteTo() has been called to flush bloom filters.
  virtual void AppendRowGroup() = 0;

  /// @brief Get the BloomFilter from column ordinal.
  ///
  /// @param column_ordinal Column ordinal in schema, which is only for leaf columns.
  ///
  /// \return BloomFilter for the column and its memory ownership belongs to the
  /// BloomFilterBuilder. It will return nullptr if bloom filter is not enabled for the
  /// column.
  ///
  /// @throws ParquetException if any of following conditions applies:
  /// 1) column_ordinal is out of bound.
  /// 2) `WriteTo()` has been called already.
  /// 3) `AppendRowGroup()` is not called before `GetOrCreateBloomFilter()`
  ///     for the first row-group.
  virtual BloomFilter* GetOrCreateBloomFilter(int32_t column_ordinal) = 0;

  /// @brief Write the bloom filter to sink.
  ///
  /// The bloom filter cannot be modified after this method is called.
  ///
  /// @param[in,out] sink The output stream to write the bloom filter.
  /// @param[out] location The location of all bloom filter relative to the start of sink.
  ///
  /// @throws ParquetException if WriteTo() has been called to flush bloom filters.
  virtual void WriteTo(::arrow::io::OutputStream* sink,
                       BloomFilterLocation* location) = 0;

  virtual ~BloomFilterBuilder() = default;
};

}  // namespace parquet
