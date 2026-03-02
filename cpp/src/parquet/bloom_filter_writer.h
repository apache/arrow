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

#include "arrow/type_fwd.h"

#include "parquet/bloom_filter.h"
#include "parquet/index_location.h"
#include "parquet/type_fwd.h"

namespace parquet {

/// \brief Writer for updating a bloom filter with values of a specific Parquet type.
/// \note Boolean type is not supported.
template <typename ParquetType>
class PARQUET_EXPORT TypedBloomFilterWriter {
 public:
  using T = typename ParquetType::c_type;

  /// \param descr The descriptor of the column to write. Must outlive this writer.
  /// \param bloom_filter The bloom filter to update. Must outlive this writer.
  TypedBloomFilterWriter(const ColumnDescriptor* descr, BloomFilter* bloom_filter);

  /// \brief Update the bloom filter with typed values.
  ///
  /// \param values The values to update the bloom filter with.
  /// \param num_values The number of values to update the bloom filter with.
  void Update(const T* values, int64_t num_values);

  /// \brief Update the bloom filter with typed values that have spaces.
  ///
  /// \param values The values to update the bloom filter with.
  /// \param num_values The number of values to update the bloom filter with.
  /// \param valid_bits The validity bitmap of the values.
  /// \param valid_bits_offset The offset of the validity bitmap.
  void UpdateSpaced(const T* values, int64_t num_values, const uint8_t* valid_bits,
                    int64_t valid_bits_offset);

  /// \brief Update the bloom filter with an Arrow array.
  ///
  /// \param values The Arrow array to update the bloom filter with.
  void Update(const ::arrow::Array& values);

 private:
  const ColumnDescriptor* descr_;
  BloomFilter* bloom_filter_;
};

/// \brief Interface for building bloom filters of a parquet file.
class PARQUET_EXPORT BloomFilterBuilder {
 public:
  virtual ~BloomFilterBuilder() = default;

  /// \brief API to create a BloomFilterBuilder.
  ///
  /// \param schema The schema of the file and it must outlive the created builder.
  /// \param properties Properties to get bloom filter options. It must outlive the
  /// created builder.
  static std::unique_ptr<BloomFilterBuilder> Make(const SchemaDescriptor* schema,
                                                  const WriterProperties* properties);

  /// \brief Start a new row group to write bloom filters, meaning that next calls
  /// to `CreateBloomFilter` will create bloom filters for the new row group.
  ///
  /// \throws ParquetException if `WriteTo()` has been called already.
  virtual void AppendRowGroup() = 0;

  /// \brief Create a BloomFilter of the column ordinal of the current row group.
  ///
  /// \param column_ordinal Column ordinal for the bloom filter.
  /// \return created BloomFilter whose ownership belongs to the builder, or nullptr if
  /// bloom filter is not enabled for the column.
  /// \throws ParquetException if any condition is violated:
  ///   - `AppendRowGroup()` has not been called yet
  ///   - The column ordinal is out of bound
  ///   - Bloom filter already exists for the column
  ///   - `WriteTo()` has been called
  virtual BloomFilter* CreateBloomFilter(int32_t column_ordinal) = 0;

  /// \brief Insert a BloomFilter of the column ordinal of the current row group.
  ///
  /// \param column_ordinal Column ordinal for the bloom filter.
  /// \param bloom_filter The bloom filter to insert.
  /// \throws ParquetException if any condition is violated:
  ///   - `AppendRowGroup()` has not been called yet
  ///   - The column ordinal is out of bound
  ///   - Bloom filter already exists for the column
  ///   - `WriteTo()` has been called
  virtual void InsertBloomFilter(int32_t column_ordinal,
                                 std::unique_ptr<BloomFilter> bloom_filter) = 0;

  /// \brief Write all bloom filters to sink.
  ///
  /// The bloom filters cannot be modified after this method is called.
  ///
  /// \param[in] sink The output stream to write the bloom filters.
  /// \return The location of all bloom filters.
  /// \throws ParquetException if `WriteTo()` has been called.
  virtual IndexLocations WriteTo(::arrow::io::OutputStream* sink) = 0;
};

}  // namespace parquet
