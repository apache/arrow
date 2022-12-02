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
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "parquet/platform.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

namespace parquet {

/// \brief BoundaryOrder is a proxy around format::BoundaryOrder.
enum class PARQUET_EXPORT BoundaryOrder { Unordered = 0, Ascending = 1, Descending = 2 };

/// \brief ColumnIndex is a proxy around format::ColumnIndex.
class PARQUET_EXPORT ColumnIndex {
 public:
  /// \brief Create a ColumnIndex from a serialized thrift message.
  static std::unique_ptr<ColumnIndex> Make(const ColumnDescriptor& descr,
                                           const void* serialized_index,
                                           uint32_t index_len,
                                           const ReaderProperties& properties);

  virtual ~ColumnIndex() = default;

  /// \brief Returns number of pages in this column index.
  virtual int64_t num_pages() const = 0;

  /// \brief Returns if all values are null in a single page.
  virtual bool is_null_page(int64_t page_id) const = 0;

  /// \brief Returns whether both min_values and max_values are
  /// orderd and if so, in which direction.
  virtual BoundaryOrder boundary_order() const = 0;

  /// \brief Returns if null count is available.
  virtual bool has_null_counts() const = 0;

  /// \brief Returns null count for a single page.
  virtual int64_t null_count(int64_t page_id) const = 0;

  /// \brief The minimum value of a single page. Throws if it is null page.
  virtual const std::string& encoded_min(int64_t page_id) const = 0;

  /// \brief The maximum value of a single page. Throws if it is null page.
  virtual const std::string& encoded_max(int64_t page_id) const = 0;

  /// \brief Returns all null indicator for each page in batch.
  virtual const std::vector<bool>& null_pages() const = 0;

  /// \brief Returns null count for each page in batch.
  virtual const std::vector<int64_t>& null_counts() const = 0;
};

/// \brief Typed implementation of ColumnIndex.
template <typename DType>
class PARQUET_EXPORT TypedColumnIndex : public ColumnIndex {
 public:
  using T = typename DType::c_type;

  /// \brief The minimum value of a single page. Throws if it is null page.
  virtual T min_value(int64_t page_id) const = 0;

  /// \brief The maximum value of a single page. Throws if it is null page.
  virtual T max_value(int64_t page_id) const = 0;

  /// \brief The minimum value of every valid page.
  virtual const std::vector<T>& min_values() const = 0;

  /// \brief The maximum value of every valid page.
  virtual const std::vector<T>& max_values() const = 0;

  /// \brief Returns list of page index of all valid pages.
  /// It can be used to understand values returned from min_values/max_values.
  virtual std::vector<int64_t> GetValidPageIndices() const = 0;
};

using BoolColumnIndex = TypedColumnIndex<BooleanType>;
using Int32ColumnIndex = TypedColumnIndex<Int32Type>;
using Int64ColumnIndex = TypedColumnIndex<Int64Type>;
using FloatColumnIndex = TypedColumnIndex<FloatType>;
using DoubleColumnIndex = TypedColumnIndex<DoubleType>;
using ByteArrayColumnIndex = TypedColumnIndex<ByteArrayType>;
using FLBAColumnIndex = TypedColumnIndex<FLBAType>;

/// \brief PageLocation is a proxy around format::PageLocation.
struct PARQUET_EXPORT PageLocation {
  /// File offset of the data page.
  int64_t offset;
  /// Total compressed size of the data page and header.
  int32_t compressed_page_size;
  // row id of the first row in the page within the row group.
  int64_t first_row_index;
};

/// \brief OffsetIndex is a proxy around format::OffsetIndex.
class PARQUET_EXPORT OffsetIndex {
 public:
  /// \brief Create a OffsetIndex from a serialized thrift message.
  static std::unique_ptr<OffsetIndex> Make(const void* serialized_index,
                                           uint32_t index_len,
                                           const ReaderProperties& properties);

  virtual ~OffsetIndex() = default;

  /// \brief Returns number of pages in this column index.
  virtual int64_t num_pages() const = 0;

  /// \brief Returns PageLocation of a single page.
  virtual const PageLocation& GetPageLocation(int64_t page_id) const = 0;

  /// \brief Returns all page locations in the offset index.
  virtual const std::vector<PageLocation>& GetPageLocations() const = 0;
};

}  // namespace parquet
