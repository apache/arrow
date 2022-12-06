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

#include "parquet/page_index.h"
#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"

#include "arrow/util/unreachable.h"

#include <limits>
#include <map>
#include <numeric>

namespace parquet {

namespace {

template <typename DType>
void Decode(std::unique_ptr<typename EncodingTraits<DType>::Decoder>& decoder,
            const std::string& src, typename DType::c_type* dst) {
  decoder->SetData(/*num_values=*/1, reinterpret_cast<const uint8_t*>(src.c_str()),
                   static_cast<int>(src.size()));
  decoder->Decode(dst, /*max_values=*/1);
}

template <>
void Decode<ByteArrayType>(std::unique_ptr<ByteArrayDecoder>&, const std::string& src,
                           ByteArray* dst) {
  DCHECK_LE(src.size(), std::numeric_limits<uint32_t>::max());
  dst->len = static_cast<uint32_t>(src.size());
  dst->ptr = reinterpret_cast<const uint8_t*>(src.data());
}

template <typename DType>
class TypedColumnIndexImpl : public TypedColumnIndex<DType> {
 public:
  using T = typename DType::c_type;

  TypedColumnIndexImpl(const ColumnDescriptor& descr,
                       const format::ColumnIndex& column_index)
      : column_index_(column_index) {
    size_t num_non_null_pages = std::accumulate(
        column_index_.null_pages.cbegin(), column_index_.null_pages.cend(), 0U,
        [](size_t num_non_null_pages, bool null_page) {
          return num_non_null_pages + (null_page ? 0U : 1U);
        });
    if (num_non_null_pages != 0U) {
      min_values_.reserve(num_non_null_pages);
      max_values_.reserve(num_non_null_pages);
      // Decode min and max values into a compact form (i.e. w/o null page)
      auto plain_decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, &descr);
      T value;
      for (size_t i = 0; i < column_index_.null_pages.size(); ++i) {
        if (!column_index_.null_pages[i]) {
          non_null_page_indices_.emplace_back(static_cast<int32_t>(i));
          Decode<DType>(plain_decoder, column_index_.min_values[i], &value);
          min_values_.emplace_back(value);
          Decode<DType>(plain_decoder, column_index_.max_values[i], &value);
          max_values_.emplace_back(value);
        }
      }
    }
  }

  const std::vector<bool>& null_pages() const override {
    return column_index_.null_pages;
  }

  const std::vector<std::string>& encoded_min_values() const override {
    return column_index_.min_values;
  }

  const std::vector<std::string>& encoded_max_values() const override {
    return column_index_.max_values;
  }

  BoundaryOrder::type boundary_order() const override {
    return LoadEnumSafe(&column_index_.boundary_order);
  }

  bool has_null_counts() const override { return column_index_.__isset.null_counts; }

  const std::vector<int64_t>& null_counts() const override {
    return column_index_.null_counts;
  }

  const std::vector<T>& min_values() const override { return min_values_; }

  const std::vector<T>& max_values() const override { return max_values_; }

  const std::vector<int32_t>& non_null_page_indices() const override {
    return non_null_page_indices_;
  }

 private:
  /// Wrapped thrift column index.
  const format::ColumnIndex column_index_;
  /// Decoded typed min/max values. Null pages are set to std::nullopt.
  std::vector<T> min_values_;
  std::vector<T> max_values_;
  /// A list of page indices for not-null pages.
  std::vector<int32_t> non_null_page_indices_;
};

class OffsetIndexImpl : public OffsetIndex {
 public:
  explicit OffsetIndexImpl(const format::OffsetIndex& offset_index) {
    page_locations_.reserve(offset_index.page_locations.size());
    for (const auto& page_location : offset_index.page_locations) {
      page_locations_.emplace_back(PageLocation{page_location.offset,
                                                page_location.compressed_page_size,
                                                page_location.first_row_index});
    }
  }

  const std::vector<PageLocation>& page_locations() const override {
    return page_locations_;
  }

 private:
  std::vector<PageLocation> page_locations_;
};

}  // namespace

// ----------------------------------------------------------------------
// Public factory functions

std::unique_ptr<ColumnIndex> ColumnIndex::Make(const ColumnDescriptor& descr,
                                               const void* serialized_index,
                                               uint32_t index_len,
                                               const ReaderProperties& properties) {
  format::ColumnIndex column_index;
  ThriftDeserializer deserializer(properties);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(serialized_index),
                                  &index_len, &column_index);
  switch (descr.physical_type()) {
    case Type::BOOLEAN:
      return std::make_unique<TypedColumnIndexImpl<BooleanType>>(descr, column_index);
    case Type::INT32:
      return std::make_unique<TypedColumnIndexImpl<Int32Type>>(descr, column_index);
    case Type::INT64:
      return std::make_unique<TypedColumnIndexImpl<Int64Type>>(descr, column_index);
    case Type::INT96:
      return std::make_unique<TypedColumnIndexImpl<Int96Type>>(descr, column_index);
    case Type::FLOAT:
      return std::make_unique<TypedColumnIndexImpl<FloatType>>(descr, column_index);
    case Type::DOUBLE:
      return std::make_unique<TypedColumnIndexImpl<DoubleType>>(descr, column_index);
    case Type::BYTE_ARRAY:
      return std::make_unique<TypedColumnIndexImpl<ByteArrayType>>(descr, column_index);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_unique<TypedColumnIndexImpl<FLBAType>>(descr, column_index);
    case Type::UNDEFINED:
      ::arrow::Unreachable("Cannot make ColumnIndex of an unknown type");
      return nullptr;
  }
}

std::unique_ptr<OffsetIndex> OffsetIndex::Make(const void* serialized_index,
                                               uint32_t index_len,
                                               const ReaderProperties& properties) {
  format::OffsetIndex offset_index;
  ThriftDeserializer deserializer(properties);
  deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(serialized_index),
                                  &index_len, &offset_index);
  return std::make_unique<OffsetIndexImpl>(offset_index);
}

}  // namespace parquet
