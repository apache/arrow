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
#include "parquet/metadata.h"
#include "parquet/thrift_internal.h"

#include <map>

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
  dst->len = static_cast<uint32_t>(src.size());
  dst->ptr = reinterpret_cast<const uint8_t*>(src.c_str());
}

template <typename DType>
class TypedColumnIndexImpl : public TypedColumnIndex<DType> {
 public:
  using T = typename DType::c_type;

  explicit TypedColumnIndexImpl(const ColumnDescriptor& descr,
                                const std::vector<bool>& null_pages,
                                const std::vector<std::string>& min_values,
                                const std::vector<std::string>& max_values,
                                const BoundaryOrder& boundary_order,
                                const bool has_null_count = false,
                                const std::vector<int64_t>& null_counts = {})
      : null_pages_(null_pages),
        encoded_min_values_(min_values),
        encoded_max_values_(max_values),
        boundary_order_(boundary_order),
        has_null_count_(has_null_count),
        null_counts_(null_counts) {
    // Decode min and max values into a compact form (i.e. w/o null page)
    auto plain_decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, &descr);
    T value;
    for (size_t i = 0; i < null_pages_.size(); ++i) {
      if (!null_pages_[i]) {
        // page index -> min/max slot index
        page_indexes_.emplace(i, min_values_.size());
        Decode<DType>(plain_decoder, encoded_min_values_[i], &value);
        min_values_.push_back(value);
        Decode<DType>(plain_decoder, encoded_max_values_[i], &value);
        max_values_.push_back(value);
      }
    }
  }

  explicit TypedColumnIndexImpl(const ColumnDescriptor& descr,
                                const format::ColumnIndex& column_index)
      : TypedColumnIndexImpl(
            descr, column_index.null_pages, column_index.min_values,
            column_index.max_values,
            static_cast<BoundaryOrder>(static_cast<int>(column_index.boundary_order)),
            column_index.__isset.null_counts, column_index.null_counts) {}

  int64_t num_pages() const override { return static_cast<int64_t>(null_pages_.size()); }

  bool is_null_page(int64_t page_id) const override {
    if (page_id >= num_pages()) {
      throw ParquetException("Page index is out of bound");
    }
    return null_pages_[page_id];
  }

  BoundaryOrder boundary_order() const override { return boundary_order_; }

  bool has_null_counts() const override { return has_null_count_; }

  int64_t null_count(int64_t page_id) const override {
    if (page_id >= num_pages()) {
      throw ParquetException("Page index is out of bound");
    }
    return null_counts_[page_id];
  }

  T min_value(int64_t page_id) const override {
    return min_values_[GetMinMaxSlot(page_id)];
  }

  T max_value(int64_t page_id) const override {
    return max_values_[GetMinMaxSlot(page_id)];
  }

  const std::string& encoded_min(int64_t page_id) const override {
    if (page_id >= num_pages()) {
      throw ParquetException("Page index is out of bound");
    }
    return encoded_min_values_[page_id];
  }

  const std::string& encoded_max(int64_t page_id) const override {
    if (page_id >= num_pages()) {
      throw ParquetException("Page index is out of bound");
    }
    return encoded_max_values_[page_id];
  }

  const std::vector<bool>& null_pages() const override { return null_pages_; }

  const std::vector<int64_t>& null_counts() const override { return null_counts_; }

  const std::vector<T>& min_values() const override { return min_values_; }

  const std::vector<T>& max_values() const override { return max_values_; }

  std::vector<int64_t> GetValidPageIndices() const override {
    std::vector<int64_t> valid_page_indices;
    std::for_each(page_indexes_.cbegin(), page_indexes_.cend(),
                  [&](const std::pair<size_t, size_t>& v) {
                    valid_page_indices.push_back(v.first);
                  });
    return valid_page_indices;
  }

 private:
  size_t GetMinMaxSlot(int64_t page_id) const {
    if (page_id >= static_cast<int64_t>(null_pages_.size())) {
      throw ParquetException("Page index is out of bound");
    }
    if (null_pages_[page_id]) {
      throw ParquetException("Cannot get min/max value of null page");
    }
    auto iter = page_indexes_.find(page_id);
    if (iter == page_indexes_.cend()) {
      throw ParquetException("min/max value is unavailable");
    }
    return iter->second;
  }

  /// Values that are copied directly from the thrift message.
  std::vector<bool> null_pages_;
  std::vector<std::string> encoded_min_values_;
  std::vector<std::string> encoded_max_values_;
  BoundaryOrder boundary_order_;
  bool has_null_count_;
  std::vector<int64_t> null_counts_;

  /// page_id -> slot_id in the buffer of min_values_ & max_values_
  std::map<size_t, size_t> page_indexes_;

  /// Decoded typed min/max values.
  std::vector<T> min_values_;
  std::vector<T> max_values_;
};

class OffsetIndexImpl : public OffsetIndex {
 public:
  explicit OffsetIndexImpl(std::vector<PageLocation> page_locations)
      : page_locations_(std::move(page_locations)) {}

  explicit OffsetIndexImpl(const format::OffsetIndex& offset_index) {
    for (const auto& page_location : offset_index.page_locations) {
      page_locations_.emplace_back();
      auto& location = page_locations_.back();
      location.offset = page_location.offset;
      location.compressed_page_size = page_location.compressed_page_size;
      location.first_row_index = page_location.first_row_index;
    }
  }

  int64_t num_pages() const override { return page_locations_.size(); }

  const PageLocation& GetPageLocation(int64_t page_id) const override {
    if (page_id >= num_pages()) {
      throw ParquetException("Page index is out of bound");
    }
    return page_locations_[page_id];
  }

  const std::vector<PageLocation>& GetPageLocations() const override {
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
    default:
      break;
  }
  DCHECK(false) << "Should not be able to reach this code";
  return nullptr;
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
