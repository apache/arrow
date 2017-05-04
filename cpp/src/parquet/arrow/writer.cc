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

#include "parquet/arrow/writer.h"

#include <algorithm>
#include <string>
#include <vector>

#include "parquet/util/bit-util.h"
#include "parquet/util/logging.h"

#include "parquet/arrow/schema.h"

#include "arrow/api.h"
#include "arrow/visitor_inline.h"

using arrow::Array;
using arrow::BinaryArray;
using arrow::FixedSizeBinaryArray;
using arrow::BooleanArray;
using arrow::Int16Array;
using arrow::Int16Builder;
using arrow::Field;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::PrimitiveArray;
using arrow::ListArray;
using arrow::Status;
using arrow::Table;
using arrow::TimeUnit;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet {
namespace arrow {

namespace BitUtil = ::arrow::BitUtil;

class LevelBuilder {
 public:
  explicit LevelBuilder(MemoryPool* pool)
      : def_levels_(pool, ::arrow::int16()), rep_levels_(pool, ::arrow::int16()) {
    def_levels_buffer_ = std::make_shared<PoolBuffer>(pool);
  }

  Status VisitInline(const Array& array);

  Status Visit(const ::arrow::PrimitiveArray& array) {
    array_offsets_.push_back(array.offset());
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    values_type_ = array.type_id();
    values_array_ = &array;
    return Status::OK();
  }

  Status Visit(const ::arrow::BinaryArray& array) {
    array_offsets_.push_back(array.offset());
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    values_type_ = array.type_id();
    values_array_ = &array;
    return Status::OK();
  }

  Status Visit(const ListArray& array) {
    array_offsets_.push_back(array.offset());
    valid_bitmaps_.push_back(array.null_bitmap_data());
    null_counts_.push_back(array.null_count());
    offsets_.push_back(array.raw_value_offsets());

    min_offset_idx_ = array.value_offset(min_offset_idx_);
    max_offset_idx_ = array.value_offset(max_offset_idx_);

    return VisitInline(*array.values());
  }

#define NOT_IMPLEMENTED_VISIT(ArrowTypePrefix)                     \
  Status Visit(const ::arrow::ArrowTypePrefix##Array& array) {     \
    return Status::NotImplemented(                                 \
        "Level generation for ArrowTypePrefix not supported yet"); \
  }

  NOT_IMPLEMENTED_VISIT(Null)
  NOT_IMPLEMENTED_VISIT(Struct)
  NOT_IMPLEMENTED_VISIT(Union)
  NOT_IMPLEMENTED_VISIT(Decimal)
  NOT_IMPLEMENTED_VISIT(Dictionary)
  NOT_IMPLEMENTED_VISIT(Interval)

  Status GenerateLevels(const Array& array, const std::shared_ptr<Field>& field,
      int64_t* values_offset, ::arrow::Type::type* values_type, int64_t* num_values,
      int64_t* num_levels, std::shared_ptr<Buffer>* def_levels,
      std::shared_ptr<Buffer>* rep_levels, const Array** values_array) {
    // Work downwards to extract bitmaps and offsets
    min_offset_idx_ = 0;
    max_offset_idx_ = array.length();
    RETURN_NOT_OK(VisitInline(array));
    *num_values = max_offset_idx_ - min_offset_idx_;
    *values_offset = min_offset_idx_;
    *values_type = values_type_;
    *values_array = values_array_;

    // Walk downwards to extract nullability
    std::shared_ptr<Field> current_field = field;
    nullable_.push_back(current_field->nullable());
    while (current_field->type()->num_children() > 0) {
      if (current_field->type()->num_children() > 1) {
        return Status::NotImplemented(
            "Fields with more than one child are not supported.");
      } else {
        current_field = current_field->type()->child(0);
      }
      nullable_.push_back(current_field->nullable());
    }

    // Generate the levels.
    if (nullable_.size() == 1) {
      // We have a PrimitiveArray
      *rep_levels = nullptr;
      if (nullable_[0]) {
        RETURN_NOT_OK(def_levels_buffer_->Resize(array.length() * sizeof(int16_t)));
        auto def_levels_ptr =
            reinterpret_cast<int16_t*>(def_levels_buffer_->mutable_data());
        if (array.null_count() == 0) {
          std::fill(def_levels_ptr, def_levels_ptr + array.length(), 1);
        } else {
          const uint8_t* valid_bits = array.null_bitmap_data();
          INIT_BITSET(valid_bits, array.offset());
          for (int i = 0; i < array.length(); i++) {
            if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
              def_levels_ptr[i] = 1;
            } else {
              def_levels_ptr[i] = 0;
            }
            READ_NEXT_BITSET(valid_bits);
          }
        }
        *def_levels = def_levels_buffer_;
      } else {
        *def_levels = nullptr;
      }
      *num_levels = array.length();
    } else {
      RETURN_NOT_OK(rep_levels_.Append(0));
      HandleListEntries(0, 0, 0, array.length());

      std::shared_ptr<Array> def_levels_array;
      RETURN_NOT_OK(def_levels_.Finish(&def_levels_array));
      *def_levels = static_cast<PrimitiveArray*>(def_levels_array.get())->data();

      std::shared_ptr<Array> rep_levels_array;
      RETURN_NOT_OK(rep_levels_.Finish(&rep_levels_array));
      *rep_levels = static_cast<PrimitiveArray*>(rep_levels_array.get())->data();
      *num_levels = rep_levels_array->length();
    }

    return Status::OK();
  }

  Status HandleList(int16_t def_level, int16_t rep_level, int64_t index) {
    if (nullable_[rep_level]) {
      if (null_counts_[rep_level] == 0 ||
          BitUtil::GetBit(valid_bitmaps_[rep_level], index + array_offsets_[rep_level])) {
        return HandleNonNullList(def_level + 1, rep_level, index);
      } else {
        return def_levels_.Append(def_level);
      }
    } else {
      return HandleNonNullList(def_level, rep_level, index);
    }
  }

  Status HandleNonNullList(int16_t def_level, int16_t rep_level, int64_t index) {
    int32_t inner_offset = offsets_[rep_level][index];
    int32_t inner_length = offsets_[rep_level][index + 1] - inner_offset;
    int64_t recursion_level = rep_level + 1;
    if (inner_length == 0) { return def_levels_.Append(def_level); }
    if (recursion_level < static_cast<int64_t>(offsets_.size())) {
      return HandleListEntries(def_level + 1, rep_level + 1, inner_offset, inner_length);
    } else {
      // We have reached the leaf: primitive list, handle remaining nullables
      for (int64_t i = 0; i < inner_length; i++) {
        if (i > 0) { RETURN_NOT_OK(rep_levels_.Append(rep_level + 1)); }
        if (nullable_[recursion_level] &&
            ((null_counts_[recursion_level] == 0) ||
                BitUtil::GetBit(valid_bitmaps_[recursion_level],
                    inner_offset + i + array_offsets_[recursion_level]))) {
          RETURN_NOT_OK(def_levels_.Append(def_level + 2));
        } else {
          // This can be produced in two case:
          //  * elements are nullable and this one is null (i.e. max_def_level = def_level
          //  + 2)
          //  * elements are non-nullable (i.e. max_def_level = def_level + 1)
          RETURN_NOT_OK(def_levels_.Append(def_level + 1));
        }
      }
      return Status::OK();
    }
  }

  Status HandleListEntries(
      int16_t def_level, int16_t rep_level, int64_t offset, int64_t length) {
    for (int64_t i = 0; i < length; i++) {
      if (i > 0) { RETURN_NOT_OK(rep_levels_.Append(rep_level)); }
      RETURN_NOT_OK(HandleList(def_level, rep_level, offset + i));
    }
    return Status::OK();
  }

 private:
  Int16Builder def_levels_;
  std::shared_ptr<PoolBuffer> def_levels_buffer_;
  Int16Builder rep_levels_;

  std::vector<int64_t> null_counts_;
  std::vector<const uint8_t*> valid_bitmaps_;
  std::vector<const int32_t*> offsets_;
  std::vector<int32_t> array_offsets_;
  std::vector<bool> nullable_;

  int32_t min_offset_idx_;
  int32_t max_offset_idx_;
  ::arrow::Type::type values_type_;
  const Array* values_array_;
};

Status LevelBuilder::VisitInline(const Array& array) {
  return VisitArrayInline(array, this);
}

class FileWriter::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  template <typename ParquetType, typename ArrowType>
  Status TypedWriteBatch(ColumnWriter* writer, const std::shared_ptr<Array>& data,
      int64_t num_levels, const int16_t* def_levels, const int16_t* rep_levels);

  template <typename ParquetType, typename ArrowType>
  Status WriteNonNullableBatch(TypedColumnWriter<ParquetType>* writer,
      const ArrowType& type, int64_t num_values, int64_t num_levels,
      const int16_t* def_levels, const int16_t* rep_levels,
      const typename ArrowType::c_type* data_ptr);

  template <typename ParquetType, typename ArrowType>
  Status WriteNullableBatch(TypedColumnWriter<ParquetType>* writer, const ArrowType& type,
      int64_t num_values, int64_t num_levels, const int16_t* def_levels,
      const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
      const typename ArrowType::c_type* data_ptr);

  Status WriteColumnChunk(const Array& data);
  Status Close();

  virtual ~Impl() {}

 private:
  friend class FileWriter;

  MemoryPool* pool_;
  // Buffer used for storing the data of an array converted to the physical type
  // as expected by parquet-cpp.
  PoolBuffer data_buffer_;
  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
};

FileWriter::Impl::Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : pool_(pool),
      data_buffer_(pool),
      writer_(std::move(writer)),
      row_group_writer_(nullptr) {}

Status FileWriter::Impl::NewRowGroup(int64_t chunk_size) {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendRowGroup(chunk_size));
  return Status::OK();
}

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::TypedWriteBatch(ColumnWriter* column_writer,
    const std::shared_ptr<Array>& array, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels) {
  using ArrowCType = typename ArrowType::c_type;

  auto data = static_cast<const PrimitiveArray*>(array.get());
  auto data_ptr = reinterpret_cast<const ArrowCType*>(data->data()->data());
  auto writer = reinterpret_cast<TypedColumnWriter<ParquetType>*>(column_writer);

  if (writer->descr()->schema_node()->is_required() || (data->null_count() == 0)) {
    // no nulls, just dump the data
    RETURN_NOT_OK((WriteNonNullableBatch<ParquetType, ArrowType>(writer,
        static_cast<const ArrowType&>(*array->type()), array->length(), num_levels,
        def_levels, rep_levels, data_ptr + data->offset())));
  } else {
    const uint8_t* valid_bits = data->null_bitmap_data();
    RETURN_NOT_OK((WriteNullableBatch<ParquetType, ArrowType>(writer,
        static_cast<const ArrowType&>(*array->type()), data->length(), num_levels,
        def_levels, rep_levels, valid_bits, data->offset(), data_ptr + data->offset())));
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::WriteNonNullableBatch(TypedColumnWriter<ParquetType>* writer,
    const ArrowType& type, int64_t num_values, int64_t num_levels,
    const int16_t* def_levels, const int16_t* rep_levels,
    const typename ArrowType::c_type* data_ptr) {
  using ParquetCType = typename ParquetType::c_type;
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(ParquetCType)));
  auto buffer_ptr = reinterpret_cast<ParquetCType*>(data_buffer_.mutable_data());
  std::copy(data_ptr, data_ptr + num_values, buffer_ptr);
  PARQUET_CATCH_NOT_OK(
      writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  return Status::OK();
}

template <>
Status FileWriter::Impl::WriteNonNullableBatch<Int32Type, ::arrow::Date64Type>(
    TypedColumnWriter<Int32Type>* writer, const ::arrow::Date64Type& type,
    int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels, const int64_t* data_ptr) {
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(int32_t)));
  auto buffer_ptr = reinterpret_cast<int32_t*>(data_buffer_.mutable_data());
  for (int i = 0; i < num_values; i++) {
    buffer_ptr[i] = static_cast<int32_t>(data_ptr[i] / 86400000);
  }
  PARQUET_CATCH_NOT_OK(
      writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  return Status::OK();
}

template <>
Status FileWriter::Impl::WriteNonNullableBatch<Int32Type, ::arrow::Time32Type>(
    TypedColumnWriter<Int32Type>* writer, const ::arrow::Time32Type& type,
    int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels, const int32_t* data_ptr) {
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(int32_t)));
  auto buffer_ptr = reinterpret_cast<int32_t*>(data_buffer_.mutable_data());
  if (type.unit() == TimeUnit::SECOND) {
    for (int i = 0; i < num_values; i++) {
      buffer_ptr[i] = data_ptr[i] * 1000;
    }
  } else {
    std::copy(data_ptr, data_ptr + num_values, buffer_ptr);
  }
  PARQUET_CATCH_NOT_OK(
      writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  return Status::OK();
}

#define NONNULLABLE_BATCH_FAST_PATH(ParquetType, ArrowType, CType)         \
  template <>                                                              \
  Status FileWriter::Impl::WriteNonNullableBatch<ParquetType, ArrowType>(  \
      TypedColumnWriter<ParquetType> * writer, const ArrowType& type,      \
      int64_t num_values, int64_t num_levels, const int16_t* def_levels,   \
      const int16_t* rep_levels, const CType* data_ptr) {                  \
    PARQUET_CATCH_NOT_OK(                                                  \
        writer->WriteBatch(num_levels, def_levels, rep_levels, data_ptr)); \
    return Status::OK();                                                   \
  }

NONNULLABLE_BATCH_FAST_PATH(Int32Type, ::arrow::Int32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(Int32Type, ::arrow::Date32Type, int32_t)
NONNULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::Int64Type, int64_t)
NONNULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::TimestampType, int64_t)
NONNULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::Time64Type, int64_t)
NONNULLABLE_BATCH_FAST_PATH(FloatType, ::arrow::FloatType, float)
NONNULLABLE_BATCH_FAST_PATH(DoubleType, ::arrow::DoubleType, double)

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::WriteNullableBatch(TypedColumnWriter<ParquetType>* writer,
    const ArrowType& type, int64_t num_values, int64_t num_levels,
    const int16_t* def_levels, const int16_t* rep_levels, const uint8_t* valid_bits,
    int64_t valid_bits_offset, const typename ArrowType::c_type* data_ptr) {
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(ParquetCType)));
  auto buffer_ptr = reinterpret_cast<ParquetCType*>(data_buffer_.mutable_data());
  INIT_BITSET(valid_bits, valid_bits_offset);
  for (int i = 0; i < num_values; i++) {
    if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
      buffer_ptr[i] = static_cast<ParquetCType>(data_ptr[i]);
    }
    READ_NEXT_BITSET(valid_bits);
  }
  PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(
      num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, buffer_ptr));

  return Status::OK();
}

template <>
Status FileWriter::Impl::WriteNullableBatch<Int32Type, ::arrow::Date64Type>(
    TypedColumnWriter<Int32Type>* writer, const ::arrow::Date64Type& type,
    int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
    const int64_t* data_ptr) {
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(int32_t)));
  auto buffer_ptr = reinterpret_cast<int32_t*>(data_buffer_.mutable_data());
  INIT_BITSET(valid_bits, valid_bits_offset);
  for (int i = 0; i < num_values; i++) {
    if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
      // Convert from milliseconds into days since the epoch
      buffer_ptr[i] = static_cast<int32_t>(data_ptr[i] / 86400000);
    }
    READ_NEXT_BITSET(valid_bits);
  }
  PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(
      num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, buffer_ptr));

  return Status::OK();
}

template <>
Status FileWriter::Impl::WriteNullableBatch<Int32Type, ::arrow::Time32Type>(
    TypedColumnWriter<Int32Type>* writer, const ::arrow::Time32Type& type,
    int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
    const int32_t* data_ptr) {
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(int32_t)));
  auto buffer_ptr = reinterpret_cast<int32_t*>(data_buffer_.mutable_data());
  INIT_BITSET(valid_bits, valid_bits_offset);

  if (type.unit() == TimeUnit::SECOND) {
    for (int i = 0; i < num_values; i++) {
      if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
        buffer_ptr[i] = data_ptr[i] * 1000;
      }
      READ_NEXT_BITSET(valid_bits);
    }
  } else {
    for (int i = 0; i < num_values; i++) {
      if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
        buffer_ptr[i] = data_ptr[i];
      }
      READ_NEXT_BITSET(valid_bits);
    }
  }
  PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(
      num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, buffer_ptr));

  return Status::OK();
}

#define NULLABLE_BATCH_FAST_PATH(ParquetType, ArrowType, CType)                        \
  template <>                                                                          \
  Status FileWriter::Impl::WriteNullableBatch<ParquetType, ArrowType>(                 \
      TypedColumnWriter<ParquetType> * writer, const ArrowType& type,                  \
      int64_t num_values, int64_t num_levels, const int16_t* def_levels,               \
      const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset, \
      const CType* data_ptr) {                                                         \
    PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(                                     \
        num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, data_ptr)); \
    return Status::OK();                                                               \
  }

NULLABLE_BATCH_FAST_PATH(Int32Type, ::arrow::Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(Int32Type, ::arrow::Date32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::Int64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::TimestampType, int64_t)
NULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::Time64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(FloatType, ::arrow::FloatType, float)
NULLABLE_BATCH_FAST_PATH(DoubleType, ::arrow::DoubleType, double)

// This specialization seems quite similar but it significantly differs in two points:
// * offset is added at the most latest time to the pointer as we have sub-byte access
// * Arrow data is stored bitwise thus we cannot use std::copy to transform from
//   ArrowType::c_type to ParquetType::c_type
template <>
Status FileWriter::Impl::TypedWriteBatch<BooleanType, ::arrow::BooleanType>(
    ColumnWriter* column_writer, const std::shared_ptr<Array>& array, int64_t num_levels,
    const int16_t* def_levels, const int16_t* rep_levels) {
  RETURN_NOT_OK(data_buffer_.Resize(array->length()));
  auto data = static_cast<const BooleanArray*>(array.get());
  auto data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
  auto buffer_ptr = reinterpret_cast<bool*>(data_buffer_.mutable_data());
  auto writer = reinterpret_cast<TypedColumnWriter<BooleanType>*>(column_writer);

  int buffer_idx = 0;
  int32_t offset = array->offset();
  for (int i = 0; i < data->length(); i++) {
    if (!data->IsNull(i)) {
      buffer_ptr[buffer_idx++] = BitUtil::GetBit(data_ptr, offset + i);
    }
  }
  PARQUET_CATCH_NOT_OK(
      writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

template <>
Status FileWriter::Impl::TypedWriteBatch<ByteArrayType, ::arrow::BinaryType>(
    ColumnWriter* column_writer, const std::shared_ptr<Array>& array, int64_t num_levels,
    const int16_t* def_levels, const int16_t* rep_levels) {
  RETURN_NOT_OK(data_buffer_.Resize(array->length() * sizeof(ByteArray)));
  auto data = static_cast<const BinaryArray*>(array.get());
  auto buffer_ptr = reinterpret_cast<ByteArray*>(data_buffer_.mutable_data());
  // In the case of an array consisting of only empty strings or all null,
  // data->data() points already to a nullptr, thus data->data()->data() will
  // segfault.
  const uint8_t* data_ptr = nullptr;
  if (data->data()) {
    data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
    DCHECK(data_ptr != nullptr);
  }
  auto writer = reinterpret_cast<TypedColumnWriter<ByteArrayType>*>(column_writer);
  const int32_t* value_offset = data->raw_value_offsets();

  if (writer->descr()->schema_node()->is_required() || (data->null_count() == 0)) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < data->length(); i++) {
      buffer_ptr[i] =
          ByteArray(value_offset[i + 1] - value_offset[i], data_ptr + value_offset[i]);
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  } else {
    int buffer_idx = 0;
    for (int64_t i = 0; i < data->length(); i++) {
      if (!data->IsNull(i)) {
        buffer_ptr[buffer_idx++] =
            ByteArray(value_offset[i + 1] - value_offset[i], data_ptr + value_offset[i]);
      }
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

template <>
Status FileWriter::Impl::TypedWriteBatch<FLBAType, ::arrow::FixedSizeBinaryType>(
    ColumnWriter* column_writer, const std::shared_ptr<Array>& array, int64_t num_levels,
    const int16_t* def_levels, const int16_t* rep_levels) {
  RETURN_NOT_OK(data_buffer_.Resize(array->length() * sizeof(FLBA), false));
  auto data = static_cast<const FixedSizeBinaryArray*>(array.get());
  auto buffer_ptr = reinterpret_cast<FLBA*>(data_buffer_.mutable_data());

  auto writer = reinterpret_cast<TypedColumnWriter<FLBAType>*>(column_writer);

  if (writer->descr()->schema_node()->is_required() || (data->null_count() == 0)) {
    // no nulls, just dump the data
    // todo(advancedxy): use a writeBatch to avoid this step
    for (int64_t i = 0; i < data->length(); i++) {
      buffer_ptr[i] = FixedLenByteArray(data->GetValue(i));
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  } else {
    int buffer_idx = 0;
    for (int64_t i = 0; i < data->length(); i++) {
      if (!data->IsNull(i)) {
        buffer_ptr[buffer_idx++] = FixedLenByteArray(data->GetValue(i));
      }
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

Status FileWriter::Impl::Close() {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(writer_->Close());
  return Status::OK();
}

Status FileWriter::NewRowGroup(int64_t chunk_size) {
  return impl_->NewRowGroup(chunk_size);
}

Status FileWriter::Impl::WriteColumnChunk(const Array& data) {
  ColumnWriter* column_writer;
  PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());

  int current_column_idx = row_group_writer_->current_column();
  std::shared_ptr<::arrow::Schema> arrow_schema;
  RETURN_NOT_OK(FromParquetSchema(writer_->schema(), {current_column_idx - 1},
      writer_->key_value_metadata(), &arrow_schema));
  LevelBuilder level_builder(pool_);
  std::shared_ptr<Buffer> def_levels_buffer;
  std::shared_ptr<Buffer> rep_levels_buffer;
  int64_t values_offset;
  ::arrow::Type::type values_type;
  int64_t num_levels;
  int64_t num_values;
  const Array* _values_array;
  RETURN_NOT_OK(level_builder.GenerateLevels(data, arrow_schema->field(0), &values_offset,
      &values_type, &num_values, &num_levels, &def_levels_buffer, &rep_levels_buffer,
      &_values_array));
  const int16_t* def_levels = nullptr;
  if (def_levels_buffer) {
    def_levels = reinterpret_cast<const int16_t*>(def_levels_buffer->data());
  }
  const int16_t* rep_levels = nullptr;
  if (rep_levels_buffer) {
    rep_levels = reinterpret_cast<const int16_t*>(rep_levels_buffer->data());
  }
  std::shared_ptr<Array> values_array = _values_array->Slice(values_offset, num_values);

#define WRITE_BATCH_CASE(ArrowEnum, ArrowType, ParquetType)               \
  case ::arrow::Type::ArrowEnum:                                          \
    return TypedWriteBatch<ParquetType, ::arrow::ArrowType>(              \
        column_writer, values_array, num_levels, def_levels, rep_levels); \
    break;

  switch (values_type) {
    case ::arrow::Type::UINT32: {
      if (writer_->properties()->version() == ParquetVersion::PARQUET_1_0) {
        // Parquet 1.0 reader cannot read the UINT_32 logical type. Thus we need
        // to use the larger Int64Type to store them lossless.
        return TypedWriteBatch<Int64Type, ::arrow::UInt32Type>(
            column_writer, values_array, num_levels, def_levels, rep_levels);
      } else {
        return TypedWriteBatch<Int32Type, ::arrow::UInt32Type>(
            column_writer, values_array, num_levels, def_levels, rep_levels);
      }
    }
      WRITE_BATCH_CASE(BOOL, BooleanType, BooleanType)
      WRITE_BATCH_CASE(INT8, Int8Type, Int32Type)
      WRITE_BATCH_CASE(UINT8, UInt8Type, Int32Type)
      WRITE_BATCH_CASE(INT16, Int16Type, Int32Type)
      WRITE_BATCH_CASE(UINT16, UInt16Type, Int32Type)
      WRITE_BATCH_CASE(INT32, Int32Type, Int32Type)
      WRITE_BATCH_CASE(INT64, Int64Type, Int64Type)
      WRITE_BATCH_CASE(UINT64, UInt64Type, Int64Type)
      WRITE_BATCH_CASE(FLOAT, FloatType, FloatType)
      WRITE_BATCH_CASE(DOUBLE, DoubleType, DoubleType)
      WRITE_BATCH_CASE(BINARY, BinaryType, ByteArrayType)
      WRITE_BATCH_CASE(STRING, BinaryType, ByteArrayType)
      WRITE_BATCH_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryType, FLBAType)
      WRITE_BATCH_CASE(DATE32, Date32Type, Int32Type)
      WRITE_BATCH_CASE(DATE64, Date64Type, Int32Type)
      WRITE_BATCH_CASE(TIMESTAMP, TimestampType, Int64Type)
      WRITE_BATCH_CASE(TIME32, Time32Type, Int32Type)
      WRITE_BATCH_CASE(TIME64, Time64Type, Int64Type)
    default:
      std::stringstream ss;
      ss << "Data type not supported as list value: " << values_array->type()->ToString();
      return Status::NotImplemented(ss.str());
  }

  PARQUET_CATCH_NOT_OK(column_writer->Close());

  return Status::OK();
}

Status FileWriter::WriteColumnChunk(const ::arrow::Array& array) {
  return impl_->WriteColumnChunk(array);
}

Status FileWriter::Close() {
  return impl_->Close();
}

MemoryPool* FileWriter::memory_pool() const {
  return impl_->pool_;
}

FileWriter::~FileWriter() {}

FileWriter::FileWriter(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : impl_(new FileWriter::Impl(pool, std::move(writer))) {}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
    const std::shared_ptr<OutputStream>& sink,
    const std::shared_ptr<WriterProperties>& properties,
    std::unique_ptr<FileWriter>* writer) {
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(ToParquetSchema(&schema, *properties, &parquet_schema));

  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());

  std::unique_ptr<ParquetFileWriter> base_writer =
      ParquetFileWriter::Open(sink, schema_node, properties, schema.metadata());

  writer->reset(new FileWriter(pool, std::move(base_writer)));
  return Status::OK();
}

Status FileWriter::Open(const ::arrow::Schema& schema, ::arrow::MemoryPool* pool,
    const std::shared_ptr<::arrow::io::OutputStream>& sink,
    const std::shared_ptr<WriterProperties>& properties,
    std::unique_ptr<FileWriter>* writer) {
  auto wrapper = std::make_shared<ArrowOutputStream>(sink);
  return Open(schema, pool, wrapper, properties, writer);
}

Status FileWriter::WriteTable(const Table& table, int64_t chunk_size) {
  // TODO(ARROW-232) Support writing chunked arrays.
  for (int i = 0; i < table.num_columns(); i++) {
    if (table.column(i)->data()->num_chunks() != 1) {
      return Status::NotImplemented("No support for writing chunked arrays yet.");
    }
  }

  if (chunk_size <= 0) {
    return Status::Invalid("chunk size per row_group must be greater than 0");
  }

  for (int chunk = 0; chunk * chunk_size < table.num_rows(); chunk++) {
    int64_t offset = chunk * chunk_size;
    int64_t size = std::min(chunk_size, table.num_rows() - offset);
    RETURN_NOT_OK_ELSE(NewRowGroup(size), PARQUET_IGNORE_NOT_OK(Close()));
    for (int i = 0; i < table.num_columns(); i++) {
      std::shared_ptr<Array> array = table.column(i)->data()->chunk(0);
      array = array->Slice(offset, size);
      RETURN_NOT_OK_ELSE(WriteColumnChunk(*array), PARQUET_IGNORE_NOT_OK(Close()));
    }
  }
  return Status::OK();
}

Status WriteTable(const ::arrow::Table& table, ::arrow::MemoryPool* pool,
    const std::shared_ptr<OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  std::unique_ptr<FileWriter> writer;
  RETURN_NOT_OK(FileWriter::Open(*table.schema(), pool, sink, properties, &writer));
  RETURN_NOT_OK(writer->WriteTable(table, chunk_size));
  return writer->Close();
}

Status WriteTable(const ::arrow::Table& table, ::arrow::MemoryPool* pool,
    const std::shared_ptr<::arrow::io::OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  auto wrapper = std::make_shared<ArrowOutputStream>(sink);
  return WriteTable(table, pool, wrapper, chunk_size, properties);
}

}  // namespace arrow
}  // namespace parquet
