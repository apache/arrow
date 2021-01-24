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

#include "arrow/adapters/orc/adapter_util.h"

#include <cmath>
#include <string>
#include <vector>

#include "arrow/array/builder_base.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/range.h"
#include "orc/Exceptions.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"

// alias to not interfere with nested orc namespace
namespace liborc = orc;

namespace arrow {

namespace adapters {

namespace orc {

using internal::checked_cast;

// The number of milliseconds, microseconds and nanoseconds in a second
constexpr int64_t kOneSecondMillis = 1000LL;
constexpr int64_t kOneMicroNanos = 1000LL;
constexpr int64_t kOneSecondMicros = 1000000LL;
constexpr int64_t kOneMilliNanos = 1000000LL;
constexpr int64_t kOneSecondNanos = 1000000000LL;
// Jan 1st 2015 in UNIX timestamp
// constexpr int64_t kConverter = 1420070400LL;

Status AppendStructBatch(const liborc::Type* type,
                         liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                         int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<StructBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StructVectorBatch*>(column_vector_batch);

  const uint8_t* valid_bytes = NULLPTR;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  RETURN_NOT_OK(builder->AppendValues(length, valid_bytes));

  for (int i = 0; i < builder->num_fields(); i++) {
    RETURN_NOT_OK(AppendBatch(type->getSubtype(i), batch->fields[i], offset, length,
                              builder->field_builder(i)));
  }
  return Status::OK();
}

Status AppendListBatch(const liborc::Type* type,
                       liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                       int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<ListBuilder*>(abuilder);
  auto batch = checked_cast<liborc::ListVectorBatch*>(column_vector_batch);
  liborc::ColumnVectorBatch* elements = batch->elements.get();
  const liborc::Type* elemtype = type->getSubtype(0);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      int64_t start = batch->offsets[i];
      int64_t end = batch->offsets[i + 1];
      RETURN_NOT_OK(builder->Append());
      RETURN_NOT_OK(
          AppendBatch(elemtype, elements, start, end - start, builder->value_builder()));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendMapBatch(const liborc::Type* type,
                      liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                      int64_t length, ArrayBuilder* abuilder) {
  auto list_builder = checked_cast<ListBuilder*>(abuilder);
  auto struct_builder = checked_cast<StructBuilder*>(list_builder->value_builder());
  auto batch = checked_cast<liborc::MapVectorBatch*>(column_vector_batch);
  liborc::ColumnVectorBatch* keys = batch->keys.get();
  liborc::ColumnVectorBatch* vals = batch->elements.get();
  const liborc::Type* keytype = type->getSubtype(0);
  const liborc::Type* valtype = type->getSubtype(1);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    RETURN_NOT_OK(list_builder->Append());
    int64_t start = batch->offsets[i];
    int64_t list_length = batch->offsets[i + 1] - start;
    if (list_length && (!has_nulls || batch->notNull[i])) {
      RETURN_NOT_OK(struct_builder->AppendValues(list_length, NULLPTR));
      RETURN_NOT_OK(AppendBatch(keytype, keys, start, list_length,
                                struct_builder->field_builder(0)));
      RETURN_NOT_OK(AppendBatch(valtype, vals, start, list_length,
                                struct_builder->field_builder(1)));
    }
  }
  return Status::OK();
}

template <class builder_type, class batch_type, class elem_type>
Status AppendNumericolumn_vector_batch(liborc::ColumnVectorBatch* column_vector_batch,
                                       int64_t offset, int64_t length,
                                       ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(column_vector_batch);

  if (length == 0) {
    return Status::OK();
  }
  const uint8_t* valid_bytes = NULLPTR;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const elem_type* source = batch->data.data() + offset;
  RETURN_NOT_OK(builder->AppendValues(source, length, valid_bytes));
  return Status::OK();
}

template <class builder_type, class target_type, class batch_type, class source_type>
Status AppendNumericolumn_vector_batchCast(liborc::ColumnVectorBatch* column_vector_batch,
                                           int64_t offset, int64_t length,
                                           ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(column_vector_batch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = NULLPTR;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const source_type* source = batch->data.data() + offset;
  auto cast_iter = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<target_type>(source[index]); },
      length);

  RETURN_NOT_OK(builder->AppendValues(cast_iter.begin(), cast_iter.end(), valid_bytes));

  return Status::OK();
}

Status AppendBoolBatch(liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                       int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<BooleanBuilder*>(abuilder);
  auto batch = checked_cast<liborc::LongVectorBatch*>(column_vector_batch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = NULLPTR;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const int64_t* source = batch->data.data() + offset;

  auto cast_iter = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<bool>(source[index]); }, length);

  RETURN_NOT_OK(builder->AppendValues(cast_iter.begin(), cast_iter.end(), valid_bytes));

  return Status::OK();
}

Status AppendTimestampBatch(liborc::ColumnVectorBatch* column_vector_batch,
                            int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<TimestampBuilder*>(abuilder);
  auto batch = checked_cast<liborc::TimestampVectorBatch*>(column_vector_batch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = NULLPTR;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }

  const int64_t* seconds = batch->data.data() + offset;
  const int64_t* nanos = batch->nanoseconds.data() + offset;

  auto transform_timestamp = [seconds, nanos](int64_t index) {
    return seconds[index] * kOneSecondNanos + nanos[index];
  };

  auto transform_range = internal::MakeLazyRange(transform_timestamp, length);

  RETURN_NOT_OK(
      builder->AppendValues(transform_range.begin(), transform_range.end(), valid_bytes));
  return Status::OK();
}

template <class builder_type>
Status AppendBinaryBatch(liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                         int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(column_vector_batch);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      RETURN_NOT_OK(
          builder->Append(batch->data[i], static_cast<int32_t>(batch->length[i])));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendFixedBinaryBatch(liborc::ColumnVectorBatch* column_vector_batch,
                              int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<FixedSizeBinaryBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(column_vector_batch);

  const bool has_nulls = batch->hasNulls;
  for (int64_t i = offset; i < length + offset; i++) {
    if (!has_nulls || batch->notNull[i]) {
      RETURN_NOT_OK(builder->Append(batch->data[i]));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return Status::OK();
}

Status AppendDecimalBatch(const liborc::Type* type,
                          liborc::ColumnVectorBatch* column_vector_batch, int64_t offset,
                          int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<Decimal128Builder*>(abuilder);

  const bool has_nulls = column_vector_batch->hasNulls;
  if (type->getPrecision() == 0 || type->getPrecision() > 18) {
    auto batch = checked_cast<liborc::Decimal128VectorBatch*>(column_vector_batch);
    for (int64_t i = offset; i < length + offset; i++) {
      if (!has_nulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(
            Decimal128(batch->values[i].getHighBits(), batch->values[i].getLowBits())));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
  } else {
    auto batch = checked_cast<liborc::Decimal64VectorBatch*>(column_vector_batch);
    for (int64_t i = offset; i < length + offset; i++) {
      if (!has_nulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(Decimal128(batch->values[i])));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
  }
  return Status::OK();
}

Status AppendBatch(const liborc::Type* type, liborc::ColumnVectorBatch* batch,
                   int64_t offset, int64_t length, ArrayBuilder* builder) {
  if (type == NULLPTR) {
    return Status::OK();
  }
  liborc::TypeKind kind = type->getKind();
  switch (kind) {
    case liborc::STRUCT:
      return AppendStructBatch(type, batch, offset, length, builder);
    case liborc::LIST:
      return AppendListBatch(type, batch, offset, length, builder);
    case liborc::MAP:
      return AppendMapBatch(type, batch, offset, length, builder);
    case liborc::LONG:
      return AppendNumericolumn_vector_batch<Int64Builder, liborc::LongVectorBatch,
                                             int64_t>(batch, offset, length, builder);
    case liborc::INT:
      return AppendNumericolumn_vector_batchCast<Int32Builder, int32_t,
                                                 liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::SHORT:
      return AppendNumericolumn_vector_batchCast<Int16Builder, int16_t,
                                                 liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::BYTE:
      return AppendNumericolumn_vector_batchCast<Int8Builder, int8_t,
                                                 liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::DOUBLE:
      return AppendNumericolumn_vector_batch<DoubleBuilder, liborc::DoubleVectorBatch,
                                             double>(batch, offset, length, builder);
    case liborc::FLOAT:
      return AppendNumericolumn_vector_batchCast<FloatBuilder, float,
                                                 liborc::DoubleVectorBatch, double>(
          batch, offset, length, builder);
    case liborc::BOOLEAN:
      return AppendBoolBatch(batch, offset, length, builder);
    case liborc::VARCHAR:
    case liborc::STRING:
      return AppendBinaryBatch<StringBuilder>(batch, offset, length, builder);
    case liborc::BINARY:
      return AppendBinaryBatch<BinaryBuilder>(batch, offset, length, builder);
    case liborc::CHAR:
      return AppendFixedBinaryBatch(batch, offset, length, builder);
    case liborc::DATE:
      return AppendNumericolumn_vector_batchCast<Date32Builder, int32_t,
                                                 liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::TIMESTAMP:
      return AppendTimestampBatch(batch, offset, length, builder);
    case liborc::DECIMAL:
      return AppendDecimalBatch(type, batch, offset, length, builder);
    default:
      return Status::NotImplemented("Not implemented type kind: ", kind);
  }
}

template <class array_type, class batch_type>
Status FillNumericBatch(const DataType* type,
                        liborc::ColumnVectorBatch* column_vector_batch,
                        int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                        Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<batch_type*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->data[orc_offset] = array->Value(arrow_offset);
      batch->notNull[orc_offset] = true;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

template <class array_type, class batch_type, class target_type>
Status FillNumericBatchCast(const DataType* type,
                            liborc::ColumnVectorBatch* column_vector_batch,
                            int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                            Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<batch_type*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->data[orc_offset] = static_cast<target_type>(array->Value(arrow_offset));
      batch->notNull[orc_offset] = true;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillDate64Batch(const DataType* type,
                       liborc::ColumnVectorBatch* column_vector_batch,
                       int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                       Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<Date64Array*>(parray);
  auto batch = checked_cast<liborc::TimestampVectorBatch*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      int64_t miliseconds = array->Value(arrow_offset);
      batch->data[orc_offset] =
          static_cast<int64_t>(std::floor(miliseconds / kOneSecondMillis));
      batch->nanoseconds[orc_offset] =
          (miliseconds - kOneSecondMillis * batch->data[orc_offset]) * kOneMilliNanos;
      batch->notNull[orc_offset] = true;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillTimestampBatch(const DataType* type,
                          liborc::ColumnVectorBatch* column_vector_batch,
                          int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                          Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<TimestampArray*>(parray);
  auto batch = checked_cast<liborc::TimestampVectorBatch*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      int64_t data = array->Value(arrow_offset);
      batch->notNull[orc_offset] = true;
      switch (std::static_pointer_cast<TimestampType>(array->type())->unit()) {
        case TimeUnit::type::SECOND: {
          batch->data[orc_offset] = data;
          batch->nanoseconds[orc_offset] = 0;
          break;
        }
        case TimeUnit::type::MILLI: {
          batch->data[orc_offset] =
              static_cast<int64_t>(std::floor(data / kOneSecondMillis));
          batch->nanoseconds[orc_offset] =
              (data - kOneSecondMillis * batch->data[orc_offset]) * kOneMilliNanos;
          break;
        }
        case TimeUnit::type::MICRO: {
          batch->data[orc_offset] =
              static_cast<int64_t>(std::floor(data / kOneSecondMicros));
          batch->nanoseconds[orc_offset] =
              (data - kOneSecondMicros * batch->data[orc_offset]) * kOneMicroNanos;
          break;
        }
        default: {
          batch->data[orc_offset] =
              static_cast<int64_t>(std::floor(data / kOneSecondNanos));
          batch->nanoseconds[orc_offset] =
              data - kOneSecondNanos * batch->data[orc_offset];
        }
      }
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

template <class array_type>
Status FillStringBatch(const DataType* type,
                       liborc::ColumnVectorBatch* column_vector_batch,
                       int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                       Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<liborc::StringVectorBatch*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->notNull[orc_offset] = true;
      std::string dataString = array->GetString(arrow_offset);
      int dataStringLength = dataString.length();
      if (batch->data[orc_offset]) delete batch->data[orc_offset];
      batch->data[orc_offset] = new char[dataStringLength + 1];  // Include null
      memcpy(batch->data[orc_offset], dataString.c_str(), dataStringLength + 1);
      batch->length[orc_offset] = dataStringLength;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

template <class array_type, class offset_type>
Status FillBinaryBatch(const DataType* type,
                       liborc::ColumnVectorBatch* column_vector_batch,
                       int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                       Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<liborc::StringVectorBatch*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->notNull[orc_offset] = true;
      offset_type dataLength = 0;
      const uint8_t* data = array->GetValue(arrow_offset, &dataLength);
      if (batch->data[orc_offset]) delete batch->data[orc_offset];
      batch->data[orc_offset] = new char[dataLength];  // Do not include null
      memcpy(batch->data[orc_offset], data, dataLength);
      batch->length[orc_offset] = dataLength;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillFixedSizeBinaryBatch(const DataType* type,
                                liborc::ColumnVectorBatch* column_vector_batch,
                                int64_t& arrow_offset, int64_t& orc_offset,
                                int64_t length, Array* parray,
                                std::vector<bool>* incoming_mask) {
  auto array = checked_cast<FixedSizeBinaryArray*>(parray);
  auto batch = checked_cast<liborc::StringVectorBatch*>(column_vector_batch);
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  int32_t byteWidth = array->byte_width();
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->notNull[orc_offset] = true;
      const uint8_t* data = array->GetValue(arrow_offset);
      if (batch->data[orc_offset]) delete batch->data[orc_offset];
      batch->data[orc_offset] = new char[byteWidth];  // Do not include null
      memcpy(batch->data[orc_offset], data, byteWidth);
      batch->length[orc_offset] = byteWidth;
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

// If Arrow supports 256-bit decimals we can not support it unless ORC does it
Status FillDecimalBatch(const DataType* type,
                        liborc::ColumnVectorBatch* column_vector_batch,
                        int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                        Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<Decimal128Array*>(parray);
  auto batch = checked_cast<liborc::Decimal128VectorBatch*>(column_vector_batch);
  // Arrow uses 128 bits for decimal type and in the future, 256 bits will also be
  // supported.
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
    } else {
      batch->notNull[orc_offset] = true;
      uint8_t* rawInt128 = const_cast<uint8_t*>(array->GetValue(arrow_offset));
      uint64_t* lowerBits = reinterpret_cast<uint64_t*>(rawInt128);
      int64_t* higherBits = reinterpret_cast<int64_t*>(rawInt128 + 8);
      batch->values[orc_offset] = liborc::Int128(*higherBits, *lowerBits);
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillStructBatch(const DataType* type,
                       liborc::ColumnVectorBatch* column_vector_batch,
                       int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                       Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<StructArray*>(parray);
  auto batch = checked_cast<liborc::StructVectorBatch*>(column_vector_batch);
  std::shared_ptr<std::vector<bool>> outgoingMask;
  std::size_t size = type->fields().size();
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  int64_t initorc_offset = orc_offset;
  int64_t initarrow_offset = arrow_offset;
  // First fill fields of ColumnVectorBatch
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
    outgoingMask = std::make_shared<std::vector<bool>>(length, true);
  } else {
    outgoingMask = NULLPTR;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
      (*outgoingMask)[orc_offset] = false;
    } else {
      batch->notNull[orc_offset] = true;
    }
  }
  batch->numElements += orc_offset - initorc_offset;
  // Fill the fields
  for (std::size_t i = 0; i < size; i++) {
    orc_offset = initorc_offset;
    arrow_offset = initarrow_offset;
    RETURN_NOT_OK(FillBatch(type->field(i)->type().get(), batch->fields[i], arrow_offset,
                            orc_offset, length, array->field(i).get(),
                            outgoingMask.get()));
  }
  return Status::OK();
}

template <class array_type>
Status FillListBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                     int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                     Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<liborc::ListVectorBatch*>(column_vector_batch);
  liborc::ColumnVectorBatch* element_batch = (batch->elements).get();
  DataType* element_type = array->value_type().get();
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (orc_offset == 0) {
    batch->offsets[0] = 0;
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset];
    } else {
      batch->notNull[orc_offset] = true;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset] +
                                       array->value_offset(arrow_offset + 1) -
                                       array->value_offset(arrow_offset);
      element_batch->resize(batch->offsets[orc_offset + 1]);
      int64_t subarray_arrow_offset = array->value_offset(arrow_offset),
              subarray_orc_offset = batch->offsets[orc_offset],
              subarray_orc_length = batch->offsets[orc_offset + 1];
      RETURN_NOT_OK(FillBatch(element_type, element_batch, subarray_arrow_offset,
                              subarray_orc_offset, subarray_orc_length,
                              array->values().get(), NULLPTR));
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillFixedSizeListBatch(const DataType* type,
                              liborc::ColumnVectorBatch* column_vector_batch,
                              int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                              Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<FixedSizeListArray*>(parray);
  auto batch = checked_cast<liborc::ListVectorBatch*>(column_vector_batch);
  liborc::ColumnVectorBatch* element_batch = (batch->elements).get();
  DataType* element_type = array->value_type().get();
  int64_t arrow_length = array->length();
  int32_t element_length = array->value_length();  // Fixed length of each subarray
  if (!arrow_length) {
    return Status::OK();
  }
  if (orc_offset == 0) {
    batch->offsets[0] = 0;
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset];
    } else {
      batch->notNull[orc_offset] = true;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset] + element_length;
      int64_t subarray_arrow_offset = array->value_offset(arrow_offset),
              subarray_orc_offset = batch->offsets[orc_offset],
              subarray_orc_length = batch->offsets[orc_offset + 1];
      element_batch->resize(subarray_orc_length);
      RETURN_NOT_OK(FillBatch(element_type, element_batch, subarray_arrow_offset,
                              subarray_orc_offset, subarray_orc_length,
                              array->values().get(), NULLPTR));
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillMapBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                    int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                    Array* parray, std::vector<bool>* incoming_mask) {
  auto array = checked_cast<MapArray*>(parray);
  auto batch = checked_cast<liborc::MapVectorBatch*>(column_vector_batch);
  liborc::ColumnVectorBatch* keyBatch = (batch->keys).get();
  liborc::ColumnVectorBatch* element_batch = (batch->elements).get();
  Array* key_array = array->keys().get();
  Array* element_array = array->items().get();
  DataType* keyType = key_array->type().get();
  DataType* element_type = element_array->type().get();
  int64_t arrow_length = array->length();
  if (!arrow_length) {
    return Status::OK();
  }
  if (orc_offset == 0) {
    batch->offsets[0] = 0;
  }
  if (array->null_count() || incoming_mask) {
    batch->hasNulls = true;
  }
  for (; orc_offset < length && arrow_offset < arrow_length;
       orc_offset++, arrow_offset++) {
    if (array->IsNull(arrow_offset) || (incoming_mask && !(*incoming_mask)[orc_offset])) {
      batch->notNull[orc_offset] = false;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset];
    } else {
      batch->notNull[orc_offset] = true;
      batch->offsets[orc_offset + 1] = batch->offsets[orc_offset] +
                                       array->value_offset(arrow_offset + 1) -
                                       array->value_offset(arrow_offset);
      int64_t subarray_arrow_offset = array->value_offset(arrow_offset),
              subarray_orc_offset = batch->offsets[orc_offset],
              subarray_orc_length = batch->offsets[orc_offset + 1],
              initsubarray_arrow_offset = subarray_arrow_offset,
              initsubarray_orc_offset = subarray_orc_offset;
      keyBatch->resize(subarray_orc_length);
      element_batch->resize(subarray_orc_length);
      RETURN_NOT_OK(FillBatch(keyType, keyBatch, subarray_arrow_offset,
                              subarray_orc_offset, subarray_orc_length, key_array,
                              NULLPTR));
      subarray_arrow_offset = initsubarray_arrow_offset;
      subarray_orc_offset = initsubarray_orc_offset;
      RETURN_NOT_OK(FillBatch(element_type, element_batch, subarray_arrow_offset,
                              subarray_orc_offset, subarray_orc_length, element_array,
                              NULLPTR));
    }
  }
  batch->numElements = orc_offset;
  return Status::OK();
}

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                 int64_t& arrow_offset, int64_t& orc_offset, int64_t length,
                 Array* parray, std::vector<bool>* incoming_mask) {
  Type::type kind = type->id();
  switch (kind) {
    case Type::type::BOOL:
      return FillNumericBatchCast<BooleanArray, liborc::LongVectorBatch, int64_t>(
          type, column_vector_batch, arrow_offset, orc_offset, length, parray,
          incoming_mask);
    case Type::type::INT8:
      return FillNumericBatchCast<NumericArray<arrow::Int8Type>, liborc::LongVectorBatch,
                                  int64_t>(type, column_vector_batch, arrow_offset,
                                           orc_offset, length, parray, incoming_mask);
    case Type::type::INT16:
      return FillNumericBatchCast<NumericArray<arrow::Int16Type>, liborc::LongVectorBatch,
                                  int64_t>(type, column_vector_batch, arrow_offset,
                                           orc_offset, length, parray, incoming_mask);
    case Type::type::INT32:
      return FillNumericBatchCast<NumericArray<arrow::Int32Type>, liborc::LongVectorBatch,
                                  int64_t>(type, column_vector_batch, arrow_offset,
                                           orc_offset, length, parray, incoming_mask);
    case Type::type::INT64:
      return FillNumericBatch<NumericArray<arrow::Int64Type>, liborc::LongVectorBatch>(
          type, column_vector_batch, arrow_offset, orc_offset, length, parray,
          incoming_mask);
    case Type::type::FLOAT:
      return FillNumericBatchCast<NumericArray<arrow::FloatType>,
                                  liborc::DoubleVectorBatch, double>(
          type, column_vector_batch, arrow_offset, orc_offset, length, parray,
          incoming_mask);
    case Type::type::DOUBLE:
      return FillNumericBatch<NumericArray<arrow::DoubleType>, liborc::DoubleVectorBatch>(
          type, column_vector_batch, arrow_offset, orc_offset, length, parray,
          incoming_mask);
    case Type::type::BINARY:
      return FillBinaryBatch<BinaryArray, int32_t>(type, column_vector_batch,
                                                   arrow_offset, orc_offset, length,
                                                   parray, incoming_mask);
    case Type::type::LARGE_BINARY:
      return FillBinaryBatch<LargeBinaryArray, int64_t>(type, column_vector_batch,
                                                        arrow_offset, orc_offset, length,
                                                        parray, incoming_mask);
    case Type::type::STRING:
      return FillStringBatch<StringArray>(type, column_vector_batch, arrow_offset,
                                          orc_offset, length, parray, incoming_mask);
    case Type::type::LARGE_STRING:
      return FillStringBatch<LargeStringArray>(type, column_vector_batch, arrow_offset,
                                               orc_offset, length, parray, incoming_mask);
    case Type::type::FIXED_SIZE_BINARY:
      return FillFixedSizeBinaryBatch(type, column_vector_batch, arrow_offset, orc_offset,
                                      length, parray, incoming_mask);
    case Type::type::DATE32:
      return FillNumericBatchCast<NumericArray<arrow::Date32Type>,
                                  liborc::LongVectorBatch, int64_t>(
          type, column_vector_batch, arrow_offset, orc_offset, length, parray,
          incoming_mask);
    case Type::type::DATE64:
      return FillDate64Batch(type, column_vector_batch, arrow_offset, orc_offset, length,
                             parray, incoming_mask);
    case Type::type::TIMESTAMP:
      return FillTimestampBatch(type, column_vector_batch, arrow_offset, orc_offset,
                                length, parray, incoming_mask);
    case Type::type::DECIMAL:
      return FillDecimalBatch(type, column_vector_batch, arrow_offset, orc_offset, length,
                              parray, incoming_mask);
    case Type::type::STRUCT:
      return FillStructBatch(type, column_vector_batch, arrow_offset, orc_offset, length,
                             parray, incoming_mask);
    case Type::type::LIST:
      return FillListBatch<ListArray>(type, column_vector_batch, arrow_offset, orc_offset,
                                      length, parray, incoming_mask);
    case Type::type::LARGE_LIST:
      return FillListBatch<LargeListArray>(type, column_vector_batch, arrow_offset,
                                           orc_offset, length, parray, incoming_mask);
    case Type::type::FIXED_SIZE_LIST:
      return FillFixedSizeListBatch(type, column_vector_batch, arrow_offset, orc_offset,
                                    length, parray, incoming_mask);
    case Type::type::MAP:
      return FillMapBatch(type, column_vector_batch, arrow_offset, orc_offset, length,
                          parray, incoming_mask);
    default: {
      return Status::Invalid("Unknown or unsupported Arrow type kind: ", kind);
    }
  }
  return Status::OK();
}

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* column_vector_batch,
                 int64_t& arrow_index_offset, int& arrow_chunk_offset, int64_t length,
                 ChunkedArray* chunked_array) {
  int num_batch = chunked_array->num_chunks();
  int64_t orc_offset = 0;
  Status st;
  while (arrow_chunk_offset < num_batch && orc_offset < length) {
    st = FillBatch(type, column_vector_batch, arrow_index_offset, orc_offset, length,
                   chunked_array->chunk(arrow_chunk_offset).get(), NULLPTR);
    if (!st.ok()) {
      return st;
    }
    if (arrow_chunk_offset < num_batch && orc_offset < length) {
      arrow_index_offset = 0;
      arrow_chunk_offset++;
    }
  }
  return Status::OK();
}

Status GetArrowType(const liborc::Type* type, std::shared_ptr<DataType>* out) {
  // When subselecting fields on read, liborc will set some nodes to NULLPTR,
  // so we need to check for NULLPTR before progressing
  if (type == NULLPTR) {
    *out = null();
    return Status::OK();
  }
  liborc::TypeKind kind = type->getKind();
  const int subtype_count = static_cast<int>(type->getSubtypeCount());

  switch (kind) {
    case liborc::BOOLEAN:
      *out = boolean();
      break;
    case liborc::BYTE:
      *out = int8();
      break;
    case liborc::SHORT:
      *out = int16();
      break;
    case liborc::INT:
      *out = int32();
      break;
    case liborc::LONG:
      *out = int64();
      break;
    case liborc::FLOAT:
      *out = float32();
      break;
    case liborc::DOUBLE:
      *out = float64();
      break;
    case liborc::VARCHAR:
    case liborc::STRING:
      *out = utf8();
      break;
    case liborc::BINARY:
      *out = binary();
      break;
    case liborc::CHAR:
      *out = fixed_size_binary(static_cast<int>(type->getMaximumLength()));
      break;
    case liborc::TIMESTAMP:
      *out = timestamp(TimeUnit::NANO);
      break;
    case liborc::DATE:
      *out = date32();
      break;
    case liborc::DECIMAL: {
      const int precision = static_cast<int>(type->getPrecision());
      const int scale = static_cast<int>(type->getScale());
      if (precision == 0) {
        // In HIVE 0.11/0.12 precision is set as 0, but means max precision
        *out = decimal(38, 6);
      } else {
        *out = decimal(precision, scale);
      }
      break;
    }
    case liborc::LIST: {
      if (subtype_count != 1) {
        return Status::Invalid("Invalid Orc List type");
      }
      std::shared_ptr<DataType> elemtype;
      RETURN_NOT_OK(GetArrowType(type->getSubtype(0), &elemtype));
      *out = list(elemtype);
      break;
    }
    case liborc::MAP: {
      if (subtype_count != 2) {
        return Status::Invalid("Invalid Orc Map type");
      }
      std::shared_ptr<DataType> keytype;
      std::shared_ptr<DataType> valtype;
      RETURN_NOT_OK(GetArrowType(type->getSubtype(0), &keytype));
      RETURN_NOT_OK(GetArrowType(type->getSubtype(1), &valtype));
      *out = list(struct_({field("key", keytype), field("value", valtype)}));
      break;
    }
    case liborc::STRUCT: {
      std::vector<std::shared_ptr<Field>> fields;
      for (int child = 0; child < subtype_count; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(GetArrowType(type->getSubtype(child), &elemtype));
        std::string name = type->getFieldName(child);
        fields.push_back(field(name, elemtype));
      }
      *out = struct_(fields);
      break;
    }
    case liborc::UNION: {
      std::vector<std::shared_ptr<Field>> fields;
      std::vector<int8_t> type_codes;
      for (int child = 0; child < subtype_count; ++child) {
        std::shared_ptr<DataType> elemtype;
        RETURN_NOT_OK(GetArrowType(type->getSubtype(child), &elemtype));
        fields.push_back(field("_union_" + std::to_string(child), elemtype));
        type_codes.push_back(static_cast<int8_t>(child));
      }
      *out = sparse_union(fields, type_codes);
      break;
    }
    default: {
      return Status::Invalid("Unknown Orc type kind: ", kind);
    }
  }
  return Status::OK();
}

Status GetORCType(const DataType& type, ORC_UNIQUE_PTR<liborc::Type>* out) {
  Type::type kind = type.id();
  switch (kind) {
    case Type::type::NA: {  // Makes out NULLPTR
      out->reset();
      break;
    }
    case Type::type::BOOL:
      *out = liborc::createPrimitiveType(liborc::TypeKind::BOOLEAN);
      break;
    case Type::type::INT8:
      *out = liborc::createPrimitiveType(liborc::TypeKind::BYTE);
      break;
    case Type::type::INT16:
      *out = liborc::createPrimitiveType(liborc::TypeKind::SHORT);
      break;
    case Type::type::INT32:
      *out = liborc::createPrimitiveType(liborc::TypeKind::INT);
      break;
    case Type::type::INT64:
      *out = liborc::createPrimitiveType(liborc::TypeKind::LONG);
      break;
    case Type::type::FLOAT:
      *out = liborc::createPrimitiveType(liborc::TypeKind::FLOAT);
      break;
    case Type::type::DOUBLE:
      *out = liborc::createPrimitiveType(liborc::TypeKind::DOUBLE);
      break;
    // Use STRING instead of VARCHAR for now, both use UTF-8
    case Type::type::STRING:
    case Type::type::LARGE_STRING:
      *out = liborc::createPrimitiveType(liborc::TypeKind::STRING);
      break;
    case Type::type::BINARY:
    case Type::type::LARGE_BINARY:
    case Type::type::FIXED_SIZE_BINARY:
      *out = liborc::createPrimitiveType(liborc::TypeKind::BINARY);
      break;
    case Type::type::DATE32:
      *out = liborc::createPrimitiveType(liborc::TypeKind::DATE);
      break;
    case Type::type::DATE64:
    case Type::type::TIMESTAMP:
      *out = liborc::createPrimitiveType(liborc::TypeKind::TIMESTAMP);
      break;
    case Type::type::DECIMAL128: {
      const uint64_t precision =
          static_cast<uint64_t>(static_cast<const Decimal128Type&>(type).precision());
      const uint64_t scale =
          static_cast<uint64_t>(static_cast<const Decimal128Type&>(type).scale());
      *out = liborc::createDecimalType(precision, scale);
      break;
    }
    case Type::type::LIST:
    case Type::type::FIXED_SIZE_LIST:
    case Type::type::LARGE_LIST: {
      std::shared_ptr<DataType> arrowChildType =
          static_cast<const BaseListType&>(type).value_type();
      ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
      RETURN_NOT_OK(GetORCType(*arrowChildType, &orcSubtype));
      *out = liborc::createListType(std::move(orcSubtype));
      break;
    }
    case Type::type::STRUCT: {
      *out = liborc::createStructType();
      std::vector<std::shared_ptr<Field>> arrowFields =
          checked_cast<const StructType&>(type).fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin();
           it != arrowFields.end(); ++it) {
        std::string fieldName = (*it)->name();
        std::shared_ptr<DataType> arrowChildType = (*it)->type();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(*arrowChildType, &orcSubtype));
        (*out)->addStructField(fieldName, std::move(orcSubtype));
      }
      break;
    }
    case Type::type::MAP: {
      std::shared_ptr<DataType> keyArrowType =
          checked_cast<const MapType&>(type).key_type();
      std::shared_ptr<DataType> itemArrowType =
          checked_cast<const MapType&>(type).item_type();
      ORC_UNIQUE_PTR<liborc::Type> keyORCType, itemORCType;
      RETURN_NOT_OK(GetORCType(*keyArrowType, &keyORCType));
      RETURN_NOT_OK(GetORCType(*itemArrowType, &itemORCType));
      *out = liborc::createMapType(std::move(keyORCType), std::move(itemORCType));
      break;
    }
    case Type::type::DENSE_UNION:
    case Type::type::SPARSE_UNION: {
      *out = liborc::createUnionType();
      std::vector<std::shared_ptr<Field>> arrowFields =
          checked_cast<const UnionType&>(type).fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin();
           it != arrowFields.end(); ++it) {
        std::string fieldName = (*it)->name();
        std::shared_ptr<DataType> arrowChildType = (*it)->type();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(*arrowChildType, &orcSubtype));
        (*out)->addUnionChild(std::move(orcSubtype));
      }
      break;
    }
    // Dictionary is an encoding method, not a TypeKind in ORC. Hence we need to get the
    // actual value type.
    // case Type::type::DICTIONARY: {
    //   DataType* arrowValueType =
    //       checked_cast<const DictionaryType*>(type)->dictionary()->type().get();
    //   RETURN_NOT_OK(GetORCType(arrowValueType, std::move(out)));
    // }
    default: {
      return Status::Invalid("Unknown or unsupported Arrow type kind: ", kind);
    }
  }
  return Status::OK();
}

Status GetORCType(const Schema& schema, ORC_UNIQUE_PTR<liborc::Type>* out) {
  int numFields = schema.num_fields();
  *out = liborc::createStructType();
  for (int i = 0; i < numFields; i++) {
    std::shared_ptr<Field> field = schema.field(i);
    std::string fieldName = field->name();
    std::shared_ptr<DataType> arrowChildType = field->type();
    ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
    RETURN_NOT_OK(GetORCType(*arrowChildType, &orcSubtype));
    (*out)->addStructField(fieldName, std::move(orcSubtype));
  }
  return Status::OK();
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
