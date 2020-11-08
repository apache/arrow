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

#include <string>
#include <vector>

#include "arrow/adapters/orc/adapter_util.h"
#include "arrow/array/builder_base.h"
#include "arrow/builder.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/chunked_array.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/range.h"

#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"
#include "orc/MemoryPool.hh"

// alias to not interfere with nested orc namespace
namespace liborc = orc;

namespace arrow {

namespace adapters {

namespace orc {

using internal::checked_cast;

// The number of nanoseconds in a second
constexpr int64_t kOneSecondNanos = 1000000000LL;

Status AppendStructBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                         int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<StructBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StructVectorBatch*>(cbatch);

  const uint8_t* valid_bytes = nullptr;
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

Status AppendListBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                       int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<ListBuilder*>(abuilder);
  auto batch = checked_cast<liborc::ListVectorBatch*>(cbatch);
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

Status AppendMapBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                      int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto list_builder = checked_cast<ListBuilder*>(abuilder);
  auto struct_builder = checked_cast<StructBuilder*>(list_builder->value_builder());
  auto batch = checked_cast<liborc::MapVectorBatch*>(cbatch);
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
      RETURN_NOT_OK(struct_builder->AppendValues(list_length, nullptr));
      RETURN_NOT_OK(AppendBatch(keytype, keys, start, list_length,
                                struct_builder->field_builder(0)));
      RETURN_NOT_OK(AppendBatch(valtype, vals, start, list_length,
                                struct_builder->field_builder(1)));
    }
  }
  return Status::OK();
}

template <class builder_type, class batch_type, class elem_type>
Status AppendNumericBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                          int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }
  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const elem_type* source = batch->data.data() + offset;
  RETURN_NOT_OK(builder->AppendValues(source, length, valid_bytes));
  return Status::OK();
}

template <class builder_type, class target_type, class batch_type, class source_type>
Status AppendNumericBatchCast(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                              int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<batch_type*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
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

Status AppendBoolBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset, int64_t length,
                       ArrayBuilder* abuilder) {
  auto builder = checked_cast<BooleanBuilder*>(abuilder);
  auto batch = checked_cast<liborc::LongVectorBatch*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
  if (batch->hasNulls) {
    valid_bytes = reinterpret_cast<const uint8_t*>(batch->notNull.data()) + offset;
  }
  const int64_t* source = batch->data.data() + offset;

  auto cast_iter = internal::MakeLazyRange(
      [&source](int64_t index) { return static_cast<bool>(source[index]); }, length);

  RETURN_NOT_OK(builder->AppendValues(cast_iter.begin(), cast_iter.end(), valid_bytes));

  return Status::OK();
}

Status AppendTimestampBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                            int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<TimestampBuilder*>(abuilder);
  auto batch = checked_cast<liborc::TimestampVectorBatch*>(cbatch);

  if (length == 0) {
    return Status::OK();
  }

  const uint8_t* valid_bytes = nullptr;
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
Status AppendBinaryBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                         int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<builder_type*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);

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

Status AppendFixedBinaryBatch(liborc::ColumnVectorBatch* cbatch, int64_t offset,
                              int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<FixedSizeBinaryBuilder*>(abuilder);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);

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

Status AppendDecimalBatch(const liborc::Type* type, liborc::ColumnVectorBatch* cbatch,
                          int64_t offset, int64_t length, ArrayBuilder* abuilder) {
  auto builder = checked_cast<Decimal128Builder*>(abuilder);

  const bool has_nulls = cbatch->hasNulls;
  if (type->getPrecision() == 0 || type->getPrecision() > 18) {
    auto batch = checked_cast<liborc::Decimal128VectorBatch*>(cbatch);
    for (int64_t i = offset; i < length + offset; i++) {
      if (!has_nulls || batch->notNull[i]) {
        RETURN_NOT_OK(builder->Append(
            Decimal128(batch->values[i].getHighBits(), batch->values[i].getLowBits())));
      } else {
        RETURN_NOT_OK(builder->AppendNull());
      }
    }
  } else {
    auto batch = checked_cast<liborc::Decimal64VectorBatch*>(cbatch);
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
  if (type == nullptr) {
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
      return AppendNumericBatch<Int64Builder, liborc::LongVectorBatch, int64_t>(
          batch, offset, length, builder);
    case liborc::INT:
      return AppendNumericBatchCast<Int32Builder, int32_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::SHORT:
      return AppendNumericBatchCast<Int16Builder, int16_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::BYTE:
      return AppendNumericBatchCast<Int8Builder, int8_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::DOUBLE:
      return AppendNumericBatch<DoubleBuilder, liborc::DoubleVectorBatch, double>(
          batch, offset, length, builder);
    case liborc::FLOAT:
      return AppendNumericBatchCast<FloatBuilder, float, liborc::DoubleVectorBatch,
                                    double>(batch, offset, length, builder);
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
      return AppendNumericBatchCast<Date32Builder, int32_t, liborc::LongVectorBatch,
                                    int64_t>(batch, offset, length, builder);
    case liborc::TIMESTAMP:
      return AppendTimestampBatch(batch, offset, length, builder);
    case liborc::DECIMAL:
      return AppendDecimalBatch(type, batch, offset, length, builder);
    default:
      return Status::NotImplemented("Not implemented type kind: ", kind);
  }
}

template <class array_type, class batch_type>
Status FillNumericBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<batch_type*>(cbatch);
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset;
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
    else {
      batch->data[orcOffset] = array->Value(arrowOffset);
    }
  }
  batch->numElements += orcOffset - initORCOffset;
  return Status::OK();
}

template <class array_type, class batch_type, class target_type>
Status FillNumericBatchCast(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<batch_type*>(cbatch);
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset;
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
    else {
      batch->data[orcOffset] = static_cast<target_type>(array->Value(arrowOffset));
    }
  }
  batch->numElements += orcOffset - initORCOffset;
  return Status::OK();
}

template <class array_type>
Status FillBinaryBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset;
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
    else {
      std::string dataString = array->GetString(arrowOffset);
      int dataStringLength = dataString.length();  
      if (batch->data[orcOffset])
        delete batch->data[orcOffset];
      batch->data[orcOffset] = new char[dataStringLength];
      memcpy(batch->data[orcOffset], dataString.c_str(), dataStringLength);
      batch->length[orcOffset] = dataStringLength;
    }
  }
  batch->numElements += orcOffset - initORCOffset;
  return Status::OK();
}

Status FillFixedSizeBinaryBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<FixedSizeBinaryArray*>(parray);
  auto batch = checked_cast<liborc::StringVectorBatch*>(cbatch);
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset;
  int32_t byteWidth = array->byte_width(); 
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
    else {
      std::string dataString = array->GetString(arrowOffset);
      if (batch->data[orcOffset])
        delete batch->data[orcOffset];
      batch->data[orcOffset] = new char[byteWidth];
      memcpy(batch->data[orcOffset], array->GetString(arrowOffset).c_str(), byteWidth);
      batch->length[orcOffset] = static_cast<int64_t>(byteWidth);
    }
  }
  batch->numElements += orcOffset - initORCOffset;
  return Status::OK();
}

//If Arrow supports 256-bit decimals we can not support it unless ORC does it
// Status FillDecimalBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
//   auto array = checked_cast<Decimal128Array*>(parray);
//   auto batch = checked_cast<liborc::Decimal128VectorBatch*>(cbatch);//Arrow uses 128 bits for decimal type and in the future, 256 bits will also be supported.
//   int64_t arrowLength = array->length();
//   if (!arrowLength)
//     return Status::OK();
//   int64_t arrowEnd = arrowOffset + arrowLength;
//   int64_t initORCOffset = orcOffset;
//   if (array->null_count())
//     batch->hasNulls = true;
//   for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
//     if (array->IsNull(arrowOffset)) {
//       batch->notNull[orcOffset] = false;
//     }
//     else {
//       char* rawInt128 = const_cast<char *>(array->GetString(arrowOffset).c_str());
//       uint64_t * lowerBits = reinterpret_cast<uint64_t *>(rawInt128);
//       int64_t * higherBits = reinterpret_cast<int64_t *>(rawInt128 + 8);
//       batch->values[orcOffset] = liborc::Int128(*higherBits, *lowerBits);
//     }
//   }
//   batch->numElements += orcOffset - initORCOffset;
//   return Status::OK();
// }

Status FillStructBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<StructArray*>(parray);
  auto batch = checked_cast<liborc::StructVectorBatch*>(cbatch);
  std::size_t size = type->fields().size();
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset;
  int64_t initArrowOffset = arrowOffset;
  //First fill fields of ColumnVectorBatch
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
  }
  batch->numElements += orcOffset - initORCOffset;
  //Fill the fields
  for (std::size_t i = 0; i < size; i++) {
    orcOffset = initORCOffset;
    arrowOffset = initArrowOffset;
    RETURN_NOT_OK(FillBatch(type->field(i)->type().get(), batch->fields[i], arrowOffset, orcOffset, length, array->field(i).get()));
  }
  return Status::OK();
}

template <class array_type, class scalar_type>
Status FillListBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<array_type*>(parray);
  auto batch = checked_cast<liborc::ListVectorBatch*>(cbatch);
  auto elementBatch = (batch->elements).get();
  DataType* elementType = array->value_type().get();
  int64_t arrowLength = array->length();
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset, initArrowOffset = arrowOffset;
  if (orcOffset == 0)
    batch->offsets[0] = 0;
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    batch->offsets[orcOffset + 1] = batch->offsets[orcOffset] + array->value_offset(arrowOffset + 1) - array->value_offset(arrowOffset);
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
  }
  batch->numElements += orcOffset - initORCOffset; 
  int64_t initSubarrayArrowOffset = array->value_offset(initArrowOffset), initSubarrayORCOffset = batch->offsets[initORCOffset];
  //Let the subbatch take care of itself. Don't manipulate it here.
  RETURN_NOT_OK(FillBatch(elementType, elementBatch, initSubarrayArrowOffset, initSubarrayORCOffset, array->value_offset(arrowOffset) - array->value_offset(initArrowOffset), array->values().get()));
  return Status::OK();
}

Status FillFixedSizeListBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  auto array = checked_cast<FixedSizeListArray*>(parray);
  auto batch = checked_cast<liborc::ListVectorBatch*>(cbatch);
  auto elementBatch = (batch->elements).get();
  DataType* elementType = array->value_type().get();
  int64_t arrowLength = array->length();
  int32_t elementLength = array->value_length(); //Fixed length of each subarray
  if (!arrowLength)
    return Status::OK();
  int64_t arrowEnd = arrowOffset + arrowLength;
  int64_t initORCOffset = orcOffset, initArrowOffset = arrowOffset;
  if (orcOffset == 0)
    batch->offsets[0] = 0;
  if (array->null_count())
    batch->hasNulls = true;
  for (; orcOffset < length && arrowOffset < arrowEnd; orcOffset++, arrowOffset++) {
    batch->offsets[orcOffset + 1] = batch->offsets[orcOffset] + elementLength;
    if (array->IsNull(arrowOffset)) {
      batch->notNull[orcOffset] = false;
    }
  }
  int64_t numProcessedSubarrays = orcOffset - initORCOffset;
  batch->numElements += numProcessedSubarrays; 
  int64_t initSubarrayArrowOffset = array->value_offset(initArrowOffset), initSubarrayORCOffset = batch->offsets[initORCOffset];
  //Let the subbatch take care of itself. Don't manipulate it here.
  RETURN_NOT_OK(FillBatch(elementType, elementBatch, initSubarrayArrowOffset, initSubarrayORCOffset, elementLength * numProcessedSubarrays, array->values().get()));
  return Status::OK();
}

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowOffset, int64_t& orcOffset, int64_t length, Array* parray){
  //Check for nullptr
  
  if (type == nullptr){
    cbatch = nullptr;
    return Status::OK();
  }
  Type::type kind = type->id();
  switch (kind) {
    case Type::type::NA:
      cbatch = nullptr; //Makes cbatch nullptr
      break;
    case Type::type::BOOL:
      return FillNumericBatchCast<BooleanArray, liborc::LongVectorBatch, int64_t>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::INT8:
      return FillNumericBatchCast<NumericArray<arrow::Int8Type>, liborc::LongVectorBatch, int64_t>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::INT16:
      return FillNumericBatchCast<NumericArray<arrow::Int16Type>, liborc::LongVectorBatch, int64_t>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::INT32:
      return FillNumericBatchCast<NumericArray<arrow::Int32Type>, liborc::LongVectorBatch, int64_t>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::INT64:
      return FillNumericBatch<NumericArray<arrow::Int64Type>, liborc::LongVectorBatch>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::FLOAT:
      return FillNumericBatchCast<NumericArray<arrow::FloatType>, liborc::DoubleVectorBatch, double>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::DOUBLE:
      return FillNumericBatch<NumericArray<arrow::DoubleType>, liborc::DoubleVectorBatch>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::BINARY:
      return FillBinaryBatch<BinaryArray>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::LARGE_BINARY:
      return FillBinaryBatch<LargeBinaryArray>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::STRING:
      return FillBinaryBatch<StringArray>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::LARGE_STRING:
      return FillBinaryBatch<LargeStringArray>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::FIXED_SIZE_BINARY:
      return FillFixedSizeBinaryBatch(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::DATE32:
      return FillNumericBatchCast<NumericArray<arrow::Date32Type>, liborc::LongVectorBatch, int64_t>(type, cbatch, arrowOffset, orcOffset, length, parray);
    // case Type::type::DECIMAL:
    //   return FillDecimalBatch(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::STRUCT:
      return FillStructBatch(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::LIST:
      return FillListBatch<ListArray, ListScalar>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::LARGE_LIST:
      return FillListBatch<LargeListArray, LargeListScalar>(type, cbatch, arrowOffset, orcOffset, length, parray);
    case Type::type::FIXED_SIZE_LIST:
      return FillFixedSizeListBatch(type, cbatch, arrowOffset, orcOffset, length, parray);
    default: {
      return Status::Invalid("Unknown or unsupported Arrow type kind: ", kind);
    }
  }
  return Status::OK();
}

Status FillBatch(const DataType* type, liborc::ColumnVectorBatch* cbatch, int64_t& arrowIndexOffset, int& arrowChunkOffset, int64_t length, ChunkedArray* pchunkedArray){
  int numBatch = pchunkedArray->num_chunks();
  int64_t orcOffset = 0;
  Status st;
  while (arrowChunkOffset < numBatch && orcOffset < length) {
    st = FillBatch(type, cbatch, arrowIndexOffset, orcOffset, length, pchunkedArray -> chunk(arrowChunkOffset).get());
    if (!st.ok()) {
      return st;
    }
    if (arrowChunkOffset < numBatch && orcOffset < length) {
      arrowIndexOffset = 0;
      arrowChunkOffset++;
    }
  }
  return Status::OK();
}


Status GetArrowType(const liborc::Type* type, std::shared_ptr<DataType>* out) {
  // When subselecting fields on read, liborc will set some nodes to nullptr,
  // so we need to check for nullptr before progressing
  if (type == nullptr) {
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

Status GetORCType(const DataType* type, ORC_UNIQUE_PTR<liborc::Type> out) {

  //Check for nullptr
  if (type == nullptr){
    out.reset();
    return Status::OK();
  }

  Type::type kind = type->id();
  //const int subtype_count = static_cast<int>(type->num_fields());

  switch (kind) {
    case Type::type::NA:
      out.reset(); //Makes out nullptr
      break;
    case Type::type::BOOL:
      out = liborc::createPrimitiveType(liborc::TypeKind::BOOLEAN);
      break;
    case Type::type::INT8:
      out = liborc::createPrimitiveType(liborc::TypeKind::BYTE);
      break;
    case Type::type::INT16:
      out = liborc::createPrimitiveType(liborc::TypeKind::SHORT);
      break;
    case Type::type::INT32:
      out = liborc::createPrimitiveType(liborc::TypeKind::INT);
      break;
    case Type::type::INT64:
      out = liborc::createPrimitiveType(liborc::TypeKind::LONG);
      break;
    case Type::type::FLOAT:
      out = liborc::createPrimitiveType(liborc::TypeKind::FLOAT);
      break;
    case Type::type::DOUBLE:
      out = liborc::createPrimitiveType(liborc::TypeKind::DOUBLE);
      break;
    //Use STRING instead of VARCHAR for now, both use UTF-8
    case Type::type::STRING:
    case Type::type::LARGE_STRING:
      out = liborc::createPrimitiveType(liborc::TypeKind::STRING);
      break;
    case Type::type::FIXED_SIZE_BINARY:
      out = liborc::createCharType(liborc::TypeKind::CHAR, internal::GetByteWidth(*type));
      break;
    case Type::type::BINARY:
    case Type::type::LARGE_BINARY:
      out = liborc::createPrimitiveType(liborc::TypeKind::BINARY);
      break;
    case Type::type::DATE32:
      out = liborc::createPrimitiveType(liborc::TypeKind::DATE);
      break;
    case Type::type::TIMESTAMP:
    case Type::type::TIME32:
    case Type::type::TIME64:
    case Type::type::DATE64:
      out = liborc::createPrimitiveType(liborc::TypeKind::TIMESTAMP);
      break;
    // case Type::type::UINT64: {
    //   out = std::move(liborc::createDecimalType(liborc::TypeKind::DECIMAL));
    //   break;
    // }
    case Type::type::DECIMAL: {
      const int precision = static_cast<int>(checked_cast<const DecimalType*>(type)->precision());
      const int scale = static_cast<int>(checked_cast<const DecimalType*>(type)->scale());
      if (precision == 0) {
        // In HIVE 0.11/0.12 precision is set as 0, but means max precision
        out = liborc::createDecimalType();
      } else {
        out = liborc::createDecimalType(precision, scale);
      }
      break;
    }
    case Type::type::LIST:
    case Type::type::FIXED_SIZE_LIST:
    case Type::type::LARGE_LIST: {
      DataType* arrowChildType = checked_cast<const BaseListType*>(type)->value_type().get();
      ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
      RETURN_NOT_OK(GetORCType(arrowChildType, std::move(orcSubtype)));
      out = liborc::createListType(std::move(orcSubtype));
      break;
    }
    case Type::type::STRUCT: {
      out = liborc::createStructType();
      std::vector<std::shared_ptr<Field>> arrowFields = checked_cast<const StructType*>(type)->fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin(); it != arrowFields.end(); ++it) {
        std::string fieldName = (*it)->name();
        DataType* arrowChildType = (*it)->type().get();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(arrowChildType, std::move(orcSubtype)));
        out->addStructField(fieldName, std::move(orcSubtype));
      }
      break;
    }
    case Type::type::MAP: {
      DataType* keyArrowType = checked_cast<const MapType*>(type)->key_type().get();
      DataType* valueArrowType = checked_cast<const MapType*>(type)->value_type().get();
      ORC_UNIQUE_PTR<liborc::Type> keyORCType;
      RETURN_NOT_OK(GetORCType(keyArrowType, std::move(keyORCType)));
      ORC_UNIQUE_PTR<liborc::Type> valueORCType;
      RETURN_NOT_OK(GetORCType(valueArrowType, std::move(valueORCType)));
      out = liborc::createMapType(std::move(keyORCType), std::move(valueORCType));
      break;
    }
    case Type::type::DENSE_UNION:
    case Type::type::SPARSE_UNION: {
      out = liborc::createUnionType();
      std::vector<std::shared_ptr<Field>> arrowFields = checked_cast<const UnionType*>(type)->fields();
      for (std::vector<std::shared_ptr<Field>>::iterator it = arrowFields.begin(); it != arrowFields.end(); ++it) {
        std::string fieldName = (*it)->name();
        DataType* arrowChildType = (*it)->type().get();
        ORC_UNIQUE_PTR<liborc::Type> orcSubtype;
        RETURN_NOT_OK(GetORCType(arrowChildType, std::move(orcSubtype)));
        out->addUnionChild(std::move(orcSubtype));
      }
      break;
    }
    //Dictionary is an encoding method, not a TypeKind in ORC. Hence we need to get the actual value type.
    case Type::type::DICTIONARY: {
      DataType* arrowValueType = checked_cast<const DictionaryType*>(type)->value_type().get();
      RETURN_NOT_OK(GetORCType(arrowValueType, std::move(out)));
    }
    default: {
      return Status::Invalid("Unknown or unsupported Arrow type kind: ", kind);
    }
  }
  return Status::OK();
}

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
