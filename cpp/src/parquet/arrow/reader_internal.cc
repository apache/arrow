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

#include "parquet/arrow/reader_internal.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"

#include "parquet/column_reader.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::ResizableBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;
using arrow::compute::Datum;

using ::arrow::BitUtil::FromBigEndian;
using ::arrow::internal::SafeLeftShift;
using ::arrow::util::SafeLoadAs;

using parquet::internal::RecordReader;

namespace parquet {
namespace arrow {

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

// ----------------------------------------------------------------------
// Primitive types

template <typename ArrowType, typename ParquetType>
Status TransferInt(RecordReader* reader, MemoryPool* pool,
                   const std::shared_ptr<DataType>& type, Datum* out) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;
  int64_t length = reader->values_written();
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(ArrowCType), &data));

  auto values = reinterpret_cast<const ParquetCType*>(reader->values());
  auto out_ptr = reinterpret_cast<ArrowCType*>(data->mutable_data());
  std::copy(values, values + length, out_ptr);
  *out = std::make_shared<ArrayType<ArrowType>>(
      type, length, data, reader->ReleaseIsValid(), reader->null_count());
  return Status::OK();
}

std::shared_ptr<Array> TransferZeroCopy(RecordReader* reader,
                                        const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Buffer>> buffers = {reader->ReleaseIsValid(),
                                                  reader->ReleaseValues()};
  auto data = std::make_shared<::arrow::ArrayData>(type, reader->values_written(),
                                                   buffers, reader->null_count());
  return ::arrow::MakeArray(data);
}

Status TransferBool(RecordReader* reader, MemoryPool* pool, Datum* out) {
  int64_t length = reader->values_written();
  std::shared_ptr<Buffer> data;

  const int64_t buffer_size = BitUtil::BytesForBits(length);
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, buffer_size, &data));

  // Transfer boolean values to packed bitmap
  auto values = reinterpret_cast<const bool*>(reader->values());
  uint8_t* data_ptr = data->mutable_data();
  memset(data_ptr, 0, buffer_size);

  for (int64_t i = 0; i < length; i++) {
    if (values[i]) {
      ::arrow::BitUtil::SetBit(data_ptr, i);
    }
  }

  *out = std::make_shared<BooleanArray>(length, data, reader->ReleaseIsValid(),
                                        reader->null_count());
  return Status::OK();
}

Status TransferInt96(RecordReader* reader, MemoryPool* pool,
                     const std::shared_ptr<DataType>& type, Datum* out) {
  int64_t length = reader->values_written();
  auto values = reinterpret_cast<const Int96*>(reader->values());
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));
  auto data_ptr = reinterpret_cast<int64_t*>(data->mutable_data());
  for (int64_t i = 0; i < length; i++) {
    *data_ptr++ = Int96GetNanoSeconds(values[i]);
  }
  *out = std::make_shared<TimestampArray>(type, length, data, reader->ReleaseIsValid(),
                                          reader->null_count());
  return Status::OK();
}

Status TransferDate64(RecordReader* reader, MemoryPool* pool,
                      const std::shared_ptr<DataType>& type, Datum* out) {
  int64_t length = reader->values_written();
  auto values = reinterpret_cast<const int32_t*>(reader->values());

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * sizeof(int64_t), &data));
  auto out_ptr = reinterpret_cast<int64_t*>(data->mutable_data());

  for (int64_t i = 0; i < length; i++) {
    *out_ptr++ = static_cast<int64_t>(values[i]) * kMillisecondsPerDay;
  }

  *out = std::make_shared<::arrow::Date64Array>(
      type, length, data, reader->ReleaseIsValid(), reader->null_count());
  return Status::OK();
}

// ----------------------------------------------------------------------
// Binary, direct to dictionary-encoded

// Some ugly hacks here for now to handle binary dictionaries casted to their
// logical type
std::shared_ptr<Array> ShallowCast(const Array& arr,
                                   const std::shared_ptr<DataType>& new_type) {
  std::shared_ptr<::arrow::ArrayData> new_data = arr.data()->Copy();
  new_data->type = new_type;
  if (new_type->id() == ::arrow::Type::DICTIONARY) {
    // Cast dictionary, too
    const auto& dict_type = static_cast<const ::arrow::DictionaryType&>(*new_type);
    new_data->dictionary = ShallowCast(*new_data->dictionary, dict_type.value_type());
  }
  return MakeArray(new_data);
}

std::shared_ptr<ChunkedArray> CastChunksTo(
    const ChunkedArray& data, const std::shared_ptr<DataType>& logical_value_type) {
  std::vector<std::shared_ptr<Array>> string_chunks;
  for (int i = 0; i < data.num_chunks(); ++i) {
    string_chunks.push_back(ShallowCast(*data.chunk(i), logical_value_type));
  }
  return std::make_shared<ChunkedArray>(string_chunks);
}

Status TransferDictionary(RecordReader* reader,
                          const std::shared_ptr<DataType>& logical_value_type,
                          std::shared_ptr<ChunkedArray>* out) {
  auto dict_reader = dynamic_cast<internal::DictionaryRecordReader*>(reader);
  DCHECK(dict_reader);
  *out = dict_reader->GetResult();

  const auto& dict_type = static_cast<const ::arrow::DictionaryType&>(*(*out)->type());
  if (!logical_value_type->Equals(*dict_type.value_type())) {
    *out = CastChunksTo(**out,
                        ::arrow::dictionary(dict_type.index_type(), logical_value_type));
  }
  return Status::OK();
}

Status TransferBinary(RecordReader* reader,
                      const std::shared_ptr<DataType>& logical_value_type,
                      std::shared_ptr<ChunkedArray>* out) {
  if (reader->read_dictionary()) {
    return TransferDictionary(reader, logical_value_type, out);
  }
  auto binary_reader = dynamic_cast<internal::BinaryRecordReader*>(reader);
  DCHECK(binary_reader);
  *out = std::make_shared<ChunkedArray>(binary_reader->GetBuilderChunks());
  if (!logical_value_type->Equals(*(*out)->type())) {
    *out = CastChunksTo(**out, logical_value_type);
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// INT32 / INT64 / BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY -> Decimal128

static uint64_t BytesToInteger(const uint8_t* bytes, int32_t start, int32_t stop) {
  const int32_t length = stop - start;

  DCHECK_GE(length, 0);
  DCHECK_LE(length, 8);

  switch (length) {
    case 0:
      return 0;
    case 1:
      return bytes[start];
    case 2:
      return FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
    case 3: {
      const uint64_t first_two_bytes = FromBigEndian(SafeLoadAs<uint16_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_two_bytes << 8 | last_byte;
    }
    case 4:
      return FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
    case 5: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 8 | last_byte;
    }
    case 6: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t last_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      return first_four_bytes << 16 | last_two_bytes;
    }
    case 7: {
      const uint64_t first_four_bytes =
          FromBigEndian(SafeLoadAs<uint32_t>(bytes + start));
      const uint64_t second_two_bytes =
          FromBigEndian(SafeLoadAs<uint16_t>(bytes + start + 4));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 24 | second_two_bytes << 8 | last_byte;
    }
    case 8:
      return FromBigEndian(SafeLoadAs<uint64_t>(bytes + start));
    default: {
      DCHECK(false);
      return UINT64_MAX;
    }
  }
}

static constexpr int32_t kMinDecimalBytes = 1;
static constexpr int32_t kMaxDecimalBytes = 16;

/// \brief Convert a sequence of big-endian bytes to one int64_t (high bits) and one
/// uint64_t (low bits).
static void BytesToIntegerPair(const uint8_t* bytes, const int32_t length,
                               int64_t* out_high, uint64_t* out_low) {
  DCHECK_GE(length, kMinDecimalBytes);
  DCHECK_LE(length, kMaxDecimalBytes);

  // XXX This code is copied from Decimal::FromBigEndian

  int64_t high, low;

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = BytesToInteger(bytes, 0, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits = BytesToInteger(bytes, high_bits_offset, length);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  *out_high = high;
  *out_low = static_cast<uint64_t>(low);
}

static inline void RawBytesToDecimalBytes(const uint8_t* value, int32_t byte_width,
                                          uint8_t* out_buf) {
  // view the first 8 bytes as an unsigned 64-bit integer
  auto low = reinterpret_cast<uint64_t*>(out_buf);

  // view the second 8 bytes as a signed 64-bit integer
  auto high = reinterpret_cast<int64_t*>(out_buf + sizeof(uint64_t));

  // Convert the fixed size binary array bytes into a Decimal128 compatible layout
  BytesToIntegerPair(value, byte_width, high, low);
}

template <typename T>
Status ConvertToDecimal128(const Array& array, const std::shared_ptr<DataType>&,
                           MemoryPool* pool, std::shared_ptr<Array>*) {
  return Status::NotImplemented("not implemented");
}

template <>
Status ConvertToDecimal128<FLBAType>(const Array& array,
                                     const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& fixed_size_binary_array =
      static_cast<const ::arrow::FixedSizeBinaryArray&>(array);

  // The byte width of each decimal value
  const int32_t type_length =
      static_cast<const ::arrow::Decimal128Type&>(*type).byte_width();

  // number of elements in the entire array
  const int64_t length = fixed_size_binary_array.length();

  // Get the byte width of the values in the FixedSizeBinaryArray. Most of the time
  // this will be different from the decimal array width because we write the minimum
  // number of bytes necessary to represent a given precision
  const int32_t byte_width =
      static_cast<const ::arrow::FixedSizeBinaryType&>(*fixed_size_binary_array.type())
          .byte_width();

  // allocate memory for the decimal array
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  // convert each FixedSizeBinary value to valid decimal bytes
  const int64_t null_count = fixed_size_binary_array.null_count();
  if (null_count > 0) {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      if (!fixed_size_binary_array.IsNull(i)) {
        RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
      RawBytesToDecimalBytes(fixed_size_binary_array.GetValue(i), byte_width, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, fixed_size_binary_array.null_bitmap(), null_count);

  return Status::OK();
}

template <>
Status ConvertToDecimal128<ByteArrayType>(const Array& array,
                                          const std::shared_ptr<DataType>& type,
                                          MemoryPool* pool, std::shared_ptr<Array>* out) {
  const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(array);
  const int64_t length = binary_array.length();

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));

  // raw bytes that we can write to
  uint8_t* out_ptr = data->mutable_data();

  const int64_t null_count = binary_array.null_count();

  // convert each BinaryArray value to valid decimal bytes
  for (int64_t i = 0; i < length; i++, out_ptr += type_length) {
    int32_t record_len = 0;
    const uint8_t* record_loc = binary_array.GetValue(i, &record_len);

    if ((record_len < 0) || (record_len > type_length)) {
      return Status::Invalid("Invalid BYTE_ARRAY size");
    }

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);
    out_ptr_view[0] = 0;
    out_ptr_view[1] = 0;

    // only convert rows that are not null if there are nulls, or
    // all rows, if there are not
    if (((null_count > 0) && !binary_array.IsNull(i)) || (null_count <= 0)) {
      RawBytesToDecimalBytes(record_loc, record_len, out_ptr);
    }
  }

  *out = std::make_shared<::arrow::Decimal128Array>(
      type, length, data, binary_array.null_bitmap(), null_count);
  return Status::OK();
}

/// \brief Convert an Int32 or Int64 array into a Decimal128Array
/// The parquet spec allows systems to write decimals in int32, int64 if the values are
/// small enough to fit in less 4 bytes or less than 8 bytes, respectively.
/// This function implements the conversion from int32 and int64 arrays to decimal arrays.
template <typename ParquetIntegerType,
          typename = typename std::enable_if<
              std::is_same<ParquetIntegerType, Int32Type>::value ||
              std::is_same<ParquetIntegerType, Int64Type>::value>::type>
static Status DecimalIntegerTransfer(RecordReader* reader, MemoryPool* pool,
                                     const std::shared_ptr<DataType>& type, Datum* out) {
  DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

  const int64_t length = reader->values_written();

  using ElementType = typename ParquetIntegerType::c_type;
  static_assert(std::is_same<ElementType, int32_t>::value ||
                    std::is_same<ElementType, int64_t>::value,
                "ElementType must be int32_t or int64_t");

  const auto values = reinterpret_cast<const ElementType*>(reader->values());

  const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
  const int64_t type_length = decimal_type.byte_width();

  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(::arrow::AllocateBuffer(pool, length * type_length, &data));
  uint8_t* out_ptr = data->mutable_data();

  using ::arrow::BitUtil::FromLittleEndian;

  for (int64_t i = 0; i < length; ++i, out_ptr += type_length) {
    // sign/zero extend int32_t values, otherwise a no-op
    const auto value = static_cast<int64_t>(values[i]);

    auto out_ptr_view = reinterpret_cast<uint64_t*>(out_ptr);

    // No-op on little endian machines, byteswap on big endian
    out_ptr_view[0] = FromLittleEndian(static_cast<uint64_t>(value));

    // no need to byteswap here because we're sign/zero extending exactly 8 bytes
    out_ptr_view[1] = static_cast<uint64_t>(value < 0 ? -1 : 0);
  }

  if (reader->nullable_values()) {
    std::shared_ptr<ResizableBuffer> is_valid = reader->ReleaseIsValid();
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data, is_valid,
                                                      reader->null_count());
  } else {
    *out = std::make_shared<::arrow::Decimal128Array>(type, length, data);
  }
  return Status::OK();
}

/// \brief Convert an arrow::BinaryArray to an arrow::Decimal128Array
/// We do this by:
/// 1. Creating an arrow::BinaryArray from the RecordReader's builder
/// 2. Allocating a buffer for the arrow::Decimal128Array
/// 3. Converting the big-endian bytes in each BinaryArray entry to two integers
///    representing the high and low bits of each decimal value.
template <typename ParquetType>
Status TransferDecimal(RecordReader* reader, MemoryPool* pool,
                       const std::shared_ptr<DataType>& type, Datum* out) {
  DCHECK_EQ(type->id(), ::arrow::Type::DECIMAL);

  auto binary_reader = dynamic_cast<internal::BinaryRecordReader*>(reader);
  DCHECK(binary_reader);
  ::arrow::ArrayVector chunks = binary_reader->GetBuilderChunks();
  for (size_t i = 0; i < chunks.size(); ++i) {
    std::shared_ptr<Array> chunk_as_decimal;
    RETURN_NOT_OK(
        ConvertToDecimal128<ParquetType>(*chunks[i], type, pool, &chunk_as_decimal));
    // Replace the chunk, which will hopefully also free memory as we go
    chunks[i] = chunk_as_decimal;
  }
  *out = std::make_shared<ChunkedArray>(chunks);
  return Status::OK();
}

#define TRANSFER_INT32(ENUM, ArrowType)                                              \
  case ::arrow::Type::ENUM: {                                                        \
    Status s = TransferInt<ArrowType, Int32Type>(reader, pool, value_type, &result); \
    RETURN_NOT_OK(s);                                                                \
  } break;

#define TRANSFER_INT64(ENUM, ArrowType)                                              \
  case ::arrow::Type::ENUM: {                                                        \
    Status s = TransferInt<ArrowType, Int64Type>(reader, pool, value_type, &result); \
    RETURN_NOT_OK(s);                                                                \
  } break;

Status TransferColumnData(internal::RecordReader* reader,
                          std::shared_ptr<DataType> value_type,
                          const ColumnDescriptor* descr, MemoryPool* pool,
                          std::shared_ptr<ChunkedArray>* out) {
  Datum result;
  switch (value_type->id()) {
    case ::arrow::Type::NA: {
      result = std::make_shared<::arrow::NullArray>(reader->values_written());
      break;
    }
    case ::arrow::Type::INT32:
    case ::arrow::Type::INT64:
    case ::arrow::Type::FLOAT:
    case ::arrow::Type::DOUBLE:
      result = TransferZeroCopy(reader, value_type);
      break;
    case ::arrow::Type::BOOL:
      RETURN_NOT_OK(TransferBool(reader, pool, &result));
      break;
      TRANSFER_INT32(UINT8, ::arrow::UInt8Type);
      TRANSFER_INT32(INT8, ::arrow::Int8Type);
      TRANSFER_INT32(UINT16, ::arrow::UInt16Type);
      TRANSFER_INT32(INT16, ::arrow::Int16Type);
      TRANSFER_INT32(UINT32, ::arrow::UInt32Type);
      TRANSFER_INT64(UINT64, ::arrow::UInt64Type);
      TRANSFER_INT32(DATE32, ::arrow::Date32Type);
      TRANSFER_INT32(TIME32, ::arrow::Time32Type);
      TRANSFER_INT64(TIME64, ::arrow::Time64Type);
    case ::arrow::Type::DATE64:
      RETURN_NOT_OK(TransferDate64(reader, pool, value_type, &result));
      break;
    case ::arrow::Type::FIXED_SIZE_BINARY:
    case ::arrow::Type::BINARY:
    case ::arrow::Type::STRING: {
      std::shared_ptr<ChunkedArray> out;
      RETURN_NOT_OK(TransferBinary(reader, value_type, &out));
      result = out;
    } break;
    case ::arrow::Type::DECIMAL: {
      switch (descr->physical_type()) {
        case ::parquet::Type::INT32: {
          RETURN_NOT_OK(
              DecimalIntegerTransfer<Int32Type>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::INT64: {
          RETURN_NOT_OK(
              DecimalIntegerTransfer<Int64Type>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::BYTE_ARRAY: {
          RETURN_NOT_OK(
              TransferDecimal<ByteArrayType>(reader, pool, value_type, &result));
        } break;
        case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
          RETURN_NOT_OK(TransferDecimal<FLBAType>(reader, pool, value_type, &result));
        } break;
        default:
          return Status::Invalid(
              "Physical type for decimal must be int32, int64, byte array, or fixed "
              "length binary");
      }
    } break;
    case ::arrow::Type::TIMESTAMP: {
      const ::arrow::TimestampType& timestamp_type =
          static_cast<::arrow::TimestampType&>(*value_type);
      switch (timestamp_type.unit()) {
        case ::arrow::TimeUnit::MILLI:
        case ::arrow::TimeUnit::MICRO: {
          result = TransferZeroCopy(reader, value_type);
        } break;
        case ::arrow::TimeUnit::NANO: {
          if (descr->physical_type() == ::parquet::Type::INT96) {
            RETURN_NOT_OK(TransferInt96(reader, pool, value_type, &result));
          } else {
            result = TransferZeroCopy(reader, value_type);
          }
        } break;
        default:
          return Status::NotImplemented("TimeUnit not supported");
      }
    } break;
    default:
      return Status::NotImplemented("No support for reading columns of type ",
                                    value_type->ToString());
  }

  DCHECK_NE(result.kind(), Datum::NONE);

  if (result.kind() == Datum::ARRAY) {
    *out = std::make_shared<ChunkedArray>(result.make_array());
  } else if (result.kind() == Datum::CHUNKED_ARRAY) {
    *out = result.chunked_array();
  } else {
    DCHECK(false) << "Should be impossible";
  }

  return Status::OK();
}

}  // namespace arrow
}  // namespace parquet
