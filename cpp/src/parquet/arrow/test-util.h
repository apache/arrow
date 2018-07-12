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

#include <limits>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/test-util.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

namespace parquet {
namespace arrow {

using ::arrow::Array;
using ::arrow::Status;

template <int32_t PRECISION>
struct DecimalWithPrecisionAndScale {
  static_assert(PRECISION >= 1 && PRECISION <= 38, "Invalid precision value");

  using type = ::arrow::Decimal128Type;
  static constexpr ::arrow::Type::type type_id = ::arrow::Decimal128Type::type_id;
  static constexpr int32_t precision = PRECISION;
  static constexpr int32_t scale = PRECISION - 1;
};

template <typename ArrowType>
using is_arrow_float = std::is_floating_point<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_int = std::is_integral<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_date = std::is_same<ArrowType, ::arrow::Date64Type>;

template <typename ArrowType>
using is_arrow_string = std::is_same<ArrowType, ::arrow::StringType>;

template <typename ArrowType>
using is_arrow_binary = std::is_same<ArrowType, ::arrow::BinaryType>;

template <typename ArrowType>
using is_arrow_fixed_size_binary = std::is_same<ArrowType, ::arrow::FixedSizeBinaryType>;

template <typename ArrowType>
using is_arrow_bool = std::is_same<ArrowType, ::arrow::BooleanType>;

template <class ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  using c_type = typename ArrowType::c_type;
  std::vector<c_type> values;
  ::arrow::test::random_real(size, 0, static_cast<c_type>(0), static_cast<c_type>(1),
                             &values);
  ::arrow::NumericBuilder<ArrowType> builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<
    is_arrow_int<ArrowType>::value && !is_arrow_date<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint(size, 0, 64, &values);

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_date<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint(size, 0, 64, &values);
  for (size_t i = 0; i < size; i++) {
    values[i] *= 86400000;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  builder.Append(values.data(), values.size());
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;
  for (size_t i = 0; i < size; i++) {
    RETURN_NOT_OK(builder.Append("test-string"));
  }
  return builder.Finish(out);
}

template <typename ArrowType>
typename std::enable_if<is_arrow_fixed_size_binary<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  // set byte_width to the length of "fixed": 5
  // todo: find a way to generate test data with more diversity.
  BuilderType builder(::arrow::fixed_size_binary(5));
  for (size_t i = 0; i < size; i++) {
    RETURN_NOT_OK(builder.Append("fixed"));
  }
  return builder.Finish(out);
}

static inline void random_decimals(int64_t n, uint32_t seed, int32_t precision,
                                   uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  const int32_t required_bytes = DecimalSize(precision);
  constexpr int32_t byte_width = 16;
  std::fill(out, out + byte_width * n, '\0');

  for (int64_t i = 0; i < n; ++i, out += byte_width) {
    std::generate(out, out + required_bytes,
                  [&d, &gen] { return static_cast<uint8_t>(d(gen)); });

    // sign extend if the sign bit is set for the last byte generated
    // 0b10000000 == 0x80 == 128
    if ((out[required_bytes - 1] & '\x80') != 0) {
      std::fill(out + required_bytes, out + byte_width, '\xFF');
    }
  }
}

template <typename ArrowType, int32_t precision = ArrowType::precision>
typename std::enable_if<
    std::is_same<ArrowType, DecimalWithPrecisionAndScale<precision>>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  constexpr int32_t kDecimalPrecision = precision;
  constexpr int32_t kDecimalScale = DecimalWithPrecisionAndScale<precision>::scale;

  const auto type = ::arrow::decimal(kDecimalPrecision, kDecimalScale);
  ::arrow::Decimal128Builder builder(type);
  const int32_t byte_width =
      static_cast<const ::arrow::Decimal128Type&>(*type).byte_width();

  constexpr int32_t seed = 0;

  std::shared_ptr<Buffer> out_buf;
  RETURN_NOT_OK(::arrow::AllocateBuffer(::arrow::default_memory_pool(), size * byte_width,
                                        &out_buf));
  random_decimals(size, seed, kDecimalPrecision, out_buf->mutable_data());

  RETURN_NOT_OK(builder.Append(out_buf->data(), size));
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;
  ::arrow::test::randint(size, 0, 1, &values);
  ::arrow::BooleanBuilder builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  using c_type = typename ArrowType::c_type;
  std::vector<c_type> values;
  ::arrow::test::random_real(size, seed, static_cast<c_type>(-1e10),
                             static_cast<c_type>(1e10), &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::NumericBuilder<ArrowType> builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<
    is_arrow_int<ArrowType>::value && !is_arrow_date<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;

  // Seed is random in Arrow right now
  (void)seed;
  ::arrow::test::randint(size, 0, 64, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

template <typename ArrowType>
typename std::enable_if<is_arrow_date<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;

  // Seed is random in Arrow right now
  (void)seed;
  ::arrow::test::randint(size, 0, 64, &values);
  for (size_t i = 0; i < size; i++) {
    values[i] *= 86400000;
  }
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <typename ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed,
              std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;

  const int kBufferSize = 10;
  uint8_t buffer[kBufferSize];
  for (size_t i = 0; i < size; i++) {
    if (!valid_bytes[i]) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      ::arrow::test::random_bytes(kBufferSize, seed + static_cast<uint32_t>(i), buffer);
      RETURN_NOT_OK(builder.Append(buffer, kBufferSize));
    }
  }
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet,
// same as NullableArray<String|Binary>(..)
template <typename ArrowType>
typename std::enable_if<is_arrow_fixed_size_binary<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed,
              std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  const int byte_width = 10;
  BuilderType builder(::arrow::fixed_size_binary(byte_width));

  const int kBufferSize = byte_width;
  uint8_t buffer[kBufferSize];
  for (size_t i = 0; i < size; i++) {
    if (!valid_bytes[i]) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      ::arrow::test::random_bytes(kBufferSize, seed + static_cast<uint32_t>(i), buffer);
      RETURN_NOT_OK(builder.Append(buffer));
    }
  }
  return builder.Finish(out);
}

template <typename ArrowType, int32_t precision = ArrowType::precision>
typename std::enable_if<
    std::is_same<ArrowType, DecimalWithPrecisionAndScale<precision>>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed,
              std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, '\1');

  for (size_t i = 0; i < num_nulls; ++i) {
    valid_bytes[i * 2] = '\0';
  }

  constexpr int32_t kDecimalPrecision = precision;
  constexpr int32_t kDecimalScale = DecimalWithPrecisionAndScale<precision>::scale;
  const auto type = ::arrow::decimal(kDecimalPrecision, kDecimalScale);
  const int32_t byte_width =
      static_cast<const ::arrow::Decimal128Type&>(*type).byte_width();

  std::shared_ptr<::arrow::Buffer> out_buf;
  RETURN_NOT_OK(::arrow::AllocateBuffer(::arrow::default_memory_pool(), size * byte_width,
                                        &out_buf));

  random_decimals(size, seed, precision, out_buf->mutable_data());

  ::arrow::Decimal128Builder builder(type);
  RETURN_NOT_OK(builder.Append(out_buf->data(), size, valid_bytes.data()));
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;

  // Seed is random in Arrow right now
  (void)seed;

  ::arrow::test::randint(size, 0, 1, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::BooleanBuilder builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

/// Wrap an Array into a ListArray by splitting it up into size lists.
///
/// This helper function only supports (size/2) nulls.
Status MakeListArray(const std::shared_ptr<Array>& values, int64_t size,
                     int64_t null_count, bool nullable_values,
                     std::shared_ptr<::arrow::ListArray>* out) {
  // We always include an empty list
  int64_t non_null_entries = size - null_count - 1;
  int64_t length_per_entry = values->length() / non_null_entries;

  auto offsets = std::make_shared<::arrow::PoolBuffer>(::arrow::default_memory_pool());
  RETURN_NOT_OK(offsets->Resize((size + 1) * sizeof(int32_t)));
  int32_t* offsets_ptr = reinterpret_cast<int32_t*>(offsets->mutable_data());

  auto null_bitmap =
      std::make_shared<::arrow::PoolBuffer>(::arrow::default_memory_pool());
  int64_t bitmap_size = ::arrow::BitUtil::CeilByte(size) / 8;
  RETURN_NOT_OK(null_bitmap->Resize(bitmap_size));
  uint8_t* null_bitmap_ptr = null_bitmap->mutable_data();
  memset(null_bitmap_ptr, 0, bitmap_size);

  int32_t current_offset = 0;
  for (int64_t i = 0; i < size; i++) {
    offsets_ptr[i] = current_offset;
    if (!(((i % 2) == 0) && ((i / 2) < null_count))) {
      // Non-null list (list with index 1 is always empty).
      ::arrow::BitUtil::SetBit(null_bitmap_ptr, i);
      if (i != 1) {
        current_offset += static_cast<int32_t>(length_per_entry);
      }
    }
  }
  offsets_ptr[size] = static_cast<int32_t>(values->length());

  auto value_field = ::arrow::field("item", values->type(), nullable_values);
  *out = std::make_shared<::arrow::ListArray>(::arrow::list(value_field), size, offsets,
                                              values, null_bitmap, null_count);

  return Status::OK();
}

// Make an array containing only empty lists, with a null values array
Status MakeEmptyListsArray(int64_t size, std::shared_ptr<Array>* out_array) {
  // Allocate an offsets buffer containing only zeroes
  std::shared_ptr<Buffer> offsets_buffer;
  const int64_t offsets_nbytes = (size + 1) * sizeof(int32_t);
  RETURN_NOT_OK(::arrow::AllocateBuffer(::arrow::default_memory_pool(), offsets_nbytes,
                                        &offsets_buffer));
  memset(offsets_buffer->mutable_data(), 0, offsets_nbytes);

  auto value_field = ::arrow::field("item", ::arrow::float64(),
                                    false /* nullable_values */);
  auto list_type = ::arrow::list(value_field);

  std::vector<std::shared_ptr<Buffer>> child_buffers = {nullptr /* null bitmap */,
                                                        nullptr /* values */ };
  auto child_data = ::arrow::ArrayData::Make(value_field->type(), 0,
                                             std::move(child_buffers));

  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr /* bitmap */,
                                                  offsets_buffer };
  auto array_data = ::arrow::ArrayData::Make(list_type, size, std::move(buffers));
  array_data->child_data.push_back(child_data);

  *out_array = ::arrow::MakeArray(array_data);
  return Status::OK();
}

static std::shared_ptr<::arrow::Column> MakeColumn(const std::string& name,
                                                   const std::shared_ptr<Array>& array,
                                                   bool nullable) {
  auto field = ::arrow::field(name, array->type(), nullable);
  return std::make_shared<::arrow::Column>(field, array);
}

static std::shared_ptr<::arrow::Column> MakeColumn(
    const std::string& name, const std::vector<std::shared_ptr<Array>>& arrays,
    bool nullable) {
  auto field = ::arrow::field(name, arrays[0]->type(), nullable);
  return std::make_shared<::arrow::Column>(field, arrays);
}

std::shared_ptr<::arrow::Table> MakeSimpleTable(const std::shared_ptr<Array>& values,
                                                bool nullable) {
  std::shared_ptr<::arrow::Column> column = MakeColumn("col", values, nullable);
  std::vector<std::shared_ptr<::arrow::Column>> columns({column});
  std::vector<std::shared_ptr<::arrow::Field>> fields({column->field()});
  auto schema = std::make_shared<::arrow::Schema>(fields);
  return ::arrow::Table::Make(schema, columns);
}

template <typename T>
void ExpectArray(T* expected, Array* result) {
  auto p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int i = 0; i < result->length(); i++) {
    EXPECT_EQ(expected[i], reinterpret_cast<const T*>(p_array->values()->data())[i]);
  }
}

template <typename ArrowType>
void ExpectArrayT(void* expected, Array* result) {
  ::arrow::PrimitiveArray* p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int64_t i = 0; i < result->length(); i++) {
    EXPECT_EQ(reinterpret_cast<typename ArrowType::c_type*>(expected)[i],
              reinterpret_cast<const typename ArrowType::c_type*>(
                  p_array->values()->data())[i]);
  }
}

template <>
void ExpectArrayT<::arrow::BooleanType>(void* expected, Array* result) {
  ::arrow::BooleanBuilder builder;
  EXPECT_OK(builder.Append(reinterpret_cast<uint8_t*>(expected), result->length()));

  std::shared_ptr<Array> expected_array;
  EXPECT_OK(builder.Finish(&expected_array));
  EXPECT_TRUE(result->Equals(*expected_array));
}

}  // namespace arrow

}  // namespace parquet
