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

#include "arrow/testing/random.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/pcg_random.h"
#include "arrow/util/string.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::ToChars;

namespace random {

namespace {

template <typename ValueType, typename DistributionType>
struct GenerateOptions {
  GenerateOptions(SeedType seed, ValueType min, ValueType max, double probability,
                  double nan_probability = 0.0)
      : min_(min),
        max_(max),
        seed_(seed),
        probability_(probability),
        nan_probability_(nan_probability) {}

  void GenerateData(uint8_t* buffer, size_t n) {
    GenerateTypedData(reinterpret_cast<ValueType*>(buffer), n);
  }

  template <typename V>
  typename std::enable_if<!std::is_floating_point<V>::value>::type GenerateTypedData(
      V* data, size_t n) {
    GenerateTypedDataNoNan(data, n);
  }

  template <typename V>
  typename std::enable_if<std::is_floating_point<V>::value>::type GenerateTypedData(
      V* data, size_t n) {
    if (nan_probability_ == 0.0) {
      GenerateTypedDataNoNan(data, n);
      return;
    }
    pcg32_fast rng(seed_++);
    DistributionType dist(min_, max_);
    ::arrow::random::bernoulli_distribution nan_dist(nan_probability_);
    const ValueType nan_value = std::numeric_limits<ValueType>::quiet_NaN();

    // A static cast is required due to the int16 -> int8 handling.
    std::generate(data, data + n, [&] {
      return nan_dist(rng) ? nan_value : static_cast<ValueType>(dist(rng));
    });
  }

  void GenerateTypedDataNoNan(ValueType* data, size_t n) {
    pcg32_fast rng(seed_++);
    DistributionType dist(min_, max_);

    // A static cast is required due to the int16 -> int8 handling.
    std::generate(data, data + n, [&] { return static_cast<ValueType>(dist(rng)); });
  }

  void GenerateBitmap(uint8_t* buffer, size_t n, int64_t* null_count) {
    int64_t count = 0;
    pcg32_fast rng(seed_++);
    ::arrow::random::bernoulli_distribution dist(1.0 - probability_);

    for (size_t i = 0; i < n; i++) {
      if (dist(rng)) {
        bit_util::SetBit(buffer, i);
      } else {
        count++;
      }
    }

    if (null_count != nullptr) *null_count = count;
  }

  ValueType min_;
  ValueType max_;
  SeedType seed_;
  double probability_;
  double nan_probability_;
};

}  // namespace

std::shared_ptr<Buffer> RandomArrayGenerator::NullBitmap(int64_t size,
                                                         double null_probability,
                                                         int64_t alignment,
                                                         MemoryPool* memory_pool) {
  // The bitmap generator does not care about the value distribution since it
  // only calls the GenerateBitmap method.
  using GenOpt = GenerateOptions<int, std::uniform_int_distribution<int>>;

  GenOpt null_gen(seed(), 0, 1, null_probability);
  std::shared_ptr<Buffer> bitmap = *AllocateEmptyBitmap(size, alignment, memory_pool);
  null_gen.GenerateBitmap(bitmap->mutable_data(), size, nullptr);

  return bitmap;
}

std::shared_ptr<Array> RandomArrayGenerator::Boolean(int64_t size,
                                                     double true_probability,
                                                     double null_probability,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  // The boolean generator does not care about the value distribution since it
  // only calls the GenerateBitmap method.
  using GenOpt = GenerateOptions<int, std::uniform_int_distribution<int>>;

  BufferVector buffers{2};
  // Need 2 distinct generators such that probabilities are not shared.

  // The "GenerateBitmap" function is written to generate validity bitmaps
  // parameterized by the null probability, which is the probability of 0. For
  // boolean data, the true probability is the probability of 1, so to use
  // GenerateBitmap we must provide the probability of false instead.
  GenOpt value_gen(seed(), 0, 1, 1 - true_probability);

  GenOpt null_gen(seed(), 0, 1, null_probability);

  int64_t null_count = 0;
  buffers[0] = *AllocateEmptyBitmap(size, alignment, memory_pool);
  null_gen.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  buffers[1] = *AllocateEmptyBitmap(size, alignment, memory_pool);
  value_gen.GenerateBitmap(buffers[1]->mutable_data(), size, nullptr);

  auto array_data = ArrayData::Make(arrow::boolean(), size, buffers, null_count);
  return std::make_shared<BooleanArray>(array_data);
}

void GenerateFullDayMillisNoNan(uint8_t* buffer, size_t n) {
  int64_t* data = reinterpret_cast<int64_t*>(buffer);
  constexpr int64_t kFullDayMillis = 1000 * 60 * 60 * 24;
  std::for_each(data, data + n, [&](int64_t& v) { return v *= kFullDayMillis; });
}

template <typename ArrowType, typename OptionType>
static std::shared_ptr<NumericArray<ArrowType>> GenerateNumericArray(
    int64_t size, OptionType options, int64_t alignment, MemoryPool* memory_pool) {
  using CType = typename ArrowType::c_type;
  auto type = TypeTraits<ArrowType>::type_singleton();
  BufferVector buffers{2};

  int64_t null_count = 0;
  buffers[0] = *AllocateEmptyBitmap(size, alignment, memory_pool);
  options.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  buffers[1] = *AllocateBuffer(sizeof(CType) * size, alignment, memory_pool);
  options.GenerateData(buffers[1]->mutable_data(), size);
  if (std::is_same<ArrowType, Date64Type>::value) {
    GenerateFullDayMillisNoNan(buffers[1]->mutable_data(), size);
  }

  auto array_data = ArrayData::Make(type, size, buffers, null_count);
  return std::make_shared<NumericArray<ArrowType>>(array_data);
}

#define PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, Distribution)                \
  std::shared_ptr<Array> RandomArrayGenerator::Name(                             \
      int64_t size, CType min, CType max, double probability, int64_t alignment, \
      MemoryPool* memory_pool) {                                                 \
    using OptionType = GenerateOptions<CType, Distribution>;                     \
    OptionType options(seed(), min, max, probability);                           \
    return GenerateNumericArray<ArrowType, OptionType>(size, options, alignment, \
                                                       memory_pool);             \
  }

#define PRIMITIVE_RAND_INTEGER_IMPL(Name, CType, ArrowType) \
  PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, std::uniform_int_distribution<CType>)

// Visual Studio does not implement uniform_int_distribution for char types.
PRIMITIVE_RAND_IMPL(UInt8, uint8_t, UInt8Type, std::uniform_int_distribution<uint16_t>)
PRIMITIVE_RAND_IMPL(Int8, int8_t, Int8Type, std::uniform_int_distribution<int16_t>)

PRIMITIVE_RAND_INTEGER_IMPL(UInt16, uint16_t, UInt16Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int16, int16_t, Int16Type)
PRIMITIVE_RAND_INTEGER_IMPL(UInt32, uint32_t, UInt32Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int32, int32_t, Int32Type)
PRIMITIVE_RAND_INTEGER_IMPL(UInt64, uint64_t, UInt64Type)
PRIMITIVE_RAND_INTEGER_IMPL(Int64, int64_t, Int64Type)
// Generate 16bit values for half-float
PRIMITIVE_RAND_INTEGER_IMPL(Float16, int16_t, HalfFloatType)

std::shared_ptr<Array> RandomArrayGenerator::Date64(int64_t size, int64_t min,
                                                    int64_t max, double null_probability,
                                                    int64_t alignment,
                                                    MemoryPool* memory_pool) {
  using OptionType = GenerateOptions<int64_t, std::uniform_int_distribution<int64_t>>;
  OptionType options(seed(), min, max, null_probability);
  return GenerateNumericArray<Date64Type, OptionType>(size, options, alignment,
                                                      memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::Float32(int64_t size, float min, float max,
                                                     double null_probability,
                                                     double nan_probability,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  using OptionType =
      GenerateOptions<float, ::arrow::random::uniform_real_distribution<float>>;
  OptionType options(seed(), min, max, null_probability, nan_probability);
  return GenerateNumericArray<FloatType, OptionType>(size, options, alignment,
                                                     memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::Float64(int64_t size, double min, double max,
                                                     double null_probability,
                                                     double nan_probability,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  using OptionType =
      GenerateOptions<double, ::arrow::random::uniform_real_distribution<double>>;
  OptionType options(seed(), min, max, null_probability, nan_probability);
  return GenerateNumericArray<DoubleType, OptionType>(size, options, alignment,
                                                      memory_pool);
}

#undef PRIMITIVE_RAND_INTEGER_IMPL
#undef PRIMITIVE_RAND_IMPL

namespace {

// A generic generator for random decimal arrays
template <typename DecimalType>
struct DecimalGenerator {
  using DecimalBuilderType = typename TypeTraits<DecimalType>::BuilderType;
  using DecimalValue = typename DecimalBuilderType::ValueType;

  std::shared_ptr<DataType> type_;
  RandomArrayGenerator* rng_;

  static uint64_t MaxDecimalInteger(int32_t digits) {
    // Need to decrement *after* the cast to uint64_t because, while
    // 10**x is exactly representable in a double for x <= 19,
    // 10**x - 1 is not.
    return static_cast<uint64_t>(std::ceil(std::pow(10.0, digits))) - 1;
  }

  std::shared_ptr<Array> MakeRandomArray(int64_t size, double null_probability,
                                         int64_t alignment, MemoryPool* memory_pool) {
    // 10**19 fits in a 64-bit unsigned integer
    static constexpr int32_t kMaxDigitsInInteger = 19;
    static constexpr int kNumIntegers = DecimalType::kByteWidth / 8;

    static_assert(
        kNumIntegers ==
            (DecimalType::kMaxPrecision + kMaxDigitsInInteger - 1) / kMaxDigitsInInteger,
        "inconsistent decimal metadata: kMaxPrecision doesn't match kByteWidth");

    // First generate separate random values for individual components:
    // boolean sign (including null-ness), and uint64 "digits" in big endian order.
    const auto& decimal_type = checked_cast<const DecimalType&>(*type_);

    const auto sign_array = checked_pointer_cast<BooleanArray>(rng_->Boolean(
        size, /*true_probability=*/0.5, null_probability, alignment, memory_pool));
    std::array<std::shared_ptr<UInt64Array>, kNumIntegers> digit_arrays;

    auto remaining_digits = decimal_type.precision();
    for (int i = kNumIntegers - 1; i >= 0; --i) {
      const auto digits = std::min(kMaxDigitsInInteger, remaining_digits);
      digit_arrays[i] = checked_pointer_cast<UInt64Array>(
          rng_->UInt64(size, 0, MaxDecimalInteger(digits), /*null_probability=*/0,
                       alignment, memory_pool));
      DCHECK_EQ(digit_arrays[i]->null_count(), 0);
      remaining_digits -= digits;
    }

    // Second compute decimal values from the individual components,
    // building up a decimal array.
    DecimalBuilderType builder(type_, memory_pool, alignment);
    ABORT_NOT_OK(builder.Reserve(size));

    const DecimalValue kDigitsMultiplier =
        DecimalValue::GetScaleMultiplier(kMaxDigitsInInteger);

    for (int64_t i = 0; i < size; ++i) {
      if (sign_array->IsValid(i)) {
        DecimalValue dec_value{0};
        for (int j = 0; j < kNumIntegers; ++j) {
          dec_value =
              dec_value * kDigitsMultiplier + DecimalValue(digit_arrays[j]->Value(i));
        }
        if (sign_array->Value(i)) {
          builder.UnsafeAppend(dec_value.Negate());
        } else {
          builder.UnsafeAppend(dec_value);
        }
      } else {
        builder.UnsafeAppendNull();
      }
    }
    std::shared_ptr<Array> array;
    ABORT_NOT_OK(builder.Finish(&array));
    return array;
  }
};

}  // namespace

std::shared_ptr<Array> RandomArrayGenerator::Decimal128(std::shared_ptr<DataType> type,
                                                        int64_t size,
                                                        double null_probability,
                                                        int64_t alignment,
                                                        MemoryPool* memory_pool) {
  DecimalGenerator<Decimal128Type> gen{type, this};
  return gen.MakeRandomArray(size, null_probability, alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::Decimal256(std::shared_ptr<DataType> type,
                                                        int64_t size,
                                                        double null_probability,
                                                        int64_t alignment,
                                                        MemoryPool* memory_pool) {
  DecimalGenerator<Decimal256Type> gen{type, this};
  return gen.MakeRandomArray(size, null_probability, alignment, memory_pool);
}

template <typename TypeClass>
static std::shared_ptr<Array> GenerateBinaryArray(RandomArrayGenerator* gen, int64_t size,
                                                  int32_t min_length, int32_t max_length,
                                                  double null_probability,
                                                  int64_t alignment,
                                                  MemoryPool* memory_pool) {
  using offset_type = typename TypeClass::offset_type;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;
  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;

  if (null_probability < 0 || null_probability > 1) {
    ABORT_NOT_OK(Status::Invalid("null_probability must be between 0 and 1"));
  }

  auto lengths = std::dynamic_pointer_cast<OffsetArrayType>(gen->Numeric<OffsetArrowType>(
      size, min_length, max_length, null_probability, alignment, memory_pool));

  // Visual Studio does not implement uniform_int_distribution for char types.
  using GenOpt = GenerateOptions<uint8_t, std::uniform_int_distribution<uint16_t>>;
  GenOpt options(gen->seed(), static_cast<uint8_t>('A'), static_cast<uint8_t>('z'),
                 /*null_probability=*/0);

  std::vector<uint8_t> str_buffer(max_length);
  BuilderType builder(memory_pool, alignment);

  for (int64_t i = 0; i < size; ++i) {
    if (lengths->IsValid(i)) {
      options.GenerateData(str_buffer.data(), lengths->Value(i));
      ABORT_NOT_OK(builder.Append(str_buffer.data(), lengths->Value(i)));
    } else {
      ABORT_NOT_OK(builder.AppendNull());
    }
  }

  std::shared_ptr<Array> result;
  ABORT_NOT_OK(builder.Finish(&result));
  return result;
}

std::shared_ptr<Array> RandomArrayGenerator::String(int64_t size, int32_t min_length,
                                                    int32_t max_length,
                                                    double null_probability,
                                                    int64_t alignment,
                                                    MemoryPool* memory_pool) {
  return GenerateBinaryArray<StringType>(this, size, min_length, max_length,
                                         null_probability, alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::LargeString(int64_t size, int32_t min_length,
                                                         int32_t max_length,
                                                         double null_probability,
                                                         int64_t alignment,
                                                         MemoryPool* memory_pool) {
  return GenerateBinaryArray<LargeStringType>(this, size, min_length, max_length,
                                              null_probability, alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::BinaryWithRepeats(
    int64_t size, int64_t unique, int32_t min_length, int32_t max_length,
    double null_probability, int64_t alignment, MemoryPool* memory_pool) {
  auto strings = StringWithRepeats(size, unique, min_length, max_length, null_probability,
                                   alignment, memory_pool);
  std::shared_ptr<Array> out;
  return *strings->View(binary());
}

std::shared_ptr<Array> RandomArrayGenerator::StringWithRepeats(
    int64_t size, int64_t unique, int32_t min_length, int32_t max_length,
    double null_probability, int64_t alignment, MemoryPool* memory_pool) {
  ARROW_CHECK_LE(unique, size);

  // Generate a random string dictionary without any nulls
  auto array = String(unique, min_length, max_length, /*null_probability=*/0);
  auto dictionary = std::dynamic_pointer_cast<StringArray>(array);

  // Generate random indices to sample the dictionary with
  auto id_array = Int64(size, 0, unique - 1, null_probability);
  auto indices = std::dynamic_pointer_cast<Int64Array>(id_array);
  StringBuilder builder;

  for (int64_t i = 0; i < size; ++i) {
    if (indices->IsValid(i)) {
      const auto index = indices->Value(i);
      const auto value = dictionary->GetView(index);
      ABORT_NOT_OK(builder.Append(value));
    } else {
      ABORT_NOT_OK(builder.AppendNull());
    }
  }

  std::shared_ptr<Array> result;
  ABORT_NOT_OK(builder.Finish(&result));
  return result;
}

std::shared_ptr<Array> RandomArrayGenerator::FixedSizeBinary(int64_t size,
                                                             int32_t byte_width,
                                                             double null_probability,
                                                             int64_t alignment,
                                                             MemoryPool* memory_pool) {
  if (null_probability < 0 || null_probability > 1) {
    ABORT_NOT_OK(Status::Invalid("null_probability must be between 0 and 1"));
  }

  // Visual Studio does not implement uniform_int_distribution for char types.
  using GenOpt = GenerateOptions<uint8_t, std::uniform_int_distribution<uint16_t>>;
  GenOpt options(seed(), static_cast<uint8_t>('A'), static_cast<uint8_t>('z'),
                 null_probability);

  int64_t null_count = 0;
  auto null_bitmap = *AllocateEmptyBitmap(size, alignment, memory_pool);
  auto data_buffer = *AllocateBuffer(size * byte_width, alignment, memory_pool);
  options.GenerateBitmap(null_bitmap->mutable_data(), size, &null_count);
  options.GenerateData(data_buffer->mutable_data(), size * byte_width);

  auto type = fixed_size_binary(byte_width);
  return std::make_shared<FixedSizeBinaryArray>(type, size, std::move(data_buffer),
                                                std::move(null_bitmap), null_count);
}

namespace {
template <typename OffsetArrayType>
std::shared_ptr<Array> GenerateOffsets(SeedType seed, int64_t size,
                                       typename OffsetArrayType::value_type first_offset,
                                       typename OffsetArrayType::value_type last_offset,
                                       double null_probability, bool force_empty_nulls,
                                       int64_t alignment, MemoryPool* memory_pool) {
  using GenOpt = GenerateOptions<
      typename OffsetArrayType::value_type,
      std::uniform_int_distribution<typename OffsetArrayType::value_type>>;
  GenOpt options(seed, first_offset, last_offset, null_probability);

  BufferVector buffers{2};

  int64_t null_count = 0;

  buffers[0] = *AllocateEmptyBitmap(size, alignment, memory_pool);
  uint8_t* null_bitmap = buffers[0]->mutable_data();
  options.GenerateBitmap(null_bitmap, size, &null_count);
  // Make sure the first and last entry are non-null
  for (const int64_t offset : std::vector<int64_t>{0, size - 1}) {
    if (!arrow::bit_util::GetBit(null_bitmap, offset)) {
      arrow::bit_util::SetBit(null_bitmap, offset);
      --null_count;
    }
  }

  buffers[1] = *AllocateBuffer(sizeof(typename OffsetArrayType::value_type) * size,
                               alignment, memory_pool);
  auto data =
      reinterpret_cast<typename OffsetArrayType::value_type*>(buffers[1]->mutable_data());
  options.GenerateTypedData(data, size);
  // Ensure offsets are in increasing order
  std::sort(data, data + size);
  // Ensure first and last offsets are as required
  DCHECK_GE(data[0], first_offset);
  DCHECK_LE(data[size - 1], last_offset);
  data[0] = first_offset;
  data[size - 1] = last_offset;

  if (force_empty_nulls) {
    arrow::internal::BitmapReader reader(null_bitmap, 0, size);
    for (int64_t i = 0; i < size; ++i) {
      if (reader.IsNotSet()) {
        // Ensure a null entry corresponds to a 0-sized list extent
        // (note this can be neither the first nor the last list entry, see above)
        data[i + 1] = data[i];
      }
      reader.Next();
    }
  }

  auto array_data = ArrayData::Make(
      std::make_shared<typename OffsetArrayType::TypeClass>(), size, buffers, null_count);
  return std::make_shared<OffsetArrayType>(array_data);
}

template <typename OffsetArrayType>
std::shared_ptr<Array> OffsetsFromLengthsArray(OffsetArrayType* lengths,
                                               bool force_empty_nulls, int64_t alignment,
                                               MemoryPool* memory_pool) {
  // Need N + 1 offsets for N items
  int64_t size = lengths->length() + 1;
  BufferVector buffers{2};

  int64_t null_count = 0;

  buffers[0] = *AllocateEmptyBitmap(size, alignment, memory_pool);
  uint8_t* null_bitmap = buffers[0]->mutable_data();
  // Make sure the first and last entry are non-null
  arrow::bit_util::SetBit(null_bitmap, 0);
  arrow::bit_util::SetBit(null_bitmap, size - 1);

  buffers[1] = *AllocateBuffer(sizeof(typename OffsetArrayType::value_type) * size,
                               alignment, memory_pool);
  auto data =
      reinterpret_cast<typename OffsetArrayType::value_type*>(buffers[1]->mutable_data());
  data[0] = 0;
  int index = 1;
  for (const auto& length : *lengths) {
    if (length.has_value()) {
      arrow::bit_util::SetBit(null_bitmap, index);
      data[index] = data[index - 1] + *length;
      DCHECK_GE(*length, 0);
    } else if (index == size - 1) {
      // Last list offset is non-null (see above)
      data[index] = data[index - 1];
    } else {
      data[index] = data[index - 1];
      null_count++;
    }
    index++;
  }

  if (force_empty_nulls) {
    arrow::internal::BitmapReader reader(null_bitmap, 0, size - 1);
    for (int64_t i = 0; i < size - 1; ++i) {
      if (reader.IsNotSet()) {
        // Ensure a null entry corresponds to a 0-sized list extent
        // (note this can be neither the first nor the last list entry, see above)
        data[i + 1] = data[i];
      }
      reader.Next();
    }
  }

  auto array_data = ArrayData::Make(
      std::make_shared<typename OffsetArrayType::TypeClass>(), size, buffers, null_count);
  return std::make_shared<OffsetArrayType>(array_data);
}
}  // namespace

std::shared_ptr<Array> RandomArrayGenerator::Offsets(
    int64_t size, int32_t first_offset, int32_t last_offset, double null_probability,
    bool force_empty_nulls, int64_t alignment, MemoryPool* memory_pool) {
  return GenerateOffsets<NumericArray<Int32Type>>(seed(), size, first_offset, last_offset,
                                                  null_probability, force_empty_nulls,
                                                  alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::LargeOffsets(
    int64_t size, int64_t first_offset, int64_t last_offset, double null_probability,
    bool force_empty_nulls, int64_t alignment, MemoryPool* memory_pool) {
  return GenerateOffsets<NumericArray<Int64Type>>(seed(), size, first_offset, last_offset,
                                                  null_probability, force_empty_nulls,
                                                  alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::List(const Array& values, int64_t size,
                                                  double null_probability,
                                                  bool force_empty_nulls,
                                                  int64_t alignment,
                                                  MemoryPool* memory_pool) {
  auto offsets = Offsets(size + 1, static_cast<int32_t>(values.offset()),
                         static_cast<int32_t>(values.offset() + values.length()),
                         null_probability, force_empty_nulls, alignment, memory_pool);
  return *::arrow::ListArray::FromArrays(*offsets, values);
}

std::shared_ptr<Array> RandomArrayGenerator::Map(const std::shared_ptr<Array>& keys,
                                                 const std::shared_ptr<Array>& items,
                                                 int64_t size, double null_probability,
                                                 bool force_empty_nulls,
                                                 int64_t alignment,
                                                 MemoryPool* memory_pool) {
  DCHECK_EQ(keys->length(), items->length());
  auto offsets = Offsets(size + 1, static_cast<int32_t>(keys->offset()),
                         static_cast<int32_t>(keys->offset() + keys->length()),
                         null_probability, force_empty_nulls, alignment, memory_pool);
  return *::arrow::MapArray::FromArrays(offsets, keys, items);
}

std::shared_ptr<Array> RandomArrayGenerator::RunEndEncoded(
    std::shared_ptr<DataType> value_type, int64_t logical_size, double null_probability) {
  Int32Builder run_ends_builder;
  pcg32_fast rng(seed());

  DCHECK_LE(logical_size, std::numeric_limits<int32_t>::max());

  std::uniform_int_distribution<int64_t> distribution(1, 100);
  int64_t current_end = 0;
  while (current_end < logical_size) {
    current_end += distribution(rng);
    current_end = std::min(current_end, logical_size);
    ARROW_CHECK_OK(run_ends_builder.Append(static_cast<int32_t>(current_end)));
  }

  std::shared_ptr<Array> run_ends = *run_ends_builder.Finish();
  std::shared_ptr<Array> values =
      ArrayOf(std::move(value_type), run_ends->length(), null_probability);

  return RunEndEncodedArray::Make(logical_size, run_ends, values).ValueOrDie();
}

std::shared_ptr<Array> RandomArrayGenerator::SparseUnion(const ArrayVector& fields,
                                                         int64_t size, int64_t alignment,
                                                         MemoryPool* memory_pool) {
  DCHECK_GT(fields.size(), 0);
  // Trivial type codes map
  std::vector<UnionArray::type_code_t> type_codes(fields.size());
  std::iota(type_codes.begin(), type_codes.end(), 0);

  // Generate array of type ids
  auto type_ids = Int8(size, 0, static_cast<int8_t>(fields.size() - 1),
                       /*null_probability=*/0, alignment, memory_pool);
  return *SparseUnionArray::Make(*type_ids, fields, type_codes);
}

std::shared_ptr<Array> RandomArrayGenerator::DenseUnion(const ArrayVector& fields,
                                                        int64_t size, int64_t alignment,
                                                        MemoryPool* memory_pool) {
  DCHECK_GT(fields.size(), 0);
  // Trivial type codes map
  std::vector<UnionArray::type_code_t> type_codes(fields.size());
  std::iota(type_codes.begin(), type_codes.end(), 0);

  // Generate array of type ids
  auto type_ids = Int8(size, 0, static_cast<int8_t>(fields.size() - 1),
                       /*null_probability=*/0, alignment, memory_pool);

  // Generate array of offsets
  const auto& concrete_ids = checked_cast<const Int8Array&>(*type_ids);
  Int32Builder offsets_builder(memory_pool, alignment);
  ABORT_NOT_OK(offsets_builder.Reserve(size));
  std::vector<int32_t> last_offsets(fields.size(), 0);
  for (int64_t i = 0; i < size; ++i) {
    const auto field_id = concrete_ids.Value(i);
    offsets_builder.UnsafeAppend(last_offsets[field_id]++);
  }
  std::shared_ptr<Array> offsets;
  ABORT_NOT_OK(offsets_builder.Finish(&offsets));

  return *DenseUnionArray::Make(*type_ids, *offsets, fields, type_codes);
}

namespace {

// Helper for RandomArrayGenerator::ArrayOf: extract some C value from
// a given metadata key.
template <typename T, typename ArrowType = typename CTypeTraits<T>::ArrowType>
enable_if_parameter_free<ArrowType, T> GetMetadata(const KeyValueMetadata* metadata,
                                                   const std::string& key,
                                                   T default_value) {
  if (!metadata) return default_value;
  const auto index = metadata->FindKey(key);
  if (index < 0) return default_value;
  const auto& value = metadata->value(index);
  T output{};
  if (!internal::ParseValue<ArrowType>(value.data(), value.length(), &output)) {
    ABORT_NOT_OK(Status::Invalid("Could not parse ", key, " = ", value));
  }
  return output;
}

}  // namespace

std::shared_ptr<Array> RandomArrayGenerator::ArrayOf(std::shared_ptr<DataType> type,
                                                     int64_t size,
                                                     double null_probability,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  auto metadata = key_value_metadata({"null_probability"}, {ToChars(null_probability)});
  auto field = ::arrow::field("", std::move(type), std::move(metadata));
  return ArrayOf(*field, size, alignment, memory_pool);
}

std::shared_ptr<Array> RandomArrayGenerator::ArrayOf(const Field& field, int64_t length,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
#define VALIDATE_RANGE(PARAM, MIN, MAX)                                          \
  if (PARAM < MIN || PARAM > MAX) {                                              \
    ABORT_NOT_OK(Status::Invalid(field.ToString(), ": ", ARROW_STRINGIFY(PARAM), \
                                 " must be in [", MIN, ", ", MAX, " ] but got ", \
                                 PARAM));                                        \
  }
#define VALIDATE_MIN_MAX(MIN, MAX)                                                  \
  if (MIN > MAX) {                                                                  \
    ABORT_NOT_OK(                                                                   \
        Status::Invalid(field.ToString(), ": min ", MIN, " must be <= max ", MAX)); \
  }
#define GENERATE_INTEGRAL_CASE_VIEW(BASE_TYPE, VIEW_TYPE)                              \
  case VIEW_TYPE::type_id: {                                                           \
    const BASE_TYPE::c_type min_value = GetMetadata<BASE_TYPE::c_type>(                \
        field.metadata().get(), "min", std::numeric_limits<BASE_TYPE::c_type>::min()); \
    const BASE_TYPE::c_type max_value = GetMetadata<BASE_TYPE::c_type>(                \
        field.metadata().get(), "max", std::numeric_limits<BASE_TYPE::c_type>::max()); \
    VALIDATE_MIN_MAX(min_value, max_value);                                            \
    return *Numeric<BASE_TYPE>(length, min_value, max_value, null_probability,         \
                               alignment, memory_pool)                                 \
                ->View(field.type());                                                  \
  }
#define GENERATE_INTEGRAL_CASE(ARROW_TYPE) \
  GENERATE_INTEGRAL_CASE_VIEW(ARROW_TYPE, ARROW_TYPE)
#define GENERATE_FLOATING_CASE(ARROW_TYPE, GENERATOR_FUNC)                              \
  case ARROW_TYPE::type_id: {                                                           \
    const ARROW_TYPE::c_type min_value = GetMetadata<ARROW_TYPE::c_type>(               \
        field.metadata().get(), "min", std::numeric_limits<ARROW_TYPE::c_type>::min()); \
    const ARROW_TYPE::c_type max_value = GetMetadata<ARROW_TYPE::c_type>(               \
        field.metadata().get(), "max", std::numeric_limits<ARROW_TYPE::c_type>::max()); \
    const double nan_probability =                                                      \
        GetMetadata<double>(field.metadata().get(), "nan_probability", 0);              \
    VALIDATE_MIN_MAX(min_value, max_value);                                             \
    VALIDATE_RANGE(nan_probability, 0.0, 1.0);                                          \
    return GENERATOR_FUNC(length, min_value, max_value, null_probability,               \
                          nan_probability, alignment, memory_pool);                     \
  }

  // Don't use compute::Sum since that may not get built
#define GENERATE_LIST_CASE(ARRAY_TYPE)                                               \
  case ARRAY_TYPE::TypeClass::type_id: {                                             \
    const auto min_length = GetMetadata<ARRAY_TYPE::TypeClass::offset_type>(         \
        field.metadata().get(), "min_length", 0);                                    \
    const auto max_length = GetMetadata<ARRAY_TYPE::TypeClass::offset_type>(         \
        field.metadata().get(), "max_length", 20);                                   \
    const auto lengths = internal::checked_pointer_cast<                             \
        CTypeTraits<ARRAY_TYPE::TypeClass::offset_type>::ArrayType>(                 \
        Numeric<CTypeTraits<ARRAY_TYPE::TypeClass::offset_type>::ArrowType>(         \
            length, min_length, max_length, null_probability));                      \
    int64_t values_length = 0;                                                       \
    for (const auto& length : *lengths) {                                            \
      if (length.has_value()) values_length += *length;                              \
    }                                                                                \
    const auto force_empty_nulls =                                                   \
        GetMetadata<bool>(field.metadata().get(), "force_empty_nulls", false);       \
    const auto values =                                                              \
        ArrayOf(*internal::checked_pointer_cast<ARRAY_TYPE::TypeClass>(field.type()) \
                     ->value_field(),                                                \
                values_length, alignment, memory_pool);                              \
    const auto offsets = OffsetsFromLengthsArray(lengths.get(), force_empty_nulls,   \
                                                 alignment, memory_pool);            \
    return *ARRAY_TYPE::FromArrays(*offsets, *values);                               \
  }

  const double null_probability =
      field.nullable()
          ? GetMetadata<double>(field.metadata().get(), "null_probability", 0.01)
          : 0.0;
  VALIDATE_RANGE(null_probability, 0.0, 1.0);
  switch (field.type()->id()) {
    case Type::type::NA: {
      return std::make_shared<NullArray>(length);
    }

    case Type::type::BOOL: {
      const double true_probability =
          GetMetadata<double>(field.metadata().get(), "true_probability", 0.5);
      return Boolean(length, true_probability, null_probability, alignment, memory_pool);
    }

      GENERATE_INTEGRAL_CASE(UInt8Type);
      GENERATE_INTEGRAL_CASE(Int8Type);
      GENERATE_INTEGRAL_CASE(UInt16Type);
      GENERATE_INTEGRAL_CASE(Int16Type);
      GENERATE_INTEGRAL_CASE(UInt32Type);
      GENERATE_INTEGRAL_CASE(Int32Type);
      GENERATE_INTEGRAL_CASE(UInt64Type);
      GENERATE_INTEGRAL_CASE(Int64Type);
      GENERATE_INTEGRAL_CASE_VIEW(Int16Type, HalfFloatType);
      GENERATE_FLOATING_CASE(FloatType, Float32);
      GENERATE_FLOATING_CASE(DoubleType, Float64);

    case Type::type::STRING:
    case Type::type::BINARY: {
      const auto min_length =
          GetMetadata<int32_t>(field.metadata().get(), "min_length", 0);
      const auto max_length =
          GetMetadata<int32_t>(field.metadata().get(), "max_length", 20);
      const auto unique_values =
          GetMetadata<int32_t>(field.metadata().get(), "unique", -1);
      if (unique_values > 0) {
        return *StringWithRepeats(length, unique_values, min_length, max_length,
                                  null_probability, alignment, memory_pool)
                    ->View(field.type());
      }
      return *String(length, min_length, max_length, null_probability, alignment,
                     memory_pool)
                  ->View(field.type());
    }

    case Type::type::DECIMAL128:
      return Decimal128(field.type(), length, null_probability, alignment, memory_pool);

    case Type::type::DECIMAL256:
      return Decimal256(field.type(), length, null_probability, alignment, memory_pool);

    case Type::type::FIXED_SIZE_BINARY: {
      auto byte_width =
          internal::checked_pointer_cast<FixedSizeBinaryType>(field.type())->byte_width();
      return *FixedSizeBinary(length, byte_width, null_probability, alignment,
                              memory_pool)
                  ->View(field.type());
    }

      GENERATE_INTEGRAL_CASE_VIEW(Int32Type, Date32Type);
      GENERATE_INTEGRAL_CASE_VIEW(Int64Type, TimestampType);
      GENERATE_INTEGRAL_CASE_VIEW(Int32Type, MonthIntervalType);

    case Type::type::DATE64: {
      using c_type = typename Date64Type::c_type;
      constexpr c_type kFullDayMillis = 1000 * 60 * 60 * 24;
      constexpr c_type kDefaultMin = std::numeric_limits<c_type>::min() / kFullDayMillis;
      constexpr c_type kDefaultMax = std::numeric_limits<c_type>::max() / kFullDayMillis;

      const c_type min_value =
          GetMetadata<c_type>(field.metadata().get(), "min", kDefaultMin);
      const c_type max_value =
          GetMetadata<c_type>(field.metadata().get(), "max", kDefaultMax);

      return *Numeric<Date64Type>(length, min_value, max_value, null_probability,
                                  alignment, memory_pool)
                  ->View(field.type());
    }

    case Type::type::TIME32: {
      TimeUnit::type unit =
          internal::checked_pointer_cast<Time32Type>(field.type())->unit();
      using c_type = typename Time32Type::c_type;
      const c_type min_value = 0;
      const c_type max_value =
          (unit == TimeUnit::SECOND) ? (60 * 60 * 24 - 1) : (1000 * 60 * 60 * 24 - 1);

      return *Numeric<Int32Type>(length, min_value, max_value, null_probability,
                                 alignment, memory_pool)
                  ->View(field.type());
    }

    case Type::type::TIME64: {
      TimeUnit::type unit =
          internal::checked_pointer_cast<Time64Type>(field.type())->unit();
      using c_type = typename Time64Type::c_type;
      const c_type min_value = 0;
      const c_type max_value = (unit == TimeUnit::MICRO)
                                   ? (1000000LL * 60 * 60 * 24 - 1)
                                   : (1000000000LL * 60 * 60 * 24 - 1);

      return *Numeric<Int64Type>(length, min_value, max_value, null_probability,
                                 alignment, memory_pool)
                  ->View(field.type());
    }

      // This isn't as flexible as it could be, but the array-of-structs layout of this
      // type means it's not a (useful) composition of other generators
      GENERATE_INTEGRAL_CASE_VIEW(Int64Type, DayTimeIntervalType);
    case Type::type::INTERVAL_MONTH_DAY_NANO: {
      return *FixedSizeBinary(length, /*byte_width=*/16, null_probability, alignment,
                              memory_pool)
                  ->View(month_day_nano_interval());
    }

      GENERATE_LIST_CASE(ListArray);

    case Type::type::STRUCT: {
      ArrayVector child_arrays(field.type()->num_fields());
      std::vector<std::string> field_names;
      for (int i = 0; i < field.type()->num_fields(); i++) {
        const auto& child_field = field.type()->field(i);
        child_arrays[i] = ArrayOf(*child_field, length, alignment, memory_pool);
        field_names.push_back(child_field->name());
      }
      return *StructArray::Make(
          child_arrays, field_names,
          NullBitmap(length, null_probability, alignment, memory_pool));
    }

    case Type::type::RUN_END_ENCODED: {
      auto* ree_type = internal::checked_cast<RunEndEncodedType*>(field.type().get());
      return RunEndEncoded(ree_type->value_type(), length, null_probability);
    }

    case Type::type::SPARSE_UNION:
    case Type::type::DENSE_UNION: {
      ArrayVector child_arrays(field.type()->num_fields());
      for (int i = 0; i < field.type()->num_fields(); i++) {
        const auto& child_field = field.type()->field(i);
        child_arrays[i] = ArrayOf(*child_field, length, alignment, memory_pool);
      }
      auto array = field.type()->id() == Type::type::SPARSE_UNION
                       ? SparseUnion(child_arrays, length, alignment, memory_pool)
                       : DenseUnion(child_arrays, length, alignment, memory_pool);
      return *array->View(field.type());
    }

    case Type::type::DICTIONARY: {
      const auto values_length =
          GetMetadata<int64_t>(field.metadata().get(), "values", 4);
      auto dict_type = internal::checked_pointer_cast<DictionaryType>(field.type());
      // TODO: no way to control generation of dictionary
      auto values =
          ArrayOf(*arrow::field("temporary", dict_type->value_type(), /*nullable=*/false),
                  values_length, alignment, memory_pool);
      auto merged = field.metadata() ? field.metadata() : key_value_metadata({}, {});
      if (merged->Contains("min"))
        ABORT_NOT_OK(Status::Invalid(field.ToString(), ": cannot specify min"));
      if (merged->Contains("max"))
        ABORT_NOT_OK(Status::Invalid(field.ToString(), ": cannot specify max"));
      merged = merged->Merge(
          *key_value_metadata({{"min", "0"}, {"max", ToChars(values_length - 1)}}));
      auto indices = ArrayOf(
          *arrow::field("temporary", dict_type->index_type(), field.nullable(), merged),
          length, alignment, memory_pool);
      return *DictionaryArray::FromArrays(field.type(), indices, values);
    }

    case Type::type::MAP: {
      const auto values_length = GetMetadata<int32_t>(field.metadata().get(), "values",
                                                      static_cast<int32_t>(length));
      const auto force_empty_nulls =
          GetMetadata<bool>(field.metadata().get(), "force_empty_nulls", false);
      auto map_type = internal::checked_pointer_cast<MapType>(field.type());
      auto keys = ArrayOf(*map_type->key_field(), values_length, alignment, memory_pool);
      auto items =
          ArrayOf(*map_type->item_field(), values_length, alignment, memory_pool);
      // need N + 1 offsets to have N values
      auto offsets = Offsets(length + 1, 0, values_length, null_probability,
                             force_empty_nulls, alignment, memory_pool);
      return *MapArray::FromArrays(map_type, offsets, keys, items);
    }

    case Type::type::EXTENSION:
      // Could be supported by generating the storage type (though any extension
      // invariants wouldn't be preserved)
      break;

    case Type::type::FIXED_SIZE_LIST: {
      auto list_type = internal::checked_pointer_cast<FixedSizeListType>(field.type());
      const int64_t values_length = list_type->list_size() * length;
      auto values =
          ArrayOf(*list_type->value_field(), values_length, alignment, memory_pool);
      auto null_bitmap = NullBitmap(length, null_probability, alignment, memory_pool);
      return std::make_shared<FixedSizeListArray>(list_type, length, values, null_bitmap);
    }

      GENERATE_INTEGRAL_CASE_VIEW(Int64Type, DurationType);

    case Type::type::LARGE_STRING:
    case Type::type::LARGE_BINARY: {
      const auto min_length =
          GetMetadata<int32_t>(field.metadata().get(), "min_length", 0);
      const auto max_length =
          GetMetadata<int32_t>(field.metadata().get(), "max_length", 20);
      const auto unique_values =
          GetMetadata<int32_t>(field.metadata().get(), "unique", -1);
      if (unique_values > 0) {
        ABORT_NOT_OK(
            Status::NotImplemented("Generating random array with repeated values for "
                                   "large string/large binary types"));
      }
      return *LargeString(length, min_length, max_length, null_probability, alignment,
                          memory_pool)
                  ->View(field.type());
    }

      GENERATE_LIST_CASE(LargeListArray);

    default:
      break;
  }
#undef GENERATE_INTEGRAL_CASE_VIEW
#undef GENERATE_INTEGRAL_CASE
#undef GENERATE_FLOATING_CASE
#undef GENERATE_LIST_CASE
#undef VALIDATE_RANGE
#undef VALIDATE_MIN_MAX

  ABORT_NOT_OK(
      Status::NotImplemented("Generating random array for field ", field.ToString()));
  return nullptr;
}

std::shared_ptr<arrow::RecordBatch> RandomArrayGenerator::BatchOf(
    const FieldVector& fields, int64_t length, int64_t alignment,
    MemoryPool* memory_pool) {
  std::vector<std::shared_ptr<Array>> arrays(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    arrays[i] = ArrayOf(*field, length, alignment, memory_pool);
  }
  return RecordBatch::Make(schema(fields), length, std::move(arrays));
}

std::shared_ptr<arrow::Array> GenerateArray(const Field& field, int64_t length,
                                            SeedType seed, int64_t alignment,
                                            MemoryPool* memory_pool) {
  return RandomArrayGenerator(seed).ArrayOf(field, length, alignment, memory_pool);
}

std::shared_ptr<arrow::RecordBatch> GenerateBatch(const FieldVector& fields,
                                                  int64_t length, SeedType seed,
                                                  int64_t alignment,
                                                  MemoryPool* memory_pool) {
  return RandomArrayGenerator(seed).BatchOf(fields, length, alignment, memory_pool);
}
}  // namespace random

void rand_day_millis(int64_t N, std::vector<DayTimeIntervalType::DayMilliseconds>* out) {
  const int random_seed = 0;
  arrow::random::pcg32_fast gen(random_seed);
  std::uniform_int_distribution<int32_t> d(std::numeric_limits<int32_t>::min(),
                                           std::numeric_limits<int32_t>::max());
  out->resize(N, {});
  std::generate(out->begin(), out->end(), [&d, &gen] {
    DayTimeIntervalType::DayMilliseconds tmp;
    tmp.days = d(gen);
    tmp.milliseconds = d(gen);
    return tmp;
  });
}

void rand_month_day_nanos(int64_t N,
                          std::vector<MonthDayNanoIntervalType::MonthDayNanos>* out) {
  const int random_seed = 0;
  arrow::random::pcg32_fast gen(random_seed);
  std::uniform_int_distribution<int64_t> d(std::numeric_limits<int64_t>::min(),
                                           std::numeric_limits<int64_t>::max());
  out->resize(N, {});
  std::generate(out->begin(), out->end(), [&d, &gen] {
    MonthDayNanoIntervalType::MonthDayNanos tmp;
    tmp.months = static_cast<int32_t>(d(gen));
    tmp.days = static_cast<int32_t>(d(gen));
    tmp.nanoseconds = d(gen);
    return tmp;
  });
}

}  // namespace arrow
