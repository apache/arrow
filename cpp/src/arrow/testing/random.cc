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

#include <algorithm>
#include <limits>
#include <memory>
#include <random>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

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
    std::default_random_engine rng(seed_++);
    DistributionType dist(min_, max_);
    std::bernoulli_distribution nan_dist(nan_probability_);
    const ValueType nan_value = std::numeric_limits<ValueType>::quiet_NaN();

    // A static cast is required due to the int16 -> int8 handling.
    std::generate(data, data + n, [&] {
      return nan_dist(rng) ? nan_value : static_cast<ValueType>(dist(rng));
    });
  }

  void GenerateTypedDataNoNan(ValueType* data, size_t n) {
    std::default_random_engine rng(seed_++);
    DistributionType dist(min_, max_);

    // A static cast is required due to the int16 -> int8 handling.
    std::generate(data, data + n, [&] { return static_cast<ValueType>(dist(rng)); });
  }

  void GenerateBitmap(uint8_t* buffer, size_t n, int64_t* null_count) {
    int64_t count = 0;
    std::default_random_engine rng(seed_++);
    std::bernoulli_distribution dist(1.0 - probability_);

    for (size_t i = 0; i < n; i++) {
      if (dist(rng)) {
        BitUtil::SetBit(buffer, i);
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
                                                         double null_probability) {
  // The bitmap generator does not care about the value distribution since it
  // only calls the GenerateBitmap method.
  using GenOpt = GenerateOptions<int, std::uniform_int_distribution<int>>;

  GenOpt null_gen(seed(), 0, 1, null_probability);
  std::shared_ptr<Buffer> bitmap = *AllocateEmptyBitmap(size);
  null_gen.GenerateBitmap(bitmap->mutable_data(), size, nullptr);

  return bitmap;
}

std::shared_ptr<Array> RandomArrayGenerator::Boolean(int64_t size,
                                                     double true_probability,
                                                     double null_probability) {
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
  buffers[0] = *AllocateEmptyBitmap(size);
  null_gen.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  buffers[1] = *AllocateEmptyBitmap(size);
  value_gen.GenerateBitmap(buffers[1]->mutable_data(), size, nullptr);

  auto array_data = ArrayData::Make(arrow::boolean(), size, buffers, null_count);
  return std::make_shared<BooleanArray>(array_data);
}

template <typename ArrowType, typename OptionType>
static std::shared_ptr<NumericArray<ArrowType>> GenerateNumericArray(int64_t size,
                                                                     OptionType options) {
  using CType = typename ArrowType::c_type;
  auto type = TypeTraits<ArrowType>::type_singleton();
  BufferVector buffers{2};

  int64_t null_count = 0;
  buffers[0] = *AllocateEmptyBitmap(size);
  options.GenerateBitmap(buffers[0]->mutable_data(), size, &null_count);

  buffers[1] = *AllocateBuffer(sizeof(CType) * size);
  options.GenerateData(buffers[1]->mutable_data(), size);

  auto array_data = ArrayData::Make(type, size, buffers, null_count);
  return std::make_shared<NumericArray<ArrowType>>(array_data);
}

#define PRIMITIVE_RAND_IMPL(Name, CType, ArrowType, Distribution)                       \
  std::shared_ptr<Array> RandomArrayGenerator::Name(int64_t size, CType min, CType max, \
                                                    double probability) {               \
    using OptionType = GenerateOptions<CType, Distribution>;                            \
    OptionType options(seed(), min, max, probability);                                  \
    return GenerateNumericArray<ArrowType, OptionType>(size, options);                  \
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

std::shared_ptr<Array> RandomArrayGenerator::Float32(int64_t size, float min, float max,
                                                     double null_probability,
                                                     double nan_probability) {
  using OptionType = GenerateOptions<float, std::uniform_real_distribution<float>>;
  OptionType options(seed(), min, max, null_probability, nan_probability);
  return GenerateNumericArray<FloatType, OptionType>(size, options);
}

std::shared_ptr<Array> RandomArrayGenerator::Float64(int64_t size, double min, double max,
                                                     double null_probability,
                                                     double nan_probability) {
  using OptionType = GenerateOptions<double, std::uniform_real_distribution<double>>;
  OptionType options(seed(), min, max, null_probability, nan_probability);
  return GenerateNumericArray<DoubleType, OptionType>(size, options);
}

#undef PRIMITIVE_RAND_INTEGER_IMPL
#undef PRIMITIVE_RAND_IMPL

template <typename TypeClass>
static std::shared_ptr<Array> GenerateBinaryArray(RandomArrayGenerator* gen, int64_t size,
                                                  int32_t min_length, int32_t max_length,
                                                  double null_probability) {
  using offset_type = typename TypeClass::offset_type;
  using BuilderType = typename TypeTraits<TypeClass>::BuilderType;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;
  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;

  if (null_probability < 0 || null_probability > 1) {
    ABORT_NOT_OK(Status::Invalid("null_probability must be between 0 and 1"));
  }

  auto lengths = std::dynamic_pointer_cast<OffsetArrayType>(
      gen->Numeric<OffsetArrowType>(size, min_length, max_length, null_probability));

  // Visual Studio does not implement uniform_int_distribution for char types.
  using GenOpt = GenerateOptions<uint8_t, std::uniform_int_distribution<uint16_t>>;
  GenOpt options(gen->seed(), static_cast<uint8_t>('A'), static_cast<uint8_t>('z'),
                 /*null_probability=*/0);

  std::vector<uint8_t> str_buffer(max_length);
  BuilderType builder;

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
                                                    double null_probability) {
  return GenerateBinaryArray<StringType>(this, size, min_length, max_length,
                                         null_probability);
}

std::shared_ptr<Array> RandomArrayGenerator::LargeString(int64_t size, int32_t min_length,
                                                         int32_t max_length,
                                                         double null_probability) {
  return GenerateBinaryArray<LargeStringType>(this, size, min_length, max_length,
                                              null_probability);
}

std::shared_ptr<Array> RandomArrayGenerator::BinaryWithRepeats(int64_t size,
                                                               int64_t unique,
                                                               int32_t min_length,
                                                               int32_t max_length,
                                                               double null_probability) {
  auto strings =
      StringWithRepeats(size, unique, min_length, max_length, null_probability);
  std::shared_ptr<Array> out;
  return *strings->View(binary());
}

std::shared_ptr<Array> RandomArrayGenerator::StringWithRepeats(int64_t size,
                                                               int64_t unique,
                                                               int32_t min_length,
                                                               int32_t max_length,
                                                               double null_probability) {
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
                                                             double null_probability) {
  if (null_probability < 0 || null_probability > 1) {
    ABORT_NOT_OK(Status::Invalid("null_probability must be between 0 and 1"));
  }

  // Visual Studio does not implement uniform_int_distribution for char types.
  using GenOpt = GenerateOptions<uint8_t, std::uniform_int_distribution<uint16_t>>;
  GenOpt options(seed(), static_cast<uint8_t>('A'), static_cast<uint8_t>('z'),
                 null_probability);

  int64_t null_count = 0;
  auto null_bitmap = *AllocateEmptyBitmap(size);
  auto data_buffer = *AllocateBuffer(size * byte_width);
  options.GenerateBitmap(null_bitmap->mutable_data(), size, &null_count);
  options.GenerateData(data_buffer->mutable_data(), size * byte_width);

  auto type = fixed_size_binary(byte_width);
  return std::make_shared<FixedSizeBinaryArray>(type, size, std::move(data_buffer),
                                                std::move(null_bitmap), null_count);
}

std::shared_ptr<Array> RandomArrayGenerator::Offsets(int64_t size, int32_t first_offset,
                                                     int32_t last_offset,
                                                     double null_probability,
                                                     bool force_empty_nulls) {
  using GenOpt = GenerateOptions<int32_t, std::uniform_int_distribution<int32_t>>;
  GenOpt options(seed(), first_offset, last_offset, null_probability);

  BufferVector buffers{2};

  int64_t null_count = 0;

  buffers[0] = *AllocateEmptyBitmap(size);
  uint8_t* null_bitmap = buffers[0]->mutable_data();
  options.GenerateBitmap(null_bitmap, size, &null_count);
  // Make sure the first and last entry are non-null
  arrow::BitUtil::SetBit(null_bitmap, 0);
  arrow::BitUtil::SetBit(null_bitmap, size - 1);

  buffers[1] = *AllocateBuffer(sizeof(int32_t) * size);
  auto data = reinterpret_cast<int32_t*>(buffers[1]->mutable_data());
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

  auto array_data = ArrayData::Make(int32(), size, buffers, null_count);
  return std::make_shared<Int32Array>(array_data);
}

std::shared_ptr<Array> RandomArrayGenerator::List(const Array& values, int64_t size,
                                                  double null_probability,
                                                  bool force_empty_nulls) {
  auto offsets = Offsets(size, static_cast<int32_t>(values.offset()),
                         static_cast<int32_t>(values.offset() + values.length()),
                         null_probability, force_empty_nulls);
  return *::arrow::ListArray::FromArrays(*offsets, values);
}

std::shared_ptr<Array> RandomArrayGenerator::SparseUnion(const ArrayVector& fields,
                                                         int64_t size) {
  DCHECK_GT(fields.size(), 0);
  // Trivial type codes map
  std::vector<UnionArray::type_code_t> type_codes(fields.size());
  std::iota(type_codes.begin(), type_codes.end(), 0);

  // Generate array of type ids
  auto type_ids = Int8(size, 0, static_cast<int8_t>(fields.size() - 1));
  return *SparseUnionArray::Make(*type_ids, fields, type_codes);
}

std::shared_ptr<Array> RandomArrayGenerator::DenseUnion(const ArrayVector& fields,
                                                        int64_t size) {
  DCHECK_GT(fields.size(), 0);
  // Trivial type codes map
  std::vector<UnionArray::type_code_t> type_codes(fields.size());
  std::iota(type_codes.begin(), type_codes.end(), 0);

  // Generate array of type ids
  auto type_ids = Int8(size, 0, static_cast<int8_t>(fields.size() - 1));

  // Generate array of offsets
  const auto& concrete_ids = checked_cast<const Int8Array&>(*type_ids);
  Int32Builder offsets_builder;
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

struct RandomArrayGeneratorOfImpl {
  Status Visit(const NullType&) {
    out_ = std::make_shared<NullArray>(size_);
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    double probability = 0.25;
    out_ = rag_->Boolean(size_, probability, null_probability_);
    return Status::OK();
  }

  template <typename T>
  enable_if_integer<T, Status> Visit(const T&) {
    auto max = std::numeric_limits<typename T::c_type>::max();
    auto min = std::numeric_limits<typename T::c_type>::lowest();

    out_ = rag_->Numeric<T>(size_, min, max, null_probability_);
    return Status::OK();
  }

  template <typename T>
  enable_if_floating_point<T, Status> Visit(const T&) {
    out_ = rag_->Numeric<T>(size_, 0., 1., null_probability_);
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_temporal_type<T>::value && !std::is_same<T, DayTimeIntervalType>::value,
              Status>
  Visit(const T&) {
    auto max = std::numeric_limits<typename T::c_type>::max();
    auto min = std::numeric_limits<typename T::c_type>::lowest();
    auto values =
        rag_->Numeric<typename T::PhysicalType>(size_, min, max, null_probability_);
    return values->View(type_).Value(&out_);
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T& t) {
    int32_t min_length = 0;
    auto max_length = static_cast<int32_t>(std::sqrt(size_));

    if (t.layout().buffers[1].byte_width == sizeof(int32_t)) {
      out_ = rag_->String(size_, min_length, max_length, null_probability_);
    } else {
      out_ = rag_->LargeString(size_, min_length, max_length, null_probability_);
    }
    return out_->View(type_).Value(&out_);
  }

  template <typename T>
  enable_if_fixed_size_binary<T, Status> Visit(const T& t) {
    const int32_t value_size = t.byte_width();
    int64_t data_nbytes = size_ * value_size;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> data, AllocateBuffer(data_nbytes));
    random_bytes(data_nbytes, /*seed=*/0, data->mutable_data());
    auto validity = rag_->Boolean(size_, 1 - null_probability_);

    // Assemble the data for a FixedSizeBinaryArray
    auto values_data = std::make_shared<ArrayData>(type_, size_);
    values_data->buffers = {validity->data()->buffers[1], data};
    out_ = MakeArray(values_data);
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("generation of random arrays of type ", t);
  }

  std::shared_ptr<Array> Finish() && {
    DCHECK_OK(VisitTypeInline(*type_, this));
    return std::move(out_);
  }

  RandomArrayGenerator* rag_;
  const std::shared_ptr<DataType>& type_;
  int64_t size_;
  double null_probability_;
  std::shared_ptr<Array> out_;
};

}  // namespace

std::shared_ptr<Array> RandomArrayGenerator::ArrayOf(std::shared_ptr<DataType> type,
                                                     int64_t size,
                                                     double null_probability) {
  return RandomArrayGeneratorOfImpl{this, type, size, null_probability, nullptr}.Finish();
}

}  // namespace random
}  // namespace arrow
