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

#include "benchmark/benchmark.h"

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_dict.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/byte_stream_split.h"
#include "arrow/visit_data_inline.h"

#include "parquet/encoding.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

#include <cmath>
#include <limits>
#include <random>

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace {

// The min/max number of values used to drive each family of encoding benchmarks
constexpr int MIN_RANGE = 1024;
constexpr int MAX_RANGE = 65536;
}  // namespace

namespace parquet {

using schema::PrimitiveNode;

std::shared_ptr<ColumnDescriptor> Int64Schema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, Type::INT64);
  return std::make_shared<ColumnDescriptor>(node, repetition != Repetition::REQUIRED,
                                            repetition == Repetition::REPEATED);
}

static void BM_PlainEncodingBoolean(benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  auto encoder = MakeEncoder(Type::BOOLEAN, Encoding::PLAIN);
  auto typed_encoder = dynamic_cast<BooleanEncoder*>(encoder.get());

  for (auto _ : state) {
    typed_encoder->Put(values, static_cast<int>(values.size()));
    typed_encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
}

BENCHMARK(BM_PlainEncodingBoolean)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainDecodingBoolean(benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  bool* output = new bool[state.range(0)];
  auto encoder = MakeEncoder(Type::BOOLEAN, Encoding::PLAIN);
  auto typed_encoder = dynamic_cast<BooleanEncoder*>(encoder.get());
  typed_encoder->Put(values, static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<BooleanType>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(output, static_cast<int>(values.size()));
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
  delete[] output;
}

BENCHMARK(BM_PlainDecodingBoolean)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainEncodingInt64(benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  auto encoder = MakeTypedEncoder<Int64Type>(Encoding::PLAIN);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainEncodingInt64)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainDecodingInt64(benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  auto encoder = MakeTypedEncoder<Int64Type>(Encoding::PLAIN);
  encoder->Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<Int64Type>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainDecodingInt64)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainEncodingDouble(benchmark::State& state) {
  std::vector<double> values(state.range(0), 64.0);
  auto encoder = MakeTypedEncoder<DoubleType>(Encoding::PLAIN);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(double));
}

BENCHMARK(BM_PlainEncodingDouble)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainEncodingDoubleNaN(benchmark::State& state) {
  std::vector<double> values(state.range(0), nan(""));
  auto encoder = MakeTypedEncoder<DoubleType>(Encoding::PLAIN);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(double));
}

BENCHMARK(BM_PlainEncodingDoubleNaN)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainDecodingDouble(benchmark::State& state) {
  std::vector<double> values(state.range(0), 64.0);
  auto encoder = MakeTypedEncoder<DoubleType>(Encoding::PLAIN);
  encoder->Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<DoubleType>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(double));
}

BENCHMARK(BM_PlainDecodingDouble)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainEncodingFloat(benchmark::State& state) {
  std::vector<float> values(state.range(0), 64.0);
  auto encoder = MakeTypedEncoder<FloatType>(Encoding::PLAIN);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(float));
}

BENCHMARK(BM_PlainEncodingFloat)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainEncodingFloatNaN(benchmark::State& state) {
  std::vector<float> values(state.range(0), nanf(""));
  auto encoder = MakeTypedEncoder<FloatType>(Encoding::PLAIN);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(float));
}

BENCHMARK(BM_PlainEncodingFloatNaN)->Range(MIN_RANGE, MAX_RANGE);

static void BM_PlainDecodingFloat(benchmark::State& state) {
  std::vector<float> values(state.range(0), 64.0);
  auto encoder = MakeTypedEncoder<FloatType>(Encoding::PLAIN);
  encoder->Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<FloatType>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(float));
}

BENCHMARK(BM_PlainDecodingFloat)->Range(MIN_RANGE, MAX_RANGE);

template <typename ParquetType>
struct BM_SpacedEncodingTraits {
  using ArrowType = typename EncodingTraits<ParquetType>::ArrowType;
  using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;
  using CType = typename ParquetType::c_type;
};

template <>
struct BM_SpacedEncodingTraits<BooleanType> {
  // Leverage UInt8 vector array data for Boolean, the input src of PutSpaced is bool*
  using ArrowType = ::arrow::UInt8Type;
  using ArrayType = ::arrow::UInt8Array;
  using CType = bool;
};

static void BM_SpacedArgs(benchmark::internal::Benchmark* bench) {
  constexpr auto kPlainSpacedSize = 32 * 1024;  // 32k

  bench->Args({/*size*/ kPlainSpacedSize, /*null_in_ten_thousand*/ 1});
  bench->Args({/*size*/ kPlainSpacedSize, /*null_in_ten_thousand*/ 100});
  bench->Args({/*size*/ kPlainSpacedSize, /*null_in_ten_thousand*/ 1000});
  bench->Args({/*size*/ kPlainSpacedSize, /*null_in_ten_thousand*/ 5000});
  bench->Args({/*size*/ kPlainSpacedSize, /*null_in_ten_thousand*/ 10000});
}

template <typename ParquetType>
static void BM_EncodingSpaced(benchmark::State& state, Encoding::type encoding) {
  using ArrowType = typename BM_SpacedEncodingTraits<ParquetType>::ArrowType;
  using ArrayType = typename BM_SpacedEncodingTraits<ParquetType>::ArrayType;
  using CType = typename BM_SpacedEncodingTraits<ParquetType>::CType;

  const int num_values = static_cast<int>(state.range(0));
  const double null_percent = static_cast<double>(state.range(1)) / 10000.0;

  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  const auto array = rand.Numeric<ArrowType>(num_values, -100, 100, null_percent);
  const auto valid_bits = array->null_bitmap_data();
  const auto array_actual = ::arrow::internal::checked_pointer_cast<ArrayType>(array);
  const auto raw_values = array_actual->raw_values();
  // Guarantee the type cast between raw_values and input of PutSpaced.
  static_assert(sizeof(CType) == sizeof(*raw_values), "Type mismatch");
  // Cast only happens for BooleanType as it use UInt8 for the array data to match a bool*
  // input to PutSpaced.
  const auto src = reinterpret_cast<const CType*>(raw_values);

  auto encoder = MakeTypedEncoder<ParquetType>(encoding);
  for (auto _ : state) {
    encoder->PutSpaced(src, num_values, valid_bits, 0);
    encoder->FlushValues();
  }
  state.counters["null_percent"] = null_percent * 100;
  state.SetBytesProcessed(state.iterations() * num_values * sizeof(CType));
}

template <>
void BM_EncodingSpaced<BooleanType>(benchmark::State& state, Encoding::type encoding) {
  using CType = bool;

  const int num_values = static_cast<int>(state.range(0));
  const double null_percent = static_cast<double>(state.range(1)) / 10000.0;

  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  const auto array = rand.Boolean(num_values, 0.5, null_percent);
  const auto valid_bits = array->null_bitmap_data();
  bool* output = new bool[state.range(0)];
  int output_idx = 0;
  PARQUET_THROW_NOT_OK(::arrow::VisitArraySpanInline<::arrow::BooleanType>(
      *array->data(),
      [&](bool value) {
        output[output_idx] = value;
        ++output_idx;
        return ::arrow::Status::OK();
      },
      []() { return ::arrow::Status::OK(); }));

  auto encoder = MakeTypedEncoder<BooleanType>(encoding);
  for (auto _ : state) {
    encoder->PutSpaced(output, num_values, valid_bits, 0);
    encoder->FlushValues();
  }
  state.counters["null_percent"] = null_percent * 100;
  state.SetBytesProcessed(state.iterations() * num_values * sizeof(CType));
  delete[] output;
}

template <typename ParquetType>
static void BM_PlainEncodingSpaced(benchmark::State& state) {
  BM_EncodingSpaced<ParquetType>(state, Encoding::PLAIN);
}

static void BM_PlainEncodingSpacedBoolean(benchmark::State& state) {
  BM_PlainEncodingSpaced<BooleanType>(state);
}
BENCHMARK(BM_PlainEncodingSpacedBoolean)->Apply(BM_SpacedArgs);

static void BM_PlainEncodingSpacedFloat(benchmark::State& state) {
  BM_PlainEncodingSpaced<FloatType>(state);
}
BENCHMARK(BM_PlainEncodingSpacedFloat)->Apply(BM_SpacedArgs);

static void BM_PlainEncodingSpacedDouble(benchmark::State& state) {
  BM_PlainEncodingSpaced<DoubleType>(state);
}
BENCHMARK(BM_PlainEncodingSpacedDouble)->Apply(BM_SpacedArgs);

template <typename ParquetType>
static void BM_DecodingSpaced(benchmark::State& state, Encoding::type encoding) {
  using ArrowType = typename BM_SpacedEncodingTraits<ParquetType>::ArrowType;
  using ArrayType = typename BM_SpacedEncodingTraits<ParquetType>::ArrayType;
  using CType = typename BM_SpacedEncodingTraits<ParquetType>::CType;

  const int num_values = static_cast<int>(state.range(0));
  const auto null_percent = static_cast<double>(state.range(1)) / 10000.0;

  auto rand = ::arrow::random::RandomArrayGenerator(1923);
  std::shared_ptr<::arrow::Array> array;
  if constexpr (std::is_same_v<ParquetType, BooleanType>) {
    array = rand.Boolean(num_values, /*true_probability*/ 0.5, null_percent);
  } else {
    array = rand.Numeric<ArrowType>(num_values, -100, 100, null_percent);
  }
  const auto valid_bits = array->null_bitmap_data();
  const int null_count = static_cast<int>(array->null_count());
  const auto array_actual = ::arrow::internal::checked_pointer_cast<ArrayType>(array);

  auto encoder = MakeTypedEncoder<ParquetType>(encoding);
  encoder->Put(*array);
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  auto decoder = MakeTypedDecoder<ParquetType>(encoding);
  std::vector<uint8_t> decode_values(num_values * sizeof(CType));
  auto decode_buf = reinterpret_cast<CType*>(decode_values.data());
  for (auto _ : state) {
    decoder->SetData(num_values - null_count, buf->data(), static_cast<int>(buf->size()));
    decoder->DecodeSpaced(decode_buf, num_values, null_count, valid_bits, 0);
  }
  state.counters["null_percent"] = null_percent * 100;
  state.SetBytesProcessed(state.iterations() * num_values * sizeof(CType));
}

template <typename ParquetType>
static void BM_PlainDecodingSpaced(benchmark::State& state) {
  BM_DecodingSpaced<ParquetType>(state, Encoding::PLAIN);
}

static void BM_PlainDecodingSpacedBoolean(benchmark::State& state) {
  BM_PlainDecodingSpaced<BooleanType>(state);
}
BENCHMARK(BM_PlainDecodingSpacedBoolean)->Apply(BM_SpacedArgs);

static void BM_PlainDecodingSpacedFloat(benchmark::State& state) {
  BM_PlainDecodingSpaced<FloatType>(state);
}
BENCHMARK(BM_PlainDecodingSpacedFloat)->Apply(BM_SpacedArgs);

static void BM_PlainDecodingSpacedDouble(benchmark::State& state) {
  BM_PlainDecodingSpaced<DoubleType>(state);
}
BENCHMARK(BM_PlainDecodingSpacedDouble)->Apply(BM_SpacedArgs);

template <typename T, typename DecodeFunc>
static void BM_ByteStreamSplitDecode(benchmark::State& state, DecodeFunc&& decode_func) {
  std::vector<T> values(state.range(0), 64.0);
  const uint8_t* values_raw = reinterpret_cast<const uint8_t*>(values.data());
  std::vector<T> output(state.range(0), 0);

  for (auto _ : state) {
    decode_func(values_raw, static_cast<int64_t>(values.size()),
                static_cast<int64_t>(values.size()), output.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(T));
}

template <typename T, typename EncodeFunc>
static void BM_ByteStreamSplitEncode(benchmark::State& state, EncodeFunc&& encode_func) {
  std::vector<T> values(state.range(0), 64.0);
  const uint8_t* values_raw = reinterpret_cast<const uint8_t*>(values.data());
  std::vector<uint8_t> output(state.range(0) * sizeof(T), 0);

  for (auto _ : state) {
    encode_func(values_raw, values.size(), output.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(T));
}

static void BM_ByteStreamSplitDecode_Float_Scalar(benchmark::State& state) {
  BM_ByteStreamSplitDecode<float>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeScalar<float>);
}

static void BM_ByteStreamSplitDecode_Double_Scalar(benchmark::State& state) {
  BM_ByteStreamSplitDecode<double>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeScalar<double>);
}

static void BM_ByteStreamSplitEncode_Float_Scalar(benchmark::State& state) {
  BM_ByteStreamSplitEncode<float>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeScalar<float>);
}

static void BM_ByteStreamSplitEncode_Double_Scalar(benchmark::State& state) {
  BM_ByteStreamSplitEncode<double>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeScalar<double>);
}

BENCHMARK(BM_ByteStreamSplitDecode_Float_Scalar)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitDecode_Double_Scalar)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Float_Scalar)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Double_Scalar)->Range(MIN_RANGE, MAX_RANGE);

#if defined(ARROW_HAVE_SSE4_2)
static void BM_ByteStreamSplitDecode_Float_Sse2(benchmark::State& state) {
  BM_ByteStreamSplitDecode<float>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeSse2<float>);
}

static void BM_ByteStreamSplitDecode_Double_Sse2(benchmark::State& state) {
  BM_ByteStreamSplitDecode<double>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeSse2<double>);
}

static void BM_ByteStreamSplitEncode_Float_Sse2(benchmark::State& state) {
  BM_ByteStreamSplitEncode<float>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeSse2<float>);
}

static void BM_ByteStreamSplitEncode_Double_Sse2(benchmark::State& state) {
  BM_ByteStreamSplitEncode<double>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeSse2<double>);
}

BENCHMARK(BM_ByteStreamSplitDecode_Float_Sse2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitDecode_Double_Sse2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Float_Sse2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Double_Sse2)->Range(MIN_RANGE, MAX_RANGE);
#endif

#if defined(ARROW_HAVE_AVX2)
static void BM_ByteStreamSplitDecode_Float_Avx2(benchmark::State& state) {
  BM_ByteStreamSplitDecode<float>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeAvx2<float>);
}

static void BM_ByteStreamSplitDecode_Double_Avx2(benchmark::State& state) {
  BM_ByteStreamSplitDecode<double>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeAvx2<double>);
}

static void BM_ByteStreamSplitEncode_Float_Avx2(benchmark::State& state) {
  BM_ByteStreamSplitEncode<float>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeAvx2<float>);
}

static void BM_ByteStreamSplitEncode_Double_Avx2(benchmark::State& state) {
  BM_ByteStreamSplitEncode<double>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeAvx2<double>);
}

BENCHMARK(BM_ByteStreamSplitDecode_Float_Avx2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitDecode_Double_Avx2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Float_Avx2)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Double_Avx2)->Range(MIN_RANGE, MAX_RANGE);
#endif

#if defined(ARROW_HAVE_AVX512)
static void BM_ByteStreamSplitDecode_Float_Avx512(benchmark::State& state) {
  BM_ByteStreamSplitDecode<float>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeAvx512<float>);
}

static void BM_ByteStreamSplitDecode_Double_Avx512(benchmark::State& state) {
  BM_ByteStreamSplitDecode<double>(
      state, ::arrow::util::internal::ByteStreamSplitDecodeAvx512<double>);
}

static void BM_ByteStreamSplitEncode_Float_Avx512(benchmark::State& state) {
  BM_ByteStreamSplitEncode<float>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeAvx512<float>);
}

static void BM_ByteStreamSplitEncode_Double_Avx512(benchmark::State& state) {
  BM_ByteStreamSplitEncode<double>(
      state, ::arrow::util::internal::ByteStreamSplitEncodeAvx512<double>);
}

BENCHMARK(BM_ByteStreamSplitDecode_Float_Avx512)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitDecode_Double_Avx512)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Float_Avx512)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_ByteStreamSplitEncode_Double_Avx512)->Range(MIN_RANGE, MAX_RANGE);
#endif

template <typename DType>
static auto MakeDeltaBitPackingInputFixed(size_t length) {
  using T = typename DType::c_type;
  return std::vector<T>(length, 42);
}

template <typename DType>
static auto MakeDeltaBitPackingInputNarrow(size_t length) {
  using T = typename DType::c_type;
  auto numbers = std::vector<T>(length);
  ::arrow::randint<T, T>(length, 0, 1000, &numbers);
  return numbers;
}

template <typename DType>
static auto MakeDeltaBitPackingInputWide(size_t length) {
  using T = typename DType::c_type;
  auto numbers = std::vector<T>(length);
  ::arrow::randint<T, T>(length, std::numeric_limits<T>::min() >> 2,
                         std::numeric_limits<T>::max() >> 2, &numbers);
  return numbers;
}

template <typename DType, typename NumberGenerator>
static void BM_DeltaBitPackingEncode(benchmark::State& state, NumberGenerator gen) {
  using T = typename DType::c_type;
  std::vector<T> values = gen(state.range(0));
  auto encoder = MakeTypedEncoder<DType>(Encoding::DELTA_BINARY_PACKED);
  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(T));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_DeltaBitPackingEncode_Int32_Fixed(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int32Type>(state, MakeDeltaBitPackingInputFixed<Int32Type>);
}

static void BM_DeltaBitPackingEncode_Int64_Fixed(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int64Type>(state, MakeDeltaBitPackingInputFixed<Int64Type>);
}

static void BM_DeltaBitPackingEncode_Int32_Narrow(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int32Type>(state, MakeDeltaBitPackingInputNarrow<Int32Type>);
}

static void BM_DeltaBitPackingEncode_Int64_Narrow(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int64Type>(state, MakeDeltaBitPackingInputNarrow<Int64Type>);
}

static void BM_DeltaBitPackingEncode_Int32_Wide(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int32Type>(state, MakeDeltaBitPackingInputWide<Int32Type>);
}

static void BM_DeltaBitPackingEncode_Int64_Wide(benchmark::State& state) {
  BM_DeltaBitPackingEncode<Int64Type>(state, MakeDeltaBitPackingInputWide<Int64Type>);
}

BENCHMARK(BM_DeltaBitPackingEncode_Int32_Fixed)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingEncode_Int64_Fixed)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingEncode_Int32_Narrow)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingEncode_Int64_Narrow)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingEncode_Int32_Wide)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingEncode_Int64_Wide)->Range(MIN_RANGE, MAX_RANGE);

template <typename DType, typename NumberGenerator>
static void BM_DeltaBitPackingDecode(benchmark::State& state, NumberGenerator gen) {
  using T = typename DType::c_type;
  std::vector<T> values = gen(state.range(0));
  auto encoder = MakeTypedEncoder<DType>(Encoding::DELTA_BINARY_PACKED);
  encoder->Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  auto decoder = MakeTypedDecoder<DType>(Encoding::DELTA_BINARY_PACKED);
  for (auto _ : state) {
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(T));
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

static void BM_DeltaBitPackingDecode_Int32_Fixed(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int32Type>(state, MakeDeltaBitPackingInputFixed<Int32Type>);
}

static void BM_DeltaBitPackingDecode_Int64_Fixed(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int64Type>(state, MakeDeltaBitPackingInputFixed<Int64Type>);
}

static void BM_DeltaBitPackingDecode_Int32_Narrow(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int32Type>(state, MakeDeltaBitPackingInputNarrow<Int32Type>);
}

static void BM_DeltaBitPackingDecode_Int64_Narrow(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int64Type>(state, MakeDeltaBitPackingInputNarrow<Int64Type>);
}

static void BM_DeltaBitPackingDecode_Int32_Wide(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int32Type>(state, MakeDeltaBitPackingInputWide<Int32Type>);
}

static void BM_DeltaBitPackingDecode_Int64_Wide(benchmark::State& state) {
  BM_DeltaBitPackingDecode<Int64Type>(state, MakeDeltaBitPackingInputWide<Int64Type>);
}

BENCHMARK(BM_DeltaBitPackingDecode_Int32_Fixed)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingDecode_Int64_Fixed)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingDecode_Int32_Narrow)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingDecode_Int64_Narrow)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingDecode_Int32_Wide)->Range(MIN_RANGE, MAX_RANGE);
BENCHMARK(BM_DeltaBitPackingDecode_Int64_Wide)->Range(MIN_RANGE, MAX_RANGE);

static void ByteArrayCustomArguments(benchmark::internal::Benchmark* b) {
  b->ArgsProduct({{8, 64, 1024}, {512, 2048}})
      ->ArgNames({"max-string-length", "batch-size"});
}

void EncodingByteArrayBenchmark(benchmark::State& state, Encoding::type encoding) {
  ::arrow::random::RandomArrayGenerator rag(0);
  // Using arrow generator to generate random data.
  int32_t max_length = static_cast<int32_t>(state.range(0));
  int32_t array_size = static_cast<int32_t>(state.range(1));
  auto array =
      rag.String(/* size */ array_size, /* min_length */ 0, /* max_length */ max_length,
                 /* null_probability */ 0);
  const auto array_actual =
      ::arrow::internal::checked_pointer_cast<::arrow::StringArray>(array);
  auto encoder = MakeTypedEncoder<ByteArrayType>(encoding);
  std::vector<ByteArray> values;
  for (int i = 0; i < array_actual->length(); ++i) {
    values.emplace_back(array_actual->GetView(i));
  }

  for (auto _ : state) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetItemsProcessed(state.iterations() * array_actual->length());
  state.SetBytesProcessed(state.iterations() * (array_actual->value_data()->size() +
                                                array_actual->value_offsets()->size()));
}

static void BM_DeltaLengthEncodingByteArray(benchmark::State& state) {
  EncodingByteArrayBenchmark(state, Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

static void BM_PlainEncodingByteArray(benchmark::State& state) {
  EncodingByteArrayBenchmark(state, Encoding::PLAIN);
}

void DecodingByteArrayBenchmark(benchmark::State& state, Encoding::type encoding) {
  ::arrow::random::RandomArrayGenerator rag(0);
  int32_t max_length = static_cast<int32_t>(state.range(0));
  int32_t array_size = static_cast<int32_t>(state.range(1));
  // Using arrow to write, because we just benchmark decoding here.
  auto array =
      rag.String(/* size */ array_size, /* min_length */ 0, /* max_length */ max_length,
                 /* null_probability */ 0);
  const auto array_actual =
      ::arrow::internal::checked_pointer_cast<::arrow::StringArray>(array);
  auto encoder = MakeTypedEncoder<ByteArrayType>(encoding);
  encoder->Put(*array);
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  std::vector<ByteArray> values;
  values.resize(array->length());
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(encoding);
    decoder->SetData(static_cast<int>(array->length()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
    ::benchmark::DoNotOptimize(values);
  }
  state.SetItemsProcessed(state.iterations() * array->length());
  state.SetBytesProcessed(state.iterations() * (array_actual->value_data()->size() +
                                                array_actual->value_offsets()->size()));
}

static void BM_PlainDecodingByteArray(benchmark::State& state) {
  DecodingByteArrayBenchmark(state, Encoding::PLAIN);
}

static void BM_DeltaLengthDecodingByteArray(benchmark::State& state) {
  DecodingByteArrayBenchmark(state, Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

BENCHMARK(BM_PlainEncodingByteArray)->Apply(ByteArrayCustomArguments);
BENCHMARK(BM_DeltaLengthEncodingByteArray)->Apply(ByteArrayCustomArguments);
BENCHMARK(BM_PlainDecodingByteArray)->Apply(ByteArrayCustomArguments);
BENCHMARK(BM_DeltaLengthDecodingByteArray)->Apply(ByteArrayCustomArguments);

static void BM_DecodingByteArraySpaced(benchmark::State& state, Encoding::type encoding) {
  const double null_percent = 0.02;

  auto rand = ::arrow::random::RandomArrayGenerator(0);
  int32_t max_length = static_cast<int32_t>(state.range(0));
  int32_t num_values = static_cast<int32_t>(state.range(1));
  const auto array = rand.String(num_values, /* min_length */ 0,
                                 /* max_length */ max_length, null_percent);
  const auto valid_bits = array->null_bitmap_data();
  const int null_count = static_cast<int>(array->null_count());
  const auto array_actual =
      ::arrow::internal::checked_pointer_cast<::arrow::StringArray>(array);

  std::vector<ByteArray> byte_arrays;
  byte_arrays.reserve(array_actual->length());
  for (int i = 0; i < array_actual->length(); ++i) {
    byte_arrays.emplace_back(array_actual->GetView(i));
  }

  auto encoder = MakeTypedEncoder<ByteArrayType>(encoding);
  encoder->PutSpaced(byte_arrays.data(), num_values, valid_bits, 0);
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  auto decoder = MakeTypedDecoder<ByteArrayType>(encoding);
  std::vector<uint8_t> decode_values(num_values * sizeof(ByteArray));
  auto decode_buf = reinterpret_cast<ByteArray*>(decode_values.data());
  for (auto _ : state) {
    decoder->SetData(num_values - null_count, buf->data(), static_cast<int>(buf->size()));
    decoder->DecodeSpaced(decode_buf, num_values, null_count, valid_bits, 0);
    ::benchmark::DoNotOptimize(decode_buf);
  }
  state.counters["null_percent"] = null_percent * 100;
  state.SetItemsProcessed(state.iterations() * array_actual->length());
  state.SetBytesProcessed(state.iterations() * (array_actual->value_data()->size() +
                                                array_actual->value_offsets()->size()));
}

static void BM_PlainDecodingSpacedByteArray(benchmark::State& state) {
  BM_DecodingByteArraySpaced(state, Encoding::PLAIN);
}

static void BM_DeltaLengthDecodingSpacedByteArray(benchmark::State& state) {
  BM_DecodingByteArraySpaced(state, Encoding::DELTA_LENGTH_BYTE_ARRAY);
}

BENCHMARK(BM_PlainDecodingSpacedByteArray)->Apply(ByteArrayCustomArguments);
BENCHMARK(BM_DeltaLengthDecodingSpacedByteArray)->Apply(ByteArrayCustomArguments);

static void BM_RleEncodingBoolean(benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  auto encoder = MakeEncoder(Type::BOOLEAN, Encoding::RLE);
  auto typed_encoder = dynamic_cast<BooleanEncoder*>(encoder.get());

  for (auto _ : state) {
    typed_encoder->Put(values, static_cast<int>(values.size()));
    typed_encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
}

BENCHMARK(BM_RleEncodingBoolean)->Range(MIN_RANGE, MAX_RANGE);

static void BM_RleDecodingBoolean(benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  bool* output = new bool[state.range(0)];
  auto encoder = MakeEncoder(Type::BOOLEAN, Encoding::RLE);
  auto typed_encoder = dynamic_cast<BooleanEncoder*>(encoder.get());
  typed_encoder->Put(values, static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  auto decoder = MakeTypedDecoder<BooleanType>(Encoding::RLE);
  for (auto _ : state) {
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(output, static_cast<int>(values.size()));
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
  delete[] output;
}

BENCHMARK(BM_RleDecodingBoolean)->Range(MIN_RANGE, MAX_RANGE);

static void BM_RleEncodingSpacedBoolean(benchmark::State& state) {
  BM_EncodingSpaced<BooleanType>(state, Encoding::RLE);
}
BENCHMARK(BM_RleEncodingSpacedBoolean)->Apply(BM_SpacedArgs);

static void BM_RleDecodingSpacedBoolean(benchmark::State& state) {
  BM_DecodingSpaced<BooleanType>(state, Encoding::RLE);
}
BENCHMARK(BM_RleDecodingSpacedBoolean)->Apply(BM_SpacedArgs);

template <typename Type>
static void DecodeDict(std::vector<typename Type::c_type>& values,
                       benchmark::State& state) {
  typedef typename Type::c_type T;
  int num_values = static_cast<int>(values.size());

  MemoryPool* allocator = default_memory_pool();
  std::shared_ptr<ColumnDescriptor> descr = Int64Schema(Repetition::REQUIRED);

  auto base_encoder =
      MakeEncoder(Type::type_num, Encoding::PLAIN, true, descr.get(), allocator);
  auto encoder =
      dynamic_cast<typename EncodingTraits<Type>::Encoder*>(base_encoder.get());
  auto dict_traits = dynamic_cast<DictEncoder<Type>*>(base_encoder.get());
  encoder->Put(values.data(), num_values);

  std::shared_ptr<ResizableBuffer> dict_buffer =
      AllocateBuffer(allocator, dict_traits->dict_encoded_size());

  std::shared_ptr<ResizableBuffer> indices =
      AllocateBuffer(allocator, encoder->EstimatedDataEncodedSize());

  dict_traits->WriteDict(dict_buffer->mutable_data());
  int actual_bytes = dict_traits->WriteIndices(indices->mutable_data(),
                                               static_cast<int>(indices->size()));

  PARQUET_THROW_NOT_OK(indices->Resize(actual_bytes));

  for (auto _ : state) {
    auto dict_decoder = MakeTypedDecoder<Type>(Encoding::PLAIN, descr.get());
    dict_decoder->SetData(dict_traits->num_entries(), dict_buffer->data(),
                          static_cast<int>(dict_buffer->size()));

    auto decoder = MakeDictDecoder<Type>(descr.get());
    decoder->SetDict(dict_decoder.get());
    decoder->SetData(num_values, indices->data(), static_cast<int>(indices->size()));
    decoder->Decode(values.data(), num_values);
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(T));
}

static void BM_DictDecodingInt64_repeats(benchmark::State& state) {
  typedef Int64Type Type;
  typedef typename Type::c_type T;

  std::vector<T> values(state.range(0), 64);
  DecodeDict<Type>(values, state);
}

BENCHMARK(BM_DictDecodingInt64_repeats)->Range(MIN_RANGE, MAX_RANGE);

static void BM_DictDecodingInt64_literals(benchmark::State& state) {
  typedef Int64Type Type;
  typedef typename Type::c_type T;

  std::vector<T> values(state.range(0));
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = i;
  }
  DecodeDict<Type>(values, state);
}

BENCHMARK(BM_DictDecodingInt64_literals)->Range(MIN_RANGE, MAX_RANGE);

static void BM_DictDecodingByteArray(benchmark::State& state) {
  ::arrow::random::RandomArrayGenerator rag(0);
  // Using arrow generator to generate random data.
  int32_t max_length = static_cast<int32_t>(state.range(0));
  int32_t array_size = static_cast<int32_t>(state.range(1));
  auto array =
      rag.String(/* size */ array_size, /* min_length */ 0, /* max_length */ max_length,
                 /* null_probability */ 0);
  const auto array_actual =
      ::arrow::internal::checked_pointer_cast<::arrow::StringArray>(array);
  auto encoder = MakeDictDecoder<ByteArrayType>();
  std::vector<ByteArray> values;
  for (int i = 0; i < array_actual->length(); ++i) {
    values.emplace_back(array_actual->GetView(i));
  }
  DecodeDict<ByteArrayType>(values, state);
  state.SetItemsProcessed(state.iterations() * array_actual->length());
  state.SetBytesProcessed(state.iterations() * (array_actual->value_data()->size() +
                                                array_actual->value_offsets()->size()));
}

BENCHMARK(BM_DictDecodingByteArray)->Apply(ByteArrayCustomArguments);

// ----------------------------------------------------------------------
// Shared benchmarks for decoding using arrow builders

using ::arrow::BinaryBuilder;
using ::arrow::BinaryDictionary32Builder;

class BenchmarkDecodeArrow : public ::benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    num_values_ = static_cast<int>(state.range());
    InitDataInputs();
    DoEncodeArrow();
  }

  void TearDown(const ::benchmark::State& state) override {
    buffer_.reset();
    input_array_.reset();
    values_.clear();
  }

  void InitDataInputs() {
    // Generate a random string dictionary without any nulls so that this dataset can
    // be used for benchmarking the DecodeArrowNonNull API
    constexpr int repeat_factor = 8;
    constexpr int64_t min_length = 2;
    constexpr int64_t max_length = 10;
    ::arrow::random::RandomArrayGenerator rag(0);
    input_array_ = rag.StringWithRepeats(num_values_, num_values_ / repeat_factor,
                                         min_length, max_length, /*null_probability=*/0);
    valid_bits_ = input_array_->null_bitmap_data();
    total_size_ = input_array_->data()->buffers[2]->size();

    values_.reserve(num_values_);
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(*input_array_);
    for (int64_t i = 0; i < binary_array.length(); i++) {
      auto view = binary_array.GetView(i);
      values_.emplace_back(static_cast<uint32_t>(view.length()),
                           reinterpret_cast<const uint8_t*>(view.data()));
    }
  }

  virtual void DoEncodeArrow() = 0;
  virtual void DoEncodeLowLevel() = 0;

  virtual std::unique_ptr<ByteArrayDecoder> InitializeDecoder() = 0;

  void EncodeArrowBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      DoEncodeArrow();
    }
    state.SetBytesProcessed(state.iterations() * total_size_);
  }

  void EncodeLowLevelBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      DoEncodeLowLevel();
    }
    state.SetBytesProcessed(state.iterations() * total_size_);
  }

  void DecodeArrowDenseBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      auto decoder = InitializeDecoder();
      typename EncodingTraits<ByteArrayType>::Accumulator acc;
      acc.builder.reset(new BinaryBuilder);
      decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &acc);
    }
    state.SetBytesProcessed(state.iterations() * total_size_);
  }

  void DecodeArrowNonNullDenseBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      auto decoder = InitializeDecoder();
      typename EncodingTraits<ByteArrayType>::Accumulator acc;
      acc.builder.reset(new BinaryBuilder);
      decoder->DecodeArrowNonNull(num_values_, &acc);
    }
    state.SetBytesProcessed(state.iterations() * total_size_);
  }

  void DecodeArrowDictBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      auto decoder = InitializeDecoder();
      BinaryDictionary32Builder builder(default_memory_pool());
      decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &builder);
    }

    state.SetBytesProcessed(state.iterations() * total_size_);
  }

  void DecodeArrowNonNullDictBenchmark(benchmark::State& state) {
    for (auto _ : state) {
      auto decoder = InitializeDecoder();
      BinaryDictionary32Builder builder(default_memory_pool());
      decoder->DecodeArrowNonNull(num_values_, &builder);
    }

    state.SetBytesProcessed(state.iterations() * total_size_);
  }

 protected:
  int num_values_;
  std::shared_ptr<::arrow::Array> input_array_;
  std::vector<ByteArray> values_;
  uint64_t total_size_;
  const uint8_t* valid_bits_;
  std::shared_ptr<Buffer> buffer_;
};

// ----------------------------------------------------------------------
// Benchmark Decoding from Plain Encoding
class BM_ArrowBinaryPlain : public BenchmarkDecodeArrow {
 public:
  void DoEncodeArrow() override {
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    encoder->Put(*input_array_);
    buffer_ = encoder->FlushValues();
  }

  void DoEncodeLowLevel() override {
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    encoder->Put(values_.data(), num_values_);
    buffer_ = encoder->FlushValues();
  }

  std::unique_ptr<ByteArrayDecoder> InitializeDecoder() override {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    return decoder;
  }
};

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, EncodeArrow)
(benchmark::State& state) { EncodeArrowBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, EncodeArrow)->Range(1 << 18, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, EncodeLowLevel)
(benchmark::State& state) { EncodeLowLevelBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, EncodeLowLevel)->Range(1 << 18, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, DecodeArrow_Dense)
(benchmark::State& state) { DecodeArrowDenseBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, DecodeArrow_Dense)->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, DecodeArrowNonNull_Dense)
(benchmark::State& state) { DecodeArrowNonNullDenseBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, DecodeArrowNonNull_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, DecodeArrow_Dict)
(benchmark::State& state) { DecodeArrowDictBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, DecodeArrow_Dict)->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryPlain, DecodeArrowNonNull_Dict)
(benchmark::State& state) { DecodeArrowNonNullDictBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryPlain, DecodeArrowNonNull_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

// ----------------------------------------------------------------------
// Benchmark Decoding from Dictionary Encoding
class BM_ArrowBinaryDict : public BenchmarkDecodeArrow {
 public:
  template <typename PutValuesFunc>
  void DoEncode(PutValuesFunc&& put_values) {
    auto node = schema::ByteArray("name");
    descr_ = std::make_unique<ColumnDescriptor>(node, 0, 0);

    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN,
                                                   /*use_dictionary=*/true, descr_.get());
    put_values(encoder.get());
    buffer_ = encoder->FlushValues();

    auto dict_encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(encoder.get());
    ASSERT_NE(dict_encoder, nullptr);
    dict_buffer_ =
        AllocateBuffer(default_memory_pool(), dict_encoder->dict_encoded_size());
    dict_encoder->WriteDict(dict_buffer_->mutable_data());
    num_dict_entries_ = dict_encoder->num_entries();
  }

  template <typename IndexType>
  void EncodeDictBenchmark(benchmark::State& state) {
    constexpr int64_t nunique = 100;
    constexpr int64_t min_length = 32;
    constexpr int64_t max_length = 32;
    ::arrow::random::RandomArrayGenerator rag(0);
    auto dict = rag.String(nunique, min_length, max_length,
                           /*null_probability=*/0);
    auto indices = rag.Numeric<IndexType, int32_t>(num_values_, 0, nunique - 1);

    auto PutValues = [&](ByteArrayEncoder* encoder) {
      auto dict_encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(encoder);
      dict_encoder->PutDictionary(*dict);
      dict_encoder->PutIndices(*indices);
    };
    for (auto _ : state) {
      DoEncode(std::move(PutValues));
    }
    state.SetItemsProcessed(state.iterations() * num_values_);
  }

  void DoEncodeArrow() override {
    auto PutValues = [&](ByteArrayEncoder* encoder) {
      ASSERT_NO_THROW(encoder->Put(*input_array_));
    };
    DoEncode(std::move(PutValues));
  }

  void DoEncodeLowLevel() override {
    auto PutValues = [&](ByteArrayEncoder* encoder) {
      encoder->Put(values_.data(), num_values_);
    };
    DoEncode(std::move(PutValues));
  }

  std::unique_ptr<ByteArrayDecoder> InitializeDecoder() override {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    decoder->SetData(num_dict_entries_, dict_buffer_->data(),
                     static_cast<int>(dict_buffer_->size()));
    auto dict_decoder = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder->SetDict(decoder.get());
    dict_decoder->SetData(num_values_, buffer_->data(),
                          static_cast<int>(buffer_->size()));
    return std::unique_ptr<ByteArrayDecoder>(
        dynamic_cast<ByteArrayDecoder*>(dict_decoder.release()));
  }

  void TearDown(const ::benchmark::State& state) override {
    BenchmarkDecodeArrow::TearDown(state);
    dict_buffer_.reset();
    descr_.reset();
  }

 protected:
  std::unique_ptr<ColumnDescriptor> descr_;
  std::shared_ptr<Buffer> dict_buffer_;
  int num_dict_entries_;
};

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeArrow)
(benchmark::State& state) { EncodeArrowBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeArrow)->Range(1 << 18, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeDictDirectInt8)
(benchmark::State& state) { EncodeDictBenchmark<::arrow::Int8Type>(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeDictDirectInt8)->Range(1 << 20, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeDictDirectInt16)
(benchmark::State& state) { EncodeDictBenchmark<::arrow::Int16Type>(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeDictDirectInt16)->Range(1 << 20, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeDictDirectInt32)
(benchmark::State& state) { EncodeDictBenchmark<::arrow::Int32Type>(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeDictDirectInt32)->Range(1 << 20, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeDictDirectInt64)
(benchmark::State& state) { EncodeDictBenchmark<::arrow::Int64Type>(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeDictDirectInt64)->Range(1 << 20, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, EncodeLowLevel)
(benchmark::State& state) { EncodeLowLevelBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, EncodeLowLevel)->Range(1 << 18, 1 << 20);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, DecodeArrow_Dense)(benchmark::State& state) {
  DecodeArrowDenseBenchmark(state);
}
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, DecodeArrow_Dense)->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, DecodeArrowNonNull_Dense)
(benchmark::State& state) { DecodeArrowNonNullDenseBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, DecodeArrowNonNull_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, DecodeArrow_Dict)
(benchmark::State& state) { DecodeArrowDictBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, DecodeArrow_Dict)->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_ArrowBinaryDict, DecodeArrowNonNull_Dict)
(benchmark::State& state) { DecodeArrowNonNullDictBenchmark(state); }
BENCHMARK_REGISTER_F(BM_ArrowBinaryDict, DecodeArrowNonNull_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

}  // namespace parquet
