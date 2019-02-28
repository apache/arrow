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
#include "arrow/testing/util.h"
#include "arrow/type.h"

#include "parquet/encoding.h"
#include "parquet/schema.h"
#include "parquet/util/memory.h"

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

std::shared_ptr<::arrow::Array> MakeRandomStringsWithRepeats(size_t num_unique,
                                                             size_t num_values) {
  const int64_t min_length = 2;
  const int64_t max_length = 10;
  std::vector<uint8_t> buffer(max_length);
  std::vector<std::string> dictionary(num_unique);

  uint32_t seed = 0;
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int64_t> length_dist(min_length, max_length);

  std::generate(dictionary.begin(), dictionary.end(), [&] {
    auto length = length_dist(gen);
    ::arrow::random_ascii(length, seed++, buffer.data());
    return std::string(buffer.begin(), buffer.begin() + length);
  });

  std::uniform_int_distribution<int64_t> indices_dist(0, num_unique - 1);
  ::arrow::StringBuilder builder;
  ABORT_NOT_OK(builder.Reserve(num_values));

  for (size_t i = 0; i < num_values; i++) {
    const auto index = indices_dist(gen);
    const auto value = dictionary[index];
    ABORT_NOT_OK(builder.Append(value));
  }

  std::shared_ptr<::arrow::Array> result;
  ABORT_NOT_OK(builder.Finish(&result));
  return result;
}

// ----------------------------------------------------------------------
// Shared benchmarks for decoding using arrow builders
class BenchmarkDecodeArrow : public ::benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    num_values_ = static_cast<int>(state.range());
    InitDataInputs();
    DoEncodeData();
  }

  void TearDown(const ::benchmark::State& state) override {}

  void InitDataInputs() {
    constexpr int repeat_factor = 8;
    input_array_ = MakeRandomStringsWithRepeats(num_values_ / repeat_factor, num_values_);
    valid_bits_ = input_array_->null_bitmap()->data();
    values_ = std::vector<ByteArray>();
    values_.reserve(num_values_);
    total_size_ = 0;
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(*input_array_);

    for (int64_t i = 0; i < binary_array.length(); i++) {
      auto view = binary_array.GetView(i);
      values_.emplace_back(static_cast<uint32_t>(view.length()),
                           reinterpret_cast<const uint8_t*>(view.data()));
      total_size_ += view.length();
    }
  }

  virtual void DoEncodeData() = 0;

 protected:
  int num_values_;
  std::shared_ptr<::arrow::Array> input_array_;
  uint64_t total_size_;
  std::vector<ByteArray> values_;
  const uint8_t* valid_bits_;
  std::shared_ptr<Buffer> buffer_;
};

// ----------------------------------------------------------------------
// Benchmark Decoding from Plain Encoding
class BM_PlainDecodingByteArray : public BenchmarkDecodeArrow {
 public:
  void DoEncodeData() override {
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    encoder->Put(values_.data(), num_values_);
    buffer_ = encoder->FlushValues();
  }
};

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrow_Dense)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::internal::ChunkedBinaryBuilder builder(static_cast<int>(buffer_->size()),
                                                    ::arrow::default_memory_pool());
    decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrow_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dense)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::internal::ChunkedBinaryBuilder builder(static_cast<int>(buffer_->size()),
                                                    ::arrow::default_memory_pool());
    decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrow_Dict)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrow_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dict)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

// ----------------------------------------------------------------------
// Benchmark Decoding from Dictionary Encoding
class BM_DictDecodingByteArray : public BenchmarkDecodeArrow {
 public:
  void DoEncodeData() override {
    auto node = schema::ByteArray("name");
    descr_ = std::unique_ptr<ColumnDescriptor>(new ColumnDescriptor(node, 0, 0));
    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN,
                                                   /*use_dictionary=*/true, descr_.get());
    ASSERT_NO_THROW(encoder->Put(values_.data(), num_values_));
    buffer_ = encoder->FlushValues();

    auto dict_encoder = dynamic_cast<DictEncoder<ByteArrayType>*>(encoder.get());
    ASSERT_NE(dict_encoder, nullptr);
    dict_buffer_ =
        AllocateBuffer(default_memory_pool(), dict_encoder->dict_encoded_size());
    dict_encoder->WriteDict(dict_buffer_->mutable_data());
    num_dict_entries_ = dict_encoder->num_entries();
  }

 protected:
  std::unique_ptr<ColumnDescriptor> descr_;
  std::unique_ptr<DictDecoder<ByteArrayType>> dict_decoder_;
  std::shared_ptr<Buffer> dict_buffer_;
  int num_dict_entries_;
};

BENCHMARK_DEFINE_F(BM_DictDecodingByteArray, DecodeArrow_Dense)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    decoder->SetData(num_dict_entries_, dict_buffer_->data(),
                     static_cast<int>(dict_buffer_->size()));
    auto dict_decoder = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder->SetDict(decoder.get());
    dict_decoder->SetData(num_values_, buffer_->data(),
                          static_cast<int>(buffer_->size()));
    auto byte_array_decoder = dynamic_cast<ByteArrayDecoder*>(dict_decoder.get());

    ::arrow::internal::ChunkedBinaryBuilder builder(
        static_cast<int>(dict_buffer_->size()), ::arrow::default_memory_pool());
    byte_array_decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_DictDecodingByteArray, DecodeArrow_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_DictDecodingByteArray, DecodeArrowNonNull_Dense)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    decoder->SetData(num_dict_entries_, dict_buffer_->data(),
                     static_cast<int>(dict_buffer_->size()));
    auto dict_decoder = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder->SetDict(decoder.get());
    dict_decoder->SetData(num_values_, buffer_->data(),
                          static_cast<int>(buffer_->size()));
    auto byte_array_decoder = dynamic_cast<ByteArrayDecoder*>(dict_decoder.get());

    ::arrow::internal::ChunkedBinaryBuilder builder(
        static_cast<int>(dict_buffer_->size()), ::arrow::default_memory_pool());
    byte_array_decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_DictDecodingByteArray, DecodeArrowNonNull_Dense)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_DictDecodingByteArray, DecodeArrow_Dict)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    decoder->SetData(num_dict_entries_, dict_buffer_->data(),
                     static_cast<int>(dict_buffer_->size()));
    auto dict_decoder = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder->SetDict(decoder.get());
    dict_decoder->SetData(num_values_, buffer_->data(),
                          static_cast<int>(buffer_->size()));
    auto byte_array_decoder = dynamic_cast<ByteArrayDecoder*>(dict_decoder.get());

    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    byte_array_decoder->DecodeArrow(num_values_, 0, valid_bits_, 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_DictDecodingByteArray, DecodeArrow_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

BENCHMARK_DEFINE_F(BM_DictDecodingByteArray, DecodeArrowNonNull_Dict)
(benchmark::State& state) {
  for (auto _ : state) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN, descr_.get());
    decoder->SetData(num_dict_entries_, dict_buffer_->data(),
                     static_cast<int>(dict_buffer_->size()));
    auto dict_decoder = MakeDictDecoder<ByteArrayType>(descr_.get());
    dict_decoder->SetDict(decoder.get());
    dict_decoder->SetData(num_values_, buffer_->data(),
                          static_cast<int>(buffer_->size()));
    auto byte_array_decoder = dynamic_cast<ByteArrayDecoder*>(dict_decoder.get());

    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    byte_array_decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_DictDecodingByteArray, DecodeArrowNonNull_Dict)
    ->Range(MIN_RANGE, MAX_RANGE);

}  // namespace parquet
