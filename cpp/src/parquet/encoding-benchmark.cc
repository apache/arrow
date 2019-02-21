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

  while (state.KeepRunning()) {
    typed_encoder->Put(values, static_cast<int>(values.size()));
    typed_encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
}

BENCHMARK(BM_PlainEncodingBoolean)->Range(1024, 65536);

static void BM_PlainDecodingBoolean(benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  bool* output = new bool[state.range(0)];
  auto encoder = MakeEncoder(Type::BOOLEAN, Encoding::PLAIN);
  auto typed_encoder = dynamic_cast<BooleanEncoder*>(encoder.get());
  typed_encoder->Put(values, static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<BooleanType>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(output, static_cast<int>(values.size()));
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
  delete[] output;
}

BENCHMARK(BM_PlainDecodingBoolean)->Range(1024, 65536);

static void BM_PlainEncodingInt64(benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  auto encoder = MakeTypedEncoder<Int64Type>(Encoding::PLAIN);
  while (state.KeepRunning()) {
    encoder->Put(values.data(), static_cast<int>(values.size()));
    encoder->FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainEncodingInt64)->Range(1024, 65536);

static void BM_PlainDecodingInt64(benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  auto encoder = MakeTypedEncoder<Int64Type>(Encoding::PLAIN);
  encoder->Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder->FlushValues();

  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<Int64Type>(Encoding::PLAIN);
    decoder->SetData(static_cast<int>(values.size()), buf->data(),
                     static_cast<int>(buf->size()));
    decoder->Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainDecodingInt64)->Range(1024, 65536);

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

  while (state.KeepRunning()) {
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

BENCHMARK(BM_DictDecodingInt64_repeats)->Range(1024, 65536);

static void BM_DictDecodingInt64_literals(benchmark::State& state) {
  typedef Int64Type Type;
  typedef typename Type::c_type T;

  std::vector<T> values(state.range(0));
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = i;
  }
  DecodeDict<Type>(values, state);
}

BENCHMARK(BM_DictDecodingInt64_literals)->Range(1024, 65536);

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

  for (size_t i = 0; i < num_values; i++) {
    const auto index = indices_dist(gen);
    const auto value = dictionary[index];
    ABORT_NOT_OK(builder.Append(value));
  }

  std::shared_ptr<::arrow::Array> result;
  ABORT_NOT_OK(builder.Finish(&result));
  return result;
}

class BM_PlainDecodingByteArray : public ::benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    num_values_ = static_cast<int>(state.range());

    input_array_ = MakeRandomStringsWithRepeats(num_values_ / 8, num_values_);
    const auto& binary_array = static_cast<const ::arrow::BinaryArray&>(*input_array_);
    values_ = std::vector<ByteArray>();
    values_.reserve(num_values_);
    total_size_ = 0;

    for (int64_t i = 0; i < binary_array.length(); i++) {
      auto view = binary_array.GetView(i);
      values_.emplace_back(static_cast<uint32_t>(view.length()),
                           reinterpret_cast<const uint8_t*>(view.data()));
      total_size_ += view.length();
    }

    valid_bits_ =
        std::vector<uint8_t>(::arrow::BitUtil::BytesForBits(num_values_) + 1, 255);

    auto encoder = MakeTypedEncoder<ByteArrayType>(Encoding::PLAIN);
    encoder->Put(values_.data(), num_values_);
    buffer_ = encoder->FlushValues();
  }

  void TearDown(const ::benchmark::State& state) override {}

 protected:
  int num_values_;
  std::shared_ptr<::arrow::Array> input_array_;
  uint64_t total_size_;
  std::vector<ByteArray> values_;
  std::vector<uint8_t> valid_bits_;
  std::shared_ptr<Buffer> buffer_;
};

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrow_Dense)
(benchmark::State& state) {
  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::internal::ChunkedBinaryBuilder builder(static_cast<int>(buffer_->size()),
                                                    ::arrow::default_memory_pool());
    decoder->DecodeArrow(num_values_, 0, valid_bits_.data(), 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrow_Dense)->Range(1024, 65536);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dense)
(benchmark::State& state) {
  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::internal::ChunkedBinaryBuilder builder(static_cast<int>(buffer_->size()),
                                                    ::arrow::default_memory_pool());
    decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dense)
    ->Range(1024, 65536);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrow_Dict)
(benchmark::State& state) {
  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    decoder->DecodeArrow(num_values_, 0, valid_bits_.data(), 0, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrow_Dict)->Range(1024, 65536);

BENCHMARK_DEFINE_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dict)
(benchmark::State& state) {
  while (state.KeepRunning()) {
    auto decoder = MakeTypedDecoder<ByteArrayType>(Encoding::PLAIN);
    decoder->SetData(num_values_, buffer_->data(), static_cast<int>(buffer_->size()));
    ::arrow::BinaryDictionaryBuilder builder(::arrow::default_memory_pool());
    decoder->DecodeArrowNonNull(num_values_, &builder);
  }

  state.SetBytesProcessed(state.iterations() * total_size_);
}

BENCHMARK_REGISTER_F(BM_PlainDecodingByteArray, DecodeArrowNonNull_Dict)
    ->Range(1024, 65536);

}  // namespace parquet
