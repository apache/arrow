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

#include "parquet/encoding-internal.h"
#include "parquet/util/memory.h"

using arrow::default_memory_pool;
using arrow::MemoryPool;

namespace parquet {

using schema::PrimitiveNode;

namespace benchmark {

std::shared_ptr<ColumnDescriptor> Int64Schema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, Type::INT64);
  return std::make_shared<ColumnDescriptor>(node, repetition != Repetition::REQUIRED,
                                            repetition == Repetition::REPEATED);
}

static void BM_PlainEncodingBoolean(::benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  PlainEncoder<BooleanType> encoder(nullptr);

  while (state.KeepRunning()) {
    encoder.Put(values, static_cast<int>(values.size()));
    encoder.FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
}

BENCHMARK(BM_PlainEncodingBoolean)->Range(1024, 65536);

static void BM_PlainDecodingBoolean(::benchmark::State& state) {
  std::vector<bool> values(state.range(0), true);
  bool* output = new bool[state.range(0)];
  PlainEncoder<BooleanType> encoder(nullptr);
  encoder.Put(values, static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder.FlushValues();

  while (state.KeepRunning()) {
    PlainDecoder<BooleanType> decoder(nullptr);
    decoder.SetData(static_cast<int>(values.size()), buf->data(),
                    static_cast<int>(buf->size()));
    decoder.Decode(output, static_cast<int>(values.size()));
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(bool));
  delete[] output;
}

BENCHMARK(BM_PlainDecodingBoolean)->Range(1024, 65536);

static void BM_PlainEncodingInt64(::benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  PlainEncoder<Int64Type> encoder(nullptr);

  while (state.KeepRunning()) {
    encoder.Put(values.data(), static_cast<int>(values.size()));
    encoder.FlushValues();
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainEncodingInt64)->Range(1024, 65536);

static void BM_PlainDecodingInt64(::benchmark::State& state) {
  std::vector<int64_t> values(state.range(0), 64);
  PlainEncoder<Int64Type> encoder(nullptr);
  encoder.Put(values.data(), static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buf = encoder.FlushValues();

  while (state.KeepRunning()) {
    PlainDecoder<Int64Type> decoder(nullptr);
    decoder.SetData(static_cast<int>(values.size()), buf->data(),
                    static_cast<int>(buf->size()));
    decoder.Decode(values.data(), static_cast<int>(values.size()));
  }
  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(int64_t));
}

BENCHMARK(BM_PlainDecodingInt64)->Range(1024, 65536);

template <typename Type>
static void DecodeDict(std::vector<typename Type::c_type>& values,
                       ::benchmark::State& state) {
  typedef typename Type::c_type T;
  int num_values = static_cast<int>(values.size());

  MemoryPool* allocator = default_memory_pool();
  std::shared_ptr<ColumnDescriptor> descr = Int64Schema(Repetition::REQUIRED);

  DictEncoder<Type> encoder(descr.get(), allocator);
  for (int i = 0; i < num_values; ++i) {
    encoder.Put(values[i]);
  }

  std::shared_ptr<ResizableBuffer> dict_buffer =
      AllocateBuffer(allocator, encoder.dict_encoded_size());

  std::shared_ptr<ResizableBuffer> indices =
      AllocateBuffer(allocator, encoder.EstimatedDataEncodedSize());

  encoder.WriteDict(dict_buffer->mutable_data());
  int actual_bytes =
      encoder.WriteIndices(indices->mutable_data(), static_cast<int>(indices->size()));

  PARQUET_THROW_NOT_OK(indices->Resize(actual_bytes));

  while (state.KeepRunning()) {
    PlainDecoder<Type> dict_decoder(descr.get());
    dict_decoder.SetData(encoder.num_entries(), dict_buffer->data(),
                         static_cast<int>(dict_buffer->size()));
    DictionaryDecoder<Type> decoder(descr.get());
    decoder.SetDict(&dict_decoder);
    decoder.SetData(num_values, indices->data(), static_cast<int>(indices->size()));
    decoder.Decode(values.data(), num_values);
  }

  state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(T));
}

static void BM_DictDecodingInt64_repeats(::benchmark::State& state) {
  typedef Int64Type Type;
  typedef typename Type::c_type T;

  std::vector<T> values(state.range(0), 64);
  DecodeDict<Type>(values, state);
}

BENCHMARK(BM_DictDecodingInt64_repeats)->Range(1024, 65536);

static void BM_DictDecodingInt64_literals(::benchmark::State& state) {
  typedef Int64Type Type;
  typedef typename Type::c_type T;

  std::vector<T> values(state.range(0));
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = i;
  }
  DecodeDict<Type>(values, state);
}

BENCHMARK(BM_DictDecodingInt64_literals)->Range(1024, 65536);

}  // namespace benchmark

}  // namespace parquet
