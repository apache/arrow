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

#include "parquet/encodings/plain-encoding.h"

namespace parquet {

namespace benchmark {

static void BM_PlainEncodingBoolean(::benchmark::State& state) {
  std::vector<bool> values(state.range_x(), 64);
  PlainEncoder<BooleanType> encoder(nullptr);

  while (state.KeepRunning()) {
    InMemoryOutputStream dst;
    encoder.Encode(values, values.size(), &dst);
  }
  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(bool));
}

BENCHMARK(BM_PlainEncodingBoolean)->Range(1024, 65536);

static void BM_PlainDecodingBoolean(::benchmark::State& state) {
  std::vector<bool> values(state.range_x(), 64);
  bool* output = new bool[state.range_x()];
  PlainEncoder<BooleanType> encoder(nullptr);
  InMemoryOutputStream dst;
  encoder.Encode(values, values.size(), &dst);
  std::shared_ptr<Buffer> buf = dst.GetBuffer();

  while (state.KeepRunning()) {
    PlainDecoder<BooleanType> decoder(nullptr);
    decoder.SetData(values.size(), buf->data(), buf->size());
    decoder.Decode(output, values.size());
  }

  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(bool));
  delete[] output;
}

BENCHMARK(BM_PlainDecodingBoolean)->Range(1024, 65536);

static void BM_PlainEncodingInt64(::benchmark::State& state) {
  std::vector<int64_t> values(state.range_x(), 64);
  PlainEncoder<Int64Type> encoder(nullptr);

  while (state.KeepRunning()) {
    InMemoryOutputStream dst;
    encoder.Encode(values.data(), values.size(), &dst);
  }
  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(int64_t));
}

BENCHMARK(BM_PlainEncodingInt64)->Range(1024, 65536);

static void BM_PlainDecodingInt64(::benchmark::State& state) {
  std::vector<int64_t> values(state.range_x(), 64);
  PlainEncoder<Int64Type> encoder(nullptr);
  InMemoryOutputStream dst;
  encoder.Encode(values.data(), values.size(), &dst);
  std::shared_ptr<Buffer> buf = dst.GetBuffer();

  while (state.KeepRunning()) {
    PlainDecoder<Int64Type> decoder(nullptr);
    decoder.SetData(values.size(), buf->data(), buf->size());
    decoder.Decode(values.data(), values.size());
  }
  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(int64_t));
}

BENCHMARK(BM_PlainDecodingInt64)->Range(1024, 65536);

}  // namespace benchmark

}  // namespace parquet
