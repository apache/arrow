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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <optional>
#include <string_view>
#include <variant>

#include "arrow/filesystem/path_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/unreachable.h"
#include "benchmark/benchmark.h"
#include "flatbuffers/flatbuffers.h"
#include "generated/parquet3_generated.h"
#include "generated/parquet_types.h"
#include "parquet/metadata3.h"
#include "parquet/thrift_internal.h"

static inline std::string GetBasename(const std::string& path) {
  auto pos = path.find_last_of("/\\");
  return (pos == std::string::npos) ? path : path.substr(pos + 1);
}

// Baseline
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=4040696
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=25680
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1379944
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=1174696
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=5906552
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2801976
//
//
// Remove deprecated ColumnChunk.file_offset
//
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=1292376
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5056
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=214192
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=226112
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=2961808
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1120360
//
//
// Optimized statistics
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3874720
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=8208
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1304568
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=721728
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=5538032
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2599152
//
//
// Optimized offsets/num_vals
//
// RowGroup size limited to 2^31 and num values to 2^31. ColumnChunk offsets are relative
// to RowGroup starts which makes then all int32s too.
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3331720
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=7560
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1214640
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=620344
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4801656
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2390080
//
//
// Optimized num_values when ColumnChunk is dense
//
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=3265192
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=7568
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1207416
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=611720
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4433832
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2343608
//
//
// Replace encoding stats with is_fully_dict_encoded
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=2622520
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=6792
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1106640
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=489016
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=4433656
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=2062584
//
//
// Remove path_in_schema in ColumnMetadata
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=2092640
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5544
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1045304
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=333176
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=3697824
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1922080
//
//
// Remove encodings in ColumnMetadata
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=1884920
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5296
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=1009328
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=292560
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=3329648
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1781344
//
//
// Optimize statistics further. Only allow 4/8 byte values. Add common prefix.
// Remove distinct_count.
//
// 0/amazon_apparel.footer: num-rgs=1182 num-cols=16 thrift=2158995 flatbuf=1350760
// 1/amazon_movie_tv.footer: num-rgs=3 num-cols=18 thrift=22578 flatbuf=5192
// 2/amazon_polarity.footer: num-rgs=900 num-cols=4 thrift=1074313 flatbuf=235368
// 3/amazon_reviews_books.footer: num-rgs=159 num-cols=44 thrift=767840 flatbuf=238656
// 4/large-footer1: num-rgs=23 num-cols=2001 thrift=3253741 flatbuf=3329632
// 5/large-footer2: num-rgs=4 num-cols=2930 thrift=2248476 flatbuf=1165112
//
//

#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-function"

namespace parquet {
namespace {

std::string ReadFile(const std::string& name) {
  std::stringstream buffer;
  std::ifstream t(name);
  buffer << t.rdbuf();
  return buffer.str();
}

std::string Serialize(const format::FileMetaData& md) {
  ThriftSerializer ser;
  std::string out;
  ser.SerializeToString(&md, &out);
  return out;
}

format::FileMetaData DeserializeThrift(const std::string& bytes) {
  ThriftDeserializer des(100 << 20, 100 << 20);
  format::FileMetaData md;
  uint32_t n = bytes.size();
  des.DeserializeMessage(reinterpret_cast<const uint8_t*>(bytes.data()), &n, &md);
  return md;
}

std::string SerializeFlatbuffer(format::FileMetaData* md) {
  std::string flatbuffer;
  parquet::ToFlatbuffer(md, &flatbuffer);
  return flatbuffer;
}

format::FileMetaData DeserializeFlatbuffer(const format3::FileMetaData* md) {
  return parquet::FromFlatbuffer(md);
}

struct Footer {
  std::string name;
  std::string thrift;
  std::string flatbuf;
  format::FileMetaData md;

  static Footer Make(const char* filename) {
    std::string bytes = ReadFile(filename);
    auto md = DeserializeThrift(bytes);
    std::string flatbuf;
    parquet::ToFlatbuffer(&md, &flatbuf);  // removes unsupported fields for fair comparison
    return {GetBasename(filename), Serialize(md), std::move(flatbuf), std::move(md)};
  }
};

void Parse(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    auto md = DeserializeThrift(footer.thrift);
  }
}

void AppendUleb(uint32_t x, std::string* out) {
  while (true) {
    uint8_t c = x & 0x7F;
    if (x < 0x80) return out->push_back(c);
    out->push_back(c + 0x80);
    x >>= 7;
  }
};

std::string AppendExtension(std::string thrift, const std::string& ext) {
  thrift.back() = '\x08';      // replace stop field with binary type
  AppendUleb(32767, &thrift);  // field-id
  AppendUleb(ext.size(), &thrift);
  thrift += ext;
  thrift += '\x00';  // add the stop field
  return thrift;
}

void EncodeFlatbuf(benchmark::State& state, const Footer& footer) {
  auto md = footer.md;
  for (auto _ : state) {
    auto ser = SerializeFlatbuffer(&md);
    benchmark::DoNotOptimize(ser);
  }
}

void ThriftFromFlatbuf(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    auto md = DeserializeFlatbuffer(format3::GetFileMetaData(footer.flatbuf.data()));
    benchmark::DoNotOptimize(md);
  }
}

void ParseAndVerifyFlatbuf(benchmark::State& state, const Footer& footer) {
  for (auto _ : state) {
    flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(footer.flatbuf.data()),
                            footer.flatbuf.size());
    auto fmd = format3::GetFileMetaData(footer.flatbuf.data());
    ARROW_DCHECK_EQ(fmd->num_rows(), footer.md.num_rows);
    bool ok = fmd->Verify(v);
    ARROW_DCHECK(ok);
  }
}

void ParseWithExtension(benchmark::State& state, const Footer& footer) {
  auto with_ext = AppendExtension(footer.thrift, footer.flatbuf);

  for (auto _ : state) {
    auto md = DeserializeThrift(with_ext);
  }
}

void Analyze(std::string_view name, const format::FileMetaData& md) {
  std::cerr << "Analyzing: " << name << "\n";
  std::vector<int> sizes;
  int num_cols = md.schema.size() - 1;
  size_t stats_bytes = 0, kv_bytes = 0;
  for (auto& kv : md.key_value_metadata) kv_bytes += kv.key.size() + kv.value.size();
  for (int i = 0; i < num_cols; ++i) {
    for (auto& rg : md.row_groups) {
      auto& cc = rg.columns[i];
      if (!cc.__isset.meta_data) continue;
      auto& cmd = cc.meta_data;
      auto& s = cmd.statistics;
      stats_bytes += s.max_value.size() + s.min_value.size();
      for (auto& kv : cmd.key_value_metadata) kv_bytes += kv.key.size() + kv.value.size();
    }
  }
  std::cerr << "num-rgs=" << md.row_groups.size() << " num-cols=" << num_cols
            << " stats_bytes=" << stats_bytes << " kv_bytes=" << kv_bytes << "\n";
}

struct SiBytes {
  double v;
  int p;
  char u;
};

SiBytes ToSiBytes(size_t v) {
  auto kb = [](size_t n) { return n << 10; };
  auto mb = [](size_t n) { return n << 20; };
  auto gb = [](size_t n) { return n << 30; };
  if (v < kb(2)) return {v * 1., 0, ' '};
  if (v < mb(2)) return {v / 1024., v < kb(10), 'k'};
  if (v < gb(2)) return {v / 1024. / 1024, v < mb(10), 'M'};
  return {v / 1024. / 1024 / 1024, v < gb(10), 'G'};
}

}  // namespace
}  // namespace parquet

int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  std::vector<parquet::Footer> footers;
  for (int i = 1; i < argc; ++i) footers.push_back(parquet::Footer::Make(argv[i]));
  struct {
    std::string name;
    void (*fn)(benchmark::State&, const parquet::Footer&);
  } benchmarks[] = {
      {"Parse", parquet::Parse},
      {"ParseWithExtension", parquet::ParseWithExtension},
      {"EncodeFlatbuf", parquet::EncodeFlatbuf},
      {"ThriftFromFlatbuf", parquet::ThriftFromFlatbuf},
      {"ParseAndVerifyFlatbuf", parquet::ParseAndVerifyFlatbuf},
  };
  for (auto&& footer : footers) {
    for (auto&& [n, fn] : benchmarks) {
      char buf[1024];
      snprintf(buf, sizeof(buf), "%30s/%s", footer.name.c_str(), n.c_str());
      ::benchmark::RegisterBenchmark(buf, fn, footer)->Unit(benchmark::kMillisecond);
    }
  }

  char key[1024];
  char val[1024];
  for (size_t i = 0; i < footers.size(); ++i) {
    auto&& f = footers[i];
    auto name = GetBasename(f.name);
    snprintf(key, sizeof(key), "%lu/%s", i, name.c_str());
    auto thrift = parquet::ToSiBytes(f.thrift.size());
    auto flatbuf = parquet::ToSiBytes(f.flatbuf.size());
    snprintf(val, sizeof(val), "num-rgs=%lu num-cols=%lu thrift=%.*f%c flatbuf=%.*f%c",
             f.md.row_groups.size(), f.md.schema.size() - 1, thrift.p, thrift.v, thrift.u,
             flatbuf.p, flatbuf.v, flatbuf.u);
    ::benchmark::AddCustomContext(key, val);
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
