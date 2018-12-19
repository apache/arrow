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

#include <sstream>
#include <string>

#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/test-util.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace json {

std::shared_ptr<Array> GetColumn(const RecordBatch& batch, const std::string& name) {
  return batch.column(batch.schema()->GetFieldIndex(name));
}

template <typename Engine>
static Status GenerateValue(const std::shared_ptr<DataType>& type, Engine& e,
                            std::ostream& os);

template <typename Engine>
static Status GenerateObject(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                             std::ostream& os);

template <typename Element>
static Status CommaDelimited(int count, std::ostream& os, Element&& element);

template <typename Engine>
struct GenerateValueImpl {
  template <typename T>
  Status Visit(T const&, enable_if_integer<T>* = nullptr) {
    auto val = std::uniform_int_distribution<>{}(e);
    os << static_cast<typename T::c_type>(val);
    return Status::OK();
  }
  template <typename T>
  Status Visit(T const&, enable_if_floating_point<T>* = nullptr) {
    os << std::normal_distribution<typename T::c_type>{0, 1 << 10}(e);
    return Status::OK();
  }
  Status Visit(HalfFloatType const&) {
    os << std::uniform_int_distribution<HalfFloatType::c_type>{}(e);
    return Status::OK();
  }
  template <typename T>
  Status Visit(T const&, enable_if_binary<T>* = nullptr) {
    auto size = std::poisson_distribution<>{4}(e);
    std::uniform_int_distribution<short> gen_char(32, 127);  // FIXME generate UTF8
    std::string s(size, '\0');
    for (char& ch : s) ch = static_cast<char>(gen_char(e));
    os << std::quoted(s);
    return Status::OK();
  }
  template <typename T>
  Status Visit(
      T const& t, typename std::enable_if<!is_number<T>::value>::type* = nullptr,
      typename std::enable_if<!std::is_base_of<BinaryType, T>::value>::type* = nullptr) {
    return Status::Invalid("can't generate a value of type " + t.name());
  }
  Status Visit(const ListType& t) {
    auto size = std::poisson_distribution<>{4}(e);
    os << "[";
    RETURN_NOT_OK(CommaDelimited(
        size, os, [&](int) { return GenerateValue(t.value_type(), e, os); }));
    os << "]";
    return Status::OK();
  }
  Status Visit(const StructType& t) { return GenerateObject(t.children(), e, os); }
  Engine& e;
  std::ostream& os;
};

template <typename Engine>
static Status GenerateValue(const std::shared_ptr<DataType>& type, Engine& e,
                            std::ostream& os) {
  if (std::uniform_real_distribution<>{0, 1}(e) < .2) {
    // one out of 5 chance of null, anywhere
    os << "null";
    return Status::OK();
  }
  GenerateValueImpl<Engine> visitor{e, os};
  return VisitTypeInline(*type, &visitor);
}

template <typename Element>
static Status CommaDelimited(int count, std::ostream& os, Element&& element) {
  bool first = true;
  for (int i = 0; i != count; ++i) {
    if (first)
      first = false;
    else
      os << ",";
    RETURN_NOT_OK(element(i));
  }
  return Status::OK();
}

template <typename Engine>
static Status GenerateObject(const std::vector<std::shared_ptr<Field>>& fields, Engine& e,
                             std::ostream& os) {
  os << "{";
  RETURN_NOT_OK(CommaDelimited(static_cast<int>(fields.size()), os, [&](int i) {
    os << std::quoted(fields[i]->name()) << ":";
    return GenerateValue(fields[i]->type(), e, os);
  }));
  os << "}";
  return Status::OK();
}

static void BenchmarkJSONParsing(benchmark::State& state,  // NOLINT non-const reference
                                 const std::string& json, int32_t num_rows,
                                 ParseOptions options) {
  BlockParser parser(options, -1, num_rows + 1);

  for (auto _ : state) {
    ABORT_NOT_OK(parser.Parse(std::string(json)));
    if (parser.num_rows() != num_rows) {
      std::cerr << "Parsing incomplete\n";
      std::abort();
    }
    std::shared_ptr<RecordBatch> parsed;
    ABORT_NOT_OK(parser.Finish(&parsed));
    auto ints_valid = GetColumn(*parsed, "int")->data()->GetValues<uint8_t>(0);
    auto ints = GetColumn(*parsed, "int")->data()->GetValues<int64_t>(1);
    auto strs_valid = GetColumn(*parsed, "str")->data()->GetValues<uint8_t>(0);
    auto offsets = GetColumn(*parsed, "str")->data()->GetValues<int32_t>(1);
    auto chars = GetColumn(*parsed, "str")->data()->GetValues<char>(2);
    int64_t ints_sum = 0;
    char strs_xor = '\0';
    for (int32_t i = 0; i != num_rows; ++i) {
      if (BitUtil::GetBit(ints_valid, i)) ints_sum += ints[i];
      if (BitUtil::GetBit(strs_valid, i)) {
        for (int32_t c = offsets[i]; c != offsets[i + 1]; ++c) {
          strs_xor ^= chars[c];
        }
      }
    }
    benchmark::DoNotOptimize(ints_sum);
    benchmark::DoNotOptimize(strs_xor);
  }
  state.SetBytesProcessed(state.iterations() * json.size());
}

static void BM_ParseJSONQuotedBlock(
    benchmark::State& state) {  // NOLINT non-const reference
  const int32_t num_rows = 5000;
  auto schm = schema({field("int", int32()), field("str", utf8())});
  std::mt19937_64 engine;
  std::stringstream json;
  for (int i = 0; i != num_rows; ++i) {
    ABORT_NOT_OK(GenerateObject(schm->fields(), engine, json));
    json << "\n";
  }
  auto options = ParseOptions::Defaults();

  BenchmarkJSONParsing(state, json.str(), num_rows, options);
}

BENCHMARK(BM_ParseJSONQuotedBlock)->Repetitions(3)->Unit(benchmark::kMicrosecond);

}  // namespace json
}  // namespace arrow
