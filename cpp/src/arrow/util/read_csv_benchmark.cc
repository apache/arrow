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

#include <arrow/io/file.h>

#include <fstream>
#include <string>
#include <vector>

#include "arrow/csv/options.h"
#include "arrow/csv/reader.h"
#include "arrow/util/parsing.h"
#include "arrow/util/timestamp_converter.h"
#include "benchmark/benchmark.h"

namespace arrow {

const std::vector<std::string> formats = {"1917-10-17,", "2018-09-13 22,",
                                          "1941-06-22 04:00,", "1945-05-09 09:45:38,"};
const std::string file_name = "__data.csv";

const int32_t n_rows = 100000;
const int32_t n_cols = 150;

class ReadCSVBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) {
    generateCSV(file_name, formats, n_cols, n_rows);
  }

  void TearDown(const ::benchmark::State& state) {
    std::remove(file_name.c_str());
  }

 private:
  void generateCSV(const std::string& path_csv, const std::vector<std::string>& dates,
                   int32_t cols, int32_t rows) {
    ::srand(777);
    std::ofstream file;
    file.open(path_csv, std::ios::out);

    for (int32_t row = 0; row < rows; ++row) {
      for (int32_t col = 0; col < cols; ++col) {
        file << dates[rand() % dates.size()];
      }
      file << "\n";
    }
    file.close();
  }
};

static void readCSV(const std::string& path_csv, const csv::ConvertOptions& convert_opt) {
  arrow::Status st;
  auto memory_pool = default_memory_pool();

  std::shared_ptr<io::ReadableFile> input;
  auto file_result = io::ReadableFile::Open(path_csv.c_str());
  st = file_result.status();
  input = file_result.ValueOrDie();

  auto read_opt = csv::ReadOptions::Defaults();
  auto parse_opt = csv::ParseOptions::Defaults();

  auto table_reader_result =
      csv::TableReader::Make(memory_pool, input, read_opt, parse_opt, convert_opt);
  st = table_reader_result.status();
  auto table_reader = table_reader_result.ValueOrDie();

  std::shared_ptr<Table> arrowTable;

  auto arrow_table_result = table_reader->Read();
  st = arrow_table_result.status();
  arrowTable = arrow_table_result.ValueOrDie();
}

BENCHMARK_F(ReadCSVBenchmark, ConverterISO)(benchmark::State& state) {
  for (auto _ : state) {
    auto convert_options = csv::ConvertOptions::Defaults();
    convert_options.timestamp_converters.push_back(
        std::make_shared<ISO8601Parser>(timestamp(TimeUnit::SECOND)));
    readCSV(file_name, convert_options);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_F(ReadCSVBenchmark, ConverterDefault)(benchmark::State& state) {
  for (auto _ : state) {
    auto convert_options = csv::ConvertOptions::Defaults();
    readCSV(file_name, convert_options);
  }
  state.SetItemsProcessed(state.iterations());
}

}  // namespace arrow
