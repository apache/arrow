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

#include <vector>

#include "arrow/io/api.h"
#include "arrow/table.h"
#include "arrow/testing/util.h"
#include "arrow/util/config.h"  // for ARROW_CSV definition

#ifdef ARROW_CSV
#include "arrow/csv/api.h"
#endif

#include "parquet/arrow/reader.h"
#include "parquet/arrow/test_util.h"

#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"

using arrow::Status;
using arrow::Table;
using arrow::DataType;

#define EXIT_NOT_OK(s)                                        \
  do {                                                        \
    ::arrow::Status _s = (s);                                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                      \
      std::cout << "Exiting: " << _s.ToString() << std::endl; \
      exit(EXIT_FAILURE);                                     \
    }                                                         \
  } while (0)


namespace arrow {

class ParquetTestException : public parquet::ParquetException {
  using ParquetException::ParquetException;
};

const char* get_data_dir() {
  const auto result = std::getenv("PARQUET_TEST_DATA");
  if (!result || !result[0]) {
    throw ParquetTestException(
        "Please point the PARQUET_TEST_DATA environment "
        "variable to the test data directory");
  }
  return result;
}

std::string get_data_file(const std::string& filename) {
  std::stringstream ss;

  ss << get_data_dir();
  ss << "/" << filename;
  return ss.str();
}


// This should result in multiple pages for most primitive types
constexpr int64_t BENCHMARK_SIZE = 10 * 1024 * 1024;

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type) {
    if (type->id() == arrow::Type::LIST) {
      return countIndicesForType(
        static_cast<arrow::ListType *>(type.get())->value_type());
    }

    if (type->id() == arrow::Type::STRUCT) {
        int indices = 0;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP) {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) +
                  countIndicesForType(map_type->item_type());
    }

    return 1;
}

static void getFileReaderAndSchema(
    const std::string& file_name,
    std::unique_ptr<parquet::arrow::FileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema) {
    auto file = get_data_file(file_name);
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(file,
                             arrow::default_memory_pool()));
    EXIT_NOT_OK(parquet::arrow::OpenFile(std::move(infile),
                arrow::default_memory_pool(), &file_reader));
    EXIT_NOT_OK(file_reader->GetSchema(&schema));
}


class ParquetRowGroupReader {
 public:
    ParquetRowGroupReader(){}

    void read(const std::string & filename) {
        if (!file_reader)
            prepareReader(filename);

        size_t parallel = 5;
        while (row_group_current < row_group_total) {
            std::vector<int> row_group_indexes;
            for (; row_group_current < row_group_total && 
            row_group_indexes.size() < parallel; ++row_group_current) {
                row_group_indexes.push_back(row_group_current);
            }

            if (row_group_indexes.empty()) {
                return;
            }
            std::shared_ptr<arrow::Table> table;
            arrow::Status read_status = file_reader->ReadRowGroups(
                                        row_group_indexes, column_indices, &table);
            ASSERT_OK(read_status);
        }
        return;
    }


    void prepareReader(const std::string & filename) {
        std::shared_ptr<arrow::Schema> schema;
        getFileReaderAndSchema(filename, file_reader, schema);

        row_group_total = file_reader->num_row_groups();
        row_group_current = 0;

        int index = 0;
        for (int i = 0; i < schema->num_fields(); ++i) {
            /// STRUCT type require the number of indexes equal to the number of
            /// nested elements, so we should recursively
            /// count the number of indices we need for this type.
            int indexes_count = countIndicesForType(schema->field(i)->type());

            for (int j = 0; j != indexes_count; ++j)
                column_indices.push_back(index + j);
            index += indexes_count;
        }
    }

    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    int row_group_total = 0;
    int row_group_current = 0;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;
};


template <uint32_t rows, uint32_t row_group>
void SetBytesProcessed(::benchmark::State& state, int64_t num_values = BENCHMARK_SIZE) {
  const int64_t items_processed = state.iterations() * num_values;
  const int64_t bytes_processed = items_processed * sizeof(rows);

  state.SetItemsProcessed(bytes_processed);
  state.SetBytesProcessed(bytes_processed);
}


template <uint32_t rows, uint32_t row_group>
static void BM_ReadFile(::benchmark::State& state) {
  while (state.KeepRunning()) {
    ParquetRowGroupReader reader;
    std::string file_name = "single_column_" + std::to_string(state.range(0)) +
                            "kw_" + std::to_string(state.range(1)) + ".parquet";
    reader.read(file_name);
  }

  SetBytesProcessed<rows, row_group>(state);
}


template <uint32_t rows, uint32_t bit_width>
static void BM_ReadFileDiffBitWidth(::benchmark::State& state) {
  while (state.KeepRunning()) {
    ParquetRowGroupReader reader;
    std::string file_name = "sc_" + std::to_string(state.range(0)) + "kw_multibit_"
                            + std::to_string(state.range(1)) + ".parquet";
    reader.read(file_name);
  }

  SetBytesProcessed<bit_width, bit_width>(state);
}

// There are two parameters here that cover different data distributions.
// null_percentage governs distribution and therefore runs of null values.
// first_value_percentage governs distribution of values (we select from 1 of 2)
// so when 0 or 100 RLE is triggered all the time.  When a value in the range (0, 100)
// there will be some percentage of RLE encoded values and some percentage of literal
// encoded values (RLE is much less likely with percentages close to 50).
BENCHMARK_TEMPLATE2(BM_ReadFile, 1, 64)
    ->Args({1, 64})
    ->Args({2, 64})
    ->Args({3, 64})
    ->Args({1, 512})
    ->Args({2, 512})
    ->Args({3, 512});

BENCHMARK_TEMPLATE2(BM_ReadFileDiffBitWidth, 1, 2)
    ->Args({1, 3})
    ->Args({1, 5})
    ->Args({1, 6})
    ->Args({1, 8})
    ->Args({1, 9})
    ->Args({1, 10})
    ->Args({1, 11})
    ->Args({1, 12})
    ->Args({1, 13})
    ->Args({1, 14})
    ->Args({1, 16})
    ->Args({1, 18});

// }  // namespace benchmark

}  // namespace arrow
