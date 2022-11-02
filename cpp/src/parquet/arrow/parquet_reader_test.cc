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

#ifdef _MSC_VER
#pragma warning(push)
// Disable forcing value to bool warnings
#pragma warning(disable : 4800)
#endif

#include "gtest/gtest.h"
#include <vector>

#include "arrow/io/api.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/config.h"  // for ARROW_CSV definition
#include "parquet/arrow/reader.h"
#include "parquet/arrow/test_util.h"
#include "parquet/test_util.h"
#include "parquet/qpl_job_pool.h"

#ifdef ARROW_CSV
#include "arrow/csv/api.h"
#endif

using arrow::Status;
using arrow::Table;
using arrow::DataType;

// namespace parquet {
namespace arrow {

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
    auto file = parquet::test::get_data_file(file_name);
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(
                                file, arrow::default_memory_pool()));
    ASSERT_OK(parquet::arrow::OpenFile(std::move(infile),
                arrow::default_memory_pool(), &file_reader));
    ASSERT_OK(file_reader->GetSchema(&schema));
}

class ParquetRowGroupReader : public ::testing::Test {
 public:
    ParquetRowGroupReader() {}

    void read(const std::string & filename) {
        if (!file_reader) {
            prepareReader(filename);
        }

        size_t parallel = 2;
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

TEST_F(ParquetRowGroupReader, ReadParquetFile) {
  std::shared_ptr<::arrow::Table> actual_table;
  read("lineorder.parquet");
}

}  // namespace arrow

#ifdef ARROW_CSV

namespace parquet {
namespace arrow {

class TestArrowReadWithQPL : public ::testing::Test {
 public:
  void ReadTableFromParquetFile(const std::string& file_name,
                                std::shared_ptr<Table>* out) {
    auto file = test::get_data_file(file_name);
    auto pool = ::arrow::default_memory_pool();
    std::unique_ptr<FileReader> parquet_reader;
    ASSERT_OK(FileReader::Make(pool, ParquetFileReader::OpenFile(file, false),
                               &parquet_reader));
    ASSERT_OK(parquet_reader->ReadTable(out));
    ASSERT_OK((*out)->ValidateFull());
  }

  void ReadTableFromCSVFile(const std::string& file_name,
                            const ::arrow::csv::ConvertOptions& convert_options,
                            std::shared_ptr<Table>* out) {
    auto file = test::get_data_file(file_name);
    ASSERT_OK_AND_ASSIGN(auto input_file, ::arrow::io::ReadableFile::Open(file));
    ASSERT_OK_AND_ASSIGN(auto csv_reader,
                         ::arrow::csv::TableReader::Make(
                             ::arrow::io::default_io_context(), input_file,
                             ::arrow::csv::ReadOptions::Defaults(),
                             ::arrow::csv::ParseOptions::Defaults(), convert_options));
    ASSERT_OK_AND_ASSIGN(*out, csv_reader->Read());
  }
};


TEST_F(TestArrowReadWithQPL, ReadSnappyParquetFile) {
  std::shared_ptr<::arrow::Table> actual_table, expect_table;
  ReadTableFromParquetFile("lineorder.parquet", &actual_table);

  auto convert_options = ::arrow::csv::ConvertOptions::Defaults();
  convert_options.column_types = {{"c_customer_sk", ::arrow::uint8()},
                                  {"c_current_cdemo_sk", ::arrow::uint8()},
                                  {"c_current_hdemo_sk", ::arrow::uint8()},
                                  {"c_current_addr_sk", ::arrow::uint8()},
                                  {"c_customer_id", ::arrow::binary()}};
  convert_options.strings_can_be_null = true;
  ReadTableFromCSVFile("lineorder.csv", convert_options,
                       &expect_table);

  ::arrow::AssertTablesEqual(*actual_table, *expect_table);
}

#else
TEST_F(TestArrowReadWithQPL, ReadSnappyParquetFile) {
  GTEST_SKIP() << "Test needs CSV reader";
}
#endif

} // namespace arrow
} // namespace parquet


