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

#include <memory>
#include <sstream>

#include <benchmark/benchmark.h>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/util/logging.h"

#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/schema.h"

namespace parquet {

using ::arrow::Buffer;
using ::arrow::io::BufferOutputStream;
using ::arrow::io::BufferReader;
using schema::GroupNode;
using schema::NodePtr;
using schema::NodeVector;

class MetadataBenchmark {
 public:
  explicit MetadataBenchmark(benchmark::State* state)
      : MetadataBenchmark(static_cast<int>(state->range(0)),
                          static_cast<int>(state->range(1))) {}

  MetadataBenchmark(int num_columns, int num_row_groups)
      : num_columns_(num_columns), num_row_groups_(num_row_groups) {
    NodeVector fields;
    for (int i = 0; i < num_columns_; ++i) {
      std::stringstream ss;
      ss << "col" << i;
      fields.push_back(parquet::schema::Int32(ss.str(), Repetition::REQUIRED));
    }
    schema_root_ = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));

    WriterProperties::Builder prop_builder;
    writer_properties_ = prop_builder.version(ParquetVersion::PARQUET_2_6)
                             ->disable_dictionary()
                             ->data_page_version(ParquetDataPageVersion::V2)
                             ->build();
  }

  std::shared_ptr<Buffer> WriteFile(benchmark::State* state) {
    PARQUET_ASSIGN_OR_THROW(auto sink, BufferOutputStream::Create());

    auto writer = ParquetFileWriter::Open(sink, schema_root_, writer_properties_);
    std::vector<int32_t> int32_values(1, 42);
    int64_t data_size = 0;
    for (int rg = 0; rg < num_row_groups_; ++rg) {
      auto row_group_writer = writer->AppendRowGroup();
      for (int col = 0; col < num_columns_; ++col) {
        auto col_writer = row_group_writer->NextColumn();
        ARROW_CHECK_EQ(col_writer->type(), Type::INT32);
        auto typed_col_writer = static_cast<Int32Writer*>(col_writer);
        typed_col_writer->WriteBatch(
            /*num_values=*/static_cast<int64_t>(int32_values.size()),
            /*def_levels=*/nullptr, /*rep_levels=*/nullptr, int32_values.data());
        typed_col_writer->Close();
      }
      row_group_writer->Close();
      data_size += row_group_writer->total_compressed_bytes_written();
    }
    writer->Close();
    PARQUET_ASSIGN_OR_THROW(auto buf, sink->Finish());
    state->counters["file_size"] = static_cast<double>(buf->size());
    // Note that "data_size" includes the Thrift page headers
    state->counters["data_size"] = static_cast<double>(data_size);
    return buf;
  }

  void ReadFile(std::shared_ptr<Buffer> contents) {
    auto source = std::make_shared<BufferReader>(contents);
    ReaderProperties props;
    auto reader = ParquetFileReader::Open(source, props);
    auto metadata = reader->metadata();
    ARROW_CHECK_EQ(metadata->num_columns(), num_columns_);
    ARROW_CHECK_EQ(metadata->num_row_groups(), num_row_groups_);
    // There should be one row per row group
    ARROW_CHECK_EQ(metadata->num_rows(), num_row_groups_);
    reader->Close();
  }

 private:
  int num_columns_;
  int num_row_groups_;
  std::shared_ptr<GroupNode> schema_root_;
  std::shared_ptr<WriterProperties> writer_properties_;
};

void WriteMetadataSetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"num_columns", "num_row_groups"});

  for (int num_columns : {1, 10, 100}) {
    for (int num_row_groups : {1, 100, 1000}) {
      bench->Args({num_columns, num_row_groups});
    }
  }
  /* For larger num_columns, restrict num_row_groups to small values
   * to avoid blowing up benchmark execution time.
   */
  for (int num_row_groups : {1, 100}) {
    bench->Args({/*num_columns=*/1000, num_row_groups});
  }
}

void ReadMetadataSetArgs(benchmark::internal::Benchmark* bench) {
  WriteMetadataSetArgs(bench);
}

void WriteFileMetadataAndData(benchmark::State& state) {
  MetadataBenchmark benchmark(&state);

  for (auto _ : state) {
    auto sink = benchmark.WriteFile(&state);
  }
  state.SetItemsProcessed(state.iterations());
}

void ReadFileMetadata(benchmark::State& state) {
  MetadataBenchmark benchmark(&state);
  auto contents = benchmark.WriteFile(&state);

  for (auto _ : state) {
    benchmark.ReadFile(contents);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(WriteFileMetadataAndData)->Apply(WriteMetadataSetArgs);
BENCHMARK(ReadFileMetadata)->Apply(ReadMetadataSetArgs);

}  // namespace parquet
