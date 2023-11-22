// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/util/type_fwd.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

#include <arrow/testing/gtest_util.h>
#include <arrow/util/range.h>
#include <iostream>

arrow::Result<std::shared_ptr<arrow::Table>> GetTable() {
  auto builder = arrow::Int32Builder();

  std::shared_ptr<arrow::Array> arr_x;
  ARROW_RETURN_NOT_OK(builder.AppendValues(arrow::internal::Iota(0, 100)));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_x));

  std::shared_ptr<arrow::Array> arr_y;
  ARROW_RETURN_NOT_OK(builder.AppendValues(arrow::internal::Iota(0, 100)));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_y));

  std::shared_ptr<arrow::Array> arr_z_values;
  std::shared_ptr<arrow::Array> arr_z_offsets;
  ARROW_RETURN_NOT_OK(builder.AppendValues(arrow::internal::Iota(0, 300)));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_z_values));
  std::vector<int> offsets = arrow::internal::Iota(0, 101);
  std::transform(offsets.begin(), offsets.end(), offsets.begin(),
                 [](int x) { return x * 3; });
  ARROW_RETURN_NOT_OK(builder.AppendValues(offsets));
  ARROW_RETURN_NOT_OK(builder.Finish(&arr_z_offsets));
  ARROW_ASSIGN_OR_RAISE(auto arr_z,
                        arrow::ListArray::FromArrays(*arr_z_offsets, *arr_z_values));

  auto schema =
      arrow::schema({arrow::field("x", arrow::int32()), arrow::field("y", arrow::int32()),
                     arrow::field("z", arrow::list(arrow::int32()))});

  return arrow::Table::Make(schema, {arr_x, arr_y, arr_z});
}

arrow::Result<std::shared_ptr<arrow::Buffer>> WriteFullFile() {
  using parquet::ArrowWriterProperties;
  using parquet::WriterProperties;

  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, GetTable());

  std::shared_ptr<WriterProperties> props =
      WriterProperties::Builder()
          .max_row_group_length(50)
          ->enable_write_page_index()
          ->write_batch_size(13)
          ->data_pagesize(1)  // this will cause every batch creating a page
          ->compression(arrow::Compression::SNAPPY)
          ->build();

  std::shared_ptr<ArrowWriterProperties> arrow_props =
      ArrowWriterProperties::Builder().store_schema()->build();

  ARROW_ASSIGN_OR_RAISE(auto outfile, ::arrow::io::BufferOutputStream::Create());

  ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table.get(),
                                                 arrow::default_memory_pool(), outfile,
                                                 /*chunk_size=*/100, props, arrow_props));
  return outfile->Finish();
}


arrow::Status ReadInBatches(std::shared_ptr<arrow::Buffer> buffer) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(4096 * 4);
  reader_properties.enable_buffered_stream();

  auto arrow_reader_props = parquet::ArrowReaderProperties();
  arrow_reader_props.set_batch_size(10);  // default 64 * 1024

  parquet::arrow::FileReaderBuilder reader_builder;
  auto in_file = std::make_shared<::arrow::io::BufferReader>(buffer);
  ARROW_RETURN_NOT_OK(reader_builder.Open(in_file, /*memory_map=*/reader_properties));
  reader_builder.memory_pool(pool);
  reader_builder.properties(arrow_reader_props);

  ARROW_ASSIGN_OR_RAISE(auto arrow_reader, reader_builder.Build());

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  auto row_ranges_map = std::make_shared<std::map<int, parquet::RowRangesPtr>>();

  std::vector<parquet::Range> ranges;
  for (int64_t i = 0; i < 50; i++) {
    if (i % 2 == 0) ranges.push_back({i, i});
  }
  row_ranges_map->insert({0, std::make_shared<parquet::RowRanges>(ranges)});

  ARROW_RETURN_NOT_OK(
      arrow_reader->GetRecordBatchReader({0, 1}, {0, 1}, row_ranges_map, &rb_reader));

  size_t total_rows = 0;
  size_t total_values = 0;
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
    // Operate on each batch...
    auto batch = maybe_batch.ValueOrDie();
    total_rows += batch->num_rows();
    std::cout << "batch size: " << batch->num_rows() << std::endl;

    auto int_array = std::dynamic_pointer_cast<arrow::Int32Array>(batch->column(1));
    for (auto iter = int_array->begin(); iter != int_array->end(); ++iter) {
      total_values += (*iter).value();
    }
  }
  std::cout << "total rows is : " << total_rows << std::endl;
  std::cout << "total value of y is : " << total_values << std::endl;
  return arrow::Status::OK();
}


arrow::Status RunExamples() {
  ARROW_ASSIGN_OR_RAISE(auto buffer, WriteFullFile());
  ARROW_RETURN_NOT_OK(ReadInBatches(buffer));
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    // Fake success for CI purposes.
    return EXIT_SUCCESS;
  }

  std::string path_to_file = argv[1];
  arrow::Status status = RunExamples();

  if (!status.ok()) {
    std::cerr << "Error occurred: " << status.message() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
