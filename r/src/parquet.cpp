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

#include "./arrow_types.h"

#if defined(ARROW_R_WITH_ARROW)

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

// [[arrow::export]]
std::shared_ptr<parquet::ArrowReaderProperties>
parquet___arrow___ArrowReaderProperties__Make(bool use_threads) {
  return std::make_shared<parquet::ArrowReaderProperties>(use_threads);
}

// [[arrow::export]]
void parquet___arrow___ArrowReaderProperties__set_use_threads(
    const std::shared_ptr<parquet::ArrowReaderProperties>& properties,
    bool use_threads) {
  properties->set_use_threads(use_threads);
}

// [[arrow::export]]
bool parquet___arrow___ArrowReaderProperties__get_use_threads(
    const std::shared_ptr<parquet::ArrowReaderProperties>& properties,
    bool use_threads) {
  return properties->use_threads();
}

// [[arrow::export]]
bool parquet___arrow___ArrowReaderProperties__get_read_dictionary(
    const std::shared_ptr<parquet::ArrowReaderProperties>& properties,
    int column_index) {
  return properties->read_dictionary(column_index);
}

// [[arrow::export]]
void parquet___arrow___ArrowReaderProperties__set_read_dictionary(
    const std::shared_ptr<parquet::ArrowReaderProperties>& properties,
    int column_index, bool read_dict) {
  properties->set_read_dictionary(column_index, read_dict);
}

// [[arrow::export]]
std::unique_ptr<parquet::arrow::FileReader> parquet___arrow___FileReader__OpenFile(
    const std::shared_ptr<arrow::io::RandomAccessFile>& file,
    const std::shared_ptr<parquet::ArrowReaderProperties>& props) {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  parquet::arrow::FileReaderBuilder builder;
  PARQUET_THROW_NOT_OK(builder.Open(file));
  PARQUET_THROW_NOT_OK(builder.properties(*props)->Build(&reader));
  return reader;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> parquet___arrow___FileReader__ReadTable1(
    const std::unique_ptr<parquet::arrow::FileReader>& reader) {
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
  return table;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> parquet___arrow___FileReader__ReadTable2(
    const std::unique_ptr<parquet::arrow::FileReader>& reader,
    const std::vector<int>& column_indices) {
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(column_indices, &table));
  return table;
}

// [[arrow::export]]
void write_parquet_file(const std::shared_ptr<arrow::Table>& table,
                        std::string filename) {
  std::shared_ptr<arrow::io::OutputStream> sink;
  PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(filename, &sink));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                  sink, table->num_rows()));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> parquet___arrow___FileReader__GetSchema(
    const std::unique_ptr<parquet::arrow::FileReader>& reader) {
  std::shared_ptr<arrow::Schema> schema;
  STOP_IF_NOT_OK(reader->GetSchema(&schema));
  return schema;
}

#endif
