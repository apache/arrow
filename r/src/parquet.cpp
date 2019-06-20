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
std::unique_ptr<parquet::arrow::FileReader> parquet___arrow___ParquetFileReader__OpenFile(const std::shared_ptr<arrow::io::RandomAccessFile>& file) {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
    parquet::arrow::OpenFile(file, arrow::default_memory_pool(), &reader));
  return reader;
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> parquet___arrow___ParquetFileReader__Read(const std::unique_ptr<parquet::arrow::FileReader>& reader) {
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

  return table;
}

// [[arrow::export]]
void write_parquet_file(const std::shared_ptr<arrow::Table>& table,
                        std::string filename) {
#ifdef ARROW_R_WITH_PARQUET
  std::shared_ptr<arrow::io::OutputStream> sink;
  PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(filename, &sink));
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                  sink, table->num_rows()));
#else
  Rcpp::stop("Support for Parquet is not available.");
#endif
}
#endif
