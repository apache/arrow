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

// This gets included in arrowExports.cpp

#pragma once

#include "./arrow_cpp11.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/dataset/type_fwd.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/type_fwd.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <arrow/util/compression.h>
#include <arrow/util/value_parsing.h>

namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;

namespace arrow {

namespace compute {

struct CastOptions;

}  // namespace compute

namespace csv {

class TableReader;
struct ConvertOptions;
struct ReadOptions;
struct ParseOptions;

}  // namespace csv

namespace json {

class TableReader;
struct ReadOptions;
struct ParseOptions;

}  // namespace json

namespace dataset {
class DirectoryPartitioning;
class HivePartitioning;
}  // namespace dataset

}  // namespace arrow

namespace parquet {

struct ParquetVersion {
  enum type {
    // forward declaration
  };
};

class ReaderProperties;
class ArrowReaderProperties;

class WriterProperties;
class WriterPropertiesBuilder;
class ArrowWriterProperties;
class ArrowWriterPropertiesBuilder;

namespace arrow {

class FileReader;
class FileWriter;

}  // namespace arrow
}  // namespace parquet

namespace cpp11 {

// Overrides of default R6 class names:
#define R6_CLASS_NAME(CLASS, NAME)                                         \
  template <>                                                              \
  struct r6_class_name<CLASS> {                                            \
    static const char* get(const std::shared_ptr<CLASS>&) { return NAME; } \
  }

R6_CLASS_NAME(arrow::csv::ReadOptions, "CsvReadOptions");
R6_CLASS_NAME(arrow::csv::ParseOptions, "CsvParseOptions");
R6_CLASS_NAME(arrow::csv::ConvertOptions, "CsvConvertOptions");
R6_CLASS_NAME(arrow::csv::TableReader, "CsvTableReader");

R6_CLASS_NAME(parquet::ArrowReaderProperties, "ParquetArrowReaderProperties");
R6_CLASS_NAME(parquet::ArrowWriterProperties, "ParquetArrowWriterProperties");
R6_CLASS_NAME(parquet::WriterProperties, "ParquetWriterProperties");
R6_CLASS_NAME(parquet::arrow::FileReader, "ParquetFileReader");
R6_CLASS_NAME(parquet::WriterPropertiesBuilder, "ParquetWriterPropertiesBuilder");
R6_CLASS_NAME(parquet::arrow::FileWriter, "ParquetFileWriter");

R6_CLASS_NAME(arrow::ipc::feather::Reader, "FeatherReader");

R6_CLASS_NAME(arrow::json::ReadOptions, "JsonReadOptions");
R6_CLASS_NAME(arrow::json::ParseOptions, "JsonParseOptions");
R6_CLASS_NAME(arrow::json::TableReader, "JsonTableReader");

#undef R6_CLASS_NAME

// Declarations of discriminated base classes.
// Definitions reside in corresponding .cpp files.
template <>
struct r6_class_name<fs::FileSystem> {
  static const char* get(const std::shared_ptr<fs::FileSystem>&);
};

template <>
struct r6_class_name<arrow::Array> {
  static const char* get(const std::shared_ptr<arrow::Array>&);
};

template <>
struct r6_class_name<arrow::Scalar> {
  static const char* get(const std::shared_ptr<arrow::Scalar>&);
};

template <>
struct r6_class_name<arrow::DataType> {
  static const char* get(const std::shared_ptr<arrow::DataType>&);
};

template <>
struct r6_class_name<ds::Dataset> {
  static const char* get(const std::shared_ptr<ds::Dataset>&);
};

template <>
struct r6_class_name<ds::FileFormat> {
  static const char* get(const std::shared_ptr<ds::FileFormat>&);
};

}  // namespace cpp11

#endif
