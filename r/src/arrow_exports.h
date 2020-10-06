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

}  // namespace arrow

namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;

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

namespace arrow {
namespace dataset {

class DirectoryPartitioning;
class HivePartitioning;

}
}

namespace cpp11 {

inline SEXP xp_to_r6(SEXP xp, SEXP r6_class) {
  // make call:  <symbol>$new(<x>)
  SEXP call = PROTECT(Rf_lang3(R_DollarSymbol, r6_class, arrow::r::symbols::new_));
  SEXP call2 = PROTECT(Rf_lang2(call, xp));

  // and then eval in arrow::
  SEXP r6 = PROTECT(Rf_eval(call2, arrow::r::ns::arrow));

  UNPROTECT(3);
  return r6;
}

template <typename T>
inline SEXP shared_ptr_to_r6(const std::shared_ptr<T>& x, const std::string& r_class_name) {
  if (x == nullptr) return R_NilValue;
  cpp11::external_pointer<std::shared_ptr<T>> xp(new std::shared_ptr<T>(x));
  SEXP r6_class = Rf_install(r_class_name.c_str());
  return xp_to_r6(xp, r6_class);
}

#define R6_HANDLE(TYPE, NAME)                      \
  template <>                                      \
  SEXP as_sexp(const std::shared_ptr<TYPE>& ptr) { \
    return shared_ptr_to_r6<TYPE>(ptr, NAME);      \
  }

R6_HANDLE(arrow::RecordBatch, "RecordBatch")
R6_HANDLE(arrow::Table, "Table")
R6_HANDLE(arrow::Schema, "Schema")
R6_HANDLE(arrow::Buffer, "Buffer")
R6_HANDLE(arrow::MemoryPool, "MemoryPool")
R6_HANDLE(arrow::Field, "Field")
R6_HANDLE(arrow::ChunkedArray, "ChunkedArray")

R6_HANDLE(arrow::dataset::DirectoryPartitioning, "DirectoryPartitioning")
R6_HANDLE(arrow::dataset::HivePartitioning, "HivePartitioning")
R6_HANDLE(arrow::dataset::PartitioningFactory, "PartitioningFactory")
R6_HANDLE(arrow::dataset::DatasetFactory, "DatasetFactory")

R6_HANDLE(parquet::WriterPropertiesBuilder, "ParquetWriterPropertiesBuilder")
R6_HANDLE(parquet::ArrowWriterProperties, "ParquetArrowWriterProperties")
R6_HANDLE(parquet::WriterProperties, "ParquetWriterProperties")
R6_HANDLE(parquet::arrow::FileWriter, "ParquetFileWriter")
R6_HANDLE(parquet::arrow::FileReader, "ParquetFileReader")
R6_HANDLE(parquet::ArrowReaderProperties, "ParquetReaderProperties")

R6_HANDLE(arrow::ipc::feather::Reader, "FeatherReader")

R6_HANDLE(arrow::csv::TableReader, "CsvTableReader")
R6_HANDLE(arrow::csv::ReadOptions, "CsvReadOptions")
R6_HANDLE(arrow::csv::ParseOptions, "CsvParseOptions")
R6_HANDLE(arrow::csv::ConvertOptions, "CsvConvertOptions")

R6_HANDLE(arrow::dataset::Expression, "Expression")

R6_HANDLE(arrow::compute::CastOptions, "CastOptions")

R6_HANDLE(arrow::util::Codec, "Codec")

R6_HANDLE(arrow::ipc::Message, "Message")
R6_HANDLE(arrow::ipc::MessageReader, "MessageReader")

}  // namespace cpp11

#endif
