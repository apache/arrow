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

// ---------- TableWriter

// [[Rcpp::export]]
void ipc___feather___TableWriter__SetDescription(
    const std::unique_ptr<arrow::ipc::feather::TableWriter>& writer,
    const std::string& description) {
  writer->SetDescription(description);
}

// [[Rcpp::export]]
void ipc___feather___TableWriter__SetNumRows(
    const std::unique_ptr<arrow::ipc::feather::TableWriter>& writer, int64_t num_rows) {
  writer->SetNumRows(num_rows);
}

// [[Rcpp::export]]
void ipc___feather___TableWriter__Append(
    const std::unique_ptr<arrow::ipc::feather::TableWriter>& writer,
    const std::string& name, const std::shared_ptr<arrow::Array>& values) {
  STOP_IF_NOT_OK(writer->Append(name, *values));
}

// [[Rcpp::export]]
void ipc___feather___TableWriter__Finalize(
    const std::unique_ptr<arrow::ipc::feather::TableWriter>& writer) {
  STOP_IF_NOT_OK(writer->Finalize());
}

// [[Rcpp::export]]
std::unique_ptr<arrow::ipc::feather::TableWriter> ipc___feather___TableWriter__Open(
    const std::shared_ptr<arrow::io::OutputStream>& stream) {
  std::unique_ptr<arrow::ipc::feather::TableWriter> writer;
  STOP_IF_NOT_OK(arrow::ipc::feather::TableWriter::Open(stream, &writer));
  return writer;
}

// [[Rcpp::export]]
void ipc___TableWriter__RecordBatch__WriteFeather(
    const std::unique_ptr<arrow::ipc::feather::TableWriter>& writer,
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  writer->SetNumRows(batch->num_rows());

  for (int i = 0; i < batch->num_columns(); i++) {
    STOP_IF_NOT_OK(writer->Append(batch->column_name(i), *batch->column(i)));
  }
  STOP_IF_NOT_OK(writer->Finalize());
}

// ----------- TableReader

// [[Rcpp::export]]
std::string ipc___feather___TableReader__GetDescription(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader) {
  return reader->GetDescription();
}

// [[Rcpp::export]]
bool ipc___feather___TableReader__HasDescription(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader) {
  return reader->HasDescription();
}

// [[Rcpp::export]]
int ipc___feather___TableReader__version(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader) {
  return reader->version();
}

// [[Rcpp::export]]
int64_t ipc___feather___TableReader__num_rows(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader) {
  return reader->num_rows();
}

// [[Rcpp::export]]
int64_t ipc___feather___TableReader__num_columns(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader) {
  return reader->num_columns();
}

// [[Rcpp::export]]
std::string ipc___feather___TableReader__GetColumnName(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader, int i) {
  return reader->GetColumnName(i);
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Column> ipc___feather___TableReader__GetColumn(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader, int i) {
  std::shared_ptr<arrow::Column> column;
  STOP_IF_NOT_OK(reader->GetColumn(i, &column));
  return column;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> ipc___feather___TableReader__Read(
    const std::unique_ptr<arrow::ipc::feather::TableReader>& reader, SEXP columns) {
  std::shared_ptr<arrow::Table> table;

  switch (TYPEOF(columns)) {
    case INTSXP: {
      R_xlen_t n = XLENGTH(columns);
      std::vector<int> indices(n);
      int* p_columns = INTEGER(columns);
      for (int i = 0; i < n; i++) {
        indices[i] = p_columns[i] - 1;
      }
      STOP_IF_NOT_OK(reader->Read(indices, &table));
      break;
    }
    case STRSXP: {
      R_xlen_t n = XLENGTH(columns);
      std::vector<std::string> names(n);
      for (R_xlen_t i = 0; i < n; i++) {
        names[i] = CHAR(STRING_ELT(columns, i));
      }
      STOP_IF_NOT_OK(reader->Read(names, &table));
      break;
    }
    case NILSXP:
      STOP_IF_NOT_OK(reader->Read(&table));
      break;
    default:
      Rcpp::stop("incompatible column specification");
      break;
  }

  return table;
}

// [[Rcpp::export]]
std::unique_ptr<arrow::ipc::feather::TableReader> ipc___feather___TableReader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& stream) {
  std::unique_ptr<arrow::ipc::feather::TableReader> reader;
  STOP_IF_NOT_OK(arrow::ipc::feather::TableReader::Open(stream, &reader));
  return reader;
}
