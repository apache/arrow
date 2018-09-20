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

#include "arrow_types.h"
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>

using namespace Rcpp;
using namespace arrow;

namespace arrow{
namespace r{

template <int RTYPE, typename Vec = Rcpp::Vector<RTYPE> >
class SimpleRBuffer : public arrow::Buffer {
public:

  SimpleRBuffer(Vec vec) :
    Buffer(reinterpret_cast<const uint8_t*>(vec.begin()), vec.size() * sizeof(typename Vec::stored_type) ),
    vec_(vec)
  {}

private:

  // vec_ holds the memory
  Vec vec_;
};

template <int RTYPE, typename Type>
std::shared_ptr<arrow::Array> SimpleArray(SEXP x){
  // a simple buffer that owns the memory of `x`
  auto buffer = std::make_shared<SimpleRBuffer<RTYPE>>(x);
  auto type = std::make_shared<Type>();

  auto data = ArrayData::Make(
    type,
    LENGTH(x),
    {nullptr, buffer}, /* for now we just use a nullptr for the null bitmap buffer */
    0, /*null_count */
    0 /*offset*/
  );

  // return the right Array class
  return std::make_shared<arrow::NumericArray<Type>>(data);
}

}
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> rvector_to_Array(SEXP x){
  switch(TYPEOF(x)){
  case INTSXP:
    if (Rf_isFactor(x)) {
      break;
    }
    return arrow::r::SimpleArray<INTSXP, arrow::Int32Type>(x);
  case REALSXP:
    // TODO: Dates, ...
    return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
  case RAWSXP:
    return arrow::r::SimpleArray<REALSXP, arrow::DoubleType>(x);
  default:
    break;
  }

  stop("not handled");
  return nullptr;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> dataframe_to_RecordBatch(DataFrame tbl){
  CharacterVector names = tbl.names();

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  int nc = tbl.size();
  for(int i=0; i<tbl.size(); i++){
    arrays.push_back(rvector_to_Array(tbl[i]));
    fields.push_back(std::make_shared<arrow::Field>(std::string(names[i]), arrays[i]->type()));
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::RecordBatch::Make(schema, tbl.nrow(), std::move(arrays));
}

// [[Rcpp::export]]
int RecordBatch_num_columns(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->num_columns();
}

// [[Rcpp::export]]
int RecordBatch_num_rows(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->num_rows();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> RecordBatch_schema(const std::shared_ptr<arrow::RecordBatch>& x){
  return x->schema();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Array> RecordBatch_column(const std::shared_ptr<arrow::RecordBatch>& batch, int i){
  return batch->column(i);
}

template <int RTYPE>
inline SEXP simple_Array_to_Vector(const std::shared_ptr<arrow::Array>& array ){
  // ignoring null buffer for now
  using stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  auto start = reinterpret_cast<const stored_type*>(array->data()->buffers[1]->data());

  return Rcpp::wrap(start, start + array->length());
}

// [[Rcpp::export]]
SEXP Array_as_vector(const std::shared_ptr<arrow::Array>& array){
  switch(array->type_id()){
  case Type::INT8: return simple_Array_to_Vector<RAWSXP>(array);
  case Type::INT32: return simple_Array_to_Vector<INTSXP>(array);
  case Type::DOUBLE: return simple_Array_to_Vector<REALSXP>(array);
  default:
    break;
  }

  stop(tfm::format("cannot handle Array of type %d", array->type_id()));
  return R_NilValue;
}

template <int RTYPE>
inline SEXP simple_ChunkedArray_to_Vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array){
  using stored_type = typename Rcpp::Vector<RTYPE>::stored_type;
  Rcpp::Vector<RTYPE> out = no_init(chunked_array->length());
  auto p = out.begin();

  for (int i=0; i<chunked_array->num_chunks(); i++) {
    auto chunk = chunked_array->chunk(i);

    // copy the data
    p = std::copy_n(
      reinterpret_cast<const stored_type*>(chunk->data()->buffers[1]->data()),
      chunk->length(),
      p
    );

    // set NA using the bitmap, TODO
  }
  return out;
}


SEXP ChunkedArray_as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array){
  switch(chunked_array->type()->id()){
  case Type::INT8: return simple_ChunkedArray_to_Vector<RAWSXP>(chunked_array);
  case Type::INT32: return simple_ChunkedArray_to_Vector<INTSXP>(chunked_array);
  case Type::DOUBLE: return simple_ChunkedArray_to_Vector<REALSXP>(chunked_array);
  default:
    break;
  }

  stop(tfm::format("cannot handle Array of type %d", chunked_array->type()->id()));
  return R_NilValue;
}

// [[Rcpp::export]]
List RecordBatch_to_dataframe(const std::shared_ptr<arrow::RecordBatch>& batch){
  int nc = batch->num_columns();
  int nr = batch->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for(int i=0; i<nc; i++) {
    tbl[i] = Array_as_vector(batch->column(i));
    names[i] = batch->column_name(i);
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbf_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> dataframe_to_Table(DataFrame tbl){
  auto rb = dataframe_to_RecordBatch(tbl);

  std::shared_ptr<arrow::Table> out;
  auto status = arrow::Table::FromRecordBatches({ std::move(rb) }, &out);
  return out;
}

// [[Rcpp::export]]
int Table_num_columns(const std::shared_ptr<arrow::Table>& x){
  return x->num_columns();
}

// [[Rcpp::export]]
int Table_num_rows(const std::shared_ptr<arrow::Table>& x){
  return x->num_rows();
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Schema> Table_schema(const std::shared_ptr<arrow::Table>& x){
  return x->schema();
}

// [[Rcpp::export]]
int RecordBatch_to_file(const std::shared_ptr<arrow::RecordBatch>& batch, std::string path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;

  R_ERROR_NOT_OK(arrow::io::FileOutputStream::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileWriter::Open(stream.get(), batch->schema(), &file_writer));
  R_ERROR_NOT_OK(file_writer->WriteRecordBatch(*batch, true));
  R_ERROR_NOT_OK(file_writer->Close());

  int64_t offset;
  R_ERROR_NOT_OK(stream->Tell(&offset));
  R_ERROR_NOT_OK(stream->Close());
  return offset;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::RecordBatch> read_record_batch_(std::string path) {
  std::shared_ptr<arrow::io::ReadableFile> stream;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> rbf_reader;

  R_ERROR_NOT_OK(arrow::io::ReadableFile::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(stream, &rbf_reader));

  std::shared_ptr<arrow::RecordBatch> batch;
  R_ERROR_NOT_OK(rbf_reader->ReadRecordBatch(0, &batch));

  R_ERROR_NOT_OK(stream->Close());
  return batch;
}

// [[Rcpp::export]]
int Table_to_file(const std::shared_ptr<arrow::Table>& table, std::string path) {
  std::shared_ptr<arrow::io::OutputStream> stream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;

  R_ERROR_NOT_OK(arrow::io::FileOutputStream::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileWriter::Open(stream.get(), table->schema(), &file_writer));
  R_ERROR_NOT_OK(file_writer->WriteTable(*table));
  R_ERROR_NOT_OK(file_writer->Close());

  int64_t offset;
  R_ERROR_NOT_OK(stream->Tell(&offset));
  R_ERROR_NOT_OK(stream->Close());
  return offset;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::Table> read_table_(std::string path) {
  std::shared_ptr<arrow::io::ReadableFile> stream;
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> rbf_reader;

  R_ERROR_NOT_OK(arrow::io::ReadableFile::Open(path, &stream));
  R_ERROR_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(stream, &rbf_reader));

  int num_batches = rbf_reader->num_record_batches();
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches(num_batches);
  for (int i=0; i<num_batches; i++) {
    R_ERROR_NOT_OK(rbf_reader->ReadRecordBatch(i, &batches[i]));
  }

  std::shared_ptr<arrow::Table> table;
  R_ERROR_NOT_OK(arrow::Table::FromRecordBatches(std::move(batches), &table)) ;
  R_ERROR_NOT_OK(stream->Close());
  return table;
}

// [[Rcpp::export]]
List Table_to_dataframe(const std::shared_ptr<arrow::Table>& table){
  int nc = table->num_columns();
  int nr = table->num_rows();
  List tbl(nc);
  CharacterVector names(nc);
  for(int i=0; i<nc; i++) {
    auto column = table->column(i);
    tbl[i] = ChunkedArray_as_vector(column->data());
    names[i] = column->name();
  }
  tbl.attr("names") = names;
  tbl.attr("class") = CharacterVector::create("tbf_df", "tbl", "data.frame");
  tbl.attr("row.names") = IntegerVector::create(NA_INTEGER, -nr);
  return tbl;
}
