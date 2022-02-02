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

#include <arrow/c/bridge.h>

// [[arrow::export]]
double external_pointer_addr_double(SEXP external_pointer) {
  // potentially lossy conversion to double needed for the current
  // implementation of import/export to Python
  return reinterpret_cast<uintptr_t>(R_ExternalPtrAddr(external_pointer));
}

// [[arrow::export]]
std::string external_pointer_addr_character(SEXP external_pointer) {
  void* ptr_void = R_ExternalPtrAddr(external_pointer);
  uint64_t ptr_int64 = reinterpret_cast<uintptr_t>(ptr_void);
  return std::to_string(ptr_int64);
}

// [[arrow::export]]
cpp11::doubles external_pointer_addr_integer64(SEXP external_pointer) {
  cpp11::writable::doubles out(1);
  void* ptr_void = R_ExternalPtrAddr(external_pointer);
  uint64_t ptr_int64 = reinterpret_cast<uintptr_t>(ptr_void);
  memcpy(REAL(out), &ptr_int64, sizeof(uint64_t));
  out.attr("class") = "integer64";
  return out;
}

// [[arrow::export]]
cpp11::raws external_pointer_addr_raw(SEXP external_pointer) {
  cpp11::writable::raws out(sizeof(void*));
  void* ptr_value = R_ExternalPtrAddr(external_pointer);
  memcpy(RAW(out), &ptr_value, sizeof(void*));
  return out;
}

// [[arrow::export]]
arrow::r::Pointer<struct ArrowSchema> allocate_arrow_schema() { return {}; }

// [[arrow::export]]
void delete_arrow_schema(arrow::r::Pointer<struct ArrowSchema> ptr) { ptr.finalize(); }

// [[arrow::export]]
arrow::r::Pointer<struct ArrowArray> allocate_arrow_array() { return {}; }

// [[arrow::export]]
void delete_arrow_array(arrow::r::Pointer<struct ArrowArray> ptr) { ptr.finalize(); }

// [[arrow::export]]
arrow::r::Pointer<struct ArrowArrayStream> allocate_arrow_array_stream() { return {}; }

// [[arrow::export]]
void delete_arrow_array_stream(arrow::r::Pointer<struct ArrowArrayStream> ptr) {
  ptr.finalize();
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> ImportArray(arrow::r::Pointer<struct ArrowArray> array,
                                          arrow::r::Pointer<struct ArrowSchema> schema) {
  return ValueOrStop(arrow::ImportArray(array, schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ImportRecordBatch(
    arrow::r::Pointer<struct ArrowArray> array,
    arrow::r::Pointer<struct ArrowSchema> schema) {
  return ValueOrStop(arrow::ImportRecordBatch(array, schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ImportSchema(
    arrow::r::Pointer<struct ArrowSchema> schema) {
  return ValueOrStop(arrow::ImportSchema(schema));
}

// [[arrow::export]]
std::shared_ptr<arrow::Field> ImportField(arrow::r::Pointer<struct ArrowSchema> field) {
  return ValueOrStop(arrow::ImportField(field));
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> ImportType(arrow::r::Pointer<struct ArrowSchema> type) {
  return ValueOrStop(arrow::ImportType(type));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatchReader> ImportRecordBatchReader(
    arrow::r::Pointer<struct ArrowArrayStream> stream) {
  return ValueOrStop(arrow::ImportRecordBatchReader(stream));
}

// [[arrow::export]]
void ExportType(const std::shared_ptr<arrow::DataType>& type,
                arrow::r::Pointer<struct ArrowSchema> ptr) {
  StopIfNotOk(arrow::ExportType(*type, ptr));
}

// [[arrow::export]]
void ExportField(const std::shared_ptr<arrow::Field>& field,
                 arrow::r::Pointer<struct ArrowSchema> ptr) {
  StopIfNotOk(arrow::ExportField(*field, ptr));
}

// [[arrow::export]]
void ExportSchema(const std::shared_ptr<arrow::Schema>& schema,
                  arrow::r::Pointer<struct ArrowSchema> ptr) {
  StopIfNotOk(arrow::ExportSchema(*schema, ptr));
}

// [[arrow::export]]
void ExportArray(const std::shared_ptr<arrow::Array>& array,
                 arrow::r::Pointer<struct ArrowArray> array_ptr,
                 arrow::r::Pointer<struct ArrowSchema> schema_ptr) {
  StopIfNotOk(arrow::ExportArray(*array, array_ptr, schema_ptr));
}

// [[arrow::export]]
void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                       arrow::r::Pointer<struct ArrowArray> array_ptr,
                       arrow::r::Pointer<struct ArrowSchema> schema_ptr) {
  StopIfNotOk(arrow::ExportRecordBatch(*batch, array_ptr, schema_ptr));
}

// [[arrow::export]]
void ExportRecordBatchReader(const std::shared_ptr<arrow::RecordBatchReader>& reader,
                             arrow::r::Pointer<struct ArrowArrayStream> stream_ptr) {
  StopIfNotOk(arrow::ExportRecordBatchReader(reader, stream_ptr));
}

#endif
