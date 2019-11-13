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

// [[arrow::export]]
std::shared_ptr<arrow::Array> ImportArray(size_t array) {
  std::shared_ptr<arrow::Array> out;
  STOP_IF_NOT_OK(arrow::ImportArray(reinterpret_cast<struct ArrowArray*>(array), &out));
  return out;
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ImportRecordBatch(size_t array) {
  std::shared_ptr<arrow::RecordBatch> out;
  STOP_IF_NOT_OK(
      arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(array), &out));
  return out;
}

// [[arrow::export]]
size_t allocate_arrow_array() { return reinterpret_cast<size_t>(new ArrowArray); }

// [[arrow::export]]
void delete_arrow_array(size_t ptr) { delete reinterpret_cast<struct ArrowArray*>(ptr); }

// [[arrow::export]]
void ExportArray(const std::shared_ptr<arrow::Array>& array, size_t ptr) {
  STOP_IF_NOT_OK(arrow::ExportArray(*array, reinterpret_cast<struct ArrowArray*>(ptr)));
}

// [[arrow::export]]
void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch>& array, size_t ptr) {
  STOP_IF_NOT_OK(
      arrow::ExportRecordBatch(*array, reinterpret_cast<struct ArrowArray*>(ptr)));
}

#endif
