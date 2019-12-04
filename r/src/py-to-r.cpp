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
std::shared_ptr<arrow::Array> ImportArray(uintptr_t array) {
  return VALUE_OR_STOP(arrow::ImportArray(reinterpret_cast<struct ArrowArray*>(array)));
}

// [[arrow::export]]
std::shared_ptr<arrow::RecordBatch> ImportRecordBatch(uintptr_t array) {
  return VALUE_OR_STOP(
      arrow::ImportRecordBatch(reinterpret_cast<struct ArrowArray*>(array)));
}

// [[arrow::export]]
uintptr_t allocate_arrow_array() { return reinterpret_cast<uintptr_t>(new ArrowArray); }

// [[arrow::export]]
void delete_arrow_array(uintptr_t ptr) {
  delete reinterpret_cast<struct ArrowArray*>(ptr);
}

// [[arrow::export]]
void ExportArray(const std::shared_ptr<arrow::Array>& array, uintptr_t ptr) {
  STOP_IF_NOT_OK(arrow::ExportArray(*array, reinterpret_cast<struct ArrowArray*>(ptr)));
}

// [[arrow::export]]
void ExportRecordBatch(const std::shared_ptr<arrow::RecordBatch>& array, uintptr_t ptr) {
  STOP_IF_NOT_OK(
      arrow::ExportRecordBatch(*array, reinterpret_cast<struct ArrowArray*>(ptr)));
}

#endif
