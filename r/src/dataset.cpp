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

// // [[arrow::export]]
// std::shared_ptr<arrow::dataset::ScanTaskIterator> dataset___DataFragment__Scan(const std::shared_ptr<arrow::dataset::DataFragment>& fragment, const std::shared_ptr<arrow::dataset::ScanContext>& scan_context) {
//   std::shared_ptr<arrow::dataset::ScanTaskIterator> iterator;
//   fragment->Scan(scan_context, iterator.get());
//   return iterator;
// }

// [[arrow::export]]
bool dataset___DataFragment__splittable(const std::shared_ptr<arrow::dataset::DataFragment>& fragment) {
  return fragment->splittable();
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::ScanOptions> dataset___DataFragment__scan_options(const std::shared_ptr<arrow::dataset::DataFragment>& fragment) {
  return fragment->scan_options();
}

// [[arrow::export]]
std::shared_ptr<arrow::dataset::SimpleDataFragment> dataset___SimpleDataFragment__create(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
  return std::make_shared<arrow::dataset::SimpleDataFragment>(batches);
}
