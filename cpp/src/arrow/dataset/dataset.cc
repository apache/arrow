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

#include "arrow/dataset/dataset.h"

#include <memory>
#include <utility>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/iterator.h"
#include "arrow/util/make_unique.h"

namespace arrow {
namespace dataset {

DataFragment::DataFragment(ScanOptionsPtr scan_options)
    : scan_options_(std::move(scan_options)), partition_expression_(scalar(true)) {}

SimpleDataFragment::SimpleDataFragment(
    std::vector<std::shared_ptr<RecordBatch>> record_batches, ScanOptionsPtr scan_options)
    : DataFragment(std::move(scan_options)), record_batches_(std::move(record_batches)) {}

Result<ScanTaskIterator> SimpleDataFragment::Scan(ScanContextPtr context) {
  // Make an explicit copy of record_batches_ to ensure Scan can be called
  // multiple times.
  auto batches_it = MakeVectorIterator(record_batches_);

  // RecordBatch -> ScanTask
  auto scan_options = scan_options_;
  auto fn = [=](std::shared_ptr<RecordBatch> batch) -> ScanTaskPtr {
    std::vector<std::shared_ptr<RecordBatch>> batches{batch};
    return ::arrow::internal::make_unique<SimpleScanTask>(
        std::move(batches), std::move(scan_options), std::move(context));
  };

  return MakeMapIterator(fn, std::move(batches_it));
}

Result<DatasetPtr> Dataset::Make(DataSourceVector sources,
                                 std::shared_ptr<Schema> schema) {
  return DatasetPtr(new Dataset(std::move(sources), std::move(schema)));
}

Result<ScannerBuilderPtr> Dataset::NewScan(ScanContextPtr context) {
  return std::make_shared<ScannerBuilder>(this->shared_from_this(), context);
}

Result<ScannerBuilderPtr> Dataset::NewScan() {
  return NewScan(std::make_shared<ScanContext>());
}

bool DataSource::AssumePartitionExpression(
    const ScanOptionsPtr& scan_options, ScanOptionsPtr* simplified_scan_options) const {
  if (partition_expression_ == nullptr) {
    if (simplified_scan_options != nullptr) {
      *simplified_scan_options = scan_options;
    }
    return true;
  }

  auto expr = scan_options->filter->Assume(*partition_expression_);
  if (expr->IsNull() || expr->Equals(false)) {
    // selector is not satisfiable; yield no fragments
    return false;
  }

  if (simplified_scan_options != nullptr) {
    auto copy = std::make_shared<ScanOptions>(*scan_options);
    copy->filter = std::move(expr);
    *simplified_scan_options = std::move(copy);
  }
  return true;
}

DataFragmentIterator DataSource::GetFragments(ScanOptionsPtr scan_options) {
  ScanOptionsPtr simplified_scan_options;
  if (!AssumePartitionExpression(scan_options, &simplified_scan_options)) {
    return MakeEmptyIterator<DataFragmentPtr>();
  }
  return GetFragmentsImpl(std::move(simplified_scan_options));
}

DataFragmentIterator SimpleDataSource::GetFragmentsImpl(ScanOptionsPtr scan_options) {
  return MakeVectorIterator(fragments_);
}

DataFragmentIterator TreeDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  return GetFragmentsFromSources(children_, options);
}

}  // namespace dataset
}  // namespace arrow
