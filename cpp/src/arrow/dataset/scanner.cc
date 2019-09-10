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

#include "arrow/dataset/scanner.h"

#include <algorithm>

#include "arrow/dataset/dataset.h"

namespace arrow {
namespace dataset {

std::unique_ptr<RecordBatchIterator> SimpleScanTask::Scan() {
  return MakeVectorIterator(record_batches_);
}

/// \brief GetFragmentsIterator transforms a vector<DataSource> in a flattened
/// Iterator<DataFragment>.
static std::unique_ptr<DataFragmentIterator> GetFragmentsIterator(
    const std::vector<std::shared_ptr<DataSource>>& sources,
    std::shared_ptr<ScanOptions> options) {
  // Iterator<DataSource>
  auto sources_it = MakeVectorIterator(sources);

  // DataSource -> Iterator<DataFragment>
  auto fn = [options](std::shared_ptr<DataSource> source)
      -> std::unique_ptr<DataFragmentIterator> { return source->GetFragments(options); };

  // Iterator<Iterator<DataFragment>>
  auto fragments_it = MakeMapIterator(fn, std::move(sources_it));

  // Iterator<DataFragment>
  return MakeFlattenIterator(std::move(fragments_it));
}

/// \brief GetScanTaskIterator transforms an Iterator<DataFragment> in a
/// flattened Iterator<ScanTask>.
static std::unique_ptr<ScanTaskIterator> GetScanTaskIterator(
    std::unique_ptr<DataFragmentIterator> fragments,
    std::shared_ptr<ScanContext> context) {
  // DataFragment -> ScanTaskIterator
  auto fn = [context](std::shared_ptr<DataFragment> fragment,
                      std::unique_ptr<ScanTaskIterator>* out) -> Status {
    return fragment->Scan(context, out);
  };

  // Iterator<Iterator<ScanTask>>
  auto maybe_scantask_it = MakeMaybeMapIterator(fn, std::move(fragments));

  // Iterator<ScanTask>
  return MakeFlattenIterator(std::move(maybe_scantask_it));
}

std::unique_ptr<ScanTaskIterator> SimpleScanner::Scan() {
  // First, transforms DataSources in a flat Iterator<DataFragment>. This
  // iterator is lazily constructed, i.e. DataSource::GetFragments is never
  // invoked.
  auto fragments_it = GetFragmentsIterator(sources_, options_);
  // Second, transforms Iterator<DataFragment> into a unified
  // Iterator<ScanTask>. The first Iterator::Next invocation is going to do
  // all the work of unwinding the chained iterators.
  return GetScanTaskIterator(std::move(fragments_it), context_);
}
ScannerBuilder::ScannerBuilder(std::shared_ptr<Dataset> dataset,
                               std::shared_ptr<ScanContext> scan_context)
    : dataset_(std::move(dataset)), scan_context_(std::move(scan_context)) {}

ScannerBuilder* ScannerBuilder::Project(const std::vector<std::string>& columns) {
  return this;
}

ScannerBuilder* ScannerBuilder::AddFilter(const std::shared_ptr<Filter>& filter) {
  return this;
}

ScannerBuilder* ScannerBuilder::SetGlobalFileOptions(
    std::shared_ptr<FileScanOptions> options) {
  return this;
}

ScannerBuilder* ScannerBuilder::IncludePartitionKeys(bool include) {
  include_partition_keys_ = include;
  return this;
}

Status ScannerBuilder::Finish(std::unique_ptr<Scanner>* out) const {
  auto options = std::make_shared<ScanOptions>();
  out->reset(new SimpleScanner(dataset_->sources(), options, scan_context_));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
