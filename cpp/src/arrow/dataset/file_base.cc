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

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <vector>

#include "arrow/dataset/filter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/stl.h"

namespace arrow {
namespace dataset {

Status FileSource::Open(std::shared_ptr<arrow::io::RandomAccessFile>* out) const {
  switch (type_) {
    case PATH:
      return filesystem_->OpenInputFile(path_, out);
    case BUFFER:
      *out = std::make_shared<::arrow::io::BufferReader>(buffer_);
      return Status::OK();
  }

  return Status::OK();
}

Status FileBasedDataFragment::Scan(std::shared_ptr<ScanContext> scan_context,
                                   ScanTaskIterator* out) {
  return format_->ScanFile(source_, scan_options_, scan_context, out);
}

FileSystemBasedDataSource::FileSystemBasedDataSource(
    fs::FileSystem* filesystem, fs::PathForest forest,
    std::shared_ptr<Expression> source_partition, PathPartitions partitions,
    std::shared_ptr<FileFormat> format)
    : DataSource(std::move(source_partition)),
      filesystem_(filesystem),
      forest_(std::move(forest)),
      partitions_(std::move(partitions)),
      format_(std::move(format)) {}

Status FileSystemBasedDataSource::Make(fs::FileSystem* filesystem,
                                       std::vector<fs::FileStats> stats,
                                       std::shared_ptr<Expression> source_partition,
                                       PathPartitions partitions,
                                       std::shared_ptr<FileFormat> format,
                                       std::shared_ptr<DataSource>* out) {
  fs::PathForest forest;
  RETURN_NOT_OK(fs::PathTree::Make(stats, &forest));
  out->reset(new FileSystemBasedDataSource(filesystem, std::move(forest),
                                           std::move(source_partition),
                                           std::move(partitions), std::move(format)));
  return Status::OK();
}

DataFragmentIterator FileSystemBasedDataSource::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> options) {
  std::vector<std::unique_ptr<fs::FileStats>> files;

  auto visitor = [&files](const fs::FileStats& stats) {
    if (stats.IsFile()) {
      files.emplace_back(new fs::FileStats(stats));
    }
    return Status::OK();
  };
  // The matcher ensures that directories (and their descendants) are not
  // visited.
  auto matcher = [this, options](const fs::FileStats& stats, bool* match) {
    *match = this->PartitionMatches(stats, options->filter);
    return Status::OK();
  };

  for (auto tree : forest_) {
    DCHECK_OK(tree->Visit(visitor, matcher));
  }

  auto file_it = MakeVectorIterator(std::move(files));
  auto file_to_fragment = [options, this](std::unique_ptr<fs::FileStats> stats,
                                          std::shared_ptr<DataFragment>* out) {
    std::unique_ptr<DataFragment> fragment;
    FileSource src(stats->path(), filesystem_);

    RETURN_NOT_OK(format_->MakeFragment(src, options, &fragment));
    *out = std::move(fragment);
    return Status::OK();
  };

  return MakeMaybeMapIterator(file_to_fragment, std::move(file_it));
}

bool FileSystemBasedDataSource::PartitionMatches(const fs::FileStats& stats,
                                                 std::shared_ptr<Expression> filter) {
  if (filter == nullptr) {
    return true;
  }

  auto found = partitions_.find(stats.path());
  if (found == partitions_.end()) {
    // No partition attached to current node (directory or file), continue.
    return true;
  }

  auto c = found->second->Assume(*filter);
  if (!c.ok()) {
    // Could not simplify expression move on!
    return true;
  }

  // TODO: pass simplified expressions to children
  auto expr = std::move(c).ValueOrDie();
  if (expr->IsNull() || expr->IsTrivialFalseCondition()) {
    // selector is not satisfiable; don't recurse in this branch.
    return false;
  }

  return true;
}

}  // namespace dataset
}  // namespace arrow
