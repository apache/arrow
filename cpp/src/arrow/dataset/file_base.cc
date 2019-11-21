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
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  if (type() == PATH) {
    return filesystem()->OpenInputFile(path());
  }

  return std::make_shared<::arrow::io::BufferReader>(buffer());
}

Result<ScanTaskIterator> FileDataFragment::Scan(ScanContextPtr context) {
  return format_->ScanFile(source_, scan_options_, context);
}

FileSystemDataSource::FileSystemDataSource(fs::FileSystemPtr filesystem,
                                           fs::PathForest forest,
                                           ExpressionPtr source_partition,
                                           ExpressionVector file_partitions,
                                           FileFormatPtr format)
    : DataSource(std::move(source_partition)),
      filesystem_(std::move(filesystem)),
      forest_(std::move(forest)),
      partitions_(std::move(file_partitions)),
      format_(std::move(format)) {}

Result<DataSourcePtr> FileSystemDataSource::Make(fs::FileSystemPtr filesystem,
                                                 fs::FileStatsVector stats,
                                                 ExpressionPtr source_partition,
                                                 FileFormatPtr format) {
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathTree::Make(std::move(stats)));

  return DataSourcePtr(new FileSystemDataSource(std::move(filesystem), std::move(forest),
                                                std::move(source_partition), {},
                                                std::move(format)));
}

Result<DataSourcePtr> FileSystemDataSource::Make(
    fs::FileSystemPtr filesystem, fs::FileStatsVector stats,
    ExpressionPtr source_partition, const PartitionSchemePtr& partition_scheme,
    FileFormatPtr format) {
  ExpressionVector partitions(stats.size(), scalar(true));
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathTree::Make(std::move(stats)));

  // apply partition_scheme to forest to derive partitions
  auto visitor = [&](const fs::FileStats& stats, int i) {
    auto segments = fs::internal::SplitAbstractPath(stats.path());

    if (segments.size() > 0) {
      auto segment_index = static_cast<int>(segments.size()) - 1;
      auto maybe_partition = partition_scheme->Parse(segments.back(), segment_index);

      partitions[i] = std::move(maybe_partition).ValueOr(scalar(true));
    }

    return Status::OK();
  };

  for (auto tree : forest) {
    DCHECK_OK(tree.Visit(visitor));
  }

  return DataSourcePtr(new FileSystemDataSource(
      std::move(filesystem), std::move(forest), std::move(source_partition),
      std::move(partitions), std::move(format)));
}

std::string FileSystemDataSource::ToString() const {
  std::string repr = "FileSystemDataSource:";

  if (forest_.size() == 0) {
    return repr + " []";
  }

  auto visitor = [&](const fs::FileStats& stats, int i) {
    auto partition = partitions_[i];
    auto segments = fs::internal::SplitAbstractPath(stats.path());

    repr += "\n" + std::string(' ', segments.size() * 2) + stats.path() +
            partition->ToString();
    return Status::OK();
  };

  for (auto tree : forest_) {
    DCHECK_OK(tree.Visit(visitor));
  }

  return repr;
}

DataFragmentIterator FileSystemDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  std::vector<std::unique_ptr<fs::FileStats>> files;

  auto visitor = [&](const fs::FileStats& stats, int i) -> fs::PathTree::MaybePrune {
    auto partition = partitions_[i];
    auto expr = options->filter->Assume(partition);

    if (expr->IsNull() || expr->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathTree::Prune;
    }

    if (stats.IsFile()) {
      files.emplace_back(new fs::FileStats(stats));
    }

    return fs::PathTree::Continue;
  };

  for (auto tree : forest_) {
    DCHECK_OK(tree.Visit(visitor));
  }

  auto file_it = MakeVectorIterator(std::move(files));
  auto file_to_fragment = [options, this](std::unique_ptr<fs::FileStats> stats,
                                          std::shared_ptr<DataFragment>* out) {
    FileSource src(stats->path(), filesystem_.get());
    ARROW_ASSIGN_OR_RAISE(*out, format_->MakeFragment(src, options));
    return Status::OK();
  };

  return MakeMaybeMapIterator(file_to_fragment, std::move(file_it));
}

}  // namespace dataset
}  // namespace arrow
