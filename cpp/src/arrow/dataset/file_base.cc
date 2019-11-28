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

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"

namespace arrow {

template <>
struct IterationTraits<dataset::FileSource> {
  static dataset::FileSource End() { return dataset::FileSource{"", nullptr}; }
};

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
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(stats)));

  return DataSourcePtr(new FileSystemDataSource(std::move(filesystem), std::move(forest),
                                                std::move(source_partition), {},
                                                std::move(format)));
}

Result<DataSourcePtr> FileSystemDataSource::Make(
    fs::FileSystemPtr filesystem, fs::FileStatsVector stats,
    ExpressionPtr source_partition, const PartitionSchemePtr& partition_scheme,
    FileFormatPtr format) {
  ExpressionVector partitions(stats.size(), scalar(true));
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(stats)));

  // apply partition_scheme to forest to derive partitions
  auto apply_partition_scheme = [&](fs::PathForest::Ref ref) {
    auto segments = fs::internal::SplitAbstractPath(ref.stats().path());

    if (segments.size() > 0) {
      auto segment_index = static_cast<int>(segments.size()) - 1;
      auto maybe_partition = partition_scheme->Parse(segments.back(), segment_index);

      partitions[ref.i] = std::move(maybe_partition).ValueOr(scalar(true));
    }

    return Status::OK();
  };

  DCHECK_OK(forest.Visit(apply_partition_scheme));

  return DataSourcePtr(new FileSystemDataSource(
      std::move(filesystem), std::move(forest), std::move(source_partition),
      std::move(partitions), std::move(format)));
}

std::string FileSystemDataSource::ToString() const {
  std::string repr = "FileSystemDataSource:";

  if (forest_.size() == 0) {
    return repr + " []";
  }

  DCHECK_OK(forest_.Visit([&](fs::PathForest::Ref ref) {
    repr += "\n" + ref.stats().path();

    if (!partitions_[ref.i]->Equals(true)) {
      repr += ": " + partitions_[ref.i]->ToString();
    }

    return Status::OK();
  }));

  return repr;
}

util::optional<std::pair<std::string, std::shared_ptr<Scalar>>> GetKey(
    const Expression& expr) {
  if (expr.type() != ExpressionType::COMPARISON) {
    return util::nullopt;
  }

  const auto& cmp = internal::checked_cast<const ComparisonExpression&>(expr);
  if (cmp.op() != compute::CompareOperator::EQUAL) {
    return util::nullopt;
  }

  // TODO(bkietz) allow this ordering to be flipped

  if (cmp.left_operand()->type() != ExpressionType::FIELD) {
    return util::nullopt;
  }

  if (cmp.right_operand()->type() != ExpressionType::SCALAR) {
    return util::nullopt;
  }

  return std::make_pair(
      internal::checked_cast<const FieldExpression&>(*cmp.left_operand()).name(),
      internal::checked_cast<const ScalarExpression&>(*cmp.right_operand()).value());
}

DataFragmentIterator FileSystemDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  std::vector<FileSource> files;
  std::vector<ScanOptionsPtr> options_per_file;

  auto collect_files = [&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    auto partition = partitions_[ref.i];
    auto expr = options->filter->Assume(partition);

    if (expr->IsNull() || expr->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathForest::Prune;
    }

    if (ref.stats().IsFile()) {
      files.emplace_back(ref.stats().path(), filesystem_.get());
      options_per_file.emplace_back(new ScanOptions(*options));
      options_per_file.back()->filter = expr;

      // TODO(bkietz) walk up parent partitions and get their keys as well
      if (options->projector != nullptr) {
        if (auto name_value = GetKey(*partition)) {
          RETURN_NOT_OK(options_per_file.back()->projector->SetDefaultValue(
              name_value->first, std::move(name_value->second)));
        }
      }
    }

    return fs::PathForest::Continue;
  };

  DCHECK_OK(forest_.Visit(collect_files));

  auto file_to_fragment = [options, this](FileSource src,
                                          std::shared_ptr<DataFragment>* out) {
    // TODO(bkietz) use options_per_file here
    return format_->MakeFragment(src, options).Value(out);
  };
  return MakeMaybeMapIterator(file_to_fragment, MakeVectorIterator(std::move(files)));
}

}  // namespace dataset
}  // namespace arrow
