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
    const std::string& partition_base_dir, FileFormatPtr format) {
  ExpressionVector partitions(stats.size(), scalar(true));
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(stats)));

  // TODO(bkietz) extract partition_base_dir and partition_scheme back out to discovery

  // apply partition_scheme to forest to derive partitions
  auto apply_partition_scheme = [&](fs::PathForest::Ref ref) {
    if (auto relative =
            fs::internal::RemoveAncestor(partition_base_dir, ref.stats().path())) {
      auto segments = fs::internal::SplitAbstractPath(relative->to_string());

      if (segments.size() > 0) {
        auto segment_index = static_cast<int>(segments.size()) - 1;
        auto maybe_partition = partition_scheme->Parse(segments.back(), segment_index);

        partitions[ref.i] = std::move(maybe_partition).ValueOr(scalar(true));
      }
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

struct SourceAndOptions {
  FileSource source;
  ScanOptionsPtr options;

  // required by optional
  bool operator==(const SourceAndOptions& other) const { return this == &other; }
};

DataFragmentIterator FileSystemDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  std::vector<SourceAndOptions> files;

  auto collect_files = [&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    auto partition = partitions_[ref.i];
    auto expr = options->filter->Assume(partition);

    if (expr->IsNull() || expr->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathForest::Prune;
    }

    if (ref.stats().IsFile()) {
      SourceAndOptions file = {FileSource(ref.stats().path(), filesystem_.get()),
                               options->Copy()};

      // use simplified filter
      for (auto ancestor = ref.parent(); ancestor.forest == &forest_;
           ancestor = ancestor.parent()) {
        expr = expr->Assume(partitions_[ancestor.i]);
      }
      file.options->filter = expr;

      // FIXME(bkietz) this means we can only materialize partition columns when a
      // projector is specified
      if (options->projector != nullptr) {
        for (auto ancestor = ref; ancestor.forest == &forest_;
             ancestor = ancestor.parent()) {
          if (auto name_value = GetKey(*partitions_[ancestor.i])) {
            auto name = std::move(name_value->first);
            if (file.options->projector->schema()->GetFieldByName(name) == nullptr) {
              continue;
            }

            RETURN_NOT_OK(file.options->projector->SetDefaultValue(
                name, std::move(name_value->second)));
          }
        }
      }

      files.push_back(std::move(file));
    }

    return fs::PathForest::Continue;
  };

  DCHECK_OK(forest_.Visit(collect_files));

  auto file_to_fragment = [this](util::optional<SourceAndOptions> file,
                                 std::shared_ptr<DataFragment>* out) {
    return format_->MakeFragment(file->source, file->options).Value(out);
  };
  return MakeMaybeMapIterator(file_to_fragment,
                              MakeVectorOptionalIterator(std::move(files)));
}

}  // namespace dataset
}  // namespace arrow
