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

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(source_, scan_options_, context);
}

FileSystemSource::FileSystemSource(std::shared_ptr<Schema> schema,
                                   std::shared_ptr<Expression> root_partition,
                                   std::shared_ptr<FileFormat> format,
                                   std::shared_ptr<fs::FileSystem> filesystem,
                                   fs::PathForest forest,
                                   ExpressionVector file_partitions)
    : Source(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      forest_(std::move(forest)),
      partitions_(std::move(file_partitions)) {
  DCHECK_EQ(static_cast<size_t>(forest_.size()), partitions_.size());
}

Result<std::shared_ptr<Source>> FileSystemSource::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    fs::FileStatsVector stats) {
  ExpressionVector partitions(stats.size(), scalar(true));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(stats), std::move(partitions));
}

Result<std::shared_ptr<Source>> FileSystemSource::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    fs::FileStatsVector stats, ExpressionVector partitions) {
  ARROW_ASSIGN_OR_RAISE(auto forest, fs::PathForest::Make(std::move(stats), &partitions));
  return Make(std::move(schema), std::move(root_partition), std::move(format),
              std::move(filesystem), std::move(forest), std::move(partitions));
}

Result<std::shared_ptr<Source>> FileSystemSource::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    fs::PathForest forest, ExpressionVector partitions) {
  return std::shared_ptr<Source>(new FileSystemSource(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(forest), std::move(partitions)));
}

std::string FileSystemSource::ToString() const {
  std::string repr = "FileSystemSource:";

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

FragmentIterator FileSystemSource::GetFragmentsImpl(
    std::shared_ptr<ScanOptions> root_options) {
  FragmentVector fragments;
  std::vector<std::shared_ptr<ScanOptions>> options(forest_.size());

  auto collect_fragments = [&](fs::PathForest::Ref ref) -> fs::PathForest::MaybePrune {
    auto partition = partitions_[ref.i];

    // if available, copy parent's filter and projector
    // (which are appropriately simplified and loaded with default values)
    if (auto parent = ref.parent()) {
      options[ref.i].reset(new ScanOptions(*options[parent.i]));
    } else {
      options[ref.i].reset(new ScanOptions(*root_options));
    }

    // simplify filter by partition information
    auto filter = options[ref.i]->filter->Assume(partition);
    options[ref.i]->filter = filter;

    if (filter->IsNull() || filter->Equals(false)) {
      // directories (and descendants) which can't satisfy the filter are pruned
      return fs::PathForest::Prune;
    }

    // if possible, extract a partition key and pass it to the projector
    auto projector = &options[ref.i]->projector;
    if (auto name_value = GetKey(*partition)) {
      auto index = projector->schema()->GetFieldIndex(name_value->first);
      if (index != -1) {
        RETURN_NOT_OK(projector->SetDefaultValue(index, std::move(name_value->second)));
      }
    }

    if (ref.stats().IsFile()) {
      // generate a fragment for this file
      FileSource src(ref.stats().path(), filesystem_.get());
      ARROW_ASSIGN_OR_RAISE(auto fragment, format_->MakeFragment(src, options[ref.i]));
      fragments.push_back(std::move(fragment));
    }

    return fs::PathForest::Continue;
  };

  auto status = forest_.Visit(collect_fragments);
  if (!status.ok()) {
    return MakeErrorIterator<std::shared_ptr<Fragment>>(status);
  }

  return MakeVectorIterator(std::move(fragments));
}

}  // namespace dataset
}  // namespace arrow
